//! Benchmark: message throughput and end-to-end latency across a
//! sweep of mesh hop counts. See ADR-0030.
//!
//! Run with `cargo run --release --example bench_mesh` - a debug
//! build's numbers wouldn't be honest, given this workspace's release
//! profile (LTO, codegen-units = 1). Prints a results table to stdout;
//! this is a manually-run, point-in-time report, not a CI-tracked
//! regression suite (see the ADR for why).

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use thoth_mesh_core::{Envelope, MessageKind, PeerId, Topic, async_framing};
use thoth_mesh_node::test_support::eventually;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};

/// Hop counts to sweep. 0 is a single node - publisher and subscriber
/// both connect to it directly, no peer link at all - the floor every
/// other row is read against. N chains N + 1 nodes the same way
/// `tests/integration.rs`'s `multi_hop_interest_propagates_across_a_
/// chain_of_peers` does: node `i` seeds only node `i - 1`'s address.
const HOP_COUNTS: &[usize] = &[0, 1, 2, 4, 8];

/// How many `Publish` envelopes each row sends. `Publish` gets no
/// `Ack` in this protocol (`PROTOCOL.md`), so nothing but the
/// connection's own backpressure paces the burst - which is exactly
/// the sustained-throughput question this benchmark is asking. Not
/// every one of these is guaranteed to arrive - see
/// DELIVERY_IDLE_TIMEOUT below.
const MESSAGE_COUNT: usize = 5_000;

/// A small, representative event payload - this is not a
/// transfer-throughput test, so payload size isn't swept.
const PAYLOAD_SIZE: usize = 64;

/// How long the subscriber waits for the *next* delivery before
/// deciding the burst is over. An unpaced burst this size can outrun
/// the per-topic broadcast channel faster than a forwarder several
/// hops downstream can drain it, and a `Lagged` gap wider than the
/// replay buffer (ADR-0024) is unrecoverable - some messages can be
/// genuinely, permanently lost under enough hops and load, not a bug
/// this benchmark should assume can't happen. 2s is generous relative
/// to the worst per-message latency actually observed even at 8 hops
/// with no loss at all.
const DELIVERY_IDLE_TIMEOUT: Duration = Duration::from_secs(2);

const BENCH_TOPIC: &str = "bench.throughput";

#[tokio::main]
async fn main() {
    // At "warn", this surfaces the same forwarder-lag warnings
    // (ADR-0024) that explain *why* delivered might come in under
    // sent at higher hop counts - useful context for reading the
    // table below, not debug noise.
    tracing_subscriber::fmt().with_env_filter("warn").init();
    println!(
        "thoth-mesh bench_mesh - {MESSAGE_COUNT} messages, {PAYLOAD_SIZE}-byte payload, per hop count\n"
    );
    println!(
        "{:>5}  {:>10}  {:>10}  {:>9}  {:>9}  {:>9}  {:>9}",
        "hops", "delivered", "msgs/sec", "min (ms)", "p50 (ms)", "p95 (ms)", "max (ms)"
    );

    let mut any_lossy = false;
    for &hops in HOP_COUNTS {
        let result = run_once(hops).await;
        let lossy = result.delivered < result.sent;
        any_lossy |= lossy;
        // A lossy run's elapsed window only covers messages that
        // actually arrived before the pipeline gave up on the rest -
        // not a full, comparable sustained-rate measurement the way a
        // complete run's is (see the note below the table). Printing
        // a number there would invite comparing it directly against a
        // lossless row's, which is exactly the wrong reading.
        let throughput = if lossy {
            "n/a*".to_owned()
        } else {
            format!("{:.0}", result.throughput)
        };
        println!(
            "{:>5}  {:>10}  {:>10}  {:>9}  {:>9}  {:>9}  {:>9}",
            hops,
            format!("{}/{}", result.delivered, result.sent),
            throughput,
            result.min_ms,
            result.p50_ms,
            result.p95_ms,
            result.max_ms
        );
    }
    if any_lossy {
        println!(
            "\n* msgs/sec omitted: this row's elapsed window only covers messages that arrived \
             before the pipeline gave up on the rest (see DELIVERY_IDLE_TIMEOUT's doc comment) - \
             not a rate comparable to a row that delivered everything."
        );
    }
}

struct BenchResult {
    sent: usize,
    delivered: usize,
    throughput: f64,
    min_ms: i64,
    p50_ms: i64,
    p95_ms: i64,
    max_ms: i64,
}

async fn run_once(hops: usize) -> BenchResult {
    let topic: Topic = BENCH_TOPIC.parse().unwrap();

    // Build a chain of hops + 1 nodes.
    let mut nodes = Vec::with_capacity(hops + 1);
    let mut addrs = Vec::with_capacity(hops + 1);
    let mut prev_addr: Option<String> = None;
    for _ in 0..=hops {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let seeds = prev_addr.clone().into_iter().collect();
        nodes.push(thoth_mesh_node::spawn(listener, seeds));
        addrs.push(addr);
        prev_addr = Some(addr.to_string());
    }

    // Wait for the whole chain to actually be linked, adjacent pair by
    // adjacent pair, before measuring - this benchmark cares about
    // steady-state throughput, not cold-start convergence. Node `i`
    // dialed node `i - 1`, so it's `i` whose membership view proves
    // the link is up.
    for pair in nodes.windows(2) {
        eventually(|| pair[1].membership.is_reachable(pair[0].id)).await;
    }

    let mut subscriber = connect(*addrs.last().unwrap()).await;
    let sub = Envelope::new(
        PeerId::new(),
        MessageKind::Subscribe {
            filter: topic.clone().into(),
        },
    );
    send(&mut subscriber, &sub).await;
    recv(&mut subscriber).await; // subscribe ack

    // Give the subscribe interest a moment to propagate all the way
    // back to the first node before publishing - otherwise the first
    // several messages would race that propagation and be dropped
    // rather than delivered, skewing the measurement toward cold
    // start instead of steady state. Interest propagation has no
    // externally-observable membership-style condition to poll for
    // (unlike link reachability above), so this is a fixed delay
    // rather than an `eventually`-style wait.
    tokio::time::sleep(Duration::from_millis(50) * (hops as u32 + 1)).await;

    let mut publisher = connect(addrs[0]).await;
    let start = Instant::now();
    let publish_topic = topic.clone();
    let publish_task = tokio::spawn(async move {
        for _ in 0..MESSAGE_COUNT {
            let envelope = Envelope::new(
                PeerId::new(),
                MessageKind::Publish {
                    topic: publish_topic.clone(),
                    payload: vec![0u8; PAYLOAD_SIZE],
                },
            );
            send(&mut publisher, &envelope).await;
        }
    });

    // Read until DELIVERY_IDLE_TIMEOUT passes with nothing arriving -
    // not until exactly MESSAGE_COUNT arrives, which could hang
    // forever if any were unrecoverably lost (see
    // DELIVERY_IDLE_TIMEOUT's doc comment).
    let mut latencies_ms = Vec::with_capacity(MESSAGE_COUNT);
    let mut delivered = 0usize;
    // Tracks the instant of the *last actual delivery*, separately
    // from when the collection loop itself exits - which, on a lossy
    // run, is DELIVERY_IDLE_TIMEOUT later than that. Throughput has to
    // be measured against the former; the latter would silently
    // deflate it by counting dead idle time as part of the burst.
    let mut last_delivery = start;
    while let Ok(Some(bytes)) = timeout(
        DELIVERY_IDLE_TIMEOUT,
        async_framing::read_frame(&mut subscriber),
    )
    .await
    .map(|result| result.ok())
    {
        let envelope = Envelope::from_bytes(&bytes).unwrap();
        delivered += 1;
        last_delivery = Instant::now();
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        if let Some((secs, nanos)) = envelope.id.timestamp() {
            let published = Duration::new(secs, nanos);
            if let Some(latency) = now.checked_sub(published) {
                latencies_ms.push(latency.as_millis() as i64);
            }
        }
        if delivered == MESSAGE_COUNT {
            break;
        }
    }
    let elapsed = last_delivery.duration_since(start);
    // Done collecting - if the publisher is still going (a loss
    // scenario cut collection short), stop it rather than let it run
    // on into the next hop count's measurement.
    publish_task.abort();

    latencies_ms.sort_unstable();
    BenchResult {
        sent: MESSAGE_COUNT,
        delivered,
        throughput: delivered as f64 / elapsed.as_secs_f64(),
        min_ms: *latencies_ms.first().unwrap_or(&0),
        p50_ms: percentile(&latencies_ms, 0.50),
        p95_ms: percentile(&latencies_ms, 0.95),
        max_ms: *latencies_ms.last().unwrap_or(&0),
    }
}

fn percentile(sorted: &[i64], p: f64) -> i64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[idx]
}

async fn connect(addr: std::net::SocketAddr) -> Compat<TcpStream> {
    TcpStream::connect(addr).await.unwrap().compat()
}

async fn send(stream: &mut Compat<TcpStream>, envelope: &Envelope) {
    let bytes = envelope.to_bytes().unwrap();
    async_framing::write_frame(stream, &bytes).await.unwrap();
}

async fn recv(stream: &mut Compat<TcpStream>) -> Envelope {
    let bytes = async_framing::read_frame(stream).await.unwrap();
    Envelope::from_bytes(&bytes).unwrap()
}
