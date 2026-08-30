//! `thoth-mesh`: command-line client for publishing, subscribing, and
//! administering a thoth-mesh node.
//!
//! v1 is intentionally minimal (see issue #13): one topic per
//! invocation, no config file, no output formatting options. Admin
//! commands are deferred until the node actually has an admin
//! protocol to talk to.

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use thoth_mesh_core::{Envelope, MessageKind, PeerId, Topic, TopicFilter, async_framing};
use thoth_mesh_tls::{MaybeTlsStream, TlsConnector, client_config, load_certs, load_private_key};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};

pub use thoth_mesh_core::DEFAULT_ADDR;

#[derive(Parser, Debug)]
#[command(version, about)]
pub struct Cli {
    /// Node address to connect to.
    #[arg(long, global = true, default_value = DEFAULT_ADDR)]
    pub addr: String,

    /// CA certificate (PEM) to trust for verifying the node's TLS
    /// certificate. Enables TLS for this connection - without it, the
    /// connection is plaintext, as before. See ADR-0016.
    #[arg(long, global = true)]
    pub tls_ca: Option<PathBuf>,

    /// This client's own TLS certificate (PEM), to identify itself to
    /// the node. Optional even with --tls-ca set - a plain client is
    /// still the `anonymous` principal for any `--topic-acl` on the
    /// node it's talking to (see ADR-0018), which may or may not be
    /// enough depending on how that node's configured. Requires
    /// --tls-key.
    #[arg(long, global = true, requires = "tls_key")]
    pub tls_cert: Option<PathBuf>,

    /// This client's own TLS private key (PEM). See --tls-cert.
    #[arg(long, global = true, requires = "tls_cert")]
    pub tls_key: Option<PathBuf>,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Publish a payload to a topic and exit.
    Publish {
        /// Topic to publish to.
        topic: String,
        /// Payload to send, as UTF-8 text.
        payload: String,
    },
    /// Subscribe to a topic filter and print delivered messages until
    /// interrupted (Ctrl-C).
    Subscribe {
        /// Topic filter to subscribe to - a literal topic name, or an
        /// MQTT-style wildcard pattern (`+` matches one segment, a
        /// trailing `#` matches the rest; see ADR-0022).
        filter: String,
    },
}

/// Runs the CLI: connects to `cli.addr` and executes `cli.command`.
pub async fn run(cli: Cli) -> std::io::Result<()> {
    // Validated up front, before dialing anything - a malformed topic
    // or filter should fail immediately, not only after a possibly
    // slow connect/TLS handshake.
    match &cli.command {
        Command::Publish { topic, .. } => {
            parse_topic(topic)?;
        }
        Command::Subscribe { filter } => {
            parse_filter(filter)?;
        }
    }

    let connector = build_connector(&cli)?;
    let tcp = TcpStream::connect(&cli.addr).await?;
    let stream = match &connector {
        Some(connector) => MaybeTlsStream::connect(connector, tcp, &cli.addr)
            .await
            .map_err(std::io::Error::other)?,
        None => MaybeTlsStream::Plain(tcp),
    };
    let mut conn = stream.compat();
    let sender = PeerId::new();

    match cli.command {
        Command::Publish { topic, payload } => {
            let topic = parse_topic(&topic)?;
            let envelope = Envelope::new(
                sender,
                MessageKind::Publish {
                    topic,
                    payload: payload.into_bytes(),
                },
            );
            send(&mut conn, &envelope).await
        }
        Command::Subscribe { filter } => {
            let filter = parse_filter(&filter)?;
            subscribe_and_print(&mut conn, sender, filter).await
        }
    }
}

/// Sends a `Subscribe` for `filter`, waits for its ack, then prints
/// each delivered message until interrupted (Ctrl-C).
async fn subscribe_and_print(
    conn: &mut Compat<MaybeTlsStream>,
    sender: PeerId,
    filter: TopicFilter,
) -> std::io::Result<()> {
    subscribe(conn, sender, filter.clone()).await?;
    println!("Subscribed to {filter}. Waiting for messages (Ctrl-C to stop)...");

    loop {
        tokio::select! {
            envelope = recv(conn) => print_if_publish(envelope?),
            _ = tokio::signal::ctrl_c() => return Ok(()),
        }
    }
}

fn print_if_publish(envelope: Envelope) {
    if let MessageKind::Publish { topic, payload } = envelope.kind {
        println!("[{topic}] {}", String::from_utf8_lossy(&payload));
    }
}

/// Sends a `Subscribe` for `filter` and waits for the matching `Ack` -
/// or, if the node refuses it (e.g. a `--topic-acl`, see ADR-0018, or
/// a wildcard `filter` with one configured, see ADR-0022), the
/// matching `Error`, surfaced as an `Err` rather than waiting forever
/// for an `Ack` that will never come.
///
/// This is the actual protocol logic behind `subscribe_and_print`,
/// factored out so tests can drive it directly without simulating
/// Ctrl-C or capturing stdout.
async fn subscribe(
    conn: &mut Compat<MaybeTlsStream>,
    sender: PeerId,
    filter: TopicFilter,
) -> std::io::Result<()> {
    let envelope = Envelope::new(sender, MessageKind::Subscribe { filter });
    send(conn, &envelope).await?;
    loop {
        let received = recv(conn).await?;
        match &received.kind {
            MessageKind::Ack { in_reply_to } if *in_reply_to == envelope.id => return Ok(()),
            MessageKind::Error {
                in_reply_to,
                message,
            } if *in_reply_to == Some(envelope.id) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    message.clone(),
                ));
            }
            _ => continue,
        }
    }
}

/// Builds this connection's TLS connector from `cli`'s `--tls-*`
/// flags, if `--tls-ca` was given - `None` (plaintext, as before)
/// otherwise. `--tls-cert`/`--tls-key`, if also given, are presented
/// as this client's own identity; clap's `requires` already enforces
/// they're both-or-neither. See ADR-0016.
fn build_connector(cli: &Cli) -> std::io::Result<Option<TlsConnector>> {
    let Some(ca_path) = &cli.tls_ca else {
        return Ok(None);
    };
    let to_io =
        |err: thoth_mesh_tls::TlsError| std::io::Error::new(std::io::ErrorKind::InvalidInput, err);

    let ca = load_certs(ca_path).map_err(to_io)?;
    let identity = match (&cli.tls_cert, &cli.tls_key) {
        (Some(cert_path), Some(key_path)) => Some((
            load_certs(cert_path).map_err(to_io)?,
            load_private_key(key_path).map_err(to_io)?,
        )),
        _ => None,
    };
    let config = client_config(ca, identity).map_err(to_io)?;
    Ok(Some(TlsConnector::from(std::sync::Arc::new(config))))
}

fn parse_topic(s: &str) -> std::io::Result<Topic> {
    s.parse().map_err(|err| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("invalid topic {s:?}: {err}"),
        )
    })
}

fn parse_filter(s: &str) -> std::io::Result<TopicFilter> {
    s.parse().map_err(|err| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("invalid topic filter {s:?}: {err}"),
        )
    })
}

async fn send(conn: &mut Compat<MaybeTlsStream>, envelope: &Envelope) -> std::io::Result<()> {
    let bytes = envelope
        .to_bytes()
        .map_err(|err| std::io::Error::other(err.to_string()))?;
    async_framing::write_frame(conn, &bytes)
        .await
        .map_err(|err| std::io::Error::other(err.to_string()))
}

async fn recv(conn: &mut Compat<MaybeTlsStream>) -> std::io::Result<Envelope> {
    let bytes = async_framing::read_frame(conn)
        .await
        .map_err(|err| std::io::Error::other(err.to_string()))?;
    Envelope::from_bytes(&bytes).map_err(|err| std::io::Error::other(err.to_string()))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use thoth_mesh_core::PeerId;
    use tokio::net::TcpListener;
    use tokio::time::timeout;

    use super::*;

    const TEST_TIMEOUT: Duration = Duration::from_secs(2);

    async fn spawn_test_node() -> std::net::SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(thoth_mesh_node::serve(listener, Vec::new()));
        addr
    }

    async fn connect(addr: std::net::SocketAddr) -> Compat<MaybeTlsStream> {
        let tcp = TcpStream::connect(addr).await.unwrap();
        MaybeTlsStream::Plain(tcp).compat()
    }

    #[tokio::test]
    async fn run_publish_delivers_to_a_real_subscriber() {
        let addr = spawn_test_node().await;

        // A raw subscriber, playing the role of another client.
        let mut subscriber = connect(addr).await;
        subscribe(
            &mut subscriber,
            PeerId::new(),
            "weather.updates".parse().unwrap(),
        )
        .await
        .unwrap();

        // The CLI's own public entry point, run against the same node.
        let cli = Cli {
            addr: addr.to_string(),
            tls_ca: None,
            tls_cert: None,
            tls_key: None,
            command: Command::Publish {
                topic: "weather.updates".into(),
                payload: "sunny".into(),
            },
        };
        run(cli).await.unwrap();

        let delivered = timeout(TEST_TIMEOUT, recv(&mut subscriber))
            .await
            .expect("timed out waiting for the publish")
            .unwrap();
        match delivered.kind {
            MessageKind::Publish { topic, payload } => {
                assert_eq!(topic, "weather.updates".parse().unwrap());
                assert_eq!(payload, b"sunny");
            }
            other => panic!("expected a Publish, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn subscribe_acks_then_receives_a_publish() {
        let addr = spawn_test_node().await;
        let mut client = connect(addr).await;
        let topic: Topic = "weather.updates".parse().unwrap();

        // subscribe() only returns once the ack for our own request
        // arrives - if it returned early or hung, this would fail or
        // time out.
        timeout(
            TEST_TIMEOUT,
            subscribe(&mut client, PeerId::new(), topic.clone().into()),
        )
        .await
        .expect("timed out waiting for the subscribe ack")
        .unwrap();

        let mut publisher = connect(addr).await;
        send(
            &mut publisher,
            &Envelope::new(
                PeerId::new(),
                MessageKind::Publish {
                    topic: topic.clone(),
                    payload: b"jam".to_vec(),
                },
            ),
        )
        .await
        .unwrap();

        let delivered = timeout(TEST_TIMEOUT, recv(&mut client))
            .await
            .expect("timed out waiting for the publish")
            .unwrap();
        assert_eq!(
            delivered.kind,
            MessageKind::Publish {
                topic,
                payload: b"jam".to_vec(),
            }
        );
    }

    #[test]
    fn parse_topic_rejects_invalid_topics() {
        assert!(parse_topic("").is_err());
    }

    #[tokio::test]
    async fn run_rejects_an_invalid_filter_before_connecting() {
        // Nothing listens on this address - if run() tried to connect
        // before validating its argument, this would time out or fail
        // with a connection error instead of InvalidInput.
        let cli = Cli {
            addr: "127.0.0.1:1".to_owned(),
            tls_ca: None,
            tls_cert: None,
            tls_key: None,
            command: Command::Subscribe {
                filter: "weather.#.updates".to_owned(),
            },
        };
        let err = timeout(TEST_TIMEOUT, run(cli))
            .await
            .expect("run() should fail validation before ever touching the network")
            .unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[test]
    fn parse_filter_rejects_invalid_filters() {
        assert!(parse_filter("").is_err());
        assert!(parse_filter("weather.#.updates").is_err());
    }

    #[test]
    fn parse_filter_accepts_a_wildcard_pattern() {
        assert!(parse_filter("weather.+").is_ok());
        assert!(parse_filter("weather.#").is_ok());
    }

    #[tokio::test]
    async fn subscribe_with_a_wildcard_filter_receives_a_matching_publish() {
        // The CLI's own Command::Subscribe now accepts ADR-0022's
        // wildcard syntax, not just a literal topic name.
        let addr = spawn_test_node().await;
        let mut client = connect(addr).await;
        let filter: TopicFilter = "weather.+".parse().unwrap();

        timeout(TEST_TIMEOUT, subscribe(&mut client, PeerId::new(), filter))
            .await
            .expect("timed out waiting for the subscribe ack")
            .unwrap();

        let mut publisher = connect(addr).await;
        let topic: Topic = "weather.updates".parse().unwrap();
        send(
            &mut publisher,
            &Envelope::new(
                PeerId::new(),
                MessageKind::Publish {
                    topic: topic.clone(),
                    payload: b"sunny".to_vec(),
                },
            ),
        )
        .await
        .unwrap();

        let delivered = timeout(TEST_TIMEOUT, recv(&mut client))
            .await
            .expect("timed out waiting for the publish")
            .unwrap();
        assert_eq!(
            delivered.kind,
            MessageKind::Publish {
                topic,
                payload: b"sunny".to_vec(),
            }
        );
    }

    #[tokio::test]
    async fn subscribe_returns_an_error_when_the_topic_acl_rejects_it() {
        // A node with a topic ACL that grants nothing at all for
        // "secret.topic" - every Subscribe to it is refused (ADR-0018).
        let acl = thoth_mesh_node::TopicAcl::parse(["anonymous|sub|weather.updates"]).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(thoth_mesh_node::serve_with_tls(
            listener,
            Vec::new(),
            thoth_mesh_node::NodeOptions {
                topic_acl: Some(acl),
                ..Default::default()
            },
        ));

        let mut client = connect(addr).await;
        let err = timeout(
            TEST_TIMEOUT,
            subscribe(&mut client, PeerId::new(), "secret.topic".parse().unwrap()),
        )
        .await
        .expect("timed out waiting for the rejection - subscribe() must not hang on an Error")
        .unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::PermissionDenied);
    }
}
