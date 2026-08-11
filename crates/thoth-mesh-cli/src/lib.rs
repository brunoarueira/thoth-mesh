//! `thoth-mesh`: command-line client for publishing, subscribing, and
//! administering a thoth-mesh node.
//!
//! v1 is intentionally minimal (see issue #13): one topic per
//! invocation, no config file, no output formatting options. Admin
//! commands are deferred until the node actually has an admin
//! protocol to talk to.

use clap::{Parser, Subcommand};
use thoth_mesh_core::{Envelope, MessageKind, PeerId, Topic, async_framing};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};

pub use thoth_mesh_core::DEFAULT_ADDR;

#[derive(Parser, Debug)]
#[command(version, about)]
pub struct Cli {
    /// Node address to connect to.
    #[arg(long, global = true, default_value = DEFAULT_ADDR)]
    pub addr: String,

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
    /// Subscribe to a topic and print delivered messages until
    /// interrupted (Ctrl-C).
    Subscribe {
        /// Topic to subscribe to.
        topic: String,
    },
}

/// Runs the CLI: connects to `cli.addr` and executes `cli.command`.
pub async fn run(cli: Cli) -> std::io::Result<()> {
    let topic_arg = match &cli.command {
        Command::Publish { topic, .. } => topic,
        Command::Subscribe { topic } => topic,
    };
    let topic = parse_topic(topic_arg)?;

    let stream = TcpStream::connect(&cli.addr).await?;
    let mut conn = stream.compat();
    let sender = PeerId::new();

    match cli.command {
        Command::Publish { payload, .. } => {
            let envelope = Envelope::new(
                sender,
                MessageKind::Publish {
                    topic,
                    payload: payload.into_bytes(),
                },
            );
            send(&mut conn, &envelope).await
        }
        Command::Subscribe { .. } => subscribe_and_print(&mut conn, sender, topic).await,
    }
}

/// Sends a `Subscribe` for `topic`, waits for its ack, then prints
/// each delivered message until interrupted (Ctrl-C).
async fn subscribe_and_print(
    conn: &mut Compat<TcpStream>,
    sender: PeerId,
    topic: Topic,
) -> std::io::Result<()> {
    subscribe(conn, sender, topic.clone()).await?;
    println!("Subscribed to {topic}. Waiting for messages (Ctrl-C to stop)...");

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

/// Sends a `Subscribe` for `topic` and waits for the matching `Ack`.
///
/// This is the actual protocol logic behind `subscribe_and_print`,
/// factored out so tests can drive it directly without simulating
/// Ctrl-C or capturing stdout.
async fn subscribe(
    conn: &mut Compat<TcpStream>,
    sender: PeerId,
    topic: Topic,
) -> std::io::Result<()> {
    let envelope = Envelope::new(sender, MessageKind::Subscribe { topic });
    send(conn, &envelope).await?;
    loop {
        let received = recv(conn).await?;
        if matches!(&received.kind, MessageKind::Ack { in_reply_to } if *in_reply_to == envelope.id)
        {
            return Ok(());
        }
    }
}

fn parse_topic(s: &str) -> std::io::Result<Topic> {
    s.parse().map_err(|err| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("invalid topic {s:?}: {err}"),
        )
    })
}

async fn send(conn: &mut Compat<TcpStream>, envelope: &Envelope) -> std::io::Result<()> {
    let bytes = envelope
        .to_bytes()
        .map_err(|err| std::io::Error::other(err.to_string()))?;
    async_framing::write_frame(conn, &bytes)
        .await
        .map_err(|err| std::io::Error::other(err.to_string()))
}

async fn recv(conn: &mut Compat<TcpStream>) -> std::io::Result<Envelope> {
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

    async fn connect(addr: std::net::SocketAddr) -> Compat<TcpStream> {
        TcpStream::connect(addr).await.unwrap().compat()
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
            subscribe(&mut client, PeerId::new(), topic.clone()),
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
}
