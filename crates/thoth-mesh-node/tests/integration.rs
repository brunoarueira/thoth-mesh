use std::net::SocketAddr;
use std::time::Duration;

use thoth_mesh_core::async_framing;
use thoth_mesh_core::{Envelope, MessageKind, PeerId, Topic};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};

const TEST_TIMEOUT: Duration = Duration::from_secs(2);

async fn spawn_test_node() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(thoth_mesh_node::serve(listener, Vec::new()));
    addr
}

async fn connect(addr: SocketAddr) -> Compat<TcpStream> {
    TcpStream::connect(addr).await.unwrap().compat()
}

async fn send(stream: &mut Compat<TcpStream>, envelope: &Envelope) {
    let bytes = envelope.to_bytes().unwrap();
    async_framing::write_frame(stream, &bytes).await.unwrap();
}

async fn recv(stream: &mut Compat<TcpStream>) -> Envelope {
    let bytes = timeout(TEST_TIMEOUT, async_framing::read_frame(stream))
        .await
        .expect("timed out waiting for a frame")
        .unwrap();
    Envelope::from_bytes(&bytes).unwrap()
}

async fn recv_times_out(stream: &mut Compat<TcpStream>) -> bool {
    timeout(
        Duration::from_millis(200),
        async_framing::read_frame(stream),
    )
    .await
    .is_err()
}

fn topic(s: &str) -> Topic {
    s.parse::<Topic>().unwrap()
}

#[tokio::test]
async fn subscribe_receives_ack() {
    let addr = spawn_test_node().await;
    let mut client = connect(addr).await;

    let sub = Envelope::new(
        PeerId::new(),
        MessageKind::Subscribe {
            topic: topic("weather.updates"),
        },
    );
    send(&mut client, &sub).await;

    let ack = recv(&mut client).await;
    assert_eq!(
        ack.kind,
        MessageKind::Ack {
            in_reply_to: sub.id
        }
    );
}

#[tokio::test]
async fn unsubscribe_receives_ack() {
    let addr = spawn_test_node().await;
    let mut client = connect(addr).await;

    let sub = Envelope::new(
        PeerId::new(),
        MessageKind::Subscribe {
            topic: topic("weather.updates"),
        },
    );
    send(&mut client, &sub).await;
    recv(&mut client).await; // subscribe ack

    let unsub = Envelope::new(
        PeerId::new(),
        MessageKind::Unsubscribe {
            topic: topic("weather.updates"),
        },
    );
    send(&mut client, &unsub).await;

    let ack = recv(&mut client).await;
    assert_eq!(
        ack.kind,
        MessageKind::Ack {
            in_reply_to: unsub.id
        }
    );
}

#[tokio::test]
async fn publish_delivers_to_subscriber() {
    let addr = spawn_test_node().await;
    let mut subscriber = connect(addr).await;
    let mut publisher = connect(addr).await;

    let sub = Envelope::new(
        PeerId::new(),
        MessageKind::Subscribe {
            topic: topic("weather.updates"),
        },
    );
    send(&mut subscriber, &sub).await;
    recv(&mut subscriber).await; // subscribe ack

    let publish = Envelope::new(
        PeerId::new(),
        MessageKind::Publish {
            topic: topic("weather.updates"),
            payload: b"sunny".to_vec(),
        },
    );
    send(&mut publisher, &publish).await;

    let delivered = recv(&mut subscriber).await;
    assert_eq!(delivered.id, publish.id);
    assert_eq!(delivered.kind, publish.kind);
}

#[tokio::test]
async fn multiple_subscribers_all_receive() {
    let addr = spawn_test_node().await;
    let mut sub_a = connect(addr).await;
    let mut sub_b = connect(addr).await;
    let mut publisher = connect(addr).await;

    for client in [&mut sub_a, &mut sub_b] {
        let sub = Envelope::new(
            PeerId::new(),
            MessageKind::Subscribe {
                topic: topic("weather.updates"),
            },
        );
        send(client, &sub).await;
        recv(client).await; // subscribe ack
    }

    let publish = Envelope::new(
        PeerId::new(),
        MessageKind::Publish {
            topic: topic("weather.updates"),
            payload: b"sunny".to_vec(),
        },
    );
    send(&mut publisher, &publish).await;

    assert_eq!(recv(&mut sub_a).await.id, publish.id);
    assert_eq!(recv(&mut sub_b).await.id, publish.id);
}

#[tokio::test]
async fn unsubscribed_client_does_not_receive_publish() {
    let addr = spawn_test_node().await;
    let mut client = connect(addr).await;
    let mut publisher = connect(addr).await;

    let sub = Envelope::new(
        PeerId::new(),
        MessageKind::Subscribe {
            topic: topic("weather.updates"),
        },
    );
    send(&mut client, &sub).await;
    recv(&mut client).await; // subscribe ack

    let unsub = Envelope::new(
        PeerId::new(),
        MessageKind::Unsubscribe {
            topic: topic("weather.updates"),
        },
    );
    send(&mut client, &unsub).await;
    recv(&mut client).await; // unsubscribe ack

    let publish = Envelope::new(
        PeerId::new(),
        MessageKind::Publish {
            topic: topic("weather.updates"),
            payload: b"sunny".to_vec(),
        },
    );
    send(&mut publisher, &publish).await;

    assert!(recv_times_out(&mut client).await);
}

#[tokio::test]
async fn distinct_topics_do_not_cross_deliver() {
    let addr = spawn_test_node().await;
    let mut subscriber = connect(addr).await;
    let mut publisher = connect(addr).await;

    let sub = Envelope::new(
        PeerId::new(),
        MessageKind::Subscribe {
            topic: topic("weather.updates"),
        },
    );
    send(&mut subscriber, &sub).await;
    recv(&mut subscriber).await; // subscribe ack

    let publish = Envelope::new(
        PeerId::new(),
        MessageKind::Publish {
            topic: topic("traffic.updates"),
            payload: b"jam".to_vec(),
        },
    );
    send(&mut publisher, &publish).await;

    assert!(recv_times_out(&mut subscriber).await);
}

#[tokio::test]
async fn hello_receives_a_hello_reply_with_our_listen_addr() {
    let addr = spawn_test_node().await;
    let mut client = connect(addr).await;

    let hello = Envelope::new(
        PeerId::new(),
        MessageKind::Hello {
            listen_addr: Some("127.0.0.1:49999".to_owned()),
        },
    );
    send(&mut client, &hello).await;

    let reply = recv(&mut client).await;
    assert_eq!(
        reply.kind,
        MessageKind::Hello {
            listen_addr: Some(addr.to_string())
        }
    );
}

#[tokio::test]
async fn malformed_frame_closes_connection() {
    let addr = spawn_test_node().await;
    let mut client = connect(addr).await;

    // A well-formed frame whose payload isn't a valid CBOR-encoded
    // Envelope at all.
    async_framing::write_frame(&mut client, b"not an envelope")
        .await
        .unwrap();

    // The server should close its side rather than hang or crash;
    // the next read surfaces an error (EOF) instead of timing out.
    let result = timeout(TEST_TIMEOUT, async_framing::read_frame(&mut client)).await;
    assert!(
        matches!(result, Ok(Err(_))),
        "expected the server to close the connection, got {result:?}"
    );
}
