//! End-to-end coverage that TLS (ADR-0016) doesn't just build - it
//! actually federates. A throwaway CA + leaf certs are generated and
//! written to temp files per test run (never checked in), since
//! `TlsConfig` takes file paths, matching how an operator would
//! configure it for real (see docs/OPERATIONS.md).

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use rcgen::{CertificateParams, KeyPair};
use thoth_mesh_core::{Envelope, MessageKind, Topic, async_framing};
use thoth_mesh_node::TlsConfig;
use thoth_mesh_node::test_support::eventually;
use thoth_mesh_tls::MaybeTlsStream;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};

const TEST_TIMEOUT: Duration = Duration::from_secs(5);

/// A throwaway CA, able to issue node identities signed by itself,
/// each written out as a `TlsConfig`'s three PEM files.
struct TestCa {
    cert: rcgen::Certificate,
    key: KeyPair,
    ca_path: PathBuf,
}

impl TestCa {
    fn new() -> Self {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(Vec::new()).unwrap();
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let cert = params.self_signed(&key).unwrap();
        let ca_path = write_temp("ca", &cert.pem());
        Self { cert, key, ca_path }
    }

    /// Issues a leaf identity for `127.0.0.1` (every node in these
    /// tests listens there) and writes out a full [`TlsConfig`].
    fn issue(&self) -> TlsConfig {
        let leaf_key = KeyPair::generate().unwrap();
        let leaf_params = CertificateParams::new(vec!["127.0.0.1".to_string()]).unwrap();
        let leaf_cert = leaf_params
            .signed_by(&leaf_key, &self.cert, &self.key)
            .unwrap();
        TlsConfig {
            cert: write_temp("cert", &leaf_cert.pem()),
            key: write_temp("key", &leaf_key.serialize_pem()),
            ca: self.ca_path.clone(),
        }
    }
}

fn write_temp(label: &str, pem: &str) -> PathBuf {
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!(
        "thoth-mesh-test-{}-{n}-{label}.pem",
        std::process::id()
    ));
    std::fs::write(&path, pem).unwrap();
    path
}

async fn connect_tls(addr: SocketAddr, ca: &TlsConfig) -> Compat<MaybeTlsStream> {
    let connector = thoth_mesh_tls::TlsConnector::from(std::sync::Arc::new(
        thoth_mesh_tls::client_config(thoth_mesh_tls::load_certs(&ca.ca).unwrap(), None).unwrap(),
    ));
    let tcp = TcpStream::connect(addr).await.unwrap();
    MaybeTlsStream::connect(&connector, tcp, &addr.to_string())
        .await
        .unwrap()
        .compat()
}

async fn send(stream: &mut Compat<MaybeTlsStream>, envelope: &Envelope) {
    let bytes = envelope.to_bytes().unwrap();
    async_framing::write_frame(stream, &bytes).await.unwrap();
}

async fn recv(stream: &mut Compat<MaybeTlsStream>) -> Envelope {
    let bytes = timeout(TEST_TIMEOUT, async_framing::read_frame(stream))
        .await
        .expect("timed out waiting for a frame")
        .unwrap();
    Envelope::from_bytes(&bytes).unwrap()
}

fn topic(s: &str) -> Topic {
    s.parse().unwrap()
}

#[tokio::test]
async fn two_tls_nodes_federate_and_a_tls_client_publishes_and_subscribes() {
    let ca = TestCa::new();

    let listener_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_a = listener_a.local_addr().unwrap();
    let node_a = thoth_mesh_node::spawn_with_tls(listener_a, Vec::new(), Some(ca.issue())).unwrap();

    let listener_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_b = listener_b.local_addr().unwrap();
    let node_b =
        thoth_mesh_node::spawn_with_tls(listener_b, vec![addr_a.to_string()], Some(ca.issue()))
            .unwrap();

    eventually(|| node_b.membership.is_reachable(node_a.id)).await;
    eventually(|| node_a.membership.is_reachable(node_b.id)).await;

    // A TLS client subscribes on node A...
    let client_identity = ca.issue();
    let mut subscriber = connect_tls(addr_a, &client_identity).await;
    let sub = Envelope::new(
        thoth_mesh_core::PeerId::new(),
        MessageKind::Subscribe {
            topic: topic("weather.updates"),
        },
    );
    send(&mut subscriber, &sub).await;
    assert_eq!(
        recv(&mut subscriber).await.kind,
        MessageKind::Ack {
            in_reply_to: sub.id
        }
    );

    // ...and a TLS client publishes on node B - delivery has to cross
    // the TLS'd peer link between A and B to reach the subscriber.
    let mut publisher = connect_tls(addr_b, &client_identity).await;
    let deadline = tokio::time::Instant::now() + TEST_TIMEOUT;
    loop {
        let publish = Envelope::new(
            thoth_mesh_core::PeerId::new(),
            MessageKind::Publish {
                topic: topic("weather.updates"),
                payload: b"sunny".to_vec(),
            },
        );
        send(&mut publisher, &publish).await;

        match timeout(
            Duration::from_millis(200),
            async_framing::read_frame(&mut subscriber),
        )
        .await
        {
            Ok(Ok(bytes)) => {
                let delivered = Envelope::from_bytes(&bytes).unwrap();
                if let MessageKind::Publish { payload, .. } = delivered.kind {
                    assert_eq!(payload, b"sunny");
                    break;
                }
            }
            _ => {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "publish never arrived at the subscriber over TLS"
                );
            }
        }
    }
}
