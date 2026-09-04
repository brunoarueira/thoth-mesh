//! End-to-end coverage that TLS (ADR-0016) doesn't just build - it
//! actually federates. A throwaway CA + leaf certs are generated and
//! written to temp files per test run (never checked in), since
//! `TlsConfig` takes file paths, matching how an operator would
//! configure it for real (see docs/OPERATIONS.md).

use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use rcgen::{CertificateParams, KeyPair};
use thoth_mesh_core::{Envelope, MessageKind, Topic, async_framing};
use thoth_mesh_node::test_support::eventually;
use thoth_mesh_node::{NodeOptions, TlsConfig};
use thoth_mesh_tls::MaybeTlsStream;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};

const TEST_TIMEOUT: Duration = Duration::from_secs(5);

/// A throwaway CA, able to issue node identities signed by itself,
/// each written out as a `TlsConfig`'s three PEM files.
struct TestCa {
    issuer: rcgen::Issuer<'static, KeyPair>,
    ca_path: PathBuf,
}

impl TestCa {
    fn new() -> Self {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(Vec::new()).unwrap();
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let cert = params.self_signed(&key).unwrap();
        let ca_path = write_temp("ca", &cert.pem());
        let issuer = rcgen::Issuer::new(params, key);
        Self { issuer, ca_path }
    }

    /// Issues a leaf identity for `127.0.0.1` (every node in these
    /// tests listens there) and writes out a full [`TlsConfig`], with
    /// no `--allow-peer` enforcement (see [`TestCa::issue_allowing`]
    /// for that).
    fn issue(&self) -> TlsConfig {
        self.issue_allowing(None)
    }

    /// Like [`TestCa::issue`], but with `allowed_peers` as the
    /// resulting `TlsConfig`'s allowlist (ADR-0017).
    fn issue_allowing(&self, allowed_peers: impl Into<Option<HashSet<[u8; 32]>>>) -> TlsConfig {
        let leaf_key = KeyPair::generate().unwrap();
        let leaf_params = CertificateParams::new(vec!["127.0.0.1".to_string()]).unwrap();
        let leaf_cert = leaf_params.signed_by(&leaf_key, &self.issuer).unwrap();
        TlsConfig {
            cert: write_temp("cert", &leaf_cert.pem()),
            key: write_temp("key", &leaf_key.serialize_pem()),
            ca: self.ca_path.clone(),
            allowed_peers: allowed_peers.into(),
        }
    }
}

/// The fingerprint (ADR-0017) of the leaf certificate `config` was
/// [`TestCa::issue`]d with - what an `--allow-peer` entry for it would
/// have to contain.
fn fingerprint_of(config: &TlsConfig) -> [u8; 32] {
    let certs = thoth_mesh_tls::load_certs(&config.cert).unwrap();
    thoth_mesh_tls::fingerprint(&certs[0])
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
    connect_tls_as(addr, ca, None).await
}

/// Like [`connect_tls`], but presents `identity` as this connection's
/// own client certificate when given - what a real peer link's dial
/// side always does (ADR-0016), and what an ADR-0017 allowlist test
/// needs to control directly, without going through a whole
/// [`thoth_mesh_node::spawn_with_tls`] node to produce one.
async fn connect_tls_as(
    addr: SocketAddr,
    ca: &TlsConfig,
    identity: Option<&TlsConfig>,
) -> Compat<MaybeTlsStream> {
    let identity = identity.map(|id| {
        (
            thoth_mesh_tls::load_certs(&id.cert).unwrap(),
            thoth_mesh_tls::load_private_key(&id.key).unwrap(),
        )
    });
    let connector = thoth_mesh_tls::TlsConnector::from(std::sync::Arc::new(
        thoth_mesh_tls::client_config(thoth_mesh_tls::load_certs(&ca.ca).unwrap(), identity)
            .unwrap(),
    ));
    let tcp = TcpStream::connect(addr).await.unwrap();
    MaybeTlsStream::connect(&connector, tcp, &addr.to_string())
        .await
        .unwrap()
        .compat()
}

/// Accepts one connection on `listener`, completing the TLS handshake
/// as the server side using `identity`. Stands in for a real peer's
/// accept side without going through a whole
/// [`thoth_mesh_node::spawn_with_tls`] node - just enough to test what
/// a dialing node does when *it's* the one enforcing an allowlist
/// (ADR-0017).
async fn accept_tls(listener: &TcpListener, identity: &TlsConfig) -> Compat<MaybeTlsStream> {
    let cert = thoth_mesh_tls::load_certs(&identity.cert).unwrap();
    let key = thoth_mesh_tls::load_private_key(&identity.key).unwrap();
    let ca = thoth_mesh_tls::load_certs(&identity.ca).unwrap();
    let acceptor = thoth_mesh_tls::TlsAcceptor::from(std::sync::Arc::new(
        thoth_mesh_tls::server_config(cert, key, ca).unwrap(),
    ));
    let (tcp, _) = listener.accept().await.unwrap();
    MaybeTlsStream::accept(&acceptor, tcp)
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
    let node_a = thoth_mesh_node::spawn_with_tls(
        listener_a,
        Vec::new(),
        NodeOptions {
            tls: Some(ca.issue()),
            ..Default::default()
        },
    )
    .unwrap();

    let listener_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_b = listener_b.local_addr().unwrap();
    let node_b = thoth_mesh_node::spawn_with_tls(
        listener_b,
        vec![addr_a.to_string()],
        NodeOptions {
            tls: Some(ca.issue()),
            ..Default::default()
        },
    )
    .unwrap();

    eventually(|| node_b.membership.is_reachable(node_a.id)).await;
    eventually(|| node_a.membership.is_reachable(node_b.id)).await;

    // A TLS client subscribes on node A...
    let client_identity = ca.issue();
    let mut subscriber = connect_tls(addr_a, &client_identity).await;
    let sub = Envelope::new(
        thoth_mesh_core::PeerId::new(),
        MessageKind::Subscribe {
            filter: topic("weather.updates").into(),
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

/// ADR-0017: with `--allow-peer` (a `TlsConfig::allowed_peers`)
/// configured on both sides and each naming the other's fingerprint,
/// federation still works exactly as it does with no allowlist at all
/// - enforcement doesn't get in the way of a peer it's actually
/// supposed to allow.
#[tokio::test]
async fn two_tls_nodes_federate_with_a_mutual_allowlist() {
    let ca = TestCa::new();

    let mut node_a_identity = ca.issue();
    let mut node_b_identity = ca.issue();
    node_a_identity.allowed_peers = Some(HashSet::from([fingerprint_of(&node_b_identity)]));
    node_b_identity.allowed_peers = Some(HashSet::from([fingerprint_of(&node_a_identity)]));

    let listener_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_a = listener_a.local_addr().unwrap();
    let node_a = thoth_mesh_node::spawn_with_tls(
        listener_a,
        Vec::new(),
        NodeOptions {
            tls: Some(node_a_identity),
            ..Default::default()
        },
    )
    .unwrap();

    let listener_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let node_b = thoth_mesh_node::spawn_with_tls(
        listener_b,
        vec![addr_a.to_string()],
        NodeOptions {
            tls: Some(node_b_identity),
            ..Default::default()
        },
    )
    .unwrap();

    eventually(|| node_b.membership.is_reachable(node_a.id)).await;
    eventually(|| node_a.membership.is_reachable(node_b.id)).await;
}

/// ADR-0017, accept side: a connection presenting a CA-signed but
/// unlisted certificate gets a `MessageKind::Error` referencing its
/// `Hello`, then the connection is closed - it never becomes a
/// registered peer link.
#[tokio::test]
async fn accept_side_rejects_an_unlisted_peer_certificate() {
    let ca = TestCa::new();

    // Node A's allowlist names a fingerprint that belongs to nobody in
    // this test, so any peer that dials in is rejected regardless of
    // how legitimate its certificate otherwise is.
    let node_a_identity = ca.issue_allowing(HashSet::from([[0u8; 32]]));

    let listener_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_a = listener_a.local_addr().unwrap();
    let node_a = thoth_mesh_node::spawn_with_tls(
        listener_a,
        Vec::new(),
        NodeOptions {
            tls: Some(node_a_identity),
            ..Default::default()
        },
    )
    .unwrap();

    // Dial in "as a peer": a real, CA-signed identity (so the TLS
    // handshake itself succeeds), then a Hello, exactly what a real
    // peer link's first message looks like.
    let peer_identity = ca.issue();
    let peer_id = thoth_mesh_core::PeerId::new();
    let mut conn = connect_tls_as(addr_a, &peer_identity, Some(&peer_identity)).await;
    let hello = Envelope::new(peer_id, MessageKind::Hello { listen_addr: None });
    send(&mut conn, &hello).await;

    match recv(&mut conn).await.kind {
        MessageKind::Error { in_reply_to, .. } => assert_eq!(in_reply_to, Some(hello.id)),
        other => panic!("expected an Error, got {other:?}"),
    }

    // No Hello reply follows - the connection closes instead.
    let closed = timeout(TEST_TIMEOUT, async_framing::read_frame(&mut conn)).await;
    assert!(
        closed.unwrap().is_err(),
        "connection should have closed after the rejection"
    );
    assert!(!node_a.membership.is_reachable(peer_id));
}

/// ADR-0017, dial side: a node dialing a seed peer whose certificate
/// isn't on its own allowlist rejects the link itself, symmetrically
/// with the accept side - the same `Error`-then-close, and the link
/// never registers, even though this node is the one that dialed.
#[tokio::test]
async fn dial_side_rejects_an_unlisted_seed_peer_certificate() {
    let ca = TestCa::new();

    // A raw TLS peer, standing in for a real thoth-mesh-node peer,
    // that the node under test will dial as a seed peer.
    let raw_peer_identity = ca.issue();
    let raw_peer_fingerprint = fingerprint_of(&raw_peer_identity);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let raw_peer_addr = listener.local_addr().unwrap();

    // The dialing node's allowlist doesn't include the raw peer's
    // fingerprint.
    assert_ne!(raw_peer_fingerprint, [0u8; 32]);
    let dialer_identity = ca.issue_allowing(HashSet::from([[0u8; 32]]));

    let dialer = thoth_mesh_node::spawn_with_tls(
        TcpListener::bind("127.0.0.1:0").await.unwrap(),
        vec![raw_peer_addr.to_string()],
        NodeOptions {
            tls: Some(dialer_identity),
            ..Default::default()
        },
    )
    .unwrap();

    let mut conn = accept_tls(&listener, &raw_peer_identity).await;
    let their_hello = recv(&mut conn).await;
    assert!(matches!(their_hello.kind, MessageKind::Hello { .. }));

    let raw_peer_id = thoth_mesh_core::PeerId::new();
    let our_hello = Envelope::new(raw_peer_id, MessageKind::Hello { listen_addr: None });
    send(&mut conn, &our_hello).await;

    match recv(&mut conn).await.kind {
        MessageKind::Error { in_reply_to, .. } => assert_eq!(in_reply_to, Some(our_hello.id)),
        other => panic!("expected an Error, got {other:?}"),
    }

    let closed = timeout(TEST_TIMEOUT, async_framing::read_frame(&mut conn)).await;
    assert!(
        closed.unwrap().is_err(),
        "connection should have closed after the rejection"
    );
    assert!(!dialer.membership.is_reachable(raw_peer_id));
}

/// ADR-0018, using a TLS certificate fingerprint as the principal
/// (rather than `anonymous`, which `tests/topic_acl.rs` already
/// covers over plain TCP): two different client certificates get two
/// different outcomes publishing to the same topic, based on which
/// one a `--topic-acl` entry names.
#[tokio::test]
async fn topic_acl_distinguishes_principals_by_certificate_fingerprint() {
    let ca = TestCa::new();
    let node_identity = ca.issue();
    let allowed_client = ca.issue();
    let other_client = ca.issue();

    let allowed_fingerprint = fingerprint_of(&allowed_client)
        .iter()
        .map(|byte| format!("{byte:02X}"))
        .collect::<String>();
    let acl = thoth_mesh_node::TopicAcl::parse([
        format!("{allowed_fingerprint}|pub|sensors.data").as_str(),
        "anonymous|sub|sensors.data",
    ])
    .unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    thoth_mesh_node::spawn_with_tls(
        listener,
        Vec::new(),
        NodeOptions {
            tls: Some(node_identity),
            topic_acl: Some(acl),
            ..Default::default()
        },
    )
    .unwrap();

    let mut subscriber = connect_tls(addr, &allowed_client).await;
    let sub = Envelope::new(
        thoth_mesh_core::PeerId::new(),
        MessageKind::Subscribe {
            filter: topic("sensors.data").into(),
        },
    );
    send(&mut subscriber, &sub).await;
    assert_eq!(
        recv(&mut subscriber).await.kind,
        MessageKind::Ack {
            in_reply_to: sub.id
        }
    );

    // The listed client's publish gets delivered normally.
    let mut allowed_conn = connect_tls_as(addr, &allowed_client, Some(&allowed_client)).await;
    let publish = Envelope::new(
        thoth_mesh_core::PeerId::new(),
        MessageKind::Publish {
            topic: topic("sensors.data"),
            payload: b"42".to_vec(),
        },
    );
    send(&mut allowed_conn, &publish).await;
    assert_eq!(recv(&mut subscriber).await.id, publish.id);

    // A different, otherwise perfectly valid CA-signed client isn't on
    // that --topic-acl entry, so its publish is rejected instead.
    let mut other_conn = connect_tls_as(addr, &other_client, Some(&other_client)).await;
    let rejected = Envelope::new(
        thoth_mesh_core::PeerId::new(),
        MessageKind::Publish {
            topic: topic("sensors.data"),
            payload: b"should not arrive".to_vec(),
        },
    );
    send(&mut other_conn, &rejected).await;
    match recv(&mut other_conn).await.kind {
        MessageKind::Error { in_reply_to, .. } => assert_eq!(in_reply_to, Some(rejected.id)),
        other => panic!("expected an Error, got {other:?}"),
    }
}

/// ADR-0038: a node with its own certificate derives `node_id` from
/// it, rather than a random `PeerId::new()`.
#[tokio::test]
async fn spawn_with_tls_derives_node_id_from_its_own_certificate() {
    let ca = TestCa::new();
    let identity = ca.issue();
    let expected_id = thoth_mesh_core::PeerId::from_fingerprint(fingerprint_of(&identity));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let node = thoth_mesh_node::spawn_with_tls(
        listener,
        Vec::new(),
        NodeOptions {
            tls: Some(identity),
            ..Default::default()
        },
    )
    .unwrap();

    assert_eq!(node.id, expected_id);
}

/// ADR-0038: unlike a random `PeerId::new()`, the derived identity is
/// the same every time the same certificate is used - the point of
/// deriving it at all (stable across a real restart, which reloads
/// the same cert/key files from disk).
#[tokio::test]
async fn spawn_with_tls_node_id_is_stable_across_a_restart_with_the_same_certificate() {
    let ca = TestCa::new();
    let identity = ca.issue();

    let listener_1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let node_1 = thoth_mesh_node::spawn_with_tls(
        listener_1,
        Vec::new(),
        NodeOptions {
            tls: Some(identity.clone()),
            ..Default::default()
        },
    )
    .unwrap();

    let listener_2 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let node_2 = thoth_mesh_node::spawn_with_tls(
        listener_2,
        Vec::new(),
        NodeOptions {
            tls: Some(identity),
            ..Default::default()
        },
    )
    .unwrap();

    assert_eq!(node_1.id, node_2.id);
}

/// ADR-0038: two nodes with two different certificates get two
/// different derived identities - not, say, some fixed placeholder
/// that'd make every TLS-enabled node collide.
#[tokio::test]
async fn spawn_with_tls_gives_distinct_certificates_distinct_node_ids() {
    let ca = TestCa::new();

    let listener_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let node_a = thoth_mesh_node::spawn_with_tls(
        listener_a,
        Vec::new(),
        NodeOptions {
            tls: Some(ca.issue()),
            ..Default::default()
        },
    )
    .unwrap();

    let listener_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let node_b = thoth_mesh_node::spawn_with_tls(
        listener_b,
        Vec::new(),
        NodeOptions {
            tls: Some(ca.issue()),
            ..Default::default()
        },
    )
    .unwrap();

    assert_ne!(node_a.id, node_b.id);
}

/// ADR-0039: a client authenticating with its own certificate but
/// claiming a different `PeerId` in its `Publish` gets silently
/// corrected to the identity its certificate actually implies -
/// including in what a subscriber actually receives, not just some
/// node-internal bookkeeping value.
#[tokio::test]
async fn a_publish_with_a_mismatched_sender_is_corrected_to_the_authenticated_identity() {
    let ca = TestCa::new();
    let client_identity = ca.issue();
    let authenticated_id =
        thoth_mesh_core::PeerId::from_fingerprint(fingerprint_of(&client_identity));
    let claimed_id = thoth_mesh_core::PeerId::new();
    assert_ne!(authenticated_id, claimed_id);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    thoth_mesh_node::spawn_with_tls(
        listener,
        Vec::new(),
        NodeOptions {
            tls: Some(ca.issue()),
            ..Default::default()
        },
    )
    .unwrap();

    let mut subscriber = connect_tls(addr, &client_identity).await;
    let sub = Envelope::new(
        thoth_mesh_core::PeerId::new(),
        MessageKind::Subscribe {
            filter: topic("weather.updates").into(),
        },
    );
    send(&mut subscriber, &sub).await;
    recv(&mut subscriber).await; // subscribe ack

    let mut publisher = connect_tls_as(addr, &client_identity, Some(&client_identity)).await;
    let publish = Envelope::new(
        claimed_id,
        MessageKind::Publish {
            topic: topic("weather.updates"),
            payload: b"sunny".to_vec(),
        },
    );
    send(&mut publisher, &publish).await;

    let delivered = recv(&mut subscriber).await;
    assert_eq!(delivered.sender, authenticated_id);
}

/// ADR-0039, the accept side: a raw connection claiming a `Hello`
/// sender that doesn't match its own certificate is registered in
/// membership under the authenticated identity, not the claim.
#[tokio::test]
async fn a_hello_with_a_mismatched_sender_is_corrected_before_being_recorded_in_membership() {
    let ca = TestCa::new();
    let peer_identity = ca.issue();
    let authenticated_id =
        thoth_mesh_core::PeerId::from_fingerprint(fingerprint_of(&peer_identity));
    let claimed_id = thoth_mesh_core::PeerId::new();
    assert_ne!(authenticated_id, claimed_id);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let node = thoth_mesh_node::spawn_with_tls(
        listener,
        Vec::new(),
        NodeOptions {
            tls: Some(ca.issue()),
            ..Default::default()
        },
    )
    .unwrap();

    let mut conn = connect_tls_as(addr, &peer_identity, Some(&peer_identity)).await;
    let hello = Envelope::new(claimed_id, MessageKind::Hello { listen_addr: None });
    send(&mut conn, &hello).await;
    recv(&mut conn).await; // the node's own Hello reply

    assert!(node.membership.is_reachable(authenticated_id));
    assert!(!node.membership.is_reachable(claimed_id));
}

/// ADR-0039, the dial side: a seed peer whose `Hello` reply claims a
/// `PeerId` that doesn't match its own certificate is registered
/// under the authenticated identity instead - the same correction
/// the accept side applies, via `admit_initial_peer` rather than
/// `handle_hello`.
#[tokio::test]
async fn dial_side_corrects_a_mismatched_seed_peer_hello_to_the_authenticated_identity() {
    let ca = TestCa::new();
    let raw_peer_identity = ca.issue();
    let authenticated_id =
        thoth_mesh_core::PeerId::from_fingerprint(fingerprint_of(&raw_peer_identity));
    let claimed_id = thoth_mesh_core::PeerId::new();
    assert_ne!(authenticated_id, claimed_id);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let raw_peer_addr = listener.local_addr().unwrap();

    let dialer = thoth_mesh_node::spawn_with_tls(
        TcpListener::bind("127.0.0.1:0").await.unwrap(),
        vec![raw_peer_addr.to_string()],
        NodeOptions {
            tls: Some(ca.issue()),
            ..Default::default()
        },
    )
    .unwrap();

    let mut conn = accept_tls(&listener, &raw_peer_identity).await;
    let their_hello = recv(&mut conn).await;
    assert!(matches!(their_hello.kind, MessageKind::Hello { .. }));

    let our_hello = Envelope::new(claimed_id, MessageKind::Hello { listen_addr: None });
    send(&mut conn, &our_hello).await;

    eventually(|| dialer.membership.is_reachable(authenticated_id)).await;
    assert!(!dialer.membership.is_reachable(claimed_id));
}

/// ADR-0040 (closing #122): a real peer and a simultaneously-connected
/// impersonation attempt - its own valid certificate, but a `Hello`
/// claiming the real peer's identity - can never collide on one
/// `Membership`/`PeerLinks` entry. The "attacker" ends up registered
/// under its own distinct authenticated identity, alongside the real
/// peer's untouched one, and both keep working independently.
#[tokio::test]
async fn an_impersonation_attempt_never_collides_with_the_real_peers_membership_entry() {
    let ca = TestCa::new();
    let real_peer_identity = ca.issue();
    let real_peer_id =
        thoth_mesh_core::PeerId::from_fingerprint(fingerprint_of(&real_peer_identity));
    let attacker_identity = ca.issue();
    let attacker_id = thoth_mesh_core::PeerId::from_fingerprint(fingerprint_of(&attacker_identity));
    assert_ne!(real_peer_id, attacker_id);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let node = thoth_mesh_node::spawn_with_tls(
        listener,
        Vec::new(),
        NodeOptions {
            tls: Some(ca.issue()),
            ..Default::default()
        },
    )
    .unwrap();

    // The real peer connects and identifies itself honestly.
    let mut real_conn = connect_tls_as(addr, &real_peer_identity, Some(&real_peer_identity)).await;
    send(
        &mut real_conn,
        &Envelope::new(real_peer_id, MessageKind::Hello { listen_addr: None }),
    )
    .await;
    recv(&mut real_conn).await; // this node's Hello reply

    // The attacker connects with its own certificate, but claims to
    // be the real peer.
    let mut attacker_conn =
        connect_tls_as(addr, &attacker_identity, Some(&attacker_identity)).await;
    send(
        &mut attacker_conn,
        &Envelope::new(real_peer_id, MessageKind::Hello { listen_addr: None }),
    )
    .await;
    recv(&mut attacker_conn).await; // this node's Hello reply

    // Two distinct entries, not one clobbering the other.
    assert!(node.membership.is_reachable(real_peer_id));
    assert!(node.membership.is_reachable(attacker_id));

    // Both connections are still independently live and correctly
    // routed - a status round trip on each (not Subscribe/Publish:
    // both connections are peer links now, and a peer link's own
    // Subscribe also echoes back to itself via interest propagation,
    // ADR-0011 - a second reply this check doesn't care about and
    // would just complicate accounting for).
    for conn in [&mut real_conn, &mut attacker_conn] {
        let request = Envelope::new(thoth_mesh_core::PeerId::new(), MessageKind::StatusRequest);
        send(conn, &request).await;
        match recv(conn).await.kind {
            MessageKind::StatusReply { in_reply_to, .. } => assert_eq!(in_reply_to, request.id),
            other => panic!("expected a StatusReply, got {other:?}"),
        }
    }
}
