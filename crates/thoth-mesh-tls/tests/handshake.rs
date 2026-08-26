//! End-to-end TLS handshake tests over real loopback sockets, using a
//! throwaway CA + leaf certs generated per test run (never checked
//! in - see ADR-0016's rationale for keeping cert generation out of
//! this crate itself; tests are the one place generating certs on
//! the fly is the right call).

use std::sync::Arc;

use rcgen::{CertificateParams, KeyPair};
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use thoth_mesh_tls::{MaybeTlsStream, TlsAcceptor, TlsConnector, client_config, server_config};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

struct Identity {
    cert: CertificateDer<'static>,
    key: PrivateKeyDer<'static>,
}

/// A throwaway CA, able to issue leaf certs signed by itself.
struct TestCa {
    cert: rcgen::Certificate,
    issuer: rcgen::Issuer<'static, KeyPair>,
}

impl TestCa {
    fn new() -> Self {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(Vec::new()).unwrap();
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let cert = params.self_signed(&key).unwrap();
        let issuer = rcgen::Issuer::new(params, key);
        Self { cert, issuer }
    }

    fn der(&self) -> CertificateDer<'static> {
        self.cert.der().clone()
    }

    /// Issues a leaf cert signed by this CA, for `name` (used as the
    /// cert's subject alt name so it verifies against `127.0.0.1`).
    fn issue(&self, name: &str) -> Identity {
        let leaf_key = KeyPair::generate().unwrap();
        let leaf_params = CertificateParams::new(vec![name.to_string()]).unwrap();
        let leaf_cert = leaf_params.signed_by(&leaf_key, &self.issuer).unwrap();
        Identity {
            cert: leaf_cert.der().clone(),
            key: PrivateKeyDer::Pkcs8(leaf_key.serialize_der().into()),
        }
    }
}

#[tokio::test]
async fn plaintext_stream_round_trips() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut stream = MaybeTlsStream::Plain(socket);
        let mut buf = [0u8; 5];
        stream.read_exact(&mut buf).await.unwrap();
        stream.write_all(&buf).await.unwrap();
    });

    let socket = tokio::net::TcpStream::connect(addr).await.unwrap();
    let mut stream = MaybeTlsStream::Plain(socket);
    stream.write_all(b"hello").await.unwrap();
    let mut buf = [0u8; 5];
    stream.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"hello");

    server.await.unwrap();
}

#[tokio::test]
async fn client_without_a_cert_still_connects_and_round_trips() {
    let ca = TestCa::new();
    let server_leaf = ca.issue("127.0.0.1");
    let server_cfg =
        server_config(vec![server_leaf.cert], server_leaf.key, vec![ca.der()]).unwrap();
    let client_cfg = client_config(vec![ca.der()], None).unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let acceptor = TlsAcceptor::from(Arc::new(server_cfg));
    let connector = TlsConnector::from(Arc::new(client_cfg));

    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut stream = MaybeTlsStream::accept(&acceptor, socket).await.unwrap();
        assert!(
            stream.peer_certificates().is_none(),
            "client presented no cert, so the server shouldn't see one"
        );
        let mut buf = [0u8; 5];
        stream.read_exact(&mut buf).await.unwrap();
        stream.write_all(&buf).await.unwrap();
    });

    let socket = tokio::net::TcpStream::connect(addr).await.unwrap();
    let mut stream = MaybeTlsStream::connect(&connector, socket, &addr.to_string())
        .await
        .unwrap();
    stream.write_all(b"hello").await.unwrap();
    let mut buf = [0u8; 5];
    stream.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"hello");

    server.await.unwrap();
}

#[tokio::test]
async fn client_with_a_cert_is_seen_as_authenticated_by_the_server() {
    let ca = TestCa::new();
    let server_leaf = ca.issue("127.0.0.1");
    let client_leaf = ca.issue("client");

    let server_cfg =
        server_config(vec![server_leaf.cert], server_leaf.key, vec![ca.der()]).unwrap();
    let client_cfg = client_config(
        vec![ca.der()],
        Some((vec![client_leaf.cert], client_leaf.key)),
    )
    .unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let acceptor = TlsAcceptor::from(Arc::new(server_cfg));
    let connector = TlsConnector::from(Arc::new(client_cfg));

    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let stream = MaybeTlsStream::accept(&acceptor, socket).await.unwrap();
        assert!(
            stream.peer_certificates().is_some(),
            "client presented a CA-signed cert, so the server should see it"
        );
    });

    let socket = tokio::net::TcpStream::connect(addr).await.unwrap();
    let _stream = MaybeTlsStream::connect(&connector, socket, &addr.to_string())
        .await
        .unwrap();

    server.await.unwrap();
}

#[tokio::test]
async fn a_cert_from_an_untrusted_ca_is_rejected() {
    let ca = TestCa::new();
    let other_ca = TestCa::new(); // unrelated CA
    let server_leaf = ca.issue("127.0.0.1");

    let server_cfg =
        server_config(vec![server_leaf.cert], server_leaf.key, vec![ca.der()]).unwrap();
    // Client trusts a *different* CA than the one the server's cert
    // is signed by - the handshake should fail rather than connect.
    let client_cfg = client_config(vec![other_ca.der()], None).unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let acceptor = TlsAcceptor::from(Arc::new(server_cfg));
    let connector = TlsConnector::from(Arc::new(client_cfg));

    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        // The handshake should fail server-side too, not hang.
        let _ = MaybeTlsStream::accept(&acceptor, socket).await;
    });

    let socket = tokio::net::TcpStream::connect(addr).await.unwrap();
    let result = MaybeTlsStream::connect(&connector, socket, &addr.to_string()).await;
    assert!(
        result.is_err(),
        "connecting with a cert chain the client doesn't trust should fail"
    );

    server.await.unwrap();
}
