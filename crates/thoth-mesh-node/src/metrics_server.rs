//! A minimal, single-endpoint HTTP responder for the Prometheus
//! scrape port opened by `--metrics-addr`. See ADR-0013 - this is
//! deliberately not a general HTTP implementation: a metrics scrape
//! endpoint has exactly one thing to serve, at any path, for any
//! method. Optionally gated by a shared-secret bearer token (see
//! `--metrics-token-file` and ADR-0019).

use std::sync::Arc;

use thoth_mesh::Membership;
use thoth_mesh_broker::Broker;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

use crate::metrics::{Metrics, render_prometheus};

/// Serves the current Prometheus render on every connection accepted
/// on `listener`, until an unrecoverable listener error occurs. If
/// `token` is `Some`, a request without a matching `Authorization:
/// Bearer <token>` header gets `401` instead of the render - see
/// ADR-0019.
///
/// Takes an already-bound listener rather than an address, same as
/// [`crate::serve`], so tests can bind an ephemeral port and read back
/// the actual bound address before connecting to it.
pub async fn serve_metrics(
    listener: TcpListener,
    membership: Membership,
    broker: Arc<Broker>,
    metrics: Metrics,
    token: Option<Arc<str>>,
) -> std::io::Result<()> {
    tracing::info!(addr = ?listener.local_addr().ok(), auth = token.is_some(), "metrics endpoint ready");
    loop {
        let (socket, _) = listener.accept().await?;
        let membership = membership.clone();
        let broker = Arc::clone(&broker);
        let metrics = metrics.clone();
        let token = token.clone();
        tokio::spawn(async move {
            if let Err(err) =
                handle_scrape(socket, &membership, &broker, &metrics, token.as_deref()).await
            {
                tracing::debug!(%err, "metrics connection ended");
            }
        });
    }
}

/// Reads the request up to the blank line ending its headers,
/// capturing `Authorization` along the way - so a well-behaved
/// scraper sees a clean response rather than a reset connection -
/// then, if `token` is configured and the request didn't present it
/// correctly, writes back `401` instead of the render.
async fn handle_scrape(
    socket: TcpStream,
    membership: &Membership,
    broker: &Broker,
    metrics: &Metrics,
    token: Option<&str>,
) -> std::io::Result<()> {
    let (reader, mut writer) = socket.into_split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();
    let mut authorization: Option<String> = None;
    loop {
        line.clear();
        let bytes_read = reader.read_line(&mut line).await?;
        if bytes_read == 0 || line == "\r\n" || line == "\n" {
            break;
        }
        if let Some((name, value)) = line.split_once(':') {
            if name.eq_ignore_ascii_case("authorization") {
                authorization = Some(value.trim().to_string());
            }
        }
    }

    if let Some(expected) = token {
        let presented = authorization
            .as_deref()
            .and_then(|value| value.strip_prefix("Bearer "));
        let authorized = presented
            .is_some_and(|presented| constant_time_eq(presented.as_bytes(), expected.as_bytes()));
        if !authorized {
            metrics.record_metrics_auth_rejection();
            let body = "unauthorized\n";
            let response = format!(
                "HTTP/1.1 401 Unauthorized\r\n\
                 WWW-Authenticate: Bearer\r\n\
                 Content-Type: text/plain\r\n\
                 Content-Length: {}\r\n\
                 Connection: close\r\n\
                 \r\n\
                 {}",
                body.len(),
                body
            );
            writer.write_all(response.as_bytes()).await?;
            writer.shutdown().await?;
            return Ok(());
        }
    }

    let body = render_prometheus(membership, broker, metrics);
    let response = format!(
        "HTTP/1.1 200 OK\r\n\
         Content-Type: text/plain; version=0.0.4\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n\
         {}",
        body.len(),
        body
    );
    writer.write_all(response.as_bytes()).await?;
    writer.shutdown().await?;
    Ok(())
}

/// Compares `a` and `b` in time that depends only on their lengths,
/// not their content - so presenting the wrong bearer token can't
/// leak how many leading bytes were right via response timing. Not a
/// claim this endpoint faces a serious threat model, just cheap to do
/// right (see ADR-0019).
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b) {
        diff |= x ^ y;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn a_scrape_gets_a_200_with_the_current_render() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let membership = Membership::new();
        membership.mark_connected(thoth_mesh_core::PeerId::new(), None);
        let broker = Arc::new(Broker::new());
        let metrics = Metrics::new();
        metrics.record_forwarder_lag(2);

        tokio::spawn(serve_metrics(listener, membership, broker, metrics, None));

        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();

        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();

        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.contains("thothmesh_peers_connected 1"));
        assert!(response.contains("thothmesh_messages_published_total 0"));
        assert!(response.contains("thothmesh_forwarder_lag_total 2"));
    }

    #[tokio::test]
    async fn a_request_with_no_body_and_no_trailing_blank_line_still_gets_a_response() {
        // A client that sends a request with no headers at all (just
        // the request line, then closes its write side) still needs
        // to see a clean response rather than a reset connection.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve_metrics(
            listener,
            Membership::new(),
            Arc::new(Broker::new()),
            Metrics::new(),
            None,
        ));

        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(b"GET / HTTP/1.0\r\n\r\n").await.unwrap();
        stream.shutdown().await.unwrap();

        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        assert!(response.starts_with("HTTP/1.1 200 OK"));
    }

    #[tokio::test]
    async fn a_scrape_with_no_token_configured_ignores_any_authorization_header() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve_metrics(
            listener,
            Membership::new(),
            Arc::new(Broker::new()),
            Metrics::new(),
            None,
        ));

        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nAuthorization: Bearer wrong\r\n\r\n")
            .await
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        assert!(response.starts_with("HTTP/1.1 200 OK"));
    }

    #[tokio::test]
    async fn a_scrape_with_a_configured_token_and_no_header_is_rejected() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let metrics = Metrics::new();
        tokio::spawn(serve_metrics(
            listener,
            Membership::new(),
            Arc::new(Broker::new()),
            metrics.clone(),
            Some(Arc::from("secret-token")),
        ));

        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\n\r\n")
            .await
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();

        assert!(response.starts_with("HTTP/1.1 401 Unauthorized"));
        assert!(response.contains("WWW-Authenticate: Bearer"));
        assert!(!response.contains("thothmesh_peers_connected"));
    }

    #[tokio::test]
    async fn a_scrape_with_the_wrong_token_is_rejected_and_counted() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let metrics = Metrics::new();
        tokio::spawn(serve_metrics(
            listener,
            Membership::new(),
            Arc::new(Broker::new()),
            metrics.clone(),
            Some(Arc::from("secret-token")),
        ));

        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nAuthorization: Bearer nope\r\n\r\n")
            .await
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        assert!(response.starts_with("HTTP/1.1 401 Unauthorized"));

        // A second, correctly-authorized scrape sees the rejection
        // reflected in the render.
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nAuthorization: Bearer secret-token\r\n\r\n")
            .await
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        assert!(response.contains("thothmesh_metrics_auth_rejections_total 1"));
    }

    #[tokio::test]
    async fn a_scrape_with_the_correct_token_gets_the_render() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve_metrics(
            listener,
            Membership::new(),
            Arc::new(Broker::new()),
            Metrics::new(),
            Some(Arc::from("secret-token")),
        ));

        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nAuthorization: Bearer secret-token\r\n\r\n")
            .await
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();

        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.contains("thothmesh_peers_connected"));
    }

    #[test]
    fn constant_time_eq_matches_normal_equality() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"ab"));
        assert!(!constant_time_eq(b"", b"a"));
        assert!(constant_time_eq(b"", b""));
    }
}
