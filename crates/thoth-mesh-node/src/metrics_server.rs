//! A minimal HTTP responder for the port opened by `--metrics-addr`:
//! the Prometheus scrape render (see ADR-0013), plus `/livez`/
//! `/readyz` health checks (see ADR-0056). Deliberately not a general
//! HTTP implementation - this is a handful of routes, not a web
//! framework. The scrape render is optionally gated by a shared-
//! secret bearer token (see `--metrics-token-file` and ADR-0019);
//! `/livez`/`/readyz` never are (see ADR-0056).

use std::sync::Arc;

use thoth_mesh::{Membership, PeerDirectory};
use thoth_mesh_broker::Broker;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

use crate::health::Readiness;
use crate::metrics::{Metrics, render_prometheus};
use crate::rate_limit::RateLimiter;

/// Serves the current Prometheus render, plus `/livez`/`/readyz`, on
/// every connection accepted on `listener`, until an unrecoverable
/// listener error occurs. If `token` is `Some`, a request for the
/// render without a matching `Authorization: Bearer <token>` header
/// gets `401` instead of it - see ADR-0019; `/livez`/`/readyz` are
/// never gated by it - see ADR-0056.
///
/// Takes an already-bound listener rather than an address, same as
/// [`crate::serve`], so tests can bind an ephemeral port and read back
/// the actual bound address before connecting to it.
#[allow(clippy::too_many_arguments)]
pub async fn serve_metrics(
    listener: TcpListener,
    membership: Membership,
    broker: Arc<Broker>,
    discover: PeerDirectory,
    metrics: Metrics,
    rate_limiter: Option<Arc<RateLimiter>>,
    readiness: Readiness,
    token: Option<Arc<str>>,
) -> std::io::Result<()> {
    tracing::info!(addr = ?listener.local_addr().ok(), auth = token.is_some(), "metrics endpoint ready");
    loop {
        let (socket, _) = listener.accept().await?;
        let membership = membership.clone();
        let broker = Arc::clone(&broker);
        let discover = discover.clone();
        let metrics = metrics.clone();
        let rate_limiter = rate_limiter.clone();
        let readiness = readiness.clone();
        let token = token.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_request(
                socket,
                &membership,
                &broker,
                &discover,
                &metrics,
                rate_limiter.as_deref(),
                &readiness,
                token.as_deref(),
            )
            .await
            {
                tracing::debug!(%err, "metrics connection ended");
            }
        });
    }
}

/// Reads the request line (to route on its path - see ADR-0056) and
/// then the rest of the headers up to the blank line ending them,
/// capturing `Authorization` along the way - so a well-behaved client
/// sees a clean response rather than a reset connection - then
/// dispatches on the path: `/livez`/`/readyz` answer unconditionally
/// (never gated by `token`); anything else - including `/metrics` and
/// an unrecognized path alike, preserving every behavior from before
/// this ADR - serves the Prometheus render, `401` instead if `token`
/// is configured and the request didn't present it correctly.
#[allow(clippy::too_many_arguments)]
async fn handle_request(
    socket: TcpStream,
    membership: &Membership,
    broker: &Broker,
    discover: &PeerDirectory,
    metrics: &Metrics,
    rate_limiter: Option<&RateLimiter>,
    readiness: &Readiness,
    token: Option<&str>,
) -> std::io::Result<()> {
    let (reader, mut writer) = socket.into_split();
    let mut reader = BufReader::new(reader);

    let mut request_line = String::new();
    reader.read_line(&mut request_line).await?;
    // "GET /readyz?probe=1 HTTP/1.1" - the request-target is
    // whichever's in the middle, query string and all; strip it
    // before matching so a probe that appends one (not unusual for
    // cache-busting) still reaches /livez or /readyz rather than
    // falling through to the render below. A blank/malformed/missing
    // request line (nothing sent, or a client that doesn't speak HTTP
    // at all) has no second field and falls through to that same
    // default, render-serving path, same as any other unrecognized
    // one.
    let target = request_line.split_whitespace().nth(1).unwrap_or("");
    let path = target.split_once('?').map_or(target, |(path, _)| path);

    let mut line = String::new();
    let mut authorization: Option<String> = None;
    loop {
        line.clear();
        let bytes_read = reader.read_line(&mut line).await?;
        if bytes_read == 0 || line == "\r\n" || line == "\n" {
            break;
        }
        if let Some((name, value)) = line.split_once(':')
            && name.eq_ignore_ascii_case("authorization")
        {
            authorization = Some(value.trim().to_string());
        }
    }

    match path {
        "/livez" => return write_response(&mut writer, 200, "OK", "", "ok\n").await,
        "/readyz" => {
            return if readiness.is_ready() {
                write_response(&mut writer, 200, "OK", "", "ready\n").await
            } else {
                write_response(&mut writer, 503, "Service Unavailable", "", "not ready\n").await
            };
        }
        _ => {}
    }

    if let Some(expected) = token {
        let presented = authorization
            .as_deref()
            .and_then(|value| value.strip_prefix("Bearer "));
        let authorized = presented
            .is_some_and(|presented| constant_time_eq(presented.as_bytes(), expected.as_bytes()));
        if !authorized {
            metrics.record_metrics_auth_rejection();
            return write_response(
                &mut writer,
                401,
                "Unauthorized",
                "WWW-Authenticate: Bearer\r\n",
                "unauthorized\n",
            )
            .await;
        }
    }

    let body = render_prometheus(membership, broker, discover, metrics, rate_limiter);
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

/// Writes a plain-text HTTP response - `status`/`reason` as the
/// status line (e.g. `200`/`"OK"`), `extra_headers` already including
/// its own trailing `\r\n` per header (empty string for none), `body`
/// as the content - then shuts the connection down, same as every
/// other response this module sends (`Connection: close` throughout:
/// a metrics scrape or a health probe is a one-shot request, not a
/// kept-alive session).
async fn write_response(
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    status: u16,
    reason: &str,
    extra_headers: &str,
    body: &str,
) -> std::io::Result<()> {
    let response = format!(
        "HTTP/1.1 {status} {reason}\r\n\
         Content-Type: text/plain\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         {extra_headers}\
         \r\n\
         {body}",
        body.len(),
    );
    writer.write_all(response.as_bytes()).await?;
    writer.shutdown().await
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

        tokio::spawn(serve_metrics(
            listener,
            membership,
            broker,
            PeerDirectory::new(),
            metrics,
            None,
            Readiness::new(),
            None,
        ));

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
            PeerDirectory::new(),
            Metrics::new(),
            None,
            Readiness::new(),
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
            PeerDirectory::new(),
            Metrics::new(),
            None,
            Readiness::new(),
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
            PeerDirectory::new(),
            metrics.clone(),
            None,
            Readiness::new(),
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
            PeerDirectory::new(),
            metrics.clone(),
            None,
            Readiness::new(),
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
            PeerDirectory::new(),
            Metrics::new(),
            None,
            Readiness::new(),
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

    #[tokio::test]
    async fn livez_always_answers_200_regardless_of_readiness() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        // Deliberately never marked ready - livez doesn't care.
        tokio::spawn(serve_metrics(
            listener,
            Membership::new(),
            Arc::new(Broker::new()),
            PeerDirectory::new(),
            Metrics::new(),
            None,
            Readiness::new(),
            None,
        ));

        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"GET /livez HTTP/1.1\r\n\r\n")
            .await
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.ends_with("ok\n"));
    }

    #[tokio::test]
    async fn readyz_answers_503_before_ready_and_200_after() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let readiness = Readiness::new();
        tokio::spawn(serve_metrics(
            listener,
            Membership::new(),
            Arc::new(Broker::new()),
            PeerDirectory::new(),
            Metrics::new(),
            None,
            readiness.clone(),
            None,
        ));

        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"GET /readyz HTTP/1.1\r\n\r\n")
            .await
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        assert!(response.starts_with("HTTP/1.1 503 Service Unavailable"));
        assert!(response.ends_with("not ready\n"));

        readiness.mark_ready();

        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"GET /readyz HTTP/1.1\r\n\r\n")
            .await
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.ends_with("ready\n"));
    }

    #[tokio::test]
    async fn readyz_still_matches_with_a_query_string_appended() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let readiness = Readiness::new();
        readiness.mark_ready();
        tokio::spawn(serve_metrics(
            listener,
            Membership::new(),
            Arc::new(Broker::new()),
            PeerDirectory::new(),
            Metrics::new(),
            None,
            readiness,
            None,
        ));

        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"GET /readyz?probe=1 HTTP/1.1\r\n\r\n")
            .await
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.ends_with("ready\n"));
    }

    #[tokio::test]
    async fn livez_and_readyz_ignore_a_configured_metrics_token() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let readiness = Readiness::new();
        readiness.mark_ready();
        tokio::spawn(serve_metrics(
            listener,
            Membership::new(),
            Arc::new(Broker::new()),
            PeerDirectory::new(),
            Metrics::new(),
            None,
            readiness,
            Some(Arc::from("secret-token")),
        ));

        for path in ["/livez", "/readyz"] {
            let mut stream = TcpStream::connect(addr).await.unwrap();
            stream
                .write_all(format!("GET {path} HTTP/1.1\r\n\r\n").as_bytes())
                .await
                .unwrap();
            let mut response = String::new();
            stream.read_to_string(&mut response).await.unwrap();
            assert!(
                response.starts_with("HTTP/1.1 200 OK"),
                "{path} should answer without the configured token, got: {response}"
            );
        }
    }

    #[tokio::test]
    async fn every_other_path_still_serves_the_metrics_render() {
        // Preserves the pre-ADR-0056 behavior: any path other than
        // the two new health routes gets the Prometheus render, not
        // just /metrics itself.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve_metrics(
            listener,
            Membership::new(),
            Arc::new(Broker::new()),
            PeerDirectory::new(),
            Metrics::new(),
            None,
            Readiness::new(),
            None,
        ));

        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"GET /whatever HTTP/1.1\r\n\r\n")
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
