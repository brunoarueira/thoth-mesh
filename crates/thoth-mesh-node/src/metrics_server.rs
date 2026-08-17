//! A minimal, single-endpoint HTTP responder for the Prometheus
//! scrape port opened by `--metrics-addr`. See ADR-0013 - this is
//! deliberately not a general HTTP implementation: a metrics scrape
//! endpoint has exactly one thing to serve, at any path, for any
//! method.

use std::sync::Arc;

use thoth_mesh::Membership;
use thoth_mesh_broker::Broker;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

use crate::metrics::{Metrics, render_prometheus};

/// Serves the current Prometheus render on every connection accepted
/// on `listener`, until an unrecoverable listener error occurs.
///
/// Takes an already-bound listener rather than an address, same as
/// [`crate::serve`], so tests can bind an ephemeral port and read back
/// the actual bound address before connecting to it.
pub async fn serve_metrics(
    listener: TcpListener,
    membership: Membership,
    broker: Arc<Broker>,
    metrics: Metrics,
) -> std::io::Result<()> {
    tracing::info!(addr = ?listener.local_addr().ok(), "metrics endpoint ready");
    loop {
        let (socket, _) = listener.accept().await?;
        let membership = membership.clone();
        let broker = Arc::clone(&broker);
        let metrics = metrics.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_scrape(socket, &membership, &broker, &metrics).await {
                tracing::debug!(%err, "metrics connection ended");
            }
        });
    }
}

/// Reads and discards the request up to the blank line ending its
/// headers - so a well-behaved scraper sees a clean response rather
/// than a reset connection - then always writes back `200 OK` with
/// the current render as the body, regardless of what was asked for.
async fn handle_scrape(
    socket: TcpStream,
    membership: &Membership,
    broker: &Broker,
    metrics: &Metrics,
) -> std::io::Result<()> {
    let (reader, mut writer) = socket.into_split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();
    loop {
        line.clear();
        let bytes_read = reader.read_line(&mut line).await?;
        if bytes_read == 0 || line == "\r\n" || line == "\n" {
            break;
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

        tokio::spawn(serve_metrics(listener, membership, broker, metrics));

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
        ));

        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(b"GET / HTTP/1.0\r\n\r\n").await.unwrap();
        stream.shutdown().await.unwrap();

        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        assert!(response.starts_with("HTTP/1.1 200 OK"));
    }
}
