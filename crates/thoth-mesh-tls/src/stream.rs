use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};

use rustls_pki_types::{CertificateDer, ServerName};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::{TlsAcceptor, TlsConnector, client, server};

/// A `TcpStream` that may or may not have TLS layered over it. Every
/// call site that used to hold a plain `TcpStream` holds one of these
/// instead — plaintext and TLS connections are indistinguishable past
/// this point, which is what lets TLS be an additive change to
/// `thoth-mesh-node`/`thoth-mesh-cli` rather than a rewrite (see
/// ADR-0016).
pub enum MaybeTlsStream {
    Plain(TcpStream),
    TlsServer(Box<server::TlsStream<TcpStream>>),
    TlsClient(Box<client::TlsStream<TcpStream>>),
}

impl MaybeTlsStream {
    /// Completes a TLS handshake as the accepting side of `stream`,
    /// using `acceptor`'s config (see [`crate::server_config`]).
    pub async fn accept(acceptor: &TlsAcceptor, stream: TcpStream) -> io::Result<Self> {
        acceptor
            .accept(stream)
            .await
            .map(|s| Self::TlsServer(Box::new(s)))
    }

    /// Connects to `addr` (a `host:port` string, the same one already
    /// used to `TcpStream::connect`) and completes a TLS handshake as
    /// the dialing side, using `connector`'s config (see
    /// [`crate::client_config`]). The host portion of `addr` is used
    /// as the TLS server name for certificate verification.
    pub async fn connect(
        connector: &TlsConnector,
        stream: TcpStream,
        addr: &str,
    ) -> Result<Self, TlsConnectError> {
        let name = server_name_from_addr(addr)?;
        let tls = connector.connect(name, stream).await?;
        Ok(Self::TlsClient(Box::new(tls)))
    }

    /// The peer certificate chain this connection authenticated with,
    /// if any — `None` for a plaintext connection, or a TLS
    /// connection whose far end didn't present a certificate (allowed
    /// on the accept side, see [`crate::server_config`]). Not used by
    /// anything in this crate; exposed for #47 (peer
    /// authentication/allowlisting) to build on.
    pub fn peer_certificates(&self) -> Option<&[CertificateDer<'static>]> {
        match self {
            Self::Plain(_) => None,
            Self::TlsServer(s) => s.get_ref().1.peer_certificates(),
            Self::TlsClient(s) => s.get_ref().1.peer_certificates(),
        }
    }

    /// The underlying TCP connection's remote address, whether or not
    /// TLS is layered over it.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        match self {
            Self::Plain(s) => s.peer_addr(),
            Self::TlsServer(s) => s.get_ref().0.peer_addr(),
            Self::TlsClient(s) => s.get_ref().0.peer_addr(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TlsConnectError {
    #[error("{0:?} is not a valid TLS server name")]
    InvalidServerName(String),
    #[error("TLS handshake failed: {0}")]
    Handshake(#[from] io::Error),
}

fn server_name_from_addr(addr: &str) -> Result<ServerName<'static>, TlsConnectError> {
    let host = addr
        .rsplit_once(':')
        .map(|(host, _port)| host)
        .unwrap_or(addr);
    ServerName::try_from(host.to_string())
        .map_err(|_| TlsConnectError::InvalidServerName(host.to_string()))
}

impl AsyncRead for MaybeTlsStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_read(cx, buf),
            Self::TlsServer(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
            Self::TlsClient(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for MaybeTlsStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_write(cx, buf),
            Self::TlsServer(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
            Self::TlsClient(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_flush(cx),
            Self::TlsServer(s) => Pin::new(s.as_mut()).poll_flush(cx),
            Self::TlsClient(s) => Pin::new(s.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_shutdown(cx),
            Self::TlsServer(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
            Self::TlsClient(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
        }
    }
}
