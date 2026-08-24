//! The peer handshake: the first exchange on any node-to-node
//! connection, identifying it as a peer link rather than a client
//! connection. See ADR-0009.

use futures_util::io::{AsyncRead, AsyncWrite};
use thoth_mesh_core::async_framing;
use thoth_mesh_core::{
    DecodeError, EncodeError, Envelope, FramingError, MessageId, MessageKind, PeerId,
};

/// A peer's self-reported identity, learned from its `Hello`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerInfo {
    /// The peer's identity, taken from the `Hello` envelope's sender.
    pub peer_id: PeerId,
    /// The address other peers should dial to reach this peer, if it
    /// reported one.
    pub listen_addr: Option<String>,
    /// The `Hello` envelope's own id - what an allowlist rejection
    /// (ADR-0017) references via `Error { in_reply_to, .. }` if this
    /// peer turns out not to be allowed.
    pub hello_id: MessageId,
}

/// The handshake failed.
#[derive(Debug, thiserror::Error)]
pub enum HandshakeError {
    #[error("i/o error during handshake: {0}")]
    Framing(#[from] FramingError),
    #[error("failed to decode handshake envelope: {0}")]
    Decode(#[from] DecodeError),
    #[error("failed to encode handshake envelope: {0}")]
    Encode(#[from] EncodeError),
    #[error("expected a Hello, got {0:?}")]
    UnexpectedMessage(MessageKind),
}

/// Builds this node's `Hello` envelope.
pub fn hello(sender: PeerId, listen_addr: Option<String>) -> Envelope {
    Envelope::new(sender, MessageKind::Hello { listen_addr })
}

/// Sends `envelope` as a single framed message.
async fn send<S: AsyncWrite + Unpin>(
    stream: &mut S,
    envelope: &Envelope,
) -> Result<(), HandshakeError> {
    let bytes = envelope.to_bytes()?;
    async_framing::write_frame(stream, &bytes).await?;
    Ok(())
}

/// Reads the next framed message and requires it to be a `Hello`,
/// returning the sender's [`PeerInfo`].
async fn recv_hello<S: AsyncRead + Unpin>(stream: &mut S) -> Result<PeerInfo, HandshakeError> {
    let bytes = async_framing::read_frame(stream).await?;
    let envelope = Envelope::from_bytes(&bytes)?;
    match envelope.kind {
        MessageKind::Hello { listen_addr } => Ok(PeerInfo {
            peer_id: envelope.sender,
            listen_addr,
            hello_id: envelope.id,
        }),
        other => Err(HandshakeError::UnexpectedMessage(other)),
    }
}

/// Performs the dialing side of the peer handshake: sends our own
/// `Hello` first, then waits for the peer's `Hello` back.
///
/// The accepting side doesn't need this function - it already reads
/// framed envelopes in its normal dispatch loop, and replies with its
/// own [`hello`] envelope when it sees an inbound `Hello` (see
/// `thoth-mesh-node::connection`).
pub async fn dial_handshake<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    my_id: PeerId,
    my_listen_addr: Option<String>,
) -> Result<PeerInfo, HandshakeError> {
    send(stream, &hello(my_id, my_listen_addr)).await?;
    recv_hello(stream).await
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use super::*;
    use futures_executor::block_on;
    use futures_util::io::Cursor;

    /// A minimal in-memory duplex stream: reads come from a preloaded
    /// buffer, writes accumulate separately - unlike `Cursor`, whose
    /// read and write halves share one position over one buffer and
    /// so can't stand in for two independent directions at once.
    struct MockStream {
        read: Cursor<Vec<u8>>,
        written: Vec<u8>,
    }

    impl AsyncRead for MockStream {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut [u8],
        ) -> Poll<io::Result<usize>> {
            Pin::new(&mut self.read).poll_read(cx, buf)
        }
    }

    impl AsyncWrite for MockStream {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.written.extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[test]
    fn dial_handshake_sends_our_hello_and_parses_the_reply() {
        block_on(async {
            let my_id = PeerId::new();
            let their_id = PeerId::new();

            let their_hello = hello(their_id, Some("127.0.0.1:49501".to_owned()));
            let mut preloaded = Vec::new();
            async_framing::write_frame(&mut preloaded, &their_hello.to_bytes().unwrap())
                .await
                .unwrap();
            let mut stream = MockStream {
                read: Cursor::new(preloaded),
                written: Vec::new(),
            };

            let info = dial_handshake(&mut stream, my_id, Some("127.0.0.1:49500".to_owned()))
                .await
                .unwrap();
            assert_eq!(info.peer_id, their_id);
            assert_eq!(info.listen_addr, Some("127.0.0.1:49501".to_owned()));
            assert_eq!(info.hello_id, their_hello.id);

            // And it actually sent our own Hello, not just consumed
            // the reply.
            let mut sent = Cursor::new(stream.written);
            let sent_bytes = async_framing::read_frame(&mut sent).await.unwrap();
            let sent_envelope = Envelope::from_bytes(&sent_bytes).unwrap();
            assert_eq!(sent_envelope.sender, my_id);
            assert_eq!(
                sent_envelope.kind,
                MessageKind::Hello {
                    listen_addr: Some("127.0.0.1:49500".to_owned())
                }
            );
        });
    }

    #[test]
    fn dial_handshake_rejects_a_non_hello_reply() {
        block_on(async {
            let envelope = Envelope::new(
                PeerId::new(),
                MessageKind::Subscribe {
                    topic: "weather.updates".parse().unwrap(),
                },
            );
            let mut preloaded = Vec::new();
            async_framing::write_frame(&mut preloaded, &envelope.to_bytes().unwrap())
                .await
                .unwrap();
            let mut stream = MockStream {
                read: Cursor::new(preloaded),
                written: Vec::new(),
            };

            let err = dial_handshake(&mut stream, PeerId::new(), None)
                .await
                .unwrap_err();
            assert!(matches!(err, HandshakeError::UnexpectedMessage(_)));
        });
    }
}
