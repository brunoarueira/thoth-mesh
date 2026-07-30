//! Async equivalent of `thoth_mesh_core::framing`.
//!
//! `thoth-mesh-core` deliberately keeps its framing helpers sync-only
//! (ADR-0002/0005), so this small async version lives here instead. See
//! ADR-0007. The error type is reused as-is from `thoth-mesh-core` -
//! both sync and async I/O in Rust/tokio report errors as
//! `std::io::Error`, so only the read/write control flow needs
//! duplicating, not the error type.

use thoth_mesh_core::{FramingError, MAX_FRAME_LEN};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Writes `payload` as a single length-prefixed frame: a 4-byte
/// big-endian length prefix followed by `payload` itself.
pub async fn write_frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    payload: &[u8],
) -> Result<(), FramingError> {
    let len = u32::try_from(payload.len()).map_err(|_| FramingError::FrameTooLarge {
        len: u32::MAX,
        max: MAX_FRAME_LEN,
    })?;
    if len > MAX_FRAME_LEN {
        return Err(FramingError::FrameTooLarge {
            len,
            max: MAX_FRAME_LEN,
        });
    }
    writer.write_all(&len.to_be_bytes()).await?;
    writer.write_all(payload).await?;
    Ok(())
}

/// Reads a single length-prefixed frame written by [`write_frame`],
/// returning its payload.
pub async fn read_frame<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Vec<u8>, FramingError> {
    let mut len_bytes = [0u8; 4];
    reader.read_exact(&mut len_bytes).await?;
    let len = u32::from_be_bytes(len_bytes);
    if len > MAX_FRAME_LEN {
        return Err(FramingError::FrameTooLarge {
            len,
            max: MAX_FRAME_LEN,
        });
    }
    let mut payload = vec![0u8; len as usize];
    reader.read_exact(&mut payload).await?;
    Ok(payload)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn round_trips() {
        let payload = b"hello, thoth".to_vec();
        let mut buf = Vec::new();
        write_frame(&mut buf, &payload).await.unwrap();
        assert_eq!(buf.len(), 4 + payload.len());

        let mut cursor = &buf[..];
        let decoded = read_frame(&mut cursor).await.unwrap();
        assert_eq!(decoded, payload);
    }

    #[tokio::test]
    async fn write_frame_rejects_oversized_payload() {
        let payload = vec![0u8; (MAX_FRAME_LEN + 1) as usize];
        let mut buf = Vec::new();
        let err = write_frame(&mut buf, &payload).await.unwrap_err();
        assert!(matches!(err, FramingError::FrameTooLarge { .. }));
    }

    #[tokio::test]
    async fn read_frame_rejects_oversized_length_prefix() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(MAX_FRAME_LEN + 1).to_be_bytes());
        let mut cursor = &buf[..];
        let err = read_frame(&mut cursor).await.unwrap_err();
        assert!(matches!(err, FramingError::FrameTooLarge { .. }));
    }

    #[tokio::test]
    async fn read_frame_rejects_truncated_stream() {
        let mut buf = Vec::new();
        write_frame(&mut buf, b"hello").await.unwrap();
        buf.truncate(buf.len() - 1);
        let mut cursor = &buf[..];
        assert!(read_frame(&mut cursor).await.is_err());
    }
}
