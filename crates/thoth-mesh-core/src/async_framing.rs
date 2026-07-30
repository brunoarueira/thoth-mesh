//! Runtime-agnostic async equivalent of [`crate::framing`].
//!
//! Generic over `futures_util::io::{AsyncRead, AsyncWrite}` rather than a
//! specific runtime's I/O traits, so any executor can use it - tokio via
//! `tokio_util::compat`, or any other runtime that implements (or can be
//! bridged to) these traits. See ADR-0008.

use futures_util::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::error::FramingError;
use crate::framing::MAX_FRAME_LEN;

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
    use futures_executor::block_on;
    use futures_util::io::Cursor;

    #[test]
    fn round_trips() {
        block_on(async {
            let payload = b"hello, thoth".to_vec();
            let mut buf = Vec::new();
            write_frame(&mut buf, &payload).await.unwrap();
            assert_eq!(buf.len(), 4 + payload.len());

            let mut cursor = Cursor::new(buf);
            let decoded = read_frame(&mut cursor).await.unwrap();
            assert_eq!(decoded, payload);
        });
    }

    #[test]
    fn round_trips_empty_payload() {
        block_on(async {
            let mut buf = Vec::new();
            write_frame(&mut buf, &[]).await.unwrap();
            let mut cursor = Cursor::new(buf);
            assert_eq!(read_frame(&mut cursor).await.unwrap(), Vec::<u8>::new());
        });
    }

    #[test]
    fn write_frame_rejects_oversized_payload() {
        block_on(async {
            let payload = vec![0u8; (MAX_FRAME_LEN + 1) as usize];
            let mut buf = Vec::new();
            let err = write_frame(&mut buf, &payload).await.unwrap_err();
            assert!(matches!(err, FramingError::FrameTooLarge { .. }));
        });
    }

    #[test]
    fn read_frame_rejects_oversized_length_prefix() {
        block_on(async {
            let mut buf = Vec::new();
            buf.extend_from_slice(&(MAX_FRAME_LEN + 1).to_be_bytes());
            let mut cursor = Cursor::new(buf);
            let err = read_frame(&mut cursor).await.unwrap_err();
            assert!(matches!(err, FramingError::FrameTooLarge { .. }));
        });
    }

    #[test]
    fn read_frame_rejects_truncated_stream() {
        block_on(async {
            let mut buf = Vec::new();
            write_frame(&mut buf, b"hello").await.unwrap();
            buf.truncate(buf.len() - 1);
            let mut cursor = Cursor::new(buf);
            assert!(read_frame(&mut cursor).await.is_err());
        });
    }
}
