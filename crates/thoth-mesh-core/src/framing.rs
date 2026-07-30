use std::io::{Read, Write};

use crate::error::FramingError;

/// Maximum allowed frame length, in bytes.
///
/// Bounds allocation from a corrupt or hostile length prefix. 16 MiB is
/// generous for a pub/sub payload while still being a hard ceiling.
pub const MAX_FRAME_LEN: u32 = 16 * 1024 * 1024;

/// Writes `payload` as a single length-prefixed frame: a 4-byte
/// big-endian length prefix followed by `payload` itself.
pub fn write_frame<W: Write>(writer: &mut W, payload: &[u8]) -> Result<(), FramingError> {
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
    writer.write_all(&len.to_be_bytes())?;
    writer.write_all(payload)?;
    Ok(())
}

/// Reads a single length-prefixed frame written by [`write_frame`],
/// returning its payload.
pub fn read_frame<R: Read>(reader: &mut R) -> Result<Vec<u8>, FramingError> {
    let mut len_bytes = [0u8; 4];
    reader.read_exact(&mut len_bytes)?;
    let len = u32::from_be_bytes(len_bytes);
    if len > MAX_FRAME_LEN {
        return Err(FramingError::FrameTooLarge {
            len,
            max: MAX_FRAME_LEN,
        });
    }
    let mut payload = vec![0u8; len as usize];
    reader.read_exact(&mut payload)?;
    Ok(payload)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips() {
        let payload = b"hello, thoth".to_vec();
        let mut buf = Vec::new();
        write_frame(&mut buf, &payload).unwrap();
        assert_eq!(buf.len(), 4 + payload.len());

        let mut cursor = &buf[..];
        let decoded = read_frame(&mut cursor).unwrap();
        assert_eq!(decoded, payload);
    }

    #[test]
    fn round_trips_empty_payload() {
        let mut buf = Vec::new();
        write_frame(&mut buf, &[]).unwrap();
        let mut cursor = &buf[..];
        assert_eq!(read_frame(&mut cursor).unwrap(), Vec::<u8>::new());
    }

    #[test]
    fn write_frame_rejects_oversized_payload() {
        let payload = vec![0u8; (MAX_FRAME_LEN + 1) as usize];
        let mut buf = Vec::new();
        let err = write_frame(&mut buf, &payload).unwrap_err();
        assert!(matches!(err, FramingError::FrameTooLarge { .. }));
    }

    #[test]
    fn read_frame_rejects_oversized_length_prefix() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(MAX_FRAME_LEN + 1).to_be_bytes());
        let mut cursor = &buf[..];
        let err = read_frame(&mut cursor).unwrap_err();
        assert!(matches!(err, FramingError::FrameTooLarge { .. }));
    }

    #[test]
    fn read_frame_rejects_truncated_stream() {
        let mut buf = Vec::new();
        write_frame(&mut buf, b"hello").unwrap();
        buf.truncate(buf.len() - 1);
        let mut cursor = &buf[..];
        assert!(read_frame(&mut cursor).is_err());
    }
}
