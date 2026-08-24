//! SHA-256 fingerprinting of a peer's TLS certificate - the identity
//! [`crate::server_config`]/[`crate::client_config`]'s optional
//! client-cert verification makes available, but doesn't itself act
//! on. See ADR-0017.

use rustls_pki_types::CertificateDer;

/// The SHA-256 digest of `cert`'s raw DER bytes - what an
/// `--allow-peer` entry (or equivalent) is compared against.
pub fn fingerprint(cert: &CertificateDer<'_>) -> [u8; 32] {
    let digest = ring::digest::digest(&ring::digest::SHA256, cert.as_ref());
    digest
        .as_ref()
        .try_into()
        .expect("SHA-256 digests are always 32 bytes")
}

/// [`fingerprint`], formatted the same way `openssl x509 -fingerprint
/// -sha256` prints one - colon-separated uppercase hex pairs - so a
/// value copied from that command's output and one printed by this
/// function are visually identical.
pub fn fingerprint_hex(cert: &CertificateDer<'_>) -> String {
    fingerprint(cert)
        .iter()
        .map(|byte| format!("{byte:02X}"))
        .collect::<Vec<_>>()
        .join(":")
}

/// Parses a fingerprint from `s`, tolerant of `openssl`'s own output
/// format (`sha256 Fingerprint=AA:BB:...`) as well as a bare hex
/// string, with or without colons, in either case - so a value can be
/// pasted in from `openssl`'s output verbatim rather than requiring
/// the operator to reformat it first.
///
/// Only the part after the last `=` (if any) is considered - a plain
/// hex/colon string has no `=` and is used as-is, but
/// `sha256 Fingerprint=...` also has to have its label stripped first:
/// digits in `sha256` are themselves valid hex characters, so a naive
/// "keep every hex digit in the whole string" filter would corrupt the
/// value with a stray `256`.
pub fn parse_fingerprint(s: &str) -> Result<[u8; 32], ParseFingerprintError> {
    let value = s.rsplit('=').next().unwrap_or(s);
    let hex: String = value.chars().filter(char::is_ascii_hexdigit).collect();
    if hex.len() != 64 {
        return Err(ParseFingerprintError::WrongLength { found: hex.len() });
    }
    // Every char just passed is_ascii_hexdigit, so from_str_radix on
    // a 2-char slice of them can't fail.
    let mut out = [0u8; 32];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).expect("checked ascii hex digits");
    }
    Ok(out)
}

#[derive(Debug, thiserror::Error)]
pub enum ParseFingerprintError {
    #[error("expected 64 hex digits (a SHA-256 fingerprint), found {found}")]
    WrongLength { found: usize },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fingerprint_hex_round_trips_through_parse_fingerprint() {
        let cert = CertificateDer::from(vec![1, 2, 3, 4, 5]);
        let hex = fingerprint_hex(&cert);
        assert_eq!(parse_fingerprint(&hex).unwrap(), fingerprint(&cert));
    }

    #[test]
    fn parse_fingerprint_accepts_opensslsstyle_output() {
        let cert = CertificateDer::from(vec![9, 9, 9]);
        let openssl_style = format!("sha256 Fingerprint={}", fingerprint_hex(&cert));
        assert_eq!(
            parse_fingerprint(&openssl_style).unwrap(),
            fingerprint(&cert)
        );
    }

    #[test]
    fn parse_fingerprint_accepts_lowercase_and_no_colons() {
        let cert = CertificateDer::from(vec![0xAB, 0xCD]);
        let hex = fingerprint_hex(&cert).replace(':', "").to_lowercase();
        assert_eq!(parse_fingerprint(&hex).unwrap(), fingerprint(&cert));
    }

    #[test]
    fn parse_fingerprint_rejects_the_wrong_length() {
        assert!(matches!(
            parse_fingerprint("AA:BB"),
            Err(ParseFingerprintError::WrongLength { found: 4 })
        ));
    }

    #[test]
    fn different_certs_have_different_fingerprints() {
        let a = CertificateDer::from(vec![1]);
        let b = CertificateDer::from(vec![2]);
        assert_ne!(fingerprint(&a), fingerprint(&b));
    }
}
