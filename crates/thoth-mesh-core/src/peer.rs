use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// The identity of a peer in the mesh.
///
/// [`PeerId::new()`] is a bare random UUID and carries no
/// cryptographic guarantees on its own - nothing should assume one
/// proves who sent a message. [`PeerId::from_fingerprint`] is the
/// cryptographic alternative ADR-0005 anticipated, for a connection
/// that actually has a TLS certificate to derive one from (see
/// ADR-0038); a connection with no certificate still has nothing
/// stronger to offer than a self-reported `PeerId::new()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PeerId(Uuid);

impl PeerId {
    /// Generates a new, random peer identity.
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    /// Derives a deterministic identity from a TLS certificate's
    /// SHA-256 `fingerprint` (the same primitive `--allow-peer`/
    /// `--topic-acl` already authenticate connections with) - the
    /// same certificate always derives the same `PeerId`. Packs the
    /// fingerprint's first 16 bytes as an RFC 9562 version-8
    /// ("custom") UUID, the only version reserved for
    /// implementation-defined bytes like this one - distinct from the
    /// version 7 (timestamp-ordered) [`PeerId::new()`] and
    /// [`crate::MessageId`] both use, so a cryptographically-derived
    /// `PeerId` is visibly distinguishable from a self-assigned one.
    /// See ADR-0038.
    pub fn from_fingerprint(fingerprint: [u8; 32]) -> Self {
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&fingerprint[..16]);
        Self(uuid::Builder::from_custom_bytes(bytes).into_uuid())
    }
}

impl Default for PeerId {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_ids_are_distinct() {
        assert_ne!(PeerId::new(), PeerId::new());
    }

    #[test]
    fn from_fingerprint_is_deterministic() {
        let fingerprint = [7u8; 32];
        assert_eq!(
            PeerId::from_fingerprint(fingerprint),
            PeerId::from_fingerprint(fingerprint)
        );
    }

    #[test]
    fn from_fingerprint_differs_for_different_fingerprints() {
        assert_ne!(
            PeerId::from_fingerprint([1u8; 32]),
            PeerId::from_fingerprint([2u8; 32])
        );
    }

    #[test]
    fn from_fingerprint_never_collides_with_a_self_assigned_id() {
        // PeerId::new() is always version 7 (uuid::Uuid::now_v7);
        // from_fingerprint is always version 8 (uuid::Builder::
        // from_custom_bytes) - distinct version nibbles mean the two
        // can never produce the same PeerId, structurally, no matter
        // what bytes a fingerprint happens to contain.
        assert_ne!(PeerId::new(), PeerId::from_fingerprint([0u8; 32]));
    }

    #[test]
    fn round_trips_through_cbor() {
        let id = PeerId::new();
        let mut bytes = Vec::new();
        ciborium::into_writer(&id, &mut bytes).unwrap();
        let decoded: PeerId = ciborium::from_reader(&bytes[..]).unwrap();
        assert_eq!(id, decoded);
    }
}
