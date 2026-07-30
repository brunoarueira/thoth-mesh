use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// The identity of a peer in the mesh.
///
/// This is currently an opaque UUID and carries no cryptographic
/// guarantees - nothing should assume a `PeerId` proves who sent a
/// message. It is expected to be replaced by a cryptographic identity
/// once federation/trust work begins (see ADR-0005).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PeerId(Uuid);

impl PeerId {
    /// Generates a new, random peer identity.
    pub fn new() -> Self {
        Self(Uuid::now_v7())
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
    fn round_trips_through_cbor() {
        let id = PeerId::new();
        let mut bytes = Vec::new();
        ciborium::into_writer(&id, &mut bytes).unwrap();
        let decoded: PeerId = ciborium::from_reader(&bytes[..]).unwrap();
        assert_eq!(id, decoded);
    }
}
