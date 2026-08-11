//! Federation and gossip layer: peer discovery, membership, and replication
//! for the thoth-mesh federated pub/sub system.
//!
//! Peer connections share `thoth-mesh-node`'s client-facing port and
//! wire format (`thoth-mesh-core`'s `Envelope`/`MessageKind`),
//! identifying themselves via a `Hello` handshake. See ADR-0009.

mod handshake;

pub use handshake::{HandshakeError, PeerInfo, dial_handshake, hello};
