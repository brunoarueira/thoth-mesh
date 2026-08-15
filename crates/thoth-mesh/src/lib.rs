//! Federation and gossip layer: peer discovery, membership, and replication
//! for the thoth-mesh federated pub/sub system.
//!
//! Peer connections share `thoth-mesh-node`'s client-facing port and
//! wire format (`thoth-mesh-core`'s `Envelope`/`MessageKind`),
//! identifying themselves via a `Hello` handshake. See ADR-0009. Once
//! connected, they carry topic-interest propagation so a publish on
//! one node reaches subscribers on another - see ADR-0011.

mod handshake;
mod interest;
mod membership;

pub use handshake::{HandshakeError, PeerInfo, dial_handshake, hello};
pub use interest::Interest;
pub use membership::{Membership, PeerStatus};
