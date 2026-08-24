//! Per-connection handling: reads framed envelopes off a socket,
//! dispatches them to the broker, and forwards broadcast deliveries back
//! out. See ADR-0007. Also propagates local topic-interest changes to
//! peer links (see ADR-0011), and peer addresses learned about, for
//! discovery (see ADR-0015).

use std::collections::{HashMap, HashSet};
use std::io::ErrorKind;
use std::sync::Arc;

use thoth_mesh::{Interest, PeerDirectory, PeerInfo};
use thoth_mesh_broker::Broker;
use thoth_mesh_core::async_framing;
use thoth_mesh_core::{Envelope, FramingError, MessageKind, PeerAdvert, PeerId, Topic};
use thoth_mesh_tls::{MaybeTlsStream, fingerprint};
use tokio::io::{WriteHalf, split};
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};
use tracing::Instrument;

use crate::metrics::Metrics;
use crate::peer_links::PeerLinks;
use crate::shared::Shared;

const OUTGOING_CHANNEL_CAPACITY: usize = 64;

/// Handles a single accepted connection until it disconnects or a
/// framing/decode error closes it.
///
/// `initial_peer` is `Some` when the caller already knows this
/// connection is a peer link before the loop starts - the dial side
/// completes its handshake before handing off here (see ADR-0010).
/// It's `None` on the accept side, which only learns the peer's
/// identity if and when it receives a `Hello` mid-loop, same as any
/// client connection.
///
/// Either way, `membership.mark_connected` and registering the peer
/// link happen right next to each other (see `run_connection` and the
/// `Hello` arm below) rather than the caller doing the former before
/// handing off - a peer becoming visible as reachable and its link
/// becoming usable for interest propagation (ADR-0011) are meant to
/// be one and the same moment, not two that could race apart under
/// scheduling delay.
pub async fn handle_connection(
    socket: MaybeTlsStream,
    shared: Shared,
    initial_peer: Option<PeerInfo>,
) {
    let peer_addr = socket.peer_addr().ok();
    let span = tracing::info_span!("connection", peer = ?peer_addr);
    async move {
        run_connection(socket, shared, initial_peer).await;
    }
    .instrument(span)
    .await
}

async fn run_connection(socket: MaybeTlsStream, shared: Shared, initial_peer: Option<PeerInfo>) {
    // Has to be read off `socket` itself, before it's split below -
    // the split halves only implement AsyncRead/AsyncWrite, not the
    // TLS session introspection `peer_certificates` needs. The leaf
    // cert (what gets fingerprinted) is conventionally first in the
    // chain a peer presents. See ADR-0017.
    let peer_fingerprint = socket
        .peer_certificates()
        .and_then(|certs| certs.first())
        .map(fingerprint);
    let Shared {
        broker,
        membership,
        interest,
        peer_links,
        node_id,
        my_listen_addr,
        metrics,
        discover,
        discovered_tx,
        tls_acceptor: _,
        tls_connector: _,
        allowed_peers,
    } = shared;
    let (reader, writer) = split(socket);
    let mut reader = reader.compat();
    let mut writer = writer.compat_write();
    let (outgoing_tx, mut outgoing_rx) = mpsc::channel::<Arc<Envelope>>(OUTGOING_CHANNEL_CAPACITY);
    let mut forwarders: HashMap<Topic, JoinHandle<()>> = HashMap::new();
    // Set once this connection is known to be a peer link - either
    // passed in already-known (dial side) or learned from an incoming
    // Hello (accept side) - so we know whose membership entry to
    // clear when it ends.
    let mut peer_identity: Option<PeerId> = None;

    if let Some(PeerInfo {
        peer_id,
        listen_addr,
        hello_id,
    }) = initial_peer
    {
        if !allowlist_permits(&allowed_peers, peer_fingerprint) {
            tracing::warn!(
                ?peer_id,
                "closing connection: peer certificate not on the --allow-peer allowlist"
            );
            let error = Envelope::new(
                node_id,
                MessageKind::Error {
                    in_reply_to: Some(hello_id),
                    message: "peer certificate not on the --allow-peer allowlist".to_owned(),
                },
            );
            let _ = send_envelope(&mut writer, &error).await;
            return;
        }
        peer_identity = Some(peer_id);
        membership.mark_connected(peer_id, listen_addr.clone());
        register_peer_link(
            &peer_links,
            &interest,
            &discover,
            node_id,
            peer_id,
            listen_addr,
            &outgoing_tx,
        );
    }

    loop {
        tokio::select! {
            frame = async_framing::read_frame(&mut reader) => {
                let bytes = match frame {
                    Ok(bytes) => bytes,
                    Err(err) => {
                        if is_clean_disconnect(&err) {
                            tracing::debug!("client disconnected");
                        } else {
                            tracing::warn!(%err, "closing connection: frame read error");
                        }
                        break;
                    }
                };
                let envelope = match Envelope::from_bytes(&bytes) {
                    Ok(envelope) => envelope,
                    Err(err) => {
                        tracing::warn!(%err, "closing connection: malformed envelope");
                        break;
                    }
                };

                match &envelope.kind {
                    MessageKind::Subscribe { topic } => {
                        let topic = topic.clone();
                        tracing::info!(sender = ?envelope.sender, %topic, "subscribed");
                        let is_new_forwarder = !forwarders.contains_key(&topic);
                        forwarders.entry(topic.clone()).or_insert_with(|| {
                            spawn_forwarder(&broker, topic.clone(), outgoing_tx.clone(), metrics.clone())
                        });
                        if is_new_forwarder && interest.subscribe(topic.clone()) {
                            propagate_interest(&peer_links, node_id, topic, true);
                        }
                        let ack = Envelope::new(node_id, MessageKind::Ack { in_reply_to: envelope.id });
                        if !send_envelope(&mut writer, &ack).await {
                            break;
                        }
                    }
                    MessageKind::Unsubscribe { topic } => {
                        tracing::info!(sender = ?envelope.sender, %topic, "unsubscribed");
                        if let Some(handle) = forwarders.remove(topic) {
                            handle.abort();
                            if interest.unsubscribe(topic) {
                                propagate_interest(&peer_links, node_id, topic.clone(), false);
                            }
                        }
                        let ack = Envelope::new(node_id, MessageKind::Ack { in_reply_to: envelope.id });
                        if !send_envelope(&mut writer, &ack).await {
                            break;
                        }
                    }
                    MessageKind::Publish { topic, payload } => {
                        tracing::debug!(sender = ?envelope.sender, %topic, len = payload.len(), "publish");
                        let topic = topic.clone();
                        broker.publish(&topic, Arc::new(envelope)).await;
                    }
                    MessageKind::Ack { .. } | MessageKind::Error { .. } => {
                        // Not actionable from a client in v1; ignore.
                    }
                    MessageKind::Hello { listen_addr } => {
                        tracing::info!(
                            sender = ?envelope.sender,
                            peer_listen_addr = ?listen_addr,
                            "peer said hello"
                        );
                        if !allowlist_permits(&allowed_peers, peer_fingerprint) {
                            tracing::warn!(
                                sender = ?envelope.sender,
                                "closing connection: peer certificate not on the --allow-peer allowlist"
                            );
                            let error = Envelope::new(
                                node_id,
                                MessageKind::Error {
                                    in_reply_to: Some(envelope.id),
                                    message: "peer certificate not on the --allow-peer allowlist"
                                        .to_owned(),
                                },
                            );
                            let _ = send_envelope(&mut writer, &error).await;
                            break;
                        }
                        peer_identity = Some(envelope.sender);
                        membership.mark_connected(envelope.sender, listen_addr.clone());
                        register_peer_link(
                            &peer_links,
                            &interest,
                            &discover,
                            node_id,
                            envelope.sender,
                            listen_addr.clone(),
                            &outgoing_tx,
                        );
                        let reply = Envelope::new(
                            node_id,
                            MessageKind::Hello {
                                listen_addr: my_listen_addr.clone(),
                            },
                        );
                        if !send_envelope(&mut writer, &reply).await {
                            break;
                        }
                    }
                    MessageKind::PeerAnnounce { peers } => {
                        learn_peers(
                            &peer_links,
                            &discover,
                            &discovered_tx,
                            node_id,
                            peers,
                        );
                    }
                }
            }
            Some(outgoing) = outgoing_rx.recv() => {
                if !send_envelope(&mut writer, &outgoing).await {
                    tracing::warn!("closing connection: frame write error");
                    break;
                }
            }
        }
    }

    for (topic, handle) in forwarders {
        tracing::debug!(%topic, "stopping forwarder");
        handle.abort();
        if interest.unsubscribe(&topic) {
            propagate_interest(&peer_links, node_id, topic, false);
        }
    }

    if let Some(peer_id) = peer_identity {
        peer_links.unregister(peer_id, &outgoing_tx);
        membership.mark_disconnected(peer_id);
    }
}

/// Registers this connection as a peer link and catches it up on
/// every topic this node is currently interested in (ADR-0011) and
/// every other peer this node already knows how to reach (ADR-0015) -
/// so a peer that connects after either was already established
/// still learns about it, not just future transitions.
///
/// Best-effort, like [`PeerLinks::broadcast`]: a catch-up message that
/// doesn't fit in the outgoing queue right now is dropped rather than
/// blocking the connection on it, on the assumption a channel this
/// backed up already has bigger problems.
#[allow(clippy::too_many_arguments)]
fn register_peer_link(
    peer_links: &PeerLinks,
    interest: &Interest,
    discover: &PeerDirectory,
    node_id: PeerId,
    peer_id: PeerId,
    peer_listen_addr: Option<String>,
    outgoing_tx: &mpsc::Sender<Arc<Envelope>>,
) {
    peer_links.register(peer_id, outgoing_tx.clone());
    for topic in interest.snapshot() {
        let envelope = Arc::new(Envelope::new(
            node_id,
            MessageKind::Subscribe {
                topic: topic.clone(),
            },
        ));
        if outgoing_tx.try_send(envelope).is_err() {
            tracing::warn!(%topic, "outgoing queue full, dropping interest catch-up for this topic");
        }
    }

    // Peer discovery (ADR-0015): record the newly-linked peer itself,
    // if it's dialable, and tell every other active peer link about
    // it - same 0-to-1 transition idempotency ADR-0011 established
    // for interest, so this terminates the same way.
    if let Some(listen_addr) = peer_listen_addr
        && discover.record(peer_id, listen_addr.clone())
    {
        propagate_peer(peer_links, node_id, peer_id, listen_addr);
    }

    // Catch this new link up on every other peer we already know
    // about, batched into one message (unlike interest's per-topic
    // catch-up, a peer entry needs no per-connection forwarder task,
    // so there's no reason to send one envelope per peer).
    let known = discover.snapshot_excluding(peer_id);
    if !known.is_empty() {
        let peers = known
            .into_iter()
            .map(|(peer_id, listen_addr)| PeerAdvert {
                peer_id,
                listen_addr,
            })
            .collect();
        let envelope = Arc::new(Envelope::new(node_id, MessageKind::PeerAnnounce { peers }));
        if outgoing_tx.try_send(envelope).is_err() {
            tracing::warn!("outgoing queue full, dropping peer catch-up announce");
        }
    }
}

/// Records every peer in `peers` this node didn't already know about,
/// auto-dials the ones this node is responsible for dialing, and
/// propagates the newly-learned ones onward to every other active
/// peer link. See ADR-0015.
///
/// Auto-dial only happens for a peer whose `PeerId` sorts greater
/// than `node_id`'s: both sides of a pair independently learning
/// about each other at close to the same time would otherwise each
/// dial the other, racing two concurrent connections for the same
/// peer against `Membership`/`PeerLinks`' single-connection-per-peer
/// assumption. Comparing `PeerId`s deterministically picks exactly one
/// side to dial, with no coordination needed.
fn learn_peers(
    peer_links: &PeerLinks,
    discover: &PeerDirectory,
    discovered_tx: &mpsc::UnboundedSender<String>,
    node_id: PeerId,
    peers: &[PeerAdvert],
) {
    let mut newly_learned = Vec::new();
    for advert in peers {
        if advert.peer_id == node_id {
            continue; // never learn about ourselves
        }
        if discover.record(advert.peer_id, advert.listen_addr.clone()) {
            if we_should_dial(node_id, advert.peer_id) {
                let _ = discovered_tx.send(advert.listen_addr.clone());
            }
            newly_learned.push(advert.clone());
        }
    }
    if !newly_learned.is_empty() {
        let envelope = Arc::new(Envelope::new(
            node_id,
            MessageKind::PeerAnnounce {
                peers: newly_learned,
            },
        ));
        peer_links.broadcast(envelope);
    }
}

/// Whether this node (`node_id`) is the one responsible for dialing a
/// newly-discovered `peer_id`, rather than waiting to be dialed by
/// it. A `PeerId` comparison, evaluated independently by both sides
/// of a pair with no coordination - see ADR-0015 for why this is
/// needed and why it's sufficient.
fn we_should_dial(node_id: PeerId, peer_id: PeerId) -> bool {
    node_id < peer_id
}

/// Tells every active peer link about a newly-learned peer. See
/// ADR-0015.
fn propagate_peer(peer_links: &PeerLinks, node_id: PeerId, peer_id: PeerId, listen_addr: String) {
    let kind = MessageKind::PeerAnnounce {
        peers: vec![PeerAdvert {
            peer_id,
            listen_addr,
        }],
    };
    peer_links.broadcast(Arc::new(Envelope::new(node_id, kind)));
}

/// Tells every active peer link about a local topic-interest
/// transition: `now_interested` selects `Subscribe` (a topic just
/// gained its first interested connection) or `Unsubscribe` (it just
/// lost its last). See ADR-0011.
fn propagate_interest(peer_links: &PeerLinks, node_id: PeerId, topic: Topic, now_interested: bool) {
    let kind = if now_interested {
        MessageKind::Subscribe { topic }
    } else {
        MessageKind::Unsubscribe { topic }
    };
    peer_links.broadcast(Arc::new(Envelope::new(node_id, kind)));
}

/// Whether a peer link presenting `peer_fingerprint` is allowed to
/// register, given `allowed_peers`. `None` - no `--allow-peer` given -
/// permits everything, unchanged from before ADR-0017. A missing
/// fingerprint (no client certificate presented) never satisfies a
/// configured allowlist; it just never matches one.
fn allowlist_permits(
    allowed_peers: &Option<Arc<HashSet<[u8; 32]>>>,
    peer_fingerprint: Option<[u8; 32]>,
) -> bool {
    match allowed_peers {
        None => true,
        Some(allowed) => peer_fingerprint.is_some_and(|fp| allowed.contains(&fp)),
    }
}

/// A [`FramingError::Io`] whose kind is `UnexpectedEof` is what a
/// clean client disconnect looks like from the read side - not
/// something worth a warning.
fn is_clean_disconnect(err: &FramingError) -> bool {
    matches!(err, FramingError::Io(io_err) if io_err.kind() == ErrorKind::UnexpectedEof)
}

async fn send_envelope(
    writer: &mut Compat<WriteHalf<MaybeTlsStream>>,
    envelope: &Envelope,
) -> bool {
    match envelope.to_bytes() {
        Ok(bytes) => async_framing::write_frame(writer, &bytes).await.is_ok(),
        Err(_) => false,
    }
}

fn spawn_forwarder(
    broker: &Arc<Broker>,
    topic: Topic,
    outgoing_tx: mpsc::Sender<Arc<Envelope>>,
    metrics: Metrics,
) -> JoinHandle<()> {
    let broker = Arc::clone(broker);
    tokio::spawn(
        async move {
            let mut rx = broker.subscribe(topic).await;
            loop {
                match rx.recv().await {
                    Ok(envelope) => {
                        if outgoing_tx.send(envelope).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::warn!(skipped, "forwarder lagged, dropped messages");
                        metrics.record_forwarder_lag(skipped);
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        }
        .in_current_span(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unexpected_eof_is_a_clean_disconnect() {
        let err = FramingError::Io(std::io::Error::from(ErrorKind::UnexpectedEof));
        assert!(is_clean_disconnect(&err));
    }

    #[test]
    fn other_io_errors_are_not_a_clean_disconnect() {
        let err = FramingError::Io(std::io::Error::from(ErrorKind::ConnectionReset));
        assert!(!is_clean_disconnect(&err));
    }

    #[test]
    fn frame_too_large_is_not_a_clean_disconnect() {
        let err = FramingError::FrameTooLarge { len: 100, max: 10 };
        assert!(!is_clean_disconnect(&err));
    }

    #[test]
    fn we_should_dial_favors_the_smaller_peer_id() {
        let mut ids = [PeerId::new(), PeerId::new()];
        ids.sort();
        let [smaller, larger] = ids;
        assert!(we_should_dial(smaller, larger));
        assert!(!we_should_dial(larger, smaller));
    }

    #[test]
    fn we_should_dial_is_false_for_equal_ids() {
        let id = PeerId::new();
        assert!(!we_should_dial(id, id));
    }

    #[tokio::test]
    async fn learn_peers_skips_self_dials_only_the_larger_side_and_rebroadcasts_new_ones() {
        // Sorting three fresh ids and taking the middle one as our own
        // guarantees one peer sorts below node_id and one sorts above
        // it, exercising both branches of we_should_dial in one call.
        let mut ids = [PeerId::new(), PeerId::new(), PeerId::new()];
        ids.sort();
        let [smaller_peer, node_id, larger_peer] = ids;

        let peer_links = PeerLinks::new();
        let discover = PeerDirectory::new();
        let (discovered_tx, mut discovered_rx) = mpsc::unbounded_channel();
        let (link_tx, mut link_rx) = mpsc::channel(8);
        peer_links.register(PeerId::new(), link_tx);

        let peers = vec![
            PeerAdvert {
                peer_id: node_id,
                listen_addr: "127.0.0.1:1".to_owned(),
            },
            PeerAdvert {
                peer_id: smaller_peer,
                listen_addr: "127.0.0.1:2".to_owned(),
            },
            PeerAdvert {
                peer_id: larger_peer,
                listen_addr: "127.0.0.1:3".to_owned(),
            },
        ];

        learn_peers(&peer_links, &discover, &discovered_tx, node_id, &peers);

        // Only the larger peer gets auto-dialed; the smaller peer is
        // learned about but left for it to dial us, and our own
        // entry is ignored outright.
        assert_eq!(discovered_rx.try_recv().unwrap(), "127.0.0.1:3");
        assert!(discovered_rx.try_recv().is_err());

        let mut known = discover.snapshot_excluding(PeerId::new());
        known.sort();
        let mut expected = vec![
            (smaller_peer, "127.0.0.1:2".to_owned()),
            (larger_peer, "127.0.0.1:3".to_owned()),
        ];
        expected.sort();
        assert_eq!(known, expected);

        let broadcast = link_rx.try_recv().unwrap();
        match &broadcast.kind {
            MessageKind::PeerAnnounce { peers } => assert_eq!(peers.len(), 2),
            other => panic!("expected PeerAnnounce, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn learn_peers_does_not_rebroadcast_an_already_known_peer() {
        let node_id = PeerId::new();
        let peer_links = PeerLinks::new();
        let discover = PeerDirectory::new();
        let (discovered_tx, _discovered_rx) = mpsc::unbounded_channel();
        let (link_tx, mut link_rx) = mpsc::channel(8);
        peer_links.register(PeerId::new(), link_tx);

        let already_known = PeerId::new();
        discover.record(already_known, "127.0.0.1:1".to_owned());

        learn_peers(
            &peer_links,
            &discover,
            &discovered_tx,
            node_id,
            &[PeerAdvert {
                peer_id: already_known,
                listen_addr: "127.0.0.1:1".to_owned(),
            }],
        );

        assert!(link_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn register_peer_link_announces_itself_and_catches_up_on_known_peers() {
        let peer_links = PeerLinks::new();
        let interest = Interest::new();
        let discover = PeerDirectory::new();
        let node_id = PeerId::new();
        let already_known = PeerId::new();
        discover.record(already_known, "127.0.0.1:1".to_owned());

        let new_peer = PeerId::new();
        let (outgoing_tx, mut outgoing_rx) = mpsc::channel(8);

        register_peer_link(
            &peer_links,
            &interest,
            &discover,
            node_id,
            new_peer,
            Some("127.0.0.1:2".to_owned()),
            &outgoing_tx,
        );

        let mut announced_peers = Vec::new();
        while let Ok(envelope) = outgoing_rx.try_recv() {
            match &envelope.kind {
                MessageKind::PeerAnnounce { peers } => announced_peers.extend(peers.clone()),
                other => panic!("expected only PeerAnnounce envelopes, got {other:?}"),
            }
        }
        announced_peers.sort_by_key(|advert| advert.peer_id);

        let mut expected = vec![
            PeerAdvert {
                peer_id: already_known,
                listen_addr: "127.0.0.1:1".to_owned(),
            },
            PeerAdvert {
                peer_id: new_peer,
                listen_addr: "127.0.0.1:2".to_owned(),
            },
        ];
        expected.sort_by_key(|advert| advert.peer_id);
        assert_eq!(announced_peers, expected);

        assert!(
            discover
                .snapshot_excluding(PeerId::new())
                .into_iter()
                .any(|(id, addr)| id == new_peer && addr == "127.0.0.1:2")
        );
    }

    #[tokio::test]
    async fn register_peer_link_does_not_record_a_peer_with_no_listen_addr() {
        let peer_links = PeerLinks::new();
        let interest = Interest::new();
        let discover = PeerDirectory::new();
        let node_id = PeerId::new();
        let new_peer = PeerId::new();
        let (outgoing_tx, mut outgoing_rx) = mpsc::channel(8);

        register_peer_link(
            &peer_links,
            &interest,
            &discover,
            node_id,
            new_peer,
            None,
            &outgoing_tx,
        );

        // Nothing to catch up on, and nothing recorded about the new
        // peer itself - a dial-out-only peer isn't dialable by
        // anyone, so there's nothing useful to gossip about it.
        assert!(outgoing_rx.try_recv().is_err());
        assert!(discover.snapshot_excluding(PeerId::new()).is_empty());
    }
}
