//! Per-connection handling: reads framed envelopes off a socket,
//! dispatches them to the broker, and forwards broadcast deliveries back
//! out. See ADR-0007. Also propagates local topic-interest changes to
//! peer links (see ADR-0011), and peer addresses learned about, for
//! discovery (see ADR-0015). `Subscribe`/`Publish` are authorized
//! against a client-scoped `--topic-acl` (ADR-0018) or a peer-scoped
//! `--peer-topic-acl` (ADR-0020), whichever applies to the connection -
//! a wildcard `Subscribe` (ADR-0022) is refused outright wherever
//! either applies, regardless of what it would expand to. A
//! freshly-spawned forwarder replays the topic filter's buffered
//! backlog before it starts forwarding live deliveries, so a late
//! subscriber (client or peer link alike) can catch up on recent
//! history - see ADR-0021. A forwarder that falls behind mid-stream
//! and lags its broadcast receiver recovers what it can from that
//! same buffer rather than accepting silent loss outright - see
//! ADR-0024.

use std::collections::{HashMap, HashSet};
use std::io::ErrorKind;
use std::sync::Arc;

use thoth_mesh::{Interest, Membership, PeerDirectory, PeerInfo};
use thoth_mesh_broker::Broker;
use thoth_mesh_core::async_framing;
use thoth_mesh_core::{
    Envelope, FramingError, MessageId, MessageKind, PeerAdvert, PeerId, PeerSummary, Topic,
    TopicFilter,
};
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
use crate::topic_acl::{Action, Principal, TopicAcl};

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
    // The principal a --topic-acl check (ADR-0018) runs against -
    // derived from the same fingerprint ADR-0017's allowlist check
    // uses, no separate introspection needed.
    let principal = Principal::from_fingerprint(peer_fingerprint);
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
        dial_semaphore: _,
        tls_acceptor: _,
        tls_connector: _,
        allowed_peers,
        topic_acl,
        peer_topic_acl,
    } = shared;
    let (reader, writer) = split(socket);
    let mut reader = reader.compat();
    let writer = writer.compat_write();
    let (outgoing_tx, outgoing_rx) = mpsc::channel::<Arc<Envelope>>(OUTGOING_CHANNEL_CAPACITY);
    // Reading and writing run as two independent tasks sharing only
    // outgoing_tx/outgoing_rx, rather than one task alternating
    // between them via tokio::select! - the natural first design, but
    // a broken one: async_framing::read_frame reads a frame in two
    // sequential steps (length prefix, then payload), and select! can
    // only cancel a losing branch between polls, not partway through
    // one atomically. Cancelling the read after the length prefix is
    // already consumed from the stream - but before the payload read
    // starts - permanently desyncs every frame after it, since those
    // bytes can't be put back. Splitting means the read loop below
    // never races anything and always runs a read_frame call to
    // completion. See ADR-0029.
    //
    // Wrapped so aborting *this* task (as a test simulating this
    // connection dying does, via Node::accepted_connections) also
    // aborts the writer task - a plain JoinHandle would just be
    // dropped without stopping the task it refers to, leaving the
    // write half of the socket alive on its own even though nothing
    // is reading from this side any more.
    let writer_task = AbortOnDrop(tokio::spawn(write_loop(writer, outgoing_rx)));

    // Bundles everything a dispatched envelope's handling needs -
    // both the shared collaborators `Shared` was destructured into
    // above and this connection's own local state (`forwarders`,
    // `peer_identity`) - behind one `&mut self`, rather than each
    // match arm below closing over on the order of ten separate
    // captures. See ADR-0031.
    let mut ctx = ConnectionContext {
        broker,
        membership,
        interest,
        peer_links,
        node_id,
        my_listen_addr,
        metrics,
        discover,
        discovered_tx,
        allowed_peers,
        topic_acl,
        peer_topic_acl,
        outgoing_tx,
        forwarders: HashMap::new(),
        peer_identity: None,
        peer_fingerprint,
        principal,
    };

    if let Some(PeerInfo {
        peer_id,
        listen_addr,
        hello_id,
    }) = initial_peer
        && !ctx.admit_initial_peer(peer_id, listen_addr, hello_id).await
    {
        drop(ctx.outgoing_tx);
        let _ = writer_task.await;
        return;
    }

    loop {
        let bytes = match async_framing::read_frame(&mut reader).await {
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
        let mut envelope = match Envelope::from_bytes(&bytes) {
            Ok(envelope) => envelope,
            Err(err) => {
                tracing::warn!(%err, "closing connection: malformed envelope");
                break;
            }
        };
        // Authenticated once, here, rather than in each handler below
        // - every message kind that carries a meaningful `sender`
        // (Hello, Publish, Subscribe, Unsubscribe, StatusRequest) is
        // covered by this one call, and a Publish's corrected sender
        // is what actually gets broadcast to subscribers. See
        // ADR-0039.
        envelope.sender = ctx.authenticated_sender(envelope.sender);

        let keep_going = match &envelope.kind {
            MessageKind::Subscribe { filter } => {
                let filter = filter.clone();
                ctx.handle_subscribe(&envelope, filter).await
            }
            MessageKind::Unsubscribe { filter } => {
                let filter = filter.clone();
                ctx.handle_unsubscribe(&envelope, filter).await
            }
            MessageKind::Publish { .. } => ctx.handle_publish(envelope).await,
            MessageKind::Ack { .. }
            | MessageKind::Error { .. }
            | MessageKind::StatusReply { .. } => {
                // Not actionable from a client in v1; ignore. A
                // StatusReply is only ever sent by a node, never a
                // client, but still has to go somewhere in this match.
                true
            }
            MessageKind::Hello { listen_addr } => {
                let listen_addr = listen_addr.clone();
                ctx.handle_hello(&envelope, listen_addr).await
            }
            MessageKind::PeerAnnounce { peers } => {
                ctx.handle_peer_announce(peers);
                true
            }
            MessageKind::StatusRequest => ctx.handle_status(&envelope).await,
        };
        if !keep_going {
            break;
        }
    }

    ctx.shut_down();

    // Every other outgoing_tx clone (forwarders, the peer_links
    // registry entry) is already gone by this point - dropping this
    // last one closes the channel write_loop is reading from, and it
    // exits on its own once it's drained whatever was already queued.
    // Joined rather than left as a fire-and-forget background task, so
    // this connection isn't considered fully done until its last reply
    // has actually been flushed (or the write side already failed).
    drop(ctx.outgoing_tx);
    let _ = writer_task.await;
}

/// Everything one connection's envelope dispatch needs: the shared
/// collaborators every connection on this node has access to (what
/// `Shared` used to be destructured into, right inside
/// `run_connection`, before this struct existed - see ADR-0031), plus
/// this connection's own local state. Constructed once per connection
/// and never shared across connections.
struct ConnectionContext {
    broker: Arc<Broker>,
    membership: Membership,
    interest: Interest,
    peer_links: PeerLinks,
    node_id: PeerId,
    my_listen_addr: Option<String>,
    metrics: Metrics,
    discover: PeerDirectory,
    discovered_tx: mpsc::UnboundedSender<String>,
    allowed_peers: Option<Arc<HashSet<[u8; 32]>>>,
    topic_acl: Option<Arc<TopicAcl>>,
    peer_topic_acl: Option<Arc<TopicAcl>>,
    outgoing_tx: mpsc::Sender<Arc<Envelope>>,
    /// Every topic filter a `Subscribe` on this connection is
    /// currently forwarding for, keyed by the same filter a matching
    /// `Unsubscribe` removes it by.
    forwarders: HashMap<TopicFilter, JoinHandle<()>>,
    /// Set once this connection is known to be a peer link - either
    /// passed in already-known (dial side, ADR-0010) or learned from
    /// an incoming `Hello` (accept side) - so `shut_down` knows whose
    /// membership entry to clear, and so ACL checks know whether
    /// `topic_acl` (ADR-0018) or `peer_topic_acl` (ADR-0020) applies.
    peer_identity: Option<PeerId>,
    peer_fingerprint: Option<[u8; 32]>,
    principal: Principal,
}

impl ConnectionContext {
    /// Whether this connection is currently known to be a peer link -
    /// gates which of `topic_acl`/`peer_topic_acl` an ACL check below
    /// uses.
    fn is_peer(&self) -> bool {
        self.peer_identity.is_some()
    }

    /// The identity `claimed` should actually be treated as: if this
    /// connection has a known TLS fingerprint (ADR-0038), the
    /// identity that fingerprint derives - silently overriding
    /// `claimed` if it disagrees, rather than rejecting the
    /// connection outright (see ADR-0039). A connection with no
    /// fingerprint (plaintext, or TLS without a client certificate)
    /// has nothing to correct against, so `claimed` passes through
    /// unchanged - the same boundary `Principal::Anonymous` already
    /// draws for `--topic-acl`.
    fn authenticated_sender(&self, claimed: PeerId) -> PeerId {
        let Some(fingerprint) = self.peer_fingerprint else {
            return claimed;
        };
        let authenticated = PeerId::from_fingerprint(fingerprint);
        if claimed != authenticated {
            tracing::warn!(
                ?claimed,
                ?authenticated,
                "claimed PeerId does not match this connection's TLS identity - using the authenticated one"
            );
        }
        authenticated
    }

    /// Queues `envelope` for the write task. Returns `false` if the
    /// outgoing channel has already closed (the writer task ended,
    /// e.g. on a write failure) - every call site treats that as a
    /// signal to stop the read loop, the same way a direct write
    /// failure used to before ADR-0029 unified every reply onto this
    /// one queue.
    async fn send(&self, envelope: Envelope) -> bool {
        self.outgoing_tx.send(Arc::new(envelope)).await.is_ok()
    }

    /// Admits an already-known peer link before the read loop starts -
    /// the dial side completes its handshake and already knows the
    /// peer's identity before handing off to `run_connection` (see
    /// ADR-0010). Checks the allowlist (ADR-0017), and on rejection
    /// sends an `Error` reply (best-effort) rather than registering
    /// anything. Returns `false` if the connection should be closed
    /// immediately, before ever reading a frame.
    async fn admit_initial_peer(
        &mut self,
        peer_id: PeerId,
        listen_addr: Option<String>,
        hello_id: MessageId,
    ) -> bool {
        // The one identity the per-frame correction in run_connection's
        // read loop can't reach - this is the dial side's already-known
        // identity, established before that loop starts. See ADR-0039.
        let peer_id = self.authenticated_sender(peer_id);
        if !allowlist_permits(&self.allowed_peers, self.peer_fingerprint) {
            tracing::warn!(
                ?peer_id,
                "closing connection: peer certificate not on the --allow-peer allowlist"
            );
            let error = Envelope::new(
                self.node_id,
                MessageKind::Error {
                    in_reply_to: Some(hello_id),
                    message: "peer certificate not on the --allow-peer allowlist".to_owned(),
                },
            );
            let _ = self.send(error).await;
            return false;
        }
        self.peer_identity = Some(peer_id);
        self.membership.mark_connected(peer_id, listen_addr.clone());
        register_peer_link(
            &self.peer_links,
            &self.interest,
            &self.discover,
            self.node_id,
            peer_id,
            listen_addr,
            &self.outgoing_tx,
        );
        true
    }

    /// Handles a `Subscribe { filter }` request: authorizes it against
    /// whichever ACL applies (ADR-0018/ADR-0020) - refusing a wildcard
    /// filter outright wherever one does, regardless of what it would
    /// expand to (ADR-0022) - spawns a forwarder for a genuinely new
    /// filter (ADR-0021), acks, and, only on this connection's first
    /// subscriber for `filter`, propagates the interest transition to
    /// every peer link (ADR-0011). Returns `false` if the outgoing
    /// queue has closed and the read loop should stop.
    async fn handle_subscribe(&mut self, envelope: &Envelope, filter: TopicFilter) -> bool {
        let is_peer = self.is_peer();
        if !filter_acl_permits(
            &self.topic_acl,
            &self.peer_topic_acl,
            is_peer,
            self.principal,
            &filter,
            Action::Subscribe,
        ) {
            tracing::warn!(sender = ?envelope.sender, %filter, is_peer, "rejected: not on the configured topic ACL");
            if is_peer {
                self.metrics.record_peer_topic_acl_rejection();
            } else {
                self.metrics.record_topic_acl_rejection();
            }
            let error = Envelope::new(
                self.node_id,
                MessageKind::Error {
                    in_reply_to: Some(envelope.id),
                    message: format!("not authorized to subscribe to {filter}"),
                },
            );
            return self.send(error).await;
        }
        tracing::info!(sender = ?envelope.sender, %filter, "subscribed");
        let is_new_forwarder = !self.forwarders.contains_key(&filter);
        let broker = Arc::clone(&self.broker);
        let outgoing_tx = self.outgoing_tx.clone();
        let metrics = self.metrics.clone();
        self.forwarders
            .entry(filter.clone())
            .or_insert_with(|| spawn_forwarder(&broker, filter.clone(), outgoing_tx, metrics));
        // The ack goes out before the interest-propagation echo below
        // (ADR-0011) - both now flow through the same outgoing_tx
        // queue (see ADR-0029), so whichever is sent first is what a
        // subscriber sees first; a reply to this connection's own
        // request reads more naturally arriving ahead of a side
        // effect of it.
        let ack = Envelope::new(
            self.node_id,
            MessageKind::Ack {
                in_reply_to: envelope.id,
            },
        );
        if !self.send(ack).await {
            return false;
        }
        if is_new_forwarder && self.interest.subscribe(filter.clone()) {
            propagate_interest(&self.peer_links, self.node_id, filter, true);
        }
        true
    }

    /// Handles an `Unsubscribe { filter }` request: stops this
    /// connection's forwarder for `filter`, acks, and, only if this
    /// was the last subscriber for it, propagates the interest loss
    /// (ADR-0011). Returns `false` if the outgoing queue has closed
    /// and the read loop should stop.
    async fn handle_unsubscribe(&mut self, envelope: &Envelope, filter: TopicFilter) -> bool {
        tracing::info!(sender = ?envelope.sender, %filter, "unsubscribed");
        let had_forwarder = self
            .forwarders
            .remove(&filter)
            .inspect(|handle| handle.abort());
        // Ack before the echo - see handle_subscribe above.
        let ack = Envelope::new(
            self.node_id,
            MessageKind::Ack {
                in_reply_to: envelope.id,
            },
        );
        if !self.send(ack).await {
            return false;
        }
        if had_forwarder.is_some() && self.interest.unsubscribe(&filter) {
            propagate_interest(&self.peer_links, self.node_id, filter, false);
        }
        true
    }

    /// Handles a `Publish` envelope: authorizes it against whichever
    /// ACL applies (ADR-0018/ADR-0020), then hands it to the broker.
    /// Takes ownership of `envelope` rather than a reference, since a
    /// permitted publish is handed to the broker as-is, with no need
    /// to clone it first. Returns `false` if the outgoing queue has
    /// closed and the read loop should stop.
    async fn handle_publish(&mut self, envelope: Envelope) -> bool {
        let envelope = Arc::new(envelope);
        let MessageKind::Publish { topic, payload } = &envelope.kind else {
            unreachable!("handle_publish is only ever called for a Publish envelope");
        };
        tracing::debug!(sender = ?envelope.sender, %topic, len = payload.len(), "publish");
        let topic = topic.clone();
        let is_peer = self.is_peer();
        if !acl_permits(
            &self.topic_acl,
            &self.peer_topic_acl,
            is_peer,
            self.principal,
            &topic,
            Action::Publish,
        ) {
            tracing::warn!(sender = ?envelope.sender, %topic, is_peer, "rejected: not on the configured topic ACL");
            if is_peer {
                self.metrics.record_peer_topic_acl_rejection();
            } else {
                self.metrics.record_topic_acl_rejection();
            }
            let error = Envelope::new(
                self.node_id,
                MessageKind::Error {
                    in_reply_to: Some(envelope.id),
                    message: format!("not authorized to publish to {topic}"),
                },
            );
            return self.send(error).await;
        }
        self.broker.publish(&topic, envelope).await;
        true
    }

    /// Handles a `Hello { listen_addr }`: checks the allowlist
    /// (ADR-0017), replies in kind, and registers this connection as a
    /// peer link. Returns `false` if the connection should end -
    /// either the allowlist rejected it, or the outgoing queue closed
    /// while replying.
    async fn handle_hello(&mut self, envelope: &Envelope, listen_addr: Option<String>) -> bool {
        tracing::info!(
            sender = ?envelope.sender,
            peer_listen_addr = ?listen_addr,
            "peer said hello"
        );
        if !allowlist_permits(&self.allowed_peers, self.peer_fingerprint) {
            tracing::warn!(
                sender = ?envelope.sender,
                "closing connection: peer certificate not on the --allow-peer allowlist"
            );
            let error = Envelope::new(
                self.node_id,
                MessageKind::Error {
                    in_reply_to: Some(envelope.id),
                    message: "peer certificate not on the --allow-peer allowlist".to_owned(),
                },
            );
            let _ = self.send(error).await;
            return false;
        }
        self.peer_identity = Some(envelope.sender);
        self.membership
            .mark_connected(envelope.sender, listen_addr.clone());
        // The reply goes out before register_peer_link's own catch-up
        // traffic (interest, known peers) - dial_handshake on the far
        // end expects this Hello reply to be the very first thing it
        // reads, and register_peer_link would otherwise queue catch-up
        // messages ahead of it on the same outgoing_tx queue (see
        // ADR-0029), which the far end can't parse as a Hello.
        // Registering the link afterward is still safe: this task
        // doesn't read the next incoming frame until this whole method
        // returns, so nothing from the far end can be processed here
        // before register_peer_link runs anyway.
        let reply = Envelope::new(
            self.node_id,
            MessageKind::Hello {
                listen_addr: self.my_listen_addr.clone(),
            },
        );
        if !self.send(reply).await {
            return false;
        }
        register_peer_link(
            &self.peer_links,
            &self.interest,
            &self.discover,
            self.node_id,
            envelope.sender,
            listen_addr,
            &self.outgoing_tx,
        );
        true
    }

    /// Handles a `PeerAnnounce { peers }`: records/propagates/
    /// auto-dials as appropriate (ADR-0015). Never fails the
    /// connection - nothing here writes back to this link.
    fn handle_peer_announce(&self, peers: &[PeerAdvert]) {
        learn_peers(
            &self.peer_links,
            &self.discover,
            &self.discovered_tx,
            self.node_id,
            peers,
        );
    }

    /// Handles a `StatusRequest`: replies with this node's identity,
    /// every currently-connected peer (sorted by `peer_id`, for
    /// deterministic output - `Membership::snapshot` doesn't guarantee
    /// an order), and a metrics summary (the same numbers
    /// `render_prometheus` reports, as typed fields - ADR-0037).
    /// Answered on any connection, client or peer link, with no ACL
    /// check. Returns `false` if the outgoing queue has closed and the
    /// read loop should stop.
    async fn handle_status(&self, envelope: &Envelope) -> bool {
        let mut peers: Vec<PeerSummary> = self
            .membership
            .snapshot()
            .into_iter()
            .filter(|(_, status)| status.connected)
            .map(|(peer_id, status)| PeerSummary {
                peer_id,
                listen_addr: status.listen_addr,
            })
            .collect();
        peers.sort_by_key(|peer| peer.peer_id);
        let reply = Envelope::new(
            self.node_id,
            MessageKind::StatusReply {
                in_reply_to: envelope.id,
                node_id: self.node_id,
                listen_addr: self.my_listen_addr.clone(),
                peers,
                metrics: crate::metrics::summary(
                    &self.membership,
                    &self.broker,
                    &self.discover,
                    &self.metrics,
                ),
            },
        );
        self.send(reply).await
    }

    /// Runs once the read loop ends: stops every forwarder this
    /// connection spawned, propagating the resulting interest loss the
    /// same way an explicit `Unsubscribe` would (ADR-0011); then, if
    /// this connection turned out to be a peer link, unregisters it
    /// and marks it disconnected.
    fn shut_down(&mut self) {
        for (filter, handle) in self.forwarders.drain() {
            tracing::debug!(%filter, "stopping forwarder");
            handle.abort();
            if self.interest.unsubscribe(&filter) {
                propagate_interest(&self.peer_links, self.node_id, filter, false);
            }
        }
        if let Some(peer_id) = self.peer_identity {
            self.peer_links.unregister(peer_id, &self.outgoing_tx);
            self.membership.mark_disconnected(peer_id);
        }
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
    for filter in interest.snapshot() {
        let envelope = Arc::new(Envelope::new(
            node_id,
            MessageKind::Subscribe {
                filter: filter.clone(),
            },
        ));
        if outgoing_tx.try_send(envelope).is_err() {
            tracing::warn!(%filter, "outgoing queue full, dropping interest catch-up for this filter");
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
/// transition: `now_interested` selects `Subscribe` (a filter just
/// gained its first interested connection) or `Unsubscribe` (it just
/// lost its last). See ADR-0011; a propagated filter may itself be a
/// wildcard pattern (ADR-0022), handled with no special-casing.
fn propagate_interest(
    peer_links: &PeerLinks,
    node_id: PeerId,
    filter: TopicFilter,
    now_interested: bool,
) {
    let kind = if now_interested {
        MessageKind::Subscribe { filter }
    } else {
        MessageKind::Unsubscribe { filter }
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

/// Whether `principal` is allowed to perform `action` on `topic`,
/// given `topic_acl`. `None` - no `--topic-acl` given - permits
/// everything, unchanged from before ADR-0018. `Some` is default-deny:
/// only combinations an entry explicitly lists are permitted.
fn topic_acl_permits(
    topic_acl: &Option<Arc<TopicAcl>>,
    principal: Principal,
    topic: &Topic,
    action: Action,
) -> bool {
    match topic_acl {
        None => true,
        Some(acl) => acl.permits(principal, topic, action),
    }
}

/// Picks which of the two independent lists applies to this
/// connection - `topic_acl` (ADR-0018) if it's not (yet) known to be a
/// peer link, `peer_topic_acl` (ADR-0020) if it is - and checks
/// `principal`/`topic`/`action` against it via [`topic_acl_permits`].
/// A client is never checked against `peer_topic_acl`, and a peer link
/// is never checked against `topic_acl`.
fn acl_permits(
    topic_acl: &Option<Arc<TopicAcl>>,
    peer_topic_acl: &Option<Arc<TopicAcl>>,
    is_peer: bool,
    principal: Principal,
    topic: &Topic,
    action: Action,
) -> bool {
    let acl = if is_peer { peer_topic_acl } else { topic_acl };
    topic_acl_permits(acl, principal, topic, action)
}

/// Whether `principal` is allowed to `action` on `filter`, given
/// whichever of `topic_acl`/`peer_topic_acl` applies (see
/// [`acl_permits`]). Neither ACL is pattern-aware (ADR-0018/ADR-0020
/// both check an exact `Topic`); a literal `filter` is checked exactly
/// as [`acl_permits`] always has, but a genuine wildcard filter
/// (ADR-0022) is refused outright whenever an ACL is configured for
/// this connection's role, regardless of what it would expand to -
/// deliberately conservative rather than inventing pattern-vs-pattern
/// coverage semantics. `None` - no ACL configured for that role -
/// still permits everything, wildcard or not, same as before
/// ADR-0022.
fn filter_acl_permits(
    topic_acl: &Option<Arc<TopicAcl>>,
    peer_topic_acl: &Option<Arc<TopicAcl>>,
    is_peer: bool,
    principal: Principal,
    filter: &TopicFilter,
    action: Action,
) -> bool {
    let acl = if is_peer { peer_topic_acl } else { topic_acl };
    match (acl, filter.as_topic()) {
        (None, _) => true,
        (Some(_), None) => false,
        (Some(acl), Some(topic)) => acl.permits(principal, &topic, action),
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

/// Drains `outgoing_rx` and writes each envelope to `writer`, ending
/// once the channel closes (every sender - `run_connection`'s own
/// `outgoing_tx`, and every clone held by `peer_links`/a forwarder -
/// has been dropped) or a write fails. Owns the write half
/// exclusively and runs as its own task, concurrently with
/// `run_connection`'s read loop - see ADR-0029 for why the two don't
/// share one task via `tokio::select!` any more.
async fn write_loop(
    mut writer: Compat<WriteHalf<MaybeTlsStream>>,
    mut outgoing_rx: mpsc::Receiver<Arc<Envelope>>,
) {
    while let Some(envelope) = outgoing_rx.recv().await {
        if !send_envelope(&mut writer, &envelope).await {
            tracing::warn!("closing connection: frame write error");
            break;
        }
    }
}

/// A `JoinHandle` that aborts its task on drop, rather than detaching
/// it to keep running in the background - the default for a plain
/// `JoinHandle`. Used for [`write_loop`]'s task so cancelling the task
/// holding this (e.g. `run_connection` itself being aborted, as a test
/// simulating this connection dying does via
/// `Node::accepted_connections`) also stops the writer task, instead
/// of leaving the socket's write half alive with nothing left to read
/// the other side.
struct AbortOnDrop<T>(JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl<T> std::future::Future for AbortOnDrop<T> {
    type Output = Result<T, tokio::task::JoinError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        std::pin::Pin::new(&mut self.0).poll(cx)
    }
}

fn spawn_forwarder(
    broker: &Arc<Broker>,
    filter: TopicFilter,
    outgoing_tx: mpsc::Sender<Arc<Envelope>>,
    metrics: Metrics,
) -> JoinHandle<()> {
    let broker = Arc::clone(broker);
    tokio::spawn(
        async move {
            let (backlog, mut rx) = broker.subscribe(filter.clone()).await;
            // Replay whatever this topic's buffer already held, oldest
            // first, before falling into the live loop below - see
            // ADR-0021. `Broker::subscribe` guarantees this can't miss
            // or duplicate anything a concurrent publish sends.
            if !backlog.is_empty() {
                metrics.record_replayed_messages(backlog.len() as u64);
            }
            // The `MessageId` of the last envelope actually sent, from
            // either source below - what ADR-0024's lag recovery
            // correlates against, without the broker needing to track
            // per-forwarder position itself.
            let mut last_delivered: Option<MessageId> = None;
            for envelope in backlog {
                let id = envelope.id;
                if outgoing_tx.send(envelope).await.is_err() {
                    return;
                }
                last_delivered = Some(id);
            }
            loop {
                match rx.recv().await {
                    Ok(envelope) => {
                        let id = envelope.id;
                        if outgoing_tx.send(envelope).await.is_err() {
                            break;
                        }
                        last_delivered = Some(id);
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::warn!(skipped, "forwarder lagged, attempting recovery from replay buffer");
                        metrics.record_forwarder_lag(skipped);

                        // Re-subscribing reuses ADR-0021's atomic
                        // buffer-snapshot-plus-receiver-registration
                        // guarantee instead of a second, parallel
                        // mechanism for peeking the buffer - see
                        // ADR-0024.
                        let (fresh_backlog, fresh_rx) = broker.subscribe(filter.clone()).await;
                        rx = fresh_rx;

                        let recovered: Vec<Arc<Envelope>> = match last_delivered {
                            // Nothing was ever delivered live, so the
                            // whole fresh backlog is safe to send -
                            // there's nothing it could duplicate.
                            None => fresh_backlog,
                            Some(last_id) => {
                                match fresh_backlog.iter().position(|e| e.id == last_id) {
                                    Some(idx) => {
                                        fresh_backlog.into_iter().skip(idx + 1).collect()
                                    }
                                    None => {
                                        // The gap outran the buffer -
                                        // an unrecoverable loss, same
                                        // as before ADR-0024. Replaying
                                        // any of the fresh backlog here
                                        // risks re-delivering something
                                        // this forwarder already got
                                        // live before lagging.
                                        tracing::warn!(
                                            "lag recovery gap exceeded the replay buffer, some messages are unrecoverably lost"
                                        );
                                        Vec::new()
                                    }
                                }
                            }
                        };
                        if !recovered.is_empty() {
                            metrics.record_lag_recovered(recovered.len() as u64);
                        }
                        for envelope in recovered {
                            let id = envelope.id;
                            if outgoing_tx.send(envelope).await.is_err() {
                                return;
                            }
                            last_delivered = Some(id);
                        }
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
    use std::str::FromStr;
    use std::time::Duration;

    /// Publishes `count` sequential envelopes to `topic` - `payload` is
    /// the 4-byte big-endian index, so a test can decode delivery order
    /// back out without caring about `MessageId`s.
    async fn publish_sequence(broker: &Broker, topic: &Topic, count: u32) {
        for i in 0..count {
            let envelope = Envelope::new(
                PeerId::new(),
                MessageKind::Publish {
                    topic: topic.clone(),
                    payload: i.to_be_bytes().to_vec(),
                },
            );
            broker.publish(topic, Arc::new(envelope)).await;
        }
    }

    fn sequence_of(envelope: &Envelope) -> u32 {
        let MessageKind::Publish { payload, .. } = &envelope.kind else {
            panic!("expected a Publish envelope, got {:?}", envelope.kind);
        };
        u32::from_be_bytes(payload[..4].try_into().unwrap())
    }

    /// A `Lagged` gap that fits within the replay buffer's extra
    /// headroom over the broadcast channel's own capacity (see
    /// ADR-0024's sizing rationale) is fully recovered - a forwarder
    /// blocked on a tiny, undrained outgoing channel while several
    /// hundred envelopes are published still ends up seeing every one
    /// of them, in order, with nothing duplicated.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_lagged_forwarder_recovers_a_gap_within_the_buffers_headroom() {
        let broker = Arc::new(Broker::new());
        let topic = Topic::from_str("weather.updates").unwrap();
        let filter: TopicFilter = topic.clone().into();
        const CHANNEL_CAPACITY: usize = 8;
        const TOTAL: u32 = 500;

        let (outgoing_tx, mut outgoing_rx) = mpsc::channel(CHANNEL_CAPACITY);
        let _handle = spawn_forwarder(&broker, filter, outgoing_tx, Metrics::new());
        // Give the forwarder's initial `Broker::subscribe` a moment to
        // register before anything is published, so what follows is a
        // genuine live-then-lag sequence rather than a race against a
        // still-empty broker.
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Nobody drains `outgoing_rx` during this - the forwarder fills
        // the channel, blocks on the next send, and stops polling its
        // broadcast receiver entirely until draining starts below.
        publish_sequence(&broker, &topic, TOTAL).await;

        let mut received = Vec::with_capacity(TOTAL as usize);
        while received.len() < TOTAL as usize {
            let envelope = tokio::time::timeout(Duration::from_secs(5), outgoing_rx.recv())
                .await
                .expect("timed out waiting for a delivery")
                .expect("forwarder's outgoing channel closed early");
            received.push(sequence_of(&envelope));
        }

        assert_eq!(
            received,
            (0..TOTAL).collect::<Vec<_>>(),
            "every envelope should have arrived exactly once, in order"
        );
    }

    /// A gap larger than the replay buffer's headroom is still only
    /// *partially* recoverable, exactly as ADR-0024 documents: whatever
    /// the outgoing channel already absorbed before blocking still
    /// arrives, but the unrecoverable remainder is dropped rather than
    /// guessed at - never re-delivering something already sent live.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_lagged_forwarder_beyond_the_buffers_headroom_loses_the_unrecoverable_remainder() {
        let broker = Arc::new(Broker::new());
        let topic = Topic::from_str("weather.updates").unwrap();
        let filter: TopicFilter = topic.clone().into();
        const CHANNEL_CAPACITY: usize = 8;
        const TOTAL: u32 = 2000;

        let (outgoing_tx, mut outgoing_rx) = mpsc::channel(CHANNEL_CAPACITY);
        let _handle = spawn_forwarder(&broker, filter, outgoing_tx, Metrics::new());
        tokio::time::sleep(Duration::from_millis(20)).await;

        publish_sequence(&broker, &topic, TOTAL).await;

        let mut received = Vec::new();
        // Nothing more arrives once this times out - recovery already
        // gave up on the unrecoverable remainder.
        while let Ok(Some(envelope)) =
            tokio::time::timeout(Duration::from_millis(500), outgoing_rx.recv()).await
        {
            received.push(sequence_of(&envelope));
        }

        for pair in received.windows(2) {
            assert!(
                pair[0] < pair[1],
                "delivery should never reorder or duplicate: {received:?}"
            );
        }
        assert!(
            (received.len() as u32) < TOTAL,
            "a gap this large should exceed the buffer's headroom: {received:?}"
        );
        assert!(
            !received.is_empty(),
            "whatever fit in the outgoing channel before it blocked should still arrive"
        );
    }

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
