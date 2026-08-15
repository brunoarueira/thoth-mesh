//! Per-connection handling: reads framed envelopes off a socket,
//! dispatches them to the broker, and forwards broadcast deliveries back
//! out. See ADR-0007. Also propagates local topic-interest changes to
//! peer links; see ADR-0011.

use std::collections::HashMap;
use std::io::ErrorKind;
use std::sync::Arc;

use thoth_mesh::{Interest, PeerInfo};
use thoth_mesh_broker::Broker;
use thoth_mesh_core::async_framing;
use thoth_mesh_core::{Envelope, FramingError, MessageKind, PeerId, Topic};
use tokio::net::TcpStream;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};
use tracing::Instrument;

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
pub async fn handle_connection(socket: TcpStream, shared: Shared, initial_peer: Option<PeerInfo>) {
    let peer_addr = socket.peer_addr().ok();
    let span = tracing::info_span!("connection", peer = ?peer_addr);
    async move {
        run_connection(socket, shared, initial_peer).await;
    }
    .instrument(span)
    .await
}

async fn run_connection(socket: TcpStream, shared: Shared, initial_peer: Option<PeerInfo>) {
    let Shared {
        broker,
        membership,
        interest,
        peer_links,
        node_id,
        my_listen_addr,
    } = shared;
    let (reader, writer) = socket.into_split();
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
    }) = initial_peer
    {
        peer_identity = Some(peer_id);
        membership.mark_connected(peer_id, listen_addr);
        register_peer_link(&peer_links, &interest, node_id, peer_id, &outgoing_tx);
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
                        forwarders
                            .entry(topic.clone())
                            .or_insert_with(|| spawn_forwarder(&broker, topic.clone(), outgoing_tx.clone()));
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
                        peer_identity = Some(envelope.sender);
                        membership.mark_connected(envelope.sender, listen_addr.clone());
                        register_peer_link(&peer_links, &interest, node_id, envelope.sender, &outgoing_tx);
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
/// every topic this node is currently interested in - so a peer that
/// connects after interest was already established still learns about
/// it, not just future transitions (see ADR-0011).
///
/// Best-effort, like [`PeerLinks::broadcast`]: a catch-up `Subscribe`
/// that doesn't fit in the outgoing queue right now is dropped rather
/// than blocking the connection on it, on the assumption a channel
/// this backed up already has bigger problems.
fn register_peer_link(
    peer_links: &PeerLinks,
    interest: &Interest,
    node_id: PeerId,
    peer_id: PeerId,
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

/// A [`FramingError::Io`] whose kind is `UnexpectedEof` is what a
/// clean client disconnect looks like from the read side - not
/// something worth a warning.
fn is_clean_disconnect(err: &FramingError) -> bool {
    matches!(err, FramingError::Io(io_err) if io_err.kind() == ErrorKind::UnexpectedEof)
}

async fn send_envelope(writer: &mut Compat<OwnedWriteHalf>, envelope: &Envelope) -> bool {
    match envelope.to_bytes() {
        Ok(bytes) => async_framing::write_frame(writer, &bytes).await.is_ok(),
        Err(_) => false,
    }
}

fn spawn_forwarder(
    broker: &Arc<Broker>,
    topic: Topic,
    outgoing_tx: mpsc::Sender<Arc<Envelope>>,
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
}
