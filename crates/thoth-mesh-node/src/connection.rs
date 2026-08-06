//! Per-connection handling: reads framed envelopes off a socket,
//! dispatches them to the broker, and forwards broadcast deliveries back
//! out. See ADR-0007.

use std::collections::HashMap;
use std::io::ErrorKind;
use std::sync::Arc;

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

const OUTGOING_CHANNEL_CAPACITY: usize = 64;

/// Handles a single accepted connection until it disconnects or a
/// framing/decode error closes it.
///
/// `node_id` identifies this node as the sender on any envelope it
/// originates (acks). It does not affect envelopes forwarded from
/// other publishers, which keep their original sender.
pub async fn handle_connection(socket: TcpStream, broker: Arc<Broker>, node_id: PeerId) {
    let peer_addr = socket.peer_addr().ok();
    let span = tracing::info_span!("connection", peer = ?peer_addr);
    async move {
        run_connection(socket, broker, node_id).await;
    }
    .instrument(span)
    .await
}

async fn run_connection(socket: TcpStream, broker: Arc<Broker>, node_id: PeerId) {
    let (reader, writer) = socket.into_split();
    let mut reader = reader.compat();
    let mut writer = writer.compat_write();
    let (outgoing_tx, mut outgoing_rx) = mpsc::channel::<Arc<Envelope>>(OUTGOING_CHANNEL_CAPACITY);
    let mut forwarders: HashMap<Topic, JoinHandle<()>> = HashMap::new();

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
                        forwarders
                            .entry(topic.clone())
                            .or_insert_with(|| spawn_forwarder(&broker, topic, outgoing_tx.clone()));
                        let ack = Envelope::new(node_id, MessageKind::Ack { in_reply_to: envelope.id });
                        if !send_envelope(&mut writer, &ack).await {
                            break;
                        }
                    }
                    MessageKind::Unsubscribe { topic } => {
                        tracing::info!(sender = ?envelope.sender, %topic, "unsubscribed");
                        if let Some(handle) = forwarders.remove(topic) {
                            handle.abort();
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
    }
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
