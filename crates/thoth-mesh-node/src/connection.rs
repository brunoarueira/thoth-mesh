//! Per-connection handling: reads framed envelopes off a socket,
//! dispatches them to the broker, and forwards broadcast deliveries back
//! out. See ADR-0007.

use std::collections::HashMap;
use std::sync::Arc;

use thoth_mesh_broker::Broker;
use thoth_mesh_core::{Envelope, MessageKind, PeerId, Topic};
use tokio::io::AsyncWrite;
use tokio::net::TcpStream;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::framing;

const OUTGOING_CHANNEL_CAPACITY: usize = 64;

/// Handles a single accepted connection until it disconnects or a
/// framing/decode error closes it.
///
/// `node_id` identifies this node as the sender on any envelope it
/// originates (acks). It does not affect envelopes forwarded from
/// other publishers, which keep their original sender.
pub async fn handle_connection(socket: TcpStream, broker: Arc<Broker>, node_id: PeerId) {
    let (mut reader, mut writer) = socket.into_split();
    let (outgoing_tx, mut outgoing_rx) = mpsc::channel::<Arc<Envelope>>(OUTGOING_CHANNEL_CAPACITY);
    let mut forwarders: HashMap<Topic, JoinHandle<()>> = HashMap::new();

    loop {
        tokio::select! {
            frame = framing::read_frame(&mut reader) => {
                let Ok(bytes) = frame else { break };
                let Ok(envelope) = Envelope::from_bytes(&bytes) else { break };

                match &envelope.kind {
                    MessageKind::Subscribe { topic } => {
                        let topic = topic.clone();
                        forwarders
                            .entry(topic.clone())
                            .or_insert_with(|| spawn_forwarder(&broker, topic, outgoing_tx.clone()));
                        let ack = Envelope::new(node_id, MessageKind::Ack { in_reply_to: envelope.id });
                        if !send_envelope(&mut writer, &ack).await {
                            break;
                        }
                    }
                    MessageKind::Unsubscribe { topic } => {
                        if let Some(handle) = forwarders.remove(topic) {
                            handle.abort();
                        }
                        let ack = Envelope::new(node_id, MessageKind::Ack { in_reply_to: envelope.id });
                        if !send_envelope(&mut writer, &ack).await {
                            break;
                        }
                    }
                    MessageKind::Publish { topic, .. } => {
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
                    break;
                }
            }
        }
    }

    for (_, handle) in forwarders {
        handle.abort();
    }
}

async fn send_envelope<W: AsyncWrite + Unpin>(writer: &mut W, envelope: &Envelope) -> bool {
    match envelope.to_bytes() {
        Ok(bytes) => framing::write_frame(writer, &bytes).await.is_ok(),
        Err(_) => false,
    }
}

fn spawn_forwarder(
    broker: &Arc<Broker>,
    topic: Topic,
    outgoing_tx: mpsc::Sender<Arc<Envelope>>,
) -> JoinHandle<()> {
    let broker = Arc::clone(broker);
    tokio::spawn(async move {
        let mut rx = broker.subscribe(topic).await;
        loop {
            match rx.recv().await {
                Ok(envelope) => {
                    if outgoing_tx.send(envelope).await.is_err() {
                        break;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    })
}
