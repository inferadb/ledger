//! Batched cross-group Raft message delivery.
//!
//! [`MessageOutbox`] collects outgoing `AppendEntries` messages destined for the
//! same peer node across different Raft groups and delivers them in a single
//! `BatchSend` RPC. This reduces gRPC overhead when many Raft groups share
//! the same cluster topology.
//!
//! # Design
//!
//! Each enqueued message includes a [`tokio::sync::oneshot`] sender for returning
//! the per-group response. The outbox maintains a per-destination buffer that is
//! flushed either when:
//! - The buffer reaches `max_batch_size`, or
//! - The flush interval timer fires.
//!
//! When batching is disabled (the default), [`MessageOutbox::enqueue`] sends each
//! message immediately via an individual `AppendEntries` RPC -- no batching overhead.

use std::{sync::Arc, time::Duration};

/// Default per-peer message queue capacity.
const DEFAULT_PEER_QUEUE_CAPACITY: usize = 256;

use dashmap::DashMap;
use inferadb_ledger_proto::proto::{
    BatchRaftEntry, BatchRaftRequest, RaftAppendEntriesRequest, RaftAppendEntriesResponse,
    batch_raft_entry, batch_raft_entry_response, raft_service_client::RaftServiceClient,
};
use inferadb_ledger_types::config::BatchRaftConfig;
use tokio::sync::{mpsc, oneshot};
use tonic::transport::Channel;

/// A message pending delivery to a peer node.
struct PendingMessage {
    /// Region identifier for routing (proto enum i32, `None` = GLOBAL).
    region: Option<i32>,
    /// The `AppendEntries` request payload.
    request: RaftAppendEntriesRequest,
    /// Channel for returning the response to the caller.
    response_tx: oneshot::Sender<Result<RaftAppendEntriesResponse, tonic::Status>>,
}

/// Client factory function: resolves a node ID to a gRPC client channel.
type ClientFactory = Arc<dyn Fn(u64) -> Option<RaftServiceClient<Channel>> + Send + Sync>;

/// Batched cross-group Raft message outbox.
///
/// Collects `AppendEntries` messages per destination node and delivers them
/// either individually or in batches via `BatchSend`, depending on configuration.
pub struct MessageOutbox {
    /// Per-destination send channels for enqueuing messages.
    senders: DashMap<u64, mpsc::Sender<PendingMessage>>,
    /// Configuration controlling flush interval and batch size.
    config: BatchRaftConfig,
    /// Factory for obtaining gRPC clients to peer nodes.
    client_factory: ClientFactory,
}

impl MessageOutbox {
    /// Creates a new message outbox.
    ///
    /// The `client_factory` closure is called with a node ID and should return
    /// an existing or freshly-created `RaftServiceClient` for that peer, or
    /// `None` if the peer is unreachable.
    pub fn new(
        config: BatchRaftConfig,
        client_factory: impl Fn(u64) -> Option<RaftServiceClient<Channel>> + Send + Sync + 'static,
    ) -> Self {
        Self { senders: DashMap::new(), config, client_factory: Arc::new(client_factory) }
    }

    /// Enqueues an `AppendEntries` message for delivery to `target_node`.
    ///
    /// If batching is disabled, the message is sent immediately on the calling
    /// task. If batching is enabled, the message is buffered and flushed by
    /// a per-destination background task.
    ///
    /// Returns the `AppendEntries` response or a gRPC status error.
    pub async fn enqueue(
        &self,
        target_node: u64,
        region: Option<i32>,
        request: RaftAppendEntriesRequest,
    ) -> Result<RaftAppendEntriesResponse, tonic::Status> {
        if !self.config.enabled {
            return self.send_immediate(target_node, region, request).await;
        }

        let (response_tx, response_rx) = oneshot::channel();
        let msg = PendingMessage { region, request, response_tx };

        // Get or create the sender for this destination.
        let sender = self.get_or_create_sender(target_node);
        sender
            .send(msg)
            .await
            .map_err(|_| tonic::Status::internal("Outbox channel closed for target node"))?;

        response_rx.await.map_err(|_| tonic::Status::internal("Outbox response channel dropped"))?
    }

    /// Sends a single message immediately without batching.
    async fn send_immediate(
        &self,
        target_node: u64,
        region: Option<i32>,
        request: RaftAppendEntriesRequest,
    ) -> Result<RaftAppendEntriesResponse, tonic::Status> {
        let mut client = (self.client_factory)(target_node).ok_or_else(|| {
            tonic::Status::unavailable(format!("No client available for node {target_node}"))
        })?;

        // Set region on request if not already set.
        let mut req = request;
        if req.region.is_none() {
            req.region = region;
        }

        let response = client.append_entries(tonic::Request::new(req)).await?.into_inner();

        Ok(response)
    }

    /// Returns the sender for a destination node, spawning a flush task if needed.
    fn get_or_create_sender(&self, target_node: u64) -> mpsc::Sender<PendingMessage> {
        if let Some(sender) = self.senders.get(&target_node) {
            return sender.value().clone();
        }

        let (tx, rx) = mpsc::channel::<PendingMessage>(DEFAULT_PEER_QUEUE_CAPACITY);
        self.senders.insert(target_node, tx.clone());

        let client_factory = self.client_factory.clone();
        let flush_interval = Duration::from_millis(self.config.flush_interval_ms);
        let max_batch_size = self.config.max_batch_size;

        tokio::spawn(async move {
            flush_loop(target_node, rx, client_factory, flush_interval, max_batch_size).await;
        });

        tx
    }
}

/// Per-destination flush loop that batches and sends messages.
async fn flush_loop(
    target_node: u64,
    mut rx: mpsc::Receiver<PendingMessage>,
    client_factory: ClientFactory,
    flush_interval: Duration,
    max_batch_size: usize,
) {
    let mut buffer: Vec<PendingMessage> = Vec::with_capacity(max_batch_size);
    let mut interval = tokio::time::interval(flush_interval);
    // First tick fires immediately, skip it.
    interval.tick().await;

    loop {
        tokio::select! {
            msg = rx.recv() => {
                match msg {
                    Some(pending) => {
                        buffer.push(pending);
                        if buffer.len() >= max_batch_size {
                            flush_batch(target_node, &client_factory, &mut buffer).await;
                        }
                    }
                    None => {
                        // Channel closed -- flush remaining and exit.
                        if !buffer.is_empty() {
                            flush_batch(target_node, &client_factory, &mut buffer).await;
                        }
                        return;
                    }
                }
            }
            _ = interval.tick() => {
                if !buffer.is_empty() {
                    flush_batch(target_node, &client_factory, &mut buffer).await;
                }
            }
        }
    }
}

/// Flushes a batch of pending messages as a single `BatchSend` RPC.
async fn flush_batch(
    target_node: u64,
    client_factory: &ClientFactory,
    buffer: &mut Vec<PendingMessage>,
) {
    let batch: Vec<PendingMessage> = std::mem::take(buffer);
    if batch.is_empty() {
        return;
    }

    let mut client = match client_factory(target_node) {
        Some(c) => c,
        None => {
            // No client available -- fail all pending messages.
            let err =
                tonic::Status::unavailable(format!("No client available for node {target_node}"));
            for msg in batch {
                let _ = msg.response_tx.send(Err(err.clone()));
            }
            return;
        },
    };

    // Build the batch request.
    let entries: Vec<BatchRaftEntry> = batch
        .iter()
        .map(|msg| BatchRaftEntry {
            region: msg.region,
            message: Some(batch_raft_entry::Message::AppendEntries(msg.request.clone())),
        })
        .collect();

    let batch_request = BatchRaftRequest { entries };

    match client.batch_send(tonic::Request::new(batch_request)).await {
        Ok(response) => {
            let responses = response.into_inner().responses;
            for (msg, resp) in batch.into_iter().zip(responses.into_iter()) {
                let result = match resp.response {
                    Some(batch_raft_entry_response::Response::AppendEntries(ae_resp)) => {
                        Ok(ae_resp)
                    },
                    None => Err(tonic::Status::internal("Missing response in batch entry")),
                };
                let _ = msg.response_tx.send(result);
            }
        },
        Err(status) => {
            // Batch RPC failed -- propagate the error to all callers.
            for msg in batch {
                let _ = msg.response_tx.send(Err(status.clone()));
            }
        },
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn outbox_default_config_is_disabled() {
        let config = BatchRaftConfig::default();
        assert!(!config.enabled);
    }

    #[tokio::test]
    async fn outbox_immediate_send_no_client_returns_unavailable() {
        let config = BatchRaftConfig::default(); // disabled = immediate mode
        let outbox = MessageOutbox::new(config, |_| None);

        let request = RaftAppendEntriesRequest {
            vote: None,
            prev_log_id: None,
            entries: vec![],
            leader_commit: None,
            region: None,
        };

        let result = outbox.enqueue(1, None, request).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::Unavailable);
    }

    #[tokio::test]
    async fn outbox_creates_sender_per_destination() {
        let config = BatchRaftConfig { enabled: true, flush_interval_ms: 100, max_batch_size: 10 };
        let outbox = MessageOutbox::new(config, |_| None);

        let _sender1 = outbox.get_or_create_sender(1);
        let _sender2 = outbox.get_or_create_sender(2);
        assert_eq!(outbox.senders.len(), 2);

        // Same node returns existing sender.
        let _sender1_again = outbox.get_or_create_sender(1);
        assert_eq!(outbox.senders.len(), 2);
    }
}
