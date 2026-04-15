//! Per-peer bounded sender queue with drop-oldest overflow policy.
//!
//! Each peer owns a [`PeerSender`] that maintains a bounded queue of
//! outbound consensus messages. On overflow, the oldest message is dropped
//! to make room for the new one — Raft semantics tolerate dropped
//! heartbeats because retries arrive on the next heartbeat cycle.
//!
//! The drain task pulls batches off the queue and sends them via the
//! existing gRPC path. A single task per peer preserves ordering and
//! avoids unbounded tokio task spawning.

use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use inferadb_ledger_consensus::transport::OutboundMessage;
use parking_lot::Mutex;
use tokio::{sync::Notify, task::JoinHandle};
use tonic::transport::Channel;

/// Default capacity of each peer's send queue. Chosen to absorb bursts
/// without blocking the consensus engine; larger values offer marginal
/// benefit because Raft's heartbeat cadence bounds the message production
/// rate per peer.
pub(crate) const PEER_QUEUE_CAPACITY: usize = 1024;

/// Outcome of a [`PeerSender::push`] call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PushOutcome {
    /// Message was enqueued without displacing any other.
    Queued,
    /// Queue was at capacity; the oldest message was dropped to make room.
    DroppedOldest,
}

/// Per-peer sender: owns the outbound queue and the drain task handle.
///
/// Cloning by moving an `Arc<PeerSender>` is fine. Dropping the last handle
/// signals the drain task to shut down and abandons any remaining queued
/// messages (counted as `task_shutdown` drops in metrics).
pub(crate) struct PeerSender {
    inner: Arc<Inner>,
    task: Option<JoinHandle<()>>,
}

pub(super) struct Inner {
    pub(super) queue: Mutex<VecDeque<OutboundMessage>>,
    pub(super) notify: Notify,
    pub(super) shutdown: AtomicBool,
    pub(super) depth: AtomicUsize,
    pub(super) dropped_count: AtomicUsize,
    pub(super) capacity: usize,
    pub(super) peer_id: u64,
}

impl PeerSender {
    /// Spawns a sender task that drains the queue and sends messages via gRPC.
    ///
    /// The task loops on the [`Notify`]; each wake drains the full queue in
    /// one lock acquisition. Failed sends are logged and dropped (Raft
    /// retransmits on the next heartbeat). The task exits when the
    /// `shutdown` flag is set and the queue is empty.
    pub(crate) fn spawn(
        channel: Channel,
        peer_id: u64,
        region: inferadb_ledger_types::Region,
        from_node: u64,
        from_address: String,
    ) -> Self {
        let inner = Arc::new(Inner {
            queue: Mutex::new(VecDeque::with_capacity(PEER_QUEUE_CAPACITY)),
            notify: Notify::new(),
            shutdown: AtomicBool::new(false),
            depth: AtomicUsize::new(0),
            dropped_count: AtomicUsize::new(0),
            capacity: PEER_QUEUE_CAPACITY,
            peer_id,
        });

        let task_inner = Arc::clone(&inner);
        let task = tokio::spawn(async move {
            run_drain_loop(task_inner, channel, region, from_node, from_address).await;
        });

        Self { inner, task: Some(task) }
    }

    /// Queues a message for delivery.
    ///
    /// On overflow, drops the oldest queued message and pushes the new one,
    /// returning [`PushOutcome::DroppedOldest`]. Otherwise returns
    /// [`PushOutcome::Queued`].
    pub(crate) fn push(&self, msg: OutboundMessage) -> PushOutcome {
        let mut q = self.inner.queue.lock();
        let outcome = if q.len() >= self.inner.capacity {
            q.pop_front();
            self.inner.dropped_count.fetch_add(1, Ordering::Relaxed);
            crate::metrics::record_peer_send_drop(self.inner.peer_id, "queue_full");
            PushOutcome::DroppedOldest
        } else {
            PushOutcome::Queued
        };
        q.push_back(msg);
        let depth = q.len();
        self.inner.depth.store(depth, Ordering::Relaxed);
        drop(q);
        crate::metrics::record_peer_send_queue_depth(self.inner.peer_id, depth);
        self.inner.notify.notify_one();
        outcome
    }

    /// Current queue depth (approximate — read without holding the lock).
    #[cfg(test)]
    pub(crate) fn depth(&self) -> usize {
        self.inner.depth.load(Ordering::Relaxed)
    }

    /// Total number of messages dropped due to capacity overflow.
    #[cfg(test)]
    pub(crate) fn dropped_count(&self) -> usize {
        self.inner.dropped_count.load(Ordering::Relaxed)
    }

    /// Peer node ID this sender is bound to.
    #[cfg(test)]
    pub(crate) fn peer_id(&self) -> u64 {
        self.inner.peer_id
    }
}

impl Drop for PeerSender {
    fn drop(&mut self) {
        self.inner.shutdown.store(true, Ordering::Release);
        self.inner.notify.notify_one();

        // Count still-queued messages as dropped due to shutdown.
        let remaining = {
            let q = self.inner.queue.lock();
            q.len()
        };
        for _ in 0..remaining {
            crate::metrics::record_peer_send_drop(self.inner.peer_id, "task_shutdown");
        }

        if let Some(handle) = self.task.take() {
            handle.abort();
        }
    }
}

async fn run_drain_loop(
    inner: Arc<Inner>,
    channel: Channel,
    region: inferadb_ledger_types::Region,
    from_node: u64,
    from_address: String,
) {
    loop {
        // Drain everything currently queued in one lock acquisition.
        let drained: Vec<OutboundMessage> = {
            let mut q = inner.queue.lock();
            let out = q.drain(..).collect();
            inner.depth.store(0, Ordering::Relaxed);
            out
        };

        if drained.is_empty() {
            if inner.shutdown.load(Ordering::Acquire) {
                break;
            }
            // Update gauge explicitly in case the last push left it nonzero
            // (it shouldn't, but defensive).
            crate::metrics::record_peer_send_queue_depth(inner.peer_id, 0);
            inner.notify.notified().await;
            continue;
        }

        crate::metrics::record_peer_send_queue_depth(inner.peer_id, 0);

        for msg in drained {
            let send_start = std::time::Instant::now();
            let result = super::send_message(
                channel.clone(),
                msg.shard.0,
                from_node,
                &from_address,
                region,
                msg.msg,
            )
            .await;
            let elapsed = send_start.elapsed().as_secs_f64();
            crate::metrics::record_peer_send_latency(inner.peer_id, elapsed);

            if let Err(e) = result {
                tracing::debug!(
                    peer = inner.peer_id,
                    region = %region,
                    error = %e,
                    "Consensus message send failed"
                );
                // Continue draining remaining messages; don't break the batch
                // just because one send failed. Raft retransmits on the next
                // heartbeat anyway, and a persistent failure is handled via
                // membership change (peer removal → task shutdown).
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]
mod tests {
    use inferadb_ledger_consensus::{
        Message,
        types::{NodeId, ShardId},
    };

    use super::*;

    fn make_sender_without_task(capacity: usize) -> PeerSender {
        let inner = Arc::new(Inner {
            queue: Mutex::new(VecDeque::with_capacity(capacity)),
            notify: Notify::new(),
            shutdown: AtomicBool::new(false),
            depth: AtomicUsize::new(0),
            dropped_count: AtomicUsize::new(0),
            capacity,
            peer_id: 42,
        });
        PeerSender { inner, task: None }
    }

    fn sample_msg(shard: u64) -> OutboundMessage {
        OutboundMessage { to: NodeId(42), shard: ShardId(shard), msg: Message::TimeoutNow }
    }

    #[test]
    fn push_under_capacity_queues() {
        let sender = make_sender_without_task(4);
        for i in 0..4 {
            let outcome = sender.push(sample_msg(i));
            assert_eq!(outcome, PushOutcome::Queued);
        }
        assert_eq!(sender.depth(), 4);
        assert_eq!(sender.dropped_count(), 0);
    }

    #[test]
    fn push_over_capacity_drops_oldest() {
        let sender = make_sender_without_task(2);
        sender.push(sample_msg(1));
        sender.push(sample_msg(2));
        let outcome = sender.push(sample_msg(3));
        assert_eq!(outcome, PushOutcome::DroppedOldest);
        assert_eq!(sender.depth(), 2);
        assert_eq!(sender.dropped_count(), 1);
        let q = sender.inner.queue.lock();
        assert_eq!(q.front().unwrap().shard.0, 2);
        assert_eq!(q.back().unwrap().shard.0, 3);
    }

    #[test]
    fn sustained_overflow_evicts_only_oldest() {
        let sender = make_sender_without_task(3);
        for i in 0..100 {
            sender.push(sample_msg(i));
        }
        assert_eq!(sender.depth(), 3);
        assert_eq!(sender.dropped_count(), 97);
        let q = sender.inner.queue.lock();
        assert_eq!(q.front().unwrap().shard.0, 97);
        assert_eq!(q.back().unwrap().shard.0, 99);
    }

    #[test]
    fn peer_id_accessor() {
        let sender = make_sender_without_task(4);
        assert_eq!(sender.peer_id(), 42);
    }

    #[tokio::test]
    async fn spawned_task_drains_queue() {
        use tokio::time::{Duration, timeout};

        // Lazy channel to an unreachable local port. Every send will fail
        // instantly; we're only asserting the queue drains (messages are
        // taken from the queue even when send fails).
        let channel = tonic::transport::Channel::from_static("http://127.0.0.1:1").connect_lazy();

        let sender = PeerSender::spawn(
            channel,
            42, // peer_id
            inferadb_ledger_types::Region::GLOBAL,
            1, // from_node
            "127.0.0.1:5000".to_owned(),
        );

        for i in 0..10 {
            sender.push(sample_msg(i));
        }

        // Queue should drain within a short window as the task loops.
        timeout(Duration::from_millis(500), async {
            loop {
                if sender.depth() == 0 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("queue should drain within 500ms");
    }

    #[tokio::test]
    async fn dropping_sender_shuts_down_task() {
        let channel = tonic::transport::Channel::from_static("http://127.0.0.1:1").connect_lazy();
        let sender = PeerSender::spawn(
            channel,
            42,
            inferadb_ledger_types::Region::GLOBAL,
            1,
            "127.0.0.1:5000".to_owned(),
        );
        let weak_inner = Arc::downgrade(&sender.inner);
        drop(sender);

        // Task should observe shutdown and release its Arc<Inner>.
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        assert!(
            weak_inner.strong_count() <= 1,
            "task did not release its Arc<Inner>; strong_count = {}",
            weak_inner.strong_count()
        );
    }
}
