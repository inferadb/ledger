//! Per-peer bounded sender queue with drop-oldest overflow policy.
//!
//! Each peer owns a [`PeerSender`] that maintains a bounded queue of
//! outbound consensus messages. On overflow, the oldest message is dropped
//! to make room for the new one — Raft semantics tolerate dropped
//! heartbeats because retries arrive on the next heartbeat cycle.
//!
//! The drain task holds one long-lived bidirectional gRPC stream
//! (`Replicate`) per peer and feeds the queue's contents into
//! it. A concurrent ack-reader discards server responses to keep the
//! HTTP/2 flow-control window open. On any stream error we tear the stream
//! down and reconnect with exponential backoff (100ms → cap 5s), resetting
//! after a successful stream open. A single drain task per peer preserves
//! ordering and avoids unbounded tokio task spawning.

use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use inferadb_ledger_consensus::transport::OutboundMessage;
use parking_lot::Mutex;
use tokio::{sync::Notify, task::JoinHandle};
use tokio_stream::{StreamExt, wrappers::ReceiverStream};
use tokio_util::sync::CancellationToken;
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
    pub(super) shutdown_token: CancellationToken,
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
            shutdown_token: CancellationToken::new(),
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
        self.inner.shutdown_token.cancel();

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

/// Initial reconnect backoff after a stream failure.
const INITIAL_BACKOFF: Duration = Duration::from_millis(100);

/// Maximum reconnect backoff cap.
const MAX_BACKOFF: Duration = Duration::from_secs(5);

/// Applies equal-jitter to a backoff value before sleeping. Breaks client
/// synchronization when many peers reconnect after a shared upstream event
/// (e.g. a server restart). The backoff progression itself remains
/// deterministic — only the actual sleep for any given attempt is randomized
/// within `[0.5 * d, 1.5 * d)`.
fn jittered(d: Duration) -> Duration {
    use rand::RngExt;
    let factor = rand::rng().random_range(0.5_f64..1.5);
    Duration::from_secs_f64(d.as_secs_f64() * factor)
}

/// Build a protobuf request for a single outbound consensus message.
///
/// Returns `None` if postcard serialization fails — the message is
/// dropped (logged at debug) rather than failing the whole stream,
/// mirroring the fire-and-forget semantics of pre-streaming behavior.
fn build_consensus_request(
    msg: OutboundMessage,
    from_node: u64,
    from_address: &str,
    region: inferadb_ledger_types::Region,
) -> Option<inferadb_ledger_proto::proto::ConsensusEnvelope> {
    match postcard::to_allocvec(&msg.msg) {
        Ok(payload) => Some(inferadb_ledger_proto::proto::ConsensusEnvelope {
            shard_id: msg.shard.0,
            from_node,
            region: Some(inferadb_ledger_proto::proto::Region::from(region) as i32),
            payload,
            from_address: from_address.to_string(),
            cluster_id: 0,
        }),
        Err(e) => {
            tracing::debug!(error = %e, "Failed to serialize consensus message; dropping");
            None
        },
    }
}

/// Discards every queued message, attributing each to `reason` in drop metrics.
/// Called when we have no live stream to deliver on — Raft will retransmit
/// on the next heartbeat.
fn drop_queue(inner: &Arc<Inner>, reason: &'static str) {
    let dropped = {
        let mut q = inner.queue.lock();
        let n = q.len();
        q.clear();
        inner.depth.store(0, Ordering::Relaxed);
        n
    };
    if dropped > 0 {
        inner.dropped_count.fetch_add(dropped, Ordering::Relaxed);
        for _ in 0..dropped {
            crate::metrics::record_peer_send_drop(inner.peer_id, reason);
        }
        crate::metrics::record_peer_send_queue_depth(inner.peer_id, 0);
    }
}

/// Wait for `d` or until shutdown is cancelled. Returns `true` if
/// shutdown was observed (caller should exit), `false` if the sleep elapsed.
///
/// Uses [`CancellationToken::cancelled`] which returns immediately if the
/// token is already cancelled, avoiding the lost-notification race that
/// occurs with `AtomicBool` + `Notify`.
async fn wait_with_shutdown(inner: &Arc<Inner>, d: Duration) -> bool {
    tokio::select! {
        () = tokio::time::sleep(d) => inner.shutdown_token.is_cancelled(),
        () = inner.shutdown_token.cancelled() => true,
    }
}

async fn run_drain_loop(
    inner: Arc<Inner>,
    channel: Channel,
    region: inferadb_ledger_types::Region,
    from_node: u64,
    from_address: String,
) {
    let mut backoff = INITIAL_BACKOFF;

    'outer: loop {
        if inner.shutdown_token.is_cancelled() {
            break;
        }

        // Open a fresh bidi stream. A bounded mpsc provides our write side;
        // tonic turns the ReceiverStream into an HTTP/2 request body.
        let mut client = inferadb_ledger_proto::proto::raft_service_client::RaftServiceClient::new(
            channel.clone(),
        );
        let (req_tx, req_rx) = tokio::sync::mpsc::channel::<
            inferadb_ledger_proto::proto::ConsensusEnvelope,
        >(inner.capacity);
        let req_stream = ReceiverStream::new(req_rx);

        let ack_stream = match client.replicate(req_stream).await {
            Ok(resp) => resp.into_inner(),
            Err(e) => {
                tracing::debug!(
                    peer = inner.peer_id,
                    region = %region,
                    error = %e,
                    "Failed to open consensus stream; backing off before reconnect",
                );
                crate::metrics::record_peer_stream_reconnect(inner.peer_id);
                // Drop any messages currently queued — we have no stream to
                // deliver them on and Raft will retransmit from its own
                // state on the next heartbeat. Leaving them queued would
                // add head-of-line latency once we do reconnect, since
                // those messages reference an older term/log window.
                drop_queue(&inner, "stream_open_failed");
                if wait_with_shutdown(&inner, jittered(backoff)).await {
                    break 'outer;
                }
                backoff = (backoff * 2).min(MAX_BACKOFF);
                continue;
            },
        };

        // Reset on each successful stream open.
        backoff = INITIAL_BACKOFF;

        // Spawn the ack-consumer. It discards every response (fire-and-forget
        // semantics) while keeping the HTTP/2 flow-control window open. When
        // the server closes its half or any transport error occurs, the task
        // exits — the JoinHandle::await below observes this and drives
        // reconnect.
        let ack_peer = inner.peer_id;
        let ack_task = tokio::spawn(async move {
            let mut ack_stream = ack_stream;
            while let Some(result) = ack_stream.next().await {
                match result {
                    Ok(_) => {}, // discard
                    Err(e) => {
                        tracing::debug!(
                            peer = ack_peer,
                            error = %e,
                            "Consensus ack stream error",
                        );
                        break;
                    },
                }
            }
        });
        tokio::pin!(ack_task);
        // Tracks whether the pinned `ack_task` JoinHandle has already been
        // polled to completion via the `select!` below. Polling a completed
        // JoinHandle a second time panics in tokio internals
        // ("JoinHandle polled after completion"), so the post-loop teardown
        // must skip its own `.await` when this flag is set.
        let mut ack_consumed = false;

        // Inner drain loop. Exits on shutdown (outer loop exit), ack-task
        // completion (reconnect), or write-side error (reconnect).
        let stream_broken = loop {
            if inner.shutdown_token.is_cancelled() {
                break false; // full shutdown
            }

            // Snapshot the current queue. Holding the lock is momentary so
            // producers (push) are not blocked.
            let drained: Vec<OutboundMessage> = {
                let mut q = inner.queue.lock();
                let out = q.drain(..).collect();
                inner.depth.store(0, Ordering::Relaxed);
                out
            };

            if drained.is_empty() {
                crate::metrics::record_peer_send_queue_depth(inner.peer_id, 0);

                // Wait for either a push-notify, shutdown cancellation,
                // or ack-task completion (stream broken).
                tokio::select! {
                    biased;
                    () = inner.shutdown_token.cancelled() => break false,
                    () = inner.notify.notified() => continue,
                    res = &mut ack_task => {
                        // ack task exited — stream is broken.
                        ack_consumed = true;
                        if let Err(e) = res {
                            tracing::debug!(
                                peer = inner.peer_id,
                                error = %e,
                                "Ack task join error",
                            );
                        }
                        break true;
                    }
                }
            }

            crate::metrics::record_peer_send_queue_depth(inner.peer_id, 0);

            let mut broke = false;
            for msg in drained {
                let send_start = std::time::Instant::now();
                let Some(req) = build_consensus_request(msg, from_node, &from_address, region)
                else {
                    // Serialization failure — metric still useful for rate.
                    crate::metrics::record_peer_send_latency(
                        inner.peer_id,
                        send_start.elapsed().as_secs_f64(),
                    );
                    continue;
                };

                if let Err(e) = req_tx.send(req).await {
                    tracing::debug!(
                        peer = inner.peer_id,
                        region = %region,
                        error = %e,
                        "Consensus stream request channel closed; reconnecting",
                    );
                    broke = true;
                    break;
                }

                crate::metrics::record_peer_send_latency(
                    inner.peer_id,
                    send_start.elapsed().as_secs_f64(),
                );
            }

            if broke {
                break true;
            }
        };

        // Tear down write side; ack task will unwind next.
        drop(req_tx);

        if !stream_broken {
            // Full shutdown path — abort the ack task and exit.
            ack_task.abort();
            break 'outer;
        }

        // Stream-broken path: make sure ack task is finished (ignore result)
        // before reconnecting so we don't stack multiple readers. Skip the
        // await if the `select!` above already consumed the completion —
        // re-polling a completed JoinHandle panics in tokio internals.
        if !ack_consumed {
            let _ = ack_task.await;
        }

        // Discard anything that was enqueued after we last drained but
        // before the stream broke — those messages can't be delivered on
        // this (dead) stream, and Raft will retransmit from its own state.
        drop_queue(&inner, "stream_broken");

        crate::metrics::record_peer_stream_reconnect(inner.peer_id);
        if wait_with_shutdown(&inner, jittered(backoff)).await {
            break 'outer;
        }
        backoff = (backoff * 2).min(MAX_BACKOFF);
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
            shutdown_token: CancellationToken::new(),
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
    fn jitter_stays_within_bounds() {
        for _ in 0..100 {
            let d = jittered(Duration::from_secs(10));
            assert!(d >= Duration::from_secs(5));
            assert!(d < Duration::from_secs(15));
        }
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
