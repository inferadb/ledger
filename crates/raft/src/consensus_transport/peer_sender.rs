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
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
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

/// Maximum time to wait for an ack on an outstanding send before declaring
/// the bidi stream dead and forcing a reconnect.
///
/// Sized as a multiple of the typical Raft heartbeat round-trip plus
/// handler latency: a healthy peer acks within tens of milliseconds, so
/// a 5-second deadline is several orders of magnitude above the steady-state
/// expectation. This catches the failure mode where HTTP/2 reports the
/// stream as OPEN but the application-layer handler isn't actually polling
/// the inbound side — the stream will sit forever otherwise, since our
/// fire-and-forget acks don't drive any other timeout.
const ACK_TIMEOUT: Duration = Duration::from_secs(5);

/// Period at which the drain loop wakes (when the queue is otherwise idle)
/// to evaluate the ack-timeout condition. A 1-second tick keeps the worst-case
/// detection latency at `ACK_TIMEOUT + 1s` while costing one timer wakeup
/// per peer per second when there's no real traffic.
const ACK_TIMEOUT_TICK: Duration = Duration::from_secs(1);

/// Per-stream counters tracking inflight liveness. Shared between the drain
/// loop (writer) and the ack-consumer task (reader) for a single bidi stream;
/// constructed fresh on every reconnect so prior-stream activity doesn't
/// influence the new stream's deadline.
struct StreamLiveness {
    /// Total acks observed on this stream's response side.
    acks_received: AtomicU64,
    /// Total successful `req_tx.send`s issued on this stream.
    sends_issued: AtomicU64,
    /// Monotonic-clock millis at which the most recent ack arrived (or, on a
    /// fresh stream with no acks yet, the stream-open time). Used together
    /// with `acks_received < sends_issued` to detect a stuck peer: outstanding
    /// sends with no acks for `ACK_TIMEOUT` is treated as a dead stream.
    last_ack_at_millis: AtomicU64,
}

impl StreamLiveness {
    fn new(open_instant: Instant, base: Instant) -> Self {
        Self {
            acks_received: AtomicU64::new(0),
            sends_issued: AtomicU64::new(0),
            last_ack_at_millis: AtomicU64::new(millis_since(base, open_instant)),
        }
    }
}

/// Monotonic millisecond delta between `base` and `now`. Saturates at 0 if
/// `now < base` (cannot happen with `Instant` semantics, but defensive).
fn millis_since(base: Instant, now: Instant) -> u64 {
    u64::try_from(now.saturating_duration_since(base).as_millis()).unwrap_or(u64::MAX)
}

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

/// Static label for a [`Message`] variant. Used in `tracing` fields for
/// diagnostic correlation between sender and receiver — keeping the label
/// out of the serialized payload avoids extra wire cost while still letting
/// us identify which Raft message kind moved past which checkpoint in the
/// transport pipeline.
fn message_kind_label(msg: &inferadb_ledger_consensus::Message) -> &'static str {
    use inferadb_ledger_consensus::Message;
    match msg {
        Message::PreVoteRequest { .. } => "PreVoteRequest",
        Message::PreVoteResponse { .. } => "PreVoteResponse",
        Message::VoteRequest { .. } => "VoteRequest",
        Message::VoteResponse { .. } => "VoteResponse",
        Message::AppendEntries { .. } => "AppendEntries",
        Message::AppendEntriesResponse { .. } => "AppendEntriesResponse",
        Message::InstallSnapshot { .. } => "InstallSnapshot",
        Message::InstallSnapshotResponse { .. } => "InstallSnapshotResponse",
        Message::TimeoutNow => "TimeoutNow",
    }
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

/// Discards queued messages that the heartbeat timer will retransmit, while
/// preserving election-critical messages (PreVote / Vote requests and
/// responses) across the reconnect window.
///
/// Heartbeats and `AppendEntries` can be dropped safely because the leader's
/// heartbeat timer re-emits them every cycle — a transient stream failure
/// costs at most one heartbeat interval of replication latency.
/// Election-critical messages, by contrast, are emitted only when the
/// election timer fires (~hundreds of ms apart) and have no heartbeat-style
/// retransmit. Dropping them on every reconnect creates a liveness gap where
/// each election attempt's messages are discarded before the stream comes
/// back up, so the cluster never converges on a leader.
///
/// Called when we have no live stream to deliver on. Each dropped message is
/// attributed to `reason` in drop metrics; retained election-critical
/// messages stay in the queue and ship as soon as the next stream is open.
fn drop_queue(inner: &Arc<Inner>, reason: &'static str) {
    let (dropped, retained) = {
        let mut q = inner.queue.lock();
        let initial = q.len();
        q.retain(|msg| msg.msg.is_election_critical());
        let retained = q.len();
        let dropped = initial - retained;
        inner.depth.store(retained, Ordering::Relaxed);
        (dropped, retained)
    };
    if dropped > 0 {
        inner.dropped_count.fetch_add(dropped, Ordering::Relaxed);
        for _ in 0..dropped {
            crate::metrics::record_peer_send_drop(inner.peer_id, reason);
        }
    }
    if dropped > 0 || retained > 0 {
        tracing::debug!(
            peer = inner.peer_id,
            reason,
            dropped,
            retained,
            "peer_sender: drop_queue retained election-critical messages",
        );
    }
    crate::metrics::record_peer_send_queue_depth(inner.peer_id, retained);
}

/// True iff the stream has outstanding sends that haven't been acknowledged
/// for at least [`ACK_TIMEOUT`]. Returns `false` (no timeout) when the peer
/// has acked everything we've sent — even if the stream has been idle for a
/// long stretch — because there's no liveness signal to evaluate against
/// without an outstanding send.
///
/// `stream_open_at` is the [`Instant`] the bidi stream opened; it's used as
/// the `base` for the [`StreamLiveness::last_ack_at_millis`] atomic so the
/// initial value (set at open time) means "no acks yet" and the deadline is
/// measured from stream-open rather than from process start.
fn ack_timed_out(liveness: &StreamLiveness, stream_open_at: Instant) -> bool {
    let sends = liveness.sends_issued.load(Ordering::Relaxed);
    let acks = liveness.acks_received.load(Ordering::Relaxed);
    if acks >= sends {
        // Either no sends yet, or every send has been acked — nothing to
        // measure against.
        return false;
    }
    let last_ack_millis = liveness.last_ack_at_millis.load(Ordering::Relaxed);
    let now_millis = millis_since(stream_open_at, Instant::now());
    let elapsed_millis = now_millis.saturating_sub(last_ack_millis);
    elapsed_millis >= u64::try_from(ACK_TIMEOUT.as_millis()).unwrap_or(u64::MAX)
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

        // Per-stream liveness counters. Reset on every reconnect so prior
        // activity doesn't bias the new stream's ack-timeout deadline.
        let stream_open_at = Instant::now();
        let liveness = Arc::new(StreamLiveness::new(stream_open_at, stream_open_at));

        // Spawn the ack-consumer. Successful acks bump
        // `liveness.acks_received` and refresh `last_ack_at_millis` — the
        // drain loop reads both fields to detect a stuck peer (HTTP/2 stream
        // OPEN but server handler not polling). When the server closes its
        // half or any transport error occurs, the task exits — the
        // `JoinHandle::await` below observes this and drives reconnect.
        let ack_peer = inner.peer_id;
        let ack_liveness = Arc::clone(&liveness);
        let ack_base = stream_open_at;
        let ack_task = tokio::spawn(async move {
            let mut ack_stream = ack_stream;
            while let Some(result) = ack_stream.next().await {
                match result {
                    Ok(_) => {
                        ack_liveness.acks_received.fetch_add(1, Ordering::Relaxed);
                        ack_liveness
                            .last_ack_at_millis
                            .store(millis_since(ack_base, Instant::now()), Ordering::Relaxed);
                        tracing::debug!(
                            target_node_id = ack_peer,
                            "peer_sender: received ack from peer",
                        );
                    },
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
        // completion (reconnect), write-side error (reconnect), or
        // ack-timeout (sender-side liveness probe — see `ACK_TIMEOUT`).
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

                // Wait for a push-notify, shutdown cancellation, ack-task
                // completion (stream broken), or a periodic tick that lets
                // us evaluate the ack-timeout deadline. The ack-timeout
                // check is the sender-side liveness probe: if we have
                // outstanding sends with no acks for `ACK_TIMEOUT`, the
                // peer's stream task isn't actually polling and we should
                // reconnect even though HTTP/2 still reports OPEN.
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
                    () = tokio::time::sleep(ACK_TIMEOUT_TICK) => {
                        if ack_timed_out(&liveness, stream_open_at) {
                            tracing::warn!(
                                peer = inner.peer_id,
                                region = %region,
                                sends = liveness.sends_issued.load(Ordering::Relaxed),
                                acks = liveness.acks_received.load(Ordering::Relaxed),
                                timeout_ms = u64::try_from(ACK_TIMEOUT.as_millis()).unwrap_or(u64::MAX),
                                "Consensus stream ack-timeout exceeded; reconnecting",
                            );
                            crate::metrics::record_peer_send_drop(inner.peer_id, "ack_timeout");
                            break true;
                        }
                        continue;
                    }
                }
            }

            crate::metrics::record_peer_send_queue_depth(inner.peer_id, 0);

            let mut broke = false;
            for msg in drained {
                let send_start = std::time::Instant::now();
                // Capture diagnostic fields before `build_consensus_request`
                // consumes the message — these flow into the post-send
                // `debug!` so a sender-side trace can be correlated with
                // the receiver-side trace in `RaftService::replicate`.
                let kind_label = message_kind_label(&msg.msg);
                let shard_id = msg.shard.0;
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

                tracing::debug!(
                    target_node_id = inner.peer_id,
                    region = %region,
                    shard_id,
                    message_kind = kind_label,
                    "peer_sender: dispatched message to req_tx",
                );

                liveness.sends_issued.fetch_add(1, Ordering::Relaxed);
                crate::metrics::record_peer_send_latency(
                    inner.peer_id,
                    send_start.elapsed().as_secs_f64(),
                );
            }

            if broke {
                break true;
            }

            // Evaluate the ack-timeout after a busy batch too. A loop that
            // keeps draining new pushes faster than `ACK_TIMEOUT_TICK` would
            // otherwise never park on the `select!` arm above and never
            // observe the deadline expiring.
            if ack_timed_out(&liveness, stream_open_at) {
                tracing::warn!(
                    peer = inner.peer_id,
                    region = %region,
                    sends = liveness.sends_issued.load(Ordering::Relaxed),
                    acks = liveness.acks_received.load(Ordering::Relaxed),
                    timeout_ms = u64::try_from(ACK_TIMEOUT.as_millis()).unwrap_or(u64::MAX),
                    "Consensus stream ack-timeout exceeded mid-drain; reconnecting",
                );
                crate::metrics::record_peer_send_drop(inner.peer_id, "ack_timeout");
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
        types::{ConsensusStateId, NodeId},
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
        OutboundMessage { to: NodeId(42), shard: ConsensusStateId(shard), msg: Message::TimeoutNow }
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

    /// `ack_timed_out` must be false for a freshly opened stream with no
    /// outstanding sends — there's nothing to be late on.
    #[test]
    fn ack_timeout_false_when_no_sends_outstanding() {
        let stream_open_at = Instant::now();
        let liveness = StreamLiveness::new(stream_open_at, stream_open_at);
        assert!(!ack_timed_out(&liveness, stream_open_at));

        // Same answer when sends == acks (everything has been acknowledged).
        liveness.sends_issued.store(7, Ordering::Relaxed);
        liveness.acks_received.store(7, Ordering::Relaxed);
        assert!(!ack_timed_out(&liveness, stream_open_at));
    }

    /// With outstanding sends but a recent ack, the stream is healthy.
    /// `last_ack_at_millis` is initialized at stream-open, so a stream
    /// opened "just now" with one outstanding send still has time on the
    /// clock before [`ACK_TIMEOUT`] elapses.
    #[test]
    fn ack_timeout_false_when_recent_ack() {
        let stream_open_at = Instant::now();
        let liveness = StreamLiveness::new(stream_open_at, stream_open_at);
        liveness.sends_issued.store(3, Ordering::Relaxed);
        liveness.acks_received.store(2, Ordering::Relaxed);
        assert!(!ack_timed_out(&liveness, stream_open_at));
    }

    /// Outstanding sends + a [`StreamLiveness::last_ack_at_millis`] far in
    /// the past triggers the ack-timeout.
    #[test]
    fn ack_timeout_true_when_stale_ack() {
        let stream_open_at = Instant::now();
        let liveness = StreamLiveness::new(stream_open_at, stream_open_at);
        liveness.sends_issued.store(3, Ordering::Relaxed);
        liveness.acks_received.store(1, Ordering::Relaxed);
        // Force `last_ack_at_millis` to "stream open time" (0 ms since open),
        // and let real time advance by ACK_TIMEOUT + slack. We can't time-warp
        // the monotonic clock, so use a stream_open_at in the past instead.
        let stream_open_at = stream_open_at - (ACK_TIMEOUT + Duration::from_secs(1));
        let liveness = StreamLiveness::new(stream_open_at, stream_open_at);
        liveness.sends_issued.store(3, Ordering::Relaxed);
        liveness.acks_received.store(1, Ordering::Relaxed);
        assert!(ack_timed_out(&liveness, stream_open_at));
    }
}
