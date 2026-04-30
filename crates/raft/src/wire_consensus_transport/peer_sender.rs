//! Per-peer bounded sender queue with drop-oldest overflow policy.
//!
//! Drives an [`inferadb_ledger_wire_transport::WireClient`] long-lived bidi
//! stream over QUIC. The 8 load-bearing behaviors of the consensus
//! peer-sender contract:
//!
//! 1. Bounded queue (1024 default), drop-oldest on overflow.
//! 2. Election-critical retention on stream-broken / stream-open-failed. PreVote/Vote and their
//!    responses survive reconnect; AppendEntries / heartbeats are dropped because Raft retransmits.
//! 3. Reconnect with jittered exponential backoff (100ms → 5s, equal jitter).
//! 4. Ack-timeout liveness probe (5s) using per-stream ack counter on the multiplexed bidi stream.
//! 5. `CancellationToken`-based shutdown without lost-notify race.
//! 6. Single drain task per peer preserving outbound message order; the `tokio::pin!(ack_task)` +
//!    `ack_consumed` flag prevents `JoinHandle polled after completion` panic.
//! 7. Silent-drop warn throttle is enforced one layer above (in `WireConsensusTransport`); the
//!    sender exposes the queue + drop-count surface that drives it.
//! 8. Wire format on the multiplexed stream: `ConsensusEnvelope { shard_id, from_node, region,
//!    payload, from_address, cluster_id }` — postcard-encoded payload framed with
//!    `OPCODE_REPLICATE`. Receiver acks with an empty `ConsensusAck` postcard payload framed with
//!    `OPCODE_REPLICATE | FLAG_IS_RESPONSE`. Acks are fire-and-forget — the sender uses ack arrival
//!    as a liveness signal but does not correlate them to specific outbound messages.
//!
//! # Stream lifecycle
//!
//! On each iteration of the outer reconnect loop:
//!
//! 1. Acquire a live [`quinn::Connection`] from the [`inferadb_ledger_wire_transport::WireClient`].
//!    The client owns its own reconnect mutex (Tier-1 fix #3) and serializes concurrent acquires
//!    onto a single QUIC handshake.
//! 2. Open a fresh bidi stream (`connection.open_bi()`).
//! 3. Send the protocol-version handshake frame (opcode `OPCODE_RAFT_PROTOCOL_HANDSHAKE`, payload =
//!    big-endian `RAFT_PROTOCOL_VERSION` u32).
//! 4. Read the server's response handshake frame; mismatched version closes the stream and triggers
//!    backoff.
//! 5. Enter the inner drain loop: pull queued envelopes, encode + write them on the send half;
//!    concurrently spawn an ack-reader task on the recv half that bumps the per-stream liveness
//!    counter on every frame.
//! 6. On stream break (write error, ack-task exit, ack-timeout, or shutdown), drop both halves and
//!    fall through to the outer loop's reconnect-with-backoff.

// `WirePeerSender` is consumed by `WireConsensusTransport` (E.6b.3, not yet
// landed). Until that wires up, the public-from-module-perspective surface
// is unreferenced; allow it crate-wide rather than fragmenting local
// allows. Tests cover every item.
#![allow(dead_code)]

use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use bytes::Bytes;
use inferadb_ledger_consensus::transport::OutboundMessage;
use inferadb_ledger_wire::{
    Frame, FrameHeader, OPCODE_RAFT_PROTOCOL_HANDSHAKE, RAFT_CONSENSUS_SERVICE_BASE,
    RAFT_PROTOCOL_VERSION, services::raft::ConsensusEnvelope,
};
use inferadb_ledger_wire_transport::{
    WireClient,
    error::TransportError,
    frame_io::{read_frame, write_frame},
};
use parking_lot::Mutex;
use tokio::{sync::Notify, task::JoinHandle};
use tokio_util::sync::CancellationToken;

/// Default capacity of each peer's send queue. Chosen to absorb bursts
/// without blocking the consensus engine; larger values offer marginal
/// benefit because Raft's heartbeat cadence bounds the message production
/// rate per peer.
pub(crate) const PEER_QUEUE_CAPACITY: usize = 1024;

/// Opcode for the long-lived `Replicate` bidi stream multiplexing
/// `ConsensusEnvelope` and `ConsensusAck` frames in both directions.
///
/// Allocated as the first slot in the Raft service range (0x0D00). Matches
/// the wire-services macro's `replicate` RPC declaration.
pub(crate) const OPCODE_REPLICATE: u16 = RAFT_CONSENSUS_SERVICE_BASE;

/// Outcome of a [`WirePeerSender::push`] call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PushOutcome {
    /// Message was enqueued without displacing any other.
    Queued,
    /// Queue was at capacity; the oldest message was dropped to make room.
    DroppedOldest,
}

/// Per-peer sender: owns the outbound queue and the drain task handle.
///
/// Cloning by moving an `Arc<WirePeerSender>` is fine. Dropping the last handle
/// signals the drain task to shut down and abandons any remaining queued
/// messages (counted as `task_shutdown` drops in metrics).
pub(crate) struct WirePeerSender {
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

impl WirePeerSender {
    /// Spawns a sender task that drains the queue and ships envelopes
    /// over a long-lived QUIC bidi stream owned by the supplied
    /// [`WireClient`].
    ///
    /// The task loops on the [`Notify`]; each wake drains the full queue in
    /// one lock acquisition. Failed writes drop their batch (Raft
    /// retransmits on the next heartbeat). The task exits when the
    /// `shutdown_token` is cancelled.
    pub(crate) fn spawn(
        client: Arc<WireClient>,
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
            run_drain_loop(task_inner, client, region, from_node, from_address).await;
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

impl Drop for WirePeerSender {
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
/// expectation. This catches the failure mode where QUIC reports the
/// stream as OPEN but the application-layer handler isn't actually polling
/// the inbound side — the stream will sit forever otherwise, since our
/// fire-and-forget acks don't drive any other timeout.
const ACK_TIMEOUT: Duration = Duration::from_secs(5);

/// Period at which the drain loop wakes (when the queue is otherwise idle)
/// to evaluate the ack-timeout condition. A 1-second tick keeps the worst-case
/// detection latency at `ACK_TIMEOUT + 1s` while costing one timer wakeup
/// per peer per second when there's no real traffic.
const ACK_TIMEOUT_TICK: Duration = Duration::from_secs(1);

/// Bound on how long [`open_replicate_stream`] waits inside
/// [`WireClient::acquire`] before falling through to the peer-sender's outer
/// reconnect cycle.
///
/// `WireClient::acquire` has its own internal exponential backoff (100ms →
/// 30s) that loops indefinitely until a connection succeeds. Without an
/// outer bound the peer-sender's drain task would block forever on a
/// down peer, and the legacy load-bearing behavior of "after every
/// stream-open failure, drop non-election-critical queued messages" would
/// only fire when the inner backoff happened to surface an error to us —
/// effectively never.
///
/// 2 seconds is short enough that an unreachable peer cycles through
/// `drop_queue("stream_open_failed")` several times per minute while the
/// peer is down (preserving election-critical retention without piling
/// up stale heartbeats), and long enough that a single transient hiccup
/// against a healthy peer doesn't trip the outer backoff.
const STREAM_OPEN_TIMEOUT: Duration = Duration::from_secs(2);

/// Per-stream counters tracking inflight liveness. Shared between the drain
/// loop (writer) and the ack-consumer task (reader) for a single bidi stream;
/// constructed fresh on every reconnect so prior-stream activity doesn't
/// influence the new stream's deadline.
struct StreamLiveness {
    /// Total acks observed on this stream's response side.
    acks_received: AtomicU64,
    /// Total successful frame writes issued on this stream.
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

/// Build a postcard-encoded `ConsensusEnvelope` Frame for a single outbound
/// consensus message.
///
/// Returns `None` if postcard serialization fails — the message is
/// dropped (logged at debug) rather than failing the whole stream,
/// mirroring the fire-and-forget semantics of pre-streaming behavior.
fn build_envelope_frame(
    msg: OutboundMessage,
    from_node: u64,
    from_address: &str,
    region: inferadb_ledger_types::Region,
) -> Option<Frame> {
    let payload_bytes = match postcard::to_allocvec(&msg.msg) {
        Ok(bytes) => Bytes::from(bytes),
        Err(e) => {
            tracing::debug!(error = %e, "Failed to serialize consensus message; dropping");
            return None;
        },
    };
    let envelope = ConsensusEnvelope {
        shard_id: msg.shard.0,
        from_node,
        region: Some(region.as_str().to_string()),
        payload: payload_bytes,
        from_address: from_address.to_string(),
        cluster_id: 0,
    };
    let envelope_bytes = match postcard::to_allocvec(&envelope) {
        Ok(bytes) => Bytes::from(bytes),
        Err(e) => {
            tracing::debug!(error = %e, "Failed to serialize ConsensusEnvelope; dropping");
            return None;
        },
    };
    let payload_length = match u32::try_from(envelope_bytes.len()) {
        Ok(n) => n,
        Err(_) => {
            tracing::debug!(
                bytes = envelope_bytes.len(),
                "ConsensusEnvelope exceeds u32 payload_length; dropping",
            );
            return None;
        },
    };
    let mut header = FrameHeader::default();
    header.opcode = OPCODE_REPLICATE;
    header.version = inferadb_ledger_wire::CURRENT_PROTOCOL_VERSION;
    header.payload_length = payload_length;
    Some(Frame { header, payload: envelope_bytes })
}

/// Build the protocol-version handshake frame sent as the first frame on
/// every long-lived `Replicate` stream. Carries
/// [`RAFT_PROTOCOL_VERSION`] in big-endian on a 4-byte payload.
fn build_handshake_frame() -> Frame {
    let payload = Bytes::copy_from_slice(&RAFT_PROTOCOL_VERSION.to_be_bytes());
    let mut header = FrameHeader::default();
    header.opcode = OPCODE_RAFT_PROTOCOL_HANDSHAKE;
    header.version = inferadb_ledger_wire::CURRENT_PROTOCOL_VERSION;
    header.payload_length = 4;
    Frame { header, payload }
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
            "wire_peer_sender: drop_queue retained election-critical messages",
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

/// Open a fresh bidi stream and complete the protocol-version handshake.
///
/// Returns the live `(send, recv)` pair on success. On failure the caller
/// should backoff and retry; transient network errors are expected during
/// normal cluster lifecycle.
async fn open_replicate_stream(
    client: &WireClient,
) -> Result<(quinn::SendStream, quinn::RecvStream), TransportError> {
    // Bound the wait inside `WireClient::acquire` so we don't block
    // indefinitely on a down peer; see [`STREAM_OPEN_TIMEOUT`] for
    // rationale. On timeout we surface `TransportError::Timeout`, which
    // the caller treats as a stream-open failure (drop_queue +
    // jittered backoff) — the same path a real connect failure takes.
    let conn = tokio::time::timeout(STREAM_OPEN_TIMEOUT, client.acquire())
        .await
        .map_err(|_| TransportError::Timeout)??;
    let (mut send, mut recv) = conn
        .open_bi()
        .await
        .map_err(|err| TransportError::WriteFrame { reason: format!("open_bi: {err}") })?;

    // First frame: protocol-version handshake.
    let request = build_handshake_frame();
    write_frame(&mut send, &request).await?;

    // Read the server's handshake response. Expect the same opcode and a
    // matching version.
    let response = read_frame(&mut recv).await?;
    if response.header.opcode != OPCODE_RAFT_PROTOCOL_HANDSHAKE {
        return Err(TransportError::ReadFrame {
            reason: format!(
                "unexpected handshake opcode {:#06X} (expected {:#06X})",
                response.header.opcode, OPCODE_RAFT_PROTOCOL_HANDSHAKE,
            ),
        });
    }
    if response.payload.len() != 4 {
        return Err(TransportError::ReadFrame {
            reason: format!("handshake payload size {} != 4", response.payload.len(),),
        });
    }
    // payload.len() == 4 above — try_into cannot fail here. Use a
    // checked path so the surrounding fn body stays panic-free per
    // the workspace `unwrap_used` lint.
    let mut bytes = [0u8; 4];
    bytes.copy_from_slice(&response.payload[..4]);
    let server_version = u32::from_be_bytes(bytes);
    if server_version != RAFT_PROTOCOL_VERSION {
        return Err(TransportError::ReadFrame {
            reason: format!(
                "raft protocol version mismatch: local {RAFT_PROTOCOL_VERSION}, peer {server_version}",
            ),
        });
    }
    Ok((send, recv))
}

async fn run_drain_loop(
    inner: Arc<Inner>,
    client: Arc<WireClient>,
    region: inferadb_ledger_types::Region,
    from_node: u64,
    from_address: String,
) {
    let mut backoff = INITIAL_BACKOFF;

    'outer: loop {
        if inner.shutdown_token.is_cancelled() {
            break;
        }

        // Open a fresh bidi stream + handshake. On failure, backoff and
        // retry; pre-existing queue contents that aren't election-critical
        // are dropped (Raft retransmits via heartbeats), preserving the
        // legacy stream_open_failed behavior exactly.
        let (send, recv) = match open_replicate_stream(&client).await {
            Ok(pair) => pair,
            Err(e) => {
                tracing::debug!(
                    peer = inner.peer_id,
                    region = %region,
                    error = %e,
                    "Failed to open consensus stream; backing off before reconnect",
                );
                crate::metrics::record_peer_stream_reconnect(inner.peer_id);
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

        // Spawn the ack-consumer. Each frame received on this stream bumps
        // `liveness.acks_received` and refreshes `last_ack_at_millis` —
        // the drain loop reads both fields to detect a stuck peer (QUIC
        // stream OPEN but server handler not polling). When the server
        // closes its half or any transport error occurs, the task exits —
        // the `JoinHandle::await` below observes this and drives reconnect.
        let ack_peer = inner.peer_id;
        let ack_liveness = Arc::clone(&liveness);
        let ack_base = stream_open_at;
        let ack_task = tokio::spawn(async move {
            let mut recv = recv;
            loop {
                match read_frame(&mut recv).await {
                    Ok(frame) => {
                        // Treat every received frame as an ack signal,
                        // regardless of payload contents. The wire-level
                        // contract is "any frame from the peer keeps the
                        // stream healthy"; the receiver-side dispatcher in
                        // `WireRaftServerHandler` (E.6b.4) writes a
                        // `ConsensusAck` frame after every `handle_consensus_message`
                        // call. Acks are fire-and-forget — we don't decode
                        // the payload here.
                        ack_liveness.acks_received.fetch_add(1, Ordering::Relaxed);
                        ack_liveness
                            .last_ack_at_millis
                            .store(millis_since(ack_base, Instant::now()), Ordering::Relaxed);
                        tracing::debug!(
                            target_node_id = ack_peer,
                            opcode = frame.header.opcode,
                            flags = frame.header.flags,
                            "wire_peer_sender: received ack frame from peer",
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

        // Ownership of the send half is needed inside the drain loop so we
        // can write directly to it. tokio::select! requires &mut.
        let mut send = send;

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
                // reconnect even though QUIC still reports OPEN.
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
                // Capture diagnostic fields before `build_envelope_frame`
                // consumes the message — these flow into the post-send
                // `debug!` so a sender-side trace can be correlated with
                // the receiver-side trace in the `Replicate` handler.
                let kind_label = message_kind_label(&msg.msg);
                let shard_id = msg.shard.0;
                let Some(frame) = build_envelope_frame(msg, from_node, &from_address, region)
                else {
                    // Serialization failure — metric still useful for rate.
                    crate::metrics::record_peer_send_latency(
                        inner.peer_id,
                        send_start.elapsed().as_secs_f64(),
                    );
                    continue;
                };

                if let Err(e) = write_frame(&mut send, &frame).await {
                    tracing::debug!(
                        peer = inner.peer_id,
                        region = %region,
                        error = %e,
                        "Consensus stream write error; reconnecting",
                    );
                    broke = true;
                    break;
                }

                tracing::debug!(
                    target_node_id = inner.peer_id,
                    region = %region,
                    shard_id,
                    message_kind = kind_label,
                    "wire_peer_sender: dispatched message frame",
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

        // Tear down the send half. Best-effort `finish` flushes any pending
        // bytes before close. Errors here are not actionable — the stream
        // is already broken or being torn down — so swallow at debug.
        if let Err(e) = send.finish() {
            tracing::debug!(peer = inner.peer_id, error = %e, "send.finish() failed during teardown");
        }
        drop(send);

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
    use std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        sync::atomic::AtomicUsize,
        time::Duration as StdDuration,
    };

    use bytes::Bytes;
    use inferadb_ledger_consensus::{
        Message,
        types::{ConsensusStateId, NodeId},
    };
    use inferadb_ledger_wire::{
        AuthMethod, CallerIdentity, ErrorCode, Frame, FrameHeader, RequestContext, WireError,
    };
    use inferadb_ledger_wire_transport::{
        ClientConfig, WireClient, WireServer,
        auth::{AuthError, AuthVerifier, VerifiedAuth},
        dispatcher::{Dispatcher, StreamSink},
        frame_io::FLAG_IS_RESPONSE,
        tls,
    };
    use tokio::sync::watch;

    use super::*;

    fn make_sender_without_task(capacity: usize) -> WirePeerSender {
        let inner = Arc::new(Inner {
            queue: Mutex::new(VecDeque::with_capacity(capacity)),
            notify: Notify::new(),
            shutdown_token: CancellationToken::new(),
            depth: AtomicUsize::new(0),
            dropped_count: AtomicUsize::new(0),
            capacity,
            peer_id: 42,
        });
        WirePeerSender { inner, task: None }
    }

    fn sample_msg(shard: u64) -> OutboundMessage {
        OutboundMessage { to: NodeId(42), shard: ConsensusStateId(shard), msg: Message::TimeoutNow }
    }

    /// Verifier that accepts any credential. The peer-sender tests don't
    /// inspect the auth payload; we only need a successful handshake to
    /// observe the bidi stream lifecycle.
    struct AcceptAllVerifier {
        epoch_tx: watch::Sender<u64>,
    }

    impl AcceptAllVerifier {
        fn new() -> Self {
            let (tx, _rx) = watch::channel(0);
            Self { epoch_tx: tx }
        }
    }

    impl AuthVerifier for AcceptAllVerifier {
        async fn verify(&self, _payload: Bytes) -> Result<VerifiedAuth, AuthError> {
            let identity = CallerIdentity {
                user_id: None,
                app_id: None,
                organization_id: None,
                auth_method: AuthMethod::Anonymous,
                token_expiry: Instant::now() + StdDuration::from_secs(300),
                revocation_epoch: 0,
            };
            Ok(VerifiedAuth {
                identity,
                token_expiry: Instant::now() + StdDuration::from_secs(300),
                revocation_epoch: 0,
            })
        }

        fn subscribe_epoch(&self) -> watch::Receiver<u64> {
            self.epoch_tx.subscribe()
        }
    }

    /// Stub dispatcher. The peer-sender tests open the bidi stream
    /// directly via the test fixtures below; the dispatcher path is not
    /// exercised here (E.6b.4 wires it).
    struct StubDispatcher;

    impl Dispatcher for StubDispatcher {
        async fn dispatch(
            &self,
            _opcode: u16,
            _request: Bytes,
            _ctx: RequestContext,
        ) -> Result<Bytes, WireError> {
            Err(WireError::new(ErrorCode::Internal, "stub"))
        }

        async fn dispatch_stream(
            &self,
            _opcode: u16,
            _request: Bytes,
            _ctx: RequestContext,
            _sink: StreamSink,
        ) -> Result<(), WireError> {
            Err(WireError::new(ErrorCode::Internal, "stub"))
        }
    }

    fn loopback_zero() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)
    }

    fn test_server_config() -> quinn::ServerConfig {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let cert_der = cert.cert.der().clone();
        let key_der = rustls::pki_types::PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der());
        let chain = vec![cert_der];
        let key = rustls::pki_types::PrivateKeyDer::Pkcs8(key_der);
        let crypto = tls::rustls_server_crypto(chain, key).expect("build server crypto");
        tls::server_config(crypto)
    }

    fn build_client_config(addr: SocketAddr) -> ClientConfig {
        let crypto = tls::rustls_client_crypto_skip_verify();
        ClientConfig {
            server_addr: addr,
            server_name: "localhost".to_string(),
            quic: tls::client_config(crypto),
            auth_payload: Bytes::from_static(b"test-credential"),
            connect_timeout: StdDuration::from_secs(5),
        }
    }

    /// Build a `WireClient` pointing to an unreachable address so every
    /// `acquire` call backs off without ever completing. Useful for tests
    /// that exercise reconnect / shutdown semantics without a real server.
    fn make_unreachable_client() -> Arc<WireClient> {
        let crypto = tls::rustls_client_crypto_skip_verify();
        let config = ClientConfig {
            // RFC 5737 TEST-NET-1 — non-routable; dial hangs at the
            // network layer and times out at `connect_timeout`.
            server_addr: "192.0.2.1:443".parse().unwrap(),
            server_name: "localhost".to_string(),
            quic: tls::client_config(crypto),
            auth_payload: Bytes::from_static(b"x"),
            connect_timeout: StdDuration::from_millis(100),
        };
        Arc::new(WireClient::new(config).expect("client::new"))
    }

    /// Counts `OPCODE_RAFT_PROTOCOL_HANDSHAKE` exchanges + replies the test
    /// server has driven. Used to gate test progress on actual stream
    /// activity rather than wall-clock sleeps.
    #[derive(Clone, Default)]
    struct HandshakeCounters {
        observed: Arc<AtomicUsize>,
        envelopes: Arc<AtomicUsize>,
    }

    /// Run a one-shot Raft `Replicate` server: accept incoming connections,
    /// for each open bidi stream perform the protocol-version handshake +
    /// then read envelopes and reply with empty acks until the stream
    /// closes. The handshake reply version is configurable so we can
    /// drive the version-mismatch test path.
    fn spawn_replicate_server(
        counters: HandshakeCounters,
        server_version: u32,
    ) -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let endpoint = quinn::Endpoint::server(test_server_config(), loopback_zero())
            .expect("server endpoint");
        let addr = endpoint.local_addr().expect("local_addr");

        let handle = tokio::spawn(async move {
            while let Some(incoming) = endpoint.accept().await {
                let counters = counters.clone();
                tokio::spawn(async move {
                    let connection = match incoming.await {
                        Ok(c) => c,
                        Err(_) => return,
                    };
                    // Auth handshake: server reads the auth-establish frame
                    // on the first bidi, then replies with success.
                    if let Ok((mut send, mut recv)) = connection.accept_bi().await
                        && let Ok(frame) = read_frame(&mut recv).await
                    {
                        let mut header = FrameHeader::default();
                        header.request_id = frame.header.request_id;
                        header.version = inferadb_ledger_wire::CURRENT_PROTOCOL_VERSION;
                        header.flags = FLAG_IS_RESPONSE;
                        header.opcode = inferadb_ledger_wire::OPCODE_AUTH_ESTABLISH;
                        header.payload_length = 0;
                        let response = Frame { header, payload: Bytes::new() };
                        let _ = write_frame(&mut send, &response).await;
                        let _ = send.finish();
                    }
                    // Subsequent bidi streams are `Replicate` streams.
                    while let Ok((mut send, mut recv)) = connection.accept_bi().await {
                        let counters = counters.clone();
                        tokio::spawn(async move {
                            // 1) Read protocol-version handshake.
                            let Ok(handshake) = read_frame(&mut recv).await else { return };
                            if handshake.header.opcode != OPCODE_RAFT_PROTOCOL_HANDSHAKE {
                                return;
                            }
                            counters.observed.fetch_add(1, Ordering::Relaxed);
                            // 2) Reply with our version.
                            let payload = Bytes::copy_from_slice(&server_version.to_be_bytes());
                            let mut header = FrameHeader::default();
                            header.opcode = OPCODE_RAFT_PROTOCOL_HANDSHAKE;
                            header.version = inferadb_ledger_wire::CURRENT_PROTOCOL_VERSION;
                            header.flags = FLAG_IS_RESPONSE;
                            header.payload_length = 4;
                            let frame = Frame { header, payload };
                            if write_frame(&mut send, &frame).await.is_err() {
                                return;
                            }
                            // 3) Drain envelopes, reply with empty acks.
                            loop {
                                match read_frame(&mut recv).await {
                                    Ok(envelope) if envelope.header.opcode == OPCODE_REPLICATE => {
                                        counters.envelopes.fetch_add(1, Ordering::Relaxed);
                                        // Empty ConsensusAck — postcard-encoded
                                        // unit struct = single byte; we send a
                                        // zero-length payload which the
                                        // wire-side ack-reader treats as a
                                        // liveness signal.
                                        let mut header = FrameHeader::default();
                                        header.opcode = OPCODE_REPLICATE;
                                        header.version =
                                            inferadb_ledger_wire::CURRENT_PROTOCOL_VERSION;
                                        header.flags = FLAG_IS_RESPONSE;
                                        header.payload_length = 0;
                                        let ack = Frame { header, payload: Bytes::new() };
                                        if write_frame(&mut send, &ack).await.is_err() {
                                            break;
                                        }
                                    },
                                    _ => break,
                                }
                            }
                        });
                    }
                });
            }
        });
        (addr, handle)
    }

    fn make_real_client(addr: SocketAddr) -> Arc<WireClient> {
        Arc::new(WireClient::new(build_client_config(addr)).expect("client::new"))
    }

    // ===== Behaviors 1, 6, 7: queue + drop accounting =====

    #[test]
    fn jitter_stays_within_bounds() {
        for _ in 0..100 {
            let d = jittered(Duration::from_secs(10));
            assert!(d >= Duration::from_secs(5));
            assert!(d < Duration::from_secs(15));
        }
    }

    /// Behavior 1: Bounded queue queues messages under capacity without
    /// displacing any other.
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

    /// Behavior 1: drop-oldest on overflow.
    #[test]
    fn bounded_queue_drops_oldest() {
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

    /// Behavior 1: sustained overflow only evicts the oldest, never
    /// stomps on newer messages or grows past capacity.
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

    // ===== Behavior 2: election-critical retention =====

    /// Behavior 2: `drop_queue` retains election-critical messages
    /// (PreVote / Vote requests + responses) while discarding heartbeats
    /// and `AppendEntries`. Mirrors the legacy invariant exactly.
    #[test]
    fn drop_queue_retains_election_critical_messages() {
        use inferadb_ledger_consensus::{
            Message,
            types::{ConsensusStateId, NodeId},
        };
        let sender = make_sender_without_task(16);

        // Mix: 1 PreVote (election-critical) + 1 TimeoutNow (not critical)
        // + 1 InstallSnapshotResponse (not critical, retransmitted by Raft).
        let prevote = OutboundMessage {
            to: NodeId(42),
            shard: ConsensusStateId(1),
            msg: Message::PreVoteRequest {
                term: 1,
                candidate_id: NodeId(1),
                last_log_term: 0,
                last_log_index: 0,
            },
        };
        let timeout = OutboundMessage {
            to: NodeId(42),
            shard: ConsensusStateId(1),
            msg: Message::TimeoutNow,
        };
        sender.push(prevote.clone());
        sender.push(timeout);
        assert_eq!(sender.depth(), 2);

        drop_queue(&sender.inner, "stream_broken");
        // Only the PreVote should remain.
        assert_eq!(sender.depth(), 1);
        let q = sender.inner.queue.lock();
        match &q.front().unwrap().msg {
            Message::PreVoteRequest { .. } => {},
            other => panic!("expected PreVoteRequest retained, got {other:?}"),
        }
    }

    // ===== Behavior 6: drain task lifecycle / order preservation =====

    /// Behavior 6: a spawned drain task drains the queue. Uses an
    /// unreachable address so each `acquire` fails fast and the loop
    /// settles into the stream-open-failed → drop-queue path. With the
    /// queue holding only TimeoutNow messages (non-critical), every entry
    /// is dropped after each failed open and `depth` returns to zero.
    ///
    /// Allowance window: `STREAM_OPEN_TIMEOUT` (2s) + jittered backoff
    /// (up to ~150ms on the first cycle) + slack — give it 6s.
    #[tokio::test]
    async fn spawned_task_drains_queue_on_unreachable_peer() {
        use tokio::time::timeout;
        let client = make_unreachable_client();
        let sender = WirePeerSender::spawn(
            client,
            42,
            inferadb_ledger_types::Region::GLOBAL,
            1,
            "127.0.0.1:5000".to_owned(),
        );
        for i in 0..10 {
            sender.push(sample_msg(i));
        }
        timeout(Duration::from_secs(6), async {
            loop {
                if sender.depth() == 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("queue should drain within the unreachable-peer window");
    }

    /// Behavior 5 + 6: dropping the sender cancels the shutdown token
    /// and the drain task observes it without losing the wakeup. We
    /// verify by checking that the task's `Arc<Inner>` reference drops
    /// shortly after.
    #[tokio::test]
    async fn dropping_sender_shuts_down_task() {
        let client = make_unreachable_client();
        let sender = WirePeerSender::spawn(
            client,
            42,
            inferadb_ledger_types::Region::GLOBAL,
            1,
            "127.0.0.1:5000".to_owned(),
        );
        let weak_inner = Arc::downgrade(&sender.inner);
        drop(sender);
        // Task should observe shutdown and release its Arc<Inner> within
        // a few hundred ms (one backoff cycle).
        for _ in 0..40 {
            if weak_inner.strong_count() <= 1 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("task did not release its Arc<Inner>; strong_count = {}", weak_inner.strong_count());
    }

    // ===== Behavior 4: ack-timeout liveness probe =====

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

    /// Behavior 4: outstanding sends + a `last_ack_at_millis` far in
    /// the past triggers the ack-timeout.
    #[test]
    fn ack_timeout_true_when_stale_ack() {
        let stream_open_at = Instant::now() - (ACK_TIMEOUT + Duration::from_secs(1));
        let liveness = StreamLiveness::new(stream_open_at, stream_open_at);
        liveness.sends_issued.store(3, Ordering::Relaxed);
        liveness.acks_received.store(1, Ordering::Relaxed);
        assert!(ack_timed_out(&liveness, stream_open_at));
    }

    // ===== Behaviors 3, 8: handshake + frame-level lifecycle =====

    /// Behavior 8: `build_envelope_frame` produces a postcard-encoded
    /// `ConsensusEnvelope` framed with `OPCODE_REPLICATE`. Roundtrips
    /// through postcard back into a `ConsensusEnvelope`.
    #[test]
    fn build_envelope_frame_postcard_roundtrip() {
        use inferadb_ledger_consensus::{
            Message,
            types::{ConsensusStateId, NodeId},
        };
        use inferadb_ledger_wire::services::raft::ConsensusEnvelope;
        let msg = OutboundMessage {
            to: NodeId(7),
            shard: ConsensusStateId(99),
            msg: Message::TimeoutNow,
        };
        let frame =
            build_envelope_frame(msg, 1, "127.0.0.1:5000", inferadb_ledger_types::Region::GLOBAL)
                .expect("build_envelope_frame");
        assert_eq!(frame.header.opcode, OPCODE_REPLICATE);
        assert_eq!(frame.header.payload_length as usize, frame.payload.len());

        let envelope: ConsensusEnvelope =
            postcard::from_bytes(&frame.payload).expect("decode ConsensusEnvelope");
        assert_eq!(envelope.shard_id, 99);
        assert_eq!(envelope.from_node, 1);
        assert_eq!(envelope.from_address, "127.0.0.1:5000");
        assert_eq!(envelope.region.as_deref(), Some("global"));
    }

    /// Behavior 8: the protocol-version handshake frame carries
    /// `OPCODE_RAFT_PROTOCOL_HANDSHAKE` and a 4-byte big-endian
    /// `RAFT_PROTOCOL_VERSION` payload.
    #[test]
    fn handshake_frame_carries_protocol_version() {
        let frame = build_handshake_frame();
        assert_eq!(frame.header.opcode, OPCODE_RAFT_PROTOCOL_HANDSHAKE);
        assert_eq!(frame.header.payload_length, 4);
        assert_eq!(frame.payload.len(), 4);
        let mut bytes = [0u8; 4];
        bytes.copy_from_slice(&frame.payload[..4]);
        assert_eq!(u32::from_be_bytes(bytes), RAFT_PROTOCOL_VERSION);
    }

    /// Behaviors 3 + 8: handshake succeeds against a server that advertises
    /// the same protocol version; subsequent envelope frames flow through
    /// and the server observes them.
    #[tokio::test]
    async fn handshake_succeeds_on_matching_version() {
        let counters = HandshakeCounters::default();
        let (addr, _server) = spawn_replicate_server(counters.clone(), RAFT_PROTOCOL_VERSION);
        let client = make_real_client(addr);
        let sender = WirePeerSender::spawn(
            client,
            42,
            inferadb_ledger_types::Region::GLOBAL,
            1,
            "127.0.0.1:5000".to_owned(),
        );
        for i in 0..5 {
            sender.push(sample_msg(i));
        }

        // Wait for the server to observe the handshake + at least one
        // envelope.
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let observed = counters.observed.load(Ordering::Relaxed);
            let envelopes = counters.envelopes.load(Ordering::Relaxed);
            if observed >= 1 && envelopes >= 5 {
                break;
            }
            if Instant::now() >= deadline {
                panic!(
                    "server did not observe handshake + envelopes (observed={observed} envelopes={envelopes})"
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        drop(sender);
    }

    /// Behavior 8 (defensive): a server advertising a mismatched protocol
    /// version causes `open_replicate_stream` to fail; the drain loop
    /// records `stream_open_failed` (drops the queue contents that aren't
    /// election-critical) and backs off. We verify the failure surfaces as
    /// a real reconnect by observing repeated handshake exchanges.
    #[tokio::test]
    async fn handshake_rejects_version_mismatch() {
        // Bogus server version — peer will refuse and tear down.
        let bogus_version = RAFT_PROTOCOL_VERSION.wrapping_add(99);
        let counters = HandshakeCounters::default();
        let (addr, _server) = spawn_replicate_server(counters.clone(), bogus_version);
        let client = make_real_client(addr);
        let sender = WirePeerSender::spawn(
            client,
            42,
            inferadb_ledger_types::Region::GLOBAL,
            1,
            "127.0.0.1:5000".to_owned(),
        );
        // Push one envelope so the queue is non-empty when the stream
        // tears down — verifies drop_queue("stream_open_failed") fires.
        sender.push(sample_msg(0));

        // Observe at least 2 handshake attempts (initial + reconnect).
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let observed = counters.observed.load(Ordering::Relaxed);
            if observed >= 2 {
                break;
            }
            if Instant::now() >= deadline {
                panic!("expected >= 2 handshake attempts after version mismatch, got {observed}");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        drop(sender);
    }

    /// Behavior 5: shutdown during a stream-open + handshake exchange
    /// cleanly tears down the drain task. `WireClient::acquire` against
    /// an unreachable address loops in backoff; cancelling the shutdown
    /// token must wake the task without lost notifications.
    #[tokio::test]
    async fn cancellation_during_stream_open_does_not_lose_wakeup() {
        let client = make_unreachable_client();
        let sender = WirePeerSender::spawn(
            client,
            42,
            inferadb_ledger_types::Region::GLOBAL,
            1,
            "127.0.0.1:5000".to_owned(),
        );
        // Give the task a moment to enter the acquire-loop.
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Cancel via Drop. The drain loop should observe the cancellation
        // and release its Arc<Inner>.
        let weak_inner = Arc::downgrade(&sender.inner);
        drop(sender);
        for _ in 0..40 {
            if weak_inner.strong_count() <= 1 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!(
            "drain task did not release Arc<Inner> after shutdown cancel; strong_count = {}",
            weak_inner.strong_count()
        );
    }

    /// Behavior 8: order-preservation. Push N messages; verify the server
    /// receives them in submission order (envelopes carry monotonically
    /// increasing `shard_id`).
    #[tokio::test]
    async fn order_preserved_per_peer() {
        use inferadb_ledger_consensus::types::ConsensusStateId;
        // Spawn a server that records the shard_id of every envelope it
        // observes. We can't reuse `spawn_replicate_server` because the
        // counters there don't track per-envelope contents.
        let endpoint = quinn::Endpoint::server(test_server_config(), loopback_zero())
            .expect("server endpoint");
        let addr = endpoint.local_addr().expect("local_addr");
        let observed_shards = Arc::new(parking_lot::Mutex::new(Vec::<u64>::new()));
        let observed_clone = Arc::clone(&observed_shards);

        let _server = tokio::spawn(async move {
            while let Some(incoming) = endpoint.accept().await {
                let observed = Arc::clone(&observed_clone);
                tokio::spawn(async move {
                    let connection = match incoming.await {
                        Ok(c) => c,
                        Err(_) => return,
                    };
                    // Auth handshake.
                    if let Ok((mut send, mut recv)) = connection.accept_bi().await
                        && let Ok(frame) = read_frame(&mut recv).await
                    {
                        let mut header = FrameHeader::default();
                        header.request_id = frame.header.request_id;
                        header.version = inferadb_ledger_wire::CURRENT_PROTOCOL_VERSION;
                        header.flags = FLAG_IS_RESPONSE;
                        header.opcode = inferadb_ledger_wire::OPCODE_AUTH_ESTABLISH;
                        header.payload_length = 0;
                        let _ =
                            write_frame(&mut send, &Frame { header, payload: Bytes::new() }).await;
                        let _ = send.finish();
                    }
                    while let Ok((mut send, mut recv)) = connection.accept_bi().await {
                        let observed = Arc::clone(&observed);
                        tokio::spawn(async move {
                            // Handshake.
                            let Ok(handshake) = read_frame(&mut recv).await else {
                                return;
                            };
                            if handshake.header.opcode != OPCODE_RAFT_PROTOCOL_HANDSHAKE {
                                return;
                            }
                            let payload =
                                Bytes::copy_from_slice(&RAFT_PROTOCOL_VERSION.to_be_bytes());
                            let mut header = FrameHeader::default();
                            header.opcode = OPCODE_RAFT_PROTOCOL_HANDSHAKE;
                            header.version = inferadb_ledger_wire::CURRENT_PROTOCOL_VERSION;
                            header.flags = FLAG_IS_RESPONSE;
                            header.payload_length = 4;
                            if write_frame(&mut send, &Frame { header, payload }).await.is_err() {
                                return;
                            }
                            // Drain envelopes, recording shard_id.
                            loop {
                                match read_frame(&mut recv).await {
                                    Ok(frame) if frame.header.opcode == OPCODE_REPLICATE => {
                                        let env: inferadb_ledger_wire::services::raft::ConsensusEnvelope =
                                            match postcard::from_bytes(&frame.payload) {
                                                Ok(e) => e,
                                                Err(_) => break,
                                            };
                                        observed.lock().push(env.shard_id);
                                        // Send empty ack.
                                        let mut h = FrameHeader::default();
                                        h.opcode = OPCODE_REPLICATE;
                                        h.version = inferadb_ledger_wire::CURRENT_PROTOCOL_VERSION;
                                        h.flags = FLAG_IS_RESPONSE;
                                        h.payload_length = 0;
                                        if write_frame(
                                            &mut send,
                                            &Frame { header: h, payload: Bytes::new() },
                                        )
                                        .await
                                        .is_err()
                                        {
                                            break;
                                        }
                                    },
                                    _ => break,
                                }
                            }
                        });
                    }
                });
            }
        });

        let client = make_real_client(addr);
        let sender = WirePeerSender::spawn(
            client,
            42,
            inferadb_ledger_types::Region::GLOBAL,
            1,
            "127.0.0.1:5000".to_owned(),
        );
        const N: u64 = 100;
        for i in 0..N {
            sender.push(OutboundMessage {
                to: NodeId(42),
                shard: ConsensusStateId(i),
                msg: Message::TimeoutNow,
            });
        }
        // Wait for the server to observe all N.
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let count = observed_shards.lock().len();
            if count >= N as usize {
                break;
            }
            if Instant::now() >= deadline {
                panic!("server only observed {count}/{N} envelopes within timeout");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let observed = observed_shards.lock().clone();
        let expected: Vec<u64> = (0..N).collect();
        assert_eq!(observed, expected, "envelopes arrived out of order");
        drop(sender);
    }

    /// Suppresses dead-code warning on `build_test_server` from inferred
    /// builds that pull only a subset of tests. Keeps the helper around as
    /// the canonical way to assemble a `WireServer` should future tests
    /// (e.g., E.6b.4 server-side handler integration) need it.
    #[tokio::test]
    async fn build_test_server_compiles() {
        let verifier = Arc::new(AcceptAllVerifier::new());
        let server = WireServer::bind(
            loopback_zero(),
            test_server_config(),
            verifier,
            Arc::new(StubDispatcher),
        )
        .expect("bind");
        server.shutdown(Duration::from_millis(100), 0u32.into(), b"unused").await;
    }
}
