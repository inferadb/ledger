//! Multi-shard event loop.
//!
//! The [`Reactor`] is a single tokio task that receives events via an mpsc
//! channel, dispatches them to the appropriate [`ConsensusState`], collects [`Action`]
//! values, manages timer expirations, and performs periodic WAL flushes.

use std::{
    any::Any,
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use dashmap::DashMap;
use metrics::counter;
use tokio::sync::{mpsc, oneshot, watch};

use crate::{
    action::Action,
    clock::Clock,
    committed::{CommittedBatch, CommittedEntry},
    consensus_state::ConsensusState,
    error::ConsensusError,
    leadership::ShardState,
    message::Message,
    network_outbox::NetworkOutbox,
    rng::RngSource,
    snapshot_coordinator::{NoopSnapshotCoordinator, SnapshotCoordinator},
    snapshot_installer::{NoopSnapshotInstaller, SnapshotInstaller},
    snapshot_sender::{NoopSnapshotSender, SnapshotSender},
    timer::TimerWheel,
    transport::{NetworkTransport, OutboundMessage},
    types::{ConsensusStateId, MembershipChange, NodeId, TimerKind},
    wake::{NoopShardWakeNotifier, ShardWakeNotifier},
    wal_backend::{CheckpointFrame, FsyncPhase, WalBackend, WalFrame},
};

/// Lock-free map of paused shard IDs.
///
/// Entry presence (regardless of value) means the shard is paused; absence
/// means the shard is active. The value is unit so the type is essentially a
/// concurrent set. Shared between [`crate::ConsensusEngine`] and
/// [`Reactor`] via [`Arc`] so the engine can answer
/// [`is_shard_paused`](crate::ConsensusEngine::is_shard_paused) lock-free
/// while the reactor mutates membership through round-tripped events.
pub(crate) type PauseMap = DashMap<ConsensusStateId, ()>;

/// Events sent to the reactor via the inbox channel.
pub enum ReactorEvent {
    /// Propose a single entry to a shard.
    Propose {
        /// Target shard.
        shard: ConsensusStateId,
        /// Opaque entry data.
        data: Vec<u8>,
        /// Channel to send the proposal result back on.
        response: oneshot::Sender<Result<u64, ConsensusError>>,
    },
    /// Propose a batch of entries to a shard.
    ProposeBatch {
        /// Target shard.
        shard: ConsensusStateId,
        /// Batch of opaque entry data.
        entries: Vec<Vec<u8>>,
        /// Channel to send the proposal result back on.
        response: oneshot::Sender<Result<u64, ConsensusError>>,
    },
    /// Deliver a peer message to a shard.
    PeerMessage {
        /// Target shard.
        shard: ConsensusStateId,
        /// Sender node.
        from: NodeId,
        /// The Raft message.
        message: Message,
    },
    /// Membership change request.
    MembershipChange {
        /// Target shard.
        shard: ConsensusStateId,
        /// The membership change to apply.
        change: MembershipChange,
        /// Channel to send the result back on.
        response: oneshot::Sender<Result<(), ConsensusError>>,
    },
    /// Read the current committed index for a shard.
    ///
    /// Returns the shard's `commit_index` immediately without going through
    /// the Raft log. Use this for linearizable reads when paired with a
    /// leader-lease check, or for monotonic reads on followers.
    ReadIndex {
        /// Target shard.
        shard: ConsensusStateId,
        /// Channel to send the result back on.
        response: oneshot::Sender<Result<u64, ConsensusError>>,
    },
    /// Transfer leadership to a target voter node.
    ///
    /// Sends a `TimeoutNow` message to the target, causing it to immediately
    /// start an election. This node must be the leader and the target must
    /// be a voter in the current membership.
    TransferLeader {
        /// Target shard.
        shard: ConsensusStateId,
        /// Node to transfer leadership to.
        target: NodeId,
        /// Channel to send the result back on.
        response: oneshot::Sender<Result<(), ConsensusError>>,
    },
    /// Trigger a snapshot on a shard.
    ///
    /// Returns the snapshot metadata (last included index and term) if
    /// a snapshot was triggered, or `(0, 0)` if no snapshot was needed.
    TriggerSnapshot {
        /// Target shard.
        shard: ConsensusStateId,
        /// Channel to send the result back on.
        response: oneshot::Sender<Result<(u64, u64), ConsensusError>>,
    },
    /// Notify the reactor that a snapshot has been successfully persisted.
    ///
    /// The external coordinator sends this after writing the snapshot to disk.
    /// The reactor advances the shard's `last_snapshot_index` so future
    /// threshold checks use the correct baseline.
    SnapshotCompleted {
        /// Target shard.
        shard: ConsensusStateId,
        /// The last log index included in the completed snapshot.
        last_included_index: u64,
    },
    /// Notify the reactor that a leader-streamed snapshot has been
    /// successfully installed by the external installer.
    ///
    /// Sister of [`Self::SnapshotCompleted`]: that event is for snapshots
    /// produced locally; this event is for snapshots received from the
    /// leader (Stage 4 install path). Routes to
    /// [`crate::ConsensusState::handle_snapshot_installed`], which advances
    /// `last_applied`, `last_snapshot_index`, and `commit_index` to
    /// `last_included_index` and truncates the log prefix subsumed by the
    /// snapshot.
    SnapshotInstalled {
        /// Target shard.
        shard: ConsensusStateId,
        /// The last log index covered by the installed snapshot.
        last_included_index: u64,
        /// The term of the last log entry covered by the installed snapshot.
        last_included_term: u64,
    },
    /// Query a peer's match_index for the specified shard.
    QueryPeerState {
        /// Target shard.
        shard: ConsensusStateId,
        /// The peer node to query.
        node: NodeId,
        /// Channel to send the result back on.
        response: oneshot::Sender<Option<u64>>,
    },
    /// Flush the WAL for all shards before shutdown.
    ///
    /// Forces an immediate WAL sync so all committed proposals are durable.
    /// The reactor sends the result back on the oneshot channel.
    ShutdownFlush {
        /// Channel to acknowledge flush completion.
        ack: oneshot::Sender<Result<(), ConsensusError>>,
    },
    /// Externally assert a leader for a delegated shard.
    ///
    /// Used by the unified-leadership model — when a region coordinator's
    /// elected leader changes, every per-organization shard in
    /// [`crate::LeadershipMode::Delegated`] mode adopts the same leader
    /// without running its own election. The reactor routes this to
    /// [`crate::ConsensusState::adopt_leader`] on the target shard.
    AdoptLeader {
        /// Target shard.
        shard: ConsensusStateId,
        /// The asserted leader.
        leader: NodeId,
        /// The leader's term.
        term: u64,
    },
    /// Register a new shard at runtime.
    ///
    /// The shard is constructed externally (with its `LeadershipMode`
    /// already set via [`crate::ConsensusState::set_leadership_mode`]) and
    /// handed to the reactor; the reactor downcasts it to its concrete
    /// [`crate::ConsensusState<C, R>`] type, inserts it into the shard map,
    /// installs the state watcher, and processes initial actions (e.g.,
    /// scheduling the election timer).
    ///
    /// The `boxed_shard` must be a `Box<ConsensusState<C, R>>` whose
    /// generics match those of the running reactor. The downcast is a
    /// runtime check; a mismatch is logged and the registration is
    /// dropped.
    AddShard {
        /// The shard to register, type-erased. The reactor downcasts to
        /// its concrete `ConsensusState<C, R>` on receipt.
        boxed_shard: Box<dyn Any + Send>,
        /// ID of the shard — used to look up / guard against duplicate
        /// registration before the downcast runs.
        shard_id: ConsensusStateId,
        /// Watch sender for broadcasting the shard's state transitions.
        /// Dropped if the registration is rejected (duplicate / downcast
        /// failure), which invalidates the caller's receiver.
        state_tx: watch::Sender<ShardState>,
    },
    /// Mark a shard for graceful shutdown and drop it after a grace
    /// period.
    ///
    /// Pending proposals for the shard are rejected immediately with
    /// [`ConsensusError::NotLeader`]. Election / heartbeat timers are
    /// cancelled. A cleanup timer fires after 30 seconds to drop the
    /// shard from the reactor's maps (`shards`, `state_watchers`,
    /// `last_applied`).
    ///
    /// This is the inverse of [`ReactorEvent::AddShard`] and is the
    /// primitive used when a per-vault / per-organization shard is being
    /// decommissioned (as opposed to being removed from membership,
    /// which is driven by the [`crate::action::Action::ShardRemoved`]
    /// path).
    RemoveShard {
        /// Target shard.
        shard: ConsensusStateId,
    },
    /// Pause tick processing for a shard (O6 hibernation primitive).
    ///
    /// While paused, the reactor:
    ///   * skips delivery of expired timers for the shard,
    ///   * drops outbound [`Action::Send`] actions originating from the shard,
    ///   * drops incoming [`ReactorEvent::PeerMessage`] events for the shard after invoking the
    ///     registered [`ShardWakeNotifier`].
    ///
    /// Idempotent: pausing an already-paused shard is a no-op success.
    /// Returns [`ConsensusError::ShardUnavailable`] if the shard is not
    /// registered with the reactor.
    PauseShard {
        /// Target shard.
        shard: ConsensusStateId,
        /// Channel to send the result back on.
        response: oneshot::Sender<Result<(), ConsensusError>>,
    },
    /// Resume tick processing for a paused shard.
    ///
    /// Idempotent: resuming an already-active shard is a no-op success.
    /// Returns [`ConsensusError::ShardUnavailable`] if the shard is not
    /// registered with the reactor. Does NOT inject any synthetic timer or
    /// peer message — the shard re-arms timers via normal Raft state-machine
    /// transitions on the next legitimate event (heartbeat from the parent
    /// group's leader, application command, etc.).
    ResumeShard {
        /// Target shard.
        shard: ConsensusStateId,
        /// Channel to send the result back on.
        response: oneshot::Sender<Result<(), ConsensusError>>,
    },
    /// Graceful shutdown request.
    Shutdown,
}

/// The multi-shard event loop.
///
/// Receives [`ReactorEvent`]s, dispatches to shards, batches WAL writes,
/// and drives timer expirations. Generic over clock, RNG, and WAL backend
/// for deterministic simulation testing.
pub struct Reactor<C: Clock, R: RngSource, W: WalBackend, T: NetworkTransport> {
    shards: HashMap<ConsensusStateId, ConsensusState<C, R>>,
    timers: TimerWheel,
    /// Control-plane events (membership, snapshots, shutdown, peer messages,
    /// read-index). Polled with higher priority in the event loop so that a
    /// flood of proposals cannot starve leadership-critical traffic.
    control_inbox: mpsc::Receiver<ReactorEvent>,
    /// Proposal events (`Propose`, `ProposeBatch`). Kept separate from
    /// control so bulk write traffic has its own bounded queue and
    /// backpressure signal.
    proposal_inbox: mpsc::Receiver<ReactorEvent>,
    wal: W,
    clock: C,
    transport: T,
    commit_tx: mpsc::Sender<CommittedBatch>,
    pending_wal_frames: Vec<WalFrame>,
    outbox: NetworkOutbox,
    /// Intermediate buffer: (shard_id, up_to) pairs accumulated before flush.
    pending_commits: Vec<(ConsensusStateId, u64)>,
    /// Proposal responses awaiting quorum commit. Each entry is (shard, log_index, sender).
    /// Resolved only when the shard's commit_index >= log_index (quorum confirmation).
    /// Rejected with `NotLeader` if the shard loses leadership before commit.
    pending_responses: Vec<(ConsensusStateId, u64, oneshot::Sender<Result<u64, ConsensusError>>)>,
    flush_interval: Duration,
    /// Tracks the last index applied per shard so flush can collect the correct entry range.
    last_applied: HashMap<ConsensusStateId, u64>,
    /// Watch senders for broadcasting per-shard leadership state.
    state_watchers: HashMap<ConsensusStateId, watch::Sender<ShardState>>,
    /// Async fsync lifecycle phase — `Idle` until entries are submitted, then
    /// `Submitted` until the fsync completes (or immediately for sync backends).
    fsync_phase: FsyncPhase,
    /// Shards that were marked for explicit removal via
    /// [`ReactorEvent::RemoveShard`]. The cleanup timer unconditionally drops
    /// these from the reactor's maps, bypassing the "still in membership"
    /// restore-from-shutdown guard used for the membership-driven
    /// [`crate::action::Action::ShardRemoved`] path.
    force_removal_shards: HashSet<ConsensusStateId>,
    /// Lock-free map of paused shard IDs (O6 hibernation primitive).
    ///
    /// Shared with [`crate::ConsensusEngine`] via [`Arc`] so
    /// [`crate::ConsensusEngine::is_shard_paused`] can answer without
    /// round-tripping the reactor. Mutations always pass through a
    /// reactor event ([`ReactorEvent::PauseShard`] /
    /// [`ReactorEvent::ResumeShard`]) so the engine cannot mark a
    /// not-yet-registered shard as paused.
    pause_state: Arc<PauseMap>,
    /// Wake notifier invoked when a [`ReactorEvent::PeerMessage`] arrives for
    /// a paused shard. Default is [`NoopShardWakeNotifier`].
    wake_notifier: Arc<dyn ShardWakeNotifier>,
    /// Snapshot coordinator invoked when an
    /// [`Action::TriggerSnapshot`](crate::action::Action::TriggerSnapshot) is
    /// processed. Default is [`NoopSnapshotCoordinator`]. The raft crate
    /// installs a real implementation that builds and persists the
    /// snapshot off the reactor task.
    snapshot_coordinator: Arc<dyn SnapshotCoordinator>,
    /// Snapshot sender invoked when an
    /// [`Action::SendSnapshot`](crate::action::Action::SendSnapshot) is
    /// processed. Default is [`NoopSnapshotSender`]. The raft crate installs
    /// a real implementation that streams the at-rest encrypted snapshot
    /// bytes to the follower over the internal Raft transport's
    /// `InstallSnapshotStream` RPC. Stage 3 of the snapshot install path.
    snapshot_sender: Arc<dyn SnapshotSender>,
    /// Snapshot installer invoked when an
    /// [`Action::InstallSnapshot`](crate::action::Action::InstallSnapshot) is
    /// processed. Default is [`NoopSnapshotInstaller`]. The raft crate
    /// installs a real implementation that locates the staged snapshot file
    /// (Stage 3's receiver output), decrypts it, and feeds the plaintext
    /// through `RaftLogStore::install_snapshot`. Stage 4 of the snapshot
    /// install path.
    snapshot_installer: Arc<dyn SnapshotInstaller>,
}

impl<C: Clock + Clone, R: RngSource, W: WalBackend, T: NetworkTransport> Reactor<C, R, W, T> {
    /// Creates a new reactor.
    ///
    /// - `inbox` — channel receiving [`ReactorEvent`]s from external callers.
    /// - `wal` — WAL backend for durable persistence.
    /// - `clock` — injectable clock for deterministic testing.
    /// - `transport` — network transport for sending outbound Raft messages.
    /// - `commit_tx` — channel to notify the apply worker of committed batches.
    /// - `flush_interval` — how often to flush pending WAL frames and resolve responses.
    pub fn new(
        control_inbox: mpsc::Receiver<ReactorEvent>,
        proposal_inbox: mpsc::Receiver<ReactorEvent>,
        wal: W,
        clock: C,
        transport: T,
        commit_tx: mpsc::Sender<CommittedBatch>,
        flush_interval: Duration,
    ) -> Self {
        Self::with_all_callbacks(
            control_inbox,
            proposal_inbox,
            wal,
            clock,
            transport,
            commit_tx,
            flush_interval,
            Arc::new(PauseMap::new()),
            Arc::new(NoopShardWakeNotifier),
            Arc::new(NoopSnapshotCoordinator),
            Arc::new(NoopSnapshotSender),
            Arc::new(NoopSnapshotInstaller),
        )
    }

    /// Creates a reactor that shares its pause-state map with the surrounding
    /// [`crate::ConsensusEngine`] and delegates wake-on-paused-message events
    /// to the supplied [`ShardWakeNotifier`]. Installs the no-op snapshot
    /// coordinator — see [`Self::with_coordinators`] for the variant that
    /// accepts both notifiers.
    ///
    /// Used by [`crate::ConsensusEngine::start`] (and the
    /// `start_with_wake_notifier` overload). Tests typically use the simpler
    /// [`Reactor::new`] constructor, which installs a private pause map and
    /// no-op coordinators.
    #[allow(clippy::too_many_arguments)]
    pub fn with_wake_notifier(
        control_inbox: mpsc::Receiver<ReactorEvent>,
        proposal_inbox: mpsc::Receiver<ReactorEvent>,
        wal: W,
        clock: C,
        transport: T,
        commit_tx: mpsc::Sender<CommittedBatch>,
        flush_interval: Duration,
        pause_state: Arc<PauseMap>,
        wake_notifier: Arc<dyn ShardWakeNotifier>,
    ) -> Self {
        Self::with_all_callbacks(
            control_inbox,
            proposal_inbox,
            wal,
            clock,
            transport,
            commit_tx,
            flush_interval,
            pause_state,
            wake_notifier,
            Arc::new(NoopSnapshotCoordinator),
            Arc::new(NoopSnapshotSender),
            Arc::new(NoopSnapshotInstaller),
        )
    }

    /// Creates a reactor that shares its pause-state map with the surrounding
    /// [`crate::ConsensusEngine`] and delegates wake-on-paused-message events
    /// to the supplied [`ShardWakeNotifier`] and snapshot-trigger events to
    /// the supplied [`SnapshotCoordinator`].
    ///
    /// Used by [`crate::ConsensusEngine::start_with_coordinators`]. The
    /// snapshot coordinator's
    /// [`on_trigger_snapshot`](SnapshotCoordinator::on_trigger_snapshot) is
    /// invoked from the reactor task when an
    /// [`Action::TriggerSnapshot`] is processed; implementors MUST
    /// dispatch any I/O asynchronously and
    /// return immediately (the reactor's event loop is on hold until the
    /// callback returns).
    ///
    /// Installs the no-op snapshot sender — see
    /// [`Self::with_full_coordinators`] for the variant that accepts a
    /// real [`SnapshotSender`] for Stage 3 wire transfer.
    #[allow(clippy::too_many_arguments)]
    pub fn with_coordinators(
        control_inbox: mpsc::Receiver<ReactorEvent>,
        proposal_inbox: mpsc::Receiver<ReactorEvent>,
        wal: W,
        clock: C,
        transport: T,
        commit_tx: mpsc::Sender<CommittedBatch>,
        flush_interval: Duration,
        pause_state: Arc<PauseMap>,
        wake_notifier: Arc<dyn ShardWakeNotifier>,
        snapshot_coordinator: Arc<dyn SnapshotCoordinator>,
    ) -> Self {
        Self::with_all_callbacks(
            control_inbox,
            proposal_inbox,
            wal,
            clock,
            transport,
            commit_tx,
            flush_interval,
            pause_state,
            wake_notifier,
            snapshot_coordinator,
            Arc::new(NoopSnapshotSender),
            Arc::new(NoopSnapshotInstaller),
        )
    }

    /// Creates a reactor wired up with all three callbacks — wake notifier,
    /// snapshot coordinator, and snapshot sender.
    ///
    /// Used by [`crate::ConsensusEngine::start_with_full_coordinators`]. The
    /// snapshot sender's
    /// [`send_snapshot`](SnapshotSender::send_snapshot) is invoked from the
    /// reactor task when an [`Action::SendSnapshot`] is
    /// processed; implementors MUST dispatch any I/O asynchronously and
    /// return immediately (the reactor's event loop is on hold until the
    /// callback returns) — same contract as the wake notifier and snapshot
    /// coordinator.
    #[allow(clippy::too_many_arguments)]
    pub fn with_full_coordinators(
        control_inbox: mpsc::Receiver<ReactorEvent>,
        proposal_inbox: mpsc::Receiver<ReactorEvent>,
        wal: W,
        clock: C,
        transport: T,
        commit_tx: mpsc::Sender<CommittedBatch>,
        flush_interval: Duration,
        pause_state: Arc<PauseMap>,
        wake_notifier: Arc<dyn ShardWakeNotifier>,
        snapshot_coordinator: Arc<dyn SnapshotCoordinator>,
        snapshot_sender: Arc<dyn SnapshotSender>,
    ) -> Self {
        Self::with_all_callbacks(
            control_inbox,
            proposal_inbox,
            wal,
            clock,
            transport,
            commit_tx,
            flush_interval,
            pause_state,
            wake_notifier,
            snapshot_coordinator,
            snapshot_sender,
            Arc::new(NoopSnapshotInstaller),
        )
    }

    /// Creates a reactor wired up with all four callbacks — wake notifier,
    /// snapshot coordinator, snapshot sender, and snapshot installer.
    ///
    /// Used by [`crate::ConsensusEngine::start_with_all_callbacks`]. The
    /// snapshot installer's
    /// [`install_snapshot`](SnapshotInstaller::install_snapshot) is invoked
    /// from the reactor task when an [`Action::InstallSnapshot`]
    /// is processed; implementors MUST dispatch any I/O asynchronously and
    /// return immediately (the reactor's event loop is on hold until the
    /// callback returns) — same contract as the wake notifier, snapshot
    /// coordinator, and snapshot sender.
    #[allow(clippy::too_many_arguments)]
    pub fn with_all_callbacks(
        control_inbox: mpsc::Receiver<ReactorEvent>,
        proposal_inbox: mpsc::Receiver<ReactorEvent>,
        wal: W,
        clock: C,
        transport: T,
        commit_tx: mpsc::Sender<CommittedBatch>,
        flush_interval: Duration,
        pause_state: Arc<PauseMap>,
        wake_notifier: Arc<dyn ShardWakeNotifier>,
        snapshot_coordinator: Arc<dyn SnapshotCoordinator>,
        snapshot_sender: Arc<dyn SnapshotSender>,
        snapshot_installer: Arc<dyn SnapshotInstaller>,
    ) -> Self {
        Self {
            shards: HashMap::new(),
            timers: TimerWheel::new(),
            control_inbox,
            proposal_inbox,
            wal,
            clock,
            transport,
            commit_tx,
            pending_wal_frames: Vec::new(),
            outbox: NetworkOutbox::new(),
            pending_commits: Vec::new(),
            pending_responses: Vec::new(),
            flush_interval,
            last_applied: HashMap::new(),
            state_watchers: HashMap::new(),
            fsync_phase: FsyncPhase::Idle,
            force_removal_shards: HashSet::new(),
            pause_state,
            wake_notifier,
            snapshot_coordinator,
            snapshot_sender,
            snapshot_installer,
        }
    }

    /// Registers a shard with the reactor.
    ///
    /// If the shard has a non-zero `commit_index` (from WAL checkpoint
    /// recovery), the reactor initializes its `last_applied` tracking for
    /// this shard so committed entries up to that index are not
    /// re-dispatched to the apply worker.
    pub fn add_shard(&mut self, id: ConsensusStateId, shard: ConsensusState<C, R>) {
        let restored_commit = shard.commit_index();
        if restored_commit > 0 {
            self.last_applied.insert(id, restored_commit);
        }
        let actions = shard.initial_actions();
        self.shards.insert(id, shard);
        self.process_actions(actions);
    }

    /// Registers a watch sender for the given shard's leadership state.
    ///
    /// Called by the engine after adding a shard to wire up the watch channel
    /// created during engine startup.
    pub fn add_state_watcher(&mut self, shard: ConsensusStateId, tx: watch::Sender<ShardState>) {
        self.state_watchers.insert(shard, tx);
    }

    /// Returns the number of registered shards.
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// Runs the reactor event loop until a [`ReactorEvent::Shutdown`] is received.
    ///
    /// The select is biased toward control and proposal ingest so client
    /// traffic is picked up promptly, but periodic WAL flush and timer
    /// expiration processing run *outside* the select on every loop
    /// iteration. Handling them inside the biased select would let a
    /// saturated control or proposal arm starve them indefinitely — which
    /// would stall commit dispatch, outbound-message sends, and leadership
    /// timers under sustained catch-up traffic.
    pub async fn run(&mut self) {
        // DIAG (Task #153): record reactor entry so we can tell `tokio::spawn`
        // succeeded vs. the future being dropped immediately. Listed shard ids
        // help disambiguate the data-region engine (single shard, org_id=0)
        // from per-org engines (org shard + any vaults added via AddShard).
        let initial_shard_ids: Vec<u64> = self.shards.keys().map(|id| id.0).collect();
        tracing::debug!(
            shard_count = self.shards.len(),
            timer_count = self.timers.len(),
            shard_ids = ?initial_shard_ids,
            "reactor: Reactor::run started",
        );

        let mut flush_ticker = tokio::time::interval(self.flush_interval);
        flush_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // Consume the immediate first tick; the wall-clock guard below takes
        // over from here.
        flush_ticker.tick().await;

        // Real wall-clock tracking for flush gating. Intentionally independent
        // of `self.clock` (which can be a `SimulatedClock`) so simulation
        // tests that don't advance the injected clock still see flushes fire
        // on the tokio runtime's wall-clock timeline.
        let mut last_flush = std::time::Instant::now();

        // DIAG (Task #153): rate-limit the per-iteration tick log so we don't
        // drown the test trace under steady-state load.
        let mut last_tick_log = std::time::Instant::now();

        loop {
            // DIAG (Task #153): periodic tick log proves the reactor task is
            // alive and progressing through the select loop. Logging on a
            // 100ms cadence keeps the output readable across the 60s test
            // budget while still detecting the "task dead/blocked" failure
            // mode within a single GLOBAL-region election timeout window.
            if last_tick_log.elapsed() >= Duration::from_millis(100) {
                let next_deadline_ms = self.timers.next_deadline().map(|d| {
                    let now = self.clock.now();
                    if d <= now { 0i64 } else { d.duration_since(now).as_millis() as i64 }
                });
                tracing::debug!(
                    shard_count = self.shards.len(),
                    timer_count = self.timers.len(),
                    next_deadline_ms = ?next_deadline_ms,
                    pending_responses = self.pending_responses.len(),
                    pending_commits = self.pending_commits.len(),
                    "reactor: tick",
                );
                last_tick_log = std::time::Instant::now();
            }

            let sleep_duration = self.timers.next_deadline().map(|d| {
                let now = self.clock.now();
                if d <= now { Duration::ZERO } else { d.duration_since(now) }
            });

            let mut exit = false;

            tokio::select! {
                // `biased` polls arms in listed order every iteration so
                // control-plane events always win over proposals when both
                // are ready. This prevents a proposal flood from starving
                // leadership/membership traffic.
                biased;

                // 1) Control plane — highest priority (membership, snapshots,
                //    shutdown, peer messages, read-index).
                event = self.control_inbox.recv() => {
                    match event {
                        Some(ReactorEvent::Shutdown) | None => { exit = true; }
                        Some(ev) => {
                            let affected = self.handle_event(ev);
                            self.broadcast_shard_states(&affected);
                        }
                    }
                }

                // 2) Proposals — ranked above flush/timers so normal-load
                //    scheduling matches the pre-split reactor (client writes
                //    are picked up promptly). The priority split only
                //    protects control traffic from adversarial proposal
                //    saturation; during normal operation it is not intended
                //    to starve proposals.
                event = self.proposal_inbox.recv() => {
                    match event {
                        Some(ev) => {
                            let affected = self.handle_event(ev);
                            self.broadcast_shard_states(&affected);
                        }
                        None => { exit = true; }
                    }
                }

                // 3) Wake-up: periodic flush interval. The actual flush runs
                //    below (outside the select) so it cannot be starved by
                //    the biased ordering.
                _ = flush_ticker.tick() => {}

                // 4) Wake-up: next timer deadline. Expiration processing runs
                //    below (outside the select) for the same reason.
                _ = async {
                    match sleep_duration {
                        Some(d) if d.is_zero() => {},
                        Some(d) => tokio::time::sleep(d).await,
                        None => std::future::pending::<()>().await,
                    }
                } => {}
            }

            // Post-select work runs on EVERY iteration regardless of which
            // arm fired. Biased select cannot starve this.
            let affected = self.process_expired_timers();
            self.broadcast_shard_states(&affected);

            // Flush on interval or on exit. The wall-clock guard preserves
            // batching under heavy load — entries accumulate across events in
            // a single flush_interval window and sync together — and runs
            // unconditionally (outside the select) so control/proposal traffic
            // cannot starve commit dispatch or outbound-message delivery.
            let now = std::time::Instant::now();
            if exit || now.duration_since(last_flush) >= self.flush_interval {
                self.flush().await;
                last_flush = now;
            }

            if exit {
                // DIAG (Task #153): record reactor exit so we can tell
                // whether the run task ended cleanly (Shutdown event /
                // closed channel) vs. silently panicked. Hypothesis C
                // ("data-region reactor task dead post-restart") would
                // show this firing unexpectedly early.
                tracing::debug!(
                    shard_count = self.shards.len(),
                    timer_count = self.timers.len(),
                    "reactor: Reactor::run exiting",
                );
                break;
            }
        }
    }

    /// Dispatches a single event to the appropriate shard and collects actions.
    ///
    /// Returns the shard IDs affected by this event so the caller can broadcast
    /// updated state snapshots.
    fn handle_event(&mut self, event: ReactorEvent) -> Vec<ConsensusStateId> {
        match event {
            ReactorEvent::Propose { shard, data, response } => {
                let Some(s) = self.shards.get_mut(&shard) else {
                    let _ = response.send(Err(ConsensusError::ShardUnavailable { shard }));
                    return vec![];
                };
                if s.is_failed() {
                    let _ = response.send(Err(ConsensusError::ShardUnavailable { shard }));
                    return vec![];
                }
                match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    s.handle_propose(data)
                })) {
                    Ok(Ok(actions)) => {
                        let entry_index = actions
                            .iter()
                            .find_map(|a| match a {
                                Action::PersistEntries { entries, .. } => {
                                    entries.last().map(|e| e.index)
                                },
                                _ => None,
                            })
                            .unwrap_or(0);
                        self.pending_responses.push((shard, entry_index, response));
                        self.process_actions(actions);
                        vec![shard]
                    },
                    Ok(Err(e)) => {
                        let _ = response.send(Err(e));
                        vec![]
                    },
                    Err(payload) => {
                        let msg = panic_message(&payload);
                        tracing::error!(shard = shard.0, panic = %msg, "ConsensusState panicked — marking as Failed");
                        counter!("consensus_shard_panic_total", "shard_id" => shard.0.to_string())
                            .increment(1);
                        s.mark_failed();
                        let _ = response.send(Err(ConsensusError::ShardUnavailable { shard }));
                        vec![shard]
                    },
                }
            },
            ReactorEvent::ProposeBatch { shard, entries, response } => {
                let Some(s) = self.shards.get_mut(&shard) else {
                    let _ = response.send(Err(ConsensusError::ShardUnavailable { shard }));
                    return vec![];
                };
                if s.is_failed() {
                    let _ = response.send(Err(ConsensusError::ShardUnavailable { shard }));
                    return vec![];
                }
                match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    s.handle_propose_batch(entries)
                })) {
                    Ok(Ok(actions)) => {
                        let entry_index = actions
                            .iter()
                            .find_map(|a| match a {
                                Action::PersistEntries { entries, .. } => {
                                    entries.last().map(|e| e.index)
                                },
                                _ => None,
                            })
                            .unwrap_or(0);
                        self.pending_responses.push((shard, entry_index, response));
                        self.process_actions(actions);
                        vec![shard]
                    },
                    Ok(Err(e)) => {
                        let _ = response.send(Err(e));
                        vec![]
                    },
                    Err(payload) => {
                        let msg = panic_message(&payload);
                        tracing::error!(shard = shard.0, panic = %msg, "ConsensusState panicked — marking as Failed");
                        counter!("consensus_shard_panic_total", "shard_id" => shard.0.to_string())
                            .increment(1);
                        s.mark_failed();
                        let _ = response.send(Err(ConsensusError::ShardUnavailable { shard }));
                        vec![shard]
                    },
                }
            },
            ReactorEvent::PeerMessage { shard, from, message } => {
                if let Some(s) = self.shards.get_mut(&shard) {
                    if s.is_failed() {
                        return vec![];
                    }
                    // O6 hibernation: a paused shard drops the message and
                    // notifies the wake handler. The peer's next AppendEntries
                    // retry (Raft heartbeat) will land on the awoken shard —
                    // no buffer, no redelivery contract.
                    if self.pause_state.contains_key(&shard) {
                        counter!(
                            "consensus_paused_peer_message_dropped_total",
                            "shard_id" => shard.0.to_string()
                        )
                        .increment(1);
                        tracing::debug!(
                            shard = shard.0,
                            from = from.0,
                            "reactor: dropping PeerMessage for paused shard; invoking wake notifier",
                        );
                        self.wake_notifier.on_peer_message_for_paused_shard(shard);
                        // `from` and `message` are dropped here.
                        let _ = (from, message);
                        return vec![];
                    }
                    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        s.handle_message(from, message)
                    })) {
                        Ok(actions) => {
                            self.process_actions(actions);
                            vec![shard]
                        },
                        Err(payload) => {
                            let msg = panic_message(&payload);
                            tracing::error!(shard = shard.0, panic = %msg, "ConsensusState panicked — marking as Failed");
                            counter!(
                                "consensus_shard_panic_total",
                                "shard_id" => shard.0.to_string()
                            )
                            .increment(1);
                            s.mark_failed();
                            vec![shard]
                        },
                    }
                } else {
                    vec![]
                }
            },
            ReactorEvent::MembershipChange { shard, change, response } => {
                if let Some(s) = self.shards.get_mut(&shard) {
                    if s.is_failed() {
                        let _ = response.send(Err(ConsensusError::ShardUnavailable { shard }));
                        return vec![];
                    }
                    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        s.handle_membership_change(change)
                    })) {
                        Ok(Ok(actions)) => {
                            self.process_actions(actions);
                            let _ = response.send(Ok(()));
                            vec![shard]
                        },
                        Ok(Err(e)) => {
                            let _ = response.send(Err(e));
                            vec![]
                        },
                        Err(payload) => {
                            let msg = panic_message(&payload);
                            tracing::error!(shard = shard.0, panic = %msg, "ConsensusState panicked — marking as Failed");
                            counter!(
                                "consensus_shard_panic_total",
                                "shard_id" => shard.0.to_string()
                            )
                            .increment(1);
                            s.mark_failed();
                            let _ = response.send(Err(ConsensusError::ShardUnavailable { shard }));
                            vec![shard]
                        },
                    }
                } else {
                    let _ = response.send(Err(ConsensusError::ShardUnavailable { shard }));
                    vec![]
                }
            },
            ReactorEvent::ReadIndex { shard, response } => {
                let result = if let Some(s) = self.shards.get(&shard) {
                    Ok(s.commit_index())
                } else {
                    Err(ConsensusError::ShardUnavailable { shard })
                };
                let _ = response.send(result);
                vec![]
            },
            ReactorEvent::TransferLeader { shard, target, response } => {
                if let Some(s) = self.shards.get_mut(&shard) {
                    if s.is_failed() {
                        let _ = response.send(Err(ConsensusError::ShardUnavailable { shard }));
                        return vec![];
                    }
                    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        s.handle_transfer_leader(target)
                    })) {
                        Ok(Ok(actions)) => {
                            self.process_actions(actions);
                            let _ = response.send(Ok(()));
                            vec![shard]
                        },
                        Ok(Err(e)) => {
                            let _ = response.send(Err(e));
                            vec![]
                        },
                        Err(payload) => {
                            let msg = panic_message(&payload);
                            tracing::error!(shard = shard.0, panic = %msg, "ConsensusState panicked — marking as Failed");
                            counter!(
                                "consensus_shard_panic_total",
                                "shard_id" => shard.0.to_string()
                            )
                            .increment(1);
                            s.mark_failed();
                            let _ = response.send(Err(ConsensusError::ShardUnavailable { shard }));
                            vec![shard]
                        },
                    }
                } else {
                    let _ = response.send(Err(ConsensusError::ShardUnavailable { shard }));
                    vec![]
                }
            },
            ReactorEvent::TriggerSnapshot { shard, response } => {
                if let Some(s) = self.shards.get(&shard) {
                    let actions = s.handle_trigger_snapshot();
                    let result = actions
                        .iter()
                        .find_map(|a| match a {
                            Action::TriggerSnapshot {
                                last_included_index,
                                last_included_term,
                                ..
                            } => Some((*last_included_index, *last_included_term)),
                            _ => None,
                        })
                        .unwrap_or((0, 0));
                    self.process_actions(actions);
                    let _ = response.send(Ok(result));
                    vec![shard]
                } else {
                    let _ = response.send(Err(ConsensusError::ShardUnavailable { shard }));
                    vec![]
                }
            },
            ReactorEvent::SnapshotCompleted { shard, last_included_index } => {
                if let Some(s) = self.shards.get_mut(&shard) {
                    s.handle_snapshot_completed(last_included_index);
                }
                vec![]
            },
            ReactorEvent::SnapshotInstalled { shard, last_included_index, last_included_term } => {
                // Stage 4: the external installer has successfully consumed
                // the staged snapshot file and loaded it into the local
                // state-machine. Advance the shard's `last_applied`,
                // `last_snapshot_index`, and `commit_index` and truncate
                // the log prefix subsumed by the snapshot.
                //
                // Idempotent inside `handle_snapshot_installed` — repeat
                // notifications for the same index are no-ops.
                if let Some(s) = self.shards.get_mut(&shard) {
                    s.handle_snapshot_installed(last_included_index, last_included_term);
                    // Bump our `last_applied` tracker so the next commit
                    // dispatch does not re-deliver entries already covered
                    // by the installed snapshot.
                    let entry = self.last_applied.entry(shard).or_insert(0);
                    if last_included_index > *entry {
                        *entry = last_included_index;
                    }
                }
                vec![]
            },
            ReactorEvent::QueryPeerState { shard, node, response } => {
                let result = self.shards.get(&shard).and_then(|s| s.peer_match_index(node));
                let _ = response.send(result);
                vec![]
            },
            ReactorEvent::ShutdownFlush { ack } => {
                // If an async fsync is in flight, complete it first. We cannot
                // return early here (unlike the normal flush tick) because
                // shutdown requires confirmed durability before proceeding.
                if self.fsync_phase == FsyncPhase::Submitted {
                    // Poll until the in-flight fsync completes. For synchronous
                    // backends this returns true immediately. For async backends
                    // this spins — acceptable at shutdown since we need the
                    // durability guarantee and the reactor is about to stop.
                    while !self.wal.poll_fsync_completion() {
                        std::thread::yield_now();
                    }
                    self.fsync_phase = FsyncPhase::Idle;

                    // The in-flight fsync covered frames already appended to the
                    // WAL. Those entries are now durable. Resolve any pending
                    // responses that were waiting on that fsync and clear
                    // pending_commits so we don't double-process them.
                    self.resolve_committed_responses();
                    self.pending_commits.clear();
                }

                // Force-flush all remaining pending WAL frames and sync to disk.
                let frames = std::mem::take(&mut self.pending_wal_frames);
                if !frames.is_empty() && self.wal.append(&frames).is_err() {
                    // Drain pending responses so waiters get a structured error
                    // rather than a dropped-receiver error.
                    for (_, _, resp) in self.pending_responses.drain(..) {
                        let _ = resp.send(Err(ConsensusError::WalWriteError));
                    }
                    self.pending_commits.clear();
                    let _ = ack.send(Err(ConsensusError::WalWriteError));
                    return vec![];
                }
                let result = match self.wal.sync() {
                    Ok(()) => Ok(()),
                    Err(_e) => {
                        for (_, _, resp) in self.pending_responses.drain(..) {
                            let _ = resp.send(Err(ConsensusError::WalWriteError));
                        }
                        self.pending_commits.clear();
                        Err(ConsensusError::WalWriteError)
                    },
                };
                let _ = ack.send(result);
                vec![]
            },
            ReactorEvent::AddShard { boxed_shard, shard_id, state_tx } => {
                // DIAG (Task #153 hypothesis A): record reactor state before
                // and after the AddShard registration so we can verify that
                // adding (e.g.) per-vault shards does NOT clobber timer
                // entries for the existing region/org shards already in the
                // wheel. If `pre_timer_count` and `post_timer_count` agree
                // modulo +1 per scheduled-timer initial action, the wheel
                // is intact.
                let pre_existing_shard_ids: Vec<u64> = self.shards.keys().map(|id| id.0).collect();
                tracing::debug!(
                    shard = shard_id.0,
                    pre_existing_shard_count = self.shards.len(),
                    pre_existing_timer_count = self.timers.len(),
                    pre_existing_shard_ids = ?pre_existing_shard_ids,
                    "reactor: AddShard handling start",
                );
                // Duplicate-registration guard. A duplicate is a caller
                // bug — ignore and drop `state_tx` so the caller's receiver
                // observes a closed channel.
                if self.shards.contains_key(&shard_id) {
                    tracing::warn!(
                        shard = shard_id.0,
                        "AddShard: shard already registered — ignoring duplicate",
                    );
                    drop(state_tx);
                    return vec![];
                }
                // The reactor is generic over `C, R` so we know exactly the
                // target type of the downcast. A mismatch here means a
                // caller built a shard with different generics than the
                // reactor — a programming error. Log and drop to avoid
                // corrupting state.
                let shard: ConsensusState<C, R> =
                    match boxed_shard.downcast::<ConsensusState<C, R>>() {
                        Ok(b) => *b,
                        Err(_) => {
                            tracing::error!(
                                shard = shard_id.0,
                                "AddShard: boxed_shard type did not match \
                             reactor's ConsensusState<C, R> — dropping registration",
                            );
                            drop(state_tx);
                            return vec![];
                        },
                    };
                debug_assert_eq!(
                    shard.id(),
                    shard_id,
                    "AddShard: shard_id in event must match ConsensusState::id"
                );
                // Initial actions (typically schedules the election timer).
                let actions = shard.initial_actions();
                // Initialize last_applied from the shard's commit_index
                // when the shard was constructed with non-zero commit
                // (post-snapshot or post-crash recovery), matching the
                // behaviour of [`Self::add_shard`] at startup.
                let restored_commit = shard.commit_index();
                if restored_commit > 0 {
                    self.last_applied.insert(shard_id, restored_commit);
                }
                // Install into reactor maps.
                self.shards.insert(shard_id, shard);
                self.state_watchers.insert(shard_id, state_tx);
                // Process initial actions (schedules election timer).
                self.process_actions(actions);
                // DIAG (Task #153 hypothesis A): post-registration snapshot.
                tracing::debug!(
                    shard = shard_id.0,
                    post_shard_count = self.shards.len(),
                    post_timer_count = self.timers.len(),
                    "reactor: AddShard handling complete",
                );
                tracing::info!(shard = shard_id.0, "AddShard: registered at runtime");
                vec![shard_id]
            },
            ReactorEvent::RemoveShard { shard } => {
                let Some(s) = self.shards.get_mut(&shard) else {
                    tracing::debug!(
                        shard = shard.0,
                        "RemoveShard: shard not registered — ignoring",
                    );
                    return vec![];
                };
                tracing::info!(shard = shard.0, "RemoveShard: marking for graceful shutdown");
                // Mark the shard as Shutdown. It still processes in-flight
                // peer messages during the grace period but will not
                // initiate elections or heartbeats.
                s.mark_shutdown();
                // Record this as an explicit removal so the cleanup timer
                // unconditionally drops it (bypassing the
                // restore-from-shutdown guard used by the
                // membership-driven ShardRemoved path).
                self.force_removal_shards.insert(shard);
                // Cancel election / heartbeat timers — the shard is
                // winding down. `cancel_all` does not touch Cleanup, so
                // scheduling the Cleanup timer below is safe.
                self.timers.cancel_all(shard);
                // Drain pending proposal responses for this shard with
                // NotLeader so waiters unblock immediately rather than
                // hanging until the oneshot drops.
                let mut still_pending = Vec::with_capacity(self.pending_responses.len());
                for (sid, idx, resp) in self.pending_responses.drain(..) {
                    if sid == shard {
                        let _ = resp.send(Err(ConsensusError::NotLeader));
                    } else {
                        still_pending.push((sid, idx, resp));
                    }
                }
                self.pending_responses = still_pending;
                // Schedule cleanup (30s grace period for in-flight
                // messages). On expiry, process_expired_timers drops the
                // shard from the reactor's maps.
                let deadline = self.clock.now() + Duration::from_secs(30);
                self.timers.schedule(shard, TimerKind::Cleanup, deadline);
                vec![shard]
            },
            ReactorEvent::AdoptLeader { shard, leader, term } => {
                // Route to the target shard's `adopt_leader` (delegated
                // leadership). If the shard is not registered with this
                // reactor, drop the event silently — typical when an
                // organization shard hasn't been bootstrapped on this
                // node yet (the next adopt_leader after bootstrap will
                // converge state).
                if let Some(s) = self.shards.get_mut(&shard) {
                    let actions = s.adopt_leader(leader, term);
                    if !actions.is_empty() {
                        self.process_actions(actions);
                        return vec![shard];
                    }
                }
                vec![]
            },
            ReactorEvent::PauseShard { shard, response } => {
                if !self.shards.contains_key(&shard) {
                    let _ = response.send(Err(ConsensusError::ShardUnavailable { shard }));
                    return vec![];
                }
                // Idempotent: inserting an existing key is a no-op.
                let was_paused = self.pause_state.insert(shard, ()).is_some();
                tracing::info!(
                    shard = shard.0,
                    already_paused = was_paused,
                    "reactor: shard paused",
                );
                let _ = response.send(Ok(()));
                vec![]
            },
            ReactorEvent::ResumeShard { shard, response } => {
                if !self.shards.contains_key(&shard) {
                    let _ = response.send(Err(ConsensusError::ShardUnavailable { shard }));
                    return vec![];
                }
                // Idempotent: removing a non-existent key is a no-op.
                let was_paused = self.pause_state.remove(&shard).is_some();
                tracing::info!(shard = shard.0, was_paused, "reactor: shard resumed",);
                let _ = response.send(Ok(()));
                vec![]
            },
            ReactorEvent::Shutdown => {
                // Handled in run() before dispatch.
                vec![]
            },
        }
    }

    /// Drains expired timers from the timer wheel and dispatches to shards.
    ///
    /// Returns the shard IDs that processed at least one expired timer so the
    /// caller can broadcast updated state snapshots.
    fn process_expired_timers(&mut self) -> Vec<ConsensusStateId> {
        let now = self.clock.now();
        let mut all_actions = Vec::new();
        let mut affected = Vec::new();
        let mut shards_to_remove = Vec::new();

        while let Some((shard_id, kind, deadline)) = self.timers.poll_expired(now) {
            // DIAG (Task #153): every timer that actually fires gets logged
            // here. If election timers for the data-region shards never
            // appear post-restart we have ruled in hypothesis C (reactor
            // task dead) or D (deadline corruption). If they fire but
            // produce no actions / no messages, the bug is downstream of
            // the wheel (`Shard::handle_election_timeout` or transport).
            let lateness_ms = now.duration_since(deadline).as_millis() as u64;
            tracing::debug!(
                shard = shard_id.0,
                kind = ?kind,
                lateness_ms,
                "reactor: timer fired",
            );
            if let Some(shard) = self.shards.get_mut(&shard_id) {
                // O6 hibernation: skip Election/Heartbeat timer delivery for
                // paused shards. The timer was already removed from the wheel
                // by `poll_expired` above (silently dropped — cheaper than
                // re-arming). On resume, the shard re-arms normal Raft
                // election/heartbeat timers via state-machine transitions on
                // the next legitimate event (peer message from the parent
                // group's leader, application command, etc.). Cleanup timers
                // are NOT suppressed — they're reactor-internal removal
                // markers; dropping them would orphan a shard that was
                // marked for removal while paused.
                let is_paused = self.pause_state.contains_key(&shard_id);
                let suppress_for_pause =
                    is_paused && matches!(kind, TimerKind::Election | TimerKind::Heartbeat);
                if suppress_for_pause {
                    counter!(
                        "consensus_paused_timer_skipped_total",
                        "shard_id" => shard_id.0.to_string(),
                        "kind" => format!("{kind:?}"),
                    )
                    .increment(1);
                    tracing::debug!(
                        shard = shard_id.0,
                        kind = ?kind,
                        "reactor: skipping expired timer for paused shard",
                    );
                    affected.push(shard_id);
                    continue;
                }
                let actions = match kind {
                    TimerKind::Election => {
                        if shard.is_failed() {
                            Vec::new()
                        } else {
                            // DIAG (Task #153): capture pre-call shard state
                            // so we can correlate post-call action counts
                            // with the path taken inside
                            // `handle_election_timeout`. Hypothesis check: if
                            // the data-region shard is somehow in Delegated
                            // mode, only `ScheduleTimer` (1 action) will be
                            // produced; `start_pre_vote` produces N+1
                            // actions (N voter peers + 1 timer reset).
                            let pre_state = shard.state_snapshot();
                            let pre_is_voter = shard.membership().is_voter(shard.local_node_id());
                            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                shard.handle_election_timeout()
                            })) {
                                Ok(actions) => {
                                    let send_count = actions
                                        .iter()
                                        .filter(|a| matches!(a, Action::Send { .. }))
                                        .count();
                                    let schedule_count = actions
                                        .iter()
                                        .filter(|a| matches!(a, Action::ScheduleTimer { .. }))
                                        .count();
                                    tracing::debug!(
                                        shard = shard_id.0,
                                        action_count = actions.len(),
                                        send_count,
                                        schedule_count,
                                        pre_state = ?pre_state,
                                        pre_is_voter,
                                        "reactor: handle_election_timeout returned",
                                    );
                                    actions
                                },
                                Err(payload) => {
                                    let msg = panic_message(&payload);
                                    tracing::error!(shard = shard_id.0, panic = %msg, "ConsensusState panicked — marking as Failed");
                                    counter!(
                                        "consensus_shard_panic_total",
                                        "shard_id" => shard_id.0.to_string()
                                    )
                                    .increment(1);
                                    shard.mark_failed();
                                    Vec::new()
                                },
                            }
                        }
                    },
                    TimerKind::Heartbeat => {
                        if shard.is_failed() {
                            Vec::new()
                        } else {
                            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                shard.handle_heartbeat_timeout()
                            })) {
                                Ok(actions) => actions,
                                Err(payload) => {
                                    let msg = panic_message(&payload);
                                    tracing::error!(shard = shard_id.0, panic = %msg, "ConsensusState panicked — marking as Failed");
                                    counter!(
                                        "consensus_shard_panic_total",
                                        "shard_id" => shard_id.0.to_string()
                                    )
                                    .increment(1);
                                    shard.mark_failed();
                                    Vec::new()
                                },
                            }
                        }
                    },
                    TimerKind::Cleanup => {
                        // Explicit removals (via `ReactorEvent::RemoveShard`)
                        // unconditionally drop the shard. The shard is being
                        // decommissioned as a whole, not removed from Raft
                        // membership — the membership-restore heuristic below
                        // does not apply.
                        if self.force_removal_shards.contains(&shard_id) {
                            tracing::info!(
                                shard = shard_id.0,
                                "RemoveShard cleanup timer expired — removing"
                            );
                            shards_to_remove.push(shard_id);
                            Vec::new()
                        } else {
                            // Grace period expired. Only remove if the local node is
                            // still absent from the shard's membership. During log
                            // replay, a node that joined after the shard was created
                            // may see historical entries that don't include it,
                            // triggering a spurious ShardRemoved+Cleanup. By the time
                            // the timer fires, later entries will have restored
                            // membership — check before removing.
                            let local = shard.local_node_id();
                            let still_removed = !shard.membership().is_voter(local)
                                && !shard.membership().is_learner(local);
                            if still_removed {
                                tracing::info!(
                                    shard = shard_id.0,
                                    "ConsensusState cleanup timer expired — removing"
                                );
                                shards_to_remove.push(shard_id);
                            } else {
                                tracing::info!(
                                    shard = shard_id.0,
                                    "ConsensusState cleanup timer expired but node is back in membership — keeping"
                                );
                                shard.restore_from_shutdown();
                            }
                            Vec::new()
                        }
                    },
                };
                all_actions.extend(actions);
                affected.push(shard_id);
            }
        }

        // Remove shutdown shards after the grace period. Drop every
        // per-shard map entry — a later AddShard with the same id should
        // start from a clean slate.
        for shard_id in shards_to_remove {
            self.shards.remove(&shard_id);
            self.state_watchers.remove(&shard_id);
            self.last_applied.remove(&shard_id);
            // Clear any lingering pause flag so a subsequent AddShard
            // with the same id observes the shard as active.
            self.pause_state.remove(&shard_id);
            self.force_removal_shards.remove(&shard_id);
        }

        self.process_actions(all_actions);
        affected
    }

    /// Sorts actions returned by shards into the appropriate pending buffers.
    ///
    /// Two-pass processing ensures Raft safety (Figure 2):
    ///   1. **First pass**: `PersistTermState` actions write a WAL checkpoint and fsync, making
    ///      term + votedFor durable *before* any messages leave the node.
    ///   2. **Second pass**: all other actions are processed normally.
    fn process_actions(&mut self, actions: Vec<Action>) {
        // ── Pass 1: persist term state before any sends ────────────────
        //
        // Multiple PersistTermState actions in a single batch are collapsed
        // into one checkpoint write + fsync (the last one wins per shard, but
        // in practice only one fires per event).
        let mut needs_term_persist = false;
        let mut latest_term_checkpoint: Option<CheckpointFrame> = None;

        for action in &actions {
            if let Action::PersistTermState { term, voted_for, .. } = action {
                needs_term_persist = true;
                // Build a checkpoint. committed_index comes from whichever
                // shard we can observe — term state persistence is the priority.
                let committed_index =
                    self.shards.values().map(|s| s.commit_index()).max().unwrap_or(0);
                latest_term_checkpoint = Some(CheckpointFrame {
                    committed_index,
                    term: *term,
                    voted_for: voted_for.map(|n| n.0),
                });
            }
        }

        if needs_term_persist && let Some(cp) = &latest_term_checkpoint {
            // Write checkpoint + fsync synchronously. If this fails, the
            // node cannot safely participate in elections — skip all sends
            // from this batch to avoid violating Raft safety.
            if self.wal.write_checkpoint(cp).is_err() || self.wal.sync().is_err() {
                tracing::error!(
                    "Failed to persist term state (term={}, voted_for={:?}) — \
                         dropping all outbound messages from this batch to preserve Raft safety",
                    cp.term,
                    cp.voted_for,
                );
                // Still process non-Send actions (timers, commits, etc.)
                // but skip all message sends.
                for action in actions {
                    match action {
                        Action::PersistTermState { .. } | Action::Send { .. } => {},
                        Action::PersistEntries { shard, entries } => {
                            for entry in entries {
                                self.pending_wal_frames.push(WalFrame {
                                    shard_id: shard,
                                    index: entry.index,
                                    term: entry.term,
                                    data: Arc::clone(&entry.data),
                                });
                            }
                        },
                        Action::Committed { shard, up_to } => {
                            self.pending_commits.push((shard, up_to));
                        },
                        Action::ScheduleTimer { shard, kind, deadline } => {
                            self.timers.schedule(shard, kind, deadline);
                        },
                        Action::RenewLease { .. }
                        | Action::TriggerSnapshot { .. }
                        | Action::MembershipChanged { .. }
                        | Action::SendSnapshot { .. }
                        | Action::InstallSnapshot { .. }
                        | Action::ShardRemoved { .. } => {},
                    }
                }
                return;
            }
        }

        // ── Pass 2: process all remaining actions ──────────────────────
        for action in actions {
            match action {
                Action::PersistTermState { .. } => {
                    // Already handled in pass 1.
                },
                Action::Send { to, shard, msg } => {
                    // O6 hibernation: drop sends originating from a paused
                    // shard. Vote requests are dropped along with the rest;
                    // a paused shard should not be electioneering.
                    if self.pause_state.contains_key(&shard) {
                        counter!(
                            "consensus_paused_send_dropped_total",
                            "shard_id" => shard.0.to_string()
                        )
                        .increment(1);
                        tracing::debug!(
                            shard = shard.0,
                            to = to.0,
                            "reactor: dropping Send action from paused shard",
                        );
                        // `to` and `msg` are dropped at end of this match arm.
                        let _ = (to, msg);
                        continue;
                    }
                    // Vote request messages bypass the outbox to avoid election
                    // latency. This is safe because PersistTermState (if present)
                    // was already fsynced in pass 1.
                    let is_vote =
                        matches!(msg, Message::PreVoteRequest { .. } | Message::VoteRequest { .. });
                    if is_vote {
                        self.transport.send_batch(vec![OutboundMessage { to, shard, msg }]);
                    } else {
                        self.outbox.enqueue(to, shard, msg);
                    }
                },
                Action::PersistEntries { shard, entries } => {
                    for entry in entries {
                        self.pending_wal_frames.push(WalFrame {
                            shard_id: shard,
                            index: entry.index,
                            term: entry.term,
                            data: Arc::clone(&entry.data),
                        });
                    }
                },
                Action::Committed { shard, up_to } => {
                    self.pending_commits.push((shard, up_to));
                },
                Action::ScheduleTimer { shard, kind, deadline } => {
                    // DIAG (Task #153): every timer scheduled or rescheduled
                    // is logged with its deadline relative to `clock.now()`.
                    // Hypothesis D ("election_deadline corruption — Time::MAX
                    // or far-future") would surface here as an absurd
                    // `deadline_ms_from_now`. A negative value means the
                    // timer was scheduled for the past (already-expired) and
                    // should fire on the next iteration.
                    let now = self.clock.now();
                    let deadline_ms_from_now: i64 = if deadline >= now {
                        deadline.duration_since(now).as_millis() as i64
                    } else {
                        -(now.duration_since(deadline).as_millis() as i64)
                    };
                    tracing::debug!(
                        shard = shard.0,
                        kind = ?kind,
                        deadline_ms_from_now,
                        "reactor: timer scheduled",
                    );
                    self.timers.schedule(shard, kind, deadline);
                },
                Action::RenewLease { .. } => {
                    // Lease renewal happens inside the ConsensusState; no reactor action needed.
                },
                Action::MembershipChanged { membership, .. } => {
                    self.transport.on_membership_changed(&membership);
                },
                Action::TriggerSnapshot { shard, last_included_index, last_included_term } => {
                    // Stage 2: dispatch to the snapshot coordinator. The
                    // coordinator MUST spawn any I/O asynchronously; the
                    // reactor's event loop is on hold until this returns.
                    // The coordinator resolves the actual covered index from
                    // the snapshot it produces and invokes
                    // `ConsensusEngine::notify_snapshot_completed` to
                    // advance the shard's `last_snapshot_index`.
                    tracing::debug!(
                        shard = shard.0,
                        last_included_index,
                        last_included_term,
                        "reactor: TriggerSnapshot — dispatching to snapshot coordinator",
                    );
                    self.snapshot_coordinator.on_trigger_snapshot(
                        shard,
                        last_included_index,
                        last_included_term,
                    );
                },
                Action::SendSnapshot {
                    to,
                    shard,
                    term: _,
                    leader_id: _,
                    last_included_index,
                    last_included_term: _,
                } => {
                    // Stage 3: dispatch to the snapshot sender. The sender
                    // resolves the shard ID to its scope, reads the at-rest
                    // encrypted snapshot bytes via SnapshotPersister, and
                    // streams them to the follower over the internal Raft
                    // transport's `InstallSnapshotStream` RPC.
                    //
                    // The sender MUST spawn any I/O asynchronously; the
                    // reactor's event loop is on hold until this returns.
                    // On dispatch failure (no scope mapping, follower
                    // unreachable, staging-write error, CRC mismatch), the
                    // sender logs + records a metric and drops the
                    // request. The leader's heartbeat replicator re-emits
                    // `Action::SendSnapshot` on the next cycle, so
                    // transient failures self-heal without explicit retry
                    // state in the reactor — same drop-and-let-Raft-retry
                    // contract as the wake notifier and snapshot
                    // coordinator.
                    //
                    // O6 hibernation: drops from a paused shard were
                    // already filtered in pass 1 (see the `Action::Send`
                    // arm), so we do not re-check `pause_state` here.
                    tracing::debug!(
                        shard = shard.0,
                        to = to.0,
                        last_included_index,
                        "reactor: SendSnapshot — dispatching to snapshot sender",
                    );
                    self.snapshot_sender.send_snapshot(shard, to, last_included_index);
                },
                Action::InstallSnapshot {
                    shard,
                    leader_term,
                    last_included_index,
                    last_included_term,
                } => {
                    // Stage 4: dispatch to the snapshot installer. The
                    // installer resolves the shard ID to its scope, locates
                    // the staged file written by Stage 3's
                    // `snapshot_receiver`, decrypts via the
                    // `SnapshotKeyProvider`, and feeds the plaintext to
                    // `RaftLogStore::install_snapshot`. On completion the
                    // installer calls back into
                    // `ConsensusEngine::notify_snapshot_installed` so the
                    // shard's `last_applied` / `last_snapshot_index`
                    // advance.
                    //
                    // The installer MUST spawn any I/O asynchronously; the
                    // reactor's event loop is on hold until this returns.
                    // On dispatch failure (no scope mapping, staged file
                    // not found yet, decrypt error, scope mismatch), the
                    // installer logs + records a metric and drops the
                    // request. The leader's heartbeat replicator re-emits
                    // `Action::SendSnapshot` on the next cycle (re-staging
                    // the file via Stage 3 and re-emitting this action),
                    // so transient failures self-heal — same
                    // drop-and-let-Raft-retry contract as the snapshot
                    // sender / coordinator.
                    tracing::debug!(
                        shard = shard.0,
                        leader_term,
                        last_included_index,
                        last_included_term,
                        "reactor: InstallSnapshot — dispatching to snapshot installer",
                    );
                    self.snapshot_installer.install_snapshot(
                        shard,
                        leader_term,
                        last_included_index,
                        last_included_term,
                    );
                },
                Action::ShardRemoved { shard } => {
                    tracing::info!(
                        shard = shard.0,
                        "Local node removed from shard membership — scheduling cleanup"
                    );
                    // Mark the shard as Shutdown. It still processes in-flight
                    // messages during the grace period but won't initiate elections.
                    if let Some(s) = self.shards.get_mut(&shard) {
                        s.mark_shutdown();
                    }
                    // Cancel election/heartbeat timers — the shard is winding down.
                    self.timers.cancel_all(shard);
                    // Schedule cleanup timer (30s grace period for in-flight messages).
                    let deadline = self.clock.now() + Duration::from_secs(30);
                    self.timers.schedule(shard, TimerKind::Cleanup, deadline);
                },
            }
        }
    }

    /// Broadcasts the current state snapshot for the given shards.
    ///
    /// Only shards that were touched by the preceding event are broadcast,
    /// avoiding unnecessary wakeups for idle shards.
    fn broadcast_shard_states(&self, shard_ids: &[ConsensusStateId]) {
        for shard_id in shard_ids {
            if let (Some(shard), Some(tx)) =
                (self.shards.get(shard_id), self.state_watchers.get(shard_id))
            {
                let _ = tx.send(shard.state_snapshot());
            }
        }
    }

    /// Sends a `CommittedBatch` to the apply worker for each `(shard_id, up_to)` pair.
    ///
    /// Collects log entries in the range `(last_applied, up_to]` from the shard,
    /// updates `last_applied`, and marks the shard failed if the apply worker
    /// channel has closed.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(shard_count = pending.len())
    )]
    async fn dispatch_committed_batches(&mut self, pending: &[(ConsensusStateId, u64)]) {
        for &(shard_id, up_to) in pending {
            let last = self.last_applied.get(&shard_id).copied().unwrap_or(0);
            let entries = if let Some(shard) = self.shards.get(&shard_id) {
                shard
                    .log_entries(last + 1, up_to)
                    .into_iter()
                    .map(|e| CommittedEntry {
                        index: e.index,
                        term: e.term,
                        data: Arc::clone(&e.data),
                        kind: e.kind.clone(),
                    })
                    .collect()
            } else {
                vec![]
            };
            let leader_node = self.shards.get(&shard_id).and_then(|s| s.leader_id()).map(|n| n.0);
            let batch = CommittedBatch { shard: shard_id, entries, leader_node };
            if self.commit_tx.send(batch).await.is_err() {
                tracing::error!(
                    shard = shard_id.0,
                    "Apply worker channel closed — committed entries will not be applied"
                );
                if let Some(s) = self.shards.get_mut(&shard_id) {
                    s.mark_failed();
                }
            }
            self.last_applied.insert(shard_id, up_to);
        }
    }

    /// Flushes pending WAL frames to durable storage, dispatches committed
    /// batches to the apply worker, and resolves proposal responses.
    ///
    /// Proposal responses are only resolved after quorum commit, not merely
    /// after local WAL fsync. This ensures clients see success only when the
    /// entry is durable on a majority of nodes (Raft safety). Responses for
    /// shards that lose leadership are rejected with `NotLeader`.
    ///
    /// For backends that support async fsync (`supports_async_fsync() == true`),
    /// the flush cycle works in two steps:
    /// 1. Append frames and submit the fsync — set `fsync_phase` to `Submitted`, return without
    ///    resolving responses.
    /// 2. On the next flush tick, `poll_fsync_completion()` is checked first. When it returns
    ///    `true`, commits are dispatched and committed responses are resolved.
    ///
    /// For synchronous backends (the default), both steps collapse into one:
    /// `submit_async_fsync()` calls `sync()` internally, `poll_fsync_completion()`
    /// returns `true` immediately, and the cycle is identical to the old behavior.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            pending_frames = self.pending_wal_frames.len(),
            pending_commits = self.pending_commits.len(),
            pending_responses = self.pending_responses.len(),
        )
    )]
    async fn flush(&mut self) {
        // If an async fsync is in flight, poll for completion first.
        if self.fsync_phase == FsyncPhase::Submitted {
            if !self.wal.poll_fsync_completion() {
                // Still waiting — deliver outbound messages but don't resolve responses yet.
                self.outbox.flush(&self.transport);
                return;
            }
            // Fsync complete — dispatch commits, resolve committed responses, reset phase.
            self.fsync_phase = FsyncPhase::Idle;
            let pending = std::mem::take(&mut self.pending_commits);
            self.dispatch_committed_batches(&pending).await;
            self.resolve_committed_responses();
            self.reject_stale_responses();
            self.outbox.flush(&self.transport);
            return;
        }

        // Determine the maximum committed index across all shards this cycle,
        // used to write a checkpoint frame before fsync.
        let max_committed = self.pending_commits.iter().map(|(_, up_to)| *up_to).max();

        let has_wal_work = !self.pending_wal_frames.is_empty() || max_committed.is_some();

        if has_wal_work {
            let frames = std::mem::take(&mut self.pending_wal_frames);

            // Append entry frames (may be empty if only commits this cycle).
            let append_result = {
                let _span =
                    tracing::debug_span!("wal_append", frame_count = frames.len()).entered();
                if !frames.is_empty() { self.wal.append(&frames) } else { Ok(()) }
            };
            if append_result.is_err() {
                for (_, _, resp) in self.pending_responses.drain(..) {
                    let _ = resp.send(Err(ConsensusError::WalWriteError));
                }
                self.pending_commits.clear();
                self.outbox = NetworkOutbox::new();
                return;
            }

            // Write checkpoint before fsync so it becomes durable with the
            // same sync call, avoiding an extra fsync.
            if let Some(committed_index) = max_committed {
                let first_shard = self.shards.values().next();
                let term = first_shard.map_or(0, |s| s.current_term());
                let voted_for = first_shard.and_then(|s| s.voted_for().map(|n| n.0));
                let checkpoint = CheckpointFrame { committed_index, term, voted_for };
                // Best-effort: a checkpoint failure is non-fatal — crash
                // recovery can replay from the previous checkpoint.
                let _ = self.wal.write_checkpoint(&checkpoint);
            }

            if self.wal.supports_async_fsync() {
                // Async path: submit fsync and return — poll for completion next cycle.
                if self.wal.submit_async_fsync().is_err() {
                    for (_, _, resp) in self.pending_responses.drain(..) {
                        let _ = resp.send(Err(ConsensusError::WalWriteError));
                    }
                    self.pending_commits.clear();
                    self.outbox = NetworkOutbox::new();
                    return;
                }
                self.fsync_phase = FsyncPhase::Submitted;
                self.outbox.flush(&self.transport);
                return;
            }

            // Pipelined commit: dispatch committed batches, flush the
            // outbox, and resolve client responses NOW — before the blocking
            // fsync. Entries are in the kernel page cache (ordered + visible
            // to subsequent reads); durability reaches non-volatile media on
            // the sync call below. On sync failure we log but do not retract
            // already-acked responses — the client saw success, and the
            // entries survive process crash via kernel writeback. Kernel
            // panic or power loss between now and sync completion is the
            // documented loss window (see docs/architecture/durability.md).
            let pending = std::mem::take(&mut self.pending_commits);
            self.dispatch_committed_batches(&pending).await;
            self.outbox.flush(&self.transport);
            self.resolve_committed_responses();

            let sync_result = {
                let _span = tracing::debug_span!("wal_sync").entered();
                self.wal.sync()
            };
            if let Err(e) = sync_result {
                tracing::error!(
                    error = %e,
                    "WAL fsync failed after client ACK. \
                     Already-acked writes may not survive kernel panic or power loss."
                );
                counter!("consensus_pipelined_sync_failures_total").increment(1);
            }
            self.reject_stale_responses();
            return;
        }

        // Dispatch committed batches to the apply worker.
        let pending = std::mem::take(&mut self.pending_commits);
        self.dispatch_committed_batches(&pending).await;

        // Deliver outbound messages via the transport BEFORE resolving
        // responses to clients. This ordering guarantee ensures that
        // AppendEntries messages reach followers before any client sees
        // a success response. Without this, a SIGKILL between response
        // delivery and outbox flush can cause acknowledged writes to be
        // lost — the client saw success, but no follower received the
        // entry, so the new leader after election doesn't have it.
        self.outbox.flush(&self.transport);

        // Resolve responses for entries that have reached quorum commit.
        // Safe to send now: followers have the entries in their network
        // receive buffers (or have already ack'd).
        self.resolve_committed_responses();

        // Reject responses for shards where this node lost leadership.
        self.reject_stale_responses();
    }

    /// Resolves pending proposal responses whose entry index has been committed
    /// by a quorum.
    ///
    /// A response is resolved when the shard's `commit_index` is >= the entry's
    /// log index, meaning the entry has been replicated to a quorum and is
    /// durable. Unresolved responses remain in the queue for future flush cycles.
    fn resolve_committed_responses(&mut self) {
        let mut still_pending = Vec::new();
        for (shard_id, index, resp) in self.pending_responses.drain(..) {
            let committed = self.shards.get(&shard_id).map(|s| s.commit_index()).unwrap_or(0);
            if index <= committed {
                let _ = resp.send(Ok(index));
            } else {
                still_pending.push((shard_id, index, resp));
            }
        }
        self.pending_responses = still_pending;
    }

    /// Rejects pending proposal responses for shards where this node is no
    /// longer the leader.
    ///
    /// If leadership is lost (e.g., higher-term message, network partition
    /// healing), pending proposals will never be committed on this node. Clients
    /// receive [`ConsensusError::NotLeader`] so they can retry on the new leader.
    fn reject_stale_responses(&mut self) {
        let mut still_pending = Vec::new();
        for (shard_id, index, resp) in self.pending_responses.drain(..) {
            let is_leader = self
                .shards
                .get(&shard_id)
                .map(|s| s.state() == crate::types::NodeState::Leader)
                .unwrap_or(false);
            if is_leader {
                still_pending.push((shard_id, index, resp));
            } else {
                let _ = resp.send(Err(ConsensusError::NotLeader));
            }
        }
        self.pending_responses = still_pending;
    }
}

/// Extracts a human-readable message from a panic payload.
fn panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic".to_string()
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::disallowed_methods)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    use super::*;
    use crate::{
        clock::SimulatedClock,
        config::ShardConfig,
        rng::SimulatedRng,
        transport::InMemoryTransport,
        types::{Entry, Membership},
        wal::InMemoryWalBackend,
    };

    // ── Async WAL mock ────────────────────────────────────────────

    /// A WAL backend that simulates async fsync (io_uring style).
    ///
    /// `submit_async_fsync` delegates to the inner sync but defers
    /// response resolution until `poll_fsync_completion` returns `true`.
    struct AsyncMockWalBackend {
        inner: InMemoryWalBackend,
        completed: AtomicBool,
        inject_submit_error: bool,
    }

    impl AsyncMockWalBackend {
        fn new() -> Self {
            Self {
                inner: InMemoryWalBackend::new(),
                completed: AtomicBool::new(false),
                inject_submit_error: false,
            }
        }

        fn set_completed(&self, val: bool) {
            self.completed.store(val, Ordering::Release);
        }
    }

    impl WalBackend for AsyncMockWalBackend {
        fn append(&mut self, frames: &[WalFrame]) -> Result<(), crate::wal_backend::WalError> {
            self.inner.append(frames)
        }

        fn sync(&mut self) -> Result<(), crate::wal_backend::WalError> {
            self.inner.sync()
        }

        fn read_frames(
            &self,
            from_offset: u64,
        ) -> Result<Vec<WalFrame>, crate::wal_backend::WalError> {
            self.inner.read_frames(from_offset)
        }

        fn truncate_before(&mut self, offset: u64) -> Result<(), crate::wal_backend::WalError> {
            self.inner.truncate_before(offset)
        }

        fn shred_frames(
            &mut self,
            shard_id: ConsensusStateId,
        ) -> Result<u64, crate::wal_backend::WalError> {
            self.inner.shred_frames(shard_id)
        }

        fn supports_async_fsync(&self) -> bool {
            true
        }

        fn submit_async_fsync(&mut self) -> Result<(), crate::wal_backend::WalError> {
            if self.inject_submit_error {
                return Err(crate::wal_backend::WalError::Io {
                    kind: std::io::ErrorKind::Other,
                    message: "injected submit error".to_string(),
                });
            }
            self.inner.sync()
        }

        fn poll_fsync_completion(&self) -> bool {
            self.completed.load(Ordering::Acquire)
        }
    }

    /// Test helper returning a reactor plus senders for both control and
    /// proposal inboxes. Tests that only exercise one path can ignore the
    /// other; tests that want to exercise priority ordering use both.
    #[allow(clippy::type_complexity)]
    fn make_reactor() -> (
        Reactor<Arc<SimulatedClock>, SimulatedRng, InMemoryWalBackend, InMemoryTransport>,
        mpsc::Sender<ReactorEvent>, // control
        mpsc::Sender<ReactorEvent>, // proposal
        mpsc::Receiver<CommittedBatch>,
    ) {
        let (control_tx, control_rx) = mpsc::channel(64);
        let (proposal_tx, proposal_rx) = mpsc::channel(64);
        let (commit_tx, commit_rx) = mpsc::channel(64);
        let clock = Arc::new(SimulatedClock::new());
        let wal = InMemoryWalBackend::new();
        let transport = InMemoryTransport::new();
        let reactor = Reactor::new(
            control_rx,
            proposal_rx,
            wal,
            clock,
            transport,
            commit_tx,
            Duration::from_millis(10),
        );
        (reactor, control_tx, proposal_tx, commit_rx)
    }

    fn make_shard(
        id: ConsensusStateId,
        clock: Arc<SimulatedClock>,
    ) -> ConsensusState<Arc<SimulatedClock>, SimulatedRng> {
        let membership = Membership::new([NodeId(1)]);
        let config = ShardConfig::default();
        let rng = SimulatedRng::new(42);
        ConsensusState::new(id, NodeId(1), membership, config, clock, rng, 0, None, 0)
    }

    /// Creates a shard, triggers election to become leader, and adds it to the
    /// reactor. Returns the shard's clock for further time manipulation.
    ///
    /// For single-node clusters this is deterministic: the shard transitions
    /// Follower → PreCandidate → Candidate → Leader in one call to
    /// `handle_election_timeout()`.
    fn add_leader_shard(
        reactor: &mut Reactor<
            Arc<SimulatedClock>,
            SimulatedRng,
            impl WalBackend,
            impl NetworkTransport,
        >,
        id: ConsensusStateId,
    ) {
        let clock = reactor.clock.clone();
        let mut shard = make_shard(id, clock);
        // Trigger election — single-node cluster immediately becomes leader.
        let actions = shard.handle_election_timeout();
        assert_eq!(
            shard.state(),
            crate::types::NodeState::Leader,
            "single-node shard must become leader after election timeout"
        );
        reactor.add_shard(id, shard);
        reactor.process_actions(actions);
        // Flush to persist the leader's no-op entry and advance commit.
        // The no-op entry's PersistEntries + Committed actions were already
        // collected by process_actions above, so flush() writes WAL and
        // dispatches committed batches, advancing commit_index.
    }

    // ── ConsensusState registration ──────────────────────────────────────────

    #[test]
    fn test_add_shard_increases_count() {
        let (mut reactor, _tx, _proposal_tx, _rx) = make_reactor();

        assert_eq!(reactor.shard_count(), 0);

        let clock = reactor.clock.clone();
        let shard = make_shard(ConsensusStateId(1), clock);
        reactor.add_shard(ConsensusStateId(1), shard);

        assert_eq!(reactor.shard_count(), 1);
    }

    // ── process_actions routing ────────────────────────────────────

    #[test]
    fn test_process_actions_non_vote_send_enqueues_to_outbox() {
        let (mut reactor, _tx, _proposal_tx, _rx) = make_reactor();

        let actions = vec![Action::Send {
            to: NodeId(2),
            shard: ConsensusStateId(1),
            msg: Message::TimeoutNow,
        }];

        reactor.process_actions(actions);

        assert_eq!(reactor.outbox.len(), 1);
    }

    #[test]
    fn test_process_actions_vote_send_bypasses_outbox() {
        let (mut reactor, _tx, _proposal_tx, _rx) = make_reactor();

        let actions = vec![Action::Send {
            to: NodeId(2),
            shard: ConsensusStateId(1),
            msg: Message::PreVoteRequest {
                term: 1,
                candidate_id: NodeId(1),
                last_log_index: 0,
                last_log_term: 0,
            },
        }];

        reactor.process_actions(actions);

        assert_eq!(reactor.outbox.len(), 0, "vote messages should bypass the outbox");
    }

    #[test]
    fn test_process_actions_persist_entries_collects_wal_frames() {
        let (mut reactor, _tx, _proposal_tx, _rx) = make_reactor();

        let entry = Entry {
            term: 1,
            index: 1,
            data: Arc::from(b"hello" as &[u8]),
            kind: crate::types::EntryKind::Normal,
        };

        reactor.process_actions(vec![Action::PersistEntries {
            shard: ConsensusStateId(1),
            entries: vec![entry],
        }]);

        assert_eq!(reactor.pending_wal_frames.len(), 1);
        assert_eq!(reactor.pending_wal_frames[0].shard_id, ConsensusStateId(1));
        assert_eq!(&*reactor.pending_wal_frames[0].data, b"hello");
    }

    #[test]
    fn test_process_actions_schedule_timer_registers_timer() {
        let (mut reactor, _tx, _proposal_tx, _rx) = make_reactor();

        let deadline = reactor.clock.now() + Duration::from_millis(100);

        reactor.process_actions(vec![Action::ScheduleTimer {
            shard: ConsensusStateId(1),
            kind: TimerKind::Election,
            deadline,
        }]);

        assert_eq!(reactor.timers.len(), 1);
        assert_eq!(reactor.timers.next_deadline(), Some(deadline));
    }

    #[test]
    fn test_process_actions_committed_collects_pending_commits() {
        let (mut reactor, _tx, _proposal_tx, _rx) = make_reactor();

        reactor.process_actions(vec![Action::Committed { shard: ConsensusStateId(1), up_to: 5 }]);

        assert_eq!(reactor.pending_commits.len(), 1);
        assert_eq!(reactor.pending_commits[0], (ConsensusStateId(1), 5));
    }

    #[test]
    fn test_process_actions_noop_variants_do_not_panic() {
        let (mut reactor, _tx, _proposal_tx, _rx) = make_reactor();

        reactor.process_actions(vec![
            Action::RenewLease { shard: ConsensusStateId(1) },
            Action::MembershipChanged {
                shard: ConsensusStateId(1),
                membership: Membership::new([NodeId(1)]),
            },
            Action::TriggerSnapshot {
                shard: ConsensusStateId(1),
                last_included_index: 5,
                last_included_term: 1,
            },
        ]);

        // No crash, no side effects.
        assert!(reactor.outbox.is_empty());
        assert!(reactor.pending_wal_frames.is_empty());
        assert!(reactor.pending_commits.is_empty());
    }

    // ── Flush ──────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_flush_persists_wal_frames_and_drains_buffer() {
        let (mut reactor, _tx, _proposal_tx, _rx) = make_reactor();

        let entry = Entry {
            term: 1,
            index: 1,
            data: Arc::from(b"data" as &[u8]),
            kind: crate::types::EntryKind::Normal,
        };
        reactor.process_actions(vec![Action::PersistEntries {
            shard: ConsensusStateId(1),
            entries: vec![entry],
        }]);
        assert_eq!(reactor.pending_wal_frames.len(), 1);

        reactor.flush().await;

        assert!(reactor.pending_wal_frames.is_empty());

        let frames = reactor.wal.read_frames(0).expect("read_frames should succeed");
        assert_eq!(frames.len(), 1);
        assert_eq!(&*frames[0].data, b"data");
    }

    #[tokio::test]
    async fn test_flush_resolves_committed_responses() {
        let (mut reactor, _tx, _proposal_tx, mut _rx) = make_reactor();

        // Set up a leader shard so proposals can commit.
        add_leader_shard(&mut reactor, ConsensusStateId(1));
        // Flush the no-op entry from leader election.
        reactor.flush().await;

        // The leader's no-op committed at index 1. Propose via the shard
        // to get a real entry at index 2 that also commits immediately
        // (single-node quorum).
        let shard = reactor.shards.get_mut(&ConsensusStateId(1)).unwrap();
        let actions = shard.handle_propose(b"test".to_vec()).unwrap();
        let entry_index = actions
            .iter()
            .find_map(|a| match a {
                Action::PersistEntries { entries, .. } => entries.last().map(|e| e.index),
                _ => None,
            })
            .unwrap();
        reactor.process_actions(actions);

        let (resp_tx, resp_rx) = oneshot::channel();
        reactor.pending_responses.push((ConsensusStateId(1), entry_index, resp_tx));

        reactor.flush().await;

        let result = resp_rx.await.expect("response channel should not be dropped");
        assert_eq!(result.unwrap(), entry_index);
    }

    #[tokio::test]
    async fn test_flush_dispatches_committed_batches() {
        let (mut reactor, _tx, _proposal_tx, mut commit_rx) = make_reactor();

        reactor.pending_commits.push((ConsensusStateId(1), 10));

        reactor.flush().await;

        let batch = commit_rx.try_recv().expect("should receive committed batch");
        assert_eq!(batch.shard, ConsensusStateId(1));
        assert!(batch.entries.is_empty());
    }

    #[tokio::test]
    async fn test_flush_drains_outbox() {
        let (mut reactor, _tx, _proposal_tx, _rx) = make_reactor();

        reactor.outbox.enqueue(NodeId(2), ConsensusStateId(1), Message::TimeoutNow);
        assert_eq!(reactor.outbox.len(), 1);

        reactor.flush().await;

        assert!(reactor.outbox.is_empty());
    }

    #[tokio::test]
    async fn test_flush_writes_checkpoint_when_commits_present() {
        use crate::wal_backend::WalBackend as _;

        let (mut reactor, _tx, _proposal_tx, _rx) = make_reactor();

        reactor.pending_commits.push((ConsensusStateId(1), 7));

        reactor.flush().await;

        let checkpoint = reactor.wal.last_checkpoint().expect("last_checkpoint should succeed");
        assert!(checkpoint.is_some(), "expected a checkpoint after flush with commits");
        assert_eq!(checkpoint.unwrap().committed_index, 7);
    }

    #[tokio::test]
    async fn test_flush_no_checkpoint_when_no_commits() {
        use crate::wal_backend::WalBackend as _;

        let (mut reactor, _tx, _proposal_tx, _rx) = make_reactor();

        reactor.flush().await;

        let checkpoint = reactor.wal.last_checkpoint().expect("last_checkpoint should succeed");
        assert!(checkpoint.is_none());
    }

    // ── Flush: WAL append failure ───────────────────────────────────

    #[tokio::test]
    async fn test_flush_wal_append_failure_rejects_pending_responses() {
        let (mut reactor, _tx, _proposal_tx, _rx) = make_reactor();

        // Queue a WAL frame and a pending response.
        let entry = Entry {
            term: 1,
            index: 1,
            data: Arc::from(b"data" as &[u8]),
            kind: crate::types::EntryKind::Normal,
        };
        reactor.process_actions(vec![Action::PersistEntries {
            shard: ConsensusStateId(1),
            entries: vec![entry],
        }]);
        let (resp_tx, resp_rx) = oneshot::channel();
        reactor.pending_responses.push((ConsensusStateId(1), 1, resp_tx));

        // Inject an append error.
        reactor.wal.inject_append_error(true);

        reactor.flush().await;

        // Pending response should receive an error (WalWriteError).
        let result = resp_rx.await.expect("response channel should not be dropped");
        assert!(
            matches!(result, Err(ConsensusError::WalWriteError)),
            "expected WalWriteError, got {result:?}"
        );

        // WAL buffer should be drained (frames were taken out before append).
        assert!(reactor.pending_wal_frames.is_empty());
        // Pending commits should be cleared.
        assert!(reactor.pending_commits.is_empty());
    }

    #[tokio::test]
    async fn test_flush_wal_append_failure_recovery_after_clear() {
        let (mut reactor, _tx, _proposal_tx, mut _rx) = make_reactor();

        // Set up a leader shard so responses can be resolved after commit.
        add_leader_shard(&mut reactor, ConsensusStateId(1));
        reactor.flush().await;

        // First flush: inject append error.
        let entry1 = Entry {
            term: 1,
            index: 99,
            data: Arc::from(b"first" as &[u8]),
            kind: crate::types::EntryKind::Normal,
        };
        reactor.process_actions(vec![Action::PersistEntries {
            shard: ConsensusStateId(1),
            entries: vec![entry1],
        }]);
        let (resp_tx1, resp_rx1) = oneshot::channel();
        reactor.pending_responses.push((ConsensusStateId(1), 99, resp_tx1));
        reactor.wal.inject_append_error(true);

        reactor.flush().await;

        let result1 = resp_rx1.await.expect("response channel should not be dropped");
        assert!(matches!(result1, Err(ConsensusError::WalWriteError)));

        // Second flush: clear the error, propose a real entry that commits
        // immediately (single-node quorum).
        reactor.wal.inject_append_error(false);

        let shard = reactor.shards.get_mut(&ConsensusStateId(1)).unwrap();
        let actions = shard.handle_propose(b"second".to_vec()).unwrap();
        let entry_index = actions
            .iter()
            .find_map(|a| match a {
                Action::PersistEntries { entries, .. } => entries.last().map(|e| e.index),
                _ => None,
            })
            .unwrap();
        reactor.process_actions(actions);

        let (resp_tx2, resp_rx2) = oneshot::channel();
        reactor.pending_responses.push((ConsensusStateId(1), entry_index, resp_tx2));

        reactor.flush().await;

        let result2 = resp_rx2.await.expect("response channel should not be dropped");
        assert_eq!(result2.unwrap(), entry_index);
    }

    // ── handle_event: missing shard ────────────────────────────────

    #[test]
    fn test_handle_event_propose_missing_shard_returns_shard_unavailable() {
        let (mut reactor, _tx, _proposal_tx, _rx) = make_reactor();

        let (resp_tx, mut resp_rx) = oneshot::channel();
        reactor.handle_event(ReactorEvent::Propose {
            shard: ConsensusStateId(99),
            data: vec![1, 2, 3],
            response: resp_tx,
        });

        let result = resp_rx.try_recv().expect("response should be available");
        assert!(matches!(
            result,
            Err(ConsensusError::ShardUnavailable { shard: ConsensusStateId(99) })
        ));
    }

    #[test]
    fn test_handle_event_propose_batch_missing_shard_returns_shard_unavailable() {
        let (mut reactor, _tx, _proposal_tx, _rx) = make_reactor();

        let (resp_tx, mut resp_rx) = oneshot::channel();
        reactor.handle_event(ReactorEvent::ProposeBatch {
            shard: ConsensusStateId(99),
            entries: vec![vec![1]],
            response: resp_tx,
        });

        let result = resp_rx.try_recv().expect("response should be available");
        assert!(matches!(
            result,
            Err(ConsensusError::ShardUnavailable { shard: ConsensusStateId(99) })
        ));
    }

    #[test]
    fn test_handle_event_membership_change_missing_shard_returns_shard_unavailable() {
        let (mut reactor, _tx, _proposal_tx, _rx) = make_reactor();

        let (resp_tx, mut resp_rx) = oneshot::channel();
        reactor.handle_event(ReactorEvent::MembershipChange {
            shard: ConsensusStateId(99),
            change: MembershipChange::AddLearner {
                node_id: NodeId(2),
                promotable: false,
                expected_conf_epoch: None,
            },
            response: resp_tx,
        });

        let result = resp_rx.try_recv().expect("response should be available");
        assert!(matches!(
            result,
            Err(ConsensusError::ShardUnavailable { shard: ConsensusStateId(99) })
        ));
    }

    #[test]
    fn test_handle_event_transfer_leader_missing_shard_returns_shard_unavailable() {
        let (mut reactor, _tx, _proposal_tx, _rx) = make_reactor();

        let (resp_tx, mut resp_rx) = oneshot::channel();
        reactor.handle_event(ReactorEvent::TransferLeader {
            shard: ConsensusStateId(99),
            target: NodeId(2),
            response: resp_tx,
        });

        let result = resp_rx.try_recv().expect("response should be available");
        assert!(matches!(
            result,
            Err(ConsensusError::ShardUnavailable { shard: ConsensusStateId(99) })
        ));
    }

    #[test]
    fn test_handle_event_trigger_snapshot_missing_shard_returns_shard_unavailable() {
        let (mut reactor, _tx, _proposal_tx, _rx) = make_reactor();

        let (resp_tx, mut resp_rx) = oneshot::channel();
        reactor.handle_event(ReactorEvent::TriggerSnapshot {
            shard: ConsensusStateId(99),
            response: resp_tx,
        });

        let result = resp_rx.try_recv().expect("response should be available");
        assert!(matches!(
            result,
            Err(ConsensusError::ShardUnavailable { shard: ConsensusStateId(99) })
        ));
    }

    // ── handle_event: read_index ───────────────────────────────────

    #[test]
    fn test_handle_event_read_index_known_shard_returns_commit_index() {
        let (mut reactor, _tx, _proposal_tx, _rx) = make_reactor();

        let clock = reactor.clock.clone();
        let shard = make_shard(ConsensusStateId(1), clock);
        reactor.add_shard(ConsensusStateId(1), shard);

        let (resp_tx, mut resp_rx) = oneshot::channel();
        reactor.handle_event(ReactorEvent::ReadIndex {
            shard: ConsensusStateId(1),
            response: resp_tx,
        });

        let result = resp_rx.try_recv().expect("response should be available");
        assert_eq!(result.unwrap(), 0);
    }

    #[test]
    fn test_handle_event_read_index_unknown_shard_returns_shard_unavailable() {
        let (mut reactor, _tx, _proposal_tx, _rx) = make_reactor();

        let (resp_tx, mut resp_rx) = oneshot::channel();
        reactor.handle_event(ReactorEvent::ReadIndex {
            shard: ConsensusStateId(99),
            response: resp_tx,
        });

        let result = resp_rx.try_recv().expect("response should be available");
        assert!(matches!(
            result,
            Err(ConsensusError::ShardUnavailable { shard: ConsensusStateId(99) })
        ));
    }

    // ── handle_event: failed shard ─────────────────────────────────

    #[test]
    fn test_handle_event_propose_failed_shard_returns_shard_unavailable() {
        let (mut reactor, _tx, _proposal_tx, _rx) = make_reactor();

        let clock = reactor.clock.clone();
        let mut shard = make_shard(ConsensusStateId(1), clock);
        shard.mark_failed();
        reactor.add_shard(ConsensusStateId(1), shard);

        let (resp_tx, mut resp_rx) = oneshot::channel();
        reactor.handle_event(ReactorEvent::Propose {
            shard: ConsensusStateId(1),
            data: vec![1, 2, 3],
            response: resp_tx,
        });

        let result = resp_rx.try_recv().expect("response should be available");
        assert!(matches!(
            result,
            Err(ConsensusError::ShardUnavailable { shard: ConsensusStateId(1) })
        ));
    }

    #[test]
    fn test_handle_event_propose_batch_failed_shard_returns_shard_unavailable() {
        let (mut reactor, _tx, _proposal_tx, _rx) = make_reactor();

        let clock = reactor.clock.clone();
        let mut shard = make_shard(ConsensusStateId(1), clock);
        shard.mark_failed();
        reactor.add_shard(ConsensusStateId(1), shard);

        let (resp_tx, mut resp_rx) = oneshot::channel();
        reactor.handle_event(ReactorEvent::ProposeBatch {
            shard: ConsensusStateId(1),
            entries: vec![vec![1], vec![2]],
            response: resp_tx,
        });

        let result = resp_rx.try_recv().expect("response should be available");
        assert!(matches!(
            result,
            Err(ConsensusError::ShardUnavailable { shard: ConsensusStateId(1) })
        ));
    }

    #[test]
    fn test_handle_event_membership_change_failed_shard_returns_shard_unavailable() {
        let (mut reactor, _tx, _proposal_tx, _rx) = make_reactor();

        let clock = reactor.clock.clone();
        let mut shard = make_shard(ConsensusStateId(1), clock);
        shard.mark_failed();
        reactor.add_shard(ConsensusStateId(1), shard);

        let (resp_tx, mut resp_rx) = oneshot::channel();
        reactor.handle_event(ReactorEvent::MembershipChange {
            shard: ConsensusStateId(1),
            change: MembershipChange::AddLearner {
                node_id: NodeId(2),
                promotable: false,
                expected_conf_epoch: None,
            },
            response: resp_tx,
        });

        let result = resp_rx.try_recv().expect("response should be available");
        assert!(matches!(
            result,
            Err(ConsensusError::ShardUnavailable { shard: ConsensusStateId(1) })
        ));
    }

    #[test]
    fn test_handle_event_transfer_leader_failed_shard_returns_shard_unavailable() {
        let (mut reactor, _tx, _proposal_tx, _rx) = make_reactor();

        let clock = reactor.clock.clone();
        let mut shard = make_shard(ConsensusStateId(1), clock);
        shard.mark_failed();
        reactor.add_shard(ConsensusStateId(1), shard);

        let (resp_tx, mut resp_rx) = oneshot::channel();
        reactor.handle_event(ReactorEvent::TransferLeader {
            shard: ConsensusStateId(1),
            target: NodeId(2),
            response: resp_tx,
        });

        let result = resp_rx.try_recv().expect("response should be available");
        assert!(matches!(
            result,
            Err(ConsensusError::ShardUnavailable { shard: ConsensusStateId(1) })
        ));
    }

    #[test]
    fn test_handle_event_peer_message_failed_shard_silently_dropped() {
        let (mut reactor, _tx, _proposal_tx, _rx) = make_reactor();

        let clock = reactor.clock.clone();
        let mut shard = make_shard(ConsensusStateId(1), clock);
        shard.mark_failed();
        reactor.add_shard(ConsensusStateId(1), shard);

        // PeerMessage has no response channel — verify it returns empty affected list
        // without modifying shard state (the shard remains failed, not doubly-failed).
        let affected = reactor.handle_event(ReactorEvent::PeerMessage {
            shard: ConsensusStateId(1),
            from: NodeId(2),
            message: crate::message::Message::VoteRequest {
                term: 1,
                candidate_id: NodeId(2),
                last_log_index: 0,
                last_log_term: 0,
            },
        });

        assert!(affected.is_empty(), "failed shard peer message should return no affected shards");
        assert!(
            reactor.shards.get(&ConsensusStateId(1)).unwrap().is_failed(),
            "shard should remain failed"
        );
    }

    // ── ConsensusState isolation ────────────────────────────────────────────

    #[test]
    fn test_healthy_shard_unaffected_when_another_is_failed() {
        use crate::types::NodeState;

        let (mut reactor, _tx, _proposal_tx, _rx) = make_reactor();
        let clock = reactor.clock.clone();

        let mut failed_shard = make_shard(ConsensusStateId(1), clock.clone());
        failed_shard.mark_failed();
        reactor.add_shard(ConsensusStateId(1), failed_shard);

        let healthy_shard = make_shard(ConsensusStateId(2), clock);
        reactor.add_shard(ConsensusStateId(2), healthy_shard);

        let (resp_tx, mut resp_rx) = oneshot::channel();
        reactor.handle_event(ReactorEvent::Propose {
            shard: ConsensusStateId(1),
            data: vec![1],
            response: resp_tx,
        });
        let result = resp_rx.try_recv().expect("response should be available");
        assert!(matches!(
            result,
            Err(ConsensusError::ShardUnavailable { shard: ConsensusStateId(1) })
        ));

        assert_eq!(reactor.shards.get(&ConsensusStateId(2)).unwrap().state(), NodeState::Follower);
    }

    // ── Shutdown ───────────────────────────────────────────────────

    #[tokio::test]
    async fn test_shutdown_exits_run_loop() {
        let (mut reactor, tx, _proposal_tx, _rx) = make_reactor();

        tx.send(ReactorEvent::Shutdown).await.expect("send should succeed");

        reactor.run().await;

        assert_eq!(reactor.shard_count(), 0);
    }

    // ── Async fsync lifecycle ─────────────────────────────────────

    #[allow(clippy::type_complexity)]
    fn make_async_reactor() -> (
        Reactor<Arc<SimulatedClock>, SimulatedRng, AsyncMockWalBackend, InMemoryTransport>,
        mpsc::Sender<ReactorEvent>, // control
        mpsc::Sender<ReactorEvent>, // proposal
        mpsc::Receiver<CommittedBatch>,
    ) {
        let (control_tx, control_rx) = mpsc::channel(64);
        let (proposal_tx, proposal_rx) = mpsc::channel(64);
        let (commit_tx, commit_rx) = mpsc::channel(64);
        let clock = Arc::new(SimulatedClock::new());
        let wal = AsyncMockWalBackend::new();
        let transport = InMemoryTransport::new();
        let reactor = Reactor::new(
            control_rx,
            proposal_rx,
            wal,
            clock,
            transport,
            commit_tx,
            Duration::from_millis(10),
        );
        (reactor, control_tx, proposal_tx, commit_rx)
    }

    #[tokio::test]
    async fn test_flush_async_fsync_defers_responses_until_completion() {
        let (mut reactor, _tx, _proposal_tx, mut _rx) = make_async_reactor();

        // Set up a leader shard for commit-based response resolution.
        add_leader_shard(&mut reactor, ConsensusStateId(1));
        // Mark the async WAL as completed so the leader's no-op flush goes
        // through, then reset for the actual test.
        reactor.wal.set_completed(true);
        reactor.flush().await;
        reactor.wal.set_completed(false);
        reactor.fsync_phase = FsyncPhase::Idle;

        // Propose a real entry that commits immediately (single-node quorum).
        let shard = reactor.shards.get_mut(&ConsensusStateId(1)).unwrap();
        let actions = shard.handle_propose(b"deferred".to_vec()).unwrap();
        let entry_index = actions
            .iter()
            .find_map(|a| match a {
                Action::PersistEntries { entries, .. } => entries.last().map(|e| e.index),
                _ => None,
            })
            .unwrap();
        reactor.process_actions(actions);

        let (resp_tx, mut resp_rx) = oneshot::channel();
        reactor.pending_responses.push((ConsensusStateId(1), entry_index, resp_tx));

        // Act: first flush submits async fsync, does NOT resolve responses.
        reactor.flush().await;

        // Assert: response not yet available, fsync phase is Submitted.
        assert_eq!(reactor.fsync_phase, FsyncPhase::Submitted);
        assert!(resp_rx.try_recv().is_err(), "response should be deferred during async fsync");

        // Act: mark fsync complete, flush again.
        reactor.wal.set_completed(true);
        reactor.flush().await;

        // Assert: response is now resolved.
        assert_eq!(reactor.fsync_phase, FsyncPhase::Idle);
        let result = resp_rx.await.expect("response channel should not be dropped");
        assert_eq!(result.unwrap(), entry_index);
    }

    #[tokio::test]
    async fn test_flush_async_fsync_flushes_outbox_while_waiting() {
        let (mut reactor, _tx, _proposal_tx, _rx) = make_async_reactor();

        // Arrange: queue a WAL frame (so flush enters the async path) and an outbox message.
        let entry = Entry {
            term: 1,
            index: 1,
            data: Arc::from(b"x" as &[u8]),
            kind: crate::types::EntryKind::Normal,
        };
        reactor.process_actions(vec![Action::PersistEntries {
            shard: ConsensusStateId(1),
            entries: vec![entry],
        }]);
        reactor.outbox.enqueue(NodeId(2), ConsensusStateId(1), Message::TimeoutNow);

        // Act: flush submits async fsync.
        reactor.flush().await;

        // Assert: outbox was flushed even though fsync is still pending.
        assert_eq!(reactor.fsync_phase, FsyncPhase::Submitted);
        assert!(reactor.outbox.is_empty(), "outbox should be flushed during async submit");
    }

    #[tokio::test]
    async fn test_flush_async_fsync_outbox_flushed_on_poll_while_waiting() {
        let (mut reactor, _tx, _proposal_tx, _rx) = make_async_reactor();

        // Arrange: enter Submitted state.
        let entry = Entry {
            term: 1,
            index: 1,
            data: Arc::from(b"x" as &[u8]),
            kind: crate::types::EntryKind::Normal,
        };
        reactor.process_actions(vec![Action::PersistEntries {
            shard: ConsensusStateId(1),
            entries: vec![entry],
        }]);
        reactor.flush().await;
        assert_eq!(reactor.fsync_phase, FsyncPhase::Submitted);

        // Arrange: enqueue a message while fsync is pending, keep completion false.
        reactor.outbox.enqueue(NodeId(3), ConsensusStateId(1), Message::TimeoutNow);

        // Act: flush again — poll returns false, so we only deliver outbound.
        reactor.flush().await;

        // Assert: outbox drained, still in Submitted phase.
        assert!(reactor.outbox.is_empty());
        assert_eq!(reactor.fsync_phase, FsyncPhase::Submitted);
    }

    #[tokio::test]
    async fn test_flush_async_fsync_submit_failure_rejects_responses() {
        let (mut reactor, _tx, _proposal_tx, _rx) = make_async_reactor();

        // Arrange: inject submit error.
        reactor.wal.inject_submit_error = true;
        let entry = Entry {
            term: 1,
            index: 1,
            data: Arc::from(b"fail" as &[u8]),
            kind: crate::types::EntryKind::Normal,
        };
        reactor.process_actions(vec![Action::PersistEntries {
            shard: ConsensusStateId(1),
            entries: vec![entry],
        }]);
        let (resp_tx, resp_rx) = oneshot::channel();
        reactor.pending_responses.push((ConsensusStateId(1), 1, resp_tx));
        reactor.pending_commits.push((ConsensusStateId(1), 1));

        // Act
        reactor.flush().await;

        // Assert: response rejected, commits cleared, phase stays Idle.
        let result = resp_rx.await.expect("channel should not be dropped");
        assert!(
            matches!(result, Err(ConsensusError::WalWriteError)),
            "expected WalWriteError, got {result:?}"
        );
        assert!(reactor.pending_commits.is_empty());
        assert_eq!(reactor.fsync_phase, FsyncPhase::Idle);
    }

    #[tokio::test]
    async fn test_flush_async_fsync_dispatches_commits_on_completion() {
        let (mut reactor, _tx, _proposal_tx, mut commit_rx) = make_async_reactor();

        // Arrange: add a shard so commit dispatch can look up entries.
        let clock = reactor.clock.clone();
        let shard = make_shard(ConsensusStateId(1), clock);
        reactor.add_shard(ConsensusStateId(1), shard);

        // Queue a commit.
        let entry = Entry {
            term: 1,
            index: 1,
            data: Arc::from(b"committed" as &[u8]),
            kind: crate::types::EntryKind::Normal,
        };
        reactor.process_actions(vec![Action::PersistEntries {
            shard: ConsensusStateId(1),
            entries: vec![entry],
        }]);
        reactor.pending_commits.push((ConsensusStateId(1), 1));

        // Act: first flush — submits async fsync, commits deferred.
        reactor.flush().await;
        assert!(commit_rx.try_recv().is_err(), "commits should be deferred during async fsync");

        // Act: complete fsync, flush again.
        reactor.wal.set_completed(true);
        reactor.flush().await;

        // Assert: committed batch dispatched.
        let batch = commit_rx.try_recv().expect("should receive committed batch");
        assert_eq!(batch.shard, ConsensusStateId(1));
    }

    // ── WAL sync failure under always-pipelined commit ─────────────

    #[tokio::test]
    async fn test_flush_wal_sync_failure_does_not_retract_acked_responses() {
        // Under always-on pipelined commit, client responses are resolved
        // *before* the fsync runs. A subsequent sync failure is logged and
        // metric-counted but CANNOT retract responses the reactor has
        // already signalled as success — the client has already seen them.
        // This test pins that invariant: a sync failure must not send
        // `WalWriteError` back to a caller whose response was already
        // drained from `pending_responses`.
        let (mut reactor, _tx, _proposal_tx, _rx) = make_reactor();

        let entry = Entry {
            term: 1,
            index: 1,
            data: Arc::from(b"data" as &[u8]),
            kind: crate::types::EntryKind::Normal,
        };
        reactor.process_actions(vec![Action::PersistEntries {
            shard: ConsensusStateId(1),
            entries: vec![entry],
        }]);
        reactor.pending_commits.push((ConsensusStateId(1), 1));
        reactor.wal.inject_sync_error("disk full");

        // Act: flush, triggering append → dispatch → resolve → sync fail.
        reactor.flush().await;

        // Assert: pending_commits drained (dispatched to apply worker);
        // the sync failure log + metric are the only surviving signal.
        assert!(reactor.pending_commits.is_empty());
    }

    // ── commit_tx channel closed ──────────────────────────────────

    #[tokio::test]
    async fn test_flush_commit_channel_closed_marks_shard_failed() {
        let (mut reactor, _tx, _proposal_tx, commit_rx) = make_reactor();

        // Arrange: register a shard, queue a commit, then close the channel.
        let clock = reactor.clock.clone();
        let shard = make_shard(ConsensusStateId(1), clock);
        reactor.add_shard(ConsensusStateId(1), shard);
        reactor.pending_commits.push((ConsensusStateId(1), 1));
        drop(commit_rx);

        // Act
        reactor.flush().await;

        // Assert: shard is marked as failed.
        assert_eq!(
            reactor.shards.get(&ConsensusStateId(1)).unwrap().state(),
            crate::types::NodeState::Failed
        );
    }

    #[tokio::test]
    async fn test_flush_async_commit_channel_closed_marks_shard_failed() {
        let (mut reactor, _tx, _proposal_tx, commit_rx) = make_async_reactor();

        // Arrange: register a shard, queue WAL + commit, drop receiver.
        let clock = reactor.clock.clone();
        let shard = make_shard(ConsensusStateId(1), clock);
        reactor.add_shard(ConsensusStateId(1), shard);

        let entry = Entry {
            term: 1,
            index: 1,
            data: Arc::from(b"x" as &[u8]),
            kind: crate::types::EntryKind::Normal,
        };
        reactor.process_actions(vec![Action::PersistEntries {
            shard: ConsensusStateId(1),
            entries: vec![entry],
        }]);
        reactor.pending_commits.push((ConsensusStateId(1), 1));
        drop(commit_rx);

        // Act: submit fsync, then complete it.
        reactor.flush().await;
        reactor.wal.set_completed(true);
        reactor.flush().await;

        // Assert: shard is marked as failed.
        assert_eq!(
            reactor.shards.get(&ConsensusStateId(1)).unwrap().state(),
            crate::types::NodeState::Failed
        );
    }

    // ── panic_message helper ──────────────────────────────────────

    #[test]
    fn test_panic_message_extracts_str_payload() {
        let payload: Box<dyn std::any::Any + Send> = Box::new("static str panic");

        let msg = panic_message(&payload);

        assert_eq!(msg, "static str panic");
    }

    #[test]
    fn test_panic_message_extracts_string_payload() {
        let payload: Box<dyn std::any::Any + Send> = Box::new(String::from("owned string panic"));

        let msg = panic_message(&payload);

        assert_eq!(msg, "owned string panic");
    }

    #[test]
    fn test_panic_message_unknown_payload_returns_default() {
        let payload: Box<dyn std::any::Any + Send> = Box::new(42_i32);

        let msg = panic_message(&payload);

        assert_eq!(msg, "unknown panic");
    }

    #[test]
    fn add_shard_with_committed_index_initializes_last_applied() {
        let (mut reactor, _tx, _proposal_tx, _rx) = make_reactor();
        let clock = Arc::new(SimulatedClock::new());
        let membership = Membership::new([NodeId(1)]);
        let config = ShardConfig::default();
        let rng = SimulatedRng::new(42);

        // Create shard with initial_committed_index = 10.
        let shard = ConsensusState::new(
            ConsensusStateId(1),
            NodeId(1),
            membership,
            config,
            clock,
            rng,
            1,
            None,
            10,
        );

        reactor.add_shard(ConsensusStateId(1), shard);

        // The reactor should have initialized last_applied to 10.
        assert_eq!(
            reactor.last_applied.get(&ConsensusStateId(1)).copied(),
            Some(10),
            "reactor must initialize last_applied from shard commit_index"
        );
    }

    #[test]
    fn add_shard_with_zero_committed_index_does_not_set_last_applied() {
        let (mut reactor, _tx, _proposal_tx, _rx) = make_reactor();
        let shard = make_shard(ConsensusStateId(1), Arc::new(SimulatedClock::new()));

        reactor.add_shard(ConsensusStateId(1), shard);

        // A fresh shard with commit_index=0 should not have an entry.
        assert_eq!(
            reactor.last_applied.get(&ConsensusStateId(1)),
            None,
            "fresh shard should not have last_applied entry"
        );
    }

    // ── Commit-deferred response resolution ─────────────────────────

    #[tokio::test]
    async fn test_response_not_sent_before_commit() {
        let (mut reactor, _tx, _proposal_tx, mut _rx) = make_reactor();

        // Register a follower shard (not leader — proposals would fail at
        // handle_propose, but we're testing the flush path directly).
        let clock = reactor.clock.clone();
        let shard = make_shard(ConsensusStateId(1), clock);
        reactor.add_shard(ConsensusStateId(1), shard);

        // Manually push a pending response with an index above commit_index (0).
        let (resp_tx, mut resp_rx) = oneshot::channel();
        reactor.pending_responses.push((ConsensusStateId(1), 5, resp_tx));

        // Flush — the entry at index 5 is NOT committed (commit_index=0) and
        // the shard is not leader, so the response should be rejected.
        reactor.flush().await;

        let result = resp_rx.try_recv().expect("response should be available");
        assert!(
            matches!(result, Err(ConsensusError::NotLeader)),
            "expected NotLeader for follower shard, got {result:?}"
        );
    }

    #[tokio::test]
    async fn test_single_node_proposal_resolves_in_one_flush() {
        let (mut reactor, _tx, _proposal_tx, mut _rx) = make_reactor();

        // Set up a leader shard (single-node cluster commits immediately).
        add_leader_shard(&mut reactor, ConsensusStateId(1));
        reactor.flush().await;

        // Propose via the shard — single-node quorum means commit_index
        // advances in the same action batch.
        let shard = reactor.shards.get_mut(&ConsensusStateId(1)).unwrap();
        let actions = shard.handle_propose(b"single-node".to_vec()).unwrap();
        let entry_index = actions
            .iter()
            .find_map(|a| match a {
                Action::PersistEntries { entries, .. } => entries.last().map(|e| e.index),
                _ => None,
            })
            .unwrap();
        reactor.process_actions(actions);

        let (resp_tx, resp_rx) = oneshot::channel();
        reactor.pending_responses.push((ConsensusStateId(1), entry_index, resp_tx));

        // Single flush should resolve the response.
        reactor.flush().await;

        let result = resp_rx.await.expect("response channel should not be dropped");
        assert_eq!(
            result.unwrap(),
            entry_index,
            "single-node proposal should resolve in one flush cycle"
        );
    }

    #[tokio::test]
    async fn test_leadership_loss_rejects_pending_responses() {
        let (mut reactor, _tx, _proposal_tx, mut _rx) = make_reactor();

        // Create a 3-node shard so proposals need quorum (2 of 3) to commit.
        let clock = reactor.clock.clone();
        let membership = Membership::new([NodeId(1), NodeId(2), NodeId(3)]);
        let config = ShardConfig::default();
        let rng = SimulatedRng::new(42);
        let mut shard = ConsensusState::new(
            ConsensusStateId(1),
            NodeId(1),
            membership,
            config,
            clock,
            rng,
            0,
            None,
            0,
        );

        // Force the shard to become leader by simulating the election process.
        // Step 1: Pre-vote (term 0 -> PreCandidate).
        let actions = shard.handle_election_timeout();
        reactor.process_actions(actions);

        // Step 2: Grant pre-votes from peer.
        let actions = shard
            .handle_message(NodeId(2), Message::PreVoteResponse { term: 1, vote_granted: true });
        reactor.process_actions(actions);

        // Step 3: Grant real vote from peer.
        let actions =
            shard.handle_message(NodeId(2), Message::VoteResponse { term: 1, vote_granted: true });
        reactor.process_actions(actions);
        assert_eq!(shard.state(), crate::types::NodeState::Leader);

        reactor.add_shard(ConsensusStateId(1), shard);
        reactor.flush().await;

        // Propose an entry — it will NOT be committed because peer has not acked.
        let shard = reactor.shards.get_mut(&ConsensusStateId(1)).unwrap();
        let actions = shard.handle_propose(b"will-lose-leader".to_vec()).unwrap();
        let entry_index = actions
            .iter()
            .find_map(|a| match a {
                Action::PersistEntries { entries, .. } => entries.last().map(|e| e.index),
                _ => None,
            })
            .unwrap();
        reactor.process_actions(actions);

        let (resp_tx, resp_rx) = oneshot::channel();
        reactor.pending_responses.push((ConsensusStateId(1), entry_index, resp_tx));

        // Verify the entry is NOT committed (no quorum ack).
        assert!(
            reactor.shards.get(&ConsensusStateId(1)).unwrap().commit_index() < entry_index,
            "entry should not be committed without quorum"
        );

        // Simulate leadership loss: deliver a message from a higher-term leader.
        let shard = reactor.shards.get_mut(&ConsensusStateId(1)).unwrap();
        let current_term = shard.current_term();
        let actions = shard.handle_message(
            NodeId(2),
            Message::AppendEntries {
                term: current_term + 1,
                leader_id: NodeId(2),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: Arc::from(Vec::<Entry>::new()),
                leader_commit: 0,
                closed_ts_nanos: 0,
            },
        );
        reactor.process_actions(actions);

        // Verify the shard is no longer leader.
        assert_ne!(
            reactor.shards.get(&ConsensusStateId(1)).unwrap().state(),
            crate::types::NodeState::Leader,
            "shard should have stepped down after higher-term message"
        );

        // Flush — the pending response should be rejected with NotLeader.
        reactor.flush().await;

        let result = resp_rx.await.expect("response channel should not be dropped");
        assert!(
            matches!(result, Err(ConsensusError::NotLeader)),
            "expected NotLeader after leadership loss, got {result:?}"
        );
    }

    #[tokio::test]
    async fn test_uncommitted_response_stays_pending_across_flushes() {
        let (mut reactor, _tx, _proposal_tx, mut _rx) = make_reactor();

        // Set up a leader shard.
        add_leader_shard(&mut reactor, ConsensusStateId(1));
        reactor.flush().await;

        // Manually push a response for a high index that hasn't been committed.
        let (resp_tx, mut resp_rx) = oneshot::channel();
        reactor.pending_responses.push((ConsensusStateId(1), 999, resp_tx));

        // Flush — index 999 is above commit_index, but shard is still leader.
        reactor.flush().await;

        // Response should still be pending (not resolved, not rejected).
        assert!(
            resp_rx.try_recv().is_err(),
            "response for uncommitted entry should stay pending while leader"
        );
        assert_eq!(
            reactor.pending_responses.len(),
            1,
            "uncommitted response should remain in pending_responses"
        );
    }

    #[tokio::test]
    async fn test_missing_shard_rejects_pending_response() {
        let (mut reactor, _tx, _proposal_tx, _rx) = make_reactor();

        // Push a response for a shard that doesn't exist.
        let (resp_tx, resp_rx) = oneshot::channel();
        reactor.pending_responses.push((ConsensusStateId(99), 1, resp_tx));

        reactor.flush().await;

        let result = resp_rx.await.expect("response channel should not be dropped");
        assert!(
            matches!(result, Err(ConsensusError::NotLeader)),
            "expected NotLeader for missing shard, got {result:?}"
        );
    }

    // ── O6 pause / resume primitives ──────────────────────────────────

    /// Recording wake notifier — captures shard IDs the reactor passes to it
    /// so tests can assert the wake hook fired exactly once per paused
    /// PeerMessage.
    #[derive(Default)]
    struct RecordingWakeNotifier {
        seen: parking_lot::Mutex<Vec<ConsensusStateId>>,
    }

    /// Recording snapshot coordinator — captures the trigger payload so
    /// Stage 2 tests can assert the no-op reactor arm now dispatches
    /// through the coordinator.
    #[derive(Default)]
    struct RecordingSnapshotCoordinator {
        seen: parking_lot::Mutex<Vec<(ConsensusStateId, u64, u64)>>,
    }

    impl RecordingSnapshotCoordinator {
        fn new() -> Self {
            Self::default()
        }

        fn calls(&self) -> Vec<(ConsensusStateId, u64, u64)> {
            self.seen.lock().clone()
        }
    }

    impl crate::snapshot_coordinator::SnapshotCoordinator for RecordingSnapshotCoordinator {
        fn on_trigger_snapshot(
            &self,
            shard_id: ConsensusStateId,
            last_included_index: u64,
            last_included_term: u64,
        ) {
            self.seen.lock().push((shard_id, last_included_index, last_included_term));
        }
    }

    impl RecordingWakeNotifier {
        fn new() -> Self {
            Self::default()
        }

        fn calls(&self) -> Vec<ConsensusStateId> {
            self.seen.lock().clone()
        }
    }

    impl ShardWakeNotifier for RecordingWakeNotifier {
        fn on_peer_message_for_paused_shard(&self, shard_id: ConsensusStateId) {
            self.seen.lock().push(shard_id);
        }
    }

    /// Builds a reactor wired up with a shared pause map and a recording
    /// wake notifier so tests can assert both the drop + the notifier
    /// invocation.
    #[allow(clippy::type_complexity)]
    fn make_reactor_with_wake_notifier(
        notifier: Arc<RecordingWakeNotifier>,
    ) -> (
        Reactor<Arc<SimulatedClock>, SimulatedRng, InMemoryWalBackend, InMemoryTransport>,
        mpsc::Sender<ReactorEvent>,
        mpsc::Sender<ReactorEvent>,
        mpsc::Receiver<CommittedBatch>,
        Arc<PauseMap>,
    ) {
        let (control_tx, control_rx) = mpsc::channel(64);
        let (proposal_tx, proposal_rx) = mpsc::channel(64);
        let (commit_tx, commit_rx) = mpsc::channel(64);
        let clock = Arc::new(SimulatedClock::new());
        let wal = InMemoryWalBackend::new();
        let transport = InMemoryTransport::new();
        let pause_state = Arc::new(PauseMap::new());
        let notifier_dyn: Arc<dyn ShardWakeNotifier> = notifier;
        let reactor = Reactor::with_wake_notifier(
            control_rx,
            proposal_rx,
            wal,
            clock,
            transport,
            commit_tx,
            Duration::from_millis(10),
            Arc::clone(&pause_state),
            notifier_dyn,
        );
        (reactor, control_tx, proposal_tx, commit_rx, pause_state)
    }

    #[test]
    fn test_paused_shard_drops_send_action_from_outbox() {
        let (mut reactor, _tx, _proposal_tx, _rx) = make_reactor();

        // Mark shard 1 as paused via the shared pause map.
        reactor.pause_state.insert(ConsensusStateId(1), ());

        reactor.process_actions(vec![Action::Send {
            to: NodeId(2),
            shard: ConsensusStateId(1),
            msg: Message::TimeoutNow,
        }]);

        assert_eq!(
            reactor.outbox.len(),
            0,
            "Send action from a paused shard must be dropped before reaching the outbox",
        );
    }

    #[test]
    fn test_paused_vote_send_is_also_dropped() {
        let (mut reactor, _tx, _proposal_tx, _rx) = make_reactor();
        reactor.pause_state.insert(ConsensusStateId(1), ());

        // Vote requests normally bypass the outbox and go straight to
        // the transport. Pausing must drop them too — a paused shard
        // should not be electioneering.
        reactor.process_actions(vec![Action::Send {
            to: NodeId(2),
            shard: ConsensusStateId(1),
            msg: Message::PreVoteRequest {
                term: 1,
                candidate_id: NodeId(1),
                last_log_index: 0,
                last_log_term: 0,
            },
        }]);

        assert_eq!(reactor.outbox.len(), 0);
        assert_eq!(
            reactor.transport.sent_count(),
            0,
            "vote request from a paused shard must not reach the transport",
        );
    }

    #[test]
    fn test_resumed_shard_send_action_reaches_outbox() {
        let (mut reactor, _tx, _proposal_tx, _rx) = make_reactor();

        // Pause then resume — pause flag cleared, sends flow normally.
        reactor.pause_state.insert(ConsensusStateId(1), ());
        reactor.pause_state.remove(&ConsensusStateId(1));

        reactor.process_actions(vec![Action::Send {
            to: NodeId(2),
            shard: ConsensusStateId(1),
            msg: Message::TimeoutNow,
        }]);

        assert_eq!(reactor.outbox.len(), 1, "Send action after resume must reach the outbox",);
    }

    #[test]
    fn test_paused_shard_skips_expired_election_timer() {
        let (mut reactor, _tx, _proposal_tx, _rx) = make_reactor();

        let clock = reactor.clock.clone();
        let shard = make_shard(ConsensusStateId(1), clock.clone());
        reactor.add_shard(ConsensusStateId(1), shard);

        // Pause the shard before the election timer fires.
        reactor.pause_state.insert(ConsensusStateId(1), ());

        // Advance the clock past the election timeout — without pause,
        // process_expired_timers would dispatch and the single-node shard
        // would self-elect. With pause set, the timer is skipped silently.
        clock.advance(Duration::from_secs(5));

        let _ = reactor.process_expired_timers();

        // The shard must NOT have transitioned to leader.
        assert_ne!(
            reactor.shards.get(&ConsensusStateId(1)).unwrap().state(),
            crate::types::NodeState::Leader,
            "paused shard must not self-elect via skipped election timer",
        );
        // No outbound traffic from the suppressed timer.
        assert_eq!(reactor.outbox.len(), 0);
        assert_eq!(reactor.transport.sent_count(), 0);
    }

    #[tokio::test]
    async fn test_paused_shard_drops_peer_message_and_invokes_notifier() {
        let notifier = Arc::new(RecordingWakeNotifier::new());
        let (mut reactor, _tx, _proposal_tx, _rx, pause_state) =
            make_reactor_with_wake_notifier(Arc::clone(&notifier));

        let clock = reactor.clock.clone();
        let shard = make_shard(ConsensusStateId(1), clock);
        reactor.add_shard(ConsensusStateId(1), shard);

        // Mark the shard as paused.
        pause_state.insert(ConsensusStateId(1), ());

        // Capture commit_index before — a delivered AppendEntries on a
        // single-node shard would advance state. We assert it does not.
        let term_before = reactor.shards.get(&ConsensusStateId(1)).unwrap().current_term();

        let _ = reactor.handle_event(ReactorEvent::PeerMessage {
            shard: ConsensusStateId(1),
            from: NodeId(2),
            message: Message::AppendEntries {
                term: 99,
                leader_id: NodeId(2),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: Arc::from(Vec::<Entry>::new()),
                leader_commit: 0,
                closed_ts_nanos: 0,
            },
        });

        // Notifier was invoked exactly once with the paused shard's id.
        assert_eq!(notifier.calls(), vec![ConsensusStateId(1)]);

        // Term did NOT advance — the message was dropped before reaching
        // the shard. (A delivered AppendEntries with term 99 would have
        // bumped current_term.)
        assert_eq!(
            reactor.shards.get(&ConsensusStateId(1)).unwrap().current_term(),
            term_before,
            "paused shard must not observe the dropped peer message",
        );
    }

    #[tokio::test]
    async fn test_resumed_shard_processes_subsequent_send_and_message() {
        let notifier = Arc::new(RecordingWakeNotifier::new());
        let (mut reactor, _tx, _proposal_tx, _rx, pause_state) =
            make_reactor_with_wake_notifier(Arc::clone(&notifier));

        let clock = reactor.clock.clone();
        let shard = make_shard(ConsensusStateId(1), clock);
        reactor.add_shard(ConsensusStateId(1), shard);

        // Pause then resume.
        pause_state.insert(ConsensusStateId(1), ());
        pause_state.remove(&ConsensusStateId(1));

        // Send action flows normally.
        reactor.process_actions(vec![Action::Send {
            to: NodeId(2),
            shard: ConsensusStateId(1),
            msg: Message::TimeoutNow,
        }]);
        assert_eq!(reactor.outbox.len(), 1, "post-resume Send must reach outbox");

        // Peer message reaches the shard normally — observable via term bump
        // (an AppendEntries with a higher term updates current_term).
        let term_before = reactor.shards.get(&ConsensusStateId(1)).unwrap().current_term();
        let _ = reactor.handle_event(ReactorEvent::PeerMessage {
            shard: ConsensusStateId(1),
            from: NodeId(2),
            message: Message::AppendEntries {
                term: term_before + 5,
                leader_id: NodeId(2),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: Arc::from(Vec::<Entry>::new()),
                leader_commit: 0,
                closed_ts_nanos: 0,
            },
        });
        assert_eq!(
            reactor.shards.get(&ConsensusStateId(1)).unwrap().current_term(),
            term_before + 5,
            "resumed shard must observe delivered peer messages",
        );
        assert!(
            notifier.calls().is_empty(),
            "wake notifier must not fire for resumed (active) shards",
        );
    }

    #[tokio::test]
    async fn test_pause_event_unknown_shard_returns_shard_unavailable() {
        let (mut reactor, _tx, _proposal_tx, _rx) = make_reactor();

        let (resp_tx, resp_rx) = oneshot::channel();
        let _ = reactor.handle_event(ReactorEvent::PauseShard {
            shard: ConsensusStateId(99),
            response: resp_tx,
        });

        let result = resp_rx.await.expect("response channel should not be dropped");
        assert!(
            matches!(result, Err(ConsensusError::ShardUnavailable { shard: ConsensusStateId(99) })),
            "expected ShardUnavailable, got {result:?}",
        );
        assert!(!reactor.pause_state.contains_key(&ConsensusStateId(99)));
    }

    #[tokio::test]
    async fn test_pause_event_idempotent_for_known_shard() {
        let (mut reactor, _tx, _proposal_tx, _rx) = make_reactor();

        let clock = reactor.clock.clone();
        let shard = make_shard(ConsensusStateId(1), clock);
        reactor.add_shard(ConsensusStateId(1), shard);

        for _ in 0..2 {
            let (resp_tx, resp_rx) = oneshot::channel();
            let _ = reactor.handle_event(ReactorEvent::PauseShard {
                shard: ConsensusStateId(1),
                response: resp_tx,
            });
            assert!(resp_rx.await.unwrap().is_ok());
        }
        assert!(reactor.pause_state.contains_key(&ConsensusStateId(1)));
    }

    // ── Stage 2 snapshot coordinator dispatch ──────────────────────────

    #[allow(clippy::type_complexity)]
    fn make_reactor_with_snapshot_coordinator(
        coord: Arc<RecordingSnapshotCoordinator>,
    ) -> (
        Reactor<Arc<SimulatedClock>, SimulatedRng, InMemoryWalBackend, InMemoryTransport>,
        mpsc::Sender<ReactorEvent>,
        mpsc::Sender<ReactorEvent>,
        mpsc::Receiver<CommittedBatch>,
    ) {
        let (control_tx, control_rx) = mpsc::channel(64);
        let (proposal_tx, proposal_rx) = mpsc::channel(64);
        let (commit_tx, commit_rx) = mpsc::channel(64);
        let clock = Arc::new(SimulatedClock::new());
        let wal = InMemoryWalBackend::new();
        let transport = InMemoryTransport::new();
        let pause_state = Arc::new(PauseMap::new());
        let coord_dyn: Arc<dyn crate::snapshot_coordinator::SnapshotCoordinator> = coord;
        let reactor = Reactor::with_coordinators(
            control_rx,
            proposal_rx,
            wal,
            clock,
            transport,
            commit_tx,
            Duration::from_millis(10),
            pause_state,
            Arc::new(crate::wake::NoopShardWakeNotifier),
            coord_dyn,
        );
        (reactor, control_tx, proposal_tx, commit_rx)
    }

    /// Verifies that processing an `Action::TriggerSnapshot` invokes the
    /// installed `SnapshotCoordinator` exactly once with the action's
    /// `(shard, last_included_index, last_included_term)` payload.
    /// This is the contract Stage 2 introduces: the previously-no-op
    /// arm now dispatches to the coordinator.
    #[test]
    fn test_trigger_snapshot_action_dispatches_to_coordinator() {
        let coord = Arc::new(RecordingSnapshotCoordinator::new());
        let (mut reactor, _tx, _proposal_tx, _rx) =
            make_reactor_with_snapshot_coordinator(Arc::clone(&coord));

        reactor.process_actions(vec![Action::TriggerSnapshot {
            shard: ConsensusStateId(7),
            last_included_index: 42,
            last_included_term: 3,
        }]);

        assert_eq!(coord.calls(), vec![(ConsensusStateId(7), 42, 3)]);
        // The dispatch is fire-and-forget; no outbox / WAL effects.
        assert!(reactor.outbox.is_empty());
        assert!(reactor.pending_wal_frames.is_empty());
        assert!(reactor.pending_commits.is_empty());
    }

    /// The coordinator is invoked once per action — back-to-back triggers
    /// preserve order and count.
    #[test]
    fn test_multiple_trigger_snapshot_actions_each_dispatch() {
        let coord = Arc::new(RecordingSnapshotCoordinator::new());
        let (mut reactor, _tx, _proposal_tx, _rx) =
            make_reactor_with_snapshot_coordinator(Arc::clone(&coord));

        reactor.process_actions(vec![
            Action::TriggerSnapshot {
                shard: ConsensusStateId(1),
                last_included_index: 10,
                last_included_term: 2,
            },
            Action::TriggerSnapshot {
                shard: ConsensusStateId(2),
                last_included_index: 20,
                last_included_term: 2,
            },
            Action::TriggerSnapshot {
                shard: ConsensusStateId(1),
                last_included_index: 30,
                last_included_term: 4,
            },
        ]);

        assert_eq!(
            coord.calls(),
            vec![
                (ConsensusStateId(1), 10, 2),
                (ConsensusStateId(2), 20, 2),
                (ConsensusStateId(1), 30, 4),
            ],
        );
    }
}
