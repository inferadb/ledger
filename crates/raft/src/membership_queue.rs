//! Per-organization rate-limited queue for vault membership conf-changes.
//!
//! [`MembershipQueue`] is the **M2 deliverable** of the Phase 5 centralised
//! membership plan
//! (`docs/superpowers/specs/2026-04-27-phase-5-centralised-membership.md`).
//! It is the primitive that later phases (M3 watcher, M4 dispatcher, M5
//! per-vault timeout) build on top of.
//!
//! ## What it does
//!
//! Today, [`crate::raft_manager::RaftManager::cascade_membership_to_children`]
//! fans an `AddLearner` / `PromoteVoter` / `Remove` change out to every
//! per-organization and per-vault Raft shard in the affected region in
//! parallel. At 1000 vaults × 3 voters per org, that is ~3000 conf-changes in
//! flight simultaneously, each capable of triggering a snapshot RPC if the
//! vault's log is stale. The resulting **snapshot storm** dominates cluster
//! bandwidth and CPU for minutes.
//!
//! Phase 5 layers a per-org `MembershipQueue` between the cascade primitive
//! and the per-vault shards. The queue serialises in-flight conf-changes
//! through a [`Semaphore`] of capacity `max_concurrent_snapshot_producing`
//! (default `2`, matching TiKV's `max_snapshot_per_store` default), and
//! buffers backlog up to `max_backlog` (default `1000`) before refusing new
//! enqueues. Cancellation is wired through a [`CancellationToken`] so a
//! graceful shutdown drains the queue cleanly without leaking pending
//! tasks.
//!
//! ## What M2 ships
//!
//! Just the primitive. The cascade in
//! [`crate::raft_manager::RaftManager::cascade_membership_to_children`]
//! still fires synchronously through the existing path; the queue
//! receives no entries yet. M3 wires the
//! `RegionMembershipWatcher` task that produces entries; M4 wires the
//! `MembershipDispatcher` consumer that drains them.
//!
//! ## Design notes
//!
//! The queue exposes three primitive operations:
//!
//! - [`MembershipQueue::enqueue`] — async; awaits a semaphore permit (blocking when in-flight is at
//!   the configured cap), then pushes into the FIFO backlog. Returns
//!   [`MembershipQueueError::Backlog`] when the backlog is full and
//!   [`MembershipQueueError::ShuttingDown`] after [`MembershipQueue::shutdown`] has been called.
//! - [`MembershipQueue::take_next`] — async; awaits the next FIFO entry, releasing the semaphore
//!   permit so a blocked enqueuer can proceed. Returns `None` once the queue is shut down and
//!   drained.
//! - [`MembershipQueue::shutdown`] — synchronously cancels the [`CancellationToken`], waking
//!   pending enqueuers and takers so they observe the shutdown signal and unwind.
//!
//! ## Locking
//!
//! The internal `VecDeque` is guarded by a `parking_lot::Mutex` (consistent
//! with the rest of `raft`'s synchronous primitives — see
//! [`crate::leader_lease::LeaderLease`]). The lock is held only for the
//! push/pop operations, never across an `await`.

use std::{collections::VecDeque, sync::Arc};

use inferadb_ledger_types::VaultId;
use parking_lot::Mutex;
use snafu::{Backtrace, Snafu};
use tokio::sync::{Notify, Semaphore};
use tokio_util::sync::CancellationToken;

/// Default cap on concurrent snapshot-producing conf-changes per
/// organization. Matches TiKV's `max_snapshot_per_store = 2` default.
pub const DEFAULT_MAX_CONCURRENT_SNAPSHOT_PRODUCING: usize = 2;

/// Default cap on the backlog of pending entries. The semaphore at
/// `max_concurrent_snapshot_producing` keeps the realised depth far below
/// this in steady state — the backlog cap only matters as a defence
/// against a watcher producing entries faster than the dispatcher can
/// consume them.
pub const DEFAULT_MAX_BACKLOG: usize = 1000;

/// Failure modes for [`MembershipQueue`] operations.
///
/// These are deliberately lightweight — backlog overflow and shutdown are
/// not bugs but signals that the caller (the Phase 5 watcher / dispatcher
/// in M3 / M4) needs to back off or drain.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum MembershipQueueError {
    /// Pending backlog is at `max_backlog`; the caller must wait for
    /// [`MembershipQueue::take_next`] to drain entries before retrying.
    #[snafu(display(
        "Membership queue backlog is full ({backlog}/{max_backlog}); refusing to enqueue"
    ))]
    Backlog {
        /// Current backlog depth at the time of refusal.
        backlog: usize,
        /// Configured backlog cap.
        max_backlog: usize,
        /// Captured backtrace.
        backtrace: Backtrace,
    },

    /// [`MembershipQueue::shutdown`] has been called; the queue rejects
    /// further enqueues and pending takers receive `None`.
    #[snafu(display("Membership queue is shutting down; refusing to enqueue"))]
    ShuttingDown {
        /// Captured backtrace.
        backtrace: Backtrace,
    },
}

/// Result alias for queue operations.
pub type Result<T> = std::result::Result<T, MembershipQueueError>;

/// A single membership-change entry queued for a vault group.
///
/// Variants mirror
/// [`crate::raft_manager::CascadeMembershipAction`] plus the dispatch
/// context (`vault_id`, `node_id`, `addr`) the M4 dispatcher needs to
/// invoke `apply_cascade_action` on the per-vault shard. The address is
/// only required for `AddLearner` — the remaining variants reuse the
/// org's existing peer-channel state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MembershipChangeRequest {
    /// Add `node_id` (reachable at `addr`) as a learner to the vault.
    AddLearnerToVault {
        /// Target vault.
        vault_id: VaultId,
        /// Node to add as a learner.
        node_id: u64,
        /// Network address of the learner. Required so the per-vault
        /// shard can register the peer in its consensus transport.
        addr: String,
    },
    /// Promote an existing learner on the vault to voter.
    PromoteLearnerInVault {
        /// Target vault.
        vault_id: VaultId,
        /// Node to promote.
        node_id: u64,
    },
    /// Remove `node_id` from the vault.
    RemoveFromVault {
        /// Target vault.
        vault_id: VaultId,
        /// Node to remove.
        node_id: u64,
    },
}

impl MembershipChangeRequest {
    /// Returns the vault this request targets.
    pub fn vault_id(&self) -> VaultId {
        match self {
            Self::AddLearnerToVault { vault_id, .. }
            | Self::PromoteLearnerInVault { vault_id, .. }
            | Self::RemoveFromVault { vault_id, .. } => *vault_id,
        }
    }

    /// Returns the node id this request targets.
    pub fn node_id(&self) -> u64 {
        match self {
            Self::AddLearnerToVault { node_id, .. }
            | Self::PromoteLearnerInVault { node_id, .. }
            | Self::RemoveFromVault { node_id, .. } => *node_id,
        }
    }
}

/// Snapshot of the [`MembershipQueue`] configuration captured at
/// construction. Returned by [`MembershipQueue::stats`] for telemetry.
#[derive(Debug, Clone, Copy)]
pub struct MembershipQueueStats {
    /// Configured cap on concurrent in-flight conf-changes.
    pub max_concurrent_snapshot_producing: usize,
    /// Configured cap on backlog depth.
    pub max_backlog: usize,
    /// Current backlog depth.
    pub pending: usize,
    /// Currently in-flight (acquired permits).
    pub in_flight: usize,
}

/// Rate-limited queue for per-vault conf-changes.
///
/// One instance per organization-tier
/// [`InnerGroup`](crate::raft_manager::InnerGroup) (constructed and
/// surfaced through the [`OrganizationGroup`](crate::raft_manager::OrganizationGroup)
/// tier wrapper). M2 only adds the primitive — the watcher and
/// dispatcher that produce / consume entries land in M3 / M4.
///
/// See the module-level docs for the full design.
pub struct MembershipQueue {
    /// Cap on in-flight conf-changes. Each [`Self::enqueue`] acquires a
    /// permit (forgotten so it persists across the await boundary);
    /// each [`Self::take_next`] releases a permit by calling
    /// [`Semaphore::add_permits`].
    semaphore: Arc<Semaphore>,
    /// Captured copy of the configured semaphore cap. `Semaphore` does
    /// not expose its initial capacity after construction, so we record
    /// it ourselves to power [`Self::in_flight`] / [`Self::stats`].
    max_concurrent_snapshot_producing: usize,
    /// FIFO backlog of pending entries. Guarded by a `parking_lot::Mutex`
    /// — held only for push / pop, never across an `await`.
    queue: Arc<Mutex<VecDeque<MembershipChangeRequest>>>,
    /// Hard upper bound on the backlog depth. Returns
    /// [`MembershipQueueError::Backlog`] when exceeded.
    max_backlog: usize,
    /// Cancellation token. [`Self::shutdown`] cancels this and the
    /// `Notify` below; [`Self::enqueue`] and [`Self::take_next`] observe
    /// it via `tokio::select!`.
    cancel_token: CancellationToken,
    /// Wakes pending takers when a new entry is enqueued.
    notify: Arc<Notify>,
}

impl MembershipQueue {
    /// Creates a new membership queue.
    ///
    /// * `max_concurrent_snapshot_producing` — capacity of the in-flight semaphore. `2` matches
    ///   TiKV's `max_snapshot_per_store` default (see
    ///   [`DEFAULT_MAX_CONCURRENT_SNAPSHOT_PRODUCING`]).
    /// * `max_backlog` — hard cap on pending entries before [`Self::enqueue`] returns
    ///   [`MembershipQueueError::Backlog`] (see [`DEFAULT_MAX_BACKLOG`]).
    #[must_use]
    pub fn new(max_concurrent_snapshot_producing: usize, max_backlog: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(max_concurrent_snapshot_producing)),
            max_concurrent_snapshot_producing,
            queue: Arc::new(Mutex::new(VecDeque::new())),
            max_backlog,
            cancel_token: CancellationToken::new(),
            notify: Arc::new(Notify::new()),
        }
    }

    /// Creates a queue with the documented defaults.
    #[must_use]
    pub fn with_defaults() -> Self {
        Self::new(DEFAULT_MAX_CONCURRENT_SNAPSHOT_PRODUCING, DEFAULT_MAX_BACKLOG)
    }

    /// Returns a clone of the cancellation token. Useful for chaining
    /// child tokens off the queue's lifetime.
    #[must_use]
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancel_token.clone()
    }

    /// Returns the current backlog depth (entries waiting to be taken).
    #[must_use]
    pub fn pending(&self) -> usize {
        self.queue.lock().len()
    }

    /// Returns the configured cap on in-flight conf-changes.
    #[must_use]
    pub fn max_in_flight(&self) -> usize {
        self.max_concurrent_snapshot_producing
    }

    /// Returns the number of currently in-flight entries (acquired
    /// permits not yet released).
    #[must_use]
    pub fn in_flight(&self) -> usize {
        self.max_concurrent_snapshot_producing.saturating_sub(self.semaphore.available_permits())
    }

    /// Returns a snapshot of the current queue stats.
    #[must_use]
    pub fn stats(&self) -> MembershipQueueStats {
        let pending = self.pending();
        let in_flight = self.in_flight();
        MembershipQueueStats {
            max_concurrent_snapshot_producing: self.max_concurrent_snapshot_producing,
            max_backlog: self.max_backlog,
            pending,
            in_flight,
        }
    }

    /// Enqueues a [`MembershipChangeRequest`] for later dispatch.
    ///
    /// Awaits a semaphore permit (blocks when in-flight is at the
    /// configured cap). Returns [`MembershipQueueError::Backlog`] if the
    /// backlog is full and [`MembershipQueueError::ShuttingDown`] if
    /// [`Self::shutdown`] has been called.
    pub async fn enqueue(&self, req: MembershipChangeRequest) -> Result<()> {
        // Reject early if shutdown has already been signalled — this
        // avoids racing for a permit only to discard it.
        if self.cancel_token.is_cancelled() {
            return ShuttingDownSnafu.fail();
        }

        // Wait for a permit OR shutdown, whichever fires first.
        let permit = tokio::select! {
            biased;
            () = self.cancel_token.cancelled() => return ShuttingDownSnafu.fail(),
            permit = self.semaphore.clone().acquire_owned() => match permit {
                Ok(p) => p,
                // The semaphore was closed concurrently — treat as
                // shutdown.
                Err(_) => return ShuttingDownSnafu.fail(),
            },
        };

        // Push under the lock. Hold the lock only for the push.
        {
            let mut queue = self.queue.lock();
            if queue.len() >= self.max_backlog {
                let backlog = queue.len();
                let max_backlog = self.max_backlog;
                drop(queue);
                // Release the permit explicitly — the caller never got
                // to enqueue, so the in-flight counter should not be
                // charged.
                drop(permit);
                return BacklogSnafu { backlog, max_backlog }.fail();
            }
            queue.push_back(req);
        }

        // The permit is logically owned by the entry now; forget it so
        // that `take_next` releases the slot via `add_permits(1)`.
        permit.forget();

        // Wake one taker waiting on `take_next`.
        self.notify.notify_one();

        Ok(())
    }

    /// Awaits and returns the next entry from the FIFO backlog.
    ///
    /// Releases the semaphore permit charged by [`Self::enqueue`], so a
    /// blocked enqueuer can proceed. Returns `None` once
    /// [`Self::shutdown`] has been called and no entries remain.
    pub async fn take_next(&self) -> Option<MembershipChangeRequest> {
        loop {
            // Try to pop without awaiting first — this is the steady
            // state.
            if let Some(req) = self.try_pop() {
                self.semaphore.add_permits(1);
                return Some(req);
            }

            // No entry available. If we are shutting down and the queue
            // is drained, return `None`.
            if self.cancel_token.is_cancelled() {
                return None;
            }

            // Otherwise, wait for either a notification (new entry) or
            // shutdown.
            tokio::select! {
                biased;
                () = self.cancel_token.cancelled() => {
                    // Re-check under the lock — there may be a
                    // late-arriving entry that raced shutdown.
                    if let Some(req) = self.try_pop() {
                        self.semaphore.add_permits(1);
                        return Some(req);
                    }
                    return None;
                }
                () = self.notify.notified() => {
                    // Loop and re-attempt the pop. Spurious wakeups are
                    // harmless — the next iteration re-checks the queue.
                }
            }
        }
    }

    /// Initiates a graceful shutdown.
    ///
    /// Cancels the [`CancellationToken`], wakes pending enqueuers and
    /// takers, and closes the semaphore so future
    /// [`Semaphore::acquire`] calls observe the shutdown.
    ///
    /// Idempotent — repeated calls are no-ops.
    pub fn shutdown(&self) {
        self.cancel_token.cancel();
        self.semaphore.close();
        // Wake every taker. `notify_waiters` wakes all currently-parked
        // tasks; future tasks observe the cancelled token on entry.
        self.notify.notify_waiters();
    }

    /// Pops the front entry without awaiting. Internal helper shared by
    /// the fast and shutdown-drain paths in [`Self::take_next`].
    fn try_pop(&self) -> Option<MembershipChangeRequest> {
        self.queue.lock().pop_front()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods, clippy::panic)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn sample_request(vault: i64) -> MembershipChangeRequest {
        MembershipChangeRequest::AddLearnerToVault {
            vault_id: VaultId::new(vault),
            node_id: 1,
            addr: "node-1.local:50051".to_string(),
        }
    }

    #[tokio::test]
    async fn test_enqueue_within_capacity_succeeds() {
        // cap = 2 in-flight, backlog = 8. Enqueue 2 entries — both must
        // complete without blocking, since the semaphore has 2 permits
        // and no one is taking yet.
        let queue = MembershipQueue::new(2, 8);

        queue.enqueue(sample_request(1)).await.unwrap();
        queue.enqueue(sample_request(2)).await.unwrap();

        assert_eq!(queue.pending(), 2);
        assert_eq!(queue.in_flight(), 2);
    }

    #[tokio::test]
    async fn test_enqueue_at_max_blocks_until_taken() {
        // cap = 2, backlog = 8. Enqueue 2 — both succeed. The third
        // must block (no permit available) until take_next releases
        // a slot.
        let queue = Arc::new(MembershipQueue::new(2, 8));

        queue.enqueue(sample_request(1)).await.unwrap();
        queue.enqueue(sample_request(2)).await.unwrap();
        assert_eq!(queue.in_flight(), 2);

        // Spawn a task that awaits the third enqueue.
        let q_for_task = Arc::clone(&queue);
        let blocked = tokio::spawn(async move { q_for_task.enqueue(sample_request(3)).await });

        // Give the spawned task a chance to park on the semaphore.
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!blocked.is_finished(), "third enqueue should be blocked");

        // Drain one entry — this releases a permit and unblocks the
        // pending enqueue.
        let taken = queue.take_next().await.expect("entry should be present");
        assert_eq!(taken.vault_id(), VaultId::new(1));

        // The blocked enqueue should now complete.
        let res = tokio::time::timeout(Duration::from_secs(1), blocked).await;
        let inner = res.expect("blocked enqueue did not unblock in time");
        inner.expect("join failed").expect("enqueue failed after permit freed");

        // Backlog depth: original 2 - 1 taken + 1 new = 2.
        assert_eq!(queue.pending(), 2);
        assert_eq!(queue.in_flight(), 2);
    }

    #[tokio::test]
    async fn test_take_next_drains_in_fifo_order() {
        // cap large enough to admit all entries up front so we exercise
        // the FIFO ordering of the queue itself.
        let queue = MembershipQueue::new(8, 16);

        for vault in 1..=5 {
            queue.enqueue(sample_request(vault)).await.unwrap();
        }
        assert_eq!(queue.in_flight(), 5);

        for vault in 1..=5 {
            let req = queue.take_next().await.expect("entry should be present");
            assert_eq!(req.vault_id(), VaultId::new(vault));
        }

        assert_eq!(queue.pending(), 0);
        assert_eq!(queue.in_flight(), 0);
    }

    #[tokio::test]
    async fn test_shutdown_cancels_pending_takers() {
        // Empty queue. A taker awaits indefinitely. Shutdown must wake
        // it and yield `None`.
        let queue = Arc::new(MembershipQueue::new(2, 8));

        let q_for_task = Arc::clone(&queue);
        let pending = tokio::spawn(async move { q_for_task.take_next().await });

        // Park the taker.
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!pending.is_finished(), "taker should be parked");

        queue.shutdown();

        let res = tokio::time::timeout(Duration::from_secs(1), pending).await;
        let inner = res.expect("pending taker did not unblock on shutdown");
        let outcome = inner.expect("join failed");
        assert!(outcome.is_none(), "taker should yield None on shutdown");

        // Subsequent enqueues are rejected.
        let err = queue.enqueue(sample_request(1)).await.unwrap_err();
        assert!(matches!(err, MembershipQueueError::ShuttingDown { .. }));
    }

    #[tokio::test]
    async fn test_backlog_overflow_rejects_with_error() {
        // cap = 8 permits, backlog = 2. Push 2 entries — both succeed.
        // Push a third — even though the semaphore would grant a
        // permit, the backlog cap rejects.
        let queue = MembershipQueue::new(8, 2);

        queue.enqueue(sample_request(1)).await.unwrap();
        queue.enqueue(sample_request(2)).await.unwrap();

        let err = queue.enqueue(sample_request(3)).await.unwrap_err();
        assert!(matches!(err, MembershipQueueError::Backlog { .. }));

        // The two original entries are still drainable.
        assert_eq!(queue.pending(), 2);
    }

    #[test]
    fn test_change_request_accessors() {
        let r = MembershipChangeRequest::PromoteLearnerInVault {
            vault_id: VaultId::new(7),
            node_id: 42,
        };
        assert_eq!(r.vault_id(), VaultId::new(7));
        assert_eq!(r.node_id(), 42);

        let r = MembershipChangeRequest::RemoveFromVault { vault_id: VaultId::new(8), node_id: 99 };
        assert_eq!(r.vault_id(), VaultId::new(8));
        assert_eq!(r.node_id(), 99);
    }

    #[test]
    fn test_with_defaults() {
        let q = MembershipQueue::with_defaults();
        let stats = q.stats();
        assert_eq!(
            stats.max_concurrent_snapshot_producing,
            DEFAULT_MAX_CONCURRENT_SNAPSHOT_PRODUCING
        );
        assert_eq!(stats.max_backlog, DEFAULT_MAX_BACKLOG);
        assert_eq!(stats.pending, 0);
        assert_eq!(stats.in_flight, 0);
    }
}
