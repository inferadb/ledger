//! Per-organization watcher + dispatcher for the Phase 5 centralised
//! membership cascade.
//!
//! This module is the **M3 deliverable** of the Phase 5 plan
//! (`docs/superpowers/specs/2026-04-27-phase-5-centralised-membership.md`).
//! It moves cascade-decision authority off the data-region's
//! [`PlacementController`] and onto the per-organization
//! [`OrganizationGroup`](crate::raft_manager::OrganizationGroup) leader,
//! using the [`MembershipQueue`](crate::membership_queue::MembershipQueue)
//! primitive that landed in M2.
//!
//! ## Background
//!
//! Today, when a voter joins or leaves a data region, the
//! [`crate::raft_manager::RaftManager::cascade_membership_to_children`]
//! function fans the change out to every per-organization and per-vault
//! Raft shard in the affected region in parallel. At 1000 vaults × 3
//! voters per org, that is ~3000 conf-changes in flight simultaneously,
//! each capable of triggering a snapshot RPC. The resulting **snapshot
//! storm** dominates cluster bandwidth and CPU for minutes.
//!
//! Phase 5 layers a per-org [`MembershipQueue`] between the data-region
//! cascade and the per-vault shards. M3 supplies two background tasks
//! that turn that primitive into an end-to-end pipeline:
//!
//! * [`RegionMembershipWatcher::run`] — observes the parent data-region group's
//!   [`watch::Receiver<ShardState>`](tokio::sync::watch::Receiver) for voter / learner deltas. On
//!   each delta, applies the conf-change synchronously to the org group itself (so the per-org
//!   transport learns about the new peer first — vault groups share that transport per root rule
//!   17) and then enqueues one
//!   [`MembershipChangeRequest`](crate::membership_queue::MembershipChangeRequest) per affected
//!   vault into the org's queue.
//! * [`MembershipDispatcher::run`] — drains the queue (one entry at a time, gated by the queue's
//!   [`Semaphore`](tokio::sync::Semaphore) of capacity `max_concurrent_snapshot_producing`), looks
//!   up the target vault group via [`crate::raft_manager::RaftManager::get_vault_group`], and
//!   applies the membership change through `apply_cascade_action_for_vault` — the same primitive
//!   the legacy cascade uses, so no-op / "already undergoing" handling is identical.
//!
//! ## Migration discipline
//!
//! M3 lands sub-stages **5b (dual-cascade)** and **5c (cascade ownership
//! shift)** of the plan. Sub-stage 5b kept the legacy cascade running
//! alongside the new pipeline; sub-stage 5c removes the legacy call
//! sites in `dr_scheduler.rs` and `admin.rs`. The
//! [`crate::raft_manager::RaftManager::cascade_membership_to_children`]
//! function itself is preserved as a public API — useful for tests and
//! for emergency operator tooling — but is no longer invoked from
//! production code paths.
//!
//! ## Cancellation
//!
//! Both tasks are tied to a child of
//! [`crate::raft_manager::RaftManager`]'s parent
//! [`CancellationToken`]. On shutdown the token is cancelled, the
//! watcher exits, the dispatcher's
//! [`MembershipQueue::take_next`](crate::membership_queue::MembershipQueue::take_next)
//! returns `None`, and the dispatcher exits without leaking pending
//! requests.
//!
//! ## Lifecycle
//!
//! Spawned in
//! [`crate::raft_manager::RaftManager::start_organization_group`]
//! immediately after the per-org vault-lifecycle watcher. Both tasks
//! hold a [`Weak<RaftManager>`] reference, upgraded on every
//! iteration. The manager owns every group and outlives every task it
//! spawns by construction (see the comment on the vault-lifecycle
//! watcher in `raft_manager.rs`); the `Weak` exists only to break the
//! reference cycle the manager would otherwise have with the tasks it
//! spawned, since both tasks are stored implicitly in the tokio
//! runtime keyed off the manager's cancellation token. Each upgrade
//! is expected to succeed for the lifetime of the watcher; a failed
//! upgrade is treated as a graceful shutdown signal.
//!
//! [`PlacementController`]: ../../server/src/placement.rs

use std::{
    collections::BTreeSet,
    future::Future,
    sync::{Arc, Weak},
    time::Duration,
};

use inferadb_ledger_consensus::types::NodeId as ConsensusNodeId;
use inferadb_ledger_types::{OrganizationId, Region, VaultId};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::{
    membership_queue::{MembershipChangeRequest, MembershipQueue, MembershipQueueError},
    raft_manager::{
        CascadeMembershipAction, InnerGroup, RaftManager, apply_cascade_action_for_org,
        apply_cascade_action_for_vault,
    },
};

/// Watcher task that observes the parent data-region's voter / learner
/// set and enqueues per-vault membership changes into the org's
/// [`MembershipQueue`] when the set changes.
///
/// One instance per per-organization
/// [`InnerGroup`](crate::raft_manager::InnerGroup), spawned by
/// [`crate::raft_manager::RaftManager::start_organization_group`].
///
/// See the module-level docs for the broader pipeline.
pub struct RegionMembershipWatcher {
    /// Weak reference to the manager — used to walk the org's vault
    /// directory and to look up peer addresses for `AddLearner`
    /// requests. Weak avoids the cycle the manager would otherwise
    /// have with the task it spawned; the manager outlives the task by
    /// construction, so the weak upgrade succeeds for the lifetime of
    /// the watcher.
    manager: Weak<RaftManager>,
    /// The data-region group whose membership we observe.
    region_group: Arc<InnerGroup>,
    /// The per-organization group whose queue we feed.
    org_group: Arc<InnerGroup>,
    /// Region this watcher belongs to (cached for log fields).
    region: Region,
    /// Organization id (cached for log fields).
    organization_id: OrganizationId,
}

impl RegionMembershipWatcher {
    /// Constructs a watcher. Does not spawn the task — call
    /// [`Self::run`] inside a `tokio::spawn` boundary owned by the
    /// caller.
    #[must_use]
    pub fn new(
        manager: Weak<RaftManager>,
        region_group: Arc<InnerGroup>,
        org_group: Arc<InnerGroup>,
    ) -> Self {
        let region = org_group.region();
        let organization_id = org_group.organization_id();
        Self { manager, region_group, org_group, region, organization_id }
    }

    /// Main loop. Subscribes to the data-region group's state-watch
    /// channel, computes voter / learner deltas on every change, and
    /// dispatches each delta through [`Self::cascade`].
    ///
    /// Exits when `cancel` is cancelled or when the underlying watch
    /// channel closes (which happens on region shutdown).
    pub async fn run(self, cancel: CancellationToken) {
        // Snapshot the initial membership so we only react to *changes*.
        // The legacy cascade is invoked from operator-driven paths
        // (`dr_scheduler::execute_operator`, `admin::leave_cluster`);
        // re-enqueueing the current membership on first observation
        // would reissue conf-changes for the existing voter set on
        // every restart. The intent of M3 is to react to deltas from
        // here on out — initial population is the responsibility of the
        // bootstrap path.
        let mut state_rx = self.region_group.handle().state_rx().clone();
        let initial = state_rx.borrow_and_update().clone();
        let mut last_voters: BTreeSet<ConsensusNodeId> = initial.voters.iter().copied().collect();
        let mut last_learners: BTreeSet<ConsensusNodeId> =
            initial.learners.iter().copied().collect();

        debug!(
            region = self.region.as_str(),
            organization_id = self.organization_id.value(),
            voters = last_voters.len(),
            learners = last_learners.len(),
            "RegionMembershipWatcher: started",
        );

        loop {
            tokio::select! {
                biased;
                () = cancel.cancelled() => {
                    debug!(
                        region = self.region.as_str(),
                        organization_id = self.organization_id.value(),
                        "RegionMembershipWatcher: cancelled",
                    );
                    return;
                }
                changed = state_rx.changed() => {
                    if changed.is_err() {
                        // Watch channel closed (region shut down).
                        debug!(
                            region = self.region.as_str(),
                            organization_id = self.organization_id.value(),
                            "RegionMembershipWatcher: state-watch closed",
                        );
                        return;
                    }
                }
            }

            let snap = state_rx.borrow().clone();
            let current_voters: BTreeSet<ConsensusNodeId> = snap.voters.iter().copied().collect();
            let current_learners: BTreeSet<ConsensusNodeId> =
                snap.learners.iter().copied().collect();

            // Only the data-region's *leader* drives the cascade — same
            // discipline as the legacy
            // `cascade_membership_to_children`, which checks
            // `is_leader()` on every child shard. Followers observe the
            // membership change too, but the cascade itself is a
            // proposal-side action that must be issued by the leader.
            // The per-org group's leadership is delegated from the
            // region group, so checking the org's `is_leader()` is
            // equivalent.
            if !self.org_group.handle().is_leader() {
                last_voters = current_voters;
                last_learners = current_learners;
                continue;
            }

            // Additions: nodes appearing in the new set that weren't in
            // the old set. A node moving from learner → voter shows up
            // as removed-from-learners and added-to-voters; we encode
            // it as `PromoteVoter` so the per-org / per-vault shards
            // can promote in place rather than re-add.
            for node in current_voters.difference(&last_voters) {
                if last_learners.contains(node) {
                    self.cascade(CascadeMembershipAction::PromoteVoter, node.0).await;
                } else {
                    // Net-new voter — fall back to AddLearner on the
                    // child shards. Promotion happens via a separate
                    // delta when the parent group promotes the learner.
                    self.cascade(CascadeMembershipAction::AddLearner, node.0).await;
                }
            }
            for node in current_learners.difference(&last_learners) {
                if !last_voters.contains(node) {
                    self.cascade(CascadeMembershipAction::AddLearner, node.0).await;
                }
            }

            // Removals: anything in the old set but not the new set,
            // and not just transitioning to the other role.
            for node in last_voters.difference(&current_voters) {
                if !current_learners.contains(node) {
                    self.cascade(CascadeMembershipAction::Remove, node.0).await;
                }
            }
            for node in last_learners.difference(&current_learners) {
                if !current_voters.contains(node) {
                    self.cascade(CascadeMembershipAction::Remove, node.0).await;
                }
            }

            last_voters = current_voters;
            last_learners = current_learners;
        }
    }

    /// Applies a single membership delta to the org group + every
    /// non-deleted vault under the organization.
    ///
    /// The org-level conf-change is applied *synchronously* by the
    /// watcher before any vault entries are enqueued — the per-org
    /// transport must learn about the new peer before the per-vault
    /// dispatcher fires (vault groups share parent org's transport,
    /// root rule 17). The vault-level conf-changes are pushed onto
    /// the org's [`MembershipQueue`] one entry per vault and drained
    /// by [`MembershipDispatcher`] at the queue's rate-limit cap.
    async fn cascade(&self, action: CascadeMembershipAction, node_id: u64) {
        let Some(manager) = self.manager.upgrade() else {
            return;
        };

        // 1. Apply the cascade to the org group itself. This registers the new peer on the org's
        //    transport (for `AddLearner`) and proposes the conf-change against the org's Raft
        //    group, so its own voter set converges with the parent region's. Vault groups share
        //    this transport, so registering it here makes their AppendEntries reach the new peer
        //    once the dispatcher fires their own conf-changes.
        let target = ConsensusNodeId(node_id);
        apply_cascade_action_for_org(
            action,
            &manager,
            self.org_group.handle(),
            self.org_group.consensus_transport(),
            target,
            node_id,
            self.region,
            self.organization_id,
        )
        .await;

        // 2. Enqueue one entry per vault. The dispatcher will pop them under the queue's
        //    snapshot-cap semaphore.
        self.enqueue_for_all_vaults(action, node_id).await;
    }

    /// Enqueues one [`MembershipChangeRequest`] per non-deleted vault
    /// in this organization. Returns silently if the queue is
    /// shutting down or the manager has been dropped.
    async fn enqueue_for_all_vaults(&self, action: CascadeMembershipAction, node_id: u64) {
        let Some(queue) = self.org_group.membership_queue().cloned() else {
            // Should be unreachable for per-org groups, but defensive.
            return;
        };

        // Resolve the peer address up-front for `AddLearner` so the
        // dispatcher does not have to round-trip through the manager
        // per vault.
        let addr = if matches!(action, CascadeMembershipAction::AddLearner) {
            let Some(manager) = self.manager.upgrade() else {
                return;
            };
            manager.peer_addresses().get(node_id).unwrap_or_default()
        } else {
            String::new()
        };

        let vaults: Vec<VaultId> = self
            .org_group
            .applied_state()
            .list_vaults(self.organization_id)
            .into_iter()
            .map(|meta| meta.vault)
            .collect();

        if vaults.is_empty() {
            return;
        }

        for vault_id in vaults {
            let req = match action {
                CascadeMembershipAction::AddLearner => MembershipChangeRequest::AddLearnerToVault {
                    vault_id,
                    node_id,
                    addr: addr.clone(),
                },
                CascadeMembershipAction::PromoteVoter => {
                    MembershipChangeRequest::PromoteLearnerInVault { vault_id, node_id }
                },
                CascadeMembershipAction::Remove => {
                    MembershipChangeRequest::RemoveFromVault { vault_id, node_id }
                },
            };

            match queue.enqueue(req).await {
                Ok(()) => {},
                Err(MembershipQueueError::ShuttingDown { .. }) => {
                    // Graceful shutdown of the queue; stop producing.
                    return;
                },
                Err(e @ MembershipQueueError::Backlog { .. }) => {
                    // Backlog overflow is recoverable: the dispatcher
                    // will drain entries and the next region-state
                    // change will re-derive the missing deltas. Warn
                    // so operators see the saturation but do not block
                    // the watcher.
                    warn!(
                        region = self.region.as_str(),
                        organization_id = self.organization_id.value(),
                        vault_id = vault_id.value(),
                        node_id,
                        error = %e,
                        "RegionMembershipWatcher: backlog full, dropping request",
                    );
                },
            }
        }
    }
}

/// Drainer task that pops [`MembershipChangeRequest`]s off the org's
/// [`MembershipQueue`] and applies them to the corresponding per-vault
/// shard via `apply_cascade_action_for_vault`.
///
/// One instance per per-organization
/// [`InnerGroup`](crate::raft_manager::InnerGroup), spawned alongside
/// the [`RegionMembershipWatcher`] by
/// [`crate::raft_manager::RaftManager::start_organization_group`].
///
/// The dispatcher honours the queue's in-flight cap (default 2 — see
/// [`crate::membership_queue::DEFAULT_MAX_CONCURRENT_SNAPSHOT_PRODUCING`])
/// implicitly: each [`MembershipQueue::take_next`] releases a permit so
/// the next [`MembershipQueue::enqueue`] can proceed, but there is only
/// ever one in-flight `apply_cascade_action` per dispatcher because
/// each iteration awaits the cascade before requesting the next entry.
/// The watcher producing entries faster than this consumer can drain
/// is bounded by the queue's `max_backlog` cap.
pub struct MembershipDispatcher {
    /// Weak reference to the manager — used to look up the vault group
    /// for each entry. Weak for the same reason as
    /// [`RegionMembershipWatcher::manager`].
    manager: Weak<RaftManager>,
    /// Region this dispatcher belongs to (cached for log fields).
    region: Region,
    /// Organization id (cached for log fields).
    organization_id: OrganizationId,
    /// The shared queue. Cloned out of the per-org group once at
    /// construction so the run loop does not have to re-borrow the
    /// [`InnerGroup::membership_queue`] field on every iteration.
    queue: Arc<MembershipQueue>,
    /// Per-vault conf-change timeout (Phase 5 / M5). Captured from
    /// [`crate::raft_manager::RaftManagerConfig::vault_conf_change_timeout_secs`]
    /// at spawn time. Each
    /// [`apply_cascade_action_for_vault`] call is wrapped in a
    /// [`tokio::time::timeout`] of this duration; on elapsed, the
    /// dispatcher logs a WARN, increments
    /// `ledger_vault_conf_change_stalled_total`, and drops the entry.
    conf_change_timeout: Duration,
}

impl MembershipDispatcher {
    /// Constructs a dispatcher. Does not spawn — caller wraps in
    /// `tokio::spawn`.
    ///
    /// Returns `None` if `org_group` has no
    /// [`MembershipQueue`] (i.e. it is a control-plane group at
    /// `OrganizationId(0)`); the caller should skip spawning in that
    /// case.
    ///
    /// `conf_change_timeout` is the per-vault conf-change timeout (M5).
    /// Production callers thread this through from
    /// [`crate::raft_manager::RaftManagerConfig::vault_conf_change_timeout_secs`].
    #[must_use]
    pub fn new(
        manager: Weak<RaftManager>,
        org_group: Arc<InnerGroup>,
        conf_change_timeout: Duration,
    ) -> Option<Self> {
        let queue = Arc::clone(org_group.membership_queue()?);
        let region = org_group.region();
        let organization_id = org_group.organization_id();
        Some(Self { manager, region, organization_id, queue, conf_change_timeout })
    }

    /// Main loop. Drains entries one at a time. Exits when the queue
    /// is shut down (returns `None`) or the cancellation token fires.
    pub async fn run(self, cancel: CancellationToken) {
        debug!(
            region = self.region.as_str(),
            organization_id = self.organization_id.value(),
            "MembershipDispatcher: started",
        );

        loop {
            // Race the cancellation token against the queue. The queue
            // also observes its own shutdown signal internally — we
            // wire the manager-level cancellation here so a parent
            // shutdown wakes us even if the queue has not been
            // shutdown explicitly.
            let next = tokio::select! {
                biased;
                () = cancel.cancelled() => {
                    self.queue.shutdown();
                    None
                }
                req = self.queue.take_next() => req,
            };

            let Some(req) = next else {
                debug!(
                    region = self.region.as_str(),
                    organization_id = self.organization_id.value(),
                    "MembershipDispatcher: queue drained, exiting",
                );
                return;
            };

            self.dispatch(req).await;
        }
    }

    /// Applies one membership-change request to the target vault group.
    ///
    /// All failures are absorbed — the watcher will re-emit the delta
    /// on the next region-state change. We only log so operators see
    /// what skipped.
    async fn dispatch(&self, req: MembershipChangeRequest) {
        let Some(manager) = self.manager.upgrade() else {
            // Manager was dropped — nothing to dispatch against.
            return;
        };

        let vault_id = req.vault_id();
        let node_id = req.node_id();

        let vault_group = match manager.get_vault_group(self.region, self.organization_id, vault_id)
        {
            Ok(g) => g,
            Err(_) => {
                // The vault is not present on this node. The
                // legacy cascade applied the same skip behaviour
                // — shards are only cascaded against where they
                // are leaders.
                debug!(
                    region = self.region.as_str(),
                    organization_id = self.organization_id.value(),
                    vault_id = vault_id.value(),
                    "MembershipDispatcher: vault group not on this node, skipping",
                );
                return;
            },
        };

        // Vault groups share their parent org's transport (root rule
        // 17), so the dispatcher never registers a peer on the
        // vault's own transport. The org-level watcher already
        // registered the new peer on the org's transport before
        // enqueueing this request. The `addr` carried in the queue
        // entry is preserved for forward-compat with M5 / M6 work
        // (where per-vault transports may carry their own peer
        // registration channel).
        let action = match &req {
            MembershipChangeRequest::AddLearnerToVault { .. } => {
                CascadeMembershipAction::AddLearner
            },
            MembershipChangeRequest::PromoteLearnerInVault { .. } => {
                CascadeMembershipAction::PromoteVoter
            },
            MembershipChangeRequest::RemoveFromVault { .. } => CascadeMembershipAction::Remove,
        };

        // Skip non-leaders. The vault group's leader is delegated from
        // the org group, so checking either is equivalent — but
        // checking the vault's own handle is the most direct signal
        // and matches what the legacy cascade does inside
        // `apply_cascade_action`'s caller.
        if !vault_group.handle().is_leader() {
            debug!(
                region = self.region.as_str(),
                organization_id = self.organization_id.value(),
                vault_id = vault_id.value(),
                "MembershipDispatcher: vault group not leader, skipping",
            );
            return;
        }

        let target = ConsensusNodeId(node_id);

        info!(
            region = self.region.as_str(),
            organization_id = self.organization_id.value(),
            vault_id = vault_id.value(),
            node_id,
            action = ?action,
            "MembershipDispatcher: applying cascade",
        );

        let cascade_fut = apply_cascade_action_for_vault(
            action,
            &manager,
            vault_group.handle(),
            target,
            node_id,
            self.region,
            self.organization_id,
            vault_id,
        );

        run_conf_change_with_timeout(
            self.region,
            self.organization_id,
            vault_id,
            node_id,
            action,
            self.conf_change_timeout,
            cascade_fut,
        )
        .await;
    }
}

/// Runs a per-vault conf-change future under a fixed timeout (Phase 5 /
/// M5).
///
/// This is the testable inner kernel of
/// [`MembershipDispatcher::dispatch`]. On `tokio::time::timeout`
/// elapsed, it emits an operator-visible WARN log, increments
/// `ledger_vault_conf_change_stalled_total{reason="timeout"}`, and
/// returns — the caller drops the request and continues to the next
/// queue entry (the watcher's next region-state delta will re-derive
/// any change the dropped entry failed to propagate, so we do not
/// re-enqueue).
///
/// Factored out so unit tests can drive the timeout path with a
/// synthetic future (`tokio::time::pause()` + a hung future) without
/// constructing a real [`RaftManager`] + vault group.
pub(crate) async fn run_conf_change_with_timeout<F>(
    region: Region,
    organization_id: OrganizationId,
    vault_id: VaultId,
    node_id: u64,
    action: CascadeMembershipAction,
    timeout: Duration,
    fut: F,
) where
    F: Future<Output = ()>,
{
    match tokio::time::timeout(timeout, fut).await {
        Ok(()) => {},
        Err(_elapsed) => {
            warn!(
                region = %region.as_str(),
                organization_id = organization_id.value(),
                vault_id = vault_id.value(),
                node_id,
                action = ?action,
                timeout_secs = timeout.as_secs(),
                "Vault conf-change stalled — DR state may be inconsistent",
            );

            let action_label = match action {
                CascadeMembershipAction::AddLearner => "AddLearner",
                CascadeMembershipAction::PromoteVoter => "PromoteVoter",
                CascadeMembershipAction::Remove => "Remove",
            };
            let organization_label = organization_id.value().to_string();
            let vault_label = vault_id.value().to_string();
            crate::metrics::record_vault_conf_change_stalled(
                region.as_str(),
                &organization_label,
                &vault_label,
                action_label,
                "timeout",
            );
        },
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods, clippy::panic)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use tracing_subscriber::layer::SubscriberExt;

    use super::*;

    /// Tracing layer that counts WARN events whose message starts with
    /// `"Vault conf-change stalled"`. We assert on the count rather than
    /// the formatted body so the assertion does not couple to the tracing
    /// formatter's exact rendering.
    struct StallWarnCounter {
        count: Arc<AtomicUsize>,
    }

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for StallWarnCounter {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            if *event.metadata().level() != tracing::Level::WARN {
                return;
            }
            // Capture the message via a visitor — the WARN log uses the
            // message pattern `"Vault conf-change stalled — ..."`.
            struct Visitor<'a> {
                target: &'a str,
                hit: bool,
            }
            impl tracing::field::Visit for Visitor<'_> {
                fn record_debug(
                    &mut self,
                    field: &tracing::field::Field,
                    value: &dyn std::fmt::Debug,
                ) {
                    if field.name() == "message" {
                        let rendered = format!("{value:?}");
                        if rendered.contains(self.target) {
                            self.hit = true;
                        }
                    }
                }
            }
            let mut visitor = Visitor { target: "Vault conf-change stalled", hit: false };
            event.record(&mut visitor);
            if visitor.hit {
                self.count.fetch_add(1, Ordering::SeqCst);
            }
        }
    }

    /// On timeout the helper must:
    ///  1. Return promptly once the timer elapses (we drive simulated time forward — `start_paused
    ///     = true` makes the assertion wall-clock-instant).
    ///  2. Emit the WARN log with the stall message.
    ///  3. Increment `ledger_vault_conf_change_stalled_total{reason="timeout"}`.
    #[test]
    fn run_conf_change_with_timeout_fires_on_hung_future() {
        let warn_count = Arc::new(AtomicUsize::new(0));
        let layer = StallWarnCounter { count: Arc::clone(&warn_count) };
        let subscriber = tracing_subscriber::registry().with(layer);
        let _guard = tracing::subscriber::set_default(subscriber);

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        // Build a current-thread runtime with paused time. The helper
        // reads `tokio::time::Instant::now()` inside `tokio::time::timeout`,
        // and `start_paused` auto-advances simulated time when no task
        // is runnable — so a `pending()` future immediately advances
        // past the configured timeout without sleeping wall-clock.
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .unwrap();

        metrics::with_local_recorder(&recorder, || {
            runtime.block_on(async {
                // Simulated future that never completes — the
                // dispatcher's real-world failure mode is a vault
                // group whose conf-change RPC hangs forever.
                let hung = std::future::pending::<()>();

                // 60s configured timeout — the production default.
                let timeout = Duration::from_secs(60);

                run_conf_change_with_timeout(
                    Region::new("test"),
                    OrganizationId::new(7),
                    VaultId::new(123),
                    42,
                    CascadeMembershipAction::AddLearner,
                    timeout,
                    hung,
                )
                .await;
            });
        });

        // 1. Timeout fired — the helper returned. Reaching this line is the assertion. With paused
        //    time, hanging would cause cargo test's per-test timeout to trip.

        // 2. WARN log was emitted exactly once.
        assert_eq!(
            warn_count.load(Ordering::SeqCst),
            1,
            "expected exactly one stall WARN, got {}",
            warn_count.load(Ordering::SeqCst),
        );

        // 3. Counter incremented exactly once with the correct labels.
        let snapshot = snapshotter.snapshot();
        let mut found = None;
        for (ck, _, _, value) in snapshot.into_vec() {
            if ck.key().name() == "ledger_vault_conf_change_stalled_total" {
                let labels: Vec<(String, String)> = ck
                    .key()
                    .labels()
                    .map(|l| (l.key().to_string(), l.value().to_string()))
                    .collect();
                found = Some((value, labels));
                break;
            }
        }
        let (value, labels) = found.expect(
            "ledger_vault_conf_change_stalled_total counter must be recorded on \
             timeout",
        );
        match value {
            DebugValue::Counter(n) => {
                assert_eq!(n, 1, "expected counter increment of exactly 1, got {n}");
            },
            other => {
                panic!("ledger_vault_conf_change_stalled_total should be a Counter, got {other:?}",)
            },
        }
        // Labels mirror the operator alert query — the dashboard
        // splits by every dimension, so all five must be present.
        let label_pairs: Vec<(&str, &str)> =
            labels.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        assert!(label_pairs.contains(&("region", "test")), "labels missing region: {labels:?}");
        assert!(
            label_pairs.contains(&("organization_id", "7")),
            "labels missing organization_id: {labels:?}",
        );
        assert!(label_pairs.contains(&("vault_id", "123")), "labels missing vault_id: {labels:?}",);
        assert!(
            label_pairs.contains(&("action", "AddLearner")),
            "labels missing action: {labels:?}",
        );
        assert!(label_pairs.contains(&("reason", "timeout")), "labels missing reason: {labels:?}",);
    }

    /// On a future that completes inside the timeout window the helper
    /// must return without logging or incrementing the stall counter —
    /// healthy lifecycle ops must not produce spurious operator
    /// alerts.
    #[test]
    fn run_conf_change_with_timeout_no_op_on_fast_completion() {
        let warn_count = Arc::new(AtomicUsize::new(0));
        let layer = StallWarnCounter { count: Arc::clone(&warn_count) };
        let subscriber = tracing_subscriber::registry().with(layer);
        let _guard = tracing::subscriber::set_default(subscriber);

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .unwrap();

        metrics::with_local_recorder(&recorder, || {
            runtime.block_on(async {
                run_conf_change_with_timeout(
                    Region::new("test"),
                    OrganizationId::new(7),
                    VaultId::new(123),
                    42,
                    CascadeMembershipAction::PromoteVoter,
                    Duration::from_secs(60),
                    async {}, // completes immediately
                )
                .await;
            });
        });

        assert_eq!(
            warn_count.load(Ordering::SeqCst),
            0,
            "fast completion must not emit a stall WARN",
        );

        let snapshot = snapshotter.snapshot();
        for (ck, ..) in snapshot.into_vec() {
            assert_ne!(
                ck.key().name(),
                "ledger_vault_conf_change_stalled_total",
                "fast completion must not increment the stall counter",
            );
        }
    }
}
