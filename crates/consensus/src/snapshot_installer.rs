//! Snapshot installer trait used by the reactor when a follower receives a
//! [`Message::InstallSnapshot`](crate::message::Message::InstallSnapshot)
//! from the leader.
//!
//! Stage 4 of the snapshot install path. Sister of
//! [`SnapshotSender`](crate::snapshot_sender::SnapshotSender) and
//! [`SnapshotCoordinator`](crate::snapshot_coordinator::SnapshotCoordinator):
//! the trait is defined here (with a no-op default implementation) so the
//! consensus crate does not depend on the raft crate; the raft crate
//! installs a real implementation that:
//!
//! 1. Resolves the shard ID to its `(region, organization_id, vault_id?)` scope.
//! 2. Locates the staged snapshot file written by Stage 3's receiver via
//!    [`SnapshotPersister::list_staged`](../../../raft/src/snapshot_persister.rs).
//! 3. Decrypts the file via the per-scope `SnapshotKeyProvider` /
//!    [`decrypt_snapshot`](crate::snapshot_crypto::decrypt_snapshot).
//! 4. Installs the recovered plaintext through `RaftLogStore::install_snapshot` (Stage 1b's
//!    bifurcated install).
//! 5. Calls back into
//!    [`ConsensusEngine::notify_snapshot_installed`](crate::ConsensusEngine::notify_snapshot_installed)
//!    so the shard's `last_applied` / `last_snapshot_index` advance.
//! 6. Prunes the staged file via
//!    [`SnapshotPersister::remove_staged`](../../../raft/src/snapshot_persister.rs).
//!
//! ## Implementor contract
//!
//! [`SnapshotInstaller::install_snapshot`] is called from the reactor's
//! event loop. The reactor's event loop is paused for the duration of the
//! call, so implementors **must** dispatch any I/O asynchronously
//! (e.g., spawn a tokio task) and return immediately. Blocking inside this
//! method stalls every other shard managed by the reactor — same hazard as
//! [`ShardWakeNotifier`](crate::wake::ShardWakeNotifier),
//! [`SnapshotCoordinator`](crate::snapshot_coordinator::SnapshotCoordinator),
//! and [`SnapshotSender`](crate::snapshot_sender::SnapshotSender).
//!
//! ## Drop-and-let-Raft-retry
//!
//! When no installer is registered (the [`NoopSnapshotInstaller`] default),
//! the reactor logs the
//! [`Action::InstallSnapshot`](crate::action::Action::InstallSnapshot) at
//! debug level and drops it. Raft tolerates this naturally: the leader's
//! heartbeat replicator detects the still-lagging follower on the next
//! AppendEntries cycle and re-emits
//! [`Action::SendSnapshot`](crate::action::Action::SendSnapshot), which
//! re-streams the file through Stage 3 and re-emits this action. The same
//! drop-and-retry semantics apply when the installer's async dispatch
//! fails (staged file not found yet, decrypt error, scope mismatch): the
//! leader will retry on the next heartbeat tick.
//!
//! ## Why not an explicit storage handle?
//!
//! Earlier drafts threaded a storage handle through the reactor and
//! invoked it directly. This conflated two concerns: the consensus crate
//! must not depend on the raft crate (where the staged snapshot files
//! live), and the reactor must not perform I/O (root rule 10). The
//! callback split keeps the reactor I/O-free and the consensus crate
//! storage-agnostic — same shape as
//! [`SnapshotSender`](crate::snapshot_sender::SnapshotSender) and
//! [`SnapshotCoordinator`](crate::snapshot_coordinator::SnapshotCoordinator).

use crate::types::ConsensusStateId;

/// Notification trait invoked by the reactor when an
/// [`Action::InstallSnapshot`](crate::action::Action::InstallSnapshot) is
/// processed.
///
/// See module docs for the implementor contract and the drop-and-retry
/// semantics when the installer's async dispatch fails.
pub trait SnapshotInstaller: Send + Sync + 'static {
    /// Called by the reactor when an `Action::InstallSnapshot` is processed
    /// for a shard.
    ///
    /// `shard_id` identifies the shard the snapshot belongs to (the
    /// implementor resolves this to a `(region, organization_id, vault_id?)`
    /// scope and finds the staged file under that scope's directory).
    /// `leader_term` is the term carried in the originating
    /// [`Message::InstallSnapshot`](crate::message::Message::InstallSnapshot)
    /// — used purely for diagnostics and metrics. `last_included_index` /
    /// `last_included_term` identify the snapshot the shard has accepted;
    /// the implementor uses `last_included_index` to find the matching
    /// staged file and feeds both into
    /// [`ConsensusEngine::notify_snapshot_installed`](crate::ConsensusEngine::notify_snapshot_installed)
    /// after a successful install.
    ///
    /// The implementor MUST dispatch any I/O asynchronously and return
    /// immediately — the reactor's event loop is on hold until this
    /// returns. Failures are logged + recorded as metrics; the leader's
    /// heartbeat replicator re-emits `Action::SendSnapshot` on the next
    /// cycle (which re-stages the file via Stage 3 and re-emits this
    /// action), so transient failures self-heal without explicit retry
    /// state in the reactor.
    fn install_snapshot(
        &self,
        shard_id: ConsensusStateId,
        leader_term: u64,
        last_included_index: u64,
        last_included_term: u64,
    );
}

/// Default no-op implementation used when no snapshot installer is supplied.
///
/// Used by [`ConsensusEngine::start`](crate::ConsensusEngine::start),
/// [`ConsensusEngine::start_with_wake_notifier`](crate::ConsensusEngine::start_with_wake_notifier),
/// [`ConsensusEngine::start_with_coordinators`](crate::ConsensusEngine::start_with_coordinators),
/// and
/// [`ConsensusEngine::start_with_full_coordinators`](crate::ConsensusEngine::start_with_full_coordinators)
/// so existing call sites that do not yet wire up Stage 4 snapshot
/// install continue to compile without code changes. With this installed,
/// [`Action::InstallSnapshot`](crate::action::Action::InstallSnapshot) is
/// observed but no install is performed — the same drop-and-retry behaviour
/// the reactor exhibits when an async dispatch fails. The leader's next
/// heartbeat retry will re-emit `Action::SendSnapshot`; if a real
/// installer is wired by then, the snapshot installs.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopSnapshotInstaller;

impl SnapshotInstaller for NoopSnapshotInstaller {
    fn install_snapshot(
        &self,
        _shard_id: ConsensusStateId,
        _leader_term: u64,
        _last_included_index: u64,
        _last_included_term: u64,
    ) {
        // No-op. The reactor logs at debug level at the call site; there is
        // no further install action to take when no installer is wired. The
        // leader's heartbeat replicator will re-emit
        // `Action::SendSnapshot` on the next cycle, restaging the file and
        // re-triggering this action.
    }
}
