//! Follower-side snapshot install — Stage 4 of the snapshot install path.
//!
//! Stage 4 closes the loop opened by Stage 3's
//! [`snapshot_streamer`](crate::snapshot_streamer): a leader streams the
//! at-rest encrypted envelope into a `staging-{idx:020}.snap.staged` file
//! under the receiver's per-scope snapshot directory; this module's
//! `RaftManagerSnapshotInstaller` (crate-private) consumes those staged
//! files and feeds the recovered LSNP-v2 plaintext through
//! [`RaftLogStore::install_snapshot`](crate::log_storage::RaftLogStore::install_snapshot).
//!
//! The crate-private installer implements the consensus-crate
//! [`SnapshotInstaller`] trait: when the reactor processes
//! [`Action::InstallSnapshot`](inferadb_ledger_consensus::action::Action::InstallSnapshot),
//! it spawns a tokio task that:
//!
//! 1. Resolves the shard ID to its `(region, organization_id, vault_id?)` scope via
//!    [`RaftManager::resolve_shard_to_scope`](crate::raft_manager::RaftManager::resolve_shard_to_scope).
//! 2. Locates a staged file at `last_included_index` via
//!    [`SnapshotPersister::list_staged`](crate::snapshot_persister::SnapshotPersister::list_staged).
//!    A short polling window (default 5 seconds) covers the race where `Action::InstallSnapshot`
//!    arrives before the streaming RPC's atomic rename lands the file.
//! 3. Reads the file, calls
//!    [`decrypt_snapshot`](inferadb_ledger_consensus::snapshot_crypto::decrypt_snapshot) with the
//!    per-scope key from the
//!    [`SnapshotKeyProvider`](crate::snapshot_key_provider::SnapshotKeyProvider). A decrypt failure
//!    is unrecoverable for this follower; we log + record a metric and prune the staged file.
//! 4. Writes the plaintext to a temp `tokio::fs::File` and routes an
//!    [`ApplyCommand::InstallSnapshot`](crate::apply_command::ApplyCommand::InstallSnapshot) onto
//!    the apply task's control channel. The apply task — which owns the `RaftLogStore` — runs
//!    `install_snapshot` synchronously and forwards the result on a oneshot.
//! 5. On success, calls
//!    [`ConsensusEngine::notify_snapshot_installed`](inferadb_ledger_consensus::ConsensusEngine::notify_snapshot_installed)
//!    so the shard's `last_applied`, `last_snapshot_index`, and
//!    `commit_index` advance, and prunes the staged file.
//!
//! ## Race conditions
//!
//! - **A. `Action::InstallSnapshot` arrives before streaming completes.** The receiver's atomic
//!   rename hasn't landed; `list_staged` returns no match. We poll up to `staged_file_wait`
//!   (default 5s) for the file. On timeout we drop the request; the leader's next heartbeat
//!   re-emits `Action::SendSnapshot`, which restages the file via Stage 3 and re-emits this action.
//! - **B. Decrypt failure (wrong key, tampered envelope).** Unrecoverable for this follower. Log +
//!   record a metric and prune the staged file. A persistent failure (operator misconfigured key
//!   provider) surfaces through the metric.
//! - **C. Install fails partway through.** `RaftLogStore::install_snapshot` is transactional per
//!   the Stage 1b atomic-per-DB-commits invariant — partial success isn't possible. On failure we
//!   leave the staged file in place for the leader's retry; do NOT advance `last_applied`.
//! - **D. Idempotent re-install for same index.** `ConsensusState::handle_snapshot_installed` is
//!   idempotent — repeated calls for the same `last_included_index` are no-ops. The shard-side
//!   `Action::InstallSnapshot` emission also short-circuits when `last_included_index <=
//!   last_snapshot_index`.
//! - **E. Cross-scope install attempt.** `RaftLogStore::install_snapshot` rejects with
//!   `SnapshotError::ScopeMismatch` before any data is written (Stage 1b's defense-in-depth); the
//!   error surfaces through the apply-command oneshot and we drop + log + metric.
//!
//! ## Drop-and-let-Raft-retry
//!
//! Mirrors the Stage 3 sender contract: any failure is a logged + metric'd
//! drop. The leader's heartbeat replicator re-emits
//! [`Action::SendSnapshot`](inferadb_ledger_consensus::action::Action::SendSnapshot)
//! on the next cycle; that action streams the file again (Stage 3) and the
//! receiver's accept emits a fresh
//! [`Action::InstallSnapshot`](inferadb_ledger_consensus::action::Action::InstallSnapshot).
//! No explicit retry state in the reactor.

use std::{
    sync::{Arc, Weak},
    time::{Duration, Instant},
};

use inferadb_ledger_consensus::{
    snapshot_crypto::{SnapshotCryptoError, decrypt_snapshot},
    snapshot_installer::SnapshotInstaller,
    types::ConsensusStateId,
};
use inferadb_ledger_types::{OrganizationId, Region};
use snafu::{Location, ResultExt, Snafu};
use tokio::io::AsyncWriteExt;

use crate::{
    apply_command::ApplyCommand,
    log_storage::{LogId, SnapshotMeta, StoredMembership},
    metrics,
    raft_manager::RaftManager,
    snapshot::SnapshotScope,
    snapshot_persister::PersistError,
};

/// Default time we wait for the streaming RPC's atomic rename to land
/// before giving up on Stage 4's install dispatch.
///
/// Five seconds covers a slow filesystem flush on a busy node; beyond that
/// we drop the request and let the leader re-emit `Action::SendSnapshot`.
pub const DEFAULT_STAGED_WAIT: Duration = Duration::from_secs(5);

/// Polling interval inside the staged-file wait loop.
///
/// Short enough that a quick race resolves with sub-100ms install latency;
/// long enough to keep the busy-wait cost negligible.
const STAGED_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Errors emitted by the install path. Converted to `tracing::warn!` +
/// metric at the spawn boundary; not surfaced upstream.
#[derive(Debug, Snafu)]
pub enum InstallError {
    /// The shard ID did not resolve to a known scope on this node.
    #[snafu(display("no scope mapping for shard {shard_id:?}"))]
    NoScopeMapping {
        /// Shard the install was dispatched for.
        shard_id: ConsensusStateId,
        /// Implicit location.
        #[snafu(implicit)]
        location: Location,
    },

    /// No staged file found at `last_included_index` after the configured
    /// wait. Either the streaming RPC hasn't landed yet (race A) or the
    /// receiver pruned the file on a CRC mismatch / out-of-order rejection.
    #[snafu(display(
        "staged snapshot not found for scope {scope:?} at index {index} after {waited:?}"
    ))]
    StagedNotFound {
        /// Scope we were looking under.
        scope: SnapshotScope,
        /// Region we were looking under.
        region: Region,
        /// Index the action specified.
        index: u64,
        /// How long we waited.
        waited: Duration,
        /// Implicit location.
        #[snafu(implicit)]
        location: Location,
    },

    /// I/O error reading the staged file.
    #[snafu(display("read staged snapshot: {source}"))]
    Io {
        /// Underlying I/O error.
        source: std::io::Error,
        /// Implicit location.
        #[snafu(implicit)]
        location: Location,
    },

    /// Persister-side error.
    #[snafu(display("persister: {source}"))]
    Persister {
        /// Underlying persister error.
        source: PersistError,
        /// Implicit location.
        #[snafu(implicit)]
        location: Location,
    },

    /// Snapshot-decrypt failure — wrong key, tampered envelope, or no key
    /// provider configured for an encrypted file.
    #[snafu(display("decrypt staged snapshot: {source}"))]
    Decrypt {
        /// Underlying crypto error.
        source: SnapshotCryptoError,
        /// Implicit location.
        #[snafu(implicit)]
        location: Location,
    },

    /// No encryption key available for the scope, but the staged file is
    /// non-empty (so we cannot tell whether it is plaintext or encrypted).
    /// We refuse to install without a key — installing what might be raw
    /// ciphertext as plaintext would corrupt the state-machine.
    #[snafu(display(
        "no snapshot key for scope {scope:?} in region {region:?}; \
         refusing to install without a key — cannot distinguish plaintext from ciphertext \
         (configure SnapshotKeyProvider or use NoopSnapshotKeyProvider for unencrypted local-dev)"
    ))]
    NoKeyForEncrypted {
        /// Scope of the staged file.
        scope: SnapshotScope,
        /// Region of the staged file.
        region: Region,
        /// Implicit location.
        #[snafu(implicit)]
        location: Location,
    },

    /// The apply-command channel is closed (the apply worker has shut
    /// down). The scope is being torn down; drop the install.
    #[snafu(display("apply command channel closed for scope {scope:?}"))]
    ApplyChannelClosed {
        /// Scope whose apply task closed.
        scope: SnapshotScope,
        /// Implicit location.
        #[snafu(implicit)]
        location: Location,
    },

    /// `RaftLogStore::install_snapshot` rejected the install (cross-scope
    /// mismatch, table-id whitelist violation, decompression failure, etc).
    #[snafu(display("install_snapshot rejected: {message}"))]
    InstallRejected {
        /// Message from the apply task's `StoreError`.
        message: String,
        /// Implicit location.
        #[snafu(implicit)]
        location: Location,
    },

    /// The apply task dropped the completion channel without responding —
    /// the worker panicked or was cancelled mid-install.
    #[snafu(display("install completion channel dropped for scope {scope:?}"))]
    ApplyCompletionDropped {
        /// Scope whose completion channel closed.
        scope: SnapshotScope,
        /// Implicit location.
        #[snafu(implicit)]
        location: Location,
    },
}

/// Follower-side snapshot installer bound to a [`RaftManager`].
///
/// Sister of `RaftManagerWakeNotifier`, `RaftManagerSnapshotCoordinator`,
/// and `RaftManagerSnapshotSender`: holds an
/// `Arc<Mutex<Weak<RaftManager>>>` shared with the manager, which
/// bootstrap fills via [`RaftManager::install_self_weak`] immediately
/// after wrapping the manager in `Arc`.
///
/// The reactor's snapshot-installer contract requires
/// [`SnapshotInstaller::install_snapshot`] to dispatch I/O asynchronously
/// and return immediately; this implementation `tokio::spawn`s the
/// dispatch task and returns to the reactor after resolving the manager
/// handle.
pub(crate) struct RaftManagerSnapshotInstaller {
    pub(crate) manager: Arc<parking_lot::Mutex<Weak<RaftManager>>>,
    pub(crate) staged_file_wait: Duration,
}

impl SnapshotInstaller for RaftManagerSnapshotInstaller {
    fn install_snapshot(
        &self,
        shard_id: ConsensusStateId,
        leader_term: u64,
        last_included_index: u64,
        last_included_term: u64,
    ) {
        let manager = match self.manager.lock().upgrade() {
            Some(arc) => arc,
            None => {
                tracing::debug!(
                    ?shard_id,
                    leader_term,
                    last_included_index,
                    last_included_term,
                    "RaftManagerSnapshotInstaller: weak upgrade returned None; \
                     skipping install (manager dropped or self_weak not installed)",
                );
                return;
            },
        };

        let staged_wait = self.staged_file_wait;
        tokio::spawn(async move {
            let started = Instant::now();
            let kind = scope_kind_for_shard(&manager, shard_id);
            match dispatch_install(
                &manager,
                shard_id,
                leader_term,
                last_included_index,
                last_included_term,
                staged_wait,
            )
            .await
            {
                Ok(bytes) => {
                    metrics::record_snapshot_install(
                        kind,
                        "success",
                        started.elapsed().as_secs_f64(),
                        bytes,
                    );
                },
                Err(InstallError::StagedNotFound { .. }) => {
                    metrics::record_snapshot_install(
                        kind,
                        "staged_not_found",
                        started.elapsed().as_secs_f64(),
                        0,
                    );
                },
                Err(InstallError::Decrypt { .. }) | Err(InstallError::NoKeyForEncrypted { .. }) => {
                    metrics::record_snapshot_install_decrypt_failed(kind);
                    metrics::record_snapshot_install(
                        kind,
                        "decrypt_failed",
                        started.elapsed().as_secs_f64(),
                        0,
                    );
                },
                Err(InstallError::InstallRejected { ref message, .. })
                    if message.contains("ScopeMismatch") =>
                {
                    metrics::record_snapshot_install(
                        kind,
                        "scope_mismatch",
                        started.elapsed().as_secs_f64(),
                        0,
                    );
                },
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        ?shard_id,
                        leader_term,
                        last_included_index,
                        last_included_term,
                        "RaftManagerSnapshotInstaller: install_snapshot failed (will retry on next heartbeat)",
                    );
                    metrics::record_snapshot_install(
                        kind,
                        "failure",
                        started.elapsed().as_secs_f64(),
                        0,
                    );
                },
            }
        });
    }
}

/// Resolves the shard's scope and returns the metrics label without
/// borrowing the manager beyond the call. Returns `"unknown"` if the
/// shard does not resolve.
fn scope_kind_for_shard(manager: &RaftManager, shard_id: ConsensusStateId) -> &'static str {
    match manager.resolve_shard_to_scope(shard_id) {
        Some((_, _, Some(_))) => "vault",
        Some((_, _, None)) => "org",
        None => "unknown",
    }
}

/// Resolves shard → scope → staged file → decrypt → apply-command → notify.
/// Returns total bytes installed on success.
async fn dispatch_install(
    manager: &Arc<RaftManager>,
    shard_id: ConsensusStateId,
    _leader_term: u64,
    last_included_index: u64,
    last_included_term: u64,
    staged_wait: Duration,
) -> Result<u64, InstallError> {
    let (region, organization_id, vault_id) = manager
        .resolve_shard_to_scope(shard_id)
        .ok_or(InstallError::NoScopeMapping { shard_id, location: snafu::location!() })?;

    let scope = match vault_id {
        Some(vault_id) => SnapshotScope::Vault { organization_id, vault_id },
        None => SnapshotScope::Org { organization_id },
    };

    // Wait for the staged file to land — covers race A.
    let staged_path =
        wait_for_staged(manager, region, scope, last_included_index, staged_wait).await?;

    // Read + decrypt.
    let envelope = tokio::fs::read(&staged_path)
        .await
        .map_err(|e| InstallError::Io { source: e, location: snafu::location!() })?;

    let key = manager.snapshot_persister().snapshot_key_provider().snapshot_key(
        &region,
        organization_id,
        vault_id,
    );

    let plaintext_bytes = match key {
        Some(key) => match decrypt_snapshot(&key, &envelope) {
            Ok(bytes) => bytes,
            Err(e) => {
                // Decrypt failure is unrecoverable; prune the staged file
                // so the leader's retry cycle restages cleanly.
                let _ = manager
                    .snapshot_persister()
                    .remove_staged(region, scope, last_included_index)
                    .await;
                return Err(InstallError::Decrypt { source: e, location: snafu::location!() });
            },
        },
        None => {
            if envelope.is_empty() {
                envelope
            } else {
                let _ = manager
                    .snapshot_persister()
                    .remove_staged(region, scope, last_included_index)
                    .await;
                return Err(InstallError::NoKeyForEncrypted {
                    scope,
                    region,
                    location: snafu::location!(),
                });
            }
        },
    };
    let plaintext_len = plaintext_bytes.len() as u64;

    // Stage the plaintext into a temp file backed by tokio::fs::File so
    // we can hand `Box<tokio::fs::File>` into `RaftLogStore::install_snapshot`.
    // `tempfile::tempfile()` returns an anonymous file backed by the OS
    // temp directory; converting to a `tokio::fs::File` keeps the API
    // consistent with `LedgerSnapshotBuilder::build_snapshot`.
    let std_file = tempfile::tempfile()
        .map_err(|e| InstallError::Io { source: e, location: snafu::location!() })?;
    let mut tokio_file = tokio::fs::File::from_std(std_file);
    tokio_file
        .write_all(&plaintext_bytes)
        .await
        .map_err(|e| InstallError::Io { source: e, location: snafu::location!() })?;
    tokio_file
        .flush()
        .await
        .map_err(|e| InstallError::Io { source: e, location: snafu::location!() })?;

    // Build the SnapshotMeta. The on-disk LSNP-v2 plaintext also carries
    // its own header — `install_snapshot` reads + verifies the header
    // internally — but `install_snapshot`'s signature requires `&SnapshotMeta`
    // for its tracing fields and post-install accounting.
    let meta = SnapshotMeta {
        last_log_id: Some(LogId::new(last_included_term, 0, last_included_index)),
        last_membership: StoredMembership::default(),
        snapshot_id: format!(
            "leader-installed-{region}-{org}-{vault}-{idx}",
            region = region.as_str(),
            org = organization_id.value(),
            vault = vault_id.map(|v| v.value()).unwrap_or(0),
            idx = last_included_index,
        ),
    };

    // Route the install command onto the apply task that owns the
    // `RaftLogStore`. Org-level scopes route through `InnerGroup`'s
    // `apply_command_tx`; per-vault scopes route through
    // `InnerVaultGroup`'s `apply_command_tx`. Both senders are drained by
    // a `tokio::select!` arm whose install branch awaits
    // `RaftLogStore::install_snapshot` to completion before yielding back
    // to the loop, preventing concurrent batch apply.
    let (completion_tx, completion_rx) = tokio::sync::oneshot::channel();
    let apply_command_tx = match vault_id {
        Some(vault_id) => {
            match manager.apply_command_sender_for_vault(region, organization_id, vault_id) {
                Some(tx) => tx,
                None => {
                    return Err(InstallError::ApplyChannelClosed {
                        scope,
                        location: snafu::location!(),
                    });
                },
            }
        },
        None => match manager.apply_command_sender(region, organization_id) {
            Some(tx) => tx,
            None => {
                return Err(InstallError::ApplyChannelClosed {
                    scope,
                    location: snafu::location!(),
                });
            },
        },
    };
    apply_command_tx
        .send(ApplyCommand::InstallSnapshot {
            meta,
            plaintext: Box::new(tokio_file),
            completion: completion_tx,
            _backend: std::marker::PhantomData,
        })
        .await
        .map_err(|_| InstallError::ApplyChannelClosed { scope, location: snafu::location!() })?;

    let install_result = completion_rx.await.map_err(|_| InstallError::ApplyCompletionDropped {
        scope,
        location: snafu::location!(),
    })?;
    install_result.map_err(|e| InstallError::InstallRejected {
        message: e.to_string(),
        location: snafu::location!(),
    })?;

    // Notify the engine so the shard's `last_applied` /
    // `last_snapshot_index` advance and the log prefix is truncated.
    let engine = manager
        .snapshot_engine_for_shard(region, organization_id, vault_id)
        .ok_or(InstallError::ApplyChannelClosed { scope, location: snafu::location!() })?;
    if let Err(e) =
        engine.notify_snapshot_installed(shard_id, last_included_index, last_included_term).await
    {
        tracing::warn!(
            error = %e,
            ?shard_id,
            last_included_index,
            "RaftManagerSnapshotInstaller: notify_snapshot_installed failed (install succeeded but \
             shard's last_snapshot_index will lag until the next install round-trip)",
        );
    }

    // Successfully installed — prune the staged file so the persister's
    // listing stays clean.
    if let Err(e) =
        manager.snapshot_persister().remove_staged(region, scope, last_included_index).await
    {
        tracing::warn!(
            error = %e,
            ?scope,
            last_included_index,
            "RaftManagerSnapshotInstaller: remove_staged failed post-install (file will be \
             cleaned up on next list_staged sweep)",
        );
    }

    tracing::info!(
        region = region.as_str(),
        organization_id = organization_id.value(),
        vault_id = vault_id.map(|v| v.value()),
        shard = shard_id.0,
        last_included_index,
        last_included_term,
        plaintext_bytes = plaintext_len,
        "Snapshot installed",
    );

    Ok(plaintext_len)
}

/// Polls `list_staged` for up to `max_wait` waiting for a file matching
/// `index` to land. Returns the file's path on success.
async fn wait_for_staged(
    manager: &RaftManager,
    region: Region,
    scope: SnapshotScope,
    index: u64,
    max_wait: Duration,
) -> Result<std::path::PathBuf, InstallError> {
    let deadline = Instant::now() + max_wait;
    loop {
        let staged = manager
            .snapshot_persister()
            .list_staged(region, scope)
            .await
            .context(PersisterSnafu)?;
        if let Some(meta) = staged.into_iter().find(|m| m.index == index) {
            return Ok(meta.path);
        }
        if Instant::now() >= deadline {
            return Err(InstallError::StagedNotFound {
                scope,
                region,
                index,
                waited: max_wait,
                location: snafu::location!(),
            });
        }
        tokio::time::sleep(STAGED_POLL_INTERVAL).await;
    }
}

// Helper: `OrganizationId` field path is internal to RaftManager; the
// installer accesses it through public methods. No additional `use` needed
// here.
#[allow(dead_code)]
fn _unused(_: OrganizationId) {}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::disallowed_methods, clippy::panic)]
mod tests {
    use std::sync::{Arc, Weak};

    use inferadb_ledger_consensus::{
        snapshot_installer::SnapshotInstaller, types::ConsensusStateId,
    };
    use inferadb_ledger_types::Region;
    use tempfile::TempDir;

    use super::{DEFAULT_STAGED_WAIT, RaftManagerSnapshotInstaller};
    use crate::raft_manager::{RaftManager, RaftManagerConfig};

    /// Installer with no resolvable scope mapping should drop silently —
    /// the reactor's contract is "fire-and-forget"; the leader's heartbeat
    /// retransmits on the next cycle.
    #[tokio::test]
    async fn install_with_no_scope_mapping_drops_silently() {
        let temp = TempDir::new().expect("tempdir");
        let cfg = RaftManagerConfig::new(temp.path().to_path_buf(), 1, Region::GLOBAL);
        let registry = Arc::new(crate::node_registry::NodeConnectionRegistry::new());
        let manager = Arc::new(RaftManager::new(cfg, registry));
        manager.install_self_weak();

        let installer = RaftManagerSnapshotInstaller {
            manager: Arc::new(parking_lot::Mutex::new(Arc::downgrade(&manager))),
            staged_file_wait: DEFAULT_STAGED_WAIT,
        };
        // Shard 999 was never registered — the dispatch task will log +
        // metric and exit. Contract is: no panic.
        installer.install_snapshot(ConsensusStateId(999), 1, 100, 1);
        // Give the spawned task a beat to land.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        // No panic == pass.
    }

    /// Dropping the manager between weak-construction and dispatch must
    /// degrade to a debug log rather than a panic — same contract as the
    /// wake notifier, snapshot coordinator, and snapshot sender.
    #[tokio::test]
    async fn install_with_dropped_manager_no_panic() {
        let weak: Weak<RaftManager> = Weak::new();
        let installer = RaftManagerSnapshotInstaller {
            manager: Arc::new(parking_lot::Mutex::new(weak)),
            staged_file_wait: DEFAULT_STAGED_WAIT,
        };
        installer.install_snapshot(ConsensusStateId(1), 1, 1, 1);
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}
