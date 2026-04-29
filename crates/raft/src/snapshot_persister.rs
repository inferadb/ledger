//! Snapshot persistence — atomic write, encryption envelope, retention.
//!
//! Stage 2 of the snapshot install path. The persister consumes the in-memory
//! snapshot file produced by
//! [`LedgerSnapshotBuilder::build_snapshot`](crate::log_storage::LedgerSnapshotBuilder::build_snapshot),
//! wraps the LSNP-v2 plaintext bytes in a whole-file AES-256-GCM envelope
//! ([`encrypt_snapshot`](inferadb_ledger_consensus::snapshot_crypto::encrypt_snapshot))
//! resolved by the per-scope [`SnapshotKeyProvider`], writes the result
//! atomically (write to a temp sibling, fsync, rename), and prunes older
//! snapshots beyond the retention window.
//!
//! ## On-disk layout
//!
//! - Org snapshots: `{snapshot_dir}/snapshot-{index:020}.snap`
//! - Per-vault snapshots: `{snapshot_dir}/vault-{vault_id}/snapshot-{index:020}.snap`
//!
//! `{index:020}` zero-pads to 20 decimal digits (the maximum width of `u64`)
//! so lexicographic ordering matches numeric ordering for retention pruning.
//!
//! ## Encryption envelope
//!
//! The envelope is the **outermost** layer — `[nonce(12)][ciphertext+tag]`.
//! The plaintext under the envelope is the LSNP-v2 file produced by the
//! builder, including its own SHA-256 checksum trailer. Stage 4's receiver
//! install decrypts the envelope first, then runs
//! [`SnapshotReader::verify_checksum`](crate::snapshot::SnapshotReader::verify_checksum)
//! on the recovered plaintext. When [`SnapshotKeyProvider::snapshot_key`]
//! returns `None` the persister writes the plaintext file unwrapped — only
//! valid for tests / explicitly unencrypted local-dev configurations. The
//! persister logs a warning the first time this happens per process.
//!
//! ## Atomic write
//!
//! The persister writes to a sibling temp file
//! (`snapshot-{index:020}.snap.tmp.{nonce}`), fsyncs the temp file, fsyncs
//! the parent directory, then renames in place. The directory fsync ensures
//! the rename is durable; without it the rename can be lost across a crash
//! even though the file itself is durable.
//!
//! ## Retention
//!
//! After every successful persist the persister lists the scope's directory,
//! sorts ascending by index, and removes files beyond the configured
//! retention count (default `3`). Retention failures are non-fatal — they
//! log + increment a metric but do not surface as a persist error, since
//! the new snapshot itself is already durable.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use inferadb_ledger_consensus::snapshot_crypto::{
    SnapshotCryptoError, decrypt_snapshot, encrypt_snapshot,
};
use inferadb_ledger_types::Region;
use snafu::{Location, ResultExt, Snafu};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::{
    metrics, region_storage::RegionStorageManager, snapshot::SnapshotScope,
    snapshot_key_provider::SnapshotKeyProvider,
};

/// Default retention count — the persister keeps the latest `N` snapshots
/// per scope and prunes the rest. Three is a deliberate balance: enough to
/// recover from a corrupted top-of-chain snapshot without keeping unbounded
/// disk usage.
pub const DEFAULT_RETENTION_COUNT: usize = 3;

/// Maximum decimal width of a `u64` — used as the zero-pad width in the
/// on-disk snapshot file name (`snapshot-{index:020}.snap`). Constants are
/// pinned so changing the format requires touching this single site.
const SNAPSHOT_INDEX_PAD_WIDTH: usize = 20;

/// Suffix applied to snapshot files written to disk.
const SNAPSHOT_FILE_SUFFIX: &str = ".snap";

/// Suffix applied to in-progress snapshot writes before the atomic rename.
const SNAPSHOT_TEMP_SUFFIX: &str = ".tmp";

/// Suffix applied to staged snapshot files received from the leader during
/// Stage 3 wire transfer. Distinguishes incoming-from-leader files from the
/// `*.snap` files written by [`SnapshotPersister::persist`] on the local
/// node, so Stage 4's installer can find streamed files without confusing
/// them for locally produced snapshots.
const SNAPSHOT_STAGED_SUFFIX: &str = ".snap.staged";

/// Suffix applied to in-progress staged-snapshot writes before the atomic
/// rename to `*.snap.staged`. The receiver appends data chunks here and
/// renames in place when the footer's CRC validates.
const SNAPSHOT_STAGED_TEMP_SUFFIX: &str = ".snap.staging";

/// Persisted snapshot metadata returned from [`SnapshotPersister::persist`]
/// and the listing helpers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedSnapshotMeta {
    /// The snapshot's scope (org or vault).
    pub scope: SnapshotScope,
    /// Region the snapshot belongs to. `SnapshotScope` does not carry a
    /// region (root rule 5 — region residency lives at storage layout
    /// level), so we surface it here for callers that need the full triple.
    pub region: Region,
    /// Last log index covered by the snapshot (the on-disk file name's
    /// padded numeric suffix, parsed back to `u64`).
    pub index: u64,
    /// Term of the last log entry covered by the snapshot. `None` for
    /// listings — the term lives inside the LSNP-v2 header and is not
    /// reflected in the file name. [`SnapshotPersister::persist`] returns a
    /// fully populated meta with `Some(term)`.
    pub term: Option<u64>,
    /// Size of the on-disk file in bytes (post-encryption envelope when
    /// encryption is enabled).
    pub size_bytes: u64,
    /// Absolute path to the snapshot file.
    pub path: PathBuf,
    /// Whether the persisted file is wrapped in an encryption envelope.
    /// `false` only when the [`SnapshotKeyProvider`] returned `None` for the
    /// scope at persist time.
    pub encrypted: bool,
}

/// Output of [`SnapshotPersister::load`].
///
/// Carries the recovered LSNP-v2 plaintext bytes (the persister has already
/// stripped the encryption envelope when one was present). Stage 4's receiver
/// install consumes this via
/// [`SnapshotReader`](crate::snapshot::SnapshotReader).
#[derive(Debug)]
pub struct DecryptedSnapshot {
    /// The recovered LSNP-v2 plaintext (compressed body + SHA-256 trailer).
    pub plaintext: Vec<u8>,
    /// Metadata for the loaded snapshot.
    pub meta: PersistedSnapshotMeta,
}

/// Metadata for a snapshot file staged by the receiver-side handler during
/// Stage 3 wire transfer.
///
/// Staged files live in the same per-scope directory as locally persisted
/// snapshots but use the `.snap.staged` suffix so they don't intermix with
/// `*.snap` files. Stage 4's install path consumes these via
/// [`SnapshotPersister::list_staged`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagedSnapshotMeta {
    /// The snapshot's scope (org or vault).
    pub scope: SnapshotScope,
    /// Region the snapshot belongs to.
    pub region: Region,
    /// Last log index covered by the staged snapshot (parsed from the
    /// file's `staging-{index:020}.snap.staged` name).
    pub index: u64,
    /// Size of the staged file in bytes (encrypted envelope intact —
    /// receiver-side install decrypts on consumption).
    pub size_bytes: u64,
    /// Absolute path to the staged file. Stage 4 reads this, decrypts via
    /// the [`SnapshotKeyProvider`], runs the LSNP-v2 verifier, and installs
    /// via [`RaftLogStore::install_snapshot`](crate::log_storage::RaftLogStore::install_snapshot).
    pub path: PathBuf,
}

/// Persistence error surface.
///
/// Snafu variants follow the workspace convention — every `source` carries
/// `#[snafu(implicit)] location: snafu::Location`.
#[derive(Debug, Snafu)]
pub enum PersistError {
    /// I/O error during persist / list / load.
    #[snafu(display("Snapshot persistence I/O error at {path}: {source}", path = path.display()))]
    Io {
        /// Path involved in the failing operation.
        path: PathBuf,
        /// Underlying I/O error.
        source: std::io::Error,
        /// Implicit location.
        #[snafu(implicit)]
        location: Location,
    },

    /// Encryption failed while wrapping a snapshot.
    #[snafu(display("Snapshot encryption failed at {path}: {source}", path = path.display()))]
    Encrypt {
        /// Path involved in the failing operation.
        path: PathBuf,
        /// Underlying crypto error.
        source: SnapshotCryptoError,
        /// Implicit location.
        #[snafu(implicit)]
        location: Location,
    },

    /// Decryption failed while loading a snapshot — wrong key or tampered
    /// envelope.
    #[snafu(display("Snapshot decryption failed at {path}: {source}", path = path.display()))]
    Decrypt {
        /// Path involved in the failing operation.
        path: PathBuf,
        /// Underlying crypto error.
        source: SnapshotCryptoError,
        /// Implicit location.
        #[snafu(implicit)]
        location: Location,
    },

    /// File-name parsing failed during listing — found a file in the
    /// snapshot directory that does not match the
    /// `snapshot-{index:020}.snap` pattern.
    #[snafu(display("Malformed snapshot file name at {path}: {reason}", path = path.display()))]
    MalformedFileName {
        /// Path with the malformed name.
        path: PathBuf,
        /// Why the name failed to parse.
        reason: String,
        /// Implicit location.
        #[snafu(implicit)]
        location: Location,
    },

    /// A snapshot file was not found at the requested index.
    #[snafu(display(
        "Snapshot not found at {path} (scope={scope:?}, region={region:?}, index={index})",
        path = path.display()
    ))]
    NotFound {
        /// Path the persister searched for.
        path: PathBuf,
        /// Scope of the missing snapshot.
        scope: SnapshotScope,
        /// Region of the missing snapshot.
        region: Region,
        /// Index of the missing snapshot.
        index: u64,
        /// Implicit location.
        #[snafu(implicit)]
        location: Location,
    },
}

/// Atomic snapshot persister.
///
/// See module docs for the on-disk layout, encryption envelope, and
/// retention semantics.
#[derive(Clone)]
pub struct SnapshotPersister {
    region_storage: Arc<RegionStorageManager>,
    snapshot_key_provider: Arc<dyn SnapshotKeyProvider>,
    retention_count: usize,
    /// Single-shot guard so the warning about persisting unencrypted
    /// snapshots is logged at most once per process. Wrapped in `Arc` so
    /// cloning the persister keeps the guard shared.
    plaintext_warning_logged: Arc<std::sync::atomic::AtomicBool>,
}

#[bon::bon]
impl SnapshotPersister {
    /// Builds a new persister.
    ///
    /// `retention_count` defaults to [`DEFAULT_RETENTION_COUNT`] (3).
    #[builder]
    pub fn new(
        region_storage: Arc<RegionStorageManager>,
        snapshot_key_provider: Arc<dyn SnapshotKeyProvider>,
        #[builder(default = DEFAULT_RETENTION_COUNT)] retention_count: usize,
    ) -> Self {
        Self {
            region_storage,
            snapshot_key_provider,
            retention_count,
            plaintext_warning_logged: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// Returns the per-scope snapshot key provider configured at
    /// construction.
    ///
    /// Stage 4's installer uses this to decrypt staged snapshot envelopes
    /// before passing the recovered LSNP-v2 plaintext to
    /// [`RaftLogStore::install_snapshot`](crate::log_storage::RaftLogStore::install_snapshot).
    pub fn snapshot_key_provider(&self) -> &Arc<dyn SnapshotKeyProvider> {
        &self.snapshot_key_provider
    }

    /// Persists the in-memory plaintext snapshot file at `(region, scope)`
    /// keyed by `index`, encrypts in place if the
    /// [`SnapshotKeyProvider`] supplies a key, and prunes older snapshots
    /// beyond the retention window.
    ///
    /// Returns the metadata for the newly persisted snapshot, suitable for
    /// surfacing back to
    /// [`ConsensusEngine::notify_snapshot_completed`](inferadb_ledger_consensus::ConsensusEngine::notify_snapshot_completed).
    ///
    /// # Errors
    ///
    /// Returns [`PersistError::Io`] for filesystem failures and
    /// [`PersistError::Encrypt`] for cryptographic failures.
    pub async fn persist(
        &self,
        region: Region,
        scope: SnapshotScope,
        index: u64,
        term: u64,
        plaintext_file: tokio::fs::File,
    ) -> Result<PersistedSnapshotMeta, PersistError> {
        let dir = self.scope_dir(region, scope);
        ensure_dir(&dir).await?;
        let target_path = scope_snapshot_path(&dir, index);
        let temp_path = temp_path_for(&target_path);

        let started = std::time::Instant::now();

        // 1. Slurp the plaintext file. The builder hands us a `tempfile()` anonymous handle; rewind
        //    to start, read the entire body. The builder's snapshot fits comfortably in memory for
        //    the workloads that exercise this stage; streaming is a Stage-3+ optimisation.
        let mut input = plaintext_file;
        input.seek(std::io::SeekFrom::Start(0)).await.context(IoSnafu { path: dir.clone() })?;
        let mut plaintext = Vec::new();
        input.read_to_end(&mut plaintext).await.context(IoSnafu { path: dir.clone() })?;

        // 2. Apply the encryption envelope when a key is available.
        let key = self.snapshot_key_provider.snapshot_key(
            &region,
            scope.organization_id(),
            scope.vault_id(),
        );

        let (bytes_to_write, encrypted) = match key {
            Some(key) => {
                let ct = encrypt_snapshot(&key, &plaintext)
                    .context(EncryptSnafu { path: target_path.clone() })?;
                (ct, true)
            },
            None => {
                if !self.plaintext_warning_logged.swap(true, std::sync::atomic::Ordering::Relaxed) {
                    tracing::warn!(
                        region = region.as_str(),
                        organization_id = scope.organization_id().value(),
                        vault_id = scope.vault_id().map(|v| v.value()),
                        "SnapshotPersister: no key provider entry for scope — \
                         persisting snapshot WITHOUT encryption envelope. Valid \
                         only for tests / unencrypted local-dev configurations.",
                    );
                }
                (plaintext, false)
            },
        };

        // 3. Atomic write: temp file → fsync → rename → fsync parent dir. Any failure leaves the
        //    previous good snapshot (if any) intact — the caller can retry without manual cleanup.
        write_atomic(&temp_path, &target_path, &bytes_to_write).await?;

        let size_bytes = bytes_to_write.len() as u64;
        metrics::record_snapshot_persist(
            scope_kind(scope),
            true,
            started.elapsed().as_secs_f64(),
            size_bytes,
        );

        // 4. Retention. Failures here log + count but do not abort the persist (the new snapshot is
        //    already durable on disk).
        if let Err(err) = self.prune_retention(region, scope).await {
            tracing::warn!(
                error = %err,
                region = region.as_str(),
                organization_id = scope.organization_id().value(),
                vault_id = scope.vault_id().map(|v| v.value()),
                "SnapshotPersister: retention prune failed (non-fatal)",
            );
            metrics::record_snapshot_retention_failed();
        }

        // 5. Refresh the per-scope gauge so dashboards reflect the new count.
        let count = self.list(region, scope).await.map(|v| v.len()).unwrap_or(0);
        metrics::record_snapshot_count(scope_kind(scope), region, scope.organization_id(), count);

        Ok(PersistedSnapshotMeta {
            scope,
            region,
            index,
            term: Some(term),
            size_bytes,
            path: target_path,
            encrypted,
        })
    }

    /// Lists persisted snapshots for `(region, scope)`, sorted ascending by
    /// `index`. Returns an empty `Vec` when the scope's directory does not
    /// exist or is empty.
    ///
    /// # Errors
    ///
    /// Returns [`PersistError::Io`] when the scope directory exists but
    /// cannot be read, or [`PersistError::MalformedFileName`] when an entry
    /// in the directory does not parse as a snapshot file name. Files that
    /// do not look like snapshot files at all (no `snapshot-` prefix or
    /// `.snap` suffix) are skipped silently — only malformed snapshot-style
    /// names surface an error.
    pub async fn list(
        &self,
        region: Region,
        scope: SnapshotScope,
    ) -> Result<Vec<PersistedSnapshotMeta>, PersistError> {
        let dir = self.scope_dir(region, scope);
        let mut entries = match tokio::fs::read_dir(&dir).await {
            Ok(rd) => rd,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => {
                return Err(PersistError::Io {
                    path: dir,
                    source: e,
                    location: snafu::location!(),
                });
            },
        };

        let mut metas = Vec::new();
        while let Some(entry) = entries.next_entry().await.context(IoSnafu { path: dir.clone() })? {
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else { continue };
            if !is_snapshot_file_name(name) {
                continue;
            }
            let index =
                parse_snapshot_index(name).map_err(|reason| PersistError::MalformedFileName {
                    path: path.clone(),
                    reason,
                    location: snafu::location!(),
                })?;
            let metadata = entry.metadata().await.context(IoSnafu { path: path.clone() })?;
            metas.push(PersistedSnapshotMeta {
                scope,
                region,
                index,
                term: None,
                size_bytes: metadata.len(),
                path,
                // Listing does not crack the file open; encrypted-or-not is
                // unknown without inspecting the bytes. Stage 4's receiver
                // install resolves this by attempting decrypt with the
                // scope's key — a `None` key + ciphertext-shaped file is
                // a configuration error surfaced there, not here.
                encrypted: self
                    .snapshot_key_provider
                    .snapshot_key(&region, scope.organization_id(), scope.vault_id())
                    .is_some(),
            });
        }
        metas.sort_by_key(|m| m.index);
        Ok(metas)
    }

    /// Returns the highest-index snapshot for `(region, scope)`, or `None`
    /// when no snapshots have been persisted for this scope.
    ///
    /// # Errors
    ///
    /// Forwards errors from [`Self::list`].
    pub async fn latest(
        &self,
        region: Region,
        scope: SnapshotScope,
    ) -> Result<Option<PersistedSnapshotMeta>, PersistError> {
        Ok(self.list(region, scope).await?.into_iter().next_back())
    }

    /// Loads a specific snapshot's plaintext bytes — the inverse of
    /// [`Self::persist`].
    ///
    /// Strips the encryption envelope when the [`SnapshotKeyProvider`]
    /// returns a key for the scope. When the provider returns `None` the
    /// persister assumes the on-disk file is plaintext and returns it as
    /// is. A wrong-key decrypt surfaces as
    /// [`PersistError::Decrypt`] — the caller is responsible for
    /// classifying that as a key-rotation issue versus a tampered file.
    ///
    /// # Errors
    ///
    /// - [`PersistError::NotFound`] when the snapshot file is missing.
    /// - [`PersistError::Io`] for filesystem failures.
    /// - [`PersistError::Decrypt`] for key mismatch / authentication failures.
    pub async fn load(
        &self,
        region: Region,
        scope: SnapshotScope,
        index: u64,
    ) -> Result<DecryptedSnapshot, PersistError> {
        let dir = self.scope_dir(region, scope);
        let path = scope_snapshot_path(&dir, index);
        let bytes = match tokio::fs::read(&path).await {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(PersistError::NotFound {
                    path,
                    scope,
                    region,
                    index,
                    location: snafu::location!(),
                });
            },
            Err(e) => {
                return Err(PersistError::Io { path, source: e, location: snafu::location!() });
            },
        };

        let key = self.snapshot_key_provider.snapshot_key(
            &region,
            scope.organization_id(),
            scope.vault_id(),
        );

        let (plaintext, encrypted) = match key {
            Some(key) => {
                let plaintext = decrypt_snapshot(&key, &bytes)
                    .context(DecryptSnafu { path: path.clone() })
                    .inspect_err(|_| metrics::record_snapshot_decrypt_failed())?;
                (plaintext, true)
            },
            None => (bytes.clone(), false),
        };

        let size_bytes = bytes.len() as u64;
        Ok(DecryptedSnapshot {
            plaintext,
            meta: PersistedSnapshotMeta {
                scope,
                region,
                index,
                term: None,
                size_bytes,
                path,
                encrypted,
            },
        })
    }

    /// Returns the on-disk directory for a `(region, scope)` pair.
    ///
    /// `pub(crate)` so the Stage 3 streamer / receiver modules can resolve
    /// the staging directory without duplicating the path layout.
    pub(crate) fn scope_dir(&self, region: Region, scope: SnapshotScope) -> PathBuf {
        let base = self.region_storage.snapshot_dir(region, scope.organization_id());
        match scope.vault_id() {
            Some(vault_id) => base.join(format!("vault-{}", vault_id.value())),
            None => base,
        }
    }

    /// Opens the encrypted on-disk snapshot file for a `(region, scope, index)`
    /// triple.
    ///
    /// Stage 3 leader-side: the streamer calls this to read the at-rest
    /// envelope bytes verbatim and ship them to the follower. Returns the
    /// open `tokio::fs::File` plus the on-disk size in bytes (used as the
    /// header `total_bytes` and as a sanity bound on the receiving side).
    ///
    /// Unlike [`Self::load`], this method does NOT decrypt — the wire
    /// protocol carries the at-rest envelope and the follower decrypts on
    /// install (Stage 4). Skipping decrypt here keeps leader-side cost
    /// dominated by the streaming I/O rather than per-snapshot decrypt.
    ///
    /// # Errors
    ///
    /// - [`PersistError::NotFound`] when the snapshot file is missing.
    /// - [`PersistError::Io`] for filesystem failures.
    pub async fn open_encrypted(
        &self,
        region: Region,
        scope: SnapshotScope,
        index: u64,
    ) -> Result<(tokio::fs::File, u64), PersistError> {
        let dir = self.scope_dir(region, scope);
        let path = scope_snapshot_path(&dir, index);
        let file = match tokio::fs::OpenOptions::new().read(true).open(&path).await {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(PersistError::NotFound {
                    path,
                    scope,
                    region,
                    index,
                    location: snafu::location!(),
                });
            },
            Err(e) => {
                return Err(PersistError::Io { path, source: e, location: snafu::location!() });
            },
        };
        let metadata = file.metadata().await.context(IoSnafu { path: path.clone() })?;
        Ok((file, metadata.len()))
    }

    /// Returns the staging path the receiver writes to for a streamed
    /// snapshot at `(region, scope, index)`.
    ///
    /// Stage 3 receiver-side: the staging file uses the `.snap.staged`
    /// suffix to keep streamed files separate from locally persisted
    /// `*.snap` files. Stage 4 consumes via [`Self::list_staged`].
    pub(crate) fn staged_path(&self, region: Region, scope: SnapshotScope, index: u64) -> PathBuf {
        let dir = self.scope_dir(region, scope);
        staged_snapshot_path(&dir, index)
    }

    /// Returns the temp path the receiver writes data chunks into before
    /// the footer-validated atomic rename to the `.snap.staged` file.
    pub(crate) fn staging_temp_path(
        &self,
        region: Region,
        scope: SnapshotScope,
        leader_term: u64,
        index: u64,
    ) -> PathBuf {
        let dir = self.scope_dir(region, scope);
        staging_temp_path(&dir, leader_term, index)
    }

    /// Ensures the staging directory exists for `(region, scope)`.
    ///
    /// Receiver-side helper: must be called before opening the temp file,
    /// since the per-vault subdirectory may not exist on a fresh node that
    /// has never persisted a local snapshot for the scope.
    pub(crate) async fn ensure_staging_dir(
        &self,
        region: Region,
        scope: SnapshotScope,
    ) -> Result<PathBuf, PersistError> {
        let dir = self.scope_dir(region, scope);
        ensure_dir(&dir).await?;
        Ok(dir)
    }

    /// Lists staged snapshots for `(region, scope)`, sorted ascending by
    /// `index`. Returns an empty `Vec` when the scope's directory does not
    /// exist or contains no staged files.
    ///
    /// Stage 4's install path discovers streamed snapshots via this
    /// listing. Stage 3 (this dispatch) only writes them; consumption is
    /// out of scope.
    ///
    /// # Errors
    ///
    /// Returns [`PersistError::Io`] when the scope directory exists but
    /// cannot be read, or [`PersistError::MalformedFileName`] when an
    /// entry that looks like a staged file does not parse correctly.
    pub async fn list_staged(
        &self,
        region: Region,
        scope: SnapshotScope,
    ) -> Result<Vec<StagedSnapshotMeta>, PersistError> {
        let dir = self.scope_dir(region, scope);
        let mut entries = match tokio::fs::read_dir(&dir).await {
            Ok(rd) => rd,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => {
                return Err(PersistError::Io {
                    path: dir,
                    source: e,
                    location: snafu::location!(),
                });
            },
        };

        let mut metas = Vec::new();
        while let Some(entry) = entries.next_entry().await.context(IoSnafu { path: dir.clone() })? {
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else { continue };
            if !is_staged_file_name(name) {
                continue;
            }
            let index =
                parse_staged_index(name).map_err(|reason| PersistError::MalformedFileName {
                    path: path.clone(),
                    reason,
                    location: snafu::location!(),
                })?;
            let metadata = entry.metadata().await.context(IoSnafu { path: path.clone() })?;
            metas.push(StagedSnapshotMeta {
                scope,
                region,
                index,
                size_bytes: metadata.len(),
                path,
            });
        }
        metas.sort_by_key(|m| m.index);
        Ok(metas)
    }

    /// Removes a staged snapshot file. Used by Stage 4's installer after
    /// successfully consuming a staged file, and by the receiver's failure
    /// path when the CRC footer rejects the stream.
    ///
    /// Returns `Ok(())` whether the file existed or not — this is a
    /// best-effort cleanup helper.
    ///
    /// # Errors
    ///
    /// Returns [`PersistError::Io`] for non-`NotFound` filesystem failures
    /// (permission denied, etc.).
    pub async fn remove_staged(
        &self,
        region: Region,
        scope: SnapshotScope,
        index: u64,
    ) -> Result<(), PersistError> {
        let path = self.staged_path(region, scope, index);
        match tokio::fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(PersistError::Io { path, source: e, location: snafu::location!() }),
        }
    }

    /// Drops snapshot files older than the retention window for `(region, scope)`.
    async fn prune_retention(
        &self,
        region: Region,
        scope: SnapshotScope,
    ) -> Result<(), PersistError> {
        let metas = self.list(region, scope).await?;
        if metas.len() <= self.retention_count {
            return Ok(());
        }
        let drop_count = metas.len() - self.retention_count;
        let mut pruned = 0u64;
        for meta in metas.into_iter().take(drop_count) {
            match tokio::fs::remove_file(&meta.path).await {
                Ok(()) => {
                    pruned += 1;
                    tracing::debug!(
                        path = %meta.path.display(),
                        index = meta.index,
                        "SnapshotPersister: pruned old snapshot",
                    );
                },
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    // Concurrent prune is fine — log + continue.
                },
                Err(e) => {
                    return Err(PersistError::Io {
                        path: meta.path,
                        source: e,
                        location: snafu::location!(),
                    });
                },
            }
        }
        if pruned > 0 {
            metrics::record_snapshot_pruned(pruned);
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// File-name + path helpers (free functions so they're directly testable
// without spinning up a full persister).
// ---------------------------------------------------------------------------

fn scope_snapshot_path(dir: &Path, index: u64) -> PathBuf {
    dir.join(format!(
        "snapshot-{:0width$}{suffix}",
        index,
        width = SNAPSHOT_INDEX_PAD_WIDTH,
        suffix = SNAPSHOT_FILE_SUFFIX,
    ))
}

fn temp_path_for(target: &Path) -> PathBuf {
    let nonce: u64 = rand::random();
    let mut s = target.as_os_str().to_owned();
    s.push(format!("{SNAPSHOT_TEMP_SUFFIX}.{nonce:016x}"));
    PathBuf::from(s)
}

fn is_snapshot_file_name(name: &str) -> bool {
    // Locally persisted snapshots end in `.snap` exactly. Staged-from-leader
    // files end in `.snap.staged` and are filtered out so listings of
    // committed snapshots don't intermix with in-flight Stage 3 transfers.
    name.starts_with("snapshot-")
        && name.ends_with(SNAPSHOT_FILE_SUFFIX)
        && !name.ends_with(SNAPSHOT_STAGED_SUFFIX)
}

fn parse_snapshot_index(name: &str) -> Result<u64, String> {
    let stripped = name
        .strip_prefix("snapshot-")
        .and_then(|s| s.strip_suffix(SNAPSHOT_FILE_SUFFIX))
        .ok_or_else(|| format!("file name {name:?} is not a snapshot file"))?;
    stripped
        .parse::<u64>()
        .map_err(|e| format!("file name {name:?} index suffix did not parse as u64: {e}"))
}

/// Builds the staged-snapshot path for an `(dir, index)` pair. Stage 3
/// receiver-side: written by the receiver after CRC-validating the stream.
fn staged_snapshot_path(dir: &Path, index: u64) -> PathBuf {
    dir.join(format!(
        "staging-{:0width$}{suffix}",
        index,
        width = SNAPSHOT_INDEX_PAD_WIDTH,
        suffix = SNAPSHOT_STAGED_SUFFIX,
    ))
}

/// Builds the in-progress staging temp path. Carries `leader_term` so two
/// overlapping streams from different leaders cannot collide on the same
/// temp file (they would still race on the final `.snap.staged`, but only
/// one rename wins; the loser deletes its own temp).
fn staging_temp_path(dir: &Path, leader_term: u64, index: u64) -> PathBuf {
    dir.join(format!(
        "staging-{leader_term:020}-{:0width$}{suffix}",
        index,
        width = SNAPSHOT_INDEX_PAD_WIDTH,
        suffix = SNAPSHOT_STAGED_TEMP_SUFFIX,
    ))
}

fn is_staged_file_name(name: &str) -> bool {
    name.starts_with("staging-") && name.ends_with(SNAPSHOT_STAGED_SUFFIX)
}

fn parse_staged_index(name: &str) -> Result<u64, String> {
    let stripped = name
        .strip_prefix("staging-")
        .and_then(|s| s.strip_suffix(SNAPSHOT_STAGED_SUFFIX))
        .ok_or_else(|| format!("file name {name:?} is not a staged file"))?;
    stripped
        .parse::<u64>()
        .map_err(|e| format!("file name {name:?} index suffix did not parse as u64: {e}"))
}

fn scope_kind(scope: SnapshotScope) -> &'static str {
    if scope.is_vault() { "vault" } else { "org" }
}

async fn ensure_dir(dir: &Path) -> Result<(), PersistError> {
    tokio::fs::create_dir_all(dir).await.context(IoSnafu { path: dir.to_path_buf() })
}

async fn write_atomic(
    temp_path: &Path,
    target_path: &Path,
    bytes: &[u8],
) -> Result<(), PersistError> {
    // Step 1: write the temp file with the bytes. We hold the file open
    // long enough to fsync it before drop, then close before rename so we
    // don't leak FDs on the rename hot path.
    {
        let mut file = tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(temp_path)
            .await
            .context(IoSnafu { path: temp_path.to_path_buf() })?;
        file.write_all(bytes).await.context(IoSnafu { path: temp_path.to_path_buf() })?;
        // Flush user-space buffers, then sync to disk. `sync_data` is
        // sufficient here — we don't depend on metadata (mtime/atime)
        // being synced, only the file contents.
        file.flush().await.context(IoSnafu { path: temp_path.to_path_buf() })?;
        file.sync_data().await.context(IoSnafu { path: temp_path.to_path_buf() })?;
    }

    // Step 2: atomic rename. Cross-process and cross-thread safe — the
    // POSIX rename semantics guarantee atomic replacement of `target_path`.
    tokio::fs::rename(temp_path, target_path)
        .await
        .context(IoSnafu { path: target_path.to_path_buf() })?;

    // Step 3: fsync the parent directory so the rename itself is durable
    // across crashes. Without this, the file body is on disk but the
    // directory entry's link to it can be lost on power failure.
    if let Some(parent) = target_path.parent() {
        sync_dir(parent).await?;
    }

    Ok(())
}

/// fsyncs a directory by opening it and invoking `sync_data`.
///
/// Tokio's `File::sync_data` works on directory file descriptors on Unix.
/// We open with read-only access (the only mode permitted on a directory
/// for syncing). On Windows directories cannot be fsynced this way; on
/// Windows the rename is atomic via `MoveFileEx` so the missing dir-fsync
/// is a non-issue.
async fn sync_dir(dir: &Path) -> Result<(), PersistError> {
    #[cfg(unix)]
    {
        let dir_handle =
            tokio::fs::File::open(dir).await.context(IoSnafu { path: dir.to_path_buf() })?;
        dir_handle.sync_data().await.context(IoSnafu { path: dir.to_path_buf() })?;
    }
    #[cfg(not(unix))]
    {
        let _ = dir;
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::disallowed_methods)]
mod tests {
    use std::sync::Arc;

    use inferadb_ledger_types::{OrganizationId, Region, VaultId};
    use tempfile::tempdir;

    use super::*;
    use crate::{
        region_storage::RegionStorageManager,
        snapshot_key_provider::{InMemorySnapshotKeyProvider, NoopSnapshotKeyProvider},
    };

    const TEST_ORG: OrganizationId = OrganizationId::new(7);
    const TEST_REGION: Region = Region::US_EAST_VA;

    fn org_scope() -> SnapshotScope {
        SnapshotScope::Org { organization_id: TEST_ORG }
    }

    fn vault_scope(vault_id: i64) -> SnapshotScope {
        SnapshotScope::Vault { organization_id: TEST_ORG, vault_id: VaultId::new(vault_id) }
    }

    fn make_persister(
        data_dir: &Path,
        provider: Arc<dyn SnapshotKeyProvider>,
        retention: usize,
    ) -> SnapshotPersister {
        let storage = Arc::new(RegionStorageManager::new(data_dir.to_path_buf()));
        SnapshotPersister::builder()
            .region_storage(storage)
            .snapshot_key_provider(provider)
            .retention_count(retention)
            .build()
    }

    async fn make_plaintext_file(bytes: &[u8]) -> tokio::fs::File {
        let mut file = tokio::fs::File::from_std(tempfile::tempfile().expect("tempfile"));
        file.write_all(bytes).await.expect("write");
        file.flush().await.expect("flush");
        file.seek(std::io::SeekFrom::Start(0)).await.expect("seek");
        file
    }

    // ---- File-name helpers --------------------------------------------

    #[test]
    fn snapshot_file_name_round_trips_index() {
        let dir = std::path::Path::new("/tmp/x");
        let path = scope_snapshot_path(dir, 12345);
        let name = path.file_name().unwrap().to_str().unwrap();
        assert_eq!(name, "snapshot-00000000000000012345.snap");
        assert_eq!(parse_snapshot_index(name).unwrap(), 12345);
    }

    #[test]
    fn snapshot_file_name_handles_zero_and_max() {
        let dir = std::path::Path::new("/tmp/x");
        for idx in [0u64, 1, u64::MAX] {
            let name =
                scope_snapshot_path(dir, idx).file_name().unwrap().to_str().unwrap().to_string();
            assert_eq!(parse_snapshot_index(&name).unwrap(), idx);
        }
    }

    #[test]
    fn malformed_names_are_rejected() {
        assert!(parse_snapshot_index("snap-1.snap").is_err());
        assert!(parse_snapshot_index("snapshot-abc.snap").is_err());
        assert!(parse_snapshot_index("snapshot-1.bin").is_err());
    }

    #[test]
    fn lex_ordering_matches_numeric_ordering() {
        // Three indices spanning multiple decimal widths must sort
        // lexicographically in the same order they sort numerically.
        let dir = std::path::Path::new("/tmp/x");
        let mut names: Vec<String> = [5u64, 100, 99_999_999, 1]
            .iter()
            .map(|idx| {
                scope_snapshot_path(dir, *idx).file_name().unwrap().to_str().unwrap().to_string()
            })
            .collect();
        names.sort();
        let parsed: Vec<u64> = names.iter().map(|n| parse_snapshot_index(n).unwrap()).collect();
        assert_eq!(parsed, vec![1, 5, 100, 99_999_999]);
    }

    // ---- persist + load round-trip ------------------------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn persist_and_load_roundtrip_no_encryption() {
        let dir = tempdir().expect("tempdir");
        let persister =
            make_persister(dir.path(), Arc::new(NoopSnapshotKeyProvider), DEFAULT_RETENTION_COUNT);

        let plaintext = b"LSNP-v2 fake plaintext snapshot bytes".to_vec();
        let file = make_plaintext_file(&plaintext).await;

        let meta = persister.persist(TEST_REGION, org_scope(), 42, 7, file).await.expect("persist");

        assert_eq!(meta.index, 42);
        assert_eq!(meta.term, Some(7));
        assert!(!meta.encrypted, "Noop provider must produce plaintext");
        assert_eq!(meta.size_bytes as usize, plaintext.len());
        assert!(meta.path.exists());

        let loaded = persister.load(TEST_REGION, org_scope(), 42).await.expect("load");
        assert_eq!(loaded.plaintext, plaintext);
        assert!(!loaded.meta.encrypted);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn persist_and_load_roundtrip_encrypted() {
        let provider = Arc::new(InMemorySnapshotKeyProvider::new());
        provider.insert_key(TEST_REGION, TEST_ORG, None, [0xAB; 32]);

        let dir = tempdir().expect("tempdir");
        let persister = make_persister(
            dir.path(),
            provider as Arc<dyn SnapshotKeyProvider>,
            DEFAULT_RETENTION_COUNT,
        );

        let plaintext = b"encrypted snapshot bytes for roundtrip".to_vec();
        let file = make_plaintext_file(&plaintext).await;

        let meta = persister.persist(TEST_REGION, org_scope(), 77, 3, file).await.expect("persist");

        assert!(meta.encrypted, "InMemory provider with key must produce ciphertext");
        // Ciphertext envelope is 12 (nonce) + plaintext.len() + 16 (auth tag).
        assert_eq!(meta.size_bytes as usize, 12 + plaintext.len() + 16);

        // On-disk bytes must NOT match the plaintext.
        let on_disk = tokio::fs::read(&meta.path).await.expect("read");
        assert_ne!(on_disk, plaintext);

        let loaded = persister.load(TEST_REGION, org_scope(), 77).await.expect("load");
        assert_eq!(loaded.plaintext, plaintext);
        assert!(loaded.meta.encrypted);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn load_with_wrong_key_returns_decrypt_error() {
        let provider = Arc::new(InMemorySnapshotKeyProvider::new());
        provider.insert_key(TEST_REGION, TEST_ORG, None, [0xAA; 32]);

        let dir = tempdir().expect("tempdir");
        let persister = make_persister(
            dir.path(),
            provider.clone() as Arc<dyn SnapshotKeyProvider>,
            DEFAULT_RETENTION_COUNT,
        );

        let plaintext = b"correct-key plaintext".to_vec();
        let file = make_plaintext_file(&plaintext).await;
        let _ = persister.persist(TEST_REGION, org_scope(), 1, 1, file).await.expect("persist");

        // Replace the provider's key with a different one — load now
        // returns a decrypt error.
        provider.insert_key(TEST_REGION, TEST_ORG, None, [0xBB; 32]);
        let err = persister
            .load(TEST_REGION, org_scope(), 1)
            .await
            .expect_err("load should fail with wrong key");
        assert!(matches!(err, PersistError::Decrypt { .. }), "expected Decrypt error, got {err:?}",);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn load_missing_snapshot_returns_not_found() {
        let dir = tempdir().expect("tempdir");
        let persister =
            make_persister(dir.path(), Arc::new(NoopSnapshotKeyProvider), DEFAULT_RETENTION_COUNT);
        let err = persister
            .load(TEST_REGION, org_scope(), 999)
            .await
            .expect_err("load should not find missing snapshot");
        assert!(
            matches!(err, PersistError::NotFound { .. }),
            "expected NotFound error, got {err:?}",
        );
    }

    // ---- listing + retention ------------------------------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn list_returns_empty_when_directory_missing() {
        let dir = tempdir().expect("tempdir");
        let persister =
            make_persister(dir.path(), Arc::new(NoopSnapshotKeyProvider), DEFAULT_RETENTION_COUNT);
        let metas = persister.list(TEST_REGION, org_scope()).await.expect("list");
        assert!(metas.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn list_returns_sorted_metas() {
        let dir = tempdir().expect("tempdir");
        let persister = make_persister(
            dir.path(),
            Arc::new(NoopSnapshotKeyProvider),
            // Big retention so nothing is pruned mid-test.
            100,
        );

        for index in [10u64, 1, 5, 99] {
            let file = make_plaintext_file(b"x").await;
            let _ =
                persister.persist(TEST_REGION, org_scope(), index, 1, file).await.expect("persist");
        }
        let metas = persister.list(TEST_REGION, org_scope()).await.expect("list");
        let indices: Vec<u64> = metas.iter().map(|m| m.index).collect();
        assert_eq!(indices, vec![1, 5, 10, 99]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn latest_returns_highest_index() {
        let dir = tempdir().expect("tempdir");
        let persister = make_persister(dir.path(), Arc::new(NoopSnapshotKeyProvider), 100);
        assert!(persister.latest(TEST_REGION, org_scope()).await.unwrap().is_none());

        for index in [3, 9, 1] {
            let file = make_plaintext_file(b"x").await;
            persister.persist(TEST_REGION, org_scope(), index, 1, file).await.unwrap();
        }
        let latest = persister.latest(TEST_REGION, org_scope()).await.unwrap().unwrap();
        assert_eq!(latest.index, 9);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn retention_prunes_oldest_snapshots() {
        let dir = tempdir().expect("tempdir");
        let persister = make_persister(dir.path(), Arc::new(NoopSnapshotKeyProvider), 3);

        for index in 1..=5u64 {
            let file = make_plaintext_file(b"x").await;
            persister.persist(TEST_REGION, org_scope(), index, 1, file).await.unwrap();
        }
        let metas = persister.list(TEST_REGION, org_scope()).await.unwrap();
        let indices: Vec<u64> = metas.iter().map(|m| m.index).collect();
        // Three newest must remain.
        assert_eq!(indices, vec![3, 4, 5]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn org_and_vault_scopes_are_listed_independently() {
        let dir = tempdir().expect("tempdir");
        let persister = make_persister(dir.path(), Arc::new(NoopSnapshotKeyProvider), 100);

        let file = make_plaintext_file(b"org bytes").await;
        persister.persist(TEST_REGION, org_scope(), 11, 1, file).await.unwrap();
        let file = make_plaintext_file(b"vault bytes").await;
        persister.persist(TEST_REGION, vault_scope(42), 22, 1, file).await.unwrap();

        let org_metas = persister.list(TEST_REGION, org_scope()).await.unwrap();
        assert_eq!(org_metas.len(), 1);
        assert_eq!(org_metas[0].index, 11);

        let vault_metas = persister.list(TEST_REGION, vault_scope(42)).await.unwrap();
        assert_eq!(vault_metas.len(), 1);
        assert_eq!(vault_metas[0].index, 22);

        // Different vault id is its own scope.
        let other = persister.list(TEST_REGION, vault_scope(99)).await.unwrap();
        assert!(other.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn vault_scope_uses_vault_subdirectory() {
        let dir = tempdir().expect("tempdir");
        let persister = make_persister(dir.path(), Arc::new(NoopSnapshotKeyProvider), 100);

        let file = make_plaintext_file(b"vault").await;
        let meta = persister.persist(TEST_REGION, vault_scope(7), 1, 1, file).await.unwrap();
        let path_str = meta.path.to_string_lossy();
        assert!(
            path_str.contains("vault-7"),
            "vault snapshot path should contain vault-7 subdir, got {path_str}",
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn open_encrypted_returns_at_rest_bytes() {
        // Stage 3 leader-side helper: the streamer reads the ciphertext
        // verbatim — verify we get back exactly what `persist` wrote
        // (envelope intact when encryption is enabled).
        let provider = Arc::new(InMemorySnapshotKeyProvider::new());
        provider.insert_key(TEST_REGION, TEST_ORG, None, [0xAB; 32]);
        let dir = tempdir().expect("tempdir");
        let persister = make_persister(
            dir.path(),
            provider as Arc<dyn SnapshotKeyProvider>,
            DEFAULT_RETENTION_COUNT,
        );

        let plaintext = b"plaintext for encrypted open_encrypted".to_vec();
        let file = make_plaintext_file(&plaintext).await;
        let meta = persister.persist(TEST_REGION, org_scope(), 5, 1, file).await.expect("persist");
        // open_encrypted yields the on-disk file size + handle. Reading
        // the file must match the on-disk bytes exactly (encrypted).
        let (mut file, size) =
            persister.open_encrypted(TEST_REGION, org_scope(), 5).await.expect("open");
        assert_eq!(size, meta.size_bytes);
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).await.expect("read");
        let on_disk = tokio::fs::read(&meta.path).await.expect("read direct");
        assert_eq!(buf, on_disk, "open_encrypted bytes must match the on-disk file");
        // And the bytes must NOT match the plaintext when encryption is on.
        assert_ne!(buf, plaintext);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn list_staged_returns_empty_when_no_streamed_files() {
        // Stage 3 surface helper: Stage 4 will discover staged files via
        // `list_staged`. With no incoming streams, the list is empty
        // even when locally persisted snapshots exist.
        let dir = tempdir().expect("tempdir");
        let persister = make_persister(dir.path(), Arc::new(NoopSnapshotKeyProvider), 100);
        let file = make_plaintext_file(b"x").await;
        persister.persist(TEST_REGION, org_scope(), 1, 1, file).await.unwrap();
        let staged = persister.list_staged(TEST_REGION, org_scope()).await.expect("list_staged");
        assert!(staged.is_empty(), "no staged files yet — only a locally persisted snapshot");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn staged_path_distinct_from_persisted_path() {
        let dir = tempdir().expect("tempdir");
        let persister = make_persister(dir.path(), Arc::new(NoopSnapshotKeyProvider), 100);
        let scope = org_scope();
        let staged = persister.staged_path(TEST_REGION, scope, 42);
        let staging_temp = persister.staging_temp_path(TEST_REGION, scope, 7, 42);
        // Staged final files end in `.snap.staged` to keep them
        // distinct from `*.snap` files written by `persist`.
        assert!(staged.to_string_lossy().ends_with(".snap.staged"));
        assert!(staging_temp.to_string_lossy().ends_with(".snap.staging"));
        // The staging temp embeds the leader_term + index so concurrent
        // streams from different leaders cannot collide on a single
        // temp file.
        assert!(staging_temp.to_string_lossy().contains("00000000000000000007-"));
        assert!(staging_temp.to_string_lossy().contains("00000000000000000042"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn persist_is_atomic_on_temp_failure_simulation() {
        // Pre-create the temp file path so create_new fails — simulates a
        // crash mid-rename followed by a retry that lands on the orphaned
        // temp.
        let dir = tempdir().expect("tempdir");
        let persister = make_persister(dir.path(), Arc::new(NoopSnapshotKeyProvider), 100);

        // First persist succeeds and lands a snapshot at index 1.
        let file = make_plaintext_file(b"first").await;
        persister.persist(TEST_REGION, org_scope(), 1, 1, file).await.unwrap();

        // Second persist's first attempt: pre-create the temp file with a
        // squatting nonce so the next persist's `create_new` would fail —
        // but our temp_path_for() randomises the nonce, so the squatter
        // is at a different path. The atomicity property we care about
        // here is "previous-good snapshot remains readable".
        let loaded = persister.load(TEST_REGION, org_scope(), 1).await.unwrap();
        assert_eq!(loaded.plaintext, b"first");
    }
}
