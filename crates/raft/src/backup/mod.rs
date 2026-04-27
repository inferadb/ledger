//! Backup and restore operations for InferaDB Ledger.
//!
//! Provides the multi-DB tar+zstd archive backup pipeline:
//!
//! - On-demand archive creation via `CreateBackup` RPC ([`BackupManager::create_archive`])
//! - Archive enumeration via `ListBackups` RPC (reads per-archive manifests directly)
//! - Online restore staging via `RestoreBackup` RPC ([`BackupManager::stage_restore`])
//! - Offline restore swap via the `inferadb-ledger restore apply` CLI ([`apply_staged_restore`])
//! - Automated periodic archives via [`BackupJob`]
//! - Background restore-trash sweeping via [`RestoreTrashSweepJob`]
//!
//! ## Submodules
//!
//! - [`archive`] — multi-DB tar+zstd backup archive format primitives (manifest, streaming build,
//!   streaming parse, checksum verification). The archive format captures full per-organization
//!   physical state across every on-disk database (control-plane and per-vault).

pub mod archive;

use std::{
    fs, io,
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use chrono::Utc;
use inferadb_ledger_types::{
    LedgerNodeId, OrganizationId, OrganizationSlug, Region, VaultId, config::BackupConfig,
};
use snafu::{ResultExt, Snafu};
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::{
    backup::archive::{BACKUP_FORMAT, BACKUP_SCHEMA_VERSION, BackupManifest, DbEntry},
    consensus_handle::ConsensusHandle,
};

/// Backup operation failure.
#[derive(Debug, Snafu)]
pub enum BackupError {
    /// IO error during backup operations.
    #[snafu(display("Backup IO error: {source}"))]
    Io {
        /// The underlying IO error.
        source: std::io::Error,
    },

    /// Serialization error.
    #[snafu(display("Serialization error: {message}"))]
    Serialization {
        /// Description of the serialization failure.
        message: String,
    },

    /// Archive-format failure surfaced from the [`archive`] module while
    /// building or parsing a multi-DB backup archive.
    #[snafu(display("Backup archive failure: {source}"))]
    Archive {
        /// The underlying archive error.
        source: archive::BackupArchiveError,
    },

    /// `BackupManager::create_archive` was called on a manager built
    /// without a `data_dir`. The archive path requires the per-org
    /// on-disk layout to enumerate DB files.
    #[snafu(display(
        "Backup archive requires a data_dir; build BackupManager via with_data_dir(...)"
    ))]
    DataDirMissing,

    /// The archive's manifest `rmk_fingerprint` does not match the
    /// fingerprint the caller asserted. Surfaces during
    /// [`BackupManager::stage_restore`] when the restoring node's RMK
    /// material would not be able to unwrap this archive's per-vault
    /// DEKs. The check is the only crypto-level pre-flight that runs
    /// before any envelope unwrap is attempted, so a mismatch here is
    /// the operator's earliest visible signal that the archive belongs
    /// to a different RMK lineage.
    #[snafu(display(
        "Backup archive RMK fingerprint mismatch: archive declares {actual}, expected {expected}"
    ))]
    RmkFingerprintMismatch {
        /// Fingerprint the caller expected (typically the restoring
        /// node's local RMK fingerprint).
        expected: String,
        /// Fingerprint carried in the archive manifest.
        actual: String,
    },

    /// A staging directory already exists at the path
    /// [`BackupManager::stage_restore`] tried to create. The staging
    /// path embeds an `org_id-timestamp_micros` suffix so this is
    /// effectively a clock-skew or test-fixture collision, not a
    /// concurrent-restore race.
    #[snafu(display("Restore staging directory already exists: {}", path.display()))]
    StagingDirExists {
        /// The conflicting staging directory path.
        path: PathBuf,
    },

    /// The live data directory the offline `apply_staged_restore`
    /// targets is in use by a running node (its `.lock` file is held).
    /// The CLI refuses to swap a live tree on top of a running node;
    /// the operator must stop the node first.
    #[snafu(display(
        "Cannot apply staged restore: data directory {} appears to be in use by a \
         running node (lock file is held). Stop the node first.",
        path.display()
    ))]
    LiveDirInUse {
        /// The data directory whose `.lock` file is held.
        path: PathBuf,
    },
}

/// Result type for backup operations.
pub type Result<T> = std::result::Result<T, BackupError>;

/// Outcome of a successful [`BackupManager::stage_restore`] call.
///
/// The caller (typically the `RestoreBackup` admin RPC handler in
/// Slice 4) takes the staging directory path and surfaces it to the
/// operator alongside `requires_restart=true`. The follow-up CLI step
/// (`inferadb-ledger restore apply`) consumes [`Self::staging_dir`] via
/// [`apply_staged_restore`].
#[derive(Debug, Clone)]
pub struct RestoreStagingResult {
    /// Manifest read from the archive. Carries the full DB list, the
    /// RMK fingerprint, organization identifiers, and timestamps —
    /// callers expose a subset back to the operator for confirmation.
    pub manifest: BackupManifest,
    /// Absolute path of the staging directory that mirrors the live
    /// per-org tree. Stable for the lifetime of the staging window;
    /// consumed by [`apply_staged_restore`].
    pub staging_dir: PathBuf,
    /// Number of DB members copied out of the archive. Equals
    /// `manifest.dbs.len()` on success; surfaced separately so logs
    /// don't have to introspect the manifest.
    pub files_staged: usize,
    /// Total bytes written across all staged DB files. Sum of every
    /// `manifest.dbs[i].size_bytes`.
    pub bytes_staged: u64,
}

/// Outcome of a successful [`apply_staged_restore`] call.
///
/// Surfaced to the CLI for human-facing diagnostics — the trash path
/// is what the operator inspects to recover from a mistaken restore,
/// and `files_swapped` is what they verify against the manifest before
/// restarting the node.
#[derive(Debug, Clone)]
pub struct RestoreApplyResult {
    /// Trash directory holding the displaced live tree. Empty `PathBuf`
    /// if there was no pre-existing live tree to displace (fresh
    /// install on a brand-new region).
    pub trash_dir: PathBuf,
    /// Number of DB files moved from staging into the live tree. Should
    /// equal the manifest's `dbs.len()` from the staging step.
    pub files_swapped: usize,
}

/// Manages backup operations for a region.
pub struct BackupManager {
    /// Backup destination directory.
    backup_dir: PathBuf,
    /// Root data directory (parent of `{region}/{org_id}/...`). Required
    /// by the archive-based backup path ([`Self::create_archive`]).
    /// Optional so [`Self::new`] returns a manager that fails fast on the
    /// archive path until [`Self::with_data_dir`] is called by the
    /// service-layer wiring.
    data_dir: Option<PathBuf>,
    /// Node identity persisted into archive manifests for diagnostics.
    /// Populated together with [`Self::data_dir`] via [`Self::with_data_dir`].
    node_id: Option<LedgerNodeId>,
}

impl BackupManager {
    /// Creates a new backup manager.
    ///
    /// # Errors
    ///
    /// Returns [`BackupError::Io`] if the backup destination directory
    /// cannot be created.
    pub fn new(config: &BackupConfig) -> Result<Self> {
        let backup_dir = PathBuf::from(&config.destination);
        fs::create_dir_all(&backup_dir).context(IoSnafu)?;

        Ok(Self { backup_dir, data_dir: None, node_id: None })
    }

    /// Attaches the on-disk data directory + node identity used by the
    /// archive-based backup path ([`Self::create_archive`]).
    ///
    /// The service layer wires this immediately after [`Self::new`]; an
    /// archive call without a `data_dir` returns
    /// [`BackupError::DataDirMissing`].
    #[must_use]
    pub fn with_data_dir(mut self, data_dir: PathBuf, node_id: LedgerNodeId) -> Self {
        self.data_dir = Some(data_dir);
        self.node_id = Some(node_id);
        self
    }

    /// Returns the configured root data directory, if attached.
    pub fn data_dir(&self) -> Option<&Path> {
        self.data_dir.as_deref()
    }

    /// Returns the backup directory path.
    pub fn backup_dir(&self) -> &Path {
        &self.backup_dir
    }

    /// Creates a multi-DB backup archive for a single organization.
    ///
    /// Enumerates every per-org control-plane database (`_meta.db`,
    /// `blocks.db`, `raft.db`, and `events.db` if present) plus every
    /// per-vault database (`state.db`, `raft.db`, `blocks.db`, and
    /// `events.db` if present) under
    /// `{data_dir}/{region}/{organization_id}/`, builds a manifest, and
    /// writes a `tar` + `zstd` archive to `output_path` via
    /// [`archive::build_archive`].
    ///
    /// The archive is the target replacement for the legacy snapshot-based
    /// backup format introduced by Slice 1 of the multi-DB backup rewrite.
    /// It captures full physical state across every on-disk database for
    /// the organization; restore is implemented in Slice 3 and the
    /// `BackupJob` cutover lands in Slice 5.
    ///
    /// # Slug + RMK fingerprint
    ///
    /// `organization_slug` and `rmk_fingerprint` are stamped into the
    /// manifest before the archive's manifest member is written, so
    /// they are durable in the on-disk archive (not patched in memory
    /// after the fact). Callers below the service layer pass an
    /// [`OrganizationSlug::new(0)`] sentinel and an empty fingerprint
    /// only in tests; production callers always thread the real values
    /// through from the slug resolver and the per-region
    /// [`RegionKeyManager`](inferadb_ledger_store::crypto::RegionKeyManager).
    ///
    /// # Errors
    ///
    /// - [`BackupError::DataDirMissing`] if the manager was built via [`Self::new`] without
    ///   subsequently attaching a data_dir via [`Self::with_data_dir`].
    /// - [`BackupError::Io`] if any per-org or per-vault database file cannot be opened or its
    ///   metadata read, or if the output file cannot be created.
    /// - [`BackupError::Archive`] for any failure surfaced by [`archive::build_archive`] (manifest
    ///   serialization, zstd codec error, tar header construction, missing manifest member).
    pub async fn create_archive(
        &self,
        region: Region,
        organization_id: OrganizationId,
        organization_slug: OrganizationSlug,
        rmk_fingerprint: String,
        output_path: &Path,
    ) -> Result<BackupManifest> {
        let data_dir = self.data_dir.as_ref().ok_or(BackupError::DataDirMissing)?;
        let node_id = self.node_id.unwrap_or(0);

        let org_dir = organization_dir(data_dir, region, organization_id);
        let (files, vault_count) = enumerate_archive_files(&org_dir)?;

        let mut manifest = BackupManifest {
            schema_version: BACKUP_SCHEMA_VERSION,
            format: BACKUP_FORMAT.to_string(),
            region,
            organization_id,
            organization_slug,
            timestamp_micros: Utc::now().timestamp_micros(),
            rmk_fingerprint,
            node_id_at_creation: node_id,
            // Pre-populated with empty entries; `build_archive` fills in
            // `size_bytes` + `checksum` for each by streaming the source
            // file once.
            dbs: files.iter().map(|(rel_path, _)| empty_db_entry(rel_path.clone())).collect(),
            vault_count,
            created_by_app_version: env!("CARGO_PKG_VERSION").to_string(),
        };

        let output_file = fs::File::create(output_path).context(IoSnafu)?;
        archive::build_archive(output_file, &mut manifest, &files).context(ArchiveSnafu)?;

        info!(
            region = region.as_str(),
            organization_id = organization_id.value(),
            organization_slug = organization_slug.value(),
            vault_count,
            db_count = manifest.dbs.len(),
            output = %output_path.display(),
            "Multi-DB backup archive created"
        );

        Ok(manifest)
    }

    /// Stages an archive into `{data_dir}/.restore-staging/{org_id}-{ts}/`.
    ///
    /// This is the **online** half of the two-phase restore (Slice 3 of
    /// the multi-DB backup rewrite). The node is still running; the
    /// staging step never touches the live per-org tree. On success the
    /// caller is expected to:
    ///
    /// 1. Surface the manifest (region, organization id, timestamp, vault count) back to the
    ///    operator.
    /// 2. Tell the operator to stop the node and run `inferadb-ledger restore apply` (which calls
    ///    [`apply_staged_restore`]).
    ///
    /// Behaviour:
    ///
    /// - Parses the archive via [`archive::parse_archive`], which validates the manifest schema +
    ///   format sentinel before invoking the per-member callback.
    /// - For each DB member, streams bytes through a blake3 hasher while writing them to
    ///   `{staging}/{rel_path}`. On entry exhaustion the computed checksum is verified against the
    ///   manifest entry; mismatches surface as [`archive::BackupArchiveError::ChecksumMismatch`]
    ///   (wrapped in [`BackupError::Archive`]).
    /// - Verifies `manifest.rmk_fingerprint` against `expected_rmk_fingerprint`. `Some(expected)`
    ///   enforces equality and surfaces [`BackupError::RmkFingerprintMismatch`] on disagreement;
    ///   `None` skips the check and emits a `WARN` log line for operator visibility (Slice 4
    ///   placeholder support — the per-region key manager isn't wired yet).
    /// - On any error after the staging directory was created, the staging directory is removed
    ///   best-effort so the operator does not have to clean up half-staged trees by hand.
    ///
    /// # Errors
    ///
    /// - [`BackupError::DataDirMissing`] if the manager was built via [`Self::new`] without
    ///   subsequently attaching a data_dir via [`Self::with_data_dir`].
    /// - [`BackupError::Io`] for any filesystem failure (open archive, create staging dir, write
    ///   member).
    /// - [`BackupError::Archive`] for any format-level failure surfaced by
    ///   [`archive::parse_archive`] (bad manifest, schema/format sentinel mismatch, member checksum
    ///   mismatch).
    /// - [`BackupError::RmkFingerprintMismatch`] if the caller provided an expected fingerprint and
    ///   the manifest disagrees.
    /// - [`BackupError::StagingDirExists`] if the computed staging directory already exists (clock
    ///   collision or concurrent in-flight restore for the same org).
    pub async fn stage_restore(
        &self,
        archive_path: &Path,
        expected_rmk_fingerprint: Option<&str>,
    ) -> Result<RestoreStagingResult> {
        let data_dir = self.data_dir.as_ref().ok_or(BackupError::DataDirMissing)?;

        // Open the archive up front so we surface I/O errors before
        // creating the staging directory. Cleaner failure mode for the
        // operator than a half-created staging tree.
        let archive_file = fs::File::open(archive_path).context(IoSnafu)?;

        // Read the manifest non-destructively to compute the staging
        // directory path. `parse_archive` will read it again as it
        // streams the archive — that's two reads of a tiny JSON payload
        // and it's still cheap.
        let preflight_file = fs::File::open(archive_path).context(IoSnafu)?;
        let preflight_manifest = archive::read_manifest(preflight_file).context(ArchiveSnafu)?;

        // Stage at `{data_dir}/.restore-staging/{org_id}-{timestamp}/`.
        // The timestamp suffix prevents accidental overlap when an
        // operator stages two archives for the same org back-to-back
        // (e.g. for diff'ing across snapshots before swapping).
        let staging_root = data_dir.join(".restore-staging");
        let staging_suffix = format!(
            "{}-{}",
            preflight_manifest.organization_id.value(),
            preflight_manifest.timestamp_micros
        );
        let staging_dir = staging_root.join(&staging_suffix);

        if staging_dir.exists() {
            return Err(BackupError::StagingDirExists { path: staging_dir });
        }

        fs::create_dir_all(&staging_dir).context(IoSnafu)?;

        // Stream every non-manifest member through the parser callback,
        // hashing + writing concurrently into the staging tree. Errors
        // bubble out of the closure as `BackupArchiveError` so the
        // outer `parse_archive` call surfaces a single typed failure.
        let mut files_staged: usize = 0;
        let mut bytes_staged: u64 = 0;
        let staging_dir_for_callback = staging_dir.clone();

        let parse_result = archive::parse_archive(
            archive_file,
            |path, reader| -> std::result::Result<(), archive::BackupArchiveError> {
                // Reject any path that escapes the staging tree. tar
                // members in archives we built always have plain forward
                // slashes (no `..`, no leading `/`); this guard catches
                // tampered or hand-rolled archives.
                if path.is_empty()
                    || path.starts_with('/')
                    || path.split('/').any(|seg| seg == ".." || seg == ".")
                {
                    return Err(archive::BackupArchiveError::Io {
                        source: io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("rejected unsafe member path in archive: {path}"),
                        ),
                    });
                }

                let dest = staging_dir_for_callback.join(path);
                if let Some(parent) = dest.parent() {
                    fs::create_dir_all(parent)
                        .map_err(|source| archive::BackupArchiveError::Io { source })?;
                }

                // Stream member bytes through both a blake3 hasher and
                // the destination file in a single pass. We can't use
                // `compute_blake3_streaming` directly because we also
                // need to write the bytes to disk.
                let mut dest_file = fs::File::create(&dest)
                    .map_err(|source| archive::BackupArchiveError::Io { source })?;
                let mut hasher = blake3::Hasher::new();
                let mut buf = vec![0u8; 64 * 1024];
                let mut total: u64 = 0;
                loop {
                    let n = reader
                        .read(&mut buf)
                        .map_err(|source| archive::BackupArchiveError::Io { source })?;
                    if n == 0 {
                        break;
                    }
                    hasher.update(&buf[..n]);
                    dest_file
                        .write_all(&buf[..n])
                        .map_err(|source| archive::BackupArchiveError::Io { source })?;
                    total += n as u64;
                }
                dest_file
                    .sync_all()
                    .map_err(|source| archive::BackupArchiveError::Io { source })?;

                let computed =
                    format!("{}{}", archive::CHECKSUM_PREFIX_BLAKE3, hasher.finalize().to_hex());

                // Find the manifest entry for this path and check its
                // checksum. We have to re-resolve from `preflight_manifest`
                // because `parse_archive` parses its own manifest copy
                // and gives us back only the per-member iterator.
                let entry =
                    preflight_manifest.dbs.iter().find(|e| e.path == path).ok_or_else(|| {
                        archive::BackupArchiveError::UnexpectedMember { path: path.to_string() }
                    })?;

                if entry.checksum != computed {
                    return Err(archive::BackupArchiveError::ChecksumMismatch {
                        path: path.to_string(),
                        expected: entry.checksum.clone(),
                        actual: computed,
                    });
                }

                if entry.size_bytes != total {
                    return Err(archive::BackupArchiveError::Io {
                        source: io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!(
                                "member {path} size mismatch: manifest says {} bytes, \
                                 archive yielded {} bytes",
                                entry.size_bytes, total
                            ),
                        ),
                    });
                }

                files_staged += 1;
                bytes_staged += total;
                Ok(())
            },
        );

        let manifest = match parse_result {
            Ok(manifest) => manifest,
            Err(err) => {
                // Best-effort cleanup so the operator doesn't have to
                // remove half-staged trees by hand.
                if let Err(cleanup_err) = fs::remove_dir_all(&staging_dir) {
                    warn!(
                        path = %staging_dir.display(),
                        error = %cleanup_err,
                        "Failed to clean up staging directory after stage_restore failure"
                    );
                }
                return Err(BackupError::Archive { source: err });
            },
        };

        // RMK fingerprint pre-flight. `None` is the placeholder support
        // window (Slice 4 wires the per-region key manager); when set,
        // a mismatch is a hard fail and we tear down the staging tree.
        match expected_rmk_fingerprint {
            Some(expected) if manifest.rmk_fingerprint != expected => {
                let actual = manifest.rmk_fingerprint.clone();
                if let Err(cleanup_err) = fs::remove_dir_all(&staging_dir) {
                    warn!(
                        path = %staging_dir.display(),
                        error = %cleanup_err,
                        "Failed to clean up staging directory after RMK fingerprint mismatch"
                    );
                }
                return Err(BackupError::RmkFingerprintMismatch {
                    expected: expected.to_string(),
                    actual,
                });
            },
            None => {
                warn!(
                    archive = %archive_path.display(),
                    rmk_fingerprint = %manifest.rmk_fingerprint,
                    "stage_restore called without expected RMK fingerprint; skipping pre-flight \
                     check (Slice 4 wires the per-region key manager)"
                );
            },
            Some(_) => {
                // Match: nothing to do.
            },
        }

        info!(
            archive = %archive_path.display(),
            staging = %staging_dir.display(),
            organization_id = manifest.organization_id.value(),
            region = manifest.region.as_str(),
            files_staged,
            bytes_staged,
            "Restore staged"
        );

        Ok(RestoreStagingResult { manifest, staging_dir, files_staged, bytes_staged })
    }
}

/// Builds the on-disk path of a single organization's data directory:
/// `{data_dir}/{region}/{organization_id}/`. Mirrors
/// `RegionStorageManager::organization_dir` but operates on a raw
/// `data_dir` so the archive path can run without a fully wired
/// `RegionStorageManager` instance — the archive consumer needs only the
/// path layout, not the runtime DB handles the manager owns.
fn organization_dir(data_dir: &Path, region: Region, organization_id: OrganizationId) -> PathBuf {
    let region_dir = if region == Region::GLOBAL {
        data_dir.join("global")
    } else {
        data_dir.join(region.as_str())
    };
    region_dir.join(organization_id.value().to_string())
}

/// Returns an empty placeholder [`DbEntry`] for a given relative path.
/// `archive::build_archive` overwrites `size_bytes` + `checksum` for
/// every entry it sees in the file list, so the manifest is fully
/// populated by the time the archive is written to disk.
fn empty_db_entry(path: String) -> DbEntry {
    DbEntry { path, size_bytes: 0, checksum: String::new() }
}

/// Discovers every `vault-{id}` subdirectory under `{org_dir}/state/`
/// in ascending [`VaultId`] order. Mirrors the vault layout established
/// by P2b.0 of the per-vault consensus plan: each vault owns a directory
/// at `{state_dir}/vault-{id}/` holding `state.db` plus the per-vault
/// `raft.db` / `blocks.db` / `events.db` introduced by Phase 4 of that
/// plan.
///
/// Entries that fail the layout check (non-directory, non-numeric id,
/// missing `vault-` prefix) are skipped silently — the apply pipeline
/// never writes such entries, so they're either operator artefacts or
/// the pre-P2b.0 `vault-{id}.db` legacy form (which is rejected at
/// `RegionStorageManager::open_organization` time, not here).
///
/// Returns `Ok(vec![])` if `{org_dir}/state/` does not exist — an
/// organization with no vaults is a well-formed archive input.
fn discover_vault_ids(org_dir: &Path) -> Result<Vec<VaultId>> {
    let state_dir = org_dir.join("state");
    let entries = match fs::read_dir(&state_dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(BackupError::Io { source: e }),
    };

    let mut discovered = Vec::new();
    for entry in entries {
        let entry = entry.context(IoSnafu)?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let dir_name = match entry.file_name().into_string() {
            Ok(name) => name,
            Err(_) => continue,
        };
        let Some(suffix) = dir_name.strip_prefix("vault-") else {
            continue;
        };
        let Ok(id) = suffix.parse::<i64>() else {
            continue;
        };
        discovered.push(VaultId::new(id));
    }
    discovered.sort_by_key(|v| v.value());
    Ok(discovered)
}

/// Enumerates every database file the archive must capture for a single
/// organization, in declaration order matching the manifest. Returns the
/// `(rel_path, source_path)` list expected by [`archive::build_archive`]
/// alongside the per-org vault count.
///
/// Order:
///
/// 1. Org-level control-plane DBs (in deterministic order): `_meta.db`, `blocks.db`, `raft.db`,
///    then `events.db` if present.
/// 2. Per-vault DBs grouped by ascending [`VaultId`]; within each vault, the same fixed order:
///    `state.db`, `raft.db`, `blocks.db`, then `events.db` if present.
///
/// Optional files (`events.db` at both scopes) are skipped silently when
/// missing — an org with no apply-phase audit emissions has no events
/// database. Missing required files are surfaced by the archive writer
/// at copy time as an `Io` error (not by this enumerator) — keeping this
/// function pure-discovery makes the partial-state error message clearer
/// (operator sees "failed to open `_meta.db`", not "missing manifest
/// entry").
fn enumerate_archive_files(org_dir: &Path) -> Result<(Vec<(String, PathBuf)>, u32)> {
    const ORG_REQUIRED: &[&str] = &["_meta.db", "blocks.db", "raft.db"];
    const ORG_OPTIONAL: &[&str] = &["events.db"];
    // Only `state.db` is unconditionally present per vault — every
    // vault has an entity store. The Raft log + Merkle chain + audit
    // stream are present per vault only when the vault has its own
    // per-vault Raft group running (Phase 2 of per-vault consensus).
    // The system vault (`vault-0`) and freshly-created vaults that
    // have not yet had their Raft group started by `start_vault_group`
    // do not have these files on disk; the enumerator must skip them
    // rather than fail with `Io::NotFound` at archive build time.
    const VAULT_REQUIRED: &[&str] = &["state.db"];
    const VAULT_OPTIONAL: &[&str] = &["raft.db", "blocks.db", "events.db"];

    let mut files: Vec<(String, PathBuf)> = Vec::new();

    // Org-level files: always include the required set; opportunistically
    // include the optional set so a fresh org without audit traffic still
    // produces a valid archive.
    for name in ORG_REQUIRED {
        files.push(((*name).to_string(), org_dir.join(name)));
    }
    for name in ORG_OPTIONAL {
        let path = org_dir.join(name);
        if path.exists() {
            files.push(((*name).to_string(), path));
        }
    }

    // Per-vault files. `state/` may not exist on a freshly-created org
    // that has not yet produced its first vault; that is well-formed.
    let vault_ids = discover_vault_ids(org_dir)?;
    let vault_count: u32 = u32::try_from(vault_ids.len()).unwrap_or(u32::MAX);

    for vault_id in &vault_ids {
        let vault_dir = org_dir.join("state").join(format!("vault-{}", vault_id.value()));
        for name in VAULT_REQUIRED {
            let rel = format!("state/vault-{}/{name}", vault_id.value());
            files.push((rel, vault_dir.join(name)));
        }
        for name in VAULT_OPTIONAL {
            let path = vault_dir.join(name);
            if path.exists() {
                let rel = format!("state/vault-{}/{name}", vault_id.value());
                files.push((rel, path));
            }
        }
    }

    Ok((files, vault_count))
}

/// Resolves the live per-org tree under `data_dir` and swaps in the
/// staged tree produced by [`BackupManager::stage_restore`].
///
/// This is the **offline** half of the two-phase restore (Slice 3 of
/// the multi-DB backup rewrite). The function is `pub fn` (synchronous)
/// because it runs from the `inferadb-ledger restore apply` CLI when
/// the node is stopped — there is no async runtime to attach to.
///
/// Steps:
///
/// 1. Resolves `live_dir = {data_dir}/{region}/{org_id}/`.
/// 2. If `live_dir` exists with at least one entry, renames the whole tree to
///    `{data_dir}/.restore-trash/{ts_micros}/{org_id}/`. POSIX `rename` is atomic per-file within
///    the same filesystem; concurrent readers see either the old tree or are gone-empty for the
///    instant of the swap. The trash directory carries a 24h retention sweep
///    ([`sweep_restore_trash`]) so operators can recover from a mistaken restore.
/// 3. Renames the staging tree to `live_dir`. Same atomicity caveat: same FS = atomic; cross-FS
///    falls back to a recursive copy + remove (not atomic — documented as a deployment limitation).
/// 4. Writes a `{data_dir}/.restore-marker` JSON file recording which org was restored at what
///    timestamp. The marker is informational; the operator inspects it to confirm the swap before
///    restarting the node.
///
/// Atomicity:
///
/// - Same filesystem: each `rename` is atomic; power-loss mid-step leaves either the old tree or
///   the new tree intact, never a half-merge.
/// - Cross-filesystem: the fallback path performs `fs::copy` + `fs::remove_file` per file. Power
///   loss can leave the destination half-populated; the operator must inspect the trash + staging
///   directories and re-run the swap. This is documented in the spec.
/// - Lock-file pre-flight: if `{data_dir}/.lock` is held by another process, the function refuses
///   the swap and returns [`BackupError::LiveDirInUse`]. Stops the operator from racing a running
///   node.
///
/// Windows is intentionally deferred (Q4 in the spec).
///
/// # Errors
///
/// - [`BackupError::Io`] for any filesystem error.
/// - [`BackupError::LiveDirInUse`] if the data directory's `.lock` file is held by another process
///   (running node).
pub fn apply_staged_restore(
    data_dir: &Path,
    staging_dir: &Path,
    region: Region,
    organization_id: OrganizationId,
) -> Result<RestoreApplyResult> {
    if !staging_dir.is_dir() {
        return Err(BackupError::Io {
            source: io::Error::new(
                io::ErrorKind::NotFound,
                format!("staging directory does not exist: {}", staging_dir.display()),
            ),
        });
    }

    // Refuse to swap on top of a running node. The lock file is
    // created by `DataDirLock::acquire` at startup and held for the
    // node's lifetime via `flock`. We detect a held lock by attempting
    // a non-blocking `flock` ourselves; if it would block, the node is
    // running.
    enforce_node_stopped(data_dir)?;

    let live_dir = organization_dir(data_dir, region, organization_id);

    // Trash the existing live tree if it has any contents. An empty
    // directory (or no directory at all) means this is the first restore
    // for this org on this node — nothing to displace.
    let trash_dir = if live_dir_has_contents(&live_dir)? {
        let timestamp_micros = Utc::now().timestamp_micros();
        let trash_root = data_dir.join(".restore-trash").join(timestamp_micros.to_string());
        let trash_org = trash_root.join(organization_id.value().to_string());
        if let Some(parent) = trash_org.parent() {
            fs::create_dir_all(parent).context(IoSnafu)?;
        }

        // POSIX rename is atomic within the same filesystem. Cross-fs
        // falls back to recursive move below.
        match fs::rename(&live_dir, &trash_org) {
            Ok(()) => {},
            Err(e) if is_cross_device(&e) => {
                move_recursive(&live_dir, &trash_org)?;
            },
            Err(e) => return Err(BackupError::Io { source: e }),
        }

        debug!(
            live = %live_dir.display(),
            trash = %trash_org.display(),
            "Moved displaced live tree to restore trash"
        );
        trash_org
    } else {
        // No pre-existing live tree to displace.
        PathBuf::new()
    };

    // Make sure the parent of `live_dir` exists before renaming the
    // staging tree into place — fresh-region restore on a node that
    // has never hosted this org yet leaves
    // `{data_dir}/{region}/` without `{org_id}/`.
    if let Some(parent) = live_dir.parent() {
        fs::create_dir_all(parent).context(IoSnafu)?;
    }

    // If something already exists at `live_dir` after we trashed it
    // (which it shouldn't), rename will fail loudly — surface that as
    // an `Io` error rather than silently overwrite.
    match fs::rename(staging_dir, &live_dir) {
        Ok(()) => {},
        Err(e) if is_cross_device(&e) => {
            // Cross-fs move: copy recursively then remove the staging
            // tree.
            move_recursive(staging_dir, &live_dir)?;
        },
        Err(e) => return Err(BackupError::Io { source: e }),
    }

    // Count the swapped files for operator-facing diagnostics.
    let files_swapped = count_files_recursive(&live_dir)?;

    // Drop a marker file so the operator can confirm the swap before
    // restarting the node. The marker is JSON for easy machine
    // parsing; values are strings so older tools don't choke on numeric
    // overflow.
    write_restore_marker(data_dir, region, organization_id, &live_dir, &trash_dir)?;

    info!(
        live = %live_dir.display(),
        trash = %trash_dir.display(),
        organization_id = organization_id.value(),
        region = region.as_str(),
        files_swapped,
        "Restore applied"
    );

    Ok(RestoreApplyResult { trash_dir, files_swapped })
}

/// Walks `{data_dir}/.restore-trash/` and deletes timestamped trash
/// directories older than `retention`.
///
/// Returns the number of trash entries removed.
///
/// The trash retention defaults to 24h (per the operator runbook):
/// long enough for an operator to notice a mistaken restore and
/// recover the displaced tree, short enough to bound on-disk waste.
/// Slice 4 wires this into a periodic background job; for now this
/// helper exists so a startup hook (or operator script) can call it
/// directly.
///
/// Trash entries that fail to parse as `i64` microseconds-since-epoch
/// timestamps are skipped silently — they are operator artefacts the
/// sweeper has no business deleting.
///
/// # Errors
///
/// Returns [`BackupError::Io`] if `{data_dir}/.restore-trash/` cannot
/// be read or a removal fails. Missing trash directory is **not** an
/// error — returns `Ok(0)`.
pub fn sweep_restore_trash(data_dir: &Path, retention: Duration) -> Result<usize> {
    let trash_root = data_dir.join(".restore-trash");
    let entries = match fs::read_dir(&trash_root) {
        Ok(entries) => entries,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(BackupError::Io { source: e }),
    };

    let now_micros = Utc::now().timestamp_micros();
    let retention_micros = i64::try_from(retention.as_micros()).unwrap_or(i64::MAX);
    let cutoff = now_micros.saturating_sub(retention_micros);

    let mut removed = 0usize;
    for entry in entries {
        let entry = entry.context(IoSnafu)?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        let Ok(ts_micros) = name.parse::<i64>() else {
            continue;
        };
        if ts_micros < cutoff {
            fs::remove_dir_all(&path).context(IoSnafu)?;
            debug!(
                path = %path.display(),
                ts_micros,
                cutoff,
                "Pruned old restore-trash entry"
            );
            removed += 1;
        }
    }

    if removed > 0 {
        info!(removed, "Restore trash sweep completed");
    }
    Ok(removed)
}

/// Returns `true` if `dir` exists and contains at least one entry.
fn live_dir_has_contents(dir: &Path) -> Result<bool> {
    match fs::read_dir(dir) {
        Ok(mut entries) => Ok(entries.next().is_some()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(BackupError::Io { source: e }),
    }
}

/// Returns `true` if `err` is the cross-filesystem rename error.
fn is_cross_device(err: &io::Error) -> bool {
    // EXDEV is 18 on Linux/macOS; the error kind is unstable so we
    // fall back to the raw OS error.
    err.raw_os_error() == Some(18)
}

/// Recursively moves `src` to `dst`. Used as the cross-filesystem
/// fallback for `apply_staged_restore` when `fs::rename` returns
/// EXDEV. Not atomic — power loss mid-move leaves a partial tree at
/// `dst` and a partial tree at `src`. Same-filesystem callers always
/// hit `fs::rename` first and never reach this path.
fn move_recursive(src: &Path, dst: &Path) -> Result<()> {
    fs::create_dir_all(dst).context(IoSnafu)?;
    for entry in fs::read_dir(src).context(IoSnafu)? {
        let entry = entry.context(IoSnafu)?;
        let entry_path = entry.path();
        let dst_path = dst.join(entry.file_name());
        if entry_path.is_dir() {
            move_recursive(&entry_path, &dst_path)?;
        } else {
            fs::copy(&entry_path, &dst_path).context(IoSnafu)?;
            fs::remove_file(&entry_path).context(IoSnafu)?;
        }
    }
    fs::remove_dir(src).context(IoSnafu)?;
    Ok(())
}

/// Counts every file (not directory) under `dir`, recursively. Used
/// for `RestoreApplyResult::files_swapped`.
fn count_files_recursive(dir: &Path) -> Result<usize> {
    let mut count = 0usize;
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(BackupError::Io { source: e }),
    };
    for entry in entries {
        let entry = entry.context(IoSnafu)?;
        let path = entry.path();
        if path.is_dir() {
            count += count_files_recursive(&path)?;
        } else {
            count += 1;
        }
    }
    Ok(count)
}

/// Writes `{data_dir}/.restore-marker` with details of the restore that
/// just landed. Informational only — the operator inspects this before
/// restarting the node to confirm the swap happened.
fn write_restore_marker(
    data_dir: &Path,
    region: Region,
    organization_id: OrganizationId,
    live_dir: &Path,
    trash_dir: &Path,
) -> Result<()> {
    #[derive(serde::Serialize)]
    struct RestoreMarker<'a> {
        region: &'a str,
        organization_id: i64,
        applied_at_micros: i64,
        live_dir: String,
        trash_dir: Option<String>,
    }

    let marker = RestoreMarker {
        region: region.as_str(),
        organization_id: organization_id.value(),
        applied_at_micros: Utc::now().timestamp_micros(),
        live_dir: live_dir.display().to_string(),
        trash_dir: if trash_dir.as_os_str().is_empty() {
            None
        } else {
            Some(trash_dir.display().to_string())
        },
    };
    let marker_path = data_dir.join(".restore-marker");
    let json = serde_json::to_string_pretty(&marker)
        .map_err(|e| BackupError::Serialization { message: e.to_string() })?;
    fs::write(&marker_path, json).context(IoSnafu)?;
    Ok(())
}

/// Verifies that no other process holds the data-directory lock at
/// `{data_dir}/.lock`. If a node is running, this returns
/// [`BackupError::LiveDirInUse`] so the CLI fails loudly instead of
/// racing a live restore. The lock we acquire here is released
/// immediately on return — we do not hold it for the rest of the
/// `apply_staged_restore` call because a graceful restart between
/// `apply` and the operator's `start` should not be blocked by us.
fn enforce_node_stopped(data_dir: &Path) -> Result<()> {
    use fs2::FileExt;

    if !data_dir.exists() {
        // Brand-new data dir — no node could be running here.
        return Ok(());
    }
    let lock_path = data_dir.join(".lock");
    if !lock_path.exists() {
        // No lock file = no node has ever started here, or the operator
        // removed it manually. Either way, nothing to enforce.
        return Ok(());
    }

    // Open without truncating so we don't disturb the file's contents
    // (the running node never reads the file body, but be polite).
    let file = match fs::OpenOptions::new().read(true).write(true).open(&lock_path) {
        Ok(file) => file,
        Err(e) => return Err(BackupError::Io { source: e }),
    };

    match file.try_lock_exclusive() {
        Ok(()) => {
            // We got the lock — node is not running. Release immediately.
            let _ = FileExt::unlock(&file);
            Ok(())
        },
        Err(e)
            if e.kind() == io::ErrorKind::WouldBlock
                || e.raw_os_error() == Some(11)
                || e.raw_os_error() == Some(35) =>
        {
            Err(BackupError::LiveDirInUse { path: data_dir.to_path_buf() })
        },
        Err(e) => Err(BackupError::Io { source: e }),
    }
}

/// Automated backup background job.
///
/// Periodically creates one multi-DB archive per organization that lives
/// in the local node's region. Follows the same pattern as
/// `BTreeCompactor` and `ResourceMetricsCollector`.
///
/// The backup flow per cycle:
///
/// 1. Skip if not the leader for the region's control-plane group.
/// 2. Determine the set of organizations to back up — by default every org with a per-org group
///    running on this node, optionally filtered to an explicit list passed in
///    `Self::organizations`.
/// 3. Compute the local node's RMK fingerprint once and reuse it across every per-org archive built
///    this cycle.
/// 4. Call [`BackupManager::create_archive`] per organization. Failures are logged and counted in
///    metrics; one failure does not abort the rest of the cycle.
///
/// The backups directory is the manager's `backup_dir`; archive
/// filenames embed `{org_id}-{timestamp_micros}` so concurrent backups
/// across orgs cannot collide.
#[derive(bon::Builder)]
#[builder(on(_, required))]
pub struct BackupJob {
    /// Consensus handle for verifying leadership on the local region's
    /// control-plane group.
    handle: Arc<ConsensusHandle>,
    /// Backup manager for archive operations. Must have been built via
    /// [`BackupManager::with_data_dir`].
    backup_manager: Arc<BackupManager>,
    /// Raft manager — used to enumerate `(region, organization_id)`
    /// tuples that have a per-org Raft group running on this node.
    raft_manager: Arc<crate::raft_manager::RaftManager>,
    /// Region key manager — supplies the local node's RMK fingerprint
    /// stamped into every archive's manifest.
    key_manager: Arc<dyn inferadb_ledger_store::crypto::RegionKeyManager>,
    /// Optional explicit organization list. When `Some`, the job
    /// archives only these organizations. When `None`, the default
    /// behaviour is "every org with a per-org group running on this
    /// node in the local region".
    #[builder(default)]
    organizations: Option<Vec<OrganizationId>>,
    /// Optional applied-state accessor used to look up the
    /// per-organization slug for inclusion in the archive manifest.
    /// When unset, the archive manifest carries
    /// [`OrganizationSlug::new(0)`] as a sentinel — slug resolution is
    /// best-effort for the background job because it is not on the
    /// operator-facing critical path.
    #[builder(default)]
    applied_state: Option<crate::log_storage::AppliedStateAccessor>,
    /// Interval between backup cycles.
    interval: Duration,
    /// Cancellation token for graceful shutdown.
    cancellation_token: CancellationToken,
}

impl BackupJob {
    /// Starts the backup job as a background task.
    pub fn start(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = interval(self.interval);

            loop {
                tokio::select! {
                    _ = ticker.tick() => {}
                    _ = self.cancellation_token.cancelled() => {
                        info!("BackupJob shutting down");
                        break;
                    }
                }

                // Only the leader creates backups.
                if !self.handle.is_leader() {
                    debug!("Not leader, skipping backup cycle");
                    continue;
                }

                let cycle_start = Instant::now();
                let mut job = crate::logging::JobContext::new("backup", None);

                let region = self.raft_manager.local_region();

                // Resolve the set of orgs to back up this cycle. Default
                // is "every org with a per-org group running on this
                // node in the local region"; an operator override
                // (`Self::organizations`) replaces that with an exact
                // list.
                let orgs: Vec<OrganizationId> = match &self.organizations {
                    Some(list) => list.clone(),
                    None => self
                        .raft_manager
                        .list_organization_groups()
                        .into_iter()
                        .filter_map(|(r, oid)| {
                            // Skip the region's control-plane row
                            // (`OrganizationId(0)`) — backups are
                            // per-organization, and the control-plane
                            // group's DBs travel with the org tree of
                            // every member organization.
                            (r == region && oid.value() != 0).then_some(oid)
                        })
                        .collect(),
                };

                if orgs.is_empty() {
                    debug!(region = region.as_str(), "No organizations to back up this cycle");
                    continue;
                }

                info!(
                    region = region.as_str(),
                    org_count = orgs.len(),
                    "Starting automated backup cycle"
                );

                // Compute the RMK fingerprint once and reuse it across
                // every archive built this cycle.
                let rmk_fingerprint = match self.key_manager.rmk_fingerprint(region) {
                    Ok(fp) => fp,
                    Err(e) => {
                        error!(
                            region = region.as_str(),
                            error = %e,
                            "Failed to compute RMK fingerprint; aborting cycle"
                        );
                        crate::metrics::record_backup_failed();
                        job.set_failure();
                        continue;
                    },
                };

                let backup_dir = self.backup_manager.backup_dir().to_path_buf();

                let mut succeeded: u64 = 0;
                let mut failed: u64 = 0;

                for org_id in orgs {
                    let timestamp_micros = Utc::now().timestamp_micros();
                    let archive_filename =
                        format!("backup-{}-{}.tar.zst", org_id.value(), timestamp_micros);
                    let archive_path = backup_dir.join(&archive_filename);

                    let organization_slug = self
                        .applied_state
                        .as_ref()
                        .and_then(|state| state.resolve_id_to_slug(org_id))
                        .unwrap_or_else(|| OrganizationSlug::new(0));

                    match self
                        .backup_manager
                        .create_archive(
                            region,
                            org_id,
                            organization_slug,
                            rmk_fingerprint.clone(),
                            &archive_path,
                        )
                        .await
                    {
                        Ok(manifest) => {
                            let size_bytes =
                                std::fs::metadata(&archive_path).map(|m| m.len()).unwrap_or(0);
                            crate::metrics::record_backup_created(0, size_bytes);
                            job.record_items(1);
                            succeeded += 1;
                            info!(
                                organization_id = org_id.value(),
                                vault_count = manifest.vault_count,
                                size_bytes,
                                output = %archive_path.display(),
                                "Automated per-org backup completed"
                            );
                        },
                        Err(e) => {
                            error!(
                                organization_id = org_id.value(),
                                error = %e,
                                "Failed to create per-org archive; continuing"
                            );
                            crate::metrics::record_backup_failed();
                            job.set_failure();
                            failed += 1;
                        },
                    }
                }

                let duration_secs = cycle_start.elapsed().as_secs_f64();
                info!(
                    region = region.as_str(),
                    succeeded, failed, duration_secs, "Automated backup cycle finished"
                );
            }
        })
    }
}

/// Periodic background sweeper for `{data_dir}/.restore-trash/`.
///
/// `apply_staged_restore` displaces the live per-org tree into a
/// timestamped subdirectory under `.restore-trash/` instead of deleting
/// it outright, so an operator who realises the restore was a mistake
/// can recover the previous state. The trash window is finite — without
/// pruning, every restore on a given node leaks the displaced tree
/// indefinitely.
///
/// This job ticks at `Self::interval` (default 1 hour) and calls
/// [`sweep_restore_trash`] with `Self::retention` (default 24 hours).
/// The sweeper is conservative: it only removes entries whose directory
/// name parses as a microsecond timestamp older than `now - retention`;
/// hand-named directories an operator dropped in for forensic reasons
/// are skipped.
///
/// Lifecycle metrics:
/// - `ledger_restore_trash_swept_total` — count of entries removed across all cycles.
/// - `ledger_restore_trash_sweep_failures_total` — count of failed cycles.
#[derive(bon::Builder)]
#[builder(on(_, required))]
pub struct RestoreTrashSweepJob {
    /// Root data directory whose `.restore-trash/` subtree the job sweeps.
    data_dir: PathBuf,
    /// How long a trash entry survives before the sweeper removes it.
    /// Default 24 hours.
    #[builder(default = Duration::from_secs(24 * 60 * 60))]
    retention: Duration,
    /// Time between sweep cycles. Default 1 hour. Must be ≪ `retention`
    /// so a freshly-displaced tree gets several sweep ticks before it
    /// becomes a candidate for removal.
    #[builder(default = Duration::from_secs(60 * 60))]
    interval: Duration,
    /// Cancellation token for graceful shutdown.
    cancellation_token: CancellationToken,
}

impl RestoreTrashSweepJob {
    /// Starts the sweep job as a background task.
    ///
    /// The task runs on `interval` ticks, calling `sweep_restore_trash`
    /// each cycle. Shutdown is signalled by the cancellation token; the
    /// task drops out of its `tokio::select!` and exits cleanly.
    pub fn start(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = interval(self.interval);
            // The first tick fires immediately; skip it so the job
            // doesn't run a sweep cycle synchronously with `start()`.
            ticker.tick().await;

            loop {
                tokio::select! {
                    _ = ticker.tick() => {}
                    _ = self.cancellation_token.cancelled() => {
                        info!("RestoreTrashSweepJob shutting down");
                        break;
                    }
                }

                let cycle_start = Instant::now();
                let mut job = crate::logging::JobContext::new("restore_trash_sweep", None);

                match sweep_restore_trash(&self.data_dir, self.retention) {
                    Ok(removed) => {
                        if removed > 0 {
                            job.record_items(removed as u64);
                            crate::metrics::record_restore_trash_swept(removed as u64);
                        }
                        let duration_secs = cycle_start.elapsed().as_secs_f64();
                        info!(
                            data_dir = %self.data_dir.display(),
                            removed,
                            retention_secs = self.retention.as_secs(),
                            duration_secs,
                            "Restore-trash sweep cycle finished"
                        );
                    },
                    Err(e) => {
                        job.set_failure();
                        crate::metrics::record_restore_trash_sweep_failed();
                        error!(
                            data_dir = %self.data_dir.display(),
                            error = %e,
                            "Restore-trash sweep cycle failed"
                        );
                    },
                }
            }
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods, clippy::panic)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    // ------------------------------------------------------------------
    // create_archive — multi-DB archive entrypoint
    // ------------------------------------------------------------------

    /// Builds the synthetic on-disk org tree
    /// `{root}/{region}/{org_id}/...` and writes the listed files (each
    /// `(rel_path, contents)`).
    fn write_synthetic_org_tree(
        root: &Path,
        region: Region,
        organization_id: OrganizationId,
        files: &[(&str, &[u8])],
    ) -> PathBuf {
        let region_dir =
            if region == Region::GLOBAL { root.join("global") } else { root.join(region.as_str()) };
        let org_dir = region_dir.join(organization_id.value().to_string());
        for (rel_path, contents) in files {
            let target = org_dir.join(rel_path);
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent).expect("create parent dir");
            }
            fs::write(&target, contents).expect("write synthetic db");
        }
        org_dir
    }

    fn make_archive_manager(data_dir: &Path) -> BackupManager {
        let backup_dir = data_dir.join("_backups");
        let config = BackupConfig::builder()
            .destination(backup_dir.display().to_string())
            .retention_count(3_usize)
            .build()
            .expect("valid backup config");
        BackupManager::new(&config)
            .expect("create manager")
            .with_data_dir(data_dir.to_path_buf(), 7)
    }

    #[tokio::test]
    async fn create_archive_includes_org_and_vault_dbs() {
        let temp = TempDir::new().expect("tempdir");
        let region = Region::US_EAST_VA;
        let organization_id = OrganizationId::new(42);

        let _org_dir = write_synthetic_org_tree(
            temp.path(),
            region,
            organization_id,
            &[
                ("_meta.db", b"meta-db-bytes"),
                ("blocks.db", b"org-blocks-bytes"),
                ("raft.db", b"org-raft-bytes"),
                ("events.db", b"org-events-bytes"),
                ("state/vault-7/state.db", b"vault-7-state"),
                ("state/vault-7/raft.db", b"vault-7-raft"),
                ("state/vault-7/blocks.db", b"vault-7-blocks"),
                ("state/vault-7/events.db", b"vault-7-events"),
                ("state/vault-8/state.db", b"vault-8-state"),
                ("state/vault-8/raft.db", b"vault-8-raft"),
                ("state/vault-8/blocks.db", b"vault-8-blocks"),
                ("state/vault-8/events.db", b"vault-8-events"),
            ],
        );

        let manager = make_archive_manager(temp.path());
        let archive_path = temp.path().join("backup.tar.zst");

        let started_micros = Utc::now().timestamp_micros();
        let manifest = manager
            .create_archive(
                region,
                organization_id,
                OrganizationSlug::new(0),
                String::from("sha256:test-rmk"),
                &archive_path,
            )
            .await
            .expect("create_archive");

        // Manifest sanity.
        assert_eq!(manifest.region, region);
        assert_eq!(manifest.organization_id, organization_id);
        assert_eq!(manifest.vault_count, 2);
        assert_eq!(manifest.node_id_at_creation, 7);
        assert!(manifest.timestamp_micros >= started_micros);
        assert_eq!(manifest.rmk_fingerprint, "sha256:test-rmk");

        // 4 org-level + 2 * 4 per-vault = 12 entries.
        assert_eq!(manifest.dbs.len(), 12);
        let paths: Vec<&str> = manifest.dbs.iter().map(|e| e.path.as_str()).collect();
        for required in [
            "_meta.db",
            "blocks.db",
            "raft.db",
            "events.db",
            "state/vault-7/state.db",
            "state/vault-7/raft.db",
            "state/vault-7/blocks.db",
            "state/vault-7/events.db",
            "state/vault-8/state.db",
            "state/vault-8/raft.db",
            "state/vault-8/blocks.db",
            "state/vault-8/events.db",
        ] {
            assert!(paths.contains(&required), "missing entry for {required}: {paths:?}");
        }

        // Round-trip the archive: every member checksum must match the
        // value the manifest recorded, every member size must match, and
        // every member body must equal the source bytes on disk.
        let archive_file = fs::File::open(&archive_path).expect("open archive");
        let parsed = archive::parse_archive(archive_file, |path, reader| {
            let mut buf = Vec::new();
            reader.read_to_end(&mut buf).expect("read member");
            let entry =
                manifest.dbs.iter().find(|e| e.path == path).expect("manifest entry exists");
            assert_eq!(entry.size_bytes, buf.len() as u64, "size for {path}");
            let recomputed =
                format!("{}{}", archive::CHECKSUM_PREFIX_BLAKE3, blake3::hash(&buf).to_hex());
            assert_eq!(entry.checksum, recomputed, "checksum for {path}");
            Ok(())
        })
        .expect("parse archive");
        assert_eq!(parsed, manifest);
    }

    #[tokio::test]
    async fn create_archive_skips_missing_optional_files() {
        let temp = TempDir::new().expect("tempdir");
        let region = Region::US_EAST_VA;
        let organization_id = OrganizationId::new(11);

        // Required org files + one vault, **no** events.db at either
        // scope.
        write_synthetic_org_tree(
            temp.path(),
            region,
            organization_id,
            &[
                ("_meta.db", b"meta"),
                ("blocks.db", b"blocks"),
                ("raft.db", b"raft"),
                ("state/vault-3/state.db", b"v3-state"),
                ("state/vault-3/raft.db", b"v3-raft"),
                ("state/vault-3/blocks.db", b"v3-blocks"),
            ],
        );

        let manager = make_archive_manager(temp.path());
        let archive_path = temp.path().join("no-events.tar.zst");

        let manifest = manager
            .create_archive(
                region,
                organization_id,
                OrganizationSlug::new(0),
                String::from("sha256:test-rmk"),
                &archive_path,
            )
            .await
            .expect("create_archive");

        assert_eq!(manifest.vault_count, 1);
        assert_eq!(manifest.dbs.len(), 6, "3 org + 3 vault entries; no events.db");
        let paths: Vec<&str> = manifest.dbs.iter().map(|e| e.path.as_str()).collect();
        assert!(!paths.iter().any(|p| p.ends_with("events.db")), "events.db must be omitted");
    }

    #[tokio::test]
    async fn create_archive_handles_org_with_no_vaults() {
        let temp = TempDir::new().expect("tempdir");
        let region = Region::US_WEST_OR;
        let organization_id = OrganizationId::new(99);

        // Org-level files only — no `state/` directory at all.
        write_synthetic_org_tree(
            temp.path(),
            region,
            organization_id,
            &[("_meta.db", b"m"), ("blocks.db", b"b"), ("raft.db", b"r")],
        );

        let manager = make_archive_manager(temp.path());
        let archive_path = temp.path().join("no-vaults.tar.zst");

        let manifest = manager
            .create_archive(
                region,
                organization_id,
                OrganizationSlug::new(0),
                String::from("sha256:test-rmk"),
                &archive_path,
            )
            .await
            .expect("create_archive");

        assert_eq!(manifest.vault_count, 0);
        assert_eq!(manifest.dbs.len(), 3);
        let paths: Vec<&str> = manifest.dbs.iter().map(|e| e.path.as_str()).collect();
        assert_eq!(paths, vec!["_meta.db", "blocks.db", "raft.db"]);
    }

    #[tokio::test]
    async fn create_archive_requires_data_dir() {
        let temp = TempDir::new().expect("tempdir");
        let backup_dir = temp.path().join("_backups");
        let config = BackupConfig::builder()
            .destination(backup_dir.display().to_string())
            .retention_count(3_usize)
            .build()
            .expect("valid config");
        // Note: no `with_data_dir(...)` — the legacy snapshot constructor
        // is what every existing call site uses today.
        let manager = BackupManager::new(&config).expect("create manager");
        let archive_path = temp.path().join("should-not-exist.tar.zst");

        let err = manager
            .create_archive(
                Region::GLOBAL,
                OrganizationId::new(0),
                OrganizationSlug::new(0),
                String::from("sha256:test-rmk"),
                &archive_path,
            )
            .await
            .expect_err("create_archive without data_dir must fail");
        assert!(matches!(err, BackupError::DataDirMissing), "got {err:?}");
    }

    #[test]
    fn discover_vault_ids_returns_sorted_ascending() {
        let temp = TempDir::new().expect("tempdir");
        // Mix the dir creation order — discovery must sort by VaultId.
        for vault_id in [12, 3, 7, 1] {
            fs::create_dir_all(temp.path().join("state").join(format!("vault-{vault_id}")))
                .expect("create vault dir");
        }
        // Add some non-vault entries the scanner must ignore.
        fs::create_dir_all(temp.path().join("state").join("not-a-vault"))
            .expect("create unrelated dir");
        fs::create_dir_all(temp.path().join("state").join("vault-notnumeric"))
            .expect("create malformed dir");
        // Pre-P2b.0 flat-file form (`vault-{id}.db` directly inside
        // `state/`). This file is rejected at storage open time, but the
        // backup discovery scanner skips it silently because it only
        // matches directories.
        fs::write(temp.path().join("state").join("vault-99.db"), b"legacy")
            .expect("write legacy file");

        let ids = discover_vault_ids(temp.path()).expect("discover");
        let values: Vec<i64> = ids.iter().map(|v| v.value()).collect();
        assert_eq!(values, vec![1, 3, 7, 12]);
    }

    #[test]
    fn enumerate_archive_files_orders_org_then_vaults() {
        let temp = TempDir::new().expect("tempdir");
        let org_dir = temp.path();

        for name in ["_meta.db", "blocks.db", "raft.db", "events.db"] {
            fs::write(org_dir.join(name), b"x").expect("write org file");
        }
        for vault_id in [5, 2] {
            let dir = org_dir.join("state").join(format!("vault-{vault_id}"));
            fs::create_dir_all(&dir).expect("create vault dir");
            for name in ["state.db", "raft.db", "blocks.db"] {
                fs::write(dir.join(name), b"y").expect("write vault db");
            }
            // Vault 5 has events.db; vault 2 does not.
            if vault_id == 5 {
                fs::write(dir.join("events.db"), b"z").expect("write vault events");
            }
        }

        let (files, vault_count) = enumerate_archive_files(org_dir).expect("enumerate");
        assert_eq!(vault_count, 2);
        let rel_paths: Vec<&str> = files.iter().map(|(p, _)| p.as_str()).collect();
        assert_eq!(
            rel_paths,
            vec![
                "_meta.db",
                "blocks.db",
                "raft.db",
                "events.db",
                "state/vault-2/state.db",
                "state/vault-2/raft.db",
                "state/vault-2/blocks.db",
                "state/vault-5/state.db",
                "state/vault-5/raft.db",
                "state/vault-5/blocks.db",
                "state/vault-5/events.db",
            ]
        );
    }

    // ------------------------------------------------------------------
    // stage_restore + apply_staged_restore + sweep_restore_trash —
    // Slice 3 of the multi-DB backup work
    // ------------------------------------------------------------------

    /// Builds a real archive on disk and returns its path together
    /// with the manifest, the source per-org tree, and the `BackupManager`.
    /// Used to bootstrap every restore-related test.
    async fn build_test_archive(
        temp_path: &Path,
        region: Region,
        organization_id: OrganizationId,
    ) -> (BackupManager, PathBuf, BackupManifest) {
        write_synthetic_org_tree(
            temp_path,
            region,
            organization_id,
            &[
                ("_meta.db", b"meta-bytes"),
                ("blocks.db", b"blocks-bytes"),
                ("raft.db", b"raft-bytes"),
                ("events.db", b"events-bytes"),
                ("state/vault-3/state.db", b"vault-3-state"),
                ("state/vault-3/raft.db", b"vault-3-raft"),
                ("state/vault-3/blocks.db", b"vault-3-blocks"),
            ],
        );
        let manager = make_archive_manager(temp_path);
        let archive_path = temp_path.join("backup.tar.zst");
        let manifest = manager
            .create_archive(
                region,
                organization_id,
                OrganizationSlug::new(0),
                String::from("sha256:test-rmk"),
                &archive_path,
            )
            .await
            .expect("create archive");
        (manager, archive_path, manifest)
    }

    #[tokio::test]
    async fn stage_restore_validates_manifest_and_checksums() {
        let temp = TempDir::new().expect("tempdir");
        let region = Region::US_EAST_VA;
        let organization_id = OrganizationId::new(42);

        let (manager, archive_path, manifest) =
            build_test_archive(temp.path(), region, organization_id).await;

        // Run stage_restore. Expect no RMK enforcement here because
        // the per-region key manager is not yet wired (Slice 4); the
        // None branch must succeed and emit a warning.
        let result = manager.stage_restore(&archive_path, None).await.expect("stage_restore");

        assert_eq!(result.manifest, manifest);
        assert_eq!(result.files_staged, manifest.dbs.len());
        let expected_bytes: u64 = manifest.dbs.iter().map(|e| e.size_bytes).sum();
        assert_eq!(result.bytes_staged, expected_bytes);

        // Staging dir must exist and live under .restore-staging.
        assert!(result.staging_dir.exists());
        let staging_root = temp.path().join(".restore-staging");
        assert!(result.staging_dir.starts_with(&staging_root));

        // Every manifest entry maps to a real staged file with byte-identical contents.
        for entry in &manifest.dbs {
            let staged = result.staging_dir.join(&entry.path);
            assert!(staged.exists(), "missing staged file {}", entry.path);
            let bytes = fs::read(&staged).expect("read staged");
            assert_eq!(bytes.len() as u64, entry.size_bytes, "size for {}", entry.path);

            let original = match entry.path.as_str() {
                "_meta.db" => Some(b"meta-bytes".as_slice()),
                "blocks.db" => Some(b"blocks-bytes".as_slice()),
                "raft.db" => Some(b"raft-bytes".as_slice()),
                "events.db" => Some(b"events-bytes".as_slice()),
                "state/vault-3/state.db" => Some(b"vault-3-state".as_slice()),
                "state/vault-3/raft.db" => Some(b"vault-3-raft".as_slice()),
                "state/vault-3/blocks.db" => Some(b"vault-3-blocks".as_slice()),
                _ => None,
            };
            if let Some(original) = original {
                assert_eq!(bytes, original, "bytes for {}", entry.path);
            }
        }
    }

    #[tokio::test]
    async fn stage_restore_rejects_corrupted_archive() {
        let temp = TempDir::new().expect("tempdir");
        let region = Region::US_EAST_VA;
        let organization_id = OrganizationId::new(7);

        let (manager, archive_path, _manifest) =
            build_test_archive(temp.path(), region, organization_id).await;

        // Corrupt one byte deep inside the compressed archive — the
        // tar/zstd stream will surface a checksum failure (or a
        // decode failure further upstream) when stage_restore tries
        // to walk the members.
        let mut bytes = fs::read(&archive_path).expect("read archive");
        let len = bytes.len();
        // Avoid the very last byte (zstd epilogue) and the first 64
        // bytes (zstd header / first manifest member). Mutate
        // somewhere in the middle.
        let target = len / 2;
        bytes[target] ^= 0xFF;
        fs::write(&archive_path, &bytes).expect("write corrupted archive");

        let err = manager
            .stage_restore(&archive_path, None)
            .await
            .expect_err("stage_restore should fail on corrupted archive");
        // Any archive-format error is acceptable here — checksum
        // mismatch, zstd decode error, or tar-level error all map to
        // BackupError::Archive.
        assert!(matches!(err, BackupError::Archive { .. }), "expected Archive error, got {err:?}");

        // The staging directory must not be left behind on failure.
        let staging_root = temp.path().join(".restore-staging");
        if staging_root.exists() {
            let entries = fs::read_dir(&staging_root).expect("read staging root").count();
            assert_eq!(entries, 0, "stage_restore must clean up staging on failure");
        }
    }

    #[tokio::test]
    async fn stage_restore_rejects_rmk_fingerprint_mismatch() {
        let temp = TempDir::new().expect("tempdir");
        let region = Region::US_EAST_VA;
        let organization_id = OrganizationId::new(11);

        let (manager, archive_path, manifest) =
            build_test_archive(temp.path(), region, organization_id).await;

        // The placeholder fingerprint baked in by `create_archive` is
        // `placeholder:rmk-fingerprint-not-yet-wired-{region}` — feed
        // a different value so the assertion fires.
        let bogus = "sha256:does-not-match";
        let err = manager
            .stage_restore(&archive_path, Some(bogus))
            .await
            .expect_err("stage_restore should reject mismatched fingerprint");
        match err {
            BackupError::RmkFingerprintMismatch { expected, actual } => {
                assert_eq!(expected, bogus);
                assert_eq!(actual, manifest.rmk_fingerprint);
            },
            other => panic!("expected RmkFingerprintMismatch, got {other:?}"),
        }

        // Staging tree must be removed on RMK mismatch.
        let staging_root = temp.path().join(".restore-staging");
        if staging_root.exists() {
            let entries = fs::read_dir(&staging_root).expect("read staging root").count();
            assert_eq!(entries, 0, "RMK mismatch must clean up the staging tree");
        }
    }

    #[tokio::test]
    async fn stage_restore_tolerates_none_rmk_fingerprint() {
        let temp = TempDir::new().expect("tempdir");
        let region = Region::US_EAST_VA;
        let organization_id = OrganizationId::new(12);

        let (manager, archive_path, _manifest) =
            build_test_archive(temp.path(), region, organization_id).await;

        // None is the placeholder branch — must succeed regardless of
        // the manifest's fingerprint value (Slice 4 wires the
        // per-region key manager).
        let result = manager
            .stage_restore(&archive_path, None)
            .await
            .expect("stage_restore must accept None fingerprint");
        assert!(result.files_staged > 0);
    }

    #[test]
    fn apply_staged_restore_swaps_live_with_trash() {
        let temp = TempDir::new().expect("tempdir");
        let data_dir = temp.path();
        let region = Region::US_EAST_VA;
        let organization_id = OrganizationId::new(42);

        // Pre-existing live tree.
        let live_dir = data_dir.join(region.as_str()).join(organization_id.value().to_string());
        fs::create_dir_all(&live_dir).expect("create live dir");
        fs::write(live_dir.join("_meta.db"), b"OLD-meta").expect("write old meta");
        fs::write(live_dir.join("blocks.db"), b"OLD-blocks").expect("write old blocks");
        fs::create_dir_all(live_dir.join("state").join("vault-1")).expect("create live vault dir");
        fs::write(live_dir.join("state").join("vault-1").join("state.db"), b"OLD-vault-1-state")
            .expect("write old vault state");

        // Staging tree as produced by stage_restore.
        let staging_dir = data_dir.join(".restore-staging").join("42-1234567890");
        fs::create_dir_all(&staging_dir).expect("create staging dir");
        fs::write(staging_dir.join("_meta.db"), b"NEW-meta").expect("write new meta");
        fs::write(staging_dir.join("blocks.db"), b"NEW-blocks").expect("write new blocks");
        fs::write(staging_dir.join("raft.db"), b"NEW-raft").expect("write new raft");
        fs::create_dir_all(staging_dir.join("state").join("vault-2"))
            .expect("create staging vault dir");
        fs::write(staging_dir.join("state").join("vault-2").join("state.db"), b"NEW-vault-2-state")
            .expect("write new vault state");

        let result = apply_staged_restore(data_dir, &staging_dir, region, organization_id)
            .expect("apply_staged_restore");

        // Live dir now reflects the staging contents.
        assert_eq!(fs::read(live_dir.join("_meta.db")).expect("read meta"), b"NEW-meta");
        assert_eq!(fs::read(live_dir.join("blocks.db")).expect("read blocks"), b"NEW-blocks");
        assert_eq!(fs::read(live_dir.join("raft.db")).expect("read raft"), b"NEW-raft");
        assert_eq!(
            fs::read(live_dir.join("state").join("vault-2").join("state.db"))
                .expect("read new vault"),
            b"NEW-vault-2-state"
        );

        // Old vault tree (vault-1) is gone from live (replaced by vault-2).
        assert!(!live_dir.join("state").join("vault-1").exists());

        // Trash dir holds the displaced live tree.
        assert!(result.trash_dir.is_dir());
        let trashed_meta = result.trash_dir.join("_meta.db");
        assert_eq!(fs::read(&trashed_meta).expect("read trashed meta"), b"OLD-meta");
        assert!(result.trash_dir.starts_with(data_dir.join(".restore-trash")));
        assert!(
            result.files_swapped >= 4,
            "expected ≥4 swapped files, got {}",
            result.files_swapped
        );

        // Staging tree is gone (renamed onto live_dir).
        assert!(!staging_dir.exists());

        // Marker file exists and is JSON-parsable.
        let marker_path = data_dir.join(".restore-marker");
        assert!(marker_path.exists());
        let marker = fs::read_to_string(&marker_path).expect("read marker");
        assert!(marker.contains("us-east-va") || marker.contains("US_EAST_VA"));
        assert!(marker.contains(&format!("\"organization_id\": {}", organization_id.value())));
    }

    #[test]
    fn apply_staged_restore_with_no_existing_live_tree_skips_trash() {
        let temp = TempDir::new().expect("tempdir");
        let data_dir = temp.path();
        let region = Region::US_WEST_OR;
        let organization_id = OrganizationId::new(99);

        // No live tree exists — fresh restore on this node.
        let staging_dir = data_dir.join(".restore-staging").join("99-1");
        fs::create_dir_all(&staging_dir).expect("create staging dir");
        fs::write(staging_dir.join("_meta.db"), b"meta").expect("write meta");

        let result = apply_staged_restore(data_dir, &staging_dir, region, organization_id)
            .expect("apply_staged_restore");

        // Trash dir is empty (no displacement happened).
        assert!(result.trash_dir.as_os_str().is_empty());
        assert_eq!(result.files_swapped, 1);

        let live_dir = data_dir.join(region.as_str()).join(organization_id.value().to_string());
        assert_eq!(fs::read(live_dir.join("_meta.db")).expect("read live"), b"meta");
    }

    #[test]
    fn apply_staged_restore_refuses_when_node_running() {
        use fs2::FileExt;

        let temp = TempDir::new().expect("tempdir");
        let data_dir = temp.path();
        let region = Region::US_EAST_VA;
        let organization_id = OrganizationId::new(5);

        // Simulate a running node: create + flock the .lock file.
        let lock_path = data_dir.join(".lock");
        let lock_file = fs::File::create(&lock_path).expect("create lock");
        lock_file.try_lock_exclusive().expect("acquire lock");

        let staging_dir = data_dir.join(".restore-staging").join("5-1");
        fs::create_dir_all(&staging_dir).expect("create staging");
        fs::write(staging_dir.join("_meta.db"), b"x").expect("write meta");

        let err = apply_staged_restore(data_dir, &staging_dir, region, organization_id)
            .expect_err("apply must refuse against running node");
        assert!(
            matches!(err, BackupError::LiveDirInUse { .. }),
            "expected LiveDirInUse, got {err:?}"
        );

        // Drop the lock to keep the temp dir cleanup happy.
        FileExt::unlock(&lock_file).expect("release lock");
    }

    #[test]
    fn sweep_restore_trash_deletes_old_entries() {
        let temp = TempDir::new().expect("tempdir");
        let data_dir = temp.path();
        let trash_root = data_dir.join(".restore-trash");

        // Old entry: timestamp ~48h in the past.
        let old_ts = Utc::now().timestamp_micros() - (48 * 60 * 60 * 1_000_000);
        let old_dir = trash_root.join(old_ts.to_string()).join("42");
        fs::create_dir_all(&old_dir).expect("create old trash");
        fs::write(old_dir.join("payload"), b"old").expect("write old payload");

        // Recent entry: just now.
        let recent_ts = Utc::now().timestamp_micros();
        let recent_dir = trash_root.join(recent_ts.to_string()).join("42");
        fs::create_dir_all(&recent_dir).expect("create recent trash");
        fs::write(recent_dir.join("payload"), b"new").expect("write recent payload");

        // Non-numeric directory name must be skipped silently.
        fs::create_dir_all(trash_root.join("not-a-timestamp")).expect("create misnamed dir");

        let removed =
            sweep_restore_trash(data_dir, Duration::from_secs(24 * 60 * 60)).expect("sweep");
        assert_eq!(removed, 1);

        assert!(!trash_root.join(old_ts.to_string()).exists());
        assert!(trash_root.join(recent_ts.to_string()).exists());
        assert!(trash_root.join("not-a-timestamp").exists());
    }

    #[test]
    fn sweep_restore_trash_handles_missing_root() {
        let temp = TempDir::new().expect("tempdir");
        let removed =
            sweep_restore_trash(temp.path(), Duration::from_secs(24 * 60 * 60)).expect("sweep");
        assert_eq!(removed, 0);
    }

    /// `RestoreTrashSweepJob` actually runs `sweep_restore_trash` on its
    /// ticker. Drives the job through one cycle by configuring a tiny
    /// interval, lets the cancellation token fire after the cycle, and
    /// verifies the old entry is gone while the recent entry survives.
    #[tokio::test]
    async fn restore_trash_sweep_job_prunes_old_entries_on_tick() {
        let temp = TempDir::new().expect("tempdir");
        let data_dir = temp.path().to_path_buf();
        let trash_root = data_dir.join(".restore-trash");

        // Old entry: ~48h in the past.
        let old_ts = Utc::now().timestamp_micros() - (48 * 60 * 60 * 1_000_000);
        let old_dir = trash_root.join(old_ts.to_string()).join("42");
        fs::create_dir_all(&old_dir).expect("create old trash");
        fs::write(old_dir.join("payload"), b"old").expect("write old payload");

        // Recent entry: now.
        let recent_ts = Utc::now().timestamp_micros();
        let recent_dir = trash_root.join(recent_ts.to_string()).join("42");
        fs::create_dir_all(&recent_dir).expect("create recent trash");
        fs::write(recent_dir.join("payload"), b"new").expect("write recent payload");

        let token = CancellationToken::new();
        let job = RestoreTrashSweepJob::builder()
            .data_dir(data_dir.clone())
            .interval(Duration::from_millis(50))
            .retention(Duration::from_secs(24 * 60 * 60))
            .cancellation_token(token.clone())
            .build();
        let handle = job.start();

        // Wait until the old entry is gone — tick is 50ms; allow up to
        // 5s of CI jitter before failing.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while trash_root.join(old_ts.to_string()).exists() {
            if std::time::Instant::now() >= deadline {
                panic!("sweep job did not prune old entry within deadline");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        token.cancel();
        let _ = handle.await;

        assert!(!trash_root.join(old_ts.to_string()).exists());
        assert!(trash_root.join(recent_ts.to_string()).exists());
    }

    /// The job must shut down promptly when its cancellation token is
    /// triggered, even with a long interval that would otherwise leave
    /// the task parked on `ticker.tick()`.
    #[tokio::test]
    async fn restore_trash_sweep_job_responds_to_cancellation() {
        let temp = TempDir::new().expect("tempdir");
        let token = CancellationToken::new();
        let job = RestoreTrashSweepJob::builder()
            .data_dir(temp.path().to_path_buf())
            .interval(Duration::from_secs(3600))
            .retention(Duration::from_secs(24 * 60 * 60))
            .cancellation_token(token.clone())
            .build();
        let handle = job.start();

        token.cancel();
        // Bound the wait — if the select arm doesn't observe the
        // cancellation, this hangs and surfaces the bug.
        tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("job must shut down within 2s of cancellation")
            .expect("join the spawned task");
    }

    #[tokio::test]
    async fn stage_then_apply_round_trip() {
        // End-to-end: build archive, stage_restore, apply_staged_restore,
        // verify the resulting live tree matches the archive's source
        // bytes byte-for-byte.
        let temp = TempDir::new().expect("tempdir");
        let region = Region::US_EAST_VA;
        let organization_id = OrganizationId::new(77);

        // Source bytes — keep a copy so we can verify after the swap.
        let source_files: &[(&str, &[u8])] = &[
            ("_meta.db", b"src-meta"),
            ("blocks.db", b"src-blocks"),
            ("raft.db", b"src-raft"),
            ("state/vault-1/state.db", b"src-vault-1-state"),
            ("state/vault-1/raft.db", b"src-vault-1-raft"),
            ("state/vault-1/blocks.db", b"src-vault-1-blocks"),
        ];

        // Build archive in a separate "source" data directory so we
        // don't conflict with the destination data directory.
        let src_dir = temp.path().join("src");
        fs::create_dir_all(&src_dir).expect("create src dir");
        write_synthetic_org_tree(&src_dir, region, organization_id, source_files);

        let src_manager = make_archive_manager(&src_dir);
        let archive_path = temp.path().join("roundtrip.tar.zst");
        let _manifest = src_manager
            .create_archive(
                region,
                organization_id,
                OrganizationSlug::new(0),
                String::from("sha256:test-rmk"),
                &archive_path,
            )
            .await
            .expect("create archive");

        // Destination data directory — distinct from the source so the
        // test exercises restore-onto-fresh-node.
        let dst_dir = temp.path().join("dst");
        fs::create_dir_all(&dst_dir).expect("create dst dir");
        let dst_backup = dst_dir.join("_backups");
        let config = BackupConfig::builder()
            .destination(dst_backup.display().to_string())
            .retention_count(3_usize)
            .build()
            .expect("valid config");
        let dst_manager = BackupManager::new(&config)
            .expect("create dst manager")
            .with_data_dir(dst_dir.clone(), 7);

        let staging = dst_manager.stage_restore(&archive_path, None).await.expect("stage");
        let _apply = apply_staged_restore(&dst_dir, &staging.staging_dir, region, organization_id)
            .expect("apply");

        // Verify every source byte landed in the live tree.
        let live_dir = dst_dir.join(region.as_str()).join(organization_id.value().to_string());
        for (rel, expected_bytes) in source_files {
            let path = live_dir.join(rel);
            let actual = fs::read(&path).expect("read live file");
            assert_eq!(&actual, expected_bytes, "bytes for {rel}");
        }
    }
}
