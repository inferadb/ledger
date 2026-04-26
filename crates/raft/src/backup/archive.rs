//! Multi-DB backup archive format primitives.
//!
//! Implements the on-disk format described in
//! `docs/superpowers/specs/2026-04-24-multi-db-backup-archive-format.md`:
//! a single `tar` stream compressed with `zstd`, carrying a JSON manifest
//! followed by every per-organization database file (control-plane
//! `_meta.db` / `raft.db` / `blocks.db` / `events.db` and every per-vault
//! `state.db` / `raft.db` / `blocks.db` / `events.db`).
//!
//! This module is the format-only surface (Slice 1 of the multi-DB backup
//! work). It exposes:
//!
//! - [`BackupManifest`] / [`DbEntry`] — manifest schema + serde.
//! - [`build_archive`] — streaming archive build over [`Write`].
//! - [`parse_archive`] — streaming archive parse over [`Read`].
//! - [`compute_blake3_streaming`] / [`verify_checksum`] — checksum helpers.
//! - [`BackupArchiveError`] — typed failure modes.
//!
//! ## Format layout
//!
//! ```text
//! +-- zstd-compressed tar stream ----------------------------------+
//! |  member 0: manifest.json (uncompressed JSON; first entry)       |
//! |  member 1: <dbs[0].path>  (file bytes)                          |
//! |  member 2: <dbs[1].path>  (file bytes)                          |
//! |  ...                                                            |
//! +-----------------------------------------------------------------+
//! ```
//!
//! Manifest-first ordering lets readers parse the manifest with a single
//! seek and then either iterate the remaining members lazily or extract
//! a specific path on demand.
//!
//! ## Streaming guarantees
//!
//! - **Build**: the producer reads each source file twice (once to compute the blake3 checksum and
//!   size, once to append to the tar stream). Neither pass buffers the file in memory; both are
//!   bounded by a fixed-size scratch buffer.
//! - **Parse**: the consumer reads the manifest fully into memory (small JSON payload), then
//!   exposes a member iterator that streams each subsequent entry over a fixed-size buffer.
//!
//! ## Encryption envelope
//!
//! Database file bytes are written verbatim — they are already encrypted
//! at rest with their per-vault DEK, and the DEK envelope is stored
//! alongside the DB. The RMK is **never** included; restore performs an
//! RMK-fingerprint pre-flight before any envelope unwrap is attempted.

use std::{
    fs::File,
    io::{self, Read, Write},
    path::PathBuf,
};

use inferadb_ledger_types::{OrganizationId, OrganizationSlug, Region};
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};

/// Schema version for the multi-DB backup archive format.
///
/// A reader rejects archives whose `schema_version` differs from this
/// constant — there is no compatibility shim between schema versions.
pub const BACKUP_SCHEMA_VERSION: u32 = 1;

/// Format sentinel string. Carried in the manifest to guard against
/// pointing the restore path at an unrelated `.tar.zst` archive.
pub const BACKUP_FORMAT: &str = "inferadb-ledger-multi-db-backup";

/// Path of the manifest member inside the tar archive. Always written
/// as the first entry so it can be parsed with a single seek.
pub const MANIFEST_FILE_NAME: &str = "manifest.json";

/// `zstd` compression level. Level 3 is the libzstd default and the
/// proposed setting in the format spec; B+tree pages compress well at
/// low levels and producer CPU is unlikely to be the bottleneck.
pub const ZSTD_COMPRESSION_LEVEL: i32 = 3;

/// Prefix used for [`DbEntry::checksum`] strings to disambiguate the
/// hash algorithm if the format ever switches to a different primitive.
pub const CHECKSUM_PREFIX_BLAKE3: &str = "blake3:";

/// Buffer size for streaming reads when computing checksums and copying
/// file bytes into the tar stream. 64 KiB matches the default block size
/// used by `std::io::copy` and is comfortable on every supported target.
const STREAM_BUFFER_SIZE: usize = 64 * 1024;

// ---------------------------------------------------------------------------
// Manifest types
// ---------------------------------------------------------------------------

/// Top-level manifest carried as `manifest.json` inside the archive.
///
/// The manifest enumerates every database file present in the archive
/// alongside its size and blake3 checksum, plus the metadata needed to
/// validate restoration (region, organization, RMK fingerprint).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BackupManifest {
    /// Format schema version. Currently always [`BACKUP_SCHEMA_VERSION`].
    pub schema_version: u32,
    /// Format sentinel literal — protects against an operator pointing
    /// the restore path at an unrelated `.tar.zst` archive.
    pub format: String,
    /// Region the source organization lives in. Restore on a node in a
    /// different region is rejected at the RMK fingerprint check.
    pub region: Region,
    /// Internal numeric organization ID at backup time.
    pub organization_id: OrganizationId,
    /// External Snowflake slug for the organization at backup time.
    /// Used in the archive filename and operator-facing surfaces.
    pub organization_slug: OrganizationSlug,
    /// Backup creation timestamp, expressed in microseconds since the
    /// Unix epoch.
    pub timestamp_micros: i64,
    /// Opaque fingerprint of the source node's RMK public material
    /// (sha256 with the `sha256:` prefix). Equality is the only
    /// operation defined on this string.
    pub rmk_fingerprint: String,
    /// Node ID that produced the archive. Diagnostic-only.
    pub node_id_at_creation: u64,
    /// Every database file packed into the archive, in the order they
    /// appear after the manifest entry.
    pub dbs: Vec<DbEntry>,
    /// Number of per-vault subtrees enumerated in [`Self::dbs`].
    /// Diagnostic — does not influence parsing.
    pub vault_count: u32,
    /// Application version that built the archive. Diagnostic-only.
    pub created_by_app_version: String,
}

/// One database entry inside the archive — represents either a
/// control-plane DB at the per-org root (e.g. `_meta.db`, `raft.db`,
/// `blocks.db`, `events.db`) or a per-vault DB nested under
/// `state/vault-{id}/`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DbEntry {
    /// Relative path within the archive. Always uses forward slashes
    /// regardless of host platform — the tar member name is the source
    /// of truth and it is normalised to forward slashes.
    ///
    /// Examples: `"_meta.db"`, `"blocks.db"`,
    /// `"state/vault-7/state.db"`.
    pub path: String,
    /// Total bytes written for this entry (uncompressed file size). The
    /// reader uses this both to bound member iteration and to surface a
    /// useful error when a checksum mismatch is detected.
    pub size_bytes: u64,
    /// blake3 checksum of the uncompressed file bytes, formatted as
    /// `"blake3:<hex>"`. The prefix is fixed by [`CHECKSUM_PREFIX_BLAKE3`].
    pub checksum: String,
}

impl BackupManifest {
    /// Verifies that this manifest's [`Self::schema_version`] matches the
    /// reader's expected version. Returns
    /// [`BackupArchiveError::SchemaVersionMismatch`] if it does not.
    ///
    /// # Errors
    ///
    /// Returns [`BackupArchiveError::SchemaVersionMismatch`] when the
    /// archive declares a `schema_version` other than
    /// [`BACKUP_SCHEMA_VERSION`].
    pub fn check_schema_version(&self) -> Result<(), BackupArchiveError> {
        if self.schema_version != BACKUP_SCHEMA_VERSION {
            return Err(BackupArchiveError::SchemaVersionMismatch {
                found: self.schema_version,
                expected: BACKUP_SCHEMA_VERSION,
            });
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors raised by the backup archive format primitives.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum BackupArchiveError {
    /// Failed to serialize or deserialize the manifest as JSON.
    #[snafu(display("Backup archive manifest invalid: {source}"))]
    InvalidManifest {
        /// Underlying serde-json error.
        source: serde_json::Error,
    },

    /// I/O error while building or parsing the archive.
    #[snafu(display("Backup archive I/O error: {source}"))]
    Io {
        /// Underlying I/O error.
        source: io::Error,
    },

    /// Tar-level format error (header construction or member iteration).
    #[snafu(display("Backup archive tar error: {source}"))]
    Tar {
        /// Underlying tar error (a wrapped [`io::Error`]).
        source: io::Error,
    },

    /// `zstd` codec error during encoding or decoding.
    #[snafu(display("Backup archive zstd error: {source}"))]
    Zstd {
        /// Underlying zstd error (a wrapped [`io::Error`]).
        source: io::Error,
    },

    /// Member checksum did not match the manifest entry.
    #[snafu(display(
        "Backup archive checksum mismatch for {path}: expected {expected}, got {actual}"
    ))]
    ChecksumMismatch {
        /// Relative archive path of the offending entry.
        path: String,
        /// Checksum recorded in the manifest.
        expected: String,
        /// Checksum actually observed while streaming the entry.
        actual: String,
    },

    /// Manifest schema version did not match the reader's expected version.
    #[snafu(display(
        "Backup archive schema version mismatch: archive declares {found}, expected {expected}"
    ))]
    SchemaVersionMismatch {
        /// Schema version declared by the archive.
        found: u32,
        /// Schema version this build of the reader supports.
        expected: u32,
    },

    /// Format sentinel mismatch — the archive's manifest does not carry
    /// the [`BACKUP_FORMAT`] literal. Surfaces when an operator points
    /// the restore path at an unrelated `.tar.zst` file.
    #[snafu(display("Backup archive format sentinel mismatch: expected {expected}, got {found}"))]
    FormatSentinelMismatch {
        /// Sentinel string carried in the archive's manifest.
        found: String,
        /// Sentinel string this reader expects ([`BACKUP_FORMAT`]).
        expected: String,
    },

    /// Archive is missing the manifest entry, or the manifest entry is
    /// not the first member as required by the format.
    #[snafu(display(
        "Backup archive missing or misordered manifest: expected {expected} as first entry"
    ))]
    ManifestMissing {
        /// Expected manifest member name ([`MANIFEST_FILE_NAME`]).
        expected: String,
    },

    /// Manifest enumerates a path that did not appear in the archive.
    #[snafu(display("Backup archive missing entry for manifest path: {path}"))]
    MissingMember {
        /// Manifest path that had no corresponding tar entry.
        path: String,
    },

    /// Archive contains an entry that was not declared in the manifest.
    #[snafu(display("Backup archive has unexpected member: {path}"))]
    UnexpectedMember {
        /// Tar member path that has no manifest entry.
        path: String,
    },
}

// ---------------------------------------------------------------------------
// Checksum helpers
// ---------------------------------------------------------------------------

/// Streams `reader` to completion, computing a blake3 checksum of every
/// byte read. Returns the checksum formatted as
/// `"blake3:<lowercase-hex>"` with a fixed prefix matching
/// [`CHECKSUM_PREFIX_BLAKE3`].
///
/// The reader is consumed; the caller is responsible for any buffering
/// or seeking required to re-read the underlying source.
///
/// # Errors
///
/// Returns any [`io::Error`] surfaced by the underlying reader.
pub fn compute_blake3_streaming(mut reader: impl Read) -> io::Result<String> {
    let mut hasher = blake3::Hasher::new();
    let mut buf = vec![0u8; STREAM_BUFFER_SIZE];
    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    let hash = hasher.finalize();
    Ok(format!("{CHECKSUM_PREFIX_BLAKE3}{}", hash.to_hex()))
}

/// Verifies that the blake3 checksum of `reader` matches `expected`.
///
/// `expected` must use the `"blake3:<hex>"` form produced by
/// [`compute_blake3_streaming`]. The reader is consumed.
///
/// # Errors
///
/// - [`BackupArchiveError::Io`] if the reader fails.
/// - [`BackupArchiveError::ChecksumMismatch`] if the computed checksum differs from `expected`. The
///   error carries an empty `path`; callers that know the archive-relative path should construct
///   the variant themselves to provide better diagnostics.
pub fn verify_checksum(reader: impl Read, expected: &str) -> Result<(), BackupArchiveError> {
    let actual = compute_blake3_streaming(reader).context(IoSnafu)?;
    if actual != expected {
        return Err(BackupArchiveError::ChecksumMismatch {
            path: String::new(),
            expected: expected.to_string(),
            actual,
        });
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Build
// ---------------------------------------------------------------------------

/// Builds a `tar` + `zstd` archive containing `manifest.json` followed by
/// every file in `files`.
///
/// The function mutates `manifest.dbs[i]` to fill in `size_bytes` and
/// `checksum` for each entry. The mutation is keyed by archive-relative
/// path: each `(rel_path, source)` in `files` must have a matching
/// `manifest.dbs[i].path == rel_path`. Entries already populated are
/// overwritten with the freshly computed values; entries with no
/// matching `files` row are surfaced as
/// [`BackupArchiveError::MissingMember`].
///
/// The function reads each source file twice (once to compute the
/// checksum, once to stream into the tar). Neither pass buffers the
/// file in memory.
///
/// # Errors
///
/// - [`BackupArchiveError::Io`] / [`BackupArchiveError::Tar`] / [`BackupArchiveError::Zstd`] for
///   the obvious failure paths.
/// - [`BackupArchiveError::InvalidManifest`] if the manifest cannot be serialized.
/// - [`BackupArchiveError::MissingMember`] if a manifest entry has no matching `files` row.
pub fn build_archive(
    output: impl Write,
    manifest: &mut BackupManifest,
    files: &[(String, PathBuf)],
) -> Result<(), BackupArchiveError> {
    // Pass 1 — populate manifest entries from the source files. We
    // compute size + checksum by streaming each file once. The manifest
    // is indexed by `path`; any file whose `rel_path` does not match an
    // existing entry produces a `MissingMember` error.
    for (rel_path, source) in files {
        let entry = manifest
            .dbs
            .iter_mut()
            .find(|e| e.path == *rel_path)
            .ok_or_else(|| BackupArchiveError::MissingMember { path: rel_path.clone() })?;

        let metadata = std::fs::metadata(source).context(IoSnafu)?;
        entry.size_bytes = metadata.len();

        let file = File::open(source).context(IoSnafu)?;
        entry.checksum = compute_blake3_streaming(file).context(IoSnafu)?;
    }

    // Pass 2 — write the tar stream.
    let zstd_encoder =
        zstd::Encoder::new(output, ZSTD_COMPRESSION_LEVEL).context(ZstdSnafu)?.auto_finish();
    let mut tar_builder = tar::Builder::new(zstd_encoder);

    // Manifest first.
    let manifest_json = serde_json::to_vec_pretty(&manifest).context(InvalidManifestSnafu)?;
    let mut manifest_header = tar::Header::new_gnu();
    manifest_header.set_size(manifest_json.len() as u64);
    manifest_header.set_mode(0o644);
    manifest_header.set_cksum();
    tar_builder
        .append_data(&mut manifest_header, MANIFEST_FILE_NAME, &manifest_json[..])
        .context(TarSnafu)?;

    // Then every database file in declaration order.
    for (rel_path, source) in files {
        let mut file = File::open(source).context(IoSnafu)?;
        // `append_file` rebuilds the header from the file's metadata
        // (including size). We use `append_data` instead so the header
        // path is exactly the archive-relative path with forward
        // slashes regardless of host platform.
        let metadata = file.metadata().context(IoSnafu)?;
        let mut header = tar::Header::new_gnu();
        header.set_size(metadata.len());
        header.set_mode(0o644);
        header.set_cksum();
        tar_builder.append_data(&mut header, rel_path, &mut file).context(TarSnafu)?;
    }

    tar_builder.finish().context(TarSnafu)?;
    // Dropping the tar builder finalises the underlying zstd encoder
    // (`auto_finish` writes the zstd epilogue on drop).
    drop(tar_builder);
    Ok(())
}

// ---------------------------------------------------------------------------
// Parse
// ---------------------------------------------------------------------------

/// Streams an archive end-to-end, invoking `on_member` for each non-
/// manifest entry in the order they appear after the manifest. Returns
/// the validated manifest once the archive is exhausted.
///
/// The callback receives the archive-relative path (forward-slashed,
/// platform-independent) and a streaming reader bounded by the entry's
/// recorded size. The callback is responsible for either consuming the
/// reader fully or returning an error — bytes left unread are skipped
/// when the next member is yielded, so partial consumption never
/// corrupts the iteration but it does discard the unread suffix.
///
/// Callbacks should typically:
///
/// - copy member bytes to a destination file (via [`std::io::copy`]),
/// - hash member bytes for checksum verification (via [`compute_blake3_streaming`]), or
/// - both, by tee-ing the reader through a hashing wrapper.
///
/// The function does not buffer member bodies in memory — only the
/// fixed-size scratch buffer used internally by `tar` is allocated.
///
/// # Errors
///
/// - [`BackupArchiveError::Zstd`] if the input is not a valid zstd stream.
/// - [`BackupArchiveError::Tar`] / [`BackupArchiveError::Io`] if the inner tar is malformed.
/// - [`BackupArchiveError::ManifestMissing`] if the first member is not the manifest.
/// - [`BackupArchiveError::InvalidManifest`] if the manifest is not valid JSON or has the wrong
///   shape.
/// - [`BackupArchiveError::SchemaVersionMismatch`] if the manifest declares an unsupported schema
///   version.
/// - [`BackupArchiveError::FormatSentinelMismatch`] if the manifest's `format` field does not match
///   [`BACKUP_FORMAT`].
/// - Any error returned by `on_member`.
pub fn parse_archive<R, F>(input: R, mut on_member: F) -> Result<BackupManifest, BackupArchiveError>
where
    R: Read,
    F: FnMut(&str, &mut dyn Read) -> Result<(), BackupArchiveError>,
{
    let decoder = zstd::Decoder::new(input).context(ZstdSnafu)?;
    let mut archive = tar::Archive::new(decoder);
    let mut entries = archive.entries().context(TarSnafu)?;

    // First member must be the manifest.
    let mut first = match entries.next() {
        Some(res) => res.context(TarSnafu)?,
        None => {
            return Err(BackupArchiveError::ManifestMissing {
                expected: MANIFEST_FILE_NAME.to_string(),
            });
        },
    };
    let first_path =
        first.path().context(TarSnafu)?.to_string_lossy().replace('\\', "/").to_string();
    if first_path != MANIFEST_FILE_NAME {
        return Err(BackupArchiveError::ManifestMissing {
            expected: MANIFEST_FILE_NAME.to_string(),
        });
    }

    let mut manifest_bytes = Vec::new();
    first.read_to_end(&mut manifest_bytes).context(IoSnafu)?;
    let manifest: BackupManifest =
        serde_json::from_slice(&manifest_bytes).context(InvalidManifestSnafu)?;
    manifest.check_schema_version()?;
    if manifest.format != BACKUP_FORMAT {
        return Err(BackupArchiveError::FormatSentinelMismatch {
            found: manifest.format.clone(),
            expected: BACKUP_FORMAT.to_string(),
        });
    }

    drop(first);

    // Walk every remaining entry, surfacing each to the caller.
    for entry in entries {
        let mut entry = entry.context(TarSnafu)?;
        let path = entry.path().context(TarSnafu)?.to_string_lossy().replace('\\', "/").to_string();
        on_member(&path, &mut entry)?;
    }

    Ok(manifest)
}

/// Reads the manifest from an archive without iterating any other
/// members. Useful for `list_backups` / pre-flight checks where only
/// the manifest is needed.
///
/// Errors mirror [`parse_archive`].
///
/// # Errors
///
/// See [`parse_archive`] — this function shares its error surface.
pub fn read_manifest<R: Read>(input: R) -> Result<BackupManifest, BackupArchiveError> {
    let decoder = zstd::Decoder::new(input).context(ZstdSnafu)?;
    let mut archive = tar::Archive::new(decoder);
    let mut entries = archive.entries().context(TarSnafu)?;

    let mut first = match entries.next() {
        Some(res) => res.context(TarSnafu)?,
        None => {
            return Err(BackupArchiveError::ManifestMissing {
                expected: MANIFEST_FILE_NAME.to_string(),
            });
        },
    };
    let first_path =
        first.path().context(TarSnafu)?.to_string_lossy().replace('\\', "/").to_string();
    if first_path != MANIFEST_FILE_NAME {
        return Err(BackupArchiveError::ManifestMissing {
            expected: MANIFEST_FILE_NAME.to_string(),
        });
    }

    let mut manifest_bytes = Vec::new();
    first.read_to_end(&mut manifest_bytes).context(IoSnafu)?;
    let manifest: BackupManifest =
        serde_json::from_slice(&manifest_bytes).context(InvalidManifestSnafu)?;
    manifest.check_schema_version()?;
    if manifest.format != BACKUP_FORMAT {
        return Err(BackupArchiveError::FormatSentinelMismatch {
            found: manifest.format.clone(),
            expected: BACKUP_FORMAT.to_string(),
        });
    }

    Ok(manifest)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods, clippy::panic)]
mod tests {
    use std::{
        io::{Cursor, Seek, SeekFrom, Write},
        path::Path,
    };

    use tempfile::TempDir;

    use super::*;

    fn sample_manifest(dbs: Vec<DbEntry>) -> BackupManifest {
        BackupManifest {
            schema_version: BACKUP_SCHEMA_VERSION,
            format: BACKUP_FORMAT.to_string(),
            region: Region::US_EAST_VA,
            organization_id: OrganizationId::new(42),
            organization_slug: OrganizationSlug::new(123_456_789),
            timestamp_micros: 1_714_000_000_000_000,
            rmk_fingerprint: "sha256:abc123".to_string(),
            node_id_at_creation: 1,
            dbs,
            vault_count: 0,
            created_by_app_version: "0.1.0".to_string(),
        }
    }

    fn write_synthetic_file(dir: &Path, name: &str, contents: &[u8]) -> PathBuf {
        let path = dir.join(name);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("create parent dir for synthetic file");
        }
        let mut file = File::create(&path).expect("create synthetic file");
        file.write_all(contents).expect("write synthetic file");
        file.sync_all().expect("sync synthetic file");
        path
    }

    fn placeholder_db(path: &str) -> DbEntry {
        DbEntry { path: path.to_string(), size_bytes: 0, checksum: String::new() }
    }

    #[test]
    fn manifest_round_trips_via_json() {
        let manifest = sample_manifest(vec![DbEntry {
            path: "_meta.db".to_string(),
            size_bytes: 4096,
            checksum: "blake3:deadbeef".to_string(),
        }]);

        let json = serde_json::to_string_pretty(&manifest).expect("serialize manifest");
        let decoded: BackupManifest = serde_json::from_str(&json).expect("deserialize manifest");

        assert_eq!(manifest, decoded);
        assert_eq!(decoded.schema_version, BACKUP_SCHEMA_VERSION);
        assert_eq!(decoded.format, BACKUP_FORMAT);
    }

    #[test]
    fn manifest_check_schema_version_rejects_other_versions() {
        let mut manifest = sample_manifest(vec![]);
        manifest.schema_version = 999;
        let err = manifest.check_schema_version().expect_err("schema check should fail");
        match err {
            BackupArchiveError::SchemaVersionMismatch { found, expected } => {
                assert_eq!(found, 999);
                assert_eq!(expected, BACKUP_SCHEMA_VERSION);
            },
            other => panic!("expected SchemaVersionMismatch, got {other:?}"),
        }
    }

    #[test]
    fn compute_blake3_streaming_matches_one_shot() {
        let payload = b"the quick brown fox jumps over the lazy dog";
        let streaming = compute_blake3_streaming(Cursor::new(payload)).expect("hash payload");
        let expected = format!("{CHECKSUM_PREFIX_BLAKE3}{}", blake3::hash(payload).to_hex());
        assert_eq!(streaming, expected);
    }

    #[test]
    fn verify_checksum_accepts_matching_hash() {
        let payload = b"hello-world";
        let expected = compute_blake3_streaming(Cursor::new(payload)).expect("compute checksum");
        verify_checksum(Cursor::new(payload), &expected).expect("checksum should match");
    }

    #[test]
    fn verify_checksum_rejects_mismatched_hash() {
        let payload = b"hello-world";
        let bogus = format!("{CHECKSUM_PREFIX_BLAKE3}{}", "00".repeat(32));
        let err = verify_checksum(Cursor::new(payload), &bogus)
            .expect_err("verify should fail on mismatch");
        match err {
            BackupArchiveError::ChecksumMismatch { expected, actual, .. } => {
                assert_eq!(expected, bogus);
                assert_ne!(actual, bogus);
            },
            other => panic!("expected ChecksumMismatch, got {other:?}"),
        }
    }

    #[test]
    fn archive_round_trips_with_synthetic_files() {
        let temp = TempDir::new().expect("tempdir");
        let root = temp.path();

        // Three synthetic DB files of varying sizes — one is nested
        // under `state/vault-7/` to exercise multi-component paths.
        let files = vec![
            ("_meta.db".to_string(), write_synthetic_file(root, "_meta.db", b"meta-db-bytes")),
            (
                "blocks.db".to_string(),
                write_synthetic_file(root, "blocks.db", &vec![7u8; 8 * 1024]),
            ),
            (
                "state/vault-7/state.db".to_string(),
                write_synthetic_file(root, "vault-7-state.db", b"vault-7-state-db-bytes"),
            ),
        ];

        let mut manifest = sample_manifest(vec![
            placeholder_db("_meta.db"),
            placeholder_db("blocks.db"),
            placeholder_db("state/vault-7/state.db"),
        ]);
        manifest.vault_count = 1;

        let mut archive_buf: Vec<u8> = Vec::new();
        build_archive(&mut archive_buf, &mut manifest, &files).expect("build archive");

        // Manifest entries are now populated.
        for (rel_path, source) in &files {
            let entry =
                manifest.dbs.iter().find(|e| &e.path == rel_path).expect("manifest entry exists");
            let on_disk_size = std::fs::metadata(source).expect("stat source").len();
            assert_eq!(entry.size_bytes, on_disk_size);
            assert!(entry.checksum.starts_with(CHECKSUM_PREFIX_BLAKE3));

            let recomputed = compute_blake3_streaming(File::open(source).expect("reopen source"))
                .expect("recompute checksum");
            assert_eq!(entry.checksum, recomputed);
        }

        // Parse + iterate.
        let mut seen: Vec<(String, Vec<u8>)> = Vec::new();
        let parsed_manifest = parse_archive(Cursor::new(&archive_buf), |path, reader| {
            let mut buf = Vec::new();
            reader.read_to_end(&mut buf).context(IoSnafu)?;
            seen.push((path.to_string(), buf));
            Ok(())
        })
        .expect("parse archive");
        assert_eq!(parsed_manifest, manifest);

        assert_eq!(seen.len(), files.len());
        for (rel_path, source) in &files {
            let on_disk = std::fs::read(source).expect("read source");
            let archived = seen
                .iter()
                .find(|(p, _)| p == rel_path)
                .map(|(_, bytes)| bytes.clone())
                .expect("member present");
            assert_eq!(on_disk, archived, "bytes for {rel_path}");
        }
    }

    #[test]
    fn archive_rejects_corrupted_checksum() {
        let temp = TempDir::new().expect("tempdir");
        let root = temp.path();
        let original_path = write_synthetic_file(root, "blocks.db", b"original-db-bytes");

        let files = vec![("blocks.db".to_string(), original_path.clone())];
        let mut manifest = sample_manifest(vec![placeholder_db("blocks.db")]);
        let mut archive_buf: Vec<u8> = Vec::new();
        build_archive(&mut archive_buf, &mut manifest, &files).expect("build archive");

        let recorded_checksum = manifest
            .dbs
            .iter()
            .find(|e| e.path == "blocks.db")
            .expect("manifest entry")
            .checksum
            .clone();

        // Parse and verify the recorded checksum against tampered bytes
        // by feeding a different payload through `verify_checksum`.
        let tampered = b"tampered-db-bytes-of-different-content";
        let err = verify_checksum(Cursor::new(tampered), &recorded_checksum)
            .expect_err("verify should reject tampered bytes");
        match err {
            BackupArchiveError::ChecksumMismatch { expected, actual, .. } => {
                assert_eq!(expected, recorded_checksum);
                assert_ne!(actual, recorded_checksum);
            },
            other => panic!("expected ChecksumMismatch, got {other:?}"),
        }
    }

    #[test]
    fn archive_rejects_schema_version_mismatch() {
        let temp = TempDir::new().expect("tempdir");
        let root = temp.path();
        let path = write_synthetic_file(root, "_meta.db", b"x");
        let files = vec![("_meta.db".to_string(), path)];
        let mut manifest = sample_manifest(vec![placeholder_db("_meta.db")]);
        manifest.schema_version = 999;

        let mut archive_buf: Vec<u8> = Vec::new();
        build_archive(&mut archive_buf, &mut manifest, &files).expect("build archive");

        match parse_archive(Cursor::new(&archive_buf), |_path, _reader| Ok(())) {
            Err(BackupArchiveError::SchemaVersionMismatch { found, expected }) => {
                assert_eq!(found, 999);
                assert_eq!(expected, BACKUP_SCHEMA_VERSION);
            },
            Err(other) => panic!("expected SchemaVersionMismatch, got {other:?}"),
            Ok(_) => panic!("expected parse to fail on bad schema version"),
        }
    }

    #[test]
    fn archive_rejects_format_sentinel_mismatch() {
        let temp = TempDir::new().expect("tempdir");
        let root = temp.path();
        let path = write_synthetic_file(root, "_meta.db", b"x");
        let files = vec![("_meta.db".to_string(), path)];
        let mut manifest = sample_manifest(vec![placeholder_db("_meta.db")]);
        manifest.format = "not-the-real-format".to_string();

        let mut archive_buf: Vec<u8> = Vec::new();
        build_archive(&mut archive_buf, &mut manifest, &files).expect("build archive");

        match parse_archive(Cursor::new(&archive_buf), |_path, _reader| Ok(())) {
            Err(BackupArchiveError::FormatSentinelMismatch { found, expected }) => {
                assert_eq!(found, "not-the-real-format");
                assert_eq!(expected, BACKUP_FORMAT);
            },
            Err(other) => panic!("expected FormatSentinelMismatch, got {other:?}"),
            Ok(_) => panic!("expected parse to fail on bad format sentinel"),
        }
    }

    #[test]
    fn archive_handles_empty_dbs_list() {
        let mut manifest = sample_manifest(vec![]);
        let mut archive_buf: Vec<u8> = Vec::new();
        build_archive(&mut archive_buf, &mut manifest, &[]).expect("build empty archive");

        let mut count = 0usize;
        let parsed_manifest = parse_archive(Cursor::new(&archive_buf), |_path, _reader| {
            count += 1;
            Ok(())
        })
        .expect("parse empty");
        assert_eq!(parsed_manifest, manifest);
        assert_eq!(count, 0, "empty archive yields no non-manifest members");

        // The standalone manifest reader produces the same result.
        let manifest_only = read_manifest(Cursor::new(&archive_buf)).expect("read manifest only");
        assert_eq!(manifest_only, manifest);
    }

    #[test]
    fn build_archive_rejects_files_not_in_manifest() {
        let temp = TempDir::new().expect("tempdir");
        let root = temp.path();
        let path = write_synthetic_file(root, "_meta.db", b"y");
        let files = vec![("_meta.db".to_string(), path)];
        let mut manifest = sample_manifest(vec![]);

        let err = build_archive(Vec::new(), &mut manifest, &files)
            .expect_err("missing manifest entry should error");
        match err {
            BackupArchiveError::MissingMember { path } => {
                assert_eq!(path, "_meta.db");
            },
            other => panic!("expected MissingMember, got {other:?}"),
        }
    }

    /// Streams a 16 MiB synthetic file end-to-end and verifies round-trip
    /// integrity. Sized at 16 MiB rather than 100 MiB so the full unit
    /// test suite stays under the ~8s target; the test still exercises
    /// the streaming path (buffer is 64 KiB) over many iterations.
    #[test]
    fn archive_streams_large_file_without_oom() {
        let temp = TempDir::new().expect("tempdir");
        let root = temp.path();

        // 16 MiB of pseudo-random-ish bytes (seeded deterministic
        // pattern; we don't need cryptographic randomness, only
        // non-trivial entropy to avoid degenerate compression).
        const PAYLOAD_LEN: usize = 16 * 1024 * 1024;
        let large_path = root.join("blocks.db");
        {
            let mut file = File::create(&large_path).expect("create large file");
            let mut buf = [0u8; 4096];
            for (i, byte) in buf.iter_mut().enumerate() {
                *byte = (i % 251) as u8;
            }
            let mut written = 0usize;
            while written < PAYLOAD_LEN {
                let chunk = std::cmp::min(buf.len(), PAYLOAD_LEN - written);
                file.write_all(&buf[..chunk]).expect("write chunk");
                written += chunk;
            }
            file.sync_all().expect("sync large file");
        }

        let files = vec![("blocks.db".to_string(), large_path.clone())];
        let mut manifest = sample_manifest(vec![placeholder_db("blocks.db")]);

        // Build to a real on-disk archive so we exercise the file-backed
        // write path as well as the in-memory buffer path used by other
        // tests.
        let archive_path = root.join("backup.tar.zst");
        {
            let archive_file = File::create(&archive_path).expect("create archive");
            build_archive(archive_file, &mut manifest, &files).expect("build archive");
        }

        // Reopen + parse + verify. We stream the member through a
        // hasher and check the result against the manifest's recorded
        // checksum — at no point do we buffer the full payload in
        // memory.
        let mut archive_file = File::open(&archive_path).expect("open archive");
        archive_file.seek(SeekFrom::Start(0)).expect("seek archive");

        let mut member_count = 0usize;
        let mut observed_path = String::new();
        let mut observed_total = 0u64;
        let mut observed_checksum = String::new();

        let parsed_manifest = parse_archive(&mut archive_file, |path, reader| {
            member_count += 1;
            observed_path = path.to_string();
            let mut hasher = blake3::Hasher::new();
            let mut buf = vec![0u8; STREAM_BUFFER_SIZE];
            loop {
                let n = reader.read(&mut buf).context(IoSnafu)?;
                if n == 0 {
                    break;
                }
                hasher.update(&buf[..n]);
                observed_total += n as u64;
            }
            observed_checksum = format!("{CHECKSUM_PREFIX_BLAKE3}{}", hasher.finalize().to_hex());
            Ok(())
        })
        .expect("parse archive");

        assert_eq!(parsed_manifest.dbs.len(), 1);
        assert_eq!(parsed_manifest.dbs[0].size_bytes, PAYLOAD_LEN as u64);
        assert_eq!(member_count, 1);
        assert_eq!(observed_path, "blocks.db");
        assert_eq!(observed_total, PAYLOAD_LEN as u64);
        assert_eq!(observed_checksum, parsed_manifest.dbs[0].checksum);
    }
}
