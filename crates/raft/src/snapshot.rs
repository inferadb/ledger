//! File-based streaming snapshots with zstd compression and SHA-256 integrity.
//!
//! Uses `tokio::fs::File` as the snapshot transport type for file-based
//! streaming rather than in-memory buffers.
//!
//! ## Scope
//!
//! Stage 1b bifurcates snapshots into two scopes:
//!
//! - **Org snapshot** ([`SnapshotScope::Org`]) — captures the per-organization Raft control-plane
//!   tables that live on the org-level `raft.db` (the per-org Raft state-machine + organization /
//!   vault directory metadata + sequences). Contains no entity / relationship / block data — those
//!   live on per-vault databases (root rule 17).
//! - **Vault snapshot** ([`SnapshotScope::Vault`]) — captures the four-database state of a single
//!   vault: the vault's per-shard `raft.db` (vault meta + heights + hashes + health +
//!   client-sequence dedup), the vault's `state.db` (entities + relationships + per-vault string
//!   dictionary), the vault's `blocks.db` (block archive), and the vault's apply-phase event stream
//!   (events.db).
//!
//! The scope is encoded in the on-disk header so [`SnapshotReader`] can refuse a vault snapshot
//! installed into an org store (or vice versa) before any data is written.
//!
//! ## File format (v2)
//!
//! All sections from header through event_section are wrapped in a single
//! zstd-compressed stream. The SHA-256 checksum is computed over the
//! **compressed** bytes and appended after the zstd stream is finalized.
//!
//! ```text
//! [header]
//!   magic: [u8; 4]          = b"LSNP"
//!   version: u8              = 2
//!   flags: u8                = bit 0: scope (0 = Org, 1 = Vault)
//!                              bits 1-7: reserved (must be 0)
//!   core_len: u32le
//!   core: [u8; core_len]    = postcard(AppliedStateCore)
//!   if scope == Vault:
//!     organization_id: i64be
//!     vault_id: i64be
//!
//! [table_block_count: u32le]
//!   number of typed table blocks that follow
//!
//! For each table block:
//!   block_kind: u8                   = 1: org_raft, 2: vault_raft,
//!                                      3: vault_state, 4: vault_blocks
//!   block_table_count: u32le
//!   for each table:
//!     table_id: u8
//!     entry_count: u32le
//!     for each entry:
//!       key_len: u32le
//!       key: [u8; key_len]
//!       value_len: u32le
//!       value: [u8; value_len]
//!
//! [event_section]
//!   event_count: u64le               = always 0 for Org snapshots; per-vault count for Vault
//!   for each event:
//!     entry_len: u32le
//!     entry: [u8; entry_len]         = postcard(EventEntry)
//!
//! [footer]
//!   checksum: [u8; 32]               = SHA-256 of all compressed bytes
//! ```
//!
//! Block kinds map onto the source databases: an Org snapshot writes one block (`OrgRaft`); a
//! Vault snapshot writes three (`VaultRaft`, `VaultState`, `VaultBlocks`), in that order, plus the
//! event section. The order is part of the wire contract — install replays it sequentially.
//!
//! ## Version compatibility
//!
//! v1 snapshots wrote a flat table list followed by a separate `entity_section`. The v1 reader is
//! gone; on-disk v1 files are rejected with [`SnapshotError::UnsupportedVersion`]. No production
//! deployments persist v1 snapshots — Stage 2's `SnapshotPersister` is the first persistence
//! consumer, and it lands together with v2.

use std::pin::Pin;

use async_compression::tokio::{bufread::ZstdDecoder, write::ZstdEncoder};
use inferadb_ledger_types::{OrganizationId, VaultId};
use sha2::{Digest, Sha256};
use snafu::{ResultExt, Snafu};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};

/// Snapshot file magic bytes.
pub const SNAPSHOT_MAGIC: &[u8; 4] = b"LSNP";

/// Current snapshot format version. v2 adds the [`SnapshotScope`] header marker
/// and replaces the flat `table_count` / `entity_section` layout with typed
/// table blocks (see module docs).
pub const SNAPSHOT_VERSION: u8 = 2;

/// zstd compression level (matches backup path).
pub const ZSTD_LEVEL: i32 = 3;

/// Size of the SHA-256 checksum footer.
pub const CHECKSUM_SIZE: usize = 32;

/// Flags-byte bit mask for vault-scope snapshots.
///
/// When `flags & FLAG_SCOPE_VAULT != 0` the header is followed by an additional
/// `organization_id` + `vault_id` pair. When the bit is clear the snapshot is
/// org-scoped and the header ends after `core`.
pub const FLAG_SCOPE_VAULT: u8 = 0b0000_0001;

/// Scope marker for a snapshot file — discriminates org-level snapshots from
/// per-vault snapshots.
///
/// The scope is part of the LSNP v2 wire format (see module docs). Cross-scope
/// installs (e.g. an org snapshot installed into a per-vault store) are
/// rejected by
/// [`RaftLogStore::install_snapshot`](crate::log_storage::RaftLogStore::install_snapshot).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotScope {
    /// An org-level snapshot owned by an `OrganizationGroup` Raft shard.
    ///
    /// Captures the org's `raft.db` only. Per-vault entity / block data is
    /// excluded — vault scope owns that data.
    Org { organization_id: OrganizationId },
    /// A per-vault snapshot owned by a `VaultGroup` Raft shard.
    ///
    /// Captures the vault's `raft.db`, `state.db`, `blocks.db`, and the
    /// apply-phase event stream from `events.db`.
    Vault { organization_id: OrganizationId, vault_id: VaultId },
}

impl SnapshotScope {
    /// Returns the on-disk flags byte for this scope.
    pub fn flags_byte(&self) -> u8 {
        match self {
            SnapshotScope::Org { .. } => 0,
            SnapshotScope::Vault { .. } => FLAG_SCOPE_VAULT,
        }
    }

    /// Returns the `organization_id` for any scope.
    pub fn organization_id(&self) -> OrganizationId {
        match self {
            SnapshotScope::Org { organization_id }
            | SnapshotScope::Vault { organization_id, .. } => *organization_id,
        }
    }

    /// Returns the `vault_id` when the scope is `Vault`; `None` otherwise.
    pub fn vault_id(&self) -> Option<VaultId> {
        match self {
            SnapshotScope::Org { .. } => None,
            SnapshotScope::Vault { vault_id, .. } => Some(*vault_id),
        }
    }

    /// True when this scope is `Vault`.
    pub fn is_vault(&self) -> bool {
        matches!(self, SnapshotScope::Vault { .. })
    }
}

/// On-disk block kinds inside a snapshot file's table section. See the module
/// docs for layout. Values are part of the wire format; never renumber.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TableBlockKind {
    /// Tables read from the org-level `raft.db` (org snapshots only).
    OrgRaft = 1,
    /// Tables read from a per-vault `raft.db`.
    VaultRaft = 2,
    /// Tables read from a per-vault `state.db`.
    VaultState = 3,
    /// Tables read from a per-vault `blocks.db`.
    VaultBlocks = 4,
}

impl TableBlockKind {
    /// Decodes a wire byte into a `TableBlockKind`, or returns the raw byte
    /// inside a `SnapshotError::InvalidEntry` if it's outside the known range.
    pub fn from_u8(byte: u8) -> Result<Self, SnapshotError> {
        match byte {
            1 => Ok(TableBlockKind::OrgRaft),
            2 => Ok(TableBlockKind::VaultRaft),
            3 => Ok(TableBlockKind::VaultState),
            4 => Ok(TableBlockKind::VaultBlocks),
            other => Err(SnapshotError::InvalidEntry {
                reason: format!("unknown table block kind: {other}"),
            }),
        }
    }
}

// ============================================================================
// Errors
// ============================================================================

/// Snapshot operation failure.
#[derive(Debug, Snafu)]
pub enum SnapshotError {
    /// I/O error during snapshot read/write.
    #[snafu(display("Snapshot I/O error: {source}"))]
    Io {
        source: std::io::Error,
        #[snafu(implicit)]
        location: snafu::Location,
    },

    /// Invalid snapshot magic bytes.
    #[snafu(display("Invalid snapshot magic: expected LSNP, got {found:?}"))]
    BadMagic { found: [u8; 4] },

    /// Unsupported snapshot version.
    #[snafu(display("Unsupported snapshot version: {version} (expected <= {SNAPSHOT_VERSION})"))]
    UnsupportedVersion { version: u8 },

    /// SHA-256 checksum mismatch.
    #[snafu(display("Snapshot checksum mismatch: expected {expected}, got {actual}"))]
    ChecksumMismatch { expected: String, actual: String },

    /// Snapshot file is truncated (too short for expected data).
    #[snafu(display("Snapshot file truncated: {reason}"))]
    Truncated { reason: String },

    /// Unknown table ID encountered.
    #[snafu(display("Unknown table ID in snapshot: {table_id}"))]
    UnknownTableId { table_id: u8 },

    /// Postcard serialization/deserialization error.
    #[snafu(display("Snapshot codec error: {source}"))]
    Codec {
        source: postcard::Error,
        #[snafu(implicit)]
        location: snafu::Location,
    },

    /// Invalid entry in snapshot (e.g., impossible length).
    #[snafu(display("Invalid snapshot entry: {reason}"))]
    InvalidEntry { reason: String },

    /// Snapshot scope (org / vault) does not match the receiving store.
    ///
    /// Returned by
    /// [`RaftLogStore::install_snapshot`](crate::log_storage::RaftLogStore::install_snapshot)
    /// when an org snapshot is installed into a per-vault store, when a vault
    /// snapshot is installed into an org store, or when a vault snapshot is
    /// installed into a different vault's store.
    #[snafu(display("Snapshot scope mismatch: {reason}"))]
    ScopeMismatch { reason: String },
}

// ============================================================================
// HashingWriter — wraps a writer and computes SHA-256 over all bytes written
// ============================================================================

/// Wraps an `AsyncWrite` and hashes all bytes passing through it.
///
/// The SHA-256 digest is computed over the bytes **after** they pass through
/// this writer (i.e., the compressed bytes when placed between a ZstdEncoder
/// and the file).
pub struct HashingWriter<W> {
    inner: W,
    hasher: Sha256,
}

impl<W> HashingWriter<W> {
    /// Creates a new `HashingWriter` wrapping the given writer.
    pub fn new(inner: W) -> Self {
        Self { inner, hasher: Sha256::new() }
    }

    /// Consumes the writer and returns the inner writer plus the SHA-256 digest.
    pub fn finalize(self) -> (W, [u8; 32]) {
        let hash = self.hasher.finalize();
        (self.inner, hash.into())
    }
}

impl<W: AsyncWrite + Unpin> AsyncWrite for HashingWriter<W> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        let result = Pin::new(&mut this.inner).poll_write(cx, buf);
        if let std::task::Poll::Ready(Ok(n)) = &result {
            this.hasher.update(&buf[..*n]);
        }
        result
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

// ============================================================================
// SnapshotWriter — writes the sequential file format
// ============================================================================

/// Writes a snapshot file in the streaming format with zstd compression.
///
/// The writer pipeline is: uncompressed data → ZstdEncoder → HashingWriter → File.
/// After all data is written, call [`finish`](Self::finish) to finalize the
/// zstd stream and append the SHA-256 checksum.
pub struct SnapshotWriter<W: AsyncWrite + Unpin> {
    encoder: ZstdEncoder<HashingWriter<W>>,
}

impl<W: AsyncWrite + Unpin> SnapshotWriter<W> {
    /// Creates a new `SnapshotWriter` wrapping the given file/writer.
    pub fn new(writer: W) -> Self {
        let hashing = HashingWriter::new(writer);
        let encoder =
            ZstdEncoder::with_quality(hashing, async_compression::Level::Precise(ZSTD_LEVEL));
        Self { encoder }
    }

    /// Writes the snapshot header (magic, version, flags, optional vault
    /// coordinates) and the `AppliedStateCore` section.
    pub async fn write_header(
        &mut self,
        scope: SnapshotScope,
        core_bytes: &[u8],
    ) -> Result<(), SnapshotError> {
        // Magic
        self.encoder.write_all(SNAPSHOT_MAGIC).await.context(IoSnafu)?;
        // Version
        self.encoder.write_all(&[SNAPSHOT_VERSION]).await.context(IoSnafu)?;
        // Flags (scope marker)
        self.encoder.write_all(&[scope.flags_byte()]).await.context(IoSnafu)?;
        // Core length + core bytes
        let core_len =
            u32::try_from(core_bytes.len()).map_err(|_| SnapshotError::InvalidEntry {
                reason: format!("AppliedStateCore too large: {} bytes", core_bytes.len()),
            })?;
        self.encoder.write_all(&core_len.to_le_bytes()).await.context(IoSnafu)?;
        self.encoder.write_all(core_bytes).await.context(IoSnafu)?;

        // Vault coordinates trail the core bytes when scope is Vault.
        // Org snapshots already carry their organization_id inside `core`
        // (AppliedStateCore.last_applied stamps the org), so we don't
        // duplicate it on disk for the org case. Vault snapshots need an
        // explicit org+vault pair because the parent core doesn't know
        // which child the snapshot is for.
        if let SnapshotScope::Vault { organization_id, vault_id } = scope {
            self.encoder
                .write_all(&organization_id.value().to_be_bytes())
                .await
                .context(IoSnafu)?;
            self.encoder.write_all(&vault_id.value().to_be_bytes()).await.context(IoSnafu)?;
        }
        Ok(())
    }

    /// Writes the table-block-count (number of typed table blocks that
    /// follow). Stage 1b vaults write 3 blocks (raft, state, blocks); orgs
    /// write 1 (raft).
    pub async fn write_table_block_count(&mut self, count: u32) -> Result<(), SnapshotError> {
        self.encoder.write_all(&count.to_le_bytes()).await.context(IoSnafu)?;
        Ok(())
    }

    /// Writes a typed table-block header: the block kind tag plus the table
    /// count inside the block.
    pub async fn write_table_block_header(
        &mut self,
        kind: TableBlockKind,
        table_count: u32,
    ) -> Result<(), SnapshotError> {
        self.encoder.write_all(&[kind as u8]).await.context(IoSnafu)?;
        self.encoder.write_all(&table_count.to_le_bytes()).await.context(IoSnafu)?;
        Ok(())
    }

    /// Writes a table section header (table_id, entry_count).
    pub async fn write_table_header(
        &mut self,
        table_id: u8,
        entry_count: u32,
    ) -> Result<(), SnapshotError> {
        self.encoder.write_all(&[table_id]).await.context(IoSnafu)?;
        self.encoder.write_all(&entry_count.to_le_bytes()).await.context(IoSnafu)?;
        Ok(())
    }

    /// Writes a single table entry (key_len, key, value_len, value).
    pub async fn write_table_entry(
        &mut self,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), SnapshotError> {
        let key_len = u32::try_from(key.len()).map_err(|_| SnapshotError::InvalidEntry {
            reason: format!("Key too large: {} bytes", key.len()),
        })?;
        let value_len = u32::try_from(value.len()).map_err(|_| SnapshotError::InvalidEntry {
            reason: format!("Value too large: {} bytes", value.len()),
        })?;
        self.encoder.write_all(&key_len.to_le_bytes()).await.context(IoSnafu)?;
        self.encoder.write_all(key).await.context(IoSnafu)?;
        self.encoder.write_all(&value_len.to_le_bytes()).await.context(IoSnafu)?;
        self.encoder.write_all(value).await.context(IoSnafu)?;
        Ok(())
    }

    /// Writes the event section header (event_count as u64le).
    pub async fn write_event_count(&mut self, count: u64) -> Result<(), SnapshotError> {
        self.encoder.write_all(&count.to_le_bytes()).await.context(IoSnafu)?;
        Ok(())
    }

    /// Writes a single event entry (entry_len, entry_bytes).
    pub async fn write_event_entry(&mut self, entry_bytes: &[u8]) -> Result<(), SnapshotError> {
        let entry_len =
            u32::try_from(entry_bytes.len()).map_err(|_| SnapshotError::InvalidEntry {
                reason: format!("Event entry too large: {} bytes", entry_bytes.len()),
            })?;
        self.encoder.write_all(&entry_len.to_le_bytes()).await.context(IoSnafu)?;
        self.encoder.write_all(entry_bytes).await.context(IoSnafu)?;
        Ok(())
    }

    /// Finalizes the zstd stream and writes the SHA-256 checksum footer.
    ///
    /// Returns the inner writer after all data is flushed.
    pub async fn finish(mut self) -> Result<W, SnapshotError> {
        // Shutdown the encoder to finalize the zstd frame.
        // This writes the end-of-frame marker and flushes all buffered
        // compressed data to the underlying HashingWriter.
        // NOTE: `into_inner()` alone does NOT finalize — it returns the
        // inner writer with the frame potentially incomplete.
        self.encoder.shutdown().await.context(IoSnafu)?;

        // Now extract the inner writer (frame is complete)
        let hashing_writer = self.encoder.into_inner();
        // Finalize the SHA-256 digest
        let (mut inner, checksum) = hashing_writer.finalize();
        // Write checksum directly to the file (outside the compressed stream)
        inner.write_all(&checksum).await.context(IoSnafu)?;
        inner.flush().await.context(IoSnafu)?;
        Ok(inner)
    }
}

// ============================================================================
// SnapshotReader — reads and validates the sequential file format
// ============================================================================

/// Reads a snapshot from a file, verifying the SHA-256 checksum before
/// decompression.
pub struct SnapshotReader;

impl SnapshotReader {
    /// Verifies the outer SHA-256 checksum of a snapshot file.
    ///
    /// Reads all compressed bytes (everything before the trailing 32-byte
    /// checksum), computes SHA-256, and compares against the footer.
    /// Rejects corrupt files **before** any decompression.
    pub async fn verify_checksum<R: AsyncRead + Unpin>(
        reader: &mut R,
        total_file_size: u64,
    ) -> Result<(), SnapshotError> {
        if total_file_size < CHECKSUM_SIZE as u64 {
            return Err(SnapshotError::Truncated {
                reason: format!(
                    "File too small for checksum: {} bytes (minimum {})",
                    total_file_size, CHECKSUM_SIZE
                ),
            });
        }

        let compressed_size = total_file_size - CHECKSUM_SIZE as u64;

        // Hash all compressed bytes
        let mut hasher = Sha256::new();
        let mut remaining = compressed_size;
        let mut buf = vec![0u8; 64 * 1024]; // 64 KB read buffer

        while remaining > 0 {
            let to_read = (remaining as usize).min(buf.len());
            let n = reader.read(&mut buf[..to_read]).await.context(IoSnafu)?;
            if n == 0 {
                return Err(SnapshotError::Truncated {
                    reason: format!(
                        "Unexpected EOF during checksum verification: read {} of {} compressed bytes",
                        compressed_size - remaining,
                        compressed_size
                    ),
                });
            }
            hasher.update(&buf[..n]);
            remaining -= n as u64;
        }

        // Read the trailing checksum
        let mut expected_checksum = [0u8; CHECKSUM_SIZE];
        reader.read_exact(&mut expected_checksum).await.context(IoSnafu)?;

        let actual_checksum: [u8; 32] = hasher.finalize().into();

        if actual_checksum != expected_checksum {
            return Err(SnapshotError::ChecksumMismatch {
                expected: hex_encode(&expected_checksum),
                actual: hex_encode(&actual_checksum),
            });
        }

        Ok(())
    }

    /// Reads and validates the snapshot header from a decompressed stream.
    ///
    /// Returns the parsed [`SnapshotScope`] together with the
    /// `AppliedStateCore` bytes.
    pub async fn read_header<R: AsyncRead + Unpin>(
        reader: &mut R,
    ) -> Result<(SnapshotScope, Vec<u8>), SnapshotError> {
        // Magic
        let mut magic = [0u8; 4];
        read_exact_or_truncated(reader, &mut magic, "magic bytes").await?;
        if &magic != SNAPSHOT_MAGIC {
            return Err(SnapshotError::BadMagic { found: magic });
        }

        // Version
        let mut version_buf = [0u8; 1];
        reader.read_exact(&mut version_buf).await.context(IoSnafu)?;
        let version = version_buf[0];
        if version != SNAPSHOT_VERSION {
            return Err(SnapshotError::UnsupportedVersion { version });
        }

        // Flags (scope marker)
        let mut flags_buf = [0u8; 1];
        reader.read_exact(&mut flags_buf).await.context(IoSnafu)?;
        let flags = flags_buf[0];
        if flags & !FLAG_SCOPE_VAULT != 0 {
            return Err(SnapshotError::InvalidEntry {
                reason: format!("unknown flag bits set in snapshot header: 0x{flags:02X}"),
            });
        }
        let scope_is_vault = flags & FLAG_SCOPE_VAULT != 0;

        // Core length
        let mut core_len_buf = [0u8; 4];
        reader.read_exact(&mut core_len_buf).await.context(IoSnafu)?;
        let core_len = u32::from_le_bytes(core_len_buf) as usize;

        // Core bytes
        let mut core_bytes = vec![0u8; core_len];
        read_exact_or_truncated(reader, &mut core_bytes, "AppliedStateCore").await?;

        // Scope coordinates (vault snapshots only).
        let scope = if scope_is_vault {
            let mut org_buf = [0u8; 8];
            read_exact_or_truncated(reader, &mut org_buf, "vault organization_id").await?;
            let mut vault_buf = [0u8; 8];
            read_exact_or_truncated(reader, &mut vault_buf, "vault vault_id").await?;
            SnapshotScope::Vault {
                organization_id: inferadb_ledger_types::OrganizationId::new(i64::from_be_bytes(
                    org_buf,
                )),
                vault_id: inferadb_ledger_types::VaultId::new(i64::from_be_bytes(vault_buf)),
            }
        } else {
            // For org snapshots the organization_id lives inside the core
            // bytes (`AppliedStateCore` carries it). The reader returns a
            // placeholder here; callers that need the org id decode the
            // core themselves.
            SnapshotScope::Org { organization_id: inferadb_ledger_types::OrganizationId::new(0) }
        };

        Ok((scope, core_bytes))
    }

    /// Reads the table-block count from the decompressed stream.
    pub async fn read_table_block_count<R: AsyncRead + Unpin>(
        reader: &mut R,
    ) -> Result<u32, SnapshotError> {
        let mut buf = [0u8; 4];
        reader.read_exact(&mut buf).await.context(IoSnafu)?;
        Ok(u32::from_le_bytes(buf))
    }

    /// Reads a typed table-block header (kind tag + table_count).
    pub async fn read_table_block_header<R: AsyncRead + Unpin>(
        reader: &mut R,
    ) -> Result<(TableBlockKind, u32), SnapshotError> {
        let mut kind_buf = [0u8; 1];
        reader.read_exact(&mut kind_buf).await.context(IoSnafu)?;
        let kind = TableBlockKind::from_u8(kind_buf[0])?;
        let mut count_buf = [0u8; 4];
        reader.read_exact(&mut count_buf).await.context(IoSnafu)?;
        Ok((kind, u32::from_le_bytes(count_buf)))
    }

    /// Reads a table section header (table_id, entry_count).
    pub async fn read_table_header<R: AsyncRead + Unpin>(
        reader: &mut R,
    ) -> Result<(u8, u32), SnapshotError> {
        let mut table_id_buf = [0u8; 1];
        reader.read_exact(&mut table_id_buf).await.context(IoSnafu)?;
        let mut count_buf = [0u8; 4];
        reader.read_exact(&mut count_buf).await.context(IoSnafu)?;
        Ok((table_id_buf[0], u32::from_le_bytes(count_buf)))
    }

    /// Reads a single key-value entry (key_len, key, value_len, value).
    pub async fn read_kv_entry<R: AsyncRead + Unpin>(
        reader: &mut R,
    ) -> Result<(Vec<u8>, Vec<u8>), SnapshotError> {
        let mut len_buf = [0u8; 4];

        // key_len
        read_exact_or_truncated(reader, &mut len_buf, "key_len").await?;
        let key_len = u32::from_le_bytes(len_buf) as usize;

        // Reject impossibly large keys to avoid OOM
        if key_len > 64 * 1024 * 1024 {
            return Err(SnapshotError::InvalidEntry {
                reason: format!("key_len={key_len} exceeds 64 MiB limit"),
            });
        }

        let mut key = vec![0u8; key_len];
        read_exact_or_truncated(reader, &mut key, "key").await?;

        // value_len
        read_exact_or_truncated(reader, &mut len_buf, "value_len").await?;
        let value_len = u32::from_le_bytes(len_buf) as usize;

        // Reject impossibly large values
        if value_len > 256 * 1024 * 1024 {
            return Err(SnapshotError::InvalidEntry {
                reason: format!("value_len={value_len} exceeds 256 MiB limit"),
            });
        }

        let mut value = vec![0u8; value_len];
        read_exact_or_truncated(reader, &mut value, "value").await?;

        Ok((key, value))
    }

    /// Reads the event_count (u64le) from the decompressed stream.
    pub async fn read_event_count<R: AsyncRead + Unpin>(
        reader: &mut R,
    ) -> Result<u64, SnapshotError> {
        let mut buf = [0u8; 8];
        reader.read_exact(&mut buf).await.context(IoSnafu)?;
        Ok(u64::from_le_bytes(buf))
    }

    /// Reads a single event entry (entry_len, entry_bytes).
    pub async fn read_event_entry<R: AsyncRead + Unpin>(
        reader: &mut R,
    ) -> Result<Vec<u8>, SnapshotError> {
        let mut len_buf = [0u8; 4];
        read_exact_or_truncated(reader, &mut len_buf, "event entry_len").await?;
        let entry_len = u32::from_le_bytes(len_buf) as usize;

        if entry_len > 64 * 1024 * 1024 {
            return Err(SnapshotError::InvalidEntry {
                reason: format!("event entry_len={entry_len} exceeds 64 MiB limit"),
            });
        }

        let mut entry = vec![0u8; entry_len];
        read_exact_or_truncated(reader, &mut entry, "event entry").await?;

        Ok(entry)
    }

    /// Creates a zstd decompression reader from a buffered reader.
    pub fn decompressor<R: AsyncRead + Unpin>(reader: BufReader<R>) -> ZstdDecoder<BufReader<R>> {
        ZstdDecoder::new(reader)
    }
}

// ============================================================================
// Synchronous snapshot reader for install_snapshot streaming
// ============================================================================

/// Synchronous counterpart of [`SnapshotReader`] for use inside
/// `tokio::task::block_in_place()` during snapshot installation.
///
/// Uses `std::io::Read` instead of `AsyncRead`, enabling direct streaming from
/// a `zstd::Decoder` into a `WriteTransaction` without staging buffers.
pub struct SyncSnapshotReader;

/// Reads exactly `buf.len()` bytes from a synchronous reader, returning a
/// `SnapshotError::Truncated` if the reader reaches EOF before filling `buf`.
fn sync_read_exact_or_truncated<R: std::io::Read>(
    reader: &mut R,
    buf: &mut [u8],
    context: &str,
) -> Result<(), SnapshotError> {
    reader.read_exact(buf).map_err(|e| {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            SnapshotError::Truncated {
                reason: format!("Unexpected EOF reading {context} ({} bytes)", buf.len()),
            }
        } else {
            SnapshotError::Io { source: e, location: snafu::location!() }
        }
    })
}

impl SyncSnapshotReader {
    /// Reads and validates the snapshot header. Returns the parsed
    /// [`SnapshotScope`] together with the `AppliedStateCore` bytes.
    pub fn read_header<R: std::io::Read>(
        reader: &mut R,
    ) -> Result<(SnapshotScope, Vec<u8>), SnapshotError> {
        let mut magic = [0u8; 4];
        sync_read_exact_or_truncated(reader, &mut magic, "magic bytes")?;
        if &magic != SNAPSHOT_MAGIC {
            return Err(SnapshotError::BadMagic { found: magic });
        }

        let mut version_buf = [0u8; 1];
        reader
            .read_exact(&mut version_buf)
            .map_err(|e| SnapshotError::Io { source: e, location: snafu::location!() })?;
        if version_buf[0] != SNAPSHOT_VERSION {
            return Err(SnapshotError::UnsupportedVersion { version: version_buf[0] });
        }

        let mut flags_buf = [0u8; 1];
        reader
            .read_exact(&mut flags_buf)
            .map_err(|e| SnapshotError::Io { source: e, location: snafu::location!() })?;
        let flags = flags_buf[0];
        if flags & !FLAG_SCOPE_VAULT != 0 {
            return Err(SnapshotError::InvalidEntry {
                reason: format!("unknown flag bits set in snapshot header: 0x{flags:02X}"),
            });
        }
        let scope_is_vault = flags & FLAG_SCOPE_VAULT != 0;

        let mut core_len_buf = [0u8; 4];
        reader
            .read_exact(&mut core_len_buf)
            .map_err(|e| SnapshotError::Io { source: e, location: snafu::location!() })?;
        let core_len = u32::from_le_bytes(core_len_buf) as usize;

        let mut core_bytes = vec![0u8; core_len];
        sync_read_exact_or_truncated(reader, &mut core_bytes, "AppliedStateCore")?;

        let scope = if scope_is_vault {
            let mut org_buf = [0u8; 8];
            sync_read_exact_or_truncated(reader, &mut org_buf, "vault organization_id")?;
            let mut vault_buf = [0u8; 8];
            sync_read_exact_or_truncated(reader, &mut vault_buf, "vault vault_id")?;
            SnapshotScope::Vault {
                organization_id: inferadb_ledger_types::OrganizationId::new(i64::from_be_bytes(
                    org_buf,
                )),
                vault_id: inferadb_ledger_types::VaultId::new(i64::from_be_bytes(vault_buf)),
            }
        } else {
            SnapshotScope::Org { organization_id: inferadb_ledger_types::OrganizationId::new(0) }
        };

        Ok((scope, core_bytes))
    }

    /// Reads a typed table-block header (kind tag + table_count).
    pub fn read_table_block_header<R: std::io::Read>(
        reader: &mut R,
    ) -> Result<(TableBlockKind, u32), SnapshotError> {
        let mut kind_buf = [0u8; 1];
        reader
            .read_exact(&mut kind_buf)
            .map_err(|e| SnapshotError::Io { source: e, location: snafu::location!() })?;
        let kind = TableBlockKind::from_u8(kind_buf[0])?;
        let count = Self::read_u32(reader)?;
        Ok((kind, count))
    }

    /// Reads a u32le from the stream.
    pub fn read_u32<R: std::io::Read>(reader: &mut R) -> Result<u32, SnapshotError> {
        let mut buf = [0u8; 4];
        reader
            .read_exact(&mut buf)
            .map_err(|e| SnapshotError::Io { source: e, location: snafu::location!() })?;
        Ok(u32::from_le_bytes(buf))
    }

    /// Reads a u64le from the stream.
    pub fn read_u64<R: std::io::Read>(reader: &mut R) -> Result<u64, SnapshotError> {
        let mut buf = [0u8; 8];
        reader
            .read_exact(&mut buf)
            .map_err(|e| SnapshotError::Io { source: e, location: snafu::location!() })?;
        Ok(u64::from_le_bytes(buf))
    }

    /// Reads a table section header (table_id, entry_count).
    pub fn read_table_header<R: std::io::Read>(reader: &mut R) -> Result<(u8, u32), SnapshotError> {
        let mut table_id_buf = [0u8; 1];
        reader
            .read_exact(&mut table_id_buf)
            .map_err(|e| SnapshotError::Io { source: e, location: snafu::location!() })?;
        let count = Self::read_u32(reader)?;
        Ok((table_id_buf[0], count))
    }

    /// Reads a single key-value entry with OOM guards.
    pub fn read_kv_entry<R: std::io::Read>(
        reader: &mut R,
    ) -> Result<(Vec<u8>, Vec<u8>), SnapshotError> {
        let mut len_buf = [0u8; 4];

        sync_read_exact_or_truncated(reader, &mut len_buf, "key_len")?;
        let key_len = u32::from_le_bytes(len_buf) as usize;
        if key_len > 64 * 1024 * 1024 {
            return Err(SnapshotError::InvalidEntry {
                reason: format!("key_len={key_len} exceeds 64 MiB limit"),
            });
        }

        let mut key = vec![0u8; key_len];
        sync_read_exact_or_truncated(reader, &mut key, "key")?;

        sync_read_exact_or_truncated(reader, &mut len_buf, "value_len")?;
        let value_len = u32::from_le_bytes(len_buf) as usize;
        if value_len > 256 * 1024 * 1024 {
            return Err(SnapshotError::InvalidEntry {
                reason: format!("value_len={value_len} exceeds 256 MiB limit"),
            });
        }

        let mut value = vec![0u8; value_len];
        sync_read_exact_or_truncated(reader, &mut value, "value")?;

        Ok((key, value))
    }

    /// Reads a single event entry (entry_len, entry_bytes).
    pub fn read_event_entry<R: std::io::Read>(reader: &mut R) -> Result<Vec<u8>, SnapshotError> {
        let mut len_buf = [0u8; 4];
        sync_read_exact_or_truncated(reader, &mut len_buf, "event entry_len")?;
        let entry_len = u32::from_le_bytes(len_buf) as usize;

        if entry_len > 64 * 1024 * 1024 {
            return Err(SnapshotError::InvalidEntry {
                reason: format!("event entry_len={entry_len} exceeds 64 MiB limit"),
            });
        }

        let mut entry = vec![0u8; entry_len];
        sync_read_exact_or_truncated(reader, &mut entry, "event entry")?;

        Ok(entry)
    }
}

// ============================================================================
// Table ID validation
// ============================================================================

/// Tables snapshotted from an org-level `raft.db` (`OrganizationGroup` shard).
///
/// Captures only org-scoped control-plane state — entity / block / per-vault
/// data is excluded because it lives on per-vault databases (root rule 17).
pub const SNAPSHOT_TABLE_IDS_ORG_RAFT: &[u8] = &[
    7,  // RaftState — AppliedStateCore + vote
    9,  // OrganizationMeta
    10, // Sequences
    13, // OrganizationSlugIndex
    14, // VaultSlugIndex
    18, // UserSlugIndex
    19, // TeamSlugIndex
    20, // AppSlugIndex
];

/// Tables snapshotted from a per-vault `raft.db` (`VaultGroup` shard).
///
/// Captures the vault's Raft state plus the per-vault metadata that the apply
/// pipeline materialises into the vault shard's own raft.db.
pub const SNAPSHOT_TABLE_IDS_VAULT_RAFT: &[u8] = &[
    7,  // RaftState — AppliedStateCore + vote (per-shard sentinel)
    8,  // VaultMeta
    11, // ClientSequences
    15, // VaultHeights
    16, // VaultHashes
    17, // VaultHealth
];

/// Tables snapshotted from a per-vault `state.db`.
///
/// All entity / relationship / per-vault dictionary data lives here under
/// the per-vault state-DB residency contract (`crates/state/CLAUDE.md` rule
/// 11).
pub const SNAPSHOT_TABLE_IDS_VAULT_STATE: &[u8] = &[
    0,  // Entities
    1,  // Relationships
    2,  // ObjIndex
    3,  // SubjIndex
    21, // StringDictionary
    22, // StringDictionaryReverse
];

/// Tables snapshotted from a per-vault `blocks.db`.
///
/// Each vault's append-only block archive is independent; the vault's
/// `BlockArchive` owns this database directly.
pub const SNAPSHOT_TABLE_IDS_VAULT_BLOCKS: &[u8] = &[
    4, // Blocks
    5, // VaultBlockIndex
];

/// Validates that a table_id is a known table.
pub fn validate_table_id(table_id: u8) -> Result<(), SnapshotError> {
    use inferadb_ledger_store::tables::TableId;
    if TableId::from_u8(table_id).is_none() {
        return Err(SnapshotError::UnknownTableId { table_id });
    }
    Ok(())
}

/// Returns the table-id whitelist that belongs in the given block kind.
///
/// Used by the install path to assert that a snapshot's table block contains
/// only tables that semantically belong on the corresponding source DB.
pub fn allowed_tables_for_block(kind: TableBlockKind) -> &'static [u8] {
    match kind {
        TableBlockKind::OrgRaft => SNAPSHOT_TABLE_IDS_ORG_RAFT,
        TableBlockKind::VaultRaft => SNAPSHOT_TABLE_IDS_VAULT_RAFT,
        TableBlockKind::VaultState => SNAPSHOT_TABLE_IDS_VAULT_STATE,
        TableBlockKind::VaultBlocks => SNAPSHOT_TABLE_IDS_VAULT_BLOCKS,
    }
}

// ============================================================================
// Helpers
// ============================================================================

/// Reads exactly `buf.len()` bytes, converting EOF to a `Truncated` error.
async fn read_exact_or_truncated<R: AsyncRead + Unpin>(
    reader: &mut R,
    buf: &mut [u8],
    context: &str,
) -> Result<(), SnapshotError> {
    reader.read_exact(buf).await.map(|_| ()).map_err(|e| {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            SnapshotError::Truncated {
                reason: format!("Truncated reading {context} ({} bytes expected)", buf.len()),
            }
        } else {
            SnapshotError::Io { source: e, location: snafu::location!() }
        }
    })
}

// ============================================================================
// Incremental snapshot format
// ============================================================================

/// Incremental snapshot magic bytes (distinguishes from full snapshots).
pub const INCREMENTAL_MAGIC: &[u8; 4] = b"LSNI";

/// Incremental snapshot format version.
pub const SNAPSHOT_TYPE_INCREMENTAL: u8 = 1;

/// Maximum allowed page data size in an incremental snapshot (16 MiB).
///
/// Pages larger than this are rejected during read to prevent OOM.
const INCREMENTAL_MAX_PAGE_SIZE: u32 = 16 * 1024 * 1024;

/// Writes an incremental (delta) snapshot containing only pages modified
/// since a base generation.
///
/// The file format (all inside a zstd-compressed stream):
///
/// ```text
/// [magic: 4 bytes "LSNI"]
/// [version: 1 byte]
/// [base_generation: u64 LE]
/// [current_generation: u64 LE]
/// [page_count: u64 LE]
/// For each page:
///   [page_id: u64 LE]
///   [page_data_len: u32 LE]
///   [page_data: variable]
/// ```
///
/// After the compressed stream, a 32-byte SHA-256 checksum is appended
/// (same scheme as the full snapshot format).
pub struct IncrementalSnapshotWriter<W: AsyncWrite + Unpin> {
    encoder: ZstdEncoder<HashingWriter<W>>,
}

impl<W: AsyncWrite + Unpin> IncrementalSnapshotWriter<W> {
    /// Creates a new incremental snapshot writer wrapping the given writer.
    pub fn new(writer: W) -> Self {
        let hashing = HashingWriter::new(writer);
        let encoder =
            ZstdEncoder::with_quality(hashing, async_compression::Level::Precise(ZSTD_LEVEL));
        Self { encoder }
    }

    /// Writes the incremental snapshot header.
    pub async fn write_header(
        &mut self,
        base_generation: u64,
        current_generation: u64,
    ) -> Result<(), SnapshotError> {
        self.encoder.write_all(INCREMENTAL_MAGIC).await.context(IoSnafu)?;
        self.encoder.write_all(&[SNAPSHOT_TYPE_INCREMENTAL]).await.context(IoSnafu)?;
        self.encoder.write_all(&base_generation.to_le_bytes()).await.context(IoSnafu)?;
        self.encoder.write_all(&current_generation.to_le_bytes()).await.context(IoSnafu)?;
        Ok(())
    }

    /// Writes the page count.
    pub async fn write_page_count(&mut self, count: u64) -> Result<(), SnapshotError> {
        self.encoder.write_all(&count.to_le_bytes()).await.context(IoSnafu)?;
        Ok(())
    }

    /// Writes a single page entry (page_id + raw page data).
    pub async fn write_page(
        &mut self,
        page_id: u64,
        page_data: &[u8],
    ) -> Result<(), SnapshotError> {
        let data_len = u32::try_from(page_data.len()).map_err(|_| SnapshotError::InvalidEntry {
            reason: format!("Page data too large: {} bytes", page_data.len()),
        })?;
        self.encoder.write_all(&page_id.to_le_bytes()).await.context(IoSnafu)?;
        self.encoder.write_all(&data_len.to_le_bytes()).await.context(IoSnafu)?;
        self.encoder.write_all(page_data).await.context(IoSnafu)?;
        Ok(())
    }

    /// Finalizes the zstd stream and writes the SHA-256 checksum footer.
    pub async fn finish(mut self) -> Result<W, SnapshotError> {
        self.encoder.shutdown().await.context(IoSnafu)?;
        let hashing_writer = self.encoder.into_inner();
        let (mut inner, checksum) = hashing_writer.finalize();
        inner.write_all(&checksum).await.context(IoSnafu)?;
        inner.flush().await.context(IoSnafu)?;
        Ok(inner)
    }
}

/// Header data parsed from an incremental snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncrementalSnapshotHeader {
    /// The generation the delta is computed relative to.
    pub base_generation: u64,
    /// The generation this snapshot captures up to.
    pub current_generation: u64,
}

/// Reads an incremental (delta) snapshot, verifying integrity.
pub struct IncrementalSnapshotReader;

impl IncrementalSnapshotReader {
    /// Reads and validates the incremental snapshot header from a
    /// decompressed stream. Returns the header metadata.
    pub async fn read_header<R: AsyncRead + Unpin>(
        reader: &mut R,
    ) -> Result<IncrementalSnapshotHeader, SnapshotError> {
        let mut magic = [0u8; 4];
        read_exact_or_truncated(reader, &mut magic, "incremental magic").await?;
        if &magic != INCREMENTAL_MAGIC {
            return Err(SnapshotError::BadMagic { found: magic });
        }

        let mut version_buf = [0u8; 1];
        reader.read_exact(&mut version_buf).await.context(IoSnafu)?;
        if version_buf[0] > SNAPSHOT_TYPE_INCREMENTAL {
            return Err(SnapshotError::UnsupportedVersion { version: version_buf[0] });
        }

        let mut buf = [0u8; 8];
        reader.read_exact(&mut buf).await.context(IoSnafu)?;
        let base_generation = u64::from_le_bytes(buf);

        reader.read_exact(&mut buf).await.context(IoSnafu)?;
        let current_generation = u64::from_le_bytes(buf);

        Ok(IncrementalSnapshotHeader { base_generation, current_generation })
    }

    /// Reads the page count (u64 LE) from the decompressed stream.
    pub async fn read_page_count<R: AsyncRead + Unpin>(
        reader: &mut R,
    ) -> Result<u64, SnapshotError> {
        let mut buf = [0u8; 8];
        reader.read_exact(&mut buf).await.context(IoSnafu)?;
        Ok(u64::from_le_bytes(buf))
    }

    /// Reads a single page entry (page_id, page_data).
    pub async fn read_page<R: AsyncRead + Unpin>(
        reader: &mut R,
    ) -> Result<(u64, Vec<u8>), SnapshotError> {
        let mut id_buf = [0u8; 8];
        read_exact_or_truncated(reader, &mut id_buf, "page_id").await?;
        let page_id = u64::from_le_bytes(id_buf);

        let mut len_buf = [0u8; 4];
        read_exact_or_truncated(reader, &mut len_buf, "page_data_len").await?;
        let data_len = u32::from_le_bytes(len_buf);

        if data_len > INCREMENTAL_MAX_PAGE_SIZE {
            return Err(SnapshotError::InvalidEntry {
                reason: format!(
                    "page_data_len={data_len} exceeds {} byte limit",
                    INCREMENTAL_MAX_PAGE_SIZE
                ),
            });
        }

        let mut data = vec![0u8; data_len as usize];
        read_exact_or_truncated(reader, &mut data, "page_data").await?;

        Ok((page_id, data))
    }

    /// Creates a zstd decompression reader from a buffered reader.
    pub fn decompressor<R: AsyncRead + Unpin>(reader: BufReader<R>) -> ZstdDecoder<BufReader<R>> {
        ZstdDecoder::new(reader)
    }
}

/// Synchronous reader for incremental snapshots, for use inside
/// `tokio::task::block_in_place()` during snapshot installation.
pub struct SyncIncrementalSnapshotReader;

impl SyncIncrementalSnapshotReader {
    /// Reads and validates the incremental snapshot header.
    pub fn read_header<R: std::io::Read>(
        reader: &mut R,
    ) -> Result<IncrementalSnapshotHeader, SnapshotError> {
        let mut magic = [0u8; 4];
        sync_read_exact_or_truncated(reader, &mut magic, "incremental magic")?;
        if &magic != INCREMENTAL_MAGIC {
            return Err(SnapshotError::BadMagic { found: magic });
        }

        let mut version_buf = [0u8; 1];
        reader
            .read_exact(&mut version_buf)
            .map_err(|e| SnapshotError::Io { source: e, location: snafu::location!() })?;
        if version_buf[0] > SNAPSHOT_TYPE_INCREMENTAL {
            return Err(SnapshotError::UnsupportedVersion { version: version_buf[0] });
        }

        let mut buf = [0u8; 8];
        reader
            .read_exact(&mut buf)
            .map_err(|e| SnapshotError::Io { source: e, location: snafu::location!() })?;
        let base_generation = u64::from_le_bytes(buf);

        reader
            .read_exact(&mut buf)
            .map_err(|e| SnapshotError::Io { source: e, location: snafu::location!() })?;
        let current_generation = u64::from_le_bytes(buf);

        Ok(IncrementalSnapshotHeader { base_generation, current_generation })
    }

    /// Reads a u64 LE from the stream.
    pub fn read_u64<R: std::io::Read>(reader: &mut R) -> Result<u64, SnapshotError> {
        let mut buf = [0u8; 8];
        reader
            .read_exact(&mut buf)
            .map_err(|e| SnapshotError::Io { source: e, location: snafu::location!() })?;
        Ok(u64::from_le_bytes(buf))
    }

    /// Reads a single page entry (page_id, page_data).
    pub fn read_page<R: std::io::Read>(reader: &mut R) -> Result<(u64, Vec<u8>), SnapshotError> {
        let mut id_buf = [0u8; 8];
        sync_read_exact_or_truncated(reader, &mut id_buf, "page_id")?;
        let page_id = u64::from_le_bytes(id_buf);

        let mut len_buf = [0u8; 4];
        sync_read_exact_or_truncated(reader, &mut len_buf, "page_data_len")?;
        let data_len = u32::from_le_bytes(len_buf);

        if data_len > INCREMENTAL_MAX_PAGE_SIZE {
            return Err(SnapshotError::InvalidEntry {
                reason: format!(
                    "page_data_len={data_len} exceeds {} byte limit",
                    INCREMENTAL_MAX_PAGE_SIZE
                ),
            });
        }

        let mut data = vec![0u8; data_len as usize];
        sync_read_exact_or_truncated(reader, &mut data, "page_data")?;

        Ok((page_id, data))
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    inferadb_ledger_types::bytes_to_hex(bytes)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods, clippy::panic)]
mod tests {
    use inferadb_ledger_types::{OrganizationId, VaultId};
    use tokio::io::AsyncSeekExt;

    use super::*;

    /// Helper: builds an `Org` snapshot scope with a fixed organization id.
    fn org_scope() -> SnapshotScope {
        SnapshotScope::Org { organization_id: OrganizationId::new(7) }
    }

    /// Helper: builds a `Vault` snapshot scope with fixed (org, vault) ids.
    fn vault_scope() -> SnapshotScope {
        SnapshotScope::Vault { organization_id: OrganizationId::new(7), vault_id: VaultId::new(42) }
    }

    /// Round-trip test (org scope): write a snapshot with one table block
    /// and an event section, read it back, verify all sections.
    #[tokio::test]
    async fn test_snapshot_round_trip_org() {
        let core_data = b"test-core-data";
        type TableSection = Vec<(u8, Vec<(Vec<u8>, Vec<u8>)>)>;
        let table_entries: TableSection = vec![
            (7, vec![(b"org1".to_vec(), b"meta1".to_vec())]),
            (9, vec![(b"seq1".to_vec(), b"val1".to_vec()), (b"seq2".to_vec(), b"val2".to_vec())]),
        ];
        let events: Vec<Vec<u8>> = vec![b"event_bytes_1".to_vec()];

        let file = tokio::fs::File::from_std(tempfile::tempfile().unwrap());
        let mut writer = SnapshotWriter::new(file);

        writer.write_header(org_scope(), core_data).await.unwrap();

        // 1 table block (org_raft) with 2 tables
        writer.write_table_block_count(1).await.unwrap();
        writer
            .write_table_block_header(TableBlockKind::OrgRaft, table_entries.len() as u32)
            .await
            .unwrap();
        for (table_id, entries) in &table_entries {
            writer.write_table_header(*table_id, entries.len() as u32).await.unwrap();
            for (key, value) in entries {
                writer.write_table_entry(key, value).await.unwrap();
            }
        }

        writer.write_event_count(events.len() as u64).await.unwrap();
        for event in &events {
            writer.write_event_entry(event).await.unwrap();
        }

        let mut file = writer.finish().await.unwrap();

        let file_size = file.seek(std::io::SeekFrom::End(0)).await.unwrap();
        file.seek(std::io::SeekFrom::Start(0)).await.unwrap();
        SnapshotReader::verify_checksum(&mut file, file_size).await.unwrap();

        file.seek(std::io::SeekFrom::Start(0)).await.unwrap();
        let buf_reader = BufReader::new(file.take(file_size - CHECKSUM_SIZE as u64));
        let mut decoder = SnapshotReader::decompressor(buf_reader);

        let (scope, read_core) = SnapshotReader::read_header(&mut decoder).await.unwrap();
        assert_eq!(read_core, core_data);
        assert!(matches!(scope, SnapshotScope::Org { .. }));

        let block_count = SnapshotReader::read_table_block_count(&mut decoder).await.unwrap();
        assert_eq!(block_count, 1);

        let (kind, table_count) =
            SnapshotReader::read_table_block_header(&mut decoder).await.unwrap();
        assert_eq!(kind, TableBlockKind::OrgRaft);
        assert_eq!(table_count, 2);

        for (expected_id, expected_entries) in &table_entries {
            let (tid, count) = SnapshotReader::read_table_header(&mut decoder).await.unwrap();
            assert_eq!(tid, *expected_id);
            assert_eq!(count, expected_entries.len() as u32);
            for expected in expected_entries {
                let (key, value) = SnapshotReader::read_kv_entry(&mut decoder).await.unwrap();
                assert_eq!(key, expected.0);
                assert_eq!(value, expected.1);
            }
        }

        let event_count = SnapshotReader::read_event_count(&mut decoder).await.unwrap();
        assert_eq!(event_count, 1);
        let event = SnapshotReader::read_event_entry(&mut decoder).await.unwrap();
        assert_eq!(event, b"event_bytes_1");
    }

    /// Round-trip test (vault scope): write all four block kinds, read them back.
    #[tokio::test]
    async fn test_snapshot_round_trip_vault() {
        let core_data = b"vault-core";

        let file = tokio::fs::File::from_std(tempfile::tempfile().unwrap());
        let mut writer = SnapshotWriter::new(file);

        writer.write_header(vault_scope(), core_data).await.unwrap();

        // 3 typed table blocks: VaultRaft, VaultState, VaultBlocks
        writer.write_table_block_count(3).await.unwrap();

        // VaultRaft: VaultMeta (id 8) with one entry
        writer.write_table_block_header(TableBlockKind::VaultRaft, 1).await.unwrap();
        writer.write_table_header(8, 1).await.unwrap();
        writer.write_table_entry(b"vmeta_k", b"vmeta_v").await.unwrap();

        // VaultState: Entities (id 0) with one entry
        writer.write_table_block_header(TableBlockKind::VaultState, 1).await.unwrap();
        writer.write_table_header(0, 1).await.unwrap();
        writer.write_table_entry(b"ent_k", b"ent_v").await.unwrap();

        // VaultBlocks: Blocks (id 4) with one entry
        writer.write_table_block_header(TableBlockKind::VaultBlocks, 1).await.unwrap();
        writer.write_table_header(4, 1).await.unwrap();
        writer.write_table_entry(b"blk_k", b"blk_v").await.unwrap();

        writer.write_event_count(0).await.unwrap();

        let mut file = writer.finish().await.unwrap();

        let file_size = file.seek(std::io::SeekFrom::End(0)).await.unwrap();
        file.seek(std::io::SeekFrom::Start(0)).await.unwrap();
        SnapshotReader::verify_checksum(&mut file, file_size).await.unwrap();

        file.seek(std::io::SeekFrom::Start(0)).await.unwrap();
        let buf_reader = BufReader::new(file.take(file_size - CHECKSUM_SIZE as u64));
        let mut decoder = SnapshotReader::decompressor(buf_reader);

        let (scope, _core) = SnapshotReader::read_header(&mut decoder).await.unwrap();
        match scope {
            SnapshotScope::Vault { organization_id, vault_id } => {
                assert_eq!(organization_id, OrganizationId::new(7));
                assert_eq!(vault_id, VaultId::new(42));
            },
            _ => panic!("expected vault scope"),
        }

        let block_count = SnapshotReader::read_table_block_count(&mut decoder).await.unwrap();
        assert_eq!(block_count, 3);

        let kinds =
            [TableBlockKind::VaultRaft, TableBlockKind::VaultState, TableBlockKind::VaultBlocks];
        let expected_table_ids = [8u8, 0u8, 4u8];
        for (i, expected_kind) in kinds.iter().enumerate() {
            let (kind, table_count) =
                SnapshotReader::read_table_block_header(&mut decoder).await.unwrap();
            assert_eq!(kind, *expected_kind);
            assert_eq!(table_count, 1);
            let (tid, ec) = SnapshotReader::read_table_header(&mut decoder).await.unwrap();
            assert_eq!(tid, expected_table_ids[i]);
            assert_eq!(ec, 1);
            let _ = SnapshotReader::read_kv_entry(&mut decoder).await.unwrap();
        }

        let event_count = SnapshotReader::read_event_count(&mut decoder).await.unwrap();
        assert_eq!(event_count, 0);
    }

    /// Empty snapshot (no tables, no events) round-trips.
    #[tokio::test]
    async fn test_snapshot_empty_round_trip() {
        let core_data = b"empty-core";

        let file = tokio::fs::File::from_std(tempfile::tempfile().unwrap());
        let mut writer = SnapshotWriter::new(file);
        writer.write_header(org_scope(), core_data).await.unwrap();
        writer.write_table_block_count(0).await.unwrap();
        writer.write_event_count(0).await.unwrap();
        let mut file = writer.finish().await.unwrap();

        let file_size = file.seek(std::io::SeekFrom::End(0)).await.unwrap();
        file.seek(std::io::SeekFrom::Start(0)).await.unwrap();
        SnapshotReader::verify_checksum(&mut file, file_size).await.unwrap();

        file.seek(std::io::SeekFrom::Start(0)).await.unwrap();
        let buf_reader = BufReader::new(file.take(file_size - CHECKSUM_SIZE as u64));
        let mut decoder = SnapshotReader::decompressor(buf_reader);

        let (_scope, read_core) = SnapshotReader::read_header(&mut decoder).await.unwrap();
        assert_eq!(read_core, core_data);
        assert_eq!(SnapshotReader::read_table_block_count(&mut decoder).await.unwrap(), 0);
        assert_eq!(SnapshotReader::read_event_count(&mut decoder).await.unwrap(), 0);
    }

    /// Bad magic is rejected.
    #[tokio::test]
    async fn test_snapshot_bad_magic_rejected() {
        let file = tokio::fs::File::from_std(tempfile::tempfile().unwrap());
        let hashing = HashingWriter::new(file);
        let mut encoder =
            ZstdEncoder::with_quality(hashing, async_compression::Level::Precise(ZSTD_LEVEL));

        encoder.write_all(b"BAAD").await.unwrap();
        encoder.write_all(&[SNAPSHOT_VERSION, 0]).await.unwrap();
        encoder.write_all(&0u32.to_le_bytes()).await.unwrap();
        encoder.shutdown().await.unwrap();

        let hashing = encoder.into_inner();
        let (mut file, checksum) = hashing.finalize();
        file.write_all(&checksum).await.unwrap();
        file.flush().await.unwrap();

        let file_size = file.seek(std::io::SeekFrom::End(0)).await.unwrap();
        file.seek(std::io::SeekFrom::Start(0)).await.unwrap();
        SnapshotReader::verify_checksum(&mut file, file_size).await.unwrap();

        file.seek(std::io::SeekFrom::Start(0)).await.unwrap();
        let buf_reader = BufReader::new(file.take(file_size - CHECKSUM_SIZE as u64));
        let mut decoder = SnapshotReader::decompressor(buf_reader);

        let result = SnapshotReader::read_header(&mut decoder).await;
        assert!(matches!(result, Err(SnapshotError::BadMagic { .. })));
    }

    /// Unsupported version is rejected.
    #[tokio::test]
    async fn test_snapshot_unsupported_version_rejected() {
        let file = tokio::fs::File::from_std(tempfile::tempfile().unwrap());
        let hashing = HashingWriter::new(file);
        let mut encoder =
            ZstdEncoder::with_quality(hashing, async_compression::Level::Precise(ZSTD_LEVEL));

        encoder.write_all(SNAPSHOT_MAGIC).await.unwrap();
        encoder.write_all(&[99u8]).await.unwrap(); // unknown future version
        encoder.write_all(&[0u8]).await.unwrap();
        encoder.write_all(&0u32.to_le_bytes()).await.unwrap();
        encoder.shutdown().await.unwrap();

        let hashing = encoder.into_inner();
        let (mut file, checksum) = hashing.finalize();
        file.write_all(&checksum).await.unwrap();
        file.flush().await.unwrap();

        let file_size = file.seek(std::io::SeekFrom::End(0)).await.unwrap();
        file.seek(std::io::SeekFrom::Start(0)).await.unwrap();
        SnapshotReader::verify_checksum(&mut file, file_size).await.unwrap();

        file.seek(std::io::SeekFrom::Start(0)).await.unwrap();
        let buf_reader = BufReader::new(file.take(file_size - CHECKSUM_SIZE as u64));
        let mut decoder = SnapshotReader::decompressor(buf_reader);

        let result = SnapshotReader::read_header(&mut decoder).await;
        assert!(matches!(result, Err(SnapshotError::UnsupportedVersion { version: 99 })));
    }

    /// V1 (pre-Stage-1b) snapshot files are no longer accepted.
    #[tokio::test]
    async fn test_snapshot_v1_rejected() {
        let file = tokio::fs::File::from_std(tempfile::tempfile().unwrap());
        let hashing = HashingWriter::new(file);
        let mut encoder =
            ZstdEncoder::with_quality(hashing, async_compression::Level::Precise(ZSTD_LEVEL));

        encoder.write_all(SNAPSHOT_MAGIC).await.unwrap();
        encoder.write_all(&[1u8]).await.unwrap(); // v1
        encoder.write_all(&[0u8]).await.unwrap();
        encoder.write_all(&0u32.to_le_bytes()).await.unwrap();
        encoder.shutdown().await.unwrap();

        let hashing = encoder.into_inner();
        let (mut file, checksum) = hashing.finalize();
        file.write_all(&checksum).await.unwrap();
        file.flush().await.unwrap();

        let file_size = file.seek(std::io::SeekFrom::End(0)).await.unwrap();
        file.seek(std::io::SeekFrom::Start(0)).await.unwrap();
        SnapshotReader::verify_checksum(&mut file, file_size).await.unwrap();

        file.seek(std::io::SeekFrom::Start(0)).await.unwrap();
        let buf_reader = BufReader::new(file.take(file_size - CHECKSUM_SIZE as u64));
        let mut decoder = SnapshotReader::decompressor(buf_reader);

        let result = SnapshotReader::read_header(&mut decoder).await;
        assert!(matches!(result, Err(SnapshotError::UnsupportedVersion { version: 1 })));
    }

    /// Truncated file is rejected at checksum verification.
    #[tokio::test]
    async fn test_snapshot_truncated_rejected() {
        let result = SnapshotReader::verify_checksum(&mut tokio::io::empty(), 10).await;
        assert!(matches!(result, Err(SnapshotError::Truncated { .. })));
    }

    /// Checksum mismatch is detected.
    #[tokio::test]
    async fn test_snapshot_checksum_mismatch_rejected() {
        let file = tokio::fs::File::from_std(tempfile::tempfile().unwrap());
        let mut writer = SnapshotWriter::new(file);
        writer.write_header(org_scope(), b"data").await.unwrap();
        writer.write_table_block_count(0).await.unwrap();
        writer.write_event_count(0).await.unwrap();
        let mut file = writer.finish().await.unwrap();

        let file_size = file.seek(std::io::SeekFrom::End(0)).await.unwrap();
        file.seek(std::io::SeekFrom::Start(5)).await.unwrap();
        file.write_all(&[0xFF]).await.unwrap();
        file.flush().await.unwrap();

        file.seek(std::io::SeekFrom::Start(0)).await.unwrap();
        let result = SnapshotReader::verify_checksum(&mut file, file_size).await;
        assert!(matches!(result, Err(SnapshotError::ChecksumMismatch { .. })));
    }

    /// Checksum correctness: independently verify the SHA-256 digest.
    #[tokio::test]
    async fn test_snapshot_checksum_correctness() {
        let file = tokio::fs::File::from_std(tempfile::tempfile().unwrap());
        let mut writer = SnapshotWriter::new(file);
        writer.write_header(org_scope(), b"known-data").await.unwrap();
        writer.write_table_block_count(0).await.unwrap();
        writer.write_event_count(0).await.unwrap();
        let mut file = writer.finish().await.unwrap();

        let file_size = file.seek(std::io::SeekFrom::End(0)).await.unwrap();

        file.seek(std::io::SeekFrom::Start(0)).await.unwrap();
        let mut all_bytes = vec![0u8; file_size as usize];
        file.read_exact(&mut all_bytes).await.unwrap();

        let compressed_bytes = &all_bytes[..all_bytes.len() - CHECKSUM_SIZE];
        let stored_checksum = &all_bytes[all_bytes.len() - CHECKSUM_SIZE..];

        let mut hasher = Sha256::new();
        hasher.update(compressed_bytes);
        let computed: [u8; 32] = hasher.finalize().into();

        assert_eq!(&computed[..], stored_checksum);
    }

    /// Unknown table ID is rejected.
    #[tokio::test]
    async fn test_snapshot_unknown_table_id() {
        let result = validate_table_id(255);
        assert!(matches!(result, Err(SnapshotError::UnknownTableId { table_id: 255 })));
    }

    /// Unknown table-block kind is rejected.
    #[tokio::test]
    async fn test_table_block_kind_unknown_rejected() {
        let result = TableBlockKind::from_u8(99);
        assert!(matches!(result, Err(SnapshotError::InvalidEntry { .. })));
    }

    /// `allowed_tables_for_block` returns the right whitelist per block kind.
    #[test]
    fn test_allowed_tables_per_block() {
        assert_eq!(allowed_tables_for_block(TableBlockKind::OrgRaft), SNAPSHOT_TABLE_IDS_ORG_RAFT);
        assert_eq!(
            allowed_tables_for_block(TableBlockKind::VaultRaft),
            SNAPSHOT_TABLE_IDS_VAULT_RAFT
        );
        assert_eq!(
            allowed_tables_for_block(TableBlockKind::VaultState),
            SNAPSHOT_TABLE_IDS_VAULT_STATE
        );
        assert_eq!(
            allowed_tables_for_block(TableBlockKind::VaultBlocks),
            SNAPSHOT_TABLE_IDS_VAULT_BLOCKS
        );
    }

    /// Vault-scope coordinates round-trip exactly through the on-disk header.
    #[tokio::test]
    async fn test_vault_scope_coords_round_trip() {
        let scope = SnapshotScope::Vault {
            organization_id: OrganizationId::new(0x1234_5678),
            vault_id: VaultId::new(-99),
        };

        let file = tokio::fs::File::from_std(tempfile::tempfile().unwrap());
        let mut writer = SnapshotWriter::new(file);
        writer.write_header(scope, b"core").await.unwrap();
        writer.write_table_block_count(0).await.unwrap();
        writer.write_event_count(0).await.unwrap();
        let mut file = writer.finish().await.unwrap();

        let file_size = file.seek(std::io::SeekFrom::End(0)).await.unwrap();
        file.seek(std::io::SeekFrom::Start(0)).await.unwrap();
        let buf_reader = BufReader::new(file.take(file_size - CHECKSUM_SIZE as u64));
        let mut decoder = SnapshotReader::decompressor(buf_reader);

        let (read_scope, _core) = SnapshotReader::read_header(&mut decoder).await.unwrap();
        match read_scope {
            SnapshotScope::Vault { organization_id, vault_id } => {
                assert_eq!(organization_id, OrganizationId::new(0x1234_5678));
                assert_eq!(vault_id, VaultId::new(-99));
            },
            _ => panic!("expected vault scope on read-back"),
        }
    }

    /// `SnapshotScope` accessors return the expected values for both arms.
    #[test]
    fn test_snapshot_scope_accessors() {
        let org = SnapshotScope::Org { organization_id: OrganizationId::new(3) };
        assert_eq!(org.organization_id(), OrganizationId::new(3));
        assert_eq!(org.vault_id(), None);
        assert!(!org.is_vault());
        assert_eq!(org.flags_byte(), 0);

        let vault = SnapshotScope::Vault {
            organization_id: OrganizationId::new(3),
            vault_id: VaultId::new(11),
        };
        assert_eq!(vault.organization_id(), OrganizationId::new(3));
        assert_eq!(vault.vault_id(), Some(VaultId::new(11)));
        assert!(vault.is_vault());
        assert_eq!(vault.flags_byte() & FLAG_SCOPE_VAULT, FLAG_SCOPE_VAULT);
    }

    /// Reserved flag bits in the header are rejected (forward-compat guard).
    #[tokio::test]
    async fn test_unknown_flag_bits_rejected() {
        let file = tokio::fs::File::from_std(tempfile::tempfile().unwrap());
        let hashing = HashingWriter::new(file);
        let mut encoder =
            ZstdEncoder::with_quality(hashing, async_compression::Level::Precise(ZSTD_LEVEL));

        encoder.write_all(SNAPSHOT_MAGIC).await.unwrap();
        encoder.write_all(&[SNAPSHOT_VERSION]).await.unwrap();
        encoder.write_all(&[0b1000_0000]).await.unwrap(); // bit 7 reserved
        encoder.write_all(&0u32.to_le_bytes()).await.unwrap();
        encoder.shutdown().await.unwrap();

        let hashing = encoder.into_inner();
        let (mut file, checksum) = hashing.finalize();
        file.write_all(&checksum).await.unwrap();
        file.flush().await.unwrap();

        let file_size = file.seek(std::io::SeekFrom::End(0)).await.unwrap();
        file.seek(std::io::SeekFrom::Start(0)).await.unwrap();
        SnapshotReader::verify_checksum(&mut file, file_size).await.unwrap();

        file.seek(std::io::SeekFrom::Start(0)).await.unwrap();
        let buf_reader = BufReader::new(file.take(file_size - CHECKSUM_SIZE as u64));
        let mut decoder = SnapshotReader::decompressor(buf_reader);

        let result = SnapshotReader::read_header(&mut decoder).await;
        assert!(matches!(result, Err(SnapshotError::InvalidEntry { .. })));
    }

    /// Snapshot file is smaller than raw data (zstd compression effective).
    #[tokio::test]
    async fn test_snapshot_compression_effective() {
        let core_data = vec![0xABu8; 100];
        let file = tokio::fs::File::from_std(tempfile::tempfile().unwrap());
        let mut writer = SnapshotWriter::new(file);
        writer.write_header(org_scope(), &core_data).await.unwrap();

        writer.write_table_block_count(1).await.unwrap();
        writer.write_table_block_header(TableBlockKind::OrgRaft, 1).await.unwrap();
        writer.write_table_header(7, 100).await.unwrap();
        let mut raw_size: usize = core_data.len();
        for i in 0u32..100 {
            let key = format!("key_{i:04}").into_bytes();
            let value = vec![0x42u8; 200];
            raw_size += key.len() + value.len();
            writer.write_table_entry(&key, &value).await.unwrap();
        }

        writer.write_event_count(0).await.unwrap();
        let mut file = writer.finish().await.unwrap();

        let file_size = file.seek(std::io::SeekFrom::End(0)).await.unwrap();
        assert!(
            (file_size as usize) < raw_size,
            "Compressed snapshot ({file_size}) should be smaller than raw data ({raw_size})"
        );
    }

    /// Invalid zstd stream is rejected during decompression.
    #[tokio::test]
    async fn test_snapshot_invalid_zstd_rejected() {
        let mut file = tokio::fs::File::from_std(tempfile::tempfile().unwrap());
        let garbage = b"this is not zstd compressed data";
        file.write_all(garbage).await.unwrap();

        let mut hasher = Sha256::new();
        hasher.update(garbage);
        let checksum: [u8; 32] = hasher.finalize().into();
        file.write_all(&checksum).await.unwrap();
        file.flush().await.unwrap();

        let file_size = file.seek(std::io::SeekFrom::End(0)).await.unwrap();
        file.seek(std::io::SeekFrom::Start(0)).await.unwrap();

        SnapshotReader::verify_checksum(&mut file, file_size).await.unwrap();

        file.seek(std::io::SeekFrom::Start(0)).await.unwrap();
        let buf_reader = BufReader::new(file.take(file_size - CHECKSUM_SIZE as u64));
        let mut decoder = SnapshotReader::decompressor(buf_reader);

        let result = SnapshotReader::read_header(&mut decoder).await;
        assert!(result.is_err(), "Should fail with invalid zstd stream");
    }

    /// Many-entry snapshot round-trips correctly (no cap).
    #[tokio::test]
    async fn test_snapshot_10k_entries_round_trip() {
        let entry_count: u32 = 10_001;
        let core_data = b"10k-entries-core";

        let file = tokio::fs::File::from_std(tempfile::tempfile().unwrap());
        let mut writer = SnapshotWriter::new(file);
        writer.write_header(vault_scope(), core_data).await.unwrap();
        writer.write_table_block_count(1).await.unwrap();
        writer.write_table_block_header(TableBlockKind::VaultState, 1).await.unwrap();
        writer.write_table_header(0, entry_count).await.unwrap();

        for i in 0..entry_count {
            let key = format!("entity_{i:06}").into_bytes();
            let value = format!("value_{i}").into_bytes();
            writer.write_table_entry(&key, &value).await.unwrap();
        }
        writer.write_event_count(0).await.unwrap();
        let mut file = writer.finish().await.unwrap();

        let file_size = file.seek(std::io::SeekFrom::End(0)).await.unwrap();
        file.seek(std::io::SeekFrom::Start(0)).await.unwrap();
        SnapshotReader::verify_checksum(&mut file, file_size).await.unwrap();

        file.seek(std::io::SeekFrom::Start(0)).await.unwrap();
        let buf_reader = BufReader::new(file.take(file_size - CHECKSUM_SIZE as u64));
        let mut decoder = SnapshotReader::decompressor(buf_reader);

        let (_scope, read_core) = SnapshotReader::read_header(&mut decoder).await.unwrap();
        assert_eq!(read_core, core_data);

        let bc = SnapshotReader::read_table_block_count(&mut decoder).await.unwrap();
        assert_eq!(bc, 1);
        let (kind, tc) = SnapshotReader::read_table_block_header(&mut decoder).await.unwrap();
        assert_eq!(kind, TableBlockKind::VaultState);
        assert_eq!(tc, 1);
        let (tid, ec) = SnapshotReader::read_table_header(&mut decoder).await.unwrap();
        assert_eq!(tid, 0);
        assert_eq!(ec, entry_count);

        for i in 0..entry_count {
            let (key, value) = SnapshotReader::read_kv_entry(&mut decoder).await.unwrap();
            let expected_key = format!("entity_{i:06}").into_bytes();
            let expected_value = format!("value_{i}").into_bytes();
            assert_eq!(key, expected_key);
            assert_eq!(value, expected_value);
        }

        let evc = SnapshotReader::read_event_count(&mut decoder).await.unwrap();
        assert_eq!(evc, 0);
    }

    /// Corrupt entry mid-stream: valid header + valid entries + entry with
    /// `value_len = u32::MAX`. Verify `read_kv_entry()` rejects with `InvalidEntry`.
    #[tokio::test]
    async fn test_snapshot_corrupt_value_len_max() {
        let file = tokio::fs::File::from_std(tempfile::tempfile().unwrap());
        let hashing = HashingWriter::new(file);
        let mut encoder =
            ZstdEncoder::with_quality(hashing, async_compression::Level::Precise(ZSTD_LEVEL));

        encoder.write_all(SNAPSHOT_MAGIC).await.unwrap();
        encoder.write_all(&[SNAPSHOT_VERSION, 0]).await.unwrap();
        let core = b"corrupt-test";
        encoder.write_all(&(core.len() as u32).to_le_bytes()).await.unwrap();
        encoder.write_all(core).await.unwrap();

        // 1 table block, OrgRaft, 1 table, 4 entries
        encoder.write_all(&1u32.to_le_bytes()).await.unwrap();
        encoder.write_all(&[TableBlockKind::OrgRaft as u8]).await.unwrap();
        encoder.write_all(&1u32.to_le_bytes()).await.unwrap();
        encoder.write_all(&[7u8]).await.unwrap();
        encoder.write_all(&4u32.to_le_bytes()).await.unwrap();

        for i in 0u32..3 {
            let key = format!("key_{i}").into_bytes();
            let value = format!("val_{i}").into_bytes();
            encoder.write_all(&(key.len() as u32).to_le_bytes()).await.unwrap();
            encoder.write_all(&key).await.unwrap();
            encoder.write_all(&(value.len() as u32).to_le_bytes()).await.unwrap();
            encoder.write_all(&value).await.unwrap();
        }

        let bad_key = b"bad_key";
        encoder.write_all(&(bad_key.len() as u32).to_le_bytes()).await.unwrap();
        encoder.write_all(bad_key).await.unwrap();
        encoder.write_all(&u32::MAX.to_le_bytes()).await.unwrap();

        encoder.shutdown().await.unwrap();
        let hashing = encoder.into_inner();
        let (mut file, checksum) = hashing.finalize();
        file.write_all(&checksum).await.unwrap();
        file.flush().await.unwrap();

        let file_size = file.seek(std::io::SeekFrom::End(0)).await.unwrap();
        file.seek(std::io::SeekFrom::Start(0)).await.unwrap();
        SnapshotReader::verify_checksum(&mut file, file_size).await.unwrap();

        file.seek(std::io::SeekFrom::Start(0)).await.unwrap();
        let buf_reader = BufReader::new(file.take(file_size - CHECKSUM_SIZE as u64));
        let mut decoder = SnapshotReader::decompressor(buf_reader);

        let (_scope, read_core) = SnapshotReader::read_header(&mut decoder).await.unwrap();
        assert_eq!(read_core, core);

        let bc = SnapshotReader::read_table_block_count(&mut decoder).await.unwrap();
        assert_eq!(bc, 1);
        let (kind, tc) = SnapshotReader::read_table_block_header(&mut decoder).await.unwrap();
        assert_eq!(kind, TableBlockKind::OrgRaft);
        assert_eq!(tc, 1);
        let (tid, ec) = SnapshotReader::read_table_header(&mut decoder).await.unwrap();
        assert_eq!(tid, 7);
        assert_eq!(ec, 4);

        for i in 0u32..3 {
            let (key, value) = SnapshotReader::read_kv_entry(&mut decoder).await.unwrap();
            assert_eq!(key, format!("key_{i}").into_bytes());
            assert_eq!(value, format!("val_{i}").into_bytes());
        }

        let result = SnapshotReader::read_kv_entry(&mut decoder).await;
        assert!(
            matches!(result, Err(SnapshotError::InvalidEntry { .. })),
            "Expected InvalidEntry error for value_len=u32::MAX, got: {result:?}"
        );
    }

    /// Snapshot with empty `AppliedStateCore` round-trips.
    #[tokio::test]
    async fn test_snapshot_empty_state_zero_events() {
        let empty_core = b"";

        let file = tokio::fs::File::from_std(tempfile::tempfile().unwrap());
        let mut writer = SnapshotWriter::new(file);
        writer.write_header(org_scope(), empty_core).await.unwrap();
        writer.write_table_block_count(0).await.unwrap();
        writer.write_event_count(0).await.unwrap();
        let mut file = writer.finish().await.unwrap();

        let file_size = file.seek(std::io::SeekFrom::End(0)).await.unwrap();
        file.seek(std::io::SeekFrom::Start(0)).await.unwrap();
        SnapshotReader::verify_checksum(&mut file, file_size).await.unwrap();

        file.seek(std::io::SeekFrom::Start(0)).await.unwrap();
        let buf_reader = BufReader::new(file.take(file_size - CHECKSUM_SIZE as u64));
        let mut decoder = SnapshotReader::decompressor(buf_reader);

        let (_scope, core) = SnapshotReader::read_header(&mut decoder).await.unwrap();
        assert!(core.is_empty(), "Empty core should round-trip as empty");

        assert_eq!(SnapshotReader::read_table_block_count(&mut decoder).await.unwrap(), 0);
        assert_eq!(SnapshotReader::read_event_count(&mut decoder).await.unwrap(), 0);
    }

    // -----------------------------------------------------------------------
    // Incremental snapshot tests
    // -----------------------------------------------------------------------

    /// Round-trip test: write an incremental snapshot with pages, read it back.
    #[tokio::test]
    async fn test_incremental_snapshot_round_trip() {
        let base_gen = 5;
        let current_gen = 10;
        let pages: Vec<(u64, Vec<u8>)> =
            vec![(1, vec![0xAA; 4096]), (7, vec![0xBB; 4096]), (42, vec![0xCC; 2048])];

        // Write
        let file = tokio::fs::File::from_std(tempfile::tempfile().unwrap());
        let mut writer = IncrementalSnapshotWriter::new(file);
        writer.write_header(base_gen, current_gen).await.unwrap();
        writer.write_page_count(pages.len() as u64).await.unwrap();
        for (pid, data) in &pages {
            writer.write_page(*pid, data).await.unwrap();
        }
        let mut file = writer.finish().await.unwrap();

        // Verify checksum
        let file_size = file.seek(std::io::SeekFrom::End(0)).await.unwrap();
        file.seek(std::io::SeekFrom::Start(0)).await.unwrap();
        SnapshotReader::verify_checksum(&mut file, file_size).await.unwrap();

        // Read back
        file.seek(std::io::SeekFrom::Start(0)).await.unwrap();
        let buf_reader = BufReader::new(file.take(file_size - CHECKSUM_SIZE as u64));
        let mut decoder = IncrementalSnapshotReader::decompressor(buf_reader);

        let header = IncrementalSnapshotReader::read_header(&mut decoder).await.unwrap();
        assert_eq!(header.base_generation, base_gen);
        assert_eq!(header.current_generation, current_gen);

        let page_count = IncrementalSnapshotReader::read_page_count(&mut decoder).await.unwrap();
        assert_eq!(page_count, 3);

        for (expected_pid, expected_data) in &pages {
            let (pid, data) = IncrementalSnapshotReader::read_page(&mut decoder).await.unwrap();
            assert_eq!(pid, *expected_pid);
            assert_eq!(data, *expected_data);
        }
    }

    /// Empty incremental snapshot (zero pages) round-trips.
    #[tokio::test]
    async fn test_incremental_snapshot_empty_round_trip() {
        let file = tokio::fs::File::from_std(tempfile::tempfile().unwrap());
        let mut writer = IncrementalSnapshotWriter::new(file);
        writer.write_header(0, 0).await.unwrap();
        writer.write_page_count(0).await.unwrap();
        let mut file = writer.finish().await.unwrap();

        let file_size = file.seek(std::io::SeekFrom::End(0)).await.unwrap();
        file.seek(std::io::SeekFrom::Start(0)).await.unwrap();
        SnapshotReader::verify_checksum(&mut file, file_size).await.unwrap();

        file.seek(std::io::SeekFrom::Start(0)).await.unwrap();
        let buf_reader = BufReader::new(file.take(file_size - CHECKSUM_SIZE as u64));
        let mut decoder = IncrementalSnapshotReader::decompressor(buf_reader);

        let header = IncrementalSnapshotReader::read_header(&mut decoder).await.unwrap();
        assert_eq!(header.base_generation, 0);
        assert_eq!(header.current_generation, 0);

        let count = IncrementalSnapshotReader::read_page_count(&mut decoder).await.unwrap();
        assert_eq!(count, 0);
    }

    /// Bad magic is rejected for incremental snapshots.
    #[tokio::test]
    async fn test_incremental_snapshot_bad_magic() {
        let file = tokio::fs::File::from_std(tempfile::tempfile().unwrap());
        let hashing = HashingWriter::new(file);
        let mut encoder =
            ZstdEncoder::with_quality(hashing, async_compression::Level::Precise(ZSTD_LEVEL));

        encoder.write_all(b"BAAD").await.unwrap();
        encoder.write_all(&[1]).await.unwrap();
        encoder.write_all(&0u64.to_le_bytes()).await.unwrap();
        encoder.write_all(&0u64.to_le_bytes()).await.unwrap();
        encoder.shutdown().await.unwrap();

        let hashing = encoder.into_inner();
        let (mut file, checksum) = hashing.finalize();
        file.write_all(&checksum).await.unwrap();
        file.flush().await.unwrap();

        let file_size = file.seek(std::io::SeekFrom::End(0)).await.unwrap();
        file.seek(std::io::SeekFrom::Start(0)).await.unwrap();

        let buf_reader = BufReader::new(file.take(file_size - CHECKSUM_SIZE as u64));
        let mut decoder = IncrementalSnapshotReader::decompressor(buf_reader);

        let result = IncrementalSnapshotReader::read_header(&mut decoder).await;
        assert!(matches!(result, Err(SnapshotError::BadMagic { .. })));
    }

    /// Oversized page data is rejected during read.
    #[tokio::test]
    async fn test_incremental_snapshot_oversized_page_rejected() {
        let file = tokio::fs::File::from_std(tempfile::tempfile().unwrap());
        let hashing = HashingWriter::new(file);
        let mut encoder =
            ZstdEncoder::with_quality(hashing, async_compression::Level::Precise(ZSTD_LEVEL));

        // Valid header
        encoder.write_all(INCREMENTAL_MAGIC).await.unwrap();
        encoder.write_all(&[SNAPSHOT_TYPE_INCREMENTAL]).await.unwrap();
        encoder.write_all(&0u64.to_le_bytes()).await.unwrap();
        encoder.write_all(&1u64.to_le_bytes()).await.unwrap();

        // 1 page
        encoder.write_all(&1u64.to_le_bytes()).await.unwrap();

        // page_id
        encoder.write_all(&1u64.to_le_bytes()).await.unwrap();
        // page_data_len = u32::MAX (way over limit)
        encoder.write_all(&u32::MAX.to_le_bytes()).await.unwrap();

        encoder.shutdown().await.unwrap();
        let hashing = encoder.into_inner();
        let (mut file, checksum) = hashing.finalize();
        file.write_all(&checksum).await.unwrap();
        file.flush().await.unwrap();

        let file_size = file.seek(std::io::SeekFrom::End(0)).await.unwrap();
        file.seek(std::io::SeekFrom::Start(0)).await.unwrap();

        let buf_reader = BufReader::new(file.take(file_size - CHECKSUM_SIZE as u64));
        let mut decoder = IncrementalSnapshotReader::decompressor(buf_reader);

        IncrementalSnapshotReader::read_header(&mut decoder).await.unwrap();
        let count = IncrementalSnapshotReader::read_page_count(&mut decoder).await.unwrap();
        assert_eq!(count, 1);

        let result = IncrementalSnapshotReader::read_page(&mut decoder).await;
        assert!(
            matches!(result, Err(SnapshotError::InvalidEntry { .. })),
            "Expected InvalidEntry for oversized page, got: {result:?}"
        );
    }

    /// Incremental snapshot with many pages compresses effectively.
    #[tokio::test]
    async fn test_incremental_snapshot_compression() {
        let file = tokio::fs::File::from_std(tempfile::tempfile().unwrap());
        let mut writer = IncrementalSnapshotWriter::new(file);
        writer.write_header(0, 100).await.unwrap();

        let page_count = 100u64;
        writer.write_page_count(page_count).await.unwrap();
        let mut raw_size: usize = 0;
        for i in 0..page_count {
            // Repetitive data compresses well
            let data = vec![0x42u8; 4096];
            raw_size += 8 + 4 + data.len(); // page_id + len + data
            writer.write_page(i, &data).await.unwrap();
        }

        let mut file = writer.finish().await.unwrap();
        let file_size = file.seek(std::io::SeekFrom::End(0)).await.unwrap() as usize;

        assert!(
            file_size < raw_size,
            "Compressed incremental snapshot ({file_size}) should be smaller than raw ({raw_size})"
        );
    }
}
