//! Snapshot management for point-in-time state capture.
//!
//! Per DESIGN.md:
//! - Snapshots serialize state for fast recovery
//! - Uses zstd compression (3-5x ratio typical)
//! - Format: header + compressed state data
//! - Naming: {shard_height:09}.snap

use std::{
    collections::HashMap,
    fs::{self, File},
    io::{BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
};

use inferadb_ledger_types::{ChainCommitment, Entity, Hash, ShardId, VaultId, decode, encode};
use snafu::{ResultExt, Snafu};
use zstd::stream::{Decoder, Encoder};

use crate::bucket::NUM_BUCKETS;

/// Snapshot file magic bytes.
const SNAPSHOT_MAGIC: [u8; 4] = *b"LSNP";

/// Current snapshot format version.
/// v2: Added chain verification linkage (genesis_hash, previous_snapshot, chain_commitment)
const SNAPSHOT_VERSION: u32 = 2;

/// Snapshot error types.
#[derive(Debug, Snafu)]
pub enum SnapshotError {
    /// IO error during snapshot read or write.
    #[snafu(display("IO error: {source}"))]
    Io {
        /// The underlying IO error.
        source: std::io::Error,
    },

    /// The snapshot file has invalid magic bytes (not a snapshot file).
    #[snafu(display("Invalid snapshot magic"))]
    InvalidMagic,

    /// The snapshot format version is not supported by this server.
    #[snafu(display("Unsupported snapshot version: {version}"))]
    UnsupportedVersion {
        /// The unsupported version number.
        version: u32,
    },

    /// The snapshot data checksum does not match the header checksum.
    #[snafu(display("Checksum mismatch: expected {expected:?}, got {actual:?}"))]
    ChecksumMismatch {
        /// The expected checksum from the header.
        expected: Hash,
        /// The actual checksum computed from the data.
        actual: Hash,
    },

    /// Error encoding or decoding snapshot data.
    #[snafu(display("Codec error: {source}"))]
    Codec {
        /// The underlying codec error.
        source: inferadb_ledger_types::CodecError,
    },

    /// The requested snapshot file does not exist.
    #[snafu(display("Snapshot not found: {path}"))]
    NotFound {
        /// The path that was not found.
        path: String,
    },
}

/// Result type for snapshot operations.
pub type Result<T> = std::result::Result<T, SnapshotError>;

/// Metadata for a single vault within a snapshot.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct VaultSnapshotMeta {
    /// Vault identifier.
    pub vault_id: VaultId,
    /// Per-vault height at snapshot time.
    pub vault_height: u64,
    /// State root hash.
    pub state_root: Hash,
    /// Bucket roots for incremental state computation.
    /// Stored as Vec for serde compatibility (always exactly NUM_BUCKETS elements).
    pub bucket_roots: Vec<Hash>,
    /// Number of keys in this vault.
    pub key_count: u64,
}

impl VaultSnapshotMeta {
    /// Create a new vault snapshot metadata.
    pub fn new(
        vault_id: VaultId,
        vault_height: u64,
        state_root: Hash,
        bucket_roots: [Hash; NUM_BUCKETS],
        key_count: u64,
    ) -> Self {
        Self { vault_id, vault_height, state_root, bucket_roots: bucket_roots.to_vec(), key_count }
    }

    /// Get bucket roots as a fixed-size array.
    ///
    /// Returns None if the Vec doesn't have exactly NUM_BUCKETS elements.
    pub fn bucket_roots_array(&self) -> Option<[Hash; NUM_BUCKETS]> {
        if self.bucket_roots.len() != NUM_BUCKETS {
            return None;
        }
        let mut arr = [[0u8; 32]; NUM_BUCKETS];
        for (i, hash) in self.bucket_roots.iter().enumerate() {
            arr[i] = *hash;
        }
        Some(arr)
    }
}

/// Snapshot header.
///
/// Per DESIGN.md §4.4: Snapshots include chain verification linkage to enable
/// verification after block compaction.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SnapshotHeader {
    /// Magic bytes for validation.
    pub magic: [u8; 4],
    /// Format version.
    pub version: u32,
    /// Shard identifier.
    pub shard_id: ShardId,
    /// Shard height at snapshot time.
    pub shard_height: u64,
    /// Per-vault metadata.
    pub vault_states: Vec<VaultSnapshotMeta>,
    /// SHA-256 checksum of compressed state data.
    pub checksum: Hash,

    // Chain verification linkage (per DESIGN.md §4.4)
    /// Hash of the genesis block for this shard.
    /// Links the snapshot back to the shard's origin.
    pub genesis_hash: Hash,
    /// Height of the previous snapshot in the chain.
    /// None for the first snapshot.
    pub previous_snapshot_height: Option<u64>,
    /// Hash of the previous snapshot's header.
    /// None for the first snapshot. Enables snapshot chain integrity verification.
    pub previous_snapshot_hash: Option<Hash>,
    /// Accumulated cryptographic commitment for blocks since previous snapshot.
    /// Proves state evolution without requiring full block replay.
    pub chain_commitment: ChainCommitment,
}

/// Snapshot state data (serialized and compressed).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SnapshotStateData {
    /// All entities organized by vault.
    pub vault_entities: HashMap<VaultId, Vec<Entity>>,
}

/// In-memory snapshot representation.
#[derive(Debug, Clone)]
pub struct Snapshot {
    /// Snapshot header.
    pub header: SnapshotHeader,
    /// Decompressed state data.
    pub state: SnapshotStateData,
}

/// Parameters for creating a snapshot with chain verification.
#[derive(Debug, Clone)]
pub struct SnapshotChainParams {
    /// Hash of the genesis block for this shard.
    pub genesis_hash: Hash,
    /// Height of the previous snapshot (if any).
    pub previous_snapshot_height: Option<u64>,
    /// Hash of the previous snapshot's header (if any).
    pub previous_snapshot_hash: Option<Hash>,
    /// Chain commitment covering blocks since previous snapshot.
    pub chain_commitment: ChainCommitment,
}

impl Default for SnapshotChainParams {
    fn default() -> Self {
        Self {
            genesis_hash: inferadb_ledger_types::ZERO_HASH,
            previous_snapshot_height: None,
            previous_snapshot_hash: None,
            chain_commitment: ChainCommitment::default(),
        }
    }
}

impl Snapshot {
    /// Create a new snapshot with chain verification linkage.
    ///
    /// Per DESIGN.md §4.4, snapshots should include chain verification data
    /// to enable verification after block compaction.
    pub fn new(
        shard_id: ShardId,
        shard_height: u64,
        vault_states: Vec<VaultSnapshotMeta>,
        state: SnapshotStateData,
        chain_params: SnapshotChainParams,
    ) -> Result<Self> {
        // Compute checksum of state data
        let state_bytes = encode(&state).context(CodecSnafu)?;
        let checksum = inferadb_ledger_types::sha256(&state_bytes);

        let header = SnapshotHeader {
            magic: SNAPSHOT_MAGIC,
            version: SNAPSHOT_VERSION,
            shard_id,
            shard_height,
            vault_states,
            checksum,
            genesis_hash: chain_params.genesis_hash,
            previous_snapshot_height: chain_params.previous_snapshot_height,
            previous_snapshot_hash: chain_params.previous_snapshot_hash,
            chain_commitment: chain_params.chain_commitment,
        };

        Ok(Self { header, state })
    }

    /// Create a simple snapshot without chain verification (for testing).
    ///
    /// In production, prefer `new()` with proper chain parameters.
    #[cfg(test)]
    pub fn new_simple(
        shard_id: ShardId,
        shard_height: u64,
        vault_states: Vec<VaultSnapshotMeta>,
        state: SnapshotStateData,
    ) -> Result<Self> {
        Self::new(shard_id, shard_height, vault_states, state, SnapshotChainParams::default())
    }

    /// Get the shard height of this snapshot.
    pub fn shard_height(&self) -> u64 {
        self.header.shard_height
    }

    /// Get vault metadata by vault_id.
    pub fn get_vault_meta(&self, vault_id: VaultId) -> Option<&VaultSnapshotMeta> {
        self.header.vault_states.iter().find(|v| v.vault_id == vault_id)
    }

    /// Get entities for a vault.
    pub fn get_vault_entities(&self, vault_id: VaultId) -> Option<&Vec<Entity>> {
        self.state.vault_entities.get(&vault_id)
    }

    /// Write snapshot to file with zstd compression.
    pub fn write_to_file(&self, path: &Path) -> Result<()> {
        let file = File::create(path).context(IoSnafu)?;
        let mut writer = BufWriter::new(file);

        // Serialize and compress state data
        let state_bytes = encode(&self.state).context(CodecSnafu)?;

        let mut compressed = Vec::new();
        {
            let mut encoder = Encoder::new(&mut compressed, 3).context(IoSnafu)?;
            encoder.write_all(&state_bytes).context(IoSnafu)?;
            encoder.finish().context(IoSnafu)?;
        }

        // Write header (uncompressed)
        let header_bytes = encode(&self.header).context(CodecSnafu)?;

        // Length-prefixed header
        writer.write_all(&(header_bytes.len() as u32).to_le_bytes()).context(IoSnafu)?;
        writer.write_all(&header_bytes).context(IoSnafu)?;

        // Compressed state data
        writer.write_all(&compressed).context(IoSnafu)?;
        writer.flush().context(IoSnafu)?;

        Ok(())
    }

    /// Read snapshot from file.
    pub fn read_from_file(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Err(SnapshotError::NotFound { path: path.display().to_string() });
        }

        let file = File::open(path).context(IoSnafu)?;
        let mut reader = BufReader::new(file);

        // Read header length
        let mut header_len_bytes = [0u8; 4];
        reader.read_exact(&mut header_len_bytes).context(IoSnafu)?;
        let header_len = u32::from_le_bytes(header_len_bytes) as usize;

        // Read header
        let mut header_bytes = vec![0u8; header_len];
        reader.read_exact(&mut header_bytes).context(IoSnafu)?;

        let header: SnapshotHeader = decode(&header_bytes).context(CodecSnafu)?;

        // Validate magic
        if header.magic != SNAPSHOT_MAGIC {
            return Err(SnapshotError::InvalidMagic);
        }

        // Validate version
        if header.version != SNAPSHOT_VERSION {
            return Err(SnapshotError::UnsupportedVersion { version: header.version });
        }

        // Read and decompress state data
        let mut compressed = Vec::new();
        reader.read_to_end(&mut compressed).context(IoSnafu)?;

        let mut decoder = Decoder::new(&compressed[..]).context(IoSnafu)?;
        let mut state_bytes = Vec::new();
        decoder.read_to_end(&mut state_bytes).context(IoSnafu)?;

        // Verify checksum
        let computed_checksum = inferadb_ledger_types::sha256(&state_bytes);
        if computed_checksum != header.checksum {
            return Err(SnapshotError::ChecksumMismatch {
                expected: header.checksum,
                actual: computed_checksum,
            });
        }

        // Deserialize state
        let state: SnapshotStateData = decode(&state_bytes).context(CodecSnafu)?;

        Ok(Self { header, state })
    }
}

/// Snapshot file naming utilities.
pub fn snapshot_filename(shard_height: u64) -> String {
    format!("{:09}.snap", shard_height)
}

/// Parse shard height from snapshot filename.
pub fn parse_snapshot_filename(filename: &str) -> Option<u64> {
    filename.strip_suffix(".snap").and_then(|s| s.parse().ok())
}

/// Snapshot manager for a shard.
pub struct SnapshotManager {
    /// Directory containing snapshots.
    snapshot_dir: PathBuf,
    /// Maximum number of snapshots to retain.
    max_snapshots: usize,
}

impl SnapshotManager {
    /// Create a new snapshot manager.
    pub fn new(snapshot_dir: PathBuf, max_snapshots: usize) -> Self {
        Self { snapshot_dir, max_snapshots }
    }

    /// Get the snapshot directory path.
    pub fn snapshot_dir(&self) -> &Path {
        &self.snapshot_dir
    }

    /// Ensure the snapshot directory exists.
    pub fn init(&self) -> Result<()> {
        fs::create_dir_all(&self.snapshot_dir).context(IoSnafu)?;
        Ok(())
    }

    /// Save a snapshot.
    pub fn save(&self, snapshot: &Snapshot) -> Result<PathBuf> {
        self.init()?;

        let filename = snapshot_filename(snapshot.shard_height());
        let path = self.snapshot_dir.join(&filename);

        snapshot.write_to_file(&path)?;

        // Prune old snapshots
        self.prune()?;

        Ok(path)
    }

    /// Load the latest snapshot.
    pub fn load_latest(&self) -> Result<Option<Snapshot>> {
        let snapshots = self.list_snapshots()?;
        if snapshots.is_empty() {
            return Ok(None);
        }

        // List is sorted ascending, take the last one
        // Safety: we just checked that snapshots is non-empty above
        #[allow(clippy::expect_used)]
        let latest = snapshots.last().expect("not empty");
        let path = self.snapshot_dir.join(snapshot_filename(*latest));

        Ok(Some(Snapshot::read_from_file(&path)?))
    }

    /// Load a snapshot by height.
    pub fn load(&self, shard_height: u64) -> Result<Snapshot> {
        let path = self.snapshot_dir.join(snapshot_filename(shard_height));
        Snapshot::read_from_file(&path)
    }

    /// List available snapshot heights (sorted ascending).
    pub fn list_snapshots(&self) -> Result<Vec<u64>> {
        if !self.snapshot_dir.exists() {
            return Ok(Vec::new());
        }

        let mut heights = Vec::new();

        for entry in fs::read_dir(&self.snapshot_dir).context(IoSnafu)? {
            let entry = entry.context(IoSnafu)?;
            if let Some(filename) = entry.file_name().to_str()
                && let Some(height) = parse_snapshot_filename(filename)
            {
                heights.push(height);
            }
        }

        heights.sort_unstable();
        Ok(heights)
    }

    /// Prune old snapshots, keeping only the most recent `max_snapshots`.
    fn prune(&self) -> Result<()> {
        let snapshots = self.list_snapshots()?;

        if snapshots.len() <= self.max_snapshots {
            return Ok(());
        }

        // Remove oldest snapshots
        let to_remove = snapshots.len() - self.max_snapshots;
        for height in snapshots.into_iter().take(to_remove) {
            let path = self.snapshot_dir.join(snapshot_filename(height));
            fs::remove_file(&path).context(IoSnafu)?;
        }

        Ok(())
    }

    /// Find the latest snapshot at or before the given height.
    pub fn find_snapshot_at_or_before(&self, height: u64) -> Result<Option<u64>> {
        let snapshots = self.list_snapshots()?;
        // list_snapshots returns ascending order, so reverse and find first <= height
        Ok(snapshots.into_iter().rev().find(|&h| h <= height))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use inferadb_ledger_types::EMPTY_HASH;
    use tempfile::TempDir;

    use super::*;

    fn create_test_snapshot() -> Snapshot {
        let vault_states = vec![VaultSnapshotMeta {
            vault_id: VaultId::new(1),
            vault_height: 100,
            state_root: [42u8; 32],
            bucket_roots: [EMPTY_HASH; NUM_BUCKETS].to_vec(),
            key_count: 5,
        }];

        let mut vault_entities = HashMap::new();
        vault_entities.insert(
            VaultId::new(1),
            vec![
                Entity {
                    key: b"key1".to_vec(),
                    value: b"value1".to_vec(),
                    expires_at: 0,
                    version: 1,
                },
                Entity {
                    key: b"key2".to_vec(),
                    value: b"value2".to_vec(),
                    expires_at: 100,
                    version: 2,
                },
            ],
        );

        let state = SnapshotStateData { vault_entities };

        Snapshot::new_simple(ShardId::new(1), 1000, vault_states, state).expect("create snapshot")
    }

    #[test]
    fn test_snapshot_roundtrip() {
        let temp = TempDir::new().expect("create temp dir");
        let path = temp.path().join("test.snap");

        let snapshot = create_test_snapshot();
        snapshot.write_to_file(&path).expect("write snapshot");

        let loaded = Snapshot::read_from_file(&path).expect("read snapshot");

        assert_eq!(loaded.header.shard_id, snapshot.header.shard_id);
        assert_eq!(loaded.header.shard_height, snapshot.header.shard_height);
        assert_eq!(loaded.header.vault_states.len(), snapshot.header.vault_states.len());

        let entities = loaded.get_vault_entities(VaultId::new(1)).expect("vault 1 entities");
        assert_eq!(entities.len(), 2);
    }

    #[test]
    fn test_snapshot_manager() {
        let temp = TempDir::new().expect("create temp dir");
        let manager = SnapshotManager::new(temp.path().join("snapshots"), 3);

        // Save multiple snapshots
        for height in [100, 200, 300, 400, 500] {
            let vault_states = vec![VaultSnapshotMeta {
                vault_id: VaultId::new(1),
                vault_height: height / 2,
                state_root: [height as u8; 32],
                bucket_roots: [EMPTY_HASH; NUM_BUCKETS].to_vec(),
                key_count: 0,
            }];

            let snapshot = Snapshot::new_simple(
                ShardId::new(1),
                height,
                vault_states,
                SnapshotStateData { vault_entities: HashMap::new() },
            )
            .expect("create snapshot");

            manager.save(&snapshot).expect("save snapshot");
        }

        // Should only have 3 snapshots (pruned oldest 2)
        let snapshots = manager.list_snapshots().expect("list");
        assert_eq!(snapshots.len(), 3);
        assert_eq!(snapshots, vec![300, 400, 500]);

        // Load latest
        let latest = manager.load_latest().expect("load latest").expect("exists");
        assert_eq!(latest.shard_height(), 500);

        // Find snapshot at or before
        let at_350 = manager.find_snapshot_at_or_before(350).expect("find");
        assert_eq!(at_350, Some(300));
    }

    #[test]
    fn test_snapshot_filename() {
        assert_eq!(snapshot_filename(0), "000000000.snap");
        assert_eq!(snapshot_filename(1000), "000001000.snap");
        assert_eq!(snapshot_filename(999_999_999), "999999999.snap");
    }

    #[test]
    fn test_parse_snapshot_filename() {
        assert_eq!(parse_snapshot_filename("000001000.snap"), Some(1000));
        assert_eq!(parse_snapshot_filename("not_a_snapshot.txt"), None);
        assert_eq!(parse_snapshot_filename("000000000.snap"), Some(0));
    }

    #[test]
    fn test_checksum_validation() {
        let temp = TempDir::new().expect("create temp dir");
        let path = temp.path().join("corrupted.snap");

        let snapshot = create_test_snapshot();
        snapshot.write_to_file(&path).expect("write snapshot");

        // Corrupt the file by modifying a byte near the end
        let mut data = fs::read(&path).expect("read file");
        if let Some(last) = data.last_mut() {
            *last ^= 0xFF;
        }
        fs::write(&path, data).expect("write corrupted");

        // Should fail checksum validation
        let result = Snapshot::read_from_file(&path);
        assert!(matches!(result, Err(SnapshotError::ChecksumMismatch { .. })));
    }

    #[test]
    fn test_snapshot_with_chain_verification() {
        let temp = TempDir::new().expect("create temp dir");
        let path = temp.path().join("chain_verified.snap");

        let vault_states = vec![VaultSnapshotMeta {
            vault_id: VaultId::new(1),
            vault_height: 100,
            state_root: [42u8; 32],
            bucket_roots: [EMPTY_HASH; NUM_BUCKETS].to_vec(),
            key_count: 5,
        }];

        let state = SnapshotStateData { vault_entities: HashMap::new() };

        // Create chain params with actual values
        let chain_params = SnapshotChainParams {
            genesis_hash: [1u8; 32],
            previous_snapshot_height: Some(500),
            previous_snapshot_hash: Some([2u8; 32]),
            chain_commitment: ChainCommitment {
                accumulated_header_hash: [3u8; 32],
                state_root_accumulator: [4u8; 32],
                from_height: 501,
                to_height: 1000,
            },
        };

        let snapshot = Snapshot::new(ShardId::new(1), 1000, vault_states, state, chain_params)
            .expect("create snapshot");

        // Verify chain fields before write
        assert_eq!(snapshot.header.genesis_hash, [1u8; 32]);
        assert_eq!(snapshot.header.previous_snapshot_height, Some(500));
        assert_eq!(snapshot.header.previous_snapshot_hash, Some([2u8; 32]));
        assert_eq!(snapshot.header.chain_commitment.from_height, 501);
        assert_eq!(snapshot.header.chain_commitment.to_height, 1000);

        // Write and read back
        snapshot.write_to_file(&path).expect("write snapshot");
        let loaded = Snapshot::read_from_file(&path).expect("read snapshot");

        // Verify chain fields after read
        assert_eq!(loaded.header.genesis_hash, [1u8; 32]);
        assert_eq!(loaded.header.previous_snapshot_height, Some(500));
        assert_eq!(loaded.header.previous_snapshot_hash, Some([2u8; 32]));
        assert_eq!(loaded.header.chain_commitment.accumulated_header_hash, [3u8; 32]);
        assert_eq!(loaded.header.chain_commitment.state_root_accumulator, [4u8; 32]);
        assert_eq!(loaded.header.chain_commitment.from_height, 501);
        assert_eq!(loaded.header.chain_commitment.to_height, 1000);
    }

    /// Test that consecutive snapshots form a valid chain.
    ///
    /// Snapshot N+1 references snapshot N's height and hash, creating a
    /// verifiable chain of state commitments.
    #[test]
    fn test_snapshot_chain_continuity() {
        let temp = TempDir::new().expect("create temp dir");
        let manager = SnapshotManager::new(temp.path().to_path_buf(), 10);

        let vault_states = vec![VaultSnapshotMeta {
            vault_id: VaultId::new(1),
            vault_height: 50,
            state_root: [10u8; 32],
            bucket_roots: [EMPTY_HASH; NUM_BUCKETS].to_vec(),
            key_count: 3,
        }];

        let state = SnapshotStateData { vault_entities: HashMap::new() };

        // Create first snapshot (genesis — no previous)
        let chain_params_1 = SnapshotChainParams {
            genesis_hash: [0xAA; 32],
            previous_snapshot_height: None,
            previous_snapshot_hash: None,
            chain_commitment: ChainCommitment {
                accumulated_header_hash: [0xBB; 32],
                state_root_accumulator: [0xCC; 32],
                from_height: 1,
                to_height: 500,
            },
        };

        let snap1 = Snapshot::new(
            ShardId::new(1),
            500,
            vault_states.clone(),
            state.clone(),
            chain_params_1,
        )
        .expect("create snapshot 1");

        let path1 = manager.save(&snap1).expect("save snapshot 1");
        let snap1_loaded = Snapshot::read_from_file(&path1).expect("load snapshot 1");

        // Create second snapshot referencing the first
        let chain_params_2 = SnapshotChainParams {
            genesis_hash: [0xAA; 32], // Same genesis
            previous_snapshot_height: Some(500),
            previous_snapshot_hash: Some(snap1_loaded.header.checksum),
            chain_commitment: ChainCommitment {
                accumulated_header_hash: [0xDD; 32],
                state_root_accumulator: [0xEE; 32],
                from_height: 501,
                to_height: 1000,
            },
        };

        let snap2 = Snapshot::new(ShardId::new(1), 1000, vault_states, state, chain_params_2)
            .expect("create snapshot 2");

        let path2 = manager.save(&snap2).expect("save snapshot 2");
        let snap2_loaded = Snapshot::read_from_file(&path2).expect("load snapshot 2");

        // Verify chain linkage
        assert_eq!(
            snap2_loaded.header.previous_snapshot_height,
            Some(500),
            "snapshot 2 should reference snapshot 1's height"
        );
        assert_eq!(
            snap2_loaded.header.previous_snapshot_hash,
            Some(snap1_loaded.header.checksum),
            "snapshot 2 should reference snapshot 1's checksum"
        );
        assert_eq!(
            snap2_loaded.header.genesis_hash, snap1_loaded.header.genesis_hash,
            "both snapshots should share the same genesis hash"
        );
        assert_eq!(
            snap2_loaded.header.chain_commitment.from_height, 501,
            "snapshot 2 commitment should start after snapshot 1"
        );
    }

    /// Test that vault state roots are preserved across snapshot roundtrips
    /// with multiple vaults.
    ///
    /// Each vault maintains an independent state root in the snapshot, which
    /// must survive serialization. Verifiers use these roots to validate
    /// individual vault states without processing the entire shard.
    #[test]
    fn test_snapshot_preserves_multiple_vault_state_roots() {
        let temp = TempDir::new().expect("create temp dir");
        let path = temp.path().join("multi_vault.snap");

        let vault_states = vec![
            VaultSnapshotMeta {
                vault_id: VaultId::new(1),
                vault_height: 100,
                state_root: [0x11; 32],
                bucket_roots: [EMPTY_HASH; NUM_BUCKETS].to_vec(),
                key_count: 10,
            },
            VaultSnapshotMeta {
                vault_id: VaultId::new(2),
                vault_height: 200,
                state_root: [0x22; 32],
                bucket_roots: [EMPTY_HASH; NUM_BUCKETS].to_vec(),
                key_count: 20,
            },
            VaultSnapshotMeta {
                vault_id: VaultId::new(3),
                vault_height: 300,
                state_root: [0x33; 32],
                bucket_roots: [EMPTY_HASH; NUM_BUCKETS].to_vec(),
                key_count: 30,
            },
        ];

        let state = SnapshotStateData { vault_entities: HashMap::new() };

        let snapshot = Snapshot::new_simple(ShardId::new(1), 5000, vault_states, state)
            .expect("create snapshot");

        snapshot.write_to_file(&path).expect("write");
        let loaded = Snapshot::read_from_file(&path).expect("read");

        // Verify each vault's state root survived the roundtrip
        assert_eq!(loaded.header.vault_states.len(), 3);

        let v1 = loaded.get_vault_meta(VaultId::new(1)).expect("vault 1 meta");
        let v2 = loaded.get_vault_meta(VaultId::new(2)).expect("vault 2 meta");
        let v3 = loaded.get_vault_meta(VaultId::new(3)).expect("vault 3 meta");

        assert_eq!(v1.state_root, [0x11; 32], "vault 1 state root");
        assert_eq!(v1.vault_height, 100);
        assert_eq!(v1.key_count, 10);

        assert_eq!(v2.state_root, [0x22; 32], "vault 2 state root");
        assert_eq!(v2.vault_height, 200);
        assert_eq!(v2.key_count, 20);

        assert_eq!(v3.state_root, [0x33; 32], "vault 3 state root");
        assert_eq!(v3.vault_height, 300);
        assert_eq!(v3.key_count, 30);
    }
}
