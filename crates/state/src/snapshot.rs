//! Snapshot management for point-in-time state capture.
//!
//! - Snapshots serialize state for fast recovery
//! - Uses zstd compression (3-5x ratio typical)
//! - Format: header + compressed state data
//! - Naming: {region_height:09}.snap

use std::{
    collections::{HashMap, HashSet},
    fs::{self, File},
    io::{BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
};

use inferadb_ledger_types::{
    ChainCommitment, Entity, Hash, Region, UserId, VaultId, decode, encode,
};
use snafu::{ResultExt, Snafu};
use zstd::stream::{Decoder, Encoder};

use crate::bucket::NUM_BUCKETS;

/// Snapshot file magic bytes.
const SNAPSHOT_MAGIC: [u8; 4] = *b"LSNP";

/// Current snapshot format version.
/// v2: Added chain verification linkage (genesis_hash, previous_snapshot, chain_commitment)
const SNAPSHOT_VERSION: u32 = 2;

/// Errors returned by snapshot creation, reading, and management operations.
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

    /// Protected region snapshot transferred to a node outside the region.
    #[snafu(display(
        "Cross-region snapshot transfer rejected: snapshot region {snapshot_region} \
         requires residency, but target node is in region {node_region}"
    ))]
    CrossRegionTransfer {
        /// Region the snapshot belongs to.
        snapshot_region: Region,
        /// Region of the target node.
        node_region: Region,
    },

    /// Protected region snapshot restore attempted on a node outside the region.
    #[snafu(display(
        "Cross-region snapshot restore rejected: snapshot region {snapshot_region} \
         requires residency, but node is in region {node_region}"
    ))]
    CrossRegionRestore {
        /// Region the snapshot belongs to.
        snapshot_region: Region,
        /// Region of the restoring node.
        node_region: Region,
    },

    /// Snapshot references RMK versions not available in the local key cache.
    #[snafu(display("Missing RMK versions for region {region}: {missing_versions:?}"))]
    MissingRmkVersions {
        /// Region the snapshot belongs to.
        region: Region,
        /// RMK versions referenced by the snapshot but not found locally.
        missing_versions: Vec<u32>,
    },
}

/// Result type for snapshot operations.
pub type Result<T> = std::result::Result<T, SnapshotError>;

/// Metadata for a single vault within a snapshot.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct VaultSnapshotMeta {
    /// Internal vault identifier (`VaultId`).
    pub vault: VaultId,
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
    /// Creates a new vault snapshot metadata.
    pub fn new(
        vault: VaultId,
        vault_height: u64,
        state_root: Hash,
        bucket_roots: [Hash; NUM_BUCKETS],
        key_count: u64,
    ) -> Self {
        Self { vault, vault_height, state_root, bucket_roots: bucket_roots.to_vec(), key_count }
    }

    /// Returns bucket roots as a fixed-size array.
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

/// Header metadata for a serialized snapshot.
///
/// Includes chain verification linkage to enable integrity verification after
/// block compaction.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SnapshotHeader {
    /// Magic bytes for validation.
    pub magic: [u8; 4],
    /// Format version.
    pub version: u32,
    /// Region this snapshot belongs to.
    pub region: Region,
    /// Region height at snapshot time.
    pub region_height: u64,
    /// Per-vault metadata.
    pub vault_states: Vec<VaultSnapshotMeta>,
    /// SHA-256 checksum of compressed state data.
    pub checksum: Hash,

    // Chain verification linkage
    /// Hash of the genesis block for this region.
    /// Links the snapshot back to the region's origin.
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

    /// Set of user IDs whose subject keys have been destroyed via crypto-shredding.
    ///
    /// On `install_snapshot()`, the restore procedure skips subject key restoration
    /// for erased users. Tombstones are permanent — never removed from snapshot
    /// metadata. Storage cost is negligible (~8 bytes per erased user ID).
    #[serde(default)]
    pub erased_users: HashSet<UserId>,

    /// RMK versions referenced by encrypted artifacts in this snapshot.
    ///
    /// Enables pre-validation before snapshot installation: the receiving node
    /// verifies all listed versions exist in its `RmkCache` before attempting
    /// decryption. An empty vec indicates an unencrypted snapshot.
    #[serde(default)]
    pub rmk_versions: Vec<u32>,
}

/// Snapshot state data (serialized and compressed).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SnapshotStateData {
    /// All entities organized by vault.
    pub vault_entities: HashMap<VaultId, Vec<Entity>>,
}

/// In-memory snapshot containing header metadata and decompressed state data.
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
    /// Hash of the genesis block for this region.
    pub genesis_hash: Hash,
    /// Height of the previous snapshot (if any).
    pub previous_snapshot_height: Option<u64>,
    /// Hash of the previous snapshot's header (if any).
    pub previous_snapshot_hash: Option<Hash>,
    /// Chain commitment covering blocks since previous snapshot.
    pub chain_commitment: ChainCommitment,
    /// User IDs whose subject keys have been destroyed (erasure tombstones).
    pub erased_users: HashSet<UserId>,
}

impl Default for SnapshotChainParams {
    fn default() -> Self {
        Self {
            genesis_hash: inferadb_ledger_types::ZERO_HASH,
            previous_snapshot_height: None,
            previous_snapshot_hash: None,
            chain_commitment: ChainCommitment::default(),
            erased_users: HashSet::new(),
        }
    }
}

impl Snapshot {
    /// Creates a new snapshot with chain verification linkage.
    ///
    /// Chain parameters enable snapshot integrity verification even after block compaction removes
    /// transaction bodies.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotError::Codec`] if serialization of the state data fails.
    pub fn new(
        region: Region,
        region_height: u64,
        vault_states: Vec<VaultSnapshotMeta>,
        state: SnapshotStateData,
        chain_params: SnapshotChainParams,
    ) -> Result<Self> {
        Self::new_with_rmk_versions(
            region,
            region_height,
            vault_states,
            state,
            chain_params,
            Vec::new(),
        )
    }

    /// Creates a new snapshot with chain verification linkage and RMK version tracking.
    ///
    /// `rmk_versions` lists all Region Master Key versions referenced by encrypted
    /// artifacts in this snapshot. Receiving nodes pre-validate these versions exist
    /// in their key cache before attempting snapshot installation.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotError::Codec`] if serialization of the state data fails.
    pub fn new_with_rmk_versions(
        region: Region,
        region_height: u64,
        vault_states: Vec<VaultSnapshotMeta>,
        state: SnapshotStateData,
        chain_params: SnapshotChainParams,
        rmk_versions: Vec<u32>,
    ) -> Result<Self> {
        // Compute checksum of state data
        let state_bytes = encode(&state).context(CodecSnafu)?;
        let checksum = inferadb_ledger_types::sha256(&state_bytes);

        let header = SnapshotHeader {
            magic: SNAPSHOT_MAGIC,
            version: SNAPSHOT_VERSION,
            region,
            region_height,
            vault_states,
            checksum,
            genesis_hash: chain_params.genesis_hash,
            previous_snapshot_height: chain_params.previous_snapshot_height,
            previous_snapshot_hash: chain_params.previous_snapshot_hash,
            chain_commitment: chain_params.chain_commitment,
            erased_users: chain_params.erased_users,
            rmk_versions,
        };

        Ok(Self { header, state })
    }

    /// Creates a snapshot without chain verification (for testing).
    ///
    /// In production, prefer `new()` with proper chain parameters.
    #[cfg(test)]
    pub fn new_simple(
        region: Region,
        region_height: u64,
        vault_states: Vec<VaultSnapshotMeta>,
        state: SnapshotStateData,
    ) -> Result<Self> {
        Self::new(region, region_height, vault_states, state, SnapshotChainParams::default())
    }

    /// Validates that this snapshot can be transferred to a node in `target_region`.
    ///
    /// Protected regions restrict snapshot transfer to in-region nodes only.
    /// Non-protected regions allow transfer to any node. The
    /// `snapshot_requires_residency` flag is the value persisted in the
    /// snapshot's source region directory entry; callers resolve it via
    /// [`crate::system::lookup_region_residency`] (treat unknown regions as
    /// `true` per the disciplined-default policy).
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotError::CrossRegionTransfer`] if the snapshot belongs to a
    /// protected region and `target_region` differs.
    pub fn validate_transfer(
        &self,
        target_region: Region,
        snapshot_requires_residency: bool,
    ) -> Result<()> {
        let snapshot_region = self.header.region;
        if snapshot_requires_residency && snapshot_region != target_region {
            return Err(SnapshotError::CrossRegionTransfer {
                snapshot_region,
                node_region: target_region,
            });
        }
        Ok(())
    }

    /// Validates that this snapshot can be restored on a node in `node_region`.
    ///
    /// Protected regions restrict snapshot restore to in-region nodes only.
    /// Non-protected regions allow restore on any node. The
    /// `snapshot_requires_residency` flag is the value persisted in the
    /// snapshot's source region directory entry; callers resolve it via
    /// [`crate::system::lookup_region_residency`] (treat unknown regions as
    /// `true` per the disciplined-default policy).
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotError::CrossRegionRestore`] if the snapshot belongs to a
    /// protected region and `node_region` differs.
    pub fn validate_restore(
        &self,
        node_region: Region,
        snapshot_requires_residency: bool,
    ) -> Result<()> {
        let snapshot_region = self.header.region;
        if snapshot_requires_residency && snapshot_region != node_region {
            return Err(SnapshotError::CrossRegionRestore { snapshot_region, node_region });
        }
        Ok(())
    }

    /// Validates that all RMK versions referenced by this snapshot are available.
    ///
    /// Pre-validates before installation to produce a clear error instead of
    /// failing mid-install with a cryptic decryption error.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotError::MissingRmkVersions`] if any referenced version
    /// is not found in `available_versions`.
    pub fn validate_rmk_versions(&self, available_versions: &[u32]) -> Result<()> {
        if self.header.rmk_versions.is_empty() {
            return Ok(());
        }

        let missing: Vec<u32> = self
            .header
            .rmk_versions
            .iter()
            .filter(|v| !available_versions.contains(v))
            .copied()
            .collect();

        if !missing.is_empty() {
            return Err(SnapshotError::MissingRmkVersions {
                region: self.header.region,
                missing_versions: missing,
            });
        }
        Ok(())
    }

    /// Returns the region height of this snapshot.
    pub fn region_height(&self) -> u64 {
        self.header.region_height
    }

    /// Returns vault metadata by vault.
    pub fn get_vault_meta(&self, vault: VaultId) -> Option<&VaultSnapshotMeta> {
        self.header.vault_states.iter().find(|v| v.vault == vault)
    }

    /// Returns entities for a vault.
    pub fn get_vault_entities(&self, vault: VaultId) -> Option<&Vec<Entity>> {
        self.state.vault_entities.get(&vault)
    }

    /// Writes snapshot to file with zstd compression.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotError::Codec`] if serialization of state data or header fails.
    /// Returns [`SnapshotError::Io`] if file creation, compression, or writing fails.
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

    /// Reads snapshot from file.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotError::NotFound`] if the file does not exist.
    /// Returns [`SnapshotError::Io`] if file reading or decompression fails.
    /// Returns [`SnapshotError::Codec`] if deserialization of header or state fails.
    /// Returns [`SnapshotError::InvalidMagic`] if the file is not a valid snapshot.
    /// Returns [`SnapshotError::UnsupportedVersion`] if the format version is unknown.
    /// Returns [`SnapshotError::ChecksumMismatch`] if data integrity verification fails.
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

/// Returns the filename for a snapshot at the given region height.
pub fn snapshot_filename(region_height: u64) -> String {
    format!("{:09}.snap", region_height)
}

/// Parses region height from snapshot filename.
///
/// Returns `None` if the filename does not match the expected format.
pub fn parse_snapshot_filename(filename: &str) -> Option<u64> {
    filename.strip_suffix(".snap").and_then(|s| s.parse().ok())
}

/// Snapshot manager for a region.
pub struct SnapshotManager {
    /// Directory containing snapshots.
    snapshot_dir: PathBuf,
    /// Maximum number of snapshots to retain.
    max_snapshots: usize,
}

impl SnapshotManager {
    /// Creates a new snapshot manager.
    pub fn new(snapshot_dir: PathBuf, max_snapshots: usize) -> Self {
        Self { snapshot_dir, max_snapshots }
    }

    /// Returns the snapshot directory path.
    pub fn snapshot_dir(&self) -> &Path {
        &self.snapshot_dir
    }

    /// Ensures the snapshot directory exists.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotError::Io`] if directory creation fails.
    pub fn init(&self) -> Result<()> {
        fs::create_dir_all(&self.snapshot_dir).context(IoSnafu)?;
        Ok(())
    }

    /// Saves a snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotError::Io`] if directory creation or file writing fails.
    /// Returns [`SnapshotError::Codec`] if serialization fails.
    pub fn save(&self, snapshot: &Snapshot) -> Result<PathBuf> {
        self.init()?;

        let filename = snapshot_filename(snapshot.region_height());
        let path = self.snapshot_dir.join(&filename);

        snapshot.write_to_file(&path)?;

        // Prune old snapshots
        self.prune()?;

        Ok(path)
    }

    /// Loads the latest snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotError::Io`] if listing or reading snapshot files fails.
    /// Returns any [`SnapshotError`] variant from [`Snapshot::read_from_file`].
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

    /// Loads a snapshot by height.
    ///
    /// # Errors
    ///
    /// Returns any [`SnapshotError`] variant from [`Snapshot::read_from_file`].
    pub fn load(&self, region_height: u64) -> Result<Snapshot> {
        let path = self.snapshot_dir.join(snapshot_filename(region_height));
        Snapshot::read_from_file(&path)
    }

    /// Lists available snapshot heights (sorted ascending).
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotError::Io`] if reading the snapshot directory fails.
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

    /// Prunes old snapshots, keeping only the most recent `max_snapshots`.
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

    /// Finds the latest snapshot at or before the given height.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotError::Io`] if listing snapshot files fails.
    pub fn find_snapshot_at_or_before(&self, height: u64) -> Result<Option<u64>> {
        let snapshots = self.list_snapshots()?;
        // list_snapshots returns ascending order, so reverse and find first <= height
        Ok(snapshots.into_iter().rev().find(|&h| h <= height))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods, clippy::panic)]
mod tests {
    use inferadb_ledger_types::EMPTY_HASH;
    use tempfile::TempDir;

    use super::*;

    fn create_test_snapshot() -> Snapshot {
        let vault_states = vec![VaultSnapshotMeta {
            vault: VaultId::new(1),
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

        Snapshot::new_simple(Region::GLOBAL, 1000, vault_states, state).expect("create snapshot")
    }

    #[test]
    fn test_snapshot_roundtrip() {
        let temp = TempDir::new().expect("create temp dir");
        let path = temp.path().join("test.snap");

        let snapshot = create_test_snapshot();
        snapshot.write_to_file(&path).expect("write snapshot");

        let loaded = Snapshot::read_from_file(&path).expect("read snapshot");

        assert_eq!(loaded.header.region, snapshot.header.region);
        assert_eq!(loaded.header.region_height, snapshot.header.region_height);
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
                vault: VaultId::new(1),
                vault_height: height / 2,
                state_root: [height as u8; 32],
                bucket_roots: [EMPTY_HASH; NUM_BUCKETS].to_vec(),
                key_count: 0,
            }];

            let snapshot = Snapshot::new_simple(
                Region::GLOBAL,
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
        assert_eq!(latest.region_height(), 500);

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
            vault: VaultId::new(1),
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
            erased_users: HashSet::new(),
        };

        let snapshot = Snapshot::new(Region::GLOBAL, 1000, vault_states, state, chain_params)
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

    /// Tests that consecutive snapshots form a valid chain.
    ///
    /// Snapshot N+1 references snapshot N's height and hash, creating a
    /// verifiable chain of state commitments.
    #[test]
    fn test_snapshot_chain_continuity() {
        let temp = TempDir::new().expect("create temp dir");
        let manager = SnapshotManager::new(temp.path().to_path_buf(), 10);

        let vault_states = vec![VaultSnapshotMeta {
            vault: VaultId::new(1),
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
            erased_users: HashSet::new(),
        };

        let snap1 =
            Snapshot::new(Region::GLOBAL, 500, vault_states.clone(), state.clone(), chain_params_1)
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
            erased_users: HashSet::new(),
        };

        let snap2 = Snapshot::new(Region::GLOBAL, 1000, vault_states, state, chain_params_2)
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

    /// Tests that vault state roots are preserved across snapshot roundtrips
    /// with multiple vaults.
    ///
    /// Each vault maintains an independent state root in the snapshot, which
    /// must survive serialization. Verifiers use these roots to validate
    /// individual vault states without processing the entire region.
    #[test]
    fn test_snapshot_preserves_multiple_vault_state_roots() {
        let temp = TempDir::new().expect("create temp dir");
        let path = temp.path().join("multi_vault.snap");

        let vault_states = vec![
            VaultSnapshotMeta {
                vault: VaultId::new(1),
                vault_height: 100,
                state_root: [0x11; 32],
                bucket_roots: [EMPTY_HASH; NUM_BUCKETS].to_vec(),
                key_count: 10,
            },
            VaultSnapshotMeta {
                vault: VaultId::new(2),
                vault_height: 200,
                state_root: [0x22; 32],
                bucket_roots: [EMPTY_HASH; NUM_BUCKETS].to_vec(),
                key_count: 20,
            },
            VaultSnapshotMeta {
                vault: VaultId::new(3),
                vault_height: 300,
                state_root: [0x33; 32],
                bucket_roots: [EMPTY_HASH; NUM_BUCKETS].to_vec(),
                key_count: 30,
            },
        ];

        let state = SnapshotStateData { vault_entities: HashMap::new() };

        let snapshot = Snapshot::new_simple(Region::GLOBAL, 5000, vault_states, state)
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

    #[test]
    fn test_snapshot_erased_users_roundtrip() {
        let temp = TempDir::new().expect("create temp dir");
        let path = temp.path().join("erasure.snap");

        let vault_states = vec![VaultSnapshotMeta {
            vault: VaultId::new(1),
            vault_height: 100,
            state_root: [0xAAu8; 32],
            bucket_roots: [EMPTY_HASH; NUM_BUCKETS].to_vec(),
            key_count: 5,
        }];
        let state = SnapshotStateData { vault_entities: HashMap::new() };

        // Create snapshot with erased users
        let mut erased = HashSet::new();
        erased.insert(UserId::new(42));
        erased.insert(UserId::new(99));

        let chain_params = SnapshotChainParams {
            genesis_hash: [0xBB; 32],
            previous_snapshot_height: None,
            previous_snapshot_hash: None,
            chain_commitment: ChainCommitment {
                accumulated_header_hash: [0xCC; 32],
                state_root_accumulator: [0xDD; 32],
                from_height: 1,
                to_height: 100,
            },
            erased_users: erased.clone(),
        };

        let snapshot = Snapshot::new(Region::GLOBAL, 100, vault_states, state, chain_params)
            .expect("create snapshot");
        snapshot.write_to_file(&path).expect("write snapshot");

        // Read back and verify erased_users survives roundtrip
        let loaded = Snapshot::read_from_file(&path).expect("read snapshot");
        assert_eq!(loaded.header.erased_users, erased);
        assert!(loaded.header.erased_users.contains(&UserId::new(42)));
        assert!(loaded.header.erased_users.contains(&UserId::new(99)));
        assert!(!loaded.header.erased_users.contains(&UserId::new(1)));
    }

    #[test]
    fn test_snapshot_backward_compat_no_erased_users() {
        // Verify that a snapshot created with default (empty) erased_users
        // deserializes correctly — serde(default) backwards compatibility
        let temp = TempDir::new().expect("create temp dir");
        let path = temp.path().join("no_erasure.snap");

        let vault_states = vec![VaultSnapshotMeta {
            vault: VaultId::new(1),
            vault_height: 50,
            state_root: [0x11u8; 32],
            bucket_roots: [EMPTY_HASH; NUM_BUCKETS].to_vec(),
            key_count: 1,
        }];
        let state = SnapshotStateData { vault_entities: HashMap::new() };

        let snapshot =
            Snapshot::new_simple(Region::GLOBAL, 50, vault_states, state).expect("create snapshot");
        assert!(snapshot.header.erased_users.is_empty());

        snapshot.write_to_file(&path).expect("write snapshot");
        let loaded = Snapshot::read_from_file(&path).expect("read snapshot");
        assert!(loaded.header.erased_users.is_empty());
    }

    #[test]
    fn test_snapshot_rmk_versions_roundtrip() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("rmk_versions.snap");

        let vault_states = vec![VaultSnapshotMeta {
            vault: VaultId::new(1),
            vault_height: 10,
            state_root: [0xAAu8; 32],
            bucket_roots: [EMPTY_HASH; NUM_BUCKETS].to_vec(),
            key_count: 3,
        }];
        let state = SnapshotStateData { vault_entities: HashMap::new() };
        let rmk_versions = vec![1, 3, 5];

        let snapshot = Snapshot::new_with_rmk_versions(
            Region::CA_CENTRAL_QC,
            10,
            vault_states,
            state,
            SnapshotChainParams::default(),
            rmk_versions.clone(),
        )
        .expect("create snapshot");

        assert_eq!(snapshot.header.rmk_versions, rmk_versions);

        snapshot.write_to_file(&path).expect("write snapshot");
        let loaded = Snapshot::read_from_file(&path).expect("read snapshot");
        assert_eq!(loaded.header.rmk_versions, rmk_versions);
    }

    #[test]
    fn test_snapshot_rmk_versions_default_empty() {
        let state = SnapshotStateData { vault_entities: HashMap::new() };
        let snapshot = Snapshot::new_simple(Region::GLOBAL, 1, vec![], state).expect("create");
        assert!(snapshot.header.rmk_versions.is_empty());
    }

    #[test]
    fn test_validate_transfer_protected_same_region_ok() {
        let state = SnapshotStateData { vault_entities: HashMap::new() };
        let snapshot =
            Snapshot::new_simple(Region::CA_CENTRAL_QC, 1, vec![], state).expect("create");
        // Same protected region → allowed
        assert!(snapshot.validate_transfer(Region::CA_CENTRAL_QC, true).is_ok());
    }

    #[test]
    fn test_validate_transfer_protected_different_region_rejected() {
        let state = SnapshotStateData { vault_entities: HashMap::new() };
        let snapshot =
            Snapshot::new_simple(Region::CA_CENTRAL_QC, 1, vec![], state).expect("create");
        // Different region → rejected for protected region
        let err = snapshot.validate_transfer(Region::US_EAST_VA, true).unwrap_err();
        assert!(matches!(err, SnapshotError::CrossRegionTransfer { .. }));
    }

    #[test]
    fn test_validate_transfer_non_protected_any_region_ok() {
        let state = SnapshotStateData { vault_entities: HashMap::new() };
        // GLOBAL is non-protected
        let snapshot = Snapshot::new_simple(Region::GLOBAL, 1, vec![], state).expect("create");
        assert!(snapshot.validate_transfer(Region::CA_CENTRAL_QC, false).is_ok());
    }

    #[test]
    fn test_validate_transfer_us_east_non_protected() {
        let state = SnapshotStateData { vault_entities: HashMap::new() };
        // US_EAST_VA is non-protected
        let snapshot = Snapshot::new_simple(Region::US_EAST_VA, 1, vec![], state).expect("create");
        assert!(snapshot.validate_transfer(Region::IE_EAST_DUBLIN, false).is_ok());
    }

    #[test]
    fn test_validate_restore_protected_same_region_ok() {
        let state = SnapshotStateData { vault_entities: HashMap::new() };
        let snapshot =
            Snapshot::new_simple(Region::IE_EAST_DUBLIN, 1, vec![], state).expect("create");
        assert!(snapshot.validate_restore(Region::IE_EAST_DUBLIN, true).is_ok());
    }

    #[test]
    fn test_validate_restore_protected_different_region_rejected() {
        let state = SnapshotStateData { vault_entities: HashMap::new() };
        let snapshot =
            Snapshot::new_simple(Region::IE_EAST_DUBLIN, 1, vec![], state).expect("create");
        let err = snapshot.validate_restore(Region::US_EAST_VA, true).unwrap_err();
        assert!(matches!(err, SnapshotError::CrossRegionRestore { .. }));
    }

    #[test]
    fn test_validate_restore_non_protected_any_region_ok() {
        let state = SnapshotStateData { vault_entities: HashMap::new() };
        let snapshot = Snapshot::new_simple(Region::US_WEST_OR, 1, vec![], state).expect("create");
        assert!(snapshot.validate_restore(Region::CA_CENTRAL_QC, false).is_ok());
    }

    #[test]
    fn test_validate_rmk_versions_all_available() {
        let state = SnapshotStateData { vault_entities: HashMap::new() };
        let snapshot = Snapshot::new_with_rmk_versions(
            Region::GLOBAL,
            1,
            vec![],
            state,
            SnapshotChainParams::default(),
            vec![1, 2, 3],
        )
        .expect("create");

        assert!(snapshot.validate_rmk_versions(&[1, 2, 3, 4]).is_ok());
    }

    #[test]
    fn test_validate_rmk_versions_missing() {
        let state = SnapshotStateData { vault_entities: HashMap::new() };
        let snapshot = Snapshot::new_with_rmk_versions(
            Region::CA_CENTRAL_QC,
            1,
            vec![],
            state,
            SnapshotChainParams::default(),
            vec![1, 2, 5],
        )
        .expect("create");

        let err = snapshot.validate_rmk_versions(&[1, 3]).unwrap_err();
        match err {
            SnapshotError::MissingRmkVersions { region, missing_versions } => {
                assert_eq!(region, Region::CA_CENTRAL_QC);
                assert_eq!(missing_versions, vec![2, 5]);
            },
            _ => panic!("unexpected error: {err:?}"),
        }
    }

    #[test]
    fn test_validate_rmk_versions_empty_always_ok() {
        let state = SnapshotStateData { vault_entities: HashMap::new() };
        let snapshot = Snapshot::new_simple(Region::GLOBAL, 1, vec![], state).expect("create");
        // Empty rmk_versions means unencrypted → always valid
        assert!(snapshot.validate_rmk_versions(&[]).is_ok());
        assert!(snapshot.validate_rmk_versions(&[1, 2]).is_ok());
    }
}
