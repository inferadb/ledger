//! Fixed table definitions for the store engine.
//!
//! The store has exactly 20 tables, all known at compile time.
//! This enables type-safe access and eliminates dynamic table lookup overhead.

use crate::types::KeyType;

/// Compile-time table identifier. All tables are statically defined; dynamic creation is not
/// supported.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum TableId {
    // ========================================================================
    // State Tables (per-vault, bucket-prefixed keys)
    // ========================================================================
    /// Entity storage: key = {vault_id:8BE}{bucket_id:1}{local_key:var}
    Entities = 0,

    /// Relationship storage for authorization tuples.
    Relationships = 1,

    /// Object-centric index: "Who can access resource X as relation Y?"
    ObjIndex = 2,

    /// Subject-centric index: "What can subject Z access?"
    SubjIndex = 3,

    // ========================================================================
    // Block Archive Tables
    // ========================================================================
    /// Block storage: shard_height -> serialized ShardBlock
    Blocks = 4,

    /// Vault block index: (organization_id, vault_id, vault_height) -> shard_height
    VaultBlockIndex = 5,

    // ========================================================================
    // Raft Tables
    // ========================================================================
    /// Raft log entries: log_index -> serialized LogEntry
    RaftLog = 6,

    /// Raft state metadata: "vote", "committed", etc.
    RaftState = 7,

    // ========================================================================
    // Metadata Tables
    // ========================================================================
    /// Vault metadata: vault_id -> VaultMeta
    VaultMeta = 8,

    /// Organization metadata: organization_id -> OrganizationMeta
    OrganizationMeta = 9,

    /// Sequence counters for ID generation.
    Sequences = 10,

    /// Client sequence tracking for idempotency.
    ClientSequences = 11,

    /// Block compaction watermark tracking.
    CompactionMeta = 12,

    // ========================================================================
    // Time-Travel Tables
    // ========================================================================
    /// Time-travel configuration per vault.
    TimeTravelConfig = 13,

    /// Time-travel index entries for historical lookups.
    TimeTravelIndex = 14,

    // ========================================================================
    // Organization Index Tables
    // ========================================================================
    /// Organization slug → internal ID mapping for external identifier resolution.
    OrganizationSlugIndex = 15,

    // ========================================================================
    // Vault Index Tables
    // ========================================================================
    /// Vault slug → internal ID mapping for external identifier resolution.
    VaultSlugIndex = 16,

    // ========================================================================
    // Vault-Scoped State Tables (composite key: {organization_id:8BE}{vault_id:8BE})
    // ========================================================================
    /// Vault block heights: composite key → `u64` vault block height.
    VaultHeights = 17,

    /// Vault hashes: composite key → `[u8; 32]` SHA-256 hash.
    VaultHashes = 18,

    /// Vault health status: composite key → postcard `VaultHealthStatus`.
    VaultHealth = 19,
}

impl TableId {
    /// Total number of tables.
    pub const COUNT: usize = 20;

    /// Returns the key type for this table.
    #[inline]
    pub const fn key_type(self) -> KeyType {
        match self {
            // u64 keys
            Self::Blocks | Self::RaftLog => KeyType::U64,

            // i64 keys (transformed for lexicographic ordering)
            Self::VaultMeta | Self::OrganizationMeta => KeyType::I64,

            // String keys
            Self::RaftState | Self::Sequences | Self::CompactionMeta => KeyType::Str,

            // Byte slice keys (composite keys with big-endian integer prefixes)
            Self::Entities
            | Self::Relationships
            | Self::ObjIndex
            | Self::SubjIndex
            | Self::VaultBlockIndex
            | Self::TimeTravelIndex
            | Self::ClientSequences
            | Self::VaultHeights
            | Self::VaultHashes
            | Self::VaultHealth => KeyType::Bytes,

            // u64 keys (time-travel config uses vault_id)
            Self::TimeTravelConfig => KeyType::U64,

            // u64 keys (organization slug → internal ID)
            Self::OrganizationSlugIndex => KeyType::U64,

            // u64 keys (vault slug → internal ID)
            Self::VaultSlugIndex => KeyType::U64,
        }
    }

    /// Returns the human-readable name for this table.
    #[inline]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Entities => "entities",
            Self::Relationships => "relationships",
            Self::ObjIndex => "obj_index",
            Self::SubjIndex => "subj_index",
            Self::Blocks => "blocks",
            Self::VaultBlockIndex => "vault_block_index",
            Self::RaftLog => "raft_log",
            Self::RaftState => "raft_state",
            Self::VaultMeta => "vault_meta",
            Self::OrganizationMeta => "organization_meta",
            Self::Sequences => "sequences",
            Self::ClientSequences => "client_sequences",
            Self::CompactionMeta => "compaction_meta",
            Self::TimeTravelConfig => "time_travel_config",
            Self::TimeTravelIndex => "time_travel_index",
            Self::OrganizationSlugIndex => "organization_slug_index",
            Self::VaultSlugIndex => "vault_slug_index",
            Self::VaultHeights => "vault_heights",
            Self::VaultHashes => "vault_hashes",
            Self::VaultHealth => "vault_health",
        }
    }

    /// Returns all table IDs.
    pub const fn all() -> [TableId; Self::COUNT] {
        [
            Self::Entities,
            Self::Relationships,
            Self::ObjIndex,
            Self::SubjIndex,
            Self::Blocks,
            Self::VaultBlockIndex,
            Self::RaftLog,
            Self::RaftState,
            Self::VaultMeta,
            Self::OrganizationMeta,
            Self::Sequences,
            Self::ClientSequences,
            Self::CompactionMeta,
            Self::TimeTravelConfig,
            Self::TimeTravelIndex,
            Self::OrganizationSlugIndex,
            Self::VaultSlugIndex,
            Self::VaultHeights,
            Self::VaultHashes,
            Self::VaultHealth,
        ]
    }

    /// Converts from u8 to TableId.
    #[inline]
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Entities),
            1 => Some(Self::Relationships),
            2 => Some(Self::ObjIndex),
            3 => Some(Self::SubjIndex),
            4 => Some(Self::Blocks),
            5 => Some(Self::VaultBlockIndex),
            6 => Some(Self::RaftLog),
            7 => Some(Self::RaftState),
            8 => Some(Self::VaultMeta),
            9 => Some(Self::OrganizationMeta),
            10 => Some(Self::Sequences),
            11 => Some(Self::ClientSequences),
            12 => Some(Self::CompactionMeta),
            13 => Some(Self::TimeTravelConfig),
            14 => Some(Self::TimeTravelIndex),
            15 => Some(Self::OrganizationSlugIndex),
            16 => Some(Self::VaultSlugIndex),
            17 => Some(Self::VaultHeights),
            18 => Some(Self::VaultHashes),
            19 => Some(Self::VaultHealth),
            _ => None,
        }
    }
}

// ============================================================================
// Type-Safe Table Trait
// ============================================================================

/// Trait for compile-time type-safe table access.
///
/// Each table struct implements this trait with its specific key and value types.
/// Note: Key and Value are marker types - actual serialization is handled by the
/// Key/Value traits in types.rs.
pub trait Table {
    /// The table identifier.
    const ID: TableId;

    /// Key type marker for this table.
    type KeyType;

    /// Value type marker for this table.
    type ValueType;
}

// ============================================================================
// Table Definitions
// ============================================================================

/// Entities table: stores key-value entities per vault.
pub struct Entities;
impl Table for Entities {
    const ID: TableId = TableId::Entities;
    type KeyType = Vec<u8>;
    type ValueType = Vec<u8>;
}

/// Relationships table: stores authorization relationships.
pub struct Relationships;
impl Table for Relationships {
    const ID: TableId = TableId::Relationships;
    type KeyType = Vec<u8>;
    type ValueType = Vec<u8>;
}

/// Object index table: object-centric relationship lookups.
pub struct ObjIndex;
impl Table for ObjIndex {
    const ID: TableId = TableId::ObjIndex;
    type KeyType = Vec<u8>;
    type ValueType = Vec<u8>;
}

/// Subject index table: subject-centric relationship lookups.
pub struct SubjIndex;
impl Table for SubjIndex {
    const ID: TableId = TableId::SubjIndex;
    type KeyType = Vec<u8>;
    type ValueType = Vec<u8>;
}

/// Blocks table: stores serialized shard blocks.
pub struct Blocks;
impl Table for Blocks {
    const ID: TableId = TableId::Blocks;
    type KeyType = u64;
    type ValueType = Vec<u8>;
}

/// Vault block index: maps vault heights to shard heights.
pub struct VaultBlockIndex;
impl Table for VaultBlockIndex {
    const ID: TableId = TableId::VaultBlockIndex;
    type KeyType = Vec<u8>;
    type ValueType = u64;
}

/// Raft log table: stores Raft log entries.
pub struct RaftLog;
impl Table for RaftLog {
    const ID: TableId = TableId::RaftLog;
    type KeyType = u64;
    type ValueType = Vec<u8>;
}

/// Raft state table: stores Raft metadata.
pub struct RaftState;
impl Table for RaftState {
    const ID: TableId = TableId::RaftState;
    type KeyType = String;
    type ValueType = Vec<u8>;
}

/// Vault metadata table mapping vault IDs to serialized vault configuration.
pub struct VaultMeta;
impl Table for VaultMeta {
    const ID: TableId = TableId::VaultMeta;
    type KeyType = i64;
    type ValueType = Vec<u8>;
}

/// Organization metadata table mapping organization IDs to serialized organization configuration.
pub struct OrganizationMeta;
impl Table for OrganizationMeta {
    const ID: TableId = TableId::OrganizationMeta;
    type KeyType = i64;
    type ValueType = Vec<u8>;
}

/// Sequences table: ID generation counters.
pub struct Sequences;
impl Table for Sequences {
    const ID: TableId = TableId::Sequences;
    type KeyType = String;
    type ValueType = u64;
}

/// Client sequences table: idempotency tracking with composite byte keys.
///
/// Key format: `{org_id:8BE}{vault_id:8BE}{client_id:raw}` for correct lexicographic
/// ordering and collision-free encoding.
pub struct ClientSequences;
impl Table for ClientSequences {
    const ID: TableId = TableId::ClientSequences;
    type KeyType = Vec<u8>;
    type ValueType = Vec<u8>;
}

/// Compaction metadata table storing block compaction watermarks per vault.
pub struct CompactionMeta;
impl Table for CompactionMeta {
    const ID: TableId = TableId::CompactionMeta;
    type KeyType = String;
    type ValueType = u64;
}

/// Time-travel configuration table: per-vault config.
pub struct TimeTravelConfig;
impl Table for TimeTravelConfig {
    const ID: TableId = TableId::TimeTravelConfig;
    type KeyType = u64;
    type ValueType = Vec<u8>;
}

/// Time-travel index table: historical entity versions.
pub struct TimeTravelIndex;
impl Table for TimeTravelIndex {
    const ID: TableId = TableId::TimeTravelIndex;
    type KeyType = Vec<u8>;
    type ValueType = Vec<u8>;
}

/// Organization slug index: maps external slugs to internal organization IDs.
pub struct OrganizationSlugIndex;
impl Table for OrganizationSlugIndex {
    const ID: TableId = TableId::OrganizationSlugIndex;
    type KeyType = u64;
    type ValueType = Vec<u8>;
}

/// Vault slug index: maps external vault slugs to internal vault IDs.
pub struct VaultSlugIndex;
impl Table for VaultSlugIndex {
    const ID: TableId = TableId::VaultSlugIndex;
    type KeyType = u64;
    type ValueType = Vec<u8>;
}

/// Vault block heights: tracks per-vault block height.
///
/// Key: `{organization_id:8BE}{vault_id:8BE}`, Value: `u64` vault block height.
pub struct VaultHeights;
impl Table for VaultHeights {
    const ID: TableId = TableId::VaultHeights;
    type KeyType = Vec<u8>;
    type ValueType = Vec<u8>;
}

/// Vault hashes: tracks per-vault previous block hash.
///
/// Key: `{organization_id:8BE}{vault_id:8BE}`, Value: `[u8; 32]` SHA-256 hash.
pub struct VaultHashes;
impl Table for VaultHashes {
    const ID: TableId = TableId::VaultHashes;
    type KeyType = Vec<u8>;
    type ValueType = Vec<u8>;
}

/// Vault health status: tracks per-vault health state.
///
/// Key: `{organization_id:8BE}{vault_id:8BE}`, Value: postcard `VaultHealthStatus`.
pub struct VaultHealth;
impl Table for VaultHealth {
    const ID: TableId = TableId::VaultHealth;
    type KeyType = Vec<u8>;
    type ValueType = Vec<u8>;
}

// ============================================================================
// Table Directory Entry
// ============================================================================

/// On-disk representation of a table's metadata.
#[derive(Debug, Clone, Copy)]
pub struct TableEntry {
    /// Table identifier (0-19).
    pub table_id: TableId,
    /// Root page of the B-tree (0 = empty table).
    pub root_page: u64,
    /// Number of entries in the table.
    pub entry_count: u64,
}

impl TableEntry {
    /// Size of a serialized table entry in bytes.
    pub const SIZE: usize = 1 + 8 + 8; // table_id + root_page + entry_count

    /// Creates a new empty table entry.
    pub fn empty(table_id: TableId) -> Self {
        Self { table_id, root_page: 0, entry_count: 0 }
    }

    /// Serializes to bytes.
    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut buf = [0u8; Self::SIZE];
        buf[0] = self.table_id as u8;
        buf[1..9].copy_from_slice(&self.root_page.to_le_bytes());
        buf[9..17].copy_from_slice(&self.entry_count.to_le_bytes());
        buf
    }

    /// Deserializes from bytes.
    pub fn from_bytes(buf: &[u8]) -> Option<Self> {
        if buf.len() < Self::SIZE {
            return None;
        }
        let table_id = TableId::from_u8(buf[0])?;
        let root_page = u64::from_le_bytes(buf[1..9].try_into().ok()?);
        let entry_count = u64::from_le_bytes(buf[9..17].try_into().ok()?);
        Some(Self { table_id, root_page, entry_count })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_id_round_trip() {
        for id in TableId::all() {
            let byte = id as u8;
            let recovered = TableId::from_u8(byte).unwrap();
            assert_eq!(id, recovered);
        }
    }

    #[test]
    fn test_table_entry_serialization() {
        let entry = TableEntry { table_id: TableId::RaftLog, root_page: 12345, entry_count: 999 };
        let bytes = entry.to_bytes();
        let recovered = TableEntry::from_bytes(&bytes).unwrap();
        assert_eq!(entry.table_id, recovered.table_id);
        assert_eq!(entry.root_page, recovered.root_page);
        assert_eq!(entry.entry_count, recovered.entry_count);
    }

    #[test]
    fn test_new_vault_tables_round_trip() {
        for (id, expected_byte) in
            [(TableId::VaultHeights, 17u8), (TableId::VaultHashes, 18), (TableId::VaultHealth, 19)]
        {
            assert_eq!(id as u8, expected_byte);
            let recovered = TableId::from_u8(expected_byte).expect("from_u8 should succeed");
            assert_eq!(id, recovered);
        }
    }

    #[test]
    fn test_new_vault_tables_key_types() {
        assert_eq!(TableId::VaultHeights.key_type(), KeyType::Bytes);
        assert_eq!(TableId::VaultHashes.key_type(), KeyType::Bytes);
        assert_eq!(TableId::VaultHealth.key_type(), KeyType::Bytes);
    }

    #[test]
    fn test_client_sequences_key_type_is_bytes() {
        assert_eq!(TableId::ClientSequences.key_type(), KeyType::Bytes);
    }

    #[test]
    fn test_new_vault_tables_names() {
        assert_eq!(TableId::VaultHeights.name(), "vault_heights");
        assert_eq!(TableId::VaultHashes.name(), "vault_hashes");
        assert_eq!(TableId::VaultHealth.name(), "vault_health");
    }

    #[test]
    fn test_table_count_is_20() {
        assert_eq!(TableId::COUNT, 20);
        assert_eq!(TableId::all().len(), 20);
    }

    #[test]
    fn test_from_u8_rejects_out_of_range() {
        assert!(TableId::from_u8(20).is_none());
        assert!(TableId::from_u8(255).is_none());
    }

    #[test]
    fn test_directory_page_fits_minimum_page_size() {
        // With COUNT=20 tables, the directory occupies 20 * 17 = 340 bytes.
        // This must fit within the minimum 512-byte page size.
        let directory_size = TableId::COUNT * TableEntry::SIZE;
        assert_eq!(directory_size, 340);
        assert!(
            directory_size <= 512,
            "directory size {directory_size} exceeds minimum 512-byte page"
        );
    }

    #[test]
    fn test_table_roots_array_capacity() {
        // CommittedState.table_roots is [PageId; TableId::COUNT].
        // Verify COUNT matches the number of defined variants.
        let all = TableId::all();
        assert_eq!(all.len(), TableId::COUNT);

        // Verify all IDs are unique and sequential from 0.
        for (i, id) in all.iter().enumerate() {
            assert_eq!(*id as u8, i as u8, "table ID {id:?} should have discriminant {i}");
        }
    }
}
