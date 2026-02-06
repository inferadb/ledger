//! Fixed table definitions for the store engine.
//!
//! The store has exactly 13 tables, all known at compile time.
//! This enables type-safe access and eliminates dynamic table lookup overhead.

use crate::types::KeyType;

/// Table identifier - compile-time known, no dynamic creation.
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

    /// Vault block index: (namespace_id, vault_id, vault_height) -> shard_height
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

    /// Namespace metadata: namespace_id -> NamespaceMeta
    NamespaceMeta = 9,

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
}

impl TableId {
    /// Total number of tables.
    pub const COUNT: usize = 15;

    /// Get the key type for this table.
    #[inline]
    pub const fn key_type(self) -> KeyType {
        match self {
            // u64 keys
            Self::Blocks | Self::RaftLog => KeyType::U64,

            // i64 keys (transformed for lexicographic ordering)
            Self::VaultMeta | Self::NamespaceMeta => KeyType::I64,

            // String keys
            Self::RaftState | Self::Sequences | Self::ClientSequences | Self::CompactionMeta => {
                KeyType::Str
            },

            // Byte slice keys
            Self::Entities
            | Self::Relationships
            | Self::ObjIndex
            | Self::SubjIndex
            | Self::VaultBlockIndex
            | Self::TimeTravelIndex => KeyType::Bytes,

            // u64 keys (time-travel config uses vault_id)
            Self::TimeTravelConfig => KeyType::U64,
        }
    }

    /// Get the human-readable name for this table.
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
            Self::NamespaceMeta => "namespace_meta",
            Self::Sequences => "sequences",
            Self::ClientSequences => "client_sequences",
            Self::CompactionMeta => "compaction_meta",
            Self::TimeTravelConfig => "time_travel_config",
            Self::TimeTravelIndex => "time_travel_index",
        }
    }

    /// Get all table IDs.
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
            Self::NamespaceMeta,
            Self::Sequences,
            Self::ClientSequences,
            Self::CompactionMeta,
            Self::TimeTravelConfig,
            Self::TimeTravelIndex,
        ]
    }

    /// Convert from u8 to TableId.
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
            9 => Some(Self::NamespaceMeta),
            10 => Some(Self::Sequences),
            11 => Some(Self::ClientSequences),
            12 => Some(Self::CompactionMeta),
            13 => Some(Self::TimeTravelConfig),
            14 => Some(Self::TimeTravelIndex),
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

/// Vault metadata table.
pub struct VaultMeta;
impl Table for VaultMeta {
    const ID: TableId = TableId::VaultMeta;
    type KeyType = i64;
    type ValueType = Vec<u8>;
}

/// Namespace metadata table.
pub struct NamespaceMeta;
impl Table for NamespaceMeta {
    const ID: TableId = TableId::NamespaceMeta;
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

/// Client sequences table: idempotency tracking.
pub struct ClientSequences;
impl Table for ClientSequences {
    const ID: TableId = TableId::ClientSequences;
    type KeyType = String;
    type ValueType = Vec<u8>;
}

/// Compaction metadata table.
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

// ============================================================================
// Table Directory Entry
// ============================================================================

/// On-disk representation of a table's metadata.
#[derive(Debug, Clone, Copy)]
pub struct TableEntry {
    /// Table identifier (0-14).
    pub table_id: TableId,
    /// Root page of the B-tree (0 = empty table).
    pub root_page: u64,
    /// Number of entries in the table.
    pub entry_count: u64,
}

impl TableEntry {
    /// Size of a serialized table entry in bytes.
    pub const SIZE: usize = 1 + 8 + 8; // table_id + root_page + entry_count

    /// Create a new empty table entry.
    pub fn empty(table_id: TableId) -> Self {
        Self { table_id, root_page: 0, entry_count: 0 }
    }

    /// Serialize to bytes.
    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut buf = [0u8; Self::SIZE];
        buf[0] = self.table_id as u8;
        buf[1..9].copy_from_slice(&self.root_page.to_le_bytes());
        buf[9..17].copy_from_slice(&self.entry_count.to_le_bytes());
        buf
    }

    /// Deserialize from bytes.
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
}
