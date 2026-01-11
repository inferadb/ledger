//! Table definitions for redb storage.
//!
//! All tables use byte arrays as keys and values for flexibility.
//! Key encoding is handled by the keys module.

use redb::TableDefinition;

/// Table definitions for ledger storage.
pub struct Tables;

impl Tables {
    // =========================================================================
    // State Tables (per-vault, bucket-prefixed keys)
    // =========================================================================

    /// Entity storage: key → serialized Entity
    /// Key format: {vault_id:8BE}{bucket_id:1}{local_key:var}
    pub const ENTITIES: TableDefinition<'static, &'static [u8], &'static [u8]> =
        TableDefinition::new("entities");

    /// Relationship primary storage: key → serialized Relationship
    /// Key format: {vault_id:8BE}{bucket_id:1}{rel_key:var}
    /// where rel_key = "rel:{resource}#{relation}@{subject}"
    pub const RELATIONSHIPS: TableDefinition<'static, &'static [u8], &'static [u8]> =
        TableDefinition::new("relationships");

    // =========================================================================
    // Index Tables
    // =========================================================================

    /// Object index: "Who can access doc:123 as viewer?"
    /// Key: {vault_id:8BE}obj_idx:{resource}#{relation}
    /// Value: serialized set of subjects
    pub const OBJ_INDEX: TableDefinition<'static, &'static [u8], &'static [u8]> =
        TableDefinition::new("obj_index");

    /// Subject index: "What can user:alice access?"
    /// Key: {vault_id:8BE}subj_idx:{subject}
    /// Value: serialized set of (resource, relation) pairs
    pub const SUBJ_INDEX: TableDefinition<'static, &'static [u8], &'static [u8]> =
        TableDefinition::new("subj_index");

    // =========================================================================
    // Block Archive Tables
    // =========================================================================

    /// Block storage: shard_height → serialized ShardBlock
    pub const BLOCKS: TableDefinition<'static, u64, &'static [u8]> = TableDefinition::new("blocks");

    /// Vault block index: (namespace_id, vault_id, vault_height) → shard_height
    /// Allows looking up the shard block containing a specific vault block
    pub const VAULT_BLOCK_INDEX: TableDefinition<'static, &'static [u8], u64> =
        TableDefinition::new("vault_block_index");

    // =========================================================================
    // Raft Tables
    // =========================================================================

    /// Raft log entries: log_index → serialized LogEntry
    pub const RAFT_LOG: TableDefinition<'static, u64, &'static [u8]> =
        TableDefinition::new("raft_log");

    /// Raft persistent state: key → value
    /// Keys: "vote", "commit_index", "last_applied", etc.
    pub const RAFT_STATE: TableDefinition<'static, &'static str, &'static [u8]> =
        TableDefinition::new("raft_state");

    // =========================================================================
    // Metadata Tables
    // =========================================================================

    /// Vault metadata: vault_id → serialized VaultMeta
    /// Stores current height, state_root, bucket_roots, etc.
    pub const VAULT_META: TableDefinition<'static, i64, &'static [u8]> =
        TableDefinition::new("vault_meta");

    /// Namespace metadata: namespace_id → serialized NamespaceMeta
    pub const NAMESPACE_META: TableDefinition<'static, i64, &'static [u8]> =
        TableDefinition::new("namespace_meta");

    /// Sequence counters: key → u64
    /// Keys: "_meta:seq:namespace", "_meta:seq:vault", etc.
    pub const SEQUENCES: TableDefinition<'static, &'static str, u64> =
        TableDefinition::new("sequences");

    /// Client sequence tracking: client_id → (last_sequence, result_hash)
    /// For idempotency checking
    pub const CLIENT_SEQUENCES: TableDefinition<'static, &'static str, &'static [u8]> =
        TableDefinition::new("client_sequences");
}

#[cfg(test)]
mod tests {
    use super::*;
    use redb::TableHandle;

    #[test]
    fn test_table_names_unique() {
        // Verify all table names are distinct
        let names = [
            Tables::ENTITIES.name(),
            Tables::RELATIONSHIPS.name(),
            Tables::OBJ_INDEX.name(),
            Tables::SUBJ_INDEX.name(),
            Tables::BLOCKS.name(),
            Tables::VAULT_BLOCK_INDEX.name(),
            Tables::RAFT_LOG.name(),
            Tables::RAFT_STATE.name(),
            Tables::VAULT_META.name(),
            Tables::NAMESPACE_META.name(),
            Tables::SEQUENCES.name(),
            Tables::CLIENT_SEQUENCES.name(),
        ];

        let mut sorted = names.to_vec();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), names.len(), "Table names must be unique");
    }
}
