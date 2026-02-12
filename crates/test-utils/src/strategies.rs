//! Proptest strategies for InferaDB Ledger domain types.
//!
//! Reusable generators for property-based testing across crates. Strategies produce
//! well-formed domain values while exploring edge cases through random variation.
//!
//! # Usage
//!
//! ```no_run
//! use inferadb_ledger_test_utils::strategies;
//! use proptest::prelude::*;
//!
//! proptest! {
//!     #[test]
//!     fn my_property(op in strategies::arb_operation()) {
//!         // test invariant with randomly generated operation
//!     }
//! }
//! ```

use chrono::{DateTime, TimeZone, Utc};
use inferadb_ledger_types::types::{
    BlockHeader, ChainCommitment, Entity, NamespaceId, Operation, Relationship, SetCondition,
    ShardBlock, ShardId, Transaction, VaultBlock, VaultEntry, VaultId,
};
use proptest::prelude::*;

/// Generates an arbitrary entity key of 1-32 characters matching `[a-z][a-z0-9]{0,31}`.
pub fn arb_key() -> impl Strategy<Value = String> {
    "[a-z][a-z0-9]{0,31}"
}

/// Generates an arbitrary entity value of 0-255 random bytes.
pub fn arb_value() -> impl Strategy<Value = Vec<u8>> {
    proptest::collection::vec(any::<u8>(), 0..256)
}

/// Generates a small entity value of 0-31 random bytes for compact tests.
pub fn arb_small_value() -> impl Strategy<Value = Vec<u8>> {
    proptest::collection::vec(any::<u8>(), 0..32)
}

/// Generates an arbitrary resource identifier in `{type}:{key}` format.
///
/// Type is one of: `doc`, `folder`, `project`, `org`, `team`.
/// Key is 1-32 characters matching `[a-z][a-z0-9]{0,31}`.
pub fn arb_resource() -> impl Strategy<Value = String> {
    (prop::sample::select(vec!["doc", "folder", "project", "org", "team"]), arb_key())
        .prop_map(|(typ, id)| format!("{typ}:{id}"))
}

/// Generates an arbitrary relation name from: `viewer`, `editor`, `owner`, `member`, `admin`.
pub fn arb_relation() -> impl Strategy<Value = String> {
    prop::sample::select(vec![
        "viewer".to_string(),
        "editor".to_string(),
        "owner".to_string(),
        "member".to_string(),
        "admin".to_string(),
    ])
}

/// Generates an arbitrary subject identifier in `{type}:{key}` format.
///
/// Type is one of: `user`, `group`, `team`, `service`.
/// Key is 1-32 characters matching `[a-z][a-z0-9]{0,31}`.
pub fn arb_subject() -> impl Strategy<Value = String> {
    (prop::sample::select(vec!["user", "group", "team", "service"]), arb_key())
        .prop_map(|(typ, id)| format!("{typ}:{id}"))
}

/// Generates an arbitrary [`SetCondition`]: `MustNotExist`, `MustExist`, `VersionEquals(0..1M)`, or
/// `ValueEquals(0-32 bytes)`.
pub fn arb_set_condition() -> impl Strategy<Value = SetCondition> {
    prop_oneof![
        Just(SetCondition::MustNotExist),
        Just(SetCondition::MustExist),
        (0u64..1_000_000).prop_map(SetCondition::VersionEquals),
        arb_small_value().prop_map(SetCondition::ValueEquals),
    ]
}

/// Generates an arbitrary [`Operation`] covering all five variants with random payloads.
pub fn arb_operation() -> impl Strategy<Value = Operation> {
    prop_oneof![
        // SetEntity with optional condition and expiration
        (
            arb_key(),
            arb_small_value(),
            proptest::option::of(arb_set_condition()),
            proptest::option::of(1u64..u64::MAX),
        )
            .prop_map(|(key, value, condition, expires_at)| {
                Operation::SetEntity { key, value, condition, expires_at }
            }),
        // DeleteEntity
        arb_key().prop_map(|key| Operation::DeleteEntity { key }),
        // ExpireEntity
        (arb_key(), 1u64..u64::MAX)
            .prop_map(|(key, expired_at)| Operation::ExpireEntity { key, expired_at }),
        // CreateRelationship
        (arb_resource(), arb_relation(), arb_subject()).prop_map(
            |(resource, relation, subject)| {
                Operation::CreateRelationship { resource, relation, subject }
            }
        ),
        // DeleteRelationship
        (arb_resource(), arb_relation(), arb_subject()).prop_map(
            |(resource, relation, subject)| {
                Operation::DeleteRelationship { resource, relation, subject }
            }
        ),
    ]
}

/// Generates a vector of 1-20 arbitrary operations.
pub fn arb_operation_sequence() -> impl Strategy<Value = Vec<Operation>> {
    proptest::collection::vec(arb_operation(), 1..20)
}

/// Generates an arbitrary [`Entity`] with random key (0-32 bytes), value, version, and expiry.
pub fn arb_entity() -> impl Strategy<Value = Entity> {
    (arb_small_value(), arb_small_value(), any::<u64>(), any::<u64>())
        .prop_map(|(key, value, expires_at, version)| Entity { key, value, expires_at, version })
}

/// Generates an arbitrary [`Relationship`] with random resource, relation, and subject.
pub fn arb_relationship() -> impl Strategy<Value = Relationship> {
    (arb_resource(), arb_relation(), arb_subject())
        .prop_map(|(resource, relation, subject)| Relationship { resource, relation, subject })
}

/// Generates an arbitrary 32-byte hash (uniform random bytes).
pub fn arb_hash() -> impl Strategy<Value = [u8; 32]> {
    proptest::array::uniform32(any::<u8>())
}

/// Generates an arbitrary 16-byte transaction ID (uniform random bytes).
pub fn arb_tx_id() -> impl Strategy<Value = [u8; 16]> {
    proptest::array::uniform16(any::<u8>())
}

/// Generates an arbitrary [`DateTime<Utc>`] between 2020-01-01 and 2030-01-01.
pub fn arb_timestamp() -> impl Strategy<Value = DateTime<Utc>> {
    (1_577_836_800i64..1_893_456_000i64).prop_map(|secs| {
        Utc.timestamp_opt(secs, 0)
            .single()
            .unwrap_or_else(|| DateTime::<Utc>::from(std::time::UNIX_EPOCH))
    })
}

/// Generates an arbitrary [`NamespaceId`] in the range 1-9,999.
pub fn arb_namespace_id() -> impl Strategy<Value = NamespaceId> {
    (1i64..10_000).prop_map(NamespaceId::new)
}

/// Generates an arbitrary [`VaultId`] in the range 1-9,999.
pub fn arb_vault_id() -> impl Strategy<Value = VaultId> {
    (1i64..10_000).prop_map(VaultId::new)
}

/// Generates an arbitrary [`ShardId`] in the range 1-999.
pub fn arb_shard_id() -> impl Strategy<Value = ShardId> {
    (1u32..1_000).prop_map(ShardId::new)
}

/// Generates an arbitrary [`Transaction`] with 1-20 operations, random client ID, and actor.
pub fn arb_transaction() -> impl Strategy<Value = Transaction> {
    (
        arb_tx_id(),
        "[a-z]{3,10}", // client_id
        1u64..100_000, // sequence
        "[a-z]{3,10}", // actor
        arb_operation_sequence(),
        arb_timestamp(),
    )
        .prop_map(|(id, client_id, sequence, actor, operations, timestamp)| Transaction {
            id,
            client_id,
            sequence,
            actor,
            operations,
            timestamp,
        })
}

/// Generates an arbitrary [`BlockHeader`] with height 0-999,999 and random hashes.
pub fn arb_block_header() -> impl Strategy<Value = BlockHeader> {
    (
        0u64..1_000_000, // height
        arb_namespace_id(),
        arb_vault_id(),
        arb_hash(), // previous_hash
        arb_hash(), // tx_merkle_root
        arb_hash(), // state_root
        arb_timestamp(),
        0u64..1_000,     // term
        0u64..1_000_000, // committed_index
    )
        .prop_map(
            |(
                height,
                namespace_id,
                vault_id,
                previous_hash,
                tx_merkle_root,
                state_root,
                timestamp,
                term,
                committed_index,
            )| {
                BlockHeader {
                    height,
                    namespace_id,
                    vault_id,
                    previous_hash,
                    tx_merkle_root,
                    state_root,
                    timestamp,
                    term,
                    committed_index,
                }
            },
        )
}

/// Generates an arbitrary [`VaultBlock`] with a random header and 0-4 transactions.
pub fn arb_vault_block() -> impl Strategy<Value = VaultBlock> {
    (arb_block_header(), proptest::collection::vec(arb_transaction(), 0..5))
        .prop_map(|(header, transactions)| VaultBlock { header, transactions })
}

/// Generates an arbitrary [`VaultEntry`] with height 0-999,999 and 0-2 transactions.
pub fn arb_vault_entry() -> impl Strategy<Value = VaultEntry> {
    (
        arb_namespace_id(),
        arb_vault_id(),
        0u64..1_000_000,
        arb_hash(),
        proptest::collection::vec(arb_transaction(), 0..3),
        arb_hash(),
        arb_hash(),
    )
        .prop_map(
            |(
                namespace_id,
                vault_id,
                vault_height,
                previous_vault_hash,
                transactions,
                tx_merkle_root,
                state_root,
            )| {
                VaultEntry {
                    namespace_id,
                    vault_id,
                    vault_height,
                    previous_vault_hash,
                    transactions,
                    tx_merkle_root,
                    state_root,
                }
            },
        )
}

/// Generates an arbitrary [`ShardBlock`] with height 0-999,999 and 0-2 vault entries.
pub fn arb_shard_block() -> impl Strategy<Value = ShardBlock> {
    (
        arb_shard_id(),
        0u64..1_000_000,
        arb_hash(),
        proptest::collection::vec(arb_vault_entry(), 0..3),
        arb_timestamp(),
        "[a-z]{3,10}",
        0u64..1_000,
        0u64..1_000_000,
    )
        .prop_map(
            |(
                shard_id,
                shard_height,
                previous_shard_hash,
                vault_entries,
                timestamp,
                leader_id,
                term,
                committed_index,
            )| {
                ShardBlock {
                    shard_id,
                    shard_height,
                    previous_shard_hash,
                    vault_entries,
                    timestamp,
                    leader_id,
                    term,
                    committed_index,
                }
            },
        )
}

/// Generates an arbitrary [`ChainCommitment`] with `from_height <= to_height` (both 0-499,999).
pub fn arb_chain_commitment() -> impl Strategy<Value = ChainCommitment> {
    (arb_hash(), arb_hash(), 0u64..500_000, 0u64..500_000).prop_map(
        |(accumulated_header_hash, state_root_accumulator, from, to)| {
            let (from_height, to_height) = if from <= to { (from, to) } else { (to, from) };
            ChainCommitment {
                accumulated_header_hash,
                state_root_accumulator,
                from_height,
                to_height,
            }
        },
    )
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    proptest! {
        #[test]
        fn strategy_produces_valid_operations(op in arb_operation()) {
            // All variants should be well-formed
            match &op {
                Operation::SetEntity { key, .. } => prop_assert!(!key.is_empty()),
                Operation::DeleteEntity { key } => prop_assert!(!key.is_empty()),
                Operation::ExpireEntity { key, .. } => prop_assert!(!key.is_empty()),
                Operation::CreateRelationship { resource, relation, subject } => {
                    prop_assert!(!resource.is_empty());
                    prop_assert!(!relation.is_empty());
                    prop_assert!(!subject.is_empty());
                }
                Operation::DeleteRelationship { resource, relation, subject } => {
                    prop_assert!(!resource.is_empty());
                    prop_assert!(!relation.is_empty());
                    prop_assert!(!subject.is_empty());
                }
            }
        }

        #[test]
        fn strategy_produces_valid_transactions(tx in arb_transaction()) {
            prop_assert!(!tx.client_id.is_empty());
            prop_assert!(tx.sequence > 0);
            prop_assert!(!tx.actor.is_empty());
            prop_assert!(!tx.operations.is_empty());
        }

        #[test]
        fn strategy_produces_valid_block_headers(header in arb_block_header()) {
            prop_assert!(header.namespace_id.value() > 0);
            prop_assert!(header.vault_id.value() > 0);
        }

        #[test]
        fn strategy_produces_valid_chain_commitments(cc in arb_chain_commitment()) {
            prop_assert!(cc.from_height <= cc.to_height);
        }
    }
}
