use std::collections::BTreeMap;

use inferadb_ledger_types::{
    CredentialType, EmailVerifyTokenId, InvitationStatus, InviteSlug, OrganizationId,
    OrganizationSlug, PasskeyCredential, RecoveryCodeCredential, TotpAlgorithm, TotpCredential,
    UserCredential, UserEmailId, UserSlug, VaultSlug,
    events::{EventAction, EventEmission, EventEntry, EventOutcome, EventScope},
    hash::Hash,
    merkle::MerkleProof as InternalMerkleProof,
    token::ValidatedToken,
};
use tonic::Status;

use super::*;
use crate::proto;

// -------------------------------------------------------------------------
// Vote conversion tests (LedgerNodeId)
// -------------------------------------------------------------------------

#[test]
fn vote_uncommitted_ledger_node_id_to_proto() {
    use inferadb_ledger_types::LedgerNodeId;
    use openraft::Vote;
    let vote: Vote<LedgerNodeId> = Vote::new(5, 42);
    let proto: proto::RaftVote = (&vote).into();

    assert_eq!(proto.term, 5);
    assert_eq!(proto.node_id, 42);
    assert!(!proto.committed);
}

#[test]
fn vote_committed_ledger_node_id_to_proto() {
    use inferadb_ledger_types::LedgerNodeId;
    use openraft::Vote;
    let vote: Vote<LedgerNodeId> = Vote::new_committed(10, 99);
    let proto: proto::RaftVote = (&vote).into();

    assert_eq!(proto.term, 10);
    assert_eq!(proto.node_id, 99);
    assert!(proto.committed);
}

#[test]
fn proto_to_vote_uncommitted_ledger_node_id() {
    use inferadb_ledger_types::LedgerNodeId;
    use openraft::Vote;
    let proto = proto::RaftVote { term: 7, node_id: 123, committed: false };
    let vote: Vote<LedgerNodeId> = proto.into();

    assert_eq!(vote.leader_id.term, 7);
    assert_eq!(vote.leader_id.voted_for().unwrap_or(0), 123);
    assert!(!vote.committed);
}

#[test]
fn proto_to_vote_committed_ledger_node_id() {
    use inferadb_ledger_types::LedgerNodeId;
    use openraft::Vote;
    let proto = proto::RaftVote { term: 15, node_id: 456, committed: true };
    let vote: Vote<LedgerNodeId> = proto.into();

    assert_eq!(vote.leader_id.term, 15);
    assert_eq!(vote.leader_id.voted_for().unwrap_or(0), 456);
    assert!(vote.committed);
}

#[test]
fn vote_uncommitted_ledger_node_id_roundtrip() {
    use inferadb_ledger_types::LedgerNodeId;
    use openraft::Vote;
    let original: Vote<LedgerNodeId> = Vote::new(100, 200);
    let proto: proto::RaftVote = (&original).into();
    let recovered: Vote<LedgerNodeId> = proto.into();

    assert_eq!(original.leader_id.term, recovered.leader_id.term);
    assert_eq!(
        original.leader_id.voted_for().unwrap_or(0),
        recovered.leader_id.voted_for().unwrap_or(0)
    );
    assert_eq!(original.committed, recovered.committed);
}

#[test]
fn vote_committed_ledger_node_id_roundtrip() {
    use inferadb_ledger_types::LedgerNodeId;
    use openraft::Vote;
    let original: Vote<LedgerNodeId> = Vote::new_committed(50, 60);
    let proto: proto::RaftVote = (&original).into();
    let recovered: Vote<LedgerNodeId> = proto.into();

    assert_eq!(original.leader_id.term, recovered.leader_id.term);
    assert_eq!(
        original.leader_id.voted_for().unwrap_or(0),
        recovered.leader_id.voted_for().unwrap_or(0)
    );
    assert_eq!(original.committed, recovered.committed);
}

// -------------------------------------------------------------------------
// SetCondition conversion tests
// -------------------------------------------------------------------------

#[test]
#[allow(clippy::type_complexity)]
fn test_set_condition_to_proto() {
    let cases: &[(&str, inferadb_ledger_types::SetCondition, Box<dyn Fn(&proto::SetCondition)>)] =
        &[
            (
                "MustNotExist",
                inferadb_ledger_types::SetCondition::MustNotExist,
                Box::new(|p| {
                    assert!(matches!(
                        p.condition,
                        Some(proto::set_condition::Condition::NotExists(true))
                    ));
                }),
            ),
            (
                "MustExist",
                inferadb_ledger_types::SetCondition::MustExist,
                Box::new(|p| {
                    assert!(matches!(
                        p.condition,
                        Some(proto::set_condition::Condition::MustExists(true))
                    ));
                }),
            ),
            (
                "VersionEquals(42)",
                inferadb_ledger_types::SetCondition::VersionEquals(42),
                Box::new(|p| {
                    assert!(matches!(
                        p.condition,
                        Some(proto::set_condition::Condition::Version(42))
                    ));
                }),
            ),
            (
                "ValueEquals([1,2,3,4])",
                inferadb_ledger_types::SetCondition::ValueEquals(vec![1, 2, 3, 4]),
                Box::new(|p| match &p.condition {
                    Some(proto::set_condition::Condition::ValueEquals(bytes)) => {
                        assert_eq!(bytes, &[1, 2, 3, 4]);
                    },
                    other => panic!("expected ValueEquals, got {other:?}"),
                }),
            ),
        ];

    for (label, condition, check) in cases {
        let proto: proto::SetCondition = condition.into();
        check(&proto);
        // Label used for diagnostics on failure
        let _ = label;
    }
}

// -------------------------------------------------------------------------
// Operation conversion tests
// -------------------------------------------------------------------------

#[test]
fn test_operation_create_relationship() {
    let op = inferadb_ledger_types::Operation::CreateRelationship {
        resource: "doc:123".to_string(),
        relation: "viewer".to_string(),
        subject: "user:456".to_string(),
    };
    let proto: proto::Operation = (&op).into();

    match proto.op {
        Some(proto::operation::Op::CreateRelationship(cr)) => {
            assert_eq!(cr.resource, "doc:123");
            assert_eq!(cr.relation, "viewer");
            assert_eq!(cr.subject, "user:456");
        },
        _ => panic!("Expected CreateRelationship operation"),
    }
}

#[test]
fn test_operation_delete_relationship() {
    let op = inferadb_ledger_types::Operation::DeleteRelationship {
        resource: "folder:abc".to_string(),
        relation: "editor".to_string(),
        subject: "team:xyz".to_string(),
    };
    let proto: proto::Operation = (&op).into();

    match proto.op {
        Some(proto::operation::Op::DeleteRelationship(dr)) => {
            assert_eq!(dr.resource, "folder:abc");
            assert_eq!(dr.relation, "editor");
            assert_eq!(dr.subject, "team:xyz");
        },
        _ => panic!("Expected DeleteRelationship operation"),
    }
}

#[test]
fn test_operation_set_entity_simple() {
    let op = inferadb_ledger_types::Operation::SetEntity {
        key: "config:timeout".to_string(),
        value: vec![0, 0, 0, 30],
        condition: None,
        expires_at: None,
    };
    let proto: proto::Operation = (&op).into();

    match proto.op {
        Some(proto::operation::Op::SetEntity(se)) => {
            assert_eq!(se.key, "config:timeout");
            assert_eq!(se.value, vec![0, 0, 0, 30]);
            assert!(se.condition.is_none());
            assert!(se.expires_at.is_none());
        },
        _ => panic!("Expected SetEntity operation"),
    }
}

#[test]
fn test_operation_set_entity_with_condition_and_ttl() {
    let op = inferadb_ledger_types::Operation::SetEntity {
        key: "session:abc".to_string(),
        value: vec![1, 2, 3],
        condition: Some(inferadb_ledger_types::SetCondition::MustNotExist),
        expires_at: Some(1700000000),
    };
    let proto: proto::Operation = (&op).into();

    match proto.op {
        Some(proto::operation::Op::SetEntity(se)) => {
            assert_eq!(se.key, "session:abc");
            assert_eq!(se.value, vec![1, 2, 3]);
            assert!(se.condition.is_some());
            assert_eq!(se.expires_at, Some(1700000000));
        },
        _ => panic!("Expected SetEntity operation"),
    }
}

#[test]
fn test_operation_delete_entity() {
    let op = inferadb_ledger_types::Operation::DeleteEntity { key: "temp:data".to_string() };
    let proto: proto::Operation = (&op).into();

    match proto.op {
        Some(proto::operation::Op::DeleteEntity(de)) => {
            assert_eq!(de.key, "temp:data");
        },
        _ => panic!("Expected DeleteEntity operation"),
    }
}

#[test]
fn test_operation_expire_entity() {
    let op = inferadb_ledger_types::Operation::ExpireEntity {
        key: "cache:item".to_string(),
        expired_at: 1699999999,
    };
    let proto: proto::Operation = (&op).into();

    match proto.op {
        Some(proto::operation::Op::ExpireEntity(ee)) => {
            assert_eq!(ee.key, "cache:item");
            assert_eq!(ee.expired_at, 1699999999);
        },
        _ => panic!("Expected ExpireEntity operation"),
    }
}

// -------------------------------------------------------------------------
// MerkleProof conversion tests
// -------------------------------------------------------------------------

#[test]
fn test_merkle_proof_empty() {
    let internal = InternalMerkleProof {
        leaf_index: 0,
        leaf_hash: Hash::default(),
        proof_hashes: vec![],
        root: Hash::default(),
    };
    let proto: proto::MerkleProof = (&internal).into();

    assert!(proto.leaf_hash.is_some());
    assert!(proto.siblings.is_empty());
}

#[test]
fn test_merkle_proof_single_sibling() {
    let leaf_hash = Hash::from([1u8; 32]);
    let sibling_hash = Hash::from([2u8; 32]);

    let internal = InternalMerkleProof {
        leaf_index: 0, // Even index = sibling is on the right
        leaf_hash,
        proof_hashes: vec![sibling_hash],
        root: Hash::default(),
    };
    let proto: proto::MerkleProof = (&internal).into();

    assert_eq!(proto.siblings.len(), 1);
    assert_eq!(proto.siblings[0].direction, proto::Direction::Right as i32);
}

#[test]
fn test_merkle_proof_direction_alternating() {
    let internal = InternalMerkleProof {
        leaf_index: 5, // Binary: 101 -> directions: Left, Right, Left
        leaf_hash: Hash::default(),
        proof_hashes: vec![Hash::default(), Hash::default(), Hash::default()],
        root: Hash::default(),
    };
    let proto: proto::MerkleProof = (&internal).into();

    assert_eq!(proto.siblings.len(), 3);
    // Index 5 is odd -> Left
    assert_eq!(proto.siblings[0].direction, proto::Direction::Left as i32);
    // Index 2 (5/2) is even -> Right
    assert_eq!(proto.siblings[1].direction, proto::Direction::Right as i32);
    // Index 1 (2/2) is odd -> Left
    assert_eq!(proto.siblings[2].direction, proto::Direction::Left as i32);
}

// -------------------------------------------------------------------------
// VaultSlug conversion tests
// -------------------------------------------------------------------------

#[test]
fn test_vault_slug_to_proto() {
    let slug = VaultSlug::new(123_456_789);
    let proto: proto::VaultSlug = slug.into();
    assert_eq!(proto.slug, 123_456_789);
}

#[test]
fn test_proto_to_vault_slug() {
    let proto = proto::VaultSlug { slug: 987_654_321 };
    let slug: VaultSlug = proto.into();
    assert_eq!(slug.value(), 987_654_321);
}

#[test]
fn test_proto_ref_to_vault_slug() {
    let proto = proto::VaultSlug { slug: 555_666_777 };
    let slug = VaultSlug::from(&proto);
    assert_eq!(slug.value(), 555_666_777);
}

#[test]
fn test_vault_slug_roundtrip() {
    let original = VaultSlug::new(42_000_000);
    let proto: proto::VaultSlug = original.into();
    let recovered = VaultSlug::from(proto);
    assert_eq!(original, recovered);
}

// -------------------------------------------------------------------------
// VaultEntry to Block conversion tests
// -------------------------------------------------------------------------

#[test]
fn test_vault_entry_to_proto_block_empty_transactions() {
    use chrono::Utc;

    let vault = VaultSlug::new(9_999_999);

    let entry = inferadb_ledger_types::VaultEntry {
        organization: inferadb_ledger_types::OrganizationId::new(1),
        vault: inferadb_ledger_types::VaultId::new(2),
        vault_height: 10,
        previous_vault_hash: Hash::from([0xABu8; 32]),
        transactions: vec![],
        tx_merkle_root: Hash::from([0xCDu8; 32]),
        state_root: Hash::from([0xEFu8; 32]),
    };

    let region_block = inferadb_ledger_types::RegionBlock {
        region: inferadb_ledger_types::Region::US_EAST_VA,
        region_height: 100,
        previous_region_hash: Hash::default(),
        vault_entries: vec![],
        timestamp: Utc::now(),
        leader_id: inferadb_ledger_types::NodeId::new("node-1"),
        term: 5,
        committed_index: 99,
    };

    let block = vault_entry_to_proto_block(&entry, &region_block, vault);

    let header = block.header.expect("Block should have header");
    assert_eq!(header.height, 10);
    assert_eq!(header.organization.unwrap().slug, 1);
    // Vault slug is the external Snowflake identifier, not the internal VaultId
    assert_eq!(header.vault.unwrap().slug, 9_999_999);
    assert_eq!(header.term, 5);
    assert_eq!(header.committed_index, 99);
    assert!(block.transactions.is_empty());
}

#[test]
fn test_vault_entry_to_proto_block_with_transaction() {
    use chrono::Utc;

    let vault = VaultSlug::new(77_777_777);

    let tx = inferadb_ledger_types::Transaction {
        id: *uuid::Uuid::new_v4().as_bytes(),
        client_id: inferadb_ledger_types::ClientId::new("client-123"),
        sequence: 1,
        operations: vec![inferadb_ledger_types::Operation::CreateRelationship {
            resource: "doc:1".to_string(),
            relation: "owner".to_string(),
            subject: "user:1".to_string(),
        }],
        timestamp: Utc::now(),
    };

    let entry = inferadb_ledger_types::VaultEntry {
        organization: inferadb_ledger_types::OrganizationId::new(5),
        vault: inferadb_ledger_types::VaultId::new(10),
        vault_height: 1,
        previous_vault_hash: Hash::default(),
        transactions: vec![tx],
        tx_merkle_root: Hash::default(),
        state_root: Hash::default(),
    };

    let region_block = inferadb_ledger_types::RegionBlock {
        region: inferadb_ledger_types::Region::US_EAST_VA,
        region_height: 50,
        previous_region_hash: Hash::default(),
        vault_entries: vec![],
        timestamp: Utc::now(),
        leader_id: inferadb_ledger_types::NodeId::new("leader-node"),
        term: 3,
        committed_index: 49,
    };

    let block = vault_entry_to_proto_block(&entry, &region_block, vault);

    let header = block.header.expect("Block should have header");
    assert_eq!(header.vault.unwrap().slug, 77_777_777);
    assert_eq!(block.transactions.len(), 1);
    let proto_tx = &block.transactions[0];
    assert_eq!(proto_tx.client_id.as_ref().unwrap().id, "client-123");
    assert_eq!(proto_tx.sequence, 1);
    assert_eq!(proto_tx.operations.len(), 1);
}

#[test]
fn test_vault_entry_to_proto_block_slug_independent_of_vault_id() {
    use chrono::Utc;

    // The vault slug is independent of the internal VaultId — verify they differ
    let vault = VaultSlug::new(12_345_678);

    let entry = inferadb_ledger_types::VaultEntry {
        organization: inferadb_ledger_types::OrganizationId::new(1),
        vault: inferadb_ledger_types::VaultId::new(99), // Internal ID is 99
        vault_height: 1,
        previous_vault_hash: Hash::default(),
        transactions: vec![],
        tx_merkle_root: Hash::default(),
        state_root: Hash::default(),
    };

    let region_block = inferadb_ledger_types::RegionBlock {
        region: inferadb_ledger_types::Region::US_EAST_VA,
        region_height: 1,
        previous_region_hash: Hash::default(),
        vault_entries: vec![],
        timestamp: Utc::now(),
        leader_id: inferadb_ledger_types::NodeId::new("node-1"),
        term: 1,
        committed_index: 0,
    };

    let block = vault_entry_to_proto_block(&entry, &region_block, vault);
    let header = block.header.unwrap();

    // Vault slug in header should be 12_345_678 (external), NOT 99 (internal)
    assert_eq!(header.vault.unwrap().slug, 12_345_678);
}

// -------------------------------------------------------------------------
// Proto -> Domain SetCondition conversion tests
// -------------------------------------------------------------------------

#[test]
fn test_proto_to_set_condition() {
    let cases: &[(
        &str,
        Option<proto::set_condition::Condition>,
        Option<inferadb_ledger_types::SetCondition>,
    )] = &[
        (
            "NotExists(true) → MustNotExist",
            Some(proto::set_condition::Condition::NotExists(true)),
            Some(inferadb_ledger_types::SetCondition::MustNotExist),
        ),
        (
            "MustExists(true) → MustExist",
            Some(proto::set_condition::Condition::MustExists(true)),
            Some(inferadb_ledger_types::SetCondition::MustExist),
        ),
        (
            "Version(42) → VersionEquals(42)",
            Some(proto::set_condition::Condition::Version(42)),
            Some(inferadb_ledger_types::SetCondition::VersionEquals(42)),
        ),
        (
            "ValueEquals([1,2,3]) → ValueEquals([1,2,3])",
            Some(proto::set_condition::Condition::ValueEquals(vec![1, 2, 3])),
            Some(inferadb_ledger_types::SetCondition::ValueEquals(vec![1, 2, 3])),
        ),
        ("None → None", None, None),
        (
            "NotExists(false) → MustExist (inverted)",
            Some(proto::set_condition::Condition::NotExists(false)),
            Some(inferadb_ledger_types::SetCondition::MustExist),
        ),
        (
            "MustExists(false) → MustNotExist (inverted)",
            Some(proto::set_condition::Condition::MustExists(false)),
            Some(inferadb_ledger_types::SetCondition::MustNotExist),
        ),
    ];

    for (label, proto_condition, expected) in cases {
        let proto_sc = proto::SetCondition { condition: proto_condition.clone() };
        let result: Option<inferadb_ledger_types::SetCondition> = (&proto_sc).into();
        assert_eq!(result, *expected, "case: {label}");
    }
}

// -------------------------------------------------------------------------
// Proto -> Domain Operation conversion tests (TryFrom)
// -------------------------------------------------------------------------

#[test]
fn test_try_from_create_relationship() {
    let proto_op = proto::Operation {
        op: Some(proto::operation::Op::CreateRelationship(proto::CreateRelationship {
            resource: "doc:1".to_string(),
            relation: "viewer".to_string(),
            subject: "user:2".to_string(),
        })),
    };
    let result = inferadb_ledger_types::Operation::try_from(&proto_op).unwrap();
    assert_eq!(
        result,
        inferadb_ledger_types::Operation::CreateRelationship {
            resource: "doc:1".to_string(),
            relation: "viewer".to_string(),
            subject: "user:2".to_string(),
        }
    );
}

#[test]
fn test_try_from_set_entity_with_condition() {
    let proto_op = proto::Operation {
        op: Some(proto::operation::Op::SetEntity(proto::SetEntity {
            key: "key:1".to_string(),
            value: vec![1, 2, 3],
            condition: Some(proto::SetCondition {
                condition: Some(proto::set_condition::Condition::Version(5)),
            }),
            expires_at: Some(1700000000),
        })),
    };
    let result = inferadb_ledger_types::Operation::try_from(&proto_op).unwrap();
    assert_eq!(
        result,
        inferadb_ledger_types::Operation::SetEntity {
            key: "key:1".to_string(),
            value: vec![1, 2, 3],
            condition: Some(inferadb_ledger_types::SetCondition::VersionEquals(5)),
            expires_at: Some(1700000000),
        }
    );
}

#[test]
fn test_try_from_missing_op_field() {
    let proto_op = proto::Operation { op: None };
    let result = inferadb_ledger_types::Operation::try_from(&proto_op);
    assert!(result.is_err());
    let status = result.unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

#[test]
fn test_try_from_delete_entity() {
    let proto_op = proto::Operation {
        op: Some(proto::operation::Op::DeleteEntity(proto::DeleteEntity {
            key: "key:del".to_string(),
        })),
    };
    let result = inferadb_ledger_types::Operation::try_from(&proto_op).unwrap();
    assert_eq!(
        result,
        inferadb_ledger_types::Operation::DeleteEntity { key: "key:del".to_string() }
    );
}

#[test]
fn test_try_from_expire_entity() {
    let proto_op = proto::Operation {
        op: Some(proto::operation::Op::ExpireEntity(proto::ExpireEntity {
            key: "key:exp".to_string(),
            expired_at: 1699999999,
        })),
    };
    let result = inferadb_ledger_types::Operation::try_from(&proto_op).unwrap();
    assert_eq!(
        result,
        inferadb_ledger_types::Operation::ExpireEntity {
            key: "key:exp".to_string(),
            expired_at: 1699999999,
        }
    );
}

// -------------------------------------------------------------------------
// Entity conversion tests
// -------------------------------------------------------------------------

#[test]
fn test_entity_to_proto() {
    let entity = inferadb_ledger_types::Entity {
        key: b"test-key".to_vec(),
        value: vec![1, 2, 3],
        expires_at: 1700000000,
        version: 42,
    };
    let proto_entity: proto::Entity = (&entity).into();
    assert_eq!(proto_entity.key, "test-key");
    assert_eq!(proto_entity.value, vec![1, 2, 3]);
    assert_eq!(proto_entity.version, 42);
    assert_eq!(proto_entity.expires_at, Some(1700000000));
}

#[test]
fn test_entity_to_proto_never_expires() {
    let entity = inferadb_ledger_types::Entity {
        key: b"perm-key".to_vec(),
        value: vec![],
        expires_at: 0,
        version: 1,
    };
    let proto_entity: proto::Entity = (&entity).into();
    assert!(proto_entity.expires_at.is_none());
}

// -------------------------------------------------------------------------
// Relationship conversion tests
// -------------------------------------------------------------------------

#[test]
fn test_relationship_to_proto() {
    let rel = inferadb_ledger_types::Relationship {
        resource: "doc:123".to_string(),
        relation: "viewer".to_string(),
        subject: "user:456".to_string(),
    };
    let proto_rel: proto::Relationship = (&rel).into();
    assert_eq!(proto_rel.resource, "doc:123");
    assert_eq!(proto_rel.relation, "viewer");
    assert_eq!(proto_rel.subject, "user:456");
}

#[test]
fn test_relationship_to_proto_owned() {
    let rel = inferadb_ledger_types::Relationship {
        resource: "folder:abc".to_string(),
        relation: "editor".to_string(),
        subject: "team:xyz".to_string(),
    };
    let proto_rel: proto::Relationship = rel.into();
    assert_eq!(proto_rel.resource, "folder:abc");
    assert_eq!(proto_rel.relation, "editor");
    assert_eq!(proto_rel.subject, "team:xyz");
}

// -------------------------------------------------------------------------
// BlockRetentionPolicy conversion tests
// -------------------------------------------------------------------------

#[test]
fn test_retention_policy_full_roundtrip() {
    use inferadb_ledger_types::{BlockRetentionMode, BlockRetentionPolicy};
    let policy = BlockRetentionPolicy { mode: BlockRetentionMode::Full, retention_blocks: 10_000 };
    let proto_policy: proto::BlockRetentionPolicy = policy.into();
    let recovered = BlockRetentionPolicy::from(&proto_policy);
    assert_eq!(recovered.mode, BlockRetentionMode::Full);
    assert_eq!(recovered.retention_blocks, 10_000);
}

#[test]
fn test_retention_policy_compacted_roundtrip() {
    use inferadb_ledger_types::{BlockRetentionMode, BlockRetentionPolicy};
    let policy =
        BlockRetentionPolicy { mode: BlockRetentionMode::Compacted, retention_blocks: 5_000 };
    let proto_policy: proto::BlockRetentionPolicy = policy.into();
    let recovered = BlockRetentionPolicy::from(&proto_policy);
    assert_eq!(recovered.mode, BlockRetentionMode::Compacted);
    assert_eq!(recovered.retention_blocks, 5_000);
}

#[test]
fn test_retention_policy_zero_blocks_defaults() {
    use inferadb_ledger_types::BlockRetentionPolicy;
    let proto_policy = proto::BlockRetentionPolicy {
        mode: proto::BlockRetentionMode::Full.into(),
        retention_blocks: 0,
    };
    let recovered = BlockRetentionPolicy::from(&proto_policy);
    assert_eq!(recovered.retention_blocks, 10_000); // Default
}

#[test]
fn test_retention_mode_unspecified_defaults_to_full() {
    use inferadb_ledger_types::{BlockRetentionMode, BlockRetentionPolicy};
    let proto_policy = proto::BlockRetentionPolicy {
        mode: proto::BlockRetentionMode::Unspecified.into(),
        retention_blocks: 100,
    };
    let recovered = BlockRetentionPolicy::from(&proto_policy);
    assert_eq!(recovered.mode, BlockRetentionMode::Full);
}

// -------------------------------------------------------------------------
// EventScope conversion tests
// -------------------------------------------------------------------------

#[test]
fn test_event_scope_to_proto() {
    let cases: &[(&str, EventScope, proto::EventScope)] = &[
        ("System", EventScope::System, proto::EventScope::System),
        ("Organization", EventScope::Organization, proto::EventScope::Organization),
    ];

    for (label, domain, expected) in cases {
        let proto_scope = proto::EventScope::from(*domain);
        assert_eq!(proto_scope, *expected, "case: {label}");
    }
}

#[test]
fn test_event_scope_proto_to_domain() {
    let cases: &[(&str, proto::EventScope, EventScope)] = &[
        ("System → System", proto::EventScope::System, EventScope::System),
        ("Organization → Organization", proto::EventScope::Organization, EventScope::Organization),
        (
            "Unspecified → Organization (default)",
            proto::EventScope::Unspecified,
            EventScope::Organization,
        ),
    ];

    for (label, proto_scope, expected) in cases {
        assert_eq!(EventScope::from(*proto_scope), *expected, "case: {label}");
    }
}

// -------------------------------------------------------------------------
// EventOutcome conversion tests
// -------------------------------------------------------------------------

#[test]
fn test_event_outcome_to_proto() {
    let cases: &[(&str, EventOutcome, proto::EventOutcome)] = &[
        ("Success", EventOutcome::Success, proto::EventOutcome::Success),
        (
            "Failed",
            EventOutcome::Failed { code: "E1001".to_string(), detail: "disk full".to_string() },
            proto::EventOutcome::Failed,
        ),
        (
            "Denied",
            EventOutcome::Denied { reason: "rate limited".to_string() },
            proto::EventOutcome::Denied,
        ),
    ];

    for (label, outcome, expected) in cases {
        let proto_outcome = proto::EventOutcome::from(outcome);
        assert_eq!(proto_outcome, *expected, "case: {label}");
    }
}

// -------------------------------------------------------------------------
// EventEmissionPath conversion tests
// -------------------------------------------------------------------------

#[test]
fn test_event_emission_apply_phase_to_proto() {
    let proto_ep = proto::EventEmissionPath::from(&EventEmission::ApplyPhase);
    assert_eq!(proto_ep, proto::EventEmissionPath::EmissionPathApplyPhase);
}

#[test]
fn test_event_emission_handler_phase_to_proto() {
    let proto_ep = proto::EventEmissionPath::from(&EventEmission::HandlerPhase { node_id: 42 });
    assert_eq!(proto_ep, proto::EventEmissionPath::EmissionPathHandlerPhase);
}

// -------------------------------------------------------------------------
// EventEntry domain -> proto -> domain roundtrip tests
// -------------------------------------------------------------------------

#[test]
fn test_event_entry_success_roundtrip() {
    use std::collections::BTreeMap;

    use chrono::Utc;

    let entry = EventEntry {
        expires_at: 1_700_000_000,
        event_id: [1u8; 16],
        source_service: "ledger".to_string(),
        event_type: "ledger.vault.created".to_string(),
        timestamp: Utc::now(),
        scope: EventScope::Organization,
        action: EventAction::VaultCreated,
        emission: EventEmission::ApplyPhase,
        principal: "system".to_string(),
        organization_id: OrganizationId::new(42),
        organization: Some(OrganizationSlug::new(12345)),
        vault: Some(VaultSlug::new(67890)),
        outcome: EventOutcome::Success,
        details: BTreeMap::from([("key".to_string(), "val".to_string())]),
        block_height: Some(100),
        trace_id: Some("abc-123".to_string()),
        correlation_id: Some("corr-456".to_string()),
        operations_count: Some(5),
    };

    let proto_entry: proto::EventEntry = (&entry).into();
    let recovered = EventEntry::try_from(&proto_entry).unwrap();

    assert_eq!(recovered.event_id, entry.event_id);
    assert_eq!(recovered.source_service, entry.source_service);
    assert_eq!(recovered.event_type, entry.event_type);
    assert_eq!(recovered.scope, entry.scope);
    assert_eq!(recovered.action, entry.action);
    assert_eq!(recovered.emission, entry.emission);
    assert_eq!(recovered.principal, entry.principal);
    assert_eq!(recovered.organization, entry.organization);
    assert_eq!(recovered.vault, entry.vault);
    assert_eq!(recovered.outcome, entry.outcome);
    assert_eq!(recovered.details, entry.details);
    assert_eq!(recovered.block_height, entry.block_height);
    assert_eq!(recovered.trace_id, entry.trace_id);
    assert_eq!(recovered.correlation_id, entry.correlation_id);
    assert_eq!(recovered.operations_count, entry.operations_count);
    assert_eq!(recovered.expires_at, entry.expires_at);
}

#[test]
fn test_event_entry_failed_outcome_roundtrip() {
    use chrono::Utc;

    let entry = EventEntry {
        expires_at: 0,
        event_id: [2u8; 16],
        source_service: "ledger".to_string(),
        event_type: "ledger.write.committed".to_string(),
        timestamp: Utc::now(),
        scope: EventScope::Organization,
        action: EventAction::WriteCommitted,
        emission: EventEmission::ApplyPhase,
        principal: "user:1".to_string(),
        organization_id: OrganizationId::new(1),
        organization: Some(OrganizationSlug::new(100)),
        vault: None,
        outcome: EventOutcome::Failed {
            code: "E2001".to_string(),
            detail: "constraint violation".to_string(),
        },
        details: BTreeMap::new(),
        block_height: None,
        trace_id: None,
        correlation_id: None,
        operations_count: None,
    };

    let proto_entry: proto::EventEntry = (&entry).into();
    assert_eq!(proto_entry.error_code, Some("E2001".to_string()));
    assert_eq!(proto_entry.error_detail, Some("constraint violation".to_string()));
    assert!(proto_entry.denial_reason.is_none());

    let recovered = EventEntry::try_from(&proto_entry).unwrap();
    assert_eq!(recovered.outcome, entry.outcome);
}

#[test]
fn test_event_entry_denied_outcome_roundtrip() {
    use chrono::Utc;

    let entry = EventEntry {
        expires_at: 1_800_000_000,
        event_id: [3u8; 16],
        source_service: "ledger".to_string(),
        event_type: "ledger.request.rate_limited".to_string(),
        timestamp: Utc::now(),
        scope: EventScope::Organization,
        action: EventAction::RequestRateLimited,
        emission: EventEmission::HandlerPhase { node_id: 7 },
        principal: "client:abc".to_string(),
        organization_id: OrganizationId::new(5),
        organization: Some(OrganizationSlug::new(555)),
        vault: None,
        outcome: EventOutcome::Denied { reason: "rate limit exceeded".to_string() },
        details: BTreeMap::new(),
        block_height: None,
        trace_id: None,
        correlation_id: None,
        operations_count: None,
    };

    let proto_entry: proto::EventEntry = (&entry).into();
    assert!(proto_entry.error_code.is_none());
    assert!(proto_entry.error_detail.is_none());
    assert_eq!(proto_entry.denial_reason, Some("rate limit exceeded".to_string()));
    assert_eq!(proto_entry.node_id, Some(7));

    let recovered = EventEntry::try_from(&proto_entry).unwrap();
    assert_eq!(recovered.outcome, entry.outcome);
    assert_eq!(recovered.emission, entry.emission);
}

#[test]
fn test_event_entry_handler_phase_preserves_node_id() {
    use chrono::Utc;

    let entry = EventEntry {
        expires_at: 0,
        event_id: [4u8; 16],
        source_service: "ledger".to_string(),
        event_type: "ledger.request.quota_exceeded".to_string(),
        timestamp: Utc::now(),
        scope: EventScope::Organization,
        action: EventAction::QuotaExceeded,
        emission: EventEmission::HandlerPhase { node_id: 42 },
        principal: "system".to_string(),
        organization_id: OrganizationId::new(1),
        organization: None,
        vault: None,
        outcome: EventOutcome::Success,
        details: BTreeMap::new(),
        block_height: None,
        trace_id: None,
        correlation_id: None,
        operations_count: None,
    };

    let proto_entry: proto::EventEntry = (&entry).into();
    assert_eq!(
        proto_entry.emission_path,
        proto::EventEmissionPath::EmissionPathHandlerPhase as i32,
    );
    assert_eq!(proto_entry.node_id, Some(42));

    let recovered = EventEntry::try_from(&proto_entry).unwrap();
    assert_eq!(recovered.emission, EventEmission::HandlerPhase { node_id: 42 });
}

#[test]
fn test_event_entry_system_scope_roundtrip() {
    use chrono::Utc;

    let entry = EventEntry {
        expires_at: 1_700_000_000,
        event_id: [5u8; 16],
        source_service: "ledger".to_string(),
        event_type: "ledger.organization.created".to_string(),
        timestamp: Utc::now(),
        scope: EventScope::System,
        action: EventAction::OrganizationCreated,
        emission: EventEmission::ApplyPhase,
        principal: "system".to_string(),
        organization_id: OrganizationId::new(0),
        organization: Some(OrganizationSlug::new(0)),
        vault: None,
        outcome: EventOutcome::Success,
        details: BTreeMap::new(),
        block_height: Some(50),
        trace_id: None,
        correlation_id: None,
        operations_count: None,
    };

    let proto_entry: proto::EventEntry = (&entry).into();
    assert_eq!(proto_entry.scope, proto::EventScope::System as i32);

    let recovered = EventEntry::try_from(&proto_entry).unwrap();
    assert_eq!(recovered.scope, EventScope::System);
    assert_eq!(recovered.action, EventAction::OrganizationCreated);
}

#[test]
fn test_event_entry_invalid_event_id_length() {
    let proto_entry = proto::EventEntry {
        event_id: vec![1, 2, 3], // Only 3 bytes, need 16
        source_service: "ledger".to_string(),
        event_type: "ledger.vault.created".to_string(),
        action: "vault_created".to_string(),
        ..Default::default()
    };

    let result = EventEntry::try_from(&proto_entry);
    assert!(result.is_err());
    let status = result.unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

#[test]
fn test_event_entry_unknown_action() {
    let proto_entry = proto::EventEntry {
        event_id: vec![0u8; 16],
        source_service: "ledger".to_string(),
        event_type: "ledger.unknown.action".to_string(),
        action: "totally_unknown_action".to_string(),
        ..Default::default()
    };

    let result = EventEntry::try_from(&proto_entry);
    assert!(result.is_err());
    let status = result.unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

#[test]
fn test_event_entry_all_actions_roundtrip() {
    use chrono::Utc;

    for action in EventAction::ALL {
        let entry = EventEntry {
            expires_at: 0,
            event_id: [0u8; 16],
            source_service: "ledger".to_string(),
            event_type: action.event_type().to_string(),
            timestamp: Utc::now(),
            scope: action.scope(),
            action: *action,
            emission: EventEmission::ApplyPhase,
            principal: "test".to_string(),
            organization_id: OrganizationId::new(0),
            organization: None,
            vault: None,
            outcome: EventOutcome::Success,
            details: BTreeMap::new(),
            block_height: None,
            trace_id: None,
            correlation_id: None,
            operations_count: None,
        };

        let proto_entry: proto::EventEntry = (&entry).into();
        assert_eq!(proto_entry.action, action.as_str());

        let recovered = EventEntry::try_from(&proto_entry).unwrap();
        assert_eq!(recovered.action, *action, "failed for {:?}", action);
    }
}

// -------------------------------------------------------------------------
// Region conversion tests
// -------------------------------------------------------------------------

/// Mapping from every domain Region variant to its expected proto counterpart.
const REGION_PAIRS: [(inferadb_ledger_types::Region, proto::Region); 25] = {
    use inferadb_ledger_types::Region as D;
    [
        (D::GLOBAL, proto::Region::Global),
        (D::US_EAST_VA, proto::Region::UsEastVa),
        (D::US_WEST_OR, proto::Region::UsWestOr),
        (D::CA_CENTRAL_QC, proto::Region::CaCentralQc),
        (D::BR_SOUTHEAST_SP, proto::Region::BrSoutheastSp),
        (D::IE_EAST_DUBLIN, proto::Region::IeEastDublin),
        (D::FR_NORTH_PARIS, proto::Region::FrNorthParis),
        (D::DE_CENTRAL_FRANKFURT, proto::Region::DeCentralFrankfurt),
        (D::SE_EAST_STOCKHOLM, proto::Region::SeEastStockholm),
        (D::IT_NORTH_MILAN, proto::Region::ItNorthMilan),
        (D::UK_SOUTH_LONDON, proto::Region::UkSouthLondon),
        (D::SA_CENTRAL_RIYADH, proto::Region::SaCentralRiyadh),
        (D::BH_CENTRAL_MANAMA, proto::Region::BhCentralManama),
        (D::AE_CENTRAL_DUBAI, proto::Region::AeCentralDubai),
        (D::IL_CENTRAL_TEL_AVIV, proto::Region::IlCentralTelAviv),
        (D::ZA_SOUTH_CAPE_TOWN, proto::Region::ZaSouthCapeTown),
        (D::NG_WEST_LAGOS, proto::Region::NgWestLagos),
        (D::SG_CENTRAL_SINGAPORE, proto::Region::SgCentralSingapore),
        (D::AU_EAST_SYDNEY, proto::Region::AuEastSydney),
        (D::ID_WEST_JAKARTA, proto::Region::IdWestJakarta),
        (D::JP_EAST_TOKYO, proto::Region::JpEastTokyo),
        (D::KR_CENTRAL_SEOUL, proto::Region::KrCentralSeoul),
        (D::IN_WEST_MUMBAI, proto::Region::InWestMumbai),
        (D::VN_SOUTH_HCMC, proto::Region::VnSouthHcmc),
        (D::CN_NORTH_BEIJING, proto::Region::CnNorthBeijing),
    ]
};

#[test]
fn test_region_pairs_covers_all_variants() {
    assert_eq!(
        REGION_PAIRS.len(),
        inferadb_ledger_types::ALL_REGIONS.len(),
        "REGION_PAIRS is out of sync with ALL_REGIONS"
    );
}

#[test]
fn test_region_roundtrip_all_variants() {
    for (domain, expected_proto) in &REGION_PAIRS {
        // domain → proto
        let proto_region = proto::Region::from(*domain);
        assert_eq!(proto_region, *expected_proto, "domain→proto failed for {domain}");

        // proto → domain
        let recovered = inferadb_ledger_types::Region::try_from(proto_region).unwrap();
        assert_eq!(*domain, recovered, "proto→domain failed for {domain}");
    }
}

#[test]
fn test_region_unspecified_returns_error() {
    let result = inferadb_ledger_types::Region::try_from(proto::Region::Unspecified);
    let err = result.unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(err.message().contains("region must be specified"));
}

#[test]
fn test_region_from_i32_valid() {
    // Global = 1
    let region = region_from_i32(1).unwrap();
    assert_eq!(region, inferadb_ledger_types::Region::GLOBAL);

    // UsEastVa = 10
    let region = region_from_i32(10).unwrap();
    assert_eq!(region, inferadb_ledger_types::Region::US_EAST_VA);

    // CnNorthBeijing = 70
    let region = region_from_i32(70).unwrap();
    assert_eq!(region, inferadb_ledger_types::Region::CN_NORTH_BEIJING);
}

#[test]
fn test_region_from_i32_invalid_returns_error() {
    let cases: &[(&str, i32, &str)] = &[
        ("unspecified (0)", 0, "region must be specified"),
        ("unknown (999)", 999, "unknown region value: 999"),
        ("negative (-1)", -1, "unknown region value: -1"),
    ];

    for (label, value, expected_msg) in cases {
        let result = region_from_i32(*value);
        let err = result.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument, "case: {label}");
        assert!(err.message().contains(expected_msg), "case: {label}");
    }
}

#[test]
fn test_region_from_i32_all_variants_roundtrip() {
    // Test every known proto Region i32 value round-trips correctly
    let proto_values: [(i32, inferadb_ledger_types::Region); 25] = {
        use inferadb_ledger_types::Region as D;
        [
            (1, D::GLOBAL),
            (10, D::US_EAST_VA),
            (11, D::US_WEST_OR),
            (12, D::CA_CENTRAL_QC),
            (20, D::BR_SOUTHEAST_SP),
            (30, D::IE_EAST_DUBLIN),
            (31, D::FR_NORTH_PARIS),
            (32, D::DE_CENTRAL_FRANKFURT),
            (33, D::SE_EAST_STOCKHOLM),
            (34, D::IT_NORTH_MILAN),
            (40, D::UK_SOUTH_LONDON),
            (50, D::SA_CENTRAL_RIYADH),
            (51, D::BH_CENTRAL_MANAMA),
            (52, D::AE_CENTRAL_DUBAI),
            (53, D::IL_CENTRAL_TEL_AVIV),
            (54, D::ZA_SOUTH_CAPE_TOWN),
            (55, D::NG_WEST_LAGOS),
            (60, D::SG_CENTRAL_SINGAPORE),
            (61, D::AU_EAST_SYDNEY),
            (62, D::ID_WEST_JAKARTA),
            (63, D::JP_EAST_TOKYO),
            (64, D::KR_CENTRAL_SEOUL),
            (65, D::IN_WEST_MUMBAI),
            (66, D::VN_SOUTH_HCMC),
            (70, D::CN_NORTH_BEIJING),
        ]
    };

    for (i32_value, expected_domain) in &proto_values {
        let domain = region_from_i32(*i32_value).unwrap();
        assert_eq!(domain, *expected_domain, "failed for i32 value {i32_value}");
    }
}

// -------------------------------------------------------------------------
// Property-based roundtrip tests
// -------------------------------------------------------------------------

mod proptests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]

    use inferadb_ledger_test_utils::strategies::{
        arb_entity, arb_event_entry, arb_operation, arb_relationship, arb_set_condition,
        arb_vault_slug,
    };
    use inferadb_ledger_types::VaultSlug;
    use proptest::prelude::*;

    use super::*;

    proptest! {
        /// Operation roundtrip: domain -> proto -> domain preserves all data.
        #[test]
        fn operation_roundtrip(op in arb_operation()) {
            let proto: proto::Operation = (&op).into();
            let recovered = inferadb_ledger_types::Operation::try_from(&proto).unwrap();
            prop_assert_eq!(op, recovered);
        }

        /// SetCondition roundtrip: domain -> proto -> domain preserves all data.
        #[test]
        fn set_condition_roundtrip(cond in arb_set_condition()) {
            let proto: proto::SetCondition = (&cond).into();
            let recovered: Option<inferadb_ledger_types::SetCondition> = (&proto).into();
            prop_assert_eq!(Some(cond), recovered);
        }

        /// Relationship roundtrip: domain -> proto -> domain preserves fields.
        #[test]
        fn relationship_roundtrip(rel in arb_relationship()) {
            let proto: proto::Relationship = (&rel).into();
            let recovered = inferadb_ledger_types::Relationship {
                resource: proto.resource,
                relation: proto.relation,
                subject: proto.subject,
            };
            prop_assert_eq!(rel, recovered);
        }

        /// Entity roundtrip: domain -> proto -> domain preserves data
        /// (key encoding may differ for non-UTF8 keys via lossy conversion).
        #[test]
        fn entity_to_proto_preserves_value(entity in arb_entity()) {
            let proto: proto::Entity = (&entity).into();
            prop_assert_eq!(&proto.value, &entity.value);
            prop_assert_eq!(proto.version, entity.version);
            if entity.expires_at == 0 {
                prop_assert!(proto.expires_at.is_none());
            } else {
                prop_assert_eq!(proto.expires_at, Some(entity.expires_at));
            }
        }

        /// VaultSlug roundtrip: domain -> proto -> domain preserves value.
        #[test]
        fn vault_slug_roundtrip(slug in arb_vault_slug()) {
            let proto: proto::VaultSlug = slug.into();
            let recovered = VaultSlug::from(proto);
            prop_assert_eq!(slug, recovered);
        }

        /// EventEntry roundtrip: domain -> proto -> domain preserves all fields.
        ///
        /// Verifies that all `EventAction` variants, all `EventOutcome` variants,
        /// and all `EventEmission` variants survive the conversion.
        #[test]
        fn event_entry_proto_roundtrip(entry in arb_event_entry()) {
            let proto_entry: proto::EventEntry = (&entry).into();
            let recovered = EventEntry::try_from(&proto_entry).unwrap();

            prop_assert_eq!(&entry.event_id, &recovered.event_id);
            prop_assert_eq!(&entry.source_service, &recovered.source_service);
            prop_assert_eq!(&entry.event_type, &recovered.event_type);
            prop_assert_eq!(entry.scope, recovered.scope);
            prop_assert_eq!(entry.action, recovered.action);
            prop_assert_eq!(&entry.emission, &recovered.emission);
            prop_assert_eq!(&entry.principal, &recovered.principal);
            prop_assert_eq!(entry.vault, recovered.vault);
            prop_assert_eq!(&entry.outcome, &recovered.outcome);
            prop_assert_eq!(&entry.details, &recovered.details);
            prop_assert_eq!(entry.block_height, recovered.block_height);
            prop_assert_eq!(&entry.trace_id, &recovered.trace_id);
            prop_assert_eq!(&entry.correlation_id, &recovered.correlation_id);
            prop_assert_eq!(entry.operations_count, recovered.operations_count);
            prop_assert_eq!(entry.expires_at, recovered.expires_at);
        }
    }
}

// -------------------------------------------------------------------------
// UserStatus / UserRole conversions
// -------------------------------------------------------------------------

#[test]
fn user_status_roundtrip() {
    use inferadb_ledger_types::UserStatus;
    let statuses = [
        UserStatus::Active,
        UserStatus::PendingOrg,
        UserStatus::Suspended,
        UserStatus::Deleting,
        UserStatus::Deleted,
    ];
    for status in statuses {
        let proto_val: proto::UserStatus = status.into();
        let i32_val: i32 = proto_val.into();
        let back = user_status_from_i32(i32_val).expect("valid status");
        assert_eq!(status, back, "roundtrip failed for {status:?}");
    }
}

#[test]
fn user_role_roundtrip() {
    use inferadb_ledger_types::UserRole;
    let roles = [UserRole::User, UserRole::Admin];
    for role in roles {
        let proto_val: proto::UserRole = role.into();
        let i32_val: i32 = proto_val.into();
        let back = user_role_from_i32(i32_val).expect("valid role");
        assert_eq!(role, back, "roundtrip failed for {role:?}");
    }
}

#[test]
fn user_status_unspecified_rejected() {
    let result = user_status_from_i32(0); // 0 = Unspecified
    assert!(result.is_err());
}

#[test]
fn user_role_unspecified_treated_as_user() {
    use inferadb_ledger_types::UserRole;
    let result = user_role_from_i32(0); // 0 = Unspecified
    assert_eq!(result.expect("unspecified should map to User"), UserRole::User);
}

// -------------------------------------------------------------------------
// ValidatedToken conversion tests
// -------------------------------------------------------------------------

#[test]
fn validated_token_user_session_to_proto() {
    use inferadb_ledger_types::{
        TokenVersion,
        token::{TokenType, UserSessionClaims, ValidatedToken as DomainVT},
    };

    let claims = UserSessionClaims {
        iss: "inferadb".to_string(),
        sub: "user:42".to_string(),
        aud: vec!["inferadb-control".to_string()],
        exp: 1700001800,
        iat: 1700000000,
        nbf: 1700000000,
        jti: "test-jti".to_string(),
        token_type: TokenType::UserSession,
        user: UserSlug::new(42),
        role: "admin".to_string(),
        version: TokenVersion::new(1),
    };

    let resp: proto::ValidateTokenResponse = DomainVT::UserSession(claims).into();
    assert_eq!(resp.subject, "user:42");
    assert_eq!(resp.token_type, "user_session");
    assert_eq!(resp.expires_at.unwrap().seconds, 1700001800);

    let user_claims = match resp.claims.unwrap() {
        proto::validate_token_response::Claims::UserSession(c) => c,
        _ => panic!("expected UserSession claims"),
    };
    assert_eq!(user_claims.user_slug, 42);
    assert_eq!(user_claims.role, "admin");
}

#[test]
fn validated_token_vault_access_to_proto() {
    use inferadb_ledger_types::token::{TokenType, ValidatedToken as DomainVT, VaultTokenClaims};

    let claims = VaultTokenClaims {
        iss: "inferadb".to_string(),
        sub: "app:99".to_string(),
        aud: vec!["inferadb-engine".to_string()],
        exp: 1700000300,
        iat: 1700000000,
        nbf: 1700000000,
        jti: "test-jti-2".to_string(),
        token_type: TokenType::VaultAccess,
        org: OrganizationSlug::new(10),
        app: inferadb_ledger_types::AppSlug::new(99),
        vault: VaultSlug::new(50),
        scopes: vec!["vault:read".to_string()],
    };

    let resp: proto::ValidateTokenResponse = DomainVT::VaultAccess(claims).into();
    assert_eq!(resp.subject, "app:99");
    assert_eq!(resp.token_type, "vault_access");

    let vault_claims = match resp.claims.unwrap() {
        proto::validate_token_response::Claims::VaultAccess(c) => c,
        _ => panic!("expected VaultAccess claims"),
    };
    assert_eq!(vault_claims.org_slug, 10);
    assert_eq!(vault_claims.app_slug, 99);
    assert_eq!(vault_claims.vault_slug, 50);
    assert_eq!(vault_claims.scopes, vec!["vault:read"]);
}

#[test]
fn validate_token_response_proto_to_domain_user_session() {
    let resp = proto::ValidateTokenResponse {
        subject: "user:42".to_string(),
        token_type: "user_session".to_string(),
        expires_at: Some(prost_types::Timestamp { seconds: 1700001800, nanos: 0 }),
        claims: Some(proto::validate_token_response::Claims::UserSession(
            proto::UserSessionClaims { user_slug: 42, role: "admin".to_string() },
        )),
    };

    let validated: ValidatedToken = resp.try_into().unwrap();
    match validated {
        ValidatedToken::UserSession(c) => {
            assert_eq!(c.sub, "user:42");
            assert_eq!(c.user.value(), 42);
            assert_eq!(c.role, "admin");
            assert_eq!(c.exp, 1700001800);
        },
        _ => panic!("expected UserSession"),
    }
}

#[test]
fn validate_token_response_proto_to_domain_vault_access() {
    let resp = proto::ValidateTokenResponse {
        subject: "app:99".to_string(),
        token_type: "vault_access".to_string(),
        expires_at: Some(prost_types::Timestamp { seconds: 1700000300, nanos: 0 }),
        claims: Some(proto::validate_token_response::Claims::VaultAccess(
            proto::VaultAccessClaims {
                org_slug: 10,
                app_slug: 99,
                vault_slug: 50,
                scopes: vec!["entity:write".to_string()],
            },
        )),
    };

    let validated: ValidatedToken = resp.try_into().unwrap();
    match validated {
        ValidatedToken::VaultAccess(c) => {
            assert_eq!(c.sub, "app:99");
            assert_eq!(c.org.value(), 10);
            assert_eq!(c.app.value(), 99);
            assert_eq!(c.vault.value(), 50);
            assert_eq!(c.scopes, vec!["entity:write"]);
        },
        _ => panic!("expected VaultAccess"),
    }
}

#[test]
fn validate_token_response_missing_claims_rejected() {
    let resp = proto::ValidateTokenResponse {
        subject: "user:1".to_string(),
        token_type: "user_session".to_string(),
        expires_at: Some(prost_types::Timestamp { seconds: 0, nanos: 0 }),
        claims: None,
    };

    let result: Result<ValidatedToken, Status> = resp.try_into();
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument);
}

#[test]
fn validate_token_response_missing_expires_at_rejected() {
    let resp = proto::ValidateTokenResponse {
        subject: "user:1".to_string(),
        token_type: "user_session".to_string(),
        expires_at: None,
        claims: Some(proto::validate_token_response::Claims::UserSession(
            proto::UserSessionClaims { user_slug: 1, role: "user".to_string() },
        )),
    };

    let result: Result<ValidatedToken, Status> = resp.try_into();
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument);
}

// -------------------------------------------------------------------------
// SigningKeyScope helper tests
// -------------------------------------------------------------------------

#[test]
fn signing_key_scope_from_i32_global() {
    let scope = signing_key_scope_from_i32(1).unwrap();
    assert_eq!(scope, proto::SigningKeyScope::Global);
}

#[test]
fn signing_key_scope_from_i32_organization() {
    let scope = signing_key_scope_from_i32(2).unwrap();
    assert_eq!(scope, proto::SigningKeyScope::Organization);
}

#[test]
fn signing_key_scope_from_i32_unspecified_rejected() {
    let result = signing_key_scope_from_i32(0);
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument);
}

#[test]
fn signing_key_scope_from_i32_unknown_rejected() {
    let result = signing_key_scope_from_i32(99);
    assert!(result.is_err());
}

// -------------------------------------------------------------------------
// SigningKeyStatus helper tests
// -------------------------------------------------------------------------

#[test]
fn validate_signing_key_status_valid() {
    assert_eq!(validate_signing_key_status("active").unwrap(), "active");
    assert_eq!(validate_signing_key_status("rotated").unwrap(), "rotated");
    assert_eq!(validate_signing_key_status("revoked").unwrap(), "revoked");
}

#[test]
fn validate_signing_key_status_invalid() {
    assert!(validate_signing_key_status("unknown").is_err());
    assert!(validate_signing_key_status("").is_err());
    assert!(validate_signing_key_status("Active").is_err()); // case-sensitive
}

// -------------------------------------------------------------------------
// CredentialType conversion tests
// -------------------------------------------------------------------------

#[test]
fn credential_type_domain_to_proto_roundtrip() {
    for (domain, expected_proto) in [
        (CredentialType::Passkey, proto::CredentialType::Passkey),
        (CredentialType::Totp, proto::CredentialType::Totp),
        (CredentialType::RecoveryCode, proto::CredentialType::RecoveryCode),
    ] {
        let proto_ct: proto::CredentialType = domain.into();
        assert_eq!(proto_ct, expected_proto);
        let roundtrip: CredentialType = proto_ct.try_into().unwrap();
        assert_eq!(roundtrip, domain);
    }
}

#[test]
fn credential_type_unspecified_rejected() {
    let result: Result<CredentialType, Status> = proto::CredentialType::Unspecified.try_into();
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument);
}

#[test]
fn credential_type_from_i32_valid() {
    assert_eq!(credential_type_from_i32(1).unwrap(), proto::CredentialType::Passkey);
    assert_eq!(credential_type_from_i32(2).unwrap(), proto::CredentialType::Totp);
    assert_eq!(credential_type_from_i32(3).unwrap(), proto::CredentialType::RecoveryCode);
}

#[test]
fn credential_type_from_i32_unspecified_rejected() {
    let result = credential_type_from_i32(0);
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument);
}

#[test]
fn credential_type_from_i32_unknown_rejected() {
    assert!(credential_type_from_i32(99).is_err());
}

// -------------------------------------------------------------------------
// TotpAlgorithm conversion tests
// -------------------------------------------------------------------------

#[test]
fn totp_algorithm_domain_to_proto_roundtrip() {
    for (domain, expected_proto) in [
        (TotpAlgorithm::Sha1, proto::TotpAlgorithm::Sha1),
        (TotpAlgorithm::Sha256, proto::TotpAlgorithm::Sha256),
        (TotpAlgorithm::Sha512, proto::TotpAlgorithm::Sha512),
    ] {
        let proto_alg: proto::TotpAlgorithm = domain.into();
        assert_eq!(proto_alg, expected_proto);
        let roundtrip: TotpAlgorithm = proto_alg.into();
        assert_eq!(roundtrip, domain);
    }
}

#[test]
fn totp_algorithm_from_i32_valid() {
    assert_eq!(totp_algorithm_from_i32(0).unwrap(), proto::TotpAlgorithm::Sha1);
    assert_eq!(totp_algorithm_from_i32(1).unwrap(), proto::TotpAlgorithm::Sha256);
    assert_eq!(totp_algorithm_from_i32(2).unwrap(), proto::TotpAlgorithm::Sha512);
}

#[test]
fn totp_algorithm_from_i32_unknown_rejected() {
    assert!(totp_algorithm_from_i32(99).is_err());
}

// -------------------------------------------------------------------------
// PasskeyCredential conversion tests
// -------------------------------------------------------------------------

#[test]
fn passkey_credential_roundtrip() {
    let domain = PasskeyCredential {
        credential_id: vec![1, 2, 3],
        public_key: vec![4, 5, 6],
        sign_count: 42,
        transports: vec!["internal".to_string(), "usb".to_string()],
        backup_eligible: true,
        backup_state: false,
        attestation_format: Some("packed".to_string()),
        aaguid: Some([0u8; 16]),
    };

    let proto_pk: proto::PasskeyCredentialData = (&domain).into();
    assert_eq!(proto_pk.credential_id, vec![1, 2, 3]);
    assert_eq!(proto_pk.public_key, vec![4, 5, 6]);
    assert_eq!(proto_pk.sign_count, 42);
    assert_eq!(proto_pk.transports, vec!["internal", "usb"]);
    assert!(proto_pk.backup_eligible);
    assert!(!proto_pk.backup_state);
    assert_eq!(proto_pk.attestation_format, Some("packed".to_string()));
    assert_eq!(proto_pk.aaguid, Some(vec![0u8; 16]));

    let roundtrip = PasskeyCredential::try_from(&proto_pk).unwrap();
    assert_eq!(roundtrip, domain);
}

#[test]
fn passkey_credential_empty_credential_id_rejected() {
    let proto_pk = proto::PasskeyCredentialData {
        credential_id: vec![],
        public_key: vec![1],
        ..Default::default()
    };
    assert!(PasskeyCredential::try_from(&proto_pk).is_err());
}

#[test]
fn passkey_credential_empty_public_key_rejected() {
    let proto_pk = proto::PasskeyCredentialData {
        credential_id: vec![1],
        public_key: vec![],
        ..Default::default()
    };
    assert!(PasskeyCredential::try_from(&proto_pk).is_err());
}

#[test]
fn passkey_credential_bad_aaguid_rejected() {
    let proto_pk = proto::PasskeyCredentialData {
        credential_id: vec![1],
        public_key: vec![2],
        aaguid: Some(vec![0; 8]), // wrong length
        ..Default::default()
    };
    assert!(PasskeyCredential::try_from(&proto_pk).is_err());
}

// -------------------------------------------------------------------------
// TotpCredential conversion tests
// -------------------------------------------------------------------------

#[test]
fn totp_credential_roundtrip() {
    let domain = TotpCredential {
        secret: zeroize::Zeroizing::new(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
        algorithm: TotpAlgorithm::Sha1,
        digits: 6,
        period: 30,
    };

    let proto_totp: proto::TotpCredentialData = (&domain).into();
    assert_eq!(proto_totp.secret, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    assert_eq!(proto_totp.algorithm, proto::TotpAlgorithm::Sha1 as i32);
    assert_eq!(proto_totp.digits, 6);
    assert_eq!(proto_totp.period, 30);

    let roundtrip = TotpCredential::try_from(&proto_totp).unwrap();
    assert_eq!(roundtrip, domain);
}

#[test]
fn totp_credential_empty_secret_rejected() {
    let proto_totp = proto::TotpCredentialData { secret: vec![], ..Default::default() };
    assert!(TotpCredential::try_from(&proto_totp).is_err());
}

#[test]
fn totp_credential_invalid_digits_rejected() {
    let proto_totp = proto::TotpCredentialData {
        secret: vec![1; 20],
        algorithm: 0,
        digits: 4, // Only 6 or 8 are valid per RFC 6238
        period: 30,
    };
    assert!(TotpCredential::try_from(&proto_totp).is_err());
}

#[test]
fn totp_credential_8_digits_accepted() {
    let proto_totp =
        proto::TotpCredentialData { secret: vec![1; 20], algorithm: 0, digits: 8, period: 30 };
    let result = TotpCredential::try_from(&proto_totp).unwrap();
    assert_eq!(result.digits, 8);
}

#[test]
fn totp_credential_zero_period_rejected() {
    let proto_totp =
        proto::TotpCredentialData { secret: vec![1; 20], algorithm: 0, digits: 6, period: 0 };
    assert!(TotpCredential::try_from(&proto_totp).is_err());
}

// -------------------------------------------------------------------------
// RecoveryCodeCredential conversion tests
// -------------------------------------------------------------------------

#[test]
fn recovery_code_credential_roundtrip() {
    let hash1 = [1u8; 32];
    let hash2 = [2u8; 32];
    let domain = RecoveryCodeCredential { code_hashes: vec![hash1, hash2], total_generated: 10 };

    let proto_rc: proto::RecoveryCodeCredentialData = (&domain).into();
    assert_eq!(proto_rc.code_hashes.len(), 2);
    assert_eq!(proto_rc.code_hashes[0], hash1.to_vec());
    assert_eq!(proto_rc.code_hashes[1], hash2.to_vec());
    assert_eq!(proto_rc.total_generated, 10);

    let roundtrip = RecoveryCodeCredential::try_from(&proto_rc).unwrap();
    assert_eq!(roundtrip, domain);
}

#[test]
fn recovery_code_bad_hash_length_rejected() {
    let proto_rc = proto::RecoveryCodeCredentialData {
        code_hashes: vec![vec![0; 16]], // wrong length
        total_generated: 1,
    };
    assert!(RecoveryCodeCredential::try_from(&proto_rc).is_err());
}

#[test]
fn recovery_code_empty_hashes_rejected() {
    let proto_rc = proto::RecoveryCodeCredentialData { code_hashes: vec![], total_generated: 0 };
    assert!(RecoveryCodeCredential::try_from(&proto_rc).is_err());
}

// -------------------------------------------------------------------------
// UserCredential conversion tests
// -------------------------------------------------------------------------

#[test]
fn user_credential_to_proto_passkey() {
    use inferadb_ledger_types::{CredentialData, UserId};

    let cred = UserCredential {
        id: inferadb_ledger_types::UserCredentialId::new(42),
        user: UserId::new(7),
        credential_type: CredentialType::Passkey,
        credential_data: CredentialData::Passkey(PasskeyCredential {
            credential_id: vec![1, 2],
            public_key: vec![3, 4],
            sign_count: 5,
            transports: vec!["internal".to_string()],
            backup_eligible: false,
            backup_state: false,
            attestation_format: None,
            aaguid: None,
        }),
        name: "MacBook Touch ID".to_string(),
        enabled: true,
        created_at: chrono::Utc::now(),
        last_used_at: None,
    };

    let user_slug = UserSlug::new(999);
    let proto_cred = user_credential_to_proto(&cred, user_slug);

    assert_eq!(proto_cred.id, 42);
    assert_eq!(proto_cred.user.as_ref().unwrap().slug, 999);
    assert_eq!(proto_cred.credential_type, proto::CredentialType::Passkey as i32);
    assert_eq!(proto_cred.name, "MacBook Touch ID");
    assert!(proto_cred.enabled);
    assert!(proto_cred.created_at.is_some());
    assert!(proto_cred.last_used_at.is_none());
    assert!(matches!(proto_cred.data, Some(proto::user_credential::Data::Passkey(_))));
}

#[test]
fn user_credential_to_proto_totp() {
    use inferadb_ledger_types::{CredentialData, UserId};

    let cred = UserCredential {
        id: inferadb_ledger_types::UserCredentialId::new(10),
        user: UserId::new(3),
        credential_type: CredentialType::Totp,
        credential_data: CredentialData::Totp(TotpCredential {
            secret: zeroize::Zeroizing::new(vec![42; 20]),
            algorithm: TotpAlgorithm::Sha1,
            digits: 6,
            period: 30,
        }),
        name: "Authenticator app".to_string(),
        enabled: true,
        created_at: chrono::Utc::now(),
        last_used_at: Some(chrono::Utc::now()),
    };

    let proto_cred = user_credential_to_proto(&cred, UserSlug::new(555));
    assert!(proto_cred.last_used_at.is_some());
    assert!(matches!(proto_cred.data, Some(proto::user_credential::Data::Totp(_))));
}

// -------------------------------------------------------------------------
// strip_totp_secret tests
// -------------------------------------------------------------------------

#[test]
fn strip_totp_secret_clears_totp() {
    let mut cred = proto::UserCredential {
        data: Some(proto::user_credential::Data::Totp(proto::TotpCredentialData {
            secret: vec![1, 2, 3],
            algorithm: 0,
            digits: 6,
            period: 30,
        })),
        ..Default::default()
    };
    strip_totp_secret(&mut cred);
    if let Some(proto::user_credential::Data::Totp(ref totp)) = cred.data {
        assert!(totp.secret.is_empty());
    } else {
        unreachable!("expected TOTP data");
    }
}

#[test]
fn strip_totp_secret_noop_for_passkey() {
    let mut cred = proto::UserCredential {
        data: Some(proto::user_credential::Data::Passkey(proto::PasskeyCredentialData {
            credential_id: vec![1],
            public_key: vec![2],
            ..Default::default()
        })),
        ..Default::default()
    };
    strip_totp_secret(&mut cred);
    assert!(matches!(cred.data, Some(proto::user_credential::Data::Passkey(_))));
}

#[test]
fn strip_recovery_code_hashes_clears_hashes() {
    let mut cred = proto::UserCredential {
        data: Some(proto::user_credential::Data::RecoveryCode(proto::RecoveryCodeCredentialData {
            code_hashes: vec![vec![1; 32], vec![2; 32]],
            total_generated: 10,
        })),
        ..Default::default()
    };
    strip_recovery_code_hashes(&mut cred);
    if let Some(proto::user_credential::Data::RecoveryCode(ref rc)) = cred.data {
        assert!(rc.code_hashes.is_empty());
        assert_eq!(rc.total_generated, 10);
    } else {
        unreachable!("expected recovery code data");
    }
}

#[test]
fn strip_recovery_code_hashes_noop_for_passkey() {
    let mut cred = proto::UserCredential {
        data: Some(proto::user_credential::Data::Passkey(proto::PasskeyCredentialData {
            credential_id: vec![1],
            ..Default::default()
        })),
        ..Default::default()
    };
    strip_recovery_code_hashes(&mut cred);
    assert!(matches!(cred.data, Some(proto::user_credential::Data::Passkey(_))));
}

// -------------------------------------------------------------------------
// CredentialInfo validation tests
// -------------------------------------------------------------------------

#[test]
fn validate_credential_info_type_valid() {
    assert_eq!(validate_credential_info_type("passkey").unwrap(), "passkey");
    assert_eq!(validate_credential_info_type("email_code").unwrap(), "email_code");
    assert_eq!(validate_credential_info_type("recovery_code").unwrap(), "recovery_code");
}

#[test]
fn validate_credential_info_type_invalid() {
    assert!(validate_credential_info_type("unknown").is_err());
    assert!(validate_credential_info_type("").is_err());
    assert!(validate_credential_info_type("Passkey").is_err()); // case-sensitive
}

// -------------------------------------------------------------------------
// InviteSlug conversion tests
// -------------------------------------------------------------------------

#[test]
fn invite_slug_domain_to_proto() {
    let domain = InviteSlug::new(42);
    let proto: proto::InviteSlug = domain.into();
    assert_eq!(proto.slug, 42);
}

#[test]
fn invite_slug_proto_to_domain_owned() {
    let proto = proto::InviteSlug { slug: 99 };
    let domain: InviteSlug = proto.into();
    assert_eq!(domain.value(), 99);
}

#[test]
fn invite_slug_proto_to_domain_ref() {
    let proto = proto::InviteSlug { slug: 77 };
    let domain: InviteSlug = (&proto).into();
    assert_eq!(domain.value(), 77);
}

#[test]
fn invite_slug_roundtrip() {
    let original = InviteSlug::new(123456789);
    let proto: proto::InviteSlug = original.into();
    let roundtripped: InviteSlug = proto.into();
    assert_eq!(original, roundtripped);
}

// -------------------------------------------------------------------------
// InvitationStatus conversion tests
// -------------------------------------------------------------------------

#[test]
fn invitation_status_domain_to_proto_all_variants() {
    assert_eq!(
        proto::InvitationStatus::from(InvitationStatus::Pending),
        proto::InvitationStatus::Pending
    );
    assert_eq!(
        proto::InvitationStatus::from(InvitationStatus::Accepted),
        proto::InvitationStatus::Accepted
    );
    assert_eq!(
        proto::InvitationStatus::from(InvitationStatus::Declined),
        proto::InvitationStatus::Declined
    );
    assert_eq!(
        proto::InvitationStatus::from(InvitationStatus::Expired),
        proto::InvitationStatus::Expired
    );
    assert_eq!(
        proto::InvitationStatus::from(InvitationStatus::Revoked),
        proto::InvitationStatus::Revoked
    );
}

#[test]
fn invitation_status_proto_to_domain_all_variants() {
    use inferadb_ledger_types::InvitationStatus as DomainStatus;
    assert_eq!(
        DomainStatus::try_from(proto::InvitationStatus::Pending).unwrap(),
        DomainStatus::Pending
    );
    assert_eq!(
        DomainStatus::try_from(proto::InvitationStatus::Accepted).unwrap(),
        DomainStatus::Accepted
    );
    assert_eq!(
        DomainStatus::try_from(proto::InvitationStatus::Declined).unwrap(),
        DomainStatus::Declined
    );
    assert_eq!(
        DomainStatus::try_from(proto::InvitationStatus::Expired).unwrap(),
        DomainStatus::Expired
    );
    assert_eq!(
        DomainStatus::try_from(proto::InvitationStatus::Revoked).unwrap(),
        DomainStatus::Revoked
    );
}

#[test]
fn invitation_status_unspecified_rejected() {
    use inferadb_ledger_types::InvitationStatus as DomainStatus;
    let result = DomainStatus::try_from(proto::InvitationStatus::Unspecified);
    assert!(result.is_err());
}

#[test]
fn invitation_status_roundtrip() {
    use inferadb_ledger_types::InvitationStatus as DomainStatus;
    for status in [
        DomainStatus::Pending,
        DomainStatus::Accepted,
        DomainStatus::Declined,
        DomainStatus::Expired,
        DomainStatus::Revoked,
    ] {
        let proto_status = proto::InvitationStatus::from(status);
        let roundtripped = DomainStatus::try_from(proto_status).unwrap();
        assert_eq!(status, roundtripped);
    }
}

#[test]
fn invitation_status_from_i32_valid() {
    assert_eq!(invitation_status_from_i32(1).unwrap(), InvitationStatus::Pending);
    assert_eq!(invitation_status_from_i32(2).unwrap(), InvitationStatus::Accepted);
    assert_eq!(invitation_status_from_i32(3).unwrap(), InvitationStatus::Declined);
    assert_eq!(invitation_status_from_i32(4).unwrap(), InvitationStatus::Expired);
    assert_eq!(invitation_status_from_i32(5).unwrap(), InvitationStatus::Revoked);
}

#[test]
fn invitation_status_from_i32_invalid() {
    assert!(invitation_status_from_i32(99).is_err());
    assert!(invitation_status_from_i32(-1).is_err());
}

#[test]
fn invitation_status_from_i32_unspecified_rejected() {
    // 0 = UNSPECIFIED — must be rejected, not silently mapped to Pending
    assert!(invitation_status_from_i32(0).is_err());
}

// -------------------------------------------------------------------------
// UserEmailId conversion tests
// -------------------------------------------------------------------------

#[test]
fn user_email_id_domain_to_proto() {
    let domain = UserEmailId::new(42);
    let proto_id: proto::UserEmailId = domain.into();
    assert_eq!(proto_id.id, 42);
}

#[test]
fn user_email_id_proto_to_domain() {
    let proto_id = proto::UserEmailId { id: 99 };
    let domain: UserEmailId = proto_id.into();
    assert_eq!(domain.value(), 99);
}

#[test]
fn user_email_id_proto_ref_to_domain() {
    let proto_id = proto::UserEmailId { id: 77 };
    let domain: UserEmailId = (&proto_id).into();
    assert_eq!(domain.value(), 77);
}

#[test]
fn user_email_id_roundtrip() {
    let original = UserEmailId::new(123456);
    let proto_id: proto::UserEmailId = original.into();
    let roundtripped: UserEmailId = proto_id.into();
    assert_eq!(original, roundtripped);
}

// -------------------------------------------------------------------------
// EmailVerifyTokenId conversion tests
// -------------------------------------------------------------------------

#[test]
fn email_verify_token_id_domain_to_proto() {
    let domain = EmailVerifyTokenId::new(7);
    let proto_id: proto::EmailVerifyTokenId = domain.into();
    assert_eq!(proto_id.id, 7);
}

#[test]
fn email_verify_token_id_proto_to_domain() {
    let proto_id = proto::EmailVerifyTokenId { id: 55 };
    let domain: EmailVerifyTokenId = proto_id.into();
    assert_eq!(domain.value(), 55);
}

#[test]
fn email_verify_token_id_proto_ref_to_domain() {
    let proto_id = proto::EmailVerifyTokenId { id: 33 };
    let domain: EmailVerifyTokenId = (&proto_id).into();
    assert_eq!(domain.value(), 33);
}

#[test]
fn email_verify_token_id_roundtrip() {
    let original = EmailVerifyTokenId::new(987654);
    let proto_id: proto::EmailVerifyTokenId = original.into();
    let roundtripped: EmailVerifyTokenId = proto_id.into();
    assert_eq!(original, roundtripped);
}

// -------------------------------------------------------------------------
// Raft vote conversion tests
// -------------------------------------------------------------------------

#[test]
fn raft_vote_uncommitted_roundtrip() {
    let domain = openraft::Vote::new(5u64, 42u64);
    let proto_vote: proto::RaftVote = domain.into();
    assert_eq!(proto_vote.term, 5);
    assert_eq!(proto_vote.node_id, 42);
    assert!(!proto_vote.committed);
    let roundtripped: openraft::Vote<u64> = proto_vote.into();
    assert_eq!(domain, roundtripped);
}

#[test]
fn raft_vote_committed_roundtrip() {
    let domain = openraft::Vote::new_committed(3u64, 10u64);
    let proto_vote: proto::RaftVote = domain.into();
    assert_eq!(proto_vote.term, 3);
    assert_eq!(proto_vote.node_id, 10);
    assert!(proto_vote.committed);
    let roundtripped: openraft::Vote<u64> = proto_vote.into();
    assert_eq!(domain, roundtripped);
}

#[test]
fn raft_vote_ref_to_proto() {
    let domain = openraft::Vote::new(1u64, 2u64);
    let proto_vote: proto::RaftVote = (&domain).into();
    assert_eq!(proto_vote.term, 1);
    assert_eq!(proto_vote.node_id, 2);
}

#[test]
fn raft_vote_proto_ref_to_domain() {
    let proto_vote = proto::RaftVote { term: 7, node_id: 99, committed: true };
    let domain: openraft::Vote<u64> = (&proto_vote).into();
    assert!(domain.committed);
    assert_eq!(domain.leader_id.term, 7);
}

// -------------------------------------------------------------------------
// Operation ExpireEntity conversion tests (covers remaining operations gap)
// -------------------------------------------------------------------------

#[test]
fn operation_expire_entity_domain_to_proto_roundtrip() {
    let domain_op = inferadb_ledger_types::Operation::ExpireEntity {
        key: "expired-key".to_string(),
        expired_at: 1700000000,
    };
    let proto_op: proto::Operation = domain_op.clone().into();
    let roundtripped = inferadb_ledger_types::Operation::try_from(proto_op).unwrap();
    assert_eq!(domain_op, roundtripped);
}

#[test]
fn operation_expire_entity_ref_to_proto() {
    let domain_op =
        inferadb_ledger_types::Operation::ExpireEntity { key: "k".to_string(), expired_at: 42 };
    let proto_op: proto::Operation = (&domain_op).into();
    let roundtripped = inferadb_ledger_types::Operation::try_from(&proto_op).unwrap();
    assert_eq!(domain_op, roundtripped);
}
