//! Relationship storage operations.
//!
//! Stores relationship tuples (resource, relation, subject) using binary keys
//! and dictionary-interned type identifiers. Values are a single version byte
//! (`0x01`); the full [`Relationship`] is reconstructed from the key via
//! dictionary resolution.

use inferadb_ledger_store::{ReadTransaction, StorageBackend, TableId, WriteTransaction, tables};
use inferadb_ledger_types::{Relationship, VaultId};
use snafu::{ResultExt, Snafu};

use crate::{
    binary_keys::{
        InternCategory, InternId, decode_relationship_local_key, encode_relationship_local_key,
        split_typed_id,
    },
    dictionary::VaultDictionary,
    keys::{encode_storage_key, vault_prefix},
};

/// Version byte stored as the value for each relationship.
const VALUE_VERSION: u8 = 0x01;

/// Errors returned by [`RelationshipStore`] operations.
#[derive(Debug, Snafu)]
pub enum RelationshipError {
    /// Underlying storage operation failed.
    #[snafu(display("Storage error: {source}"))]
    Storage {
        source: inferadb_ledger_store::Error,
        #[snafu(implicit)]
        location: snafu::Location,
    },

    /// Dictionary operation failed.
    #[snafu(display("Dictionary error: {source}"))]
    Dictionary {
        source: crate::dictionary::DictionaryError,
        #[snafu(implicit)]
        location: snafu::Location,
    },
}

/// Result type for relationship operations.
pub type Result<T> = std::result::Result<T, RelationshipError>;

/// Resolves a relationship triple to interned components.
///
/// Returns `(res_type, res_id_bytes, rel_id, subj_type, subj_id_bytes)`, or
/// `None` if any string is unknown in the dictionary.
fn resolve_triple(
    dict: &VaultDictionary,
    resource: &str,
    relation: &str,
    subject: &str,
) -> Option<(InternId, Vec<u8>, InternId, InternId, Vec<u8>)> {
    let (res_type_str, res_local) = split_typed_id(resource).unwrap_or(("", resource));
    let res_type = dict.get_id(InternCategory::ResourceType, res_type_str)?;
    let res_id_bytes = res_local.as_bytes().to_vec();

    let rel_id = dict.get_id(InternCategory::Relation, relation)?;

    let (subj_type_str, subj_local) = split_typed_id(subject).unwrap_or(("", subject));
    let subj_type = dict.get_id(InternCategory::SubjectType, subj_type_str)?;
    let subj_id_bytes = subj_local.as_bytes().to_vec();

    Some((res_type, res_id_bytes, rel_id, subj_type, subj_id_bytes))
}

/// Builds the full storage key from interned components.
fn build_storage_key(
    vault: VaultId,
    res_type: InternId,
    res_id: &[u8],
    rel_id: InternId,
    subj_type: InternId,
    subj_id: &[u8],
) -> Vec<u8> {
    let local_key = encode_relationship_local_key(res_type, res_id, rel_id, subj_type, subj_id);
    encode_storage_key(vault, &local_key)
}

/// Reconstructs a [`Relationship`] from a decoded binary local key using the dictionary.
fn reconstruct_relationship(
    dict: &VaultDictionary,
    res_type: InternId,
    res_id: &[u8],
    rel_id: InternId,
    subj_type: InternId,
    subj_id: &[u8],
) -> Option<Relationship> {
    let resource = dict.resolve_typed_id(InternCategory::ResourceType, res_type, res_id)?;
    let relation = dict.resolve(InternCategory::Relation, rel_id)?;
    let subject = dict.resolve_typed_id(InternCategory::SubjectType, subj_type, subj_id)?;
    Some(Relationship::new(resource, relation.to_owned(), subject))
}

/// Low-level relationship storage operations on raw transactions.
///
/// Methods accepting `vault: VaultId` use the internal sequential vault identifier.
/// All methods require a [`VaultDictionary`] for binary key encoding/decoding.
pub struct RelationshipStore;

impl RelationshipStore {
    /// Creates a relationship if it does not already exist.
    ///
    /// Returns `true` if the relationship was created, `false` if it already existed.
    /// The caller must commit the transaction after this call.
    ///
    /// Note: This does not update the dual indexes ([`IndexManager`](crate::IndexManager)).
    /// [`StateLayer::apply_operations`](crate::StateLayer::apply_operations) handles
    /// index maintenance automatically.
    ///
    /// # Errors
    ///
    /// Returns `RelationshipError::Storage` if the read or write transaction fails.
    /// Returns `RelationshipError::Dictionary` if interning a type string fails.
    pub fn create<B: StorageBackend>(
        txn: &mut WriteTransaction<'_, B>,
        dict: &mut VaultDictionary,
        vault: VaultId,
        resource: &str,
        relation: &str,
        subject: &str,
    ) -> Result<bool> {
        let (res_type, res_id) = dict
            .intern_typed_id(txn, InternCategory::ResourceType, resource)
            .context(DictionarySnafu)?;
        let rel_id =
            dict.intern(txn, InternCategory::Relation, relation).context(DictionarySnafu)?;
        let (subj_type, subj_id) = dict
            .intern_typed_id(txn, InternCategory::SubjectType, subject)
            .context(DictionarySnafu)?;

        let storage_key = build_storage_key(vault, res_type, &res_id, rel_id, subj_type, &subj_id);

        if txn.get_raw(TableId::Relationships, &storage_key).context(StorageSnafu)?.is_some() {
            return Ok(false);
        }

        let value = [VALUE_VERSION];
        txn.insert_raw(TableId::Relationships, &storage_key, &value).context(StorageSnafu)?;

        Ok(true)
    }

    /// Checks if a relationship exists in the vault.
    ///
    /// # Errors
    ///
    /// Returns `RelationshipError::Storage` if the read transaction fails.
    pub fn exists<B: StorageBackend>(
        txn: &ReadTransaction<'_, B>,
        dict: &VaultDictionary,
        vault: VaultId,
        resource: &str,
        relation: &str,
        subject: &str,
    ) -> Result<bool> {
        let Some((res_type, res_id, rel_id, subj_type, subj_id)) =
            resolve_triple(dict, resource, relation, subject)
        else {
            return Ok(false);
        };

        let storage_key = build_storage_key(vault, res_type, &res_id, rel_id, subj_type, &subj_id);
        Ok(txn.get_raw(TableId::Relationships, &storage_key).context(StorageSnafu)?.is_some())
    }

    /// Checks if a relationship exists within a write transaction.
    ///
    /// # Errors
    ///
    /// Returns `RelationshipError::Storage` if the transaction read fails.
    pub fn exists_in_write<B: StorageBackend>(
        txn: &WriteTransaction<'_, B>,
        dict: &VaultDictionary,
        vault: VaultId,
        resource: &str,
        relation: &str,
        subject: &str,
    ) -> Result<bool> {
        let Some((res_type, res_id, rel_id, subj_type, subj_id)) =
            resolve_triple(dict, resource, relation, subject)
        else {
            return Ok(false);
        };

        let storage_key = build_storage_key(vault, res_type, &res_id, rel_id, subj_type, &subj_id);
        Ok(txn.get_raw(TableId::Relationships, &storage_key).context(StorageSnafu)?.is_some())
    }

    /// Returns a relationship by its components, or `None` if not found.
    ///
    /// # Errors
    ///
    /// Returns `RelationshipError::Storage` if the read transaction fails.
    pub fn get<B: StorageBackend>(
        txn: &ReadTransaction<'_, B>,
        dict: &VaultDictionary,
        vault: VaultId,
        resource: &str,
        relation: &str,
        subject: &str,
    ) -> Result<Option<Relationship>> {
        let Some((res_type, res_id, rel_id, subj_type, subj_id)) =
            resolve_triple(dict, resource, relation, subject)
        else {
            return Ok(None);
        };

        let storage_key = build_storage_key(vault, res_type, &res_id, rel_id, subj_type, &subj_id);

        if txn.get_raw(TableId::Relationships, &storage_key).context(StorageSnafu)?.is_some() {
            Ok(Some(Relationship::new(resource, relation, subject)))
        } else {
            Ok(None)
        }
    }

    /// Deletes a relationship by its components.
    ///
    /// Returns `true` if the relationship existed and was deleted, `false` if not found.
    /// The caller must commit the transaction after this call.
    ///
    /// Note: This does not update the dual indexes. See [`create`](Self::create) for details.
    ///
    /// # Errors
    ///
    /// Returns `RelationshipError::Storage` if the delete operation fails.
    pub fn delete<B: StorageBackend>(
        txn: &mut WriteTransaction<'_, B>,
        dict: &VaultDictionary,
        vault: VaultId,
        resource: &str,
        relation: &str,
        subject: &str,
    ) -> Result<bool> {
        let Some((res_type, res_id, rel_id, subj_type, subj_id)) =
            resolve_triple(dict, resource, relation, subject)
        else {
            return Ok(false);
        };

        let storage_key = build_storage_key(vault, res_type, &res_id, rel_id, subj_type, &subj_id);
        let existed = txn.delete_raw(TableId::Relationships, &storage_key).context(StorageSnafu)?;
        Ok(existed)
    }

    /// Lists relationships in a vault with pagination.
    ///
    /// Returns up to `limit` relationships starting from `offset`.
    ///
    /// # Errors
    ///
    /// Returns `RelationshipError::Storage` if the iterator or read transaction fails.
    pub fn list_in_vault<B: StorageBackend>(
        txn: &ReadTransaction<'_, B>,
        dict: &VaultDictionary,
        vault: VaultId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<Relationship>> {
        let prefix = vault_prefix(vault);
        let mut relationships = Vec::new();
        let mut count = 0;

        for (key_bytes, _) in txn.iter::<tables::Relationships>().context(StorageSnafu)? {
            if key_bytes.len() < 9 {
                continue;
            }
            if key_bytes[..8] != prefix[..] {
                if key_bytes[..8] < prefix[..] {
                    continue;
                }
                break;
            }

            if count >= offset {
                if relationships.len() >= limit {
                    break;
                }
                // Local key starts after vault_id (8) + bucket_id (1)
                let local_key = &key_bytes[9..];
                if let Some((res_type, res_id, rel_id, subj_type, subj_id)) =
                    decode_relationship_local_key(local_key)
                    && let Some(rel) =
                        reconstruct_relationship(dict, res_type, res_id, rel_id, subj_type, subj_id)
                {
                    relationships.push(rel);
                }
            }
            count += 1;
        }

        Ok(relationships)
    }

    /// Counts all relationships in a vault.
    ///
    /// # Errors
    ///
    /// Returns `RelationshipError::Storage` if the iterator or read transaction fails.
    pub fn count_in_vault<B: StorageBackend>(
        txn: &ReadTransaction<'_, B>,
        _dict: &VaultDictionary,
        vault: VaultId,
    ) -> Result<usize> {
        let prefix = vault_prefix(vault);
        let mut count = 0;

        for (key_bytes, _) in txn.iter::<tables::Relationships>().context(StorageSnafu)? {
            if key_bytes.len() < 9 {
                continue;
            }
            if key_bytes[..8] != prefix[..] {
                if key_bytes[..8] < prefix[..] {
                    continue;
                }
                break;
            }
            count += 1;
        }

        Ok(count)
    }

    /// Lists relationships for a specific resource across all relations.
    ///
    /// Scans the vault for relationships whose decoded resource matches the
    /// given resource string, returning up to `limit` matches. Because keys
    /// are distributed across buckets by hash, a full vault scan is performed
    /// internally.
    ///
    /// # Errors
    ///
    /// Returns `RelationshipError::Storage` if the iterator or read transaction fails.
    pub fn list_for_resource<B: StorageBackend>(
        txn: &ReadTransaction<'_, B>,
        dict: &VaultDictionary,
        vault: VaultId,
        resource: &str,
        limit: usize,
    ) -> Result<Vec<Relationship>> {
        let prefix = vault_prefix(vault);
        let mut relationships = Vec::new();

        for (key_bytes, _) in txn.iter::<tables::Relationships>().context(StorageSnafu)? {
            if key_bytes.len() < 9 {
                continue;
            }
            if key_bytes[..8] != prefix[..] {
                if key_bytes[..8] < prefix[..] {
                    continue;
                }
                break;
            }

            let local_key = &key_bytes[9..];
            if let Some((res_type, res_id, rel_id, subj_type, subj_id)) =
                decode_relationship_local_key(local_key)
                && let Some(rel) =
                    reconstruct_relationship(dict, res_type, res_id, rel_id, subj_type, subj_id)
                && rel.resource == resource
            {
                relationships.push(rel);
                if relationships.len() >= limit {
                    break;
                }
            }
        }

        Ok(relationships)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods, unused_mut)]
mod tests {
    use super::*;
    use crate::engine::InMemoryStorageEngine;

    /// Helper: open engine, create dict, return (engine, dict).
    fn setup(vault: VaultId) -> (InMemoryStorageEngine, VaultDictionary) {
        let engine = InMemoryStorageEngine::open().expect("open engine");
        let dict = VaultDictionary::new(vault);
        (engine, dict)
    }

    // -----------------------------------------------------------------------
    // 1. create_and_exists
    // -----------------------------------------------------------------------
    #[test]
    fn create_and_exists() {
        let vault = VaultId::new(1);
        let (engine, mut dict) = setup(vault);
        let db = engine.db();

        {
            let mut txn = db.write().expect("begin write");
            let created = RelationshipStore::create(
                &mut txn,
                &mut dict,
                vault,
                "doc:123",
                "viewer",
                "user:alice",
            )
            .expect("create");
            assert!(created);
            txn.commit().expect("commit");
        }

        {
            let txn = db.read().expect("begin read");
            assert!(
                RelationshipStore::exists(&txn, &dict, vault, "doc:123", "viewer", "user:alice")
                    .expect("exists")
            );
        }
    }

    // -----------------------------------------------------------------------
    // 2. create_idempotent
    // -----------------------------------------------------------------------
    #[test]
    fn create_idempotent() {
        let vault = VaultId::new(1);
        let (engine, mut dict) = setup(vault);
        let db = engine.db();

        let mut txn = db.write().expect("begin write");
        let first =
            RelationshipStore::create(&mut txn, &mut dict, vault, "doc:1", "viewer", "user:bob")
                .expect("first create");
        assert!(first);

        let second =
            RelationshipStore::create(&mut txn, &mut dict, vault, "doc:1", "viewer", "user:bob")
                .expect("second create");
        assert!(!second);
    }

    // -----------------------------------------------------------------------
    // 3. get_returns_relationship
    // -----------------------------------------------------------------------
    #[test]
    fn get_returns_relationship() {
        let vault = VaultId::new(1);
        let (engine, mut dict) = setup(vault);
        let db = engine.db();

        {
            let mut txn = db.write().expect("begin write");
            RelationshipStore::create(
                &mut txn,
                &mut dict,
                vault,
                "doc:123",
                "viewer",
                "user:alice",
            )
            .expect("create");
            txn.commit().expect("commit");
        }

        {
            let txn = db.read().expect("begin read");
            let rel = RelationshipStore::get(&txn, &dict, vault, "doc:123", "viewer", "user:alice")
                .expect("get")
                .expect("relationship should exist");
            assert_eq!(rel.resource, "doc:123");
            assert_eq!(rel.relation, "viewer");
            assert_eq!(rel.subject, "user:alice");
        }
    }

    // -----------------------------------------------------------------------
    // 4. get_nonexistent_returns_none
    // -----------------------------------------------------------------------
    #[test]
    fn get_nonexistent_returns_none() {
        let vault = VaultId::new(1);
        let (engine, dict) = setup(vault);
        let db = engine.db();

        let txn = db.read().expect("begin read");
        let result = RelationshipStore::get(&txn, &dict, vault, "doc:999", "editor", "user:nobody")
            .expect("get");
        assert!(result.is_none());
    }

    // -----------------------------------------------------------------------
    // 5. delete_existing
    // -----------------------------------------------------------------------
    #[test]
    fn delete_existing() {
        let vault = VaultId::new(1);
        let (engine, mut dict) = setup(vault);
        let db = engine.db();

        {
            let mut txn = db.write().expect("begin write");
            RelationshipStore::create(&mut txn, &mut dict, vault, "doc:1", "owner", "user:eve")
                .expect("create");
            txn.commit().expect("commit");
        }

        {
            let mut txn = db.write().expect("begin write");
            let deleted =
                RelationshipStore::delete(&mut txn, &dict, vault, "doc:1", "owner", "user:eve")
                    .expect("delete");
            assert!(deleted);
            txn.commit().expect("commit");
        }

        {
            let txn = db.read().expect("begin read");
            assert!(
                !RelationshipStore::exists(&txn, &dict, vault, "doc:1", "owner", "user:eve")
                    .expect("exists")
            );
        }
    }

    // -----------------------------------------------------------------------
    // 6. delete_nonexistent_returns_false
    // -----------------------------------------------------------------------
    #[test]
    fn delete_nonexistent_returns_false() {
        let vault = VaultId::new(1);
        let (engine, dict) = setup(vault);
        let db = engine.db();

        let mut txn = db.write().expect("begin write");
        let deleted =
            RelationshipStore::delete(&mut txn, &dict, vault, "doc:nope", "viewer", "user:ghost")
                .expect("delete");
        assert!(!deleted);
    }

    // -----------------------------------------------------------------------
    // 7. list_in_vault_with_pagination
    // -----------------------------------------------------------------------
    #[test]
    fn list_in_vault_with_pagination() {
        let vault = VaultId::new(1);
        let (engine, mut dict) = setup(vault);
        let db = engine.db();

        {
            let mut txn = db.write().expect("begin write");
            for i in 0..5 {
                RelationshipStore::create(
                    &mut txn,
                    &mut dict,
                    vault,
                    &format!("doc:{i}"),
                    "viewer",
                    "user:alice",
                )
                .expect("create");
            }
            txn.commit().expect("commit");
        }

        {
            let txn = db.read().expect("begin read");
            let page1 = RelationshipStore::list_in_vault(&txn, &dict, vault, 3, 0).expect("list");
            assert_eq!(page1.len(), 3);

            let page2 = RelationshipStore::list_in_vault(&txn, &dict, vault, 3, 3).expect("list");
            assert_eq!(page2.len(), 2);
        }
    }

    // -----------------------------------------------------------------------
    // 8. count_in_vault
    // -----------------------------------------------------------------------
    #[test]
    fn count_in_vault() {
        let vault = VaultId::new(1);
        let (engine, mut dict) = setup(vault);
        let db = engine.db();

        {
            let txn = db.read().expect("begin read");
            let count = RelationshipStore::count_in_vault(&txn, &dict, vault).expect("count");
            assert_eq!(count, 0);
        }

        {
            let mut txn = db.write().expect("begin write");
            RelationshipStore::create(&mut txn, &mut dict, vault, "doc:a", "viewer", "user:alice")
                .expect("create");
            RelationshipStore::create(&mut txn, &mut dict, vault, "doc:b", "editor", "user:bob")
                .expect("create");
            txn.commit().expect("commit");
        }

        {
            let txn = db.read().expect("begin read");
            let count = RelationshipStore::count_in_vault(&txn, &dict, vault).expect("count");
            assert_eq!(count, 2);
        }
    }

    // -----------------------------------------------------------------------
    // 9. vault_isolation
    // -----------------------------------------------------------------------
    #[test]
    fn vault_isolation() {
        let vault1 = VaultId::new(1);
        let vault2 = VaultId::new(2);
        let (engine, mut dict1) = setup(vault1);
        let dict2 = VaultDictionary::new(vault2);
        let db = engine.db();

        {
            let mut txn = db.write().expect("begin write");
            RelationshipStore::create(
                &mut txn,
                &mut dict1,
                vault1,
                "doc:shared",
                "viewer",
                "user:alice",
            )
            .expect("create");
            txn.commit().expect("commit");
        }

        {
            let txn = db.read().expect("begin read");
            assert!(
                !RelationshipStore::exists(
                    &txn,
                    &dict2,
                    vault2,
                    "doc:shared",
                    "viewer",
                    "user:alice"
                )
                .expect("exists")
            );
            let count = RelationshipStore::count_in_vault(&txn, &dict2, vault2).expect("count");
            assert_eq!(count, 0);
        }
    }

    // -----------------------------------------------------------------------
    // 10. value_is_minimal
    // -----------------------------------------------------------------------
    #[test]
    fn value_is_minimal() {
        let vault = VaultId::new(1);
        let (engine, mut dict) = setup(vault);
        let db = engine.db();

        {
            let mut txn = db.write().expect("begin write");
            RelationshipStore::create(
                &mut txn,
                &mut dict,
                vault,
                "doc:42",
                "viewer",
                "user:tester",
            )
            .expect("create");
            txn.commit().expect("commit");
        }

        // Read the raw value from the iterator and verify it is exactly [0x01].
        {
            let txn = db.read().expect("begin read");
            let prefix = vault_prefix(vault);
            let mut found = false;

            for (key_bytes, value) in txn.iter::<tables::Relationships>().expect("iter") {
                if key_bytes.len() >= 8 && key_bytes[..8] == prefix[..] {
                    assert_eq!(value, vec![VALUE_VERSION], "stored value should be [0x01]");
                    found = true;
                    break;
                }
            }

            assert!(found, "expected at least one relationship entry");
        }
    }

    // -----------------------------------------------------------------------
    // Additional: list_for_resource
    // -----------------------------------------------------------------------
    #[test]
    fn list_for_resource() {
        let vault = VaultId::new(1);
        let (engine, mut dict) = setup(vault);
        let db = engine.db();

        {
            let mut txn = db.write().expect("begin write");
            RelationshipStore::create(
                &mut txn,
                &mut dict,
                vault,
                "doc:123",
                "viewer",
                "user:alice",
            )
            .expect("create");
            RelationshipStore::create(&mut txn, &mut dict, vault, "doc:123", "viewer", "user:bob")
                .expect("create");
            RelationshipStore::create(
                &mut txn,
                &mut dict,
                vault,
                "doc:123",
                "editor",
                "user:charlie",
            )
            .expect("create");
            RelationshipStore::create(&mut txn, &mut dict, vault, "doc:456", "viewer", "user:dave")
                .expect("create");
            txn.commit().expect("commit");
        }

        {
            let txn = db.read().expect("begin read");
            let rels = RelationshipStore::list_for_resource(&txn, &dict, vault, "doc:123", 10)
                .expect("list");
            assert_eq!(rels.len(), 3);
            for rel in &rels {
                assert_eq!(rel.resource, "doc:123");
            }

            let rels = RelationshipStore::list_for_resource(&txn, &dict, vault, "doc:456", 10)
                .expect("list");
            assert_eq!(rels.len(), 1);

            let rels = RelationshipStore::list_for_resource(&txn, &dict, vault, "doc:999", 10)
                .expect("list");
            assert!(rels.is_empty());

            // With limit
            let rels = RelationshipStore::list_for_resource(&txn, &dict, vault, "doc:123", 2)
                .expect("list");
            assert_eq!(rels.len(), 2);
        }
    }

    // -----------------------------------------------------------------------
    // exists_in_write
    // -----------------------------------------------------------------------
    #[test]
    fn exists_in_write_transaction() {
        let vault = VaultId::new(1);
        let (engine, mut dict) = setup(vault);
        let db = engine.db();

        let mut txn = db.write().expect("begin write");
        RelationshipStore::create(&mut txn, &mut dict, vault, "doc:1", "viewer", "user:alice")
            .expect("create");

        // Check within the same write transaction (before commit)
        assert!(
            RelationshipStore::exists_in_write(&txn, &dict, vault, "doc:1", "viewer", "user:alice")
                .expect("exists_in_write")
        );
        assert!(
            !RelationshipStore::exists_in_write(
                &txn,
                &dict,
                vault,
                "doc:1",
                "editor",
                "user:alice"
            )
            .expect("exists_in_write")
        );
    }
}
