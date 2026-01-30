//! Relationship storage operations.
//!
//! Stores relationship tuples (resource, relation, subject) with dual indexing
//! for efficient lookups in both directions.

use inferadb_ledger_store::{ReadTransaction, StorageBackend, WriteTransaction, tables};
use inferadb_ledger_types::{CodecError, Relationship, VaultId, decode, encode};
use snafu::{ResultExt, Snafu};

use crate::keys::{encode_storage_key, vault_prefix};

/// Relationship store error types.
#[derive(Debug, Snafu)]
pub enum RelationshipError {
    #[snafu(display("Storage error: {source}"))]
    Storage { source: inferadb_ledger_store::Error },

    #[snafu(display("Codec error: {source}"))]
    Codec { source: CodecError },
}

/// Result type for relationship operations.
pub type Result<T> = std::result::Result<T, RelationshipError>;

/// Relationship storage operations.
pub struct RelationshipStore;

impl RelationshipStore {
    /// Get a relationship by its components.
    pub fn get<B: StorageBackend>(
        txn: &ReadTransaction<'_, B>,
        vault_id: VaultId,
        resource: &str,
        relation: &str,
        subject: &str,
    ) -> Result<Option<Relationship>> {
        let rel = Relationship::new(resource, relation, subject);
        let local_key = rel.to_key();
        let storage_key = encode_storage_key(vault_id, local_key.as_bytes());

        match txn.get::<tables::Relationships>(&storage_key).context(StorageSnafu)? {
            Some(data) => {
                let relationship = decode(&data).context(CodecSnafu)?;
                Ok(Some(relationship))
            },
            None => Ok(None),
        }
    }

    /// Check if a relationship exists.
    pub fn exists<B: StorageBackend>(
        txn: &ReadTransaction<'_, B>,
        vault_id: VaultId,
        resource: &str,
        relation: &str,
        subject: &str,
    ) -> Result<bool> {
        let rel = Relationship::new(resource, relation, subject);
        let local_key = rel.to_key();
        let storage_key = encode_storage_key(vault_id, local_key.as_bytes());

        Ok(txn.get::<tables::Relationships>(&storage_key).context(StorageSnafu)?.is_some())
    }

    /// Create a relationship.
    ///
    /// Returns true if created, false if already existed.
    pub fn create<B: StorageBackend>(
        txn: &mut WriteTransaction<'_, B>,
        vault_id: VaultId,
        resource: &str,
        relation: &str,
        subject: &str,
    ) -> Result<bool> {
        let rel = Relationship::new(resource, relation, subject);
        let local_key = rel.to_key();
        let storage_key = encode_storage_key(vault_id, local_key.as_bytes());

        // Check if already exists
        if txn.get::<tables::Relationships>(&storage_key).context(StorageSnafu)?.is_some() {
            return Ok(false);
        }

        let encoded = encode(&rel).context(CodecSnafu)?;

        txn.insert::<tables::Relationships>(&storage_key, &encoded).context(StorageSnafu)?;

        Ok(true)
    }

    /// Delete a relationship.
    ///
    /// Returns true if deleted, false if not found.
    pub fn delete<B: StorageBackend>(
        txn: &mut WriteTransaction<'_, B>,
        vault_id: VaultId,
        resource: &str,
        relation: &str,
        subject: &str,
    ) -> Result<bool> {
        let rel = Relationship::new(resource, relation, subject);
        let local_key = rel.to_key();
        let storage_key = encode_storage_key(vault_id, local_key.as_bytes());

        let existed = txn.delete::<tables::Relationships>(&storage_key).context(StorageSnafu)?;
        Ok(existed)
    }

    /// List all relationships in a vault.
    pub fn list_in_vault<B: StorageBackend>(
        txn: &ReadTransaction<'_, B>,
        vault_id: VaultId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<Relationship>> {
        let prefix = vault_prefix(vault_id);
        let mut relationships = Vec::new();
        let mut count = 0;

        for (key_bytes, value) in txn.iter::<tables::Relationships>().context(StorageSnafu)? {
            // Check we're still in the same vault
            if key_bytes.len() < 8 {
                break;
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
                let rel = decode(&value).context(CodecSnafu)?;
                relationships.push(rel);
            }
            count += 1;
        }

        Ok(relationships)
    }

    /// Count relationships in a vault.
    pub fn count_in_vault<B: StorageBackend>(
        txn: &ReadTransaction<'_, B>,
        vault_id: VaultId,
    ) -> Result<usize> {
        let prefix = vault_prefix(vault_id);
        let mut count = 0;

        for (key_bytes, _) in txn.iter::<tables::Relationships>().context(StorageSnafu)? {
            if key_bytes.len() < 8 {
                break;
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

    /// List relationships for a specific resource.
    pub fn list_for_resource<B: StorageBackend>(
        txn: &ReadTransaction<'_, B>,
        vault_id: VaultId,
        resource: &str,
        limit: usize,
    ) -> Result<Vec<Relationship>> {
        let prefix = vault_prefix(vault_id);
        let rel_prefix = format!("rel:{}#", resource);
        let mut relationships = Vec::new();

        for (key_bytes, value) in txn.iter::<tables::Relationships>().context(StorageSnafu)? {
            // Check we're still in the same vault
            if key_bytes.len() < 9 {
                continue;
            }
            if key_bytes[..8] != prefix[..] {
                if key_bytes[..8] < prefix[..] {
                    continue;
                }
                break;
            }

            // Extract local key (skip vault_id and bucket_id)
            let local_key = &key_bytes[9..];

            // Check if it matches our resource prefix
            if local_key.starts_with(rel_prefix.as_bytes()) {
                let rel: Relationship = decode(&value).context(CodecSnafu)?;
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

    #[test]
    fn test_relationship_crud() {
        let engine = InMemoryStorageEngine::open().expect("open engine");
        let db = engine.db();
        let vault_id = 1;

        // Create
        {
            let mut txn = db.write().expect("begin write");

            let created =
                RelationshipStore::create(&mut txn, vault_id, "doc:123", "viewer", "user:alice")
                    .expect("create");
            assert!(created);

            // Creating again should return false
            let created =
                RelationshipStore::create(&mut txn, vault_id, "doc:123", "viewer", "user:alice")
                    .expect("create");
            assert!(!created);

            txn.commit().expect("commit");
        }

        // Check exists
        {
            let txn = db.read().expect("begin read");

            assert!(
                RelationshipStore::exists(&txn, vault_id, "doc:123", "viewer", "user:alice")
                    .expect("exists")
            );
            assert!(
                !RelationshipStore::exists(&txn, vault_id, "doc:123", "editor", "user:alice")
                    .expect("exists")
            );
        }

        // Delete
        {
            let mut txn = db.write().expect("begin write");

            let deleted =
                RelationshipStore::delete(&mut txn, vault_id, "doc:123", "viewer", "user:alice")
                    .expect("delete");
            assert!(deleted);

            // Deleting again should return false
            let deleted =
                RelationshipStore::delete(&mut txn, vault_id, "doc:123", "viewer", "user:alice")
                    .expect("delete");
            assert!(!deleted);

            txn.commit().expect("commit");
        }

        // Verify deleted
        {
            let txn = db.read().expect("begin read");

            assert!(
                !RelationshipStore::exists(&txn, vault_id, "doc:123", "viewer", "user:alice")
                    .expect("exists")
            );
        }
    }

    #[test]
    fn test_list_in_vault() {
        let engine = InMemoryStorageEngine::open().expect("open engine");
        let db = engine.db();
        let vault_id = 1;

        // Create multiple relationships
        {
            let mut txn = db.write().expect("begin write");

            for i in 0..5 {
                RelationshipStore::create(
                    &mut txn,
                    vault_id,
                    &format!("doc:{}", i),
                    "viewer",
                    "user:alice",
                )
                .expect("create");
            }
            txn.commit().expect("commit");
        }

        // List
        {
            let txn = db.read().expect("begin read");

            let rels = RelationshipStore::list_in_vault(&txn, vault_id, 3, 0).expect("list");
            assert_eq!(rels.len(), 3);

            let count = RelationshipStore::count_in_vault(&txn, vault_id).expect("count");
            assert_eq!(count, 5);
        }
    }

    #[test]
    fn test_get_relationship() {
        let engine = InMemoryStorageEngine::open().expect("open engine");
        let db = engine.db();
        let vault_id = 1;

        // Get non-existent relationship
        {
            let txn = db.read().expect("begin read");
            let result = RelationshipStore::get(&txn, vault_id, "doc:123", "viewer", "user:alice")
                .expect("get");
            assert!(result.is_none());
        }

        // Create relationship
        {
            let mut txn = db.write().expect("begin write");
            RelationshipStore::create(&mut txn, vault_id, "doc:123", "viewer", "user:alice")
                .expect("create");
            txn.commit().expect("commit");
        }

        // Get existing relationship
        {
            let txn = db.read().expect("begin read");
            let rel = RelationshipStore::get(&txn, vault_id, "doc:123", "viewer", "user:alice")
                .expect("get")
                .expect("relationship should exist");
            assert_eq!(rel.resource, "doc:123");
            assert_eq!(rel.relation, "viewer");
            assert_eq!(rel.subject, "user:alice");
        }
    }

    #[test]
    fn test_list_for_resource() {
        let engine = InMemoryStorageEngine::open().expect("open engine");
        let db = engine.db();
        let vault_id = 1;

        // Create relationships for different resources
        {
            let mut txn = db.write().expect("begin write");

            // doc:123 has multiple viewers and an editor
            RelationshipStore::create(&mut txn, vault_id, "doc:123", "viewer", "user:alice")
                .expect("create");
            RelationshipStore::create(&mut txn, vault_id, "doc:123", "viewer", "user:bob")
                .expect("create");
            RelationshipStore::create(&mut txn, vault_id, "doc:123", "editor", "user:charlie")
                .expect("create");

            // doc:456 has different relationships
            RelationshipStore::create(&mut txn, vault_id, "doc:456", "viewer", "user:dave")
                .expect("create");
            RelationshipStore::create(&mut txn, vault_id, "doc:456", "owner", "user:eve")
                .expect("create");

            txn.commit().expect("commit");
        }

        // List relationships for doc:123
        {
            let txn = db.read().expect("begin read");
            let rels =
                RelationshipStore::list_for_resource(&txn, vault_id, "doc:123", 10).expect("list");
            assert_eq!(rels.len(), 3);
            for rel in &rels {
                assert_eq!(rel.resource, "doc:123");
            }
        }

        // List relationships for doc:456
        {
            let txn = db.read().expect("begin read");
            let rels =
                RelationshipStore::list_for_resource(&txn, vault_id, "doc:456", 10).expect("list");
            assert_eq!(rels.len(), 2);
        }

        // List with limit
        {
            let txn = db.read().expect("begin read");
            let rels =
                RelationshipStore::list_for_resource(&txn, vault_id, "doc:123", 2).expect("list");
            assert_eq!(rels.len(), 2);
        }

        // List for non-existent resource
        {
            let txn = db.read().expect("begin read");
            let rels =
                RelationshipStore::list_for_resource(&txn, vault_id, "doc:999", 10).expect("list");
            assert!(rels.is_empty());
        }
    }

    #[test]
    fn test_list_in_vault_pagination() {
        let engine = InMemoryStorageEngine::open().expect("open engine");
        let db = engine.db();
        let vault_id = 1;

        // Create 10 relationships
        {
            let mut txn = db.write().expect("begin write");
            for i in 0..10 {
                RelationshipStore::create(
                    &mut txn,
                    vault_id,
                    &format!("doc:{}", i),
                    "viewer",
                    "user:alice",
                )
                .expect("create");
            }
            txn.commit().expect("commit");
        }

        // Test pagination
        {
            let txn = db.read().expect("begin read");

            // First page
            let page1 = RelationshipStore::list_in_vault(&txn, vault_id, 3, 0).expect("list");
            assert_eq!(page1.len(), 3);

            // Second page
            let page2 = RelationshipStore::list_in_vault(&txn, vault_id, 3, 3).expect("list");
            assert_eq!(page2.len(), 3);

            // Third page
            let page3 = RelationshipStore::list_in_vault(&txn, vault_id, 3, 6).expect("list");
            assert_eq!(page3.len(), 3);

            // Fourth page (only 1 remaining)
            let page4 = RelationshipStore::list_in_vault(&txn, vault_id, 3, 9).expect("list");
            assert_eq!(page4.len(), 1);

            // Beyond end
            let page5 = RelationshipStore::list_in_vault(&txn, vault_id, 3, 10).expect("list");
            assert!(page5.is_empty());
        }
    }

    #[test]
    fn test_count_empty_vault() {
        let engine = InMemoryStorageEngine::open().expect("open engine");
        let db = engine.db();

        let txn = db.read().expect("begin read");
        let count = RelationshipStore::count_in_vault(&txn, 999).expect("count");
        assert_eq!(count, 0);
    }

    #[test]
    fn test_vault_isolation() {
        let engine = InMemoryStorageEngine::open().expect("open engine");
        let db = engine.db();

        // Create in vault 1
        {
            let mut txn = db.write().expect("begin write");
            RelationshipStore::create(&mut txn, 1, "doc:shared", "viewer", "user:alice")
                .expect("create");
            txn.commit().expect("commit");
        }

        // Vault 2 should not see vault 1's relationships
        {
            let txn = db.read().expect("begin read");

            assert!(
                !RelationshipStore::exists(&txn, 2, "doc:shared", "viewer", "user:alice")
                    .expect("exists")
            );

            let count = RelationshipStore::count_in_vault(&txn, 2).expect("count");
            assert_eq!(count, 0);
        }
    }
}
