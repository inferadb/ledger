//! Relationship storage operations.
//!
//! Stores relationship tuples (resource, relation, subject) with dual indexing
//! for efficient lookups in both directions.

use redb::{ReadableTable, ReadOnlyTable, Table};
use snafu::{ResultExt, Snafu};

use ledger_types::{Relationship, VaultId};

use crate::keys::{encode_storage_key, vault_prefix};
use crate::tables::Tables;

/// Relationship store error types.
#[derive(Debug, Snafu)]
pub enum RelationshipError {
    #[snafu(display("Storage error: {source}"))]
    Storage { source: redb::StorageError },

    #[snafu(display("Serialization error: {message}"))]
    Serialization { message: String },
}

/// Result type for relationship operations.
pub type Result<T> = std::result::Result<T, RelationshipError>;

/// Relationship storage operations.
pub struct RelationshipStore;

impl RelationshipStore {
    /// Get a relationship by its components.
    pub fn get(
        table: &ReadOnlyTable<&'static [u8], &'static [u8]>,
        vault_id: VaultId,
        resource: &str,
        relation: &str,
        subject: &str,
    ) -> Result<Option<Relationship>> {
        let rel = Relationship::new(resource, relation, subject);
        let local_key = rel.to_key();
        let storage_key = encode_storage_key(vault_id, local_key.as_bytes());

        match table.get(&storage_key[..]).context(StorageSnafu)? {
            Some(data) => {
                let relationship = postcard::from_bytes(data.value()).map_err(|e| {
                    RelationshipError::Serialization {
                        message: e.to_string(),
                    }
                })?;
                Ok(Some(relationship))
            }
            None => Ok(None),
        }
    }

    /// Check if a relationship exists.
    pub fn exists(
        table: &ReadOnlyTable<&'static [u8], &'static [u8]>,
        vault_id: VaultId,
        resource: &str,
        relation: &str,
        subject: &str,
    ) -> Result<bool> {
        let rel = Relationship::new(resource, relation, subject);
        let local_key = rel.to_key();
        let storage_key = encode_storage_key(vault_id, local_key.as_bytes());

        Ok(table.get(&storage_key[..]).context(StorageSnafu)?.is_some())
    }

    /// Create a relationship.
    ///
    /// Returns true if created, false if already existed.
    pub fn create(
        table: &mut Table<'_, &'static [u8], &'static [u8]>,
        vault_id: VaultId,
        resource: &str,
        relation: &str,
        subject: &str,
    ) -> Result<bool> {
        let rel = Relationship::new(resource, relation, subject);
        let local_key = rel.to_key();
        let storage_key = encode_storage_key(vault_id, local_key.as_bytes());

        // Check if already exists
        if table.get(&storage_key[..]).context(StorageSnafu)?.is_some() {
            return Ok(false);
        }

        let encoded =
            postcard::to_allocvec(&rel).map_err(|e| RelationshipError::Serialization {
                message: e.to_string(),
            })?;

        table
            .insert(&storage_key[..], &encoded[..])
            .context(StorageSnafu)?;

        Ok(true)
    }

    /// Delete a relationship.
    ///
    /// Returns true if deleted, false if not found.
    pub fn delete(
        table: &mut Table<'_, &'static [u8], &'static [u8]>,
        vault_id: VaultId,
        resource: &str,
        relation: &str,
        subject: &str,
    ) -> Result<bool> {
        let rel = Relationship::new(resource, relation, subject);
        let local_key = rel.to_key();
        let storage_key = encode_storage_key(vault_id, local_key.as_bytes());

        let existed = table.remove(&storage_key[..]).context(StorageSnafu)?;
        Ok(existed.is_some())
    }

    /// List all relationships in a vault.
    pub fn list_in_vault(
        table: &ReadOnlyTable<&'static [u8], &'static [u8]>,
        vault_id: VaultId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<Relationship>> {
        let prefix = vault_prefix(vault_id);
        let mut relationships = Vec::new();
        let mut count = 0;

        for result in table.range(&prefix[..]..).context(StorageSnafu)? {
            let (key, value) = result.context(StorageSnafu)?;
            let key_bytes = key.value();

            // Check we're still in the same vault
            if key_bytes.len() < 8 || &key_bytes[..8] != &prefix[..] {
                break;
            }

            if count >= offset {
                if relationships.len() >= limit {
                    break;
                }
                let rel = postcard::from_bytes(value.value()).map_err(|e| {
                    RelationshipError::Serialization {
                        message: e.to_string(),
                    }
                })?;
                relationships.push(rel);
            }
            count += 1;
        }

        Ok(relationships)
    }

    /// Count relationships in a vault.
    pub fn count_in_vault(
        table: &ReadOnlyTable<&'static [u8], &'static [u8]>,
        vault_id: VaultId,
    ) -> Result<usize> {
        let prefix = vault_prefix(vault_id);
        let mut count = 0;

        for result in table.range(&prefix[..]..).context(StorageSnafu)? {
            let (key, _) = result.context(StorageSnafu)?;
            let key_bytes = key.value();

            if key_bytes.len() < 8 || &key_bytes[..8] != &prefix[..] {
                break;
            }
            count += 1;
        }

        Ok(count)
    }

    /// List relationships for a specific resource.
    pub fn list_for_resource(
        table: &ReadOnlyTable<&'static [u8], &'static [u8]>,
        vault_id: VaultId,
        resource: &str,
        limit: usize,
    ) -> Result<Vec<Relationship>> {
        let prefix = vault_prefix(vault_id);
        let rel_prefix = format!("rel:{}#", resource);
        let mut relationships = Vec::new();

        for result in table.range(&prefix[..]..).context(StorageSnafu)? {
            let (key, value) = result.context(StorageSnafu)?;
            let key_bytes = key.value();

            // Check we're still in the same vault
            if key_bytes.len() < 9 || &key_bytes[..8] != &prefix[..] {
                break;
            }

            // Extract local key (skip vault_id and bucket_id)
            let local_key = &key_bytes[9..];

            // Check if it matches our resource prefix
            if local_key.starts_with(rel_prefix.as_bytes()) {
                let rel = postcard::from_bytes(value.value()).map_err(|e| {
                    RelationshipError::Serialization {
                        message: e.to_string(),
                    }
                })?;
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
mod tests {
    use super::*;
    use crate::engine::StorageEngine;

    #[test]
    fn test_relationship_crud() {
        let engine = StorageEngine::open_in_memory().expect("open engine");
        let vault_id = 1;

        // Create
        {
            let mut txn = engine.begin_write().expect("begin write");
            {
                let mut table = txn.open_table(Tables::RELATIONSHIPS).expect("open table");

                let created = RelationshipStore::create(
                    &mut table,
                    vault_id,
                    "doc:123",
                    "viewer",
                    "user:alice",
                )
                .expect("create");
                assert!(created);

                // Creating again should return false
                let created = RelationshipStore::create(
                    &mut table,
                    vault_id,
                    "doc:123",
                    "viewer",
                    "user:alice",
                )
                .expect("create");
                assert!(!created);
            }
            txn.commit().expect("commit");
        }

        // Check exists
        {
            let txn = engine.begin_read().expect("begin read");
            let table = txn.open_table(Tables::RELATIONSHIPS).expect("open table");

            assert!(
                RelationshipStore::exists(&table, vault_id, "doc:123", "viewer", "user:alice")
                    .expect("exists")
            );
            assert!(
                !RelationshipStore::exists(&table, vault_id, "doc:123", "editor", "user:alice")
                    .expect("exists")
            );
        }

        // Delete
        {
            let mut txn = engine.begin_write().expect("begin write");
            {
                let mut table = txn.open_table(Tables::RELATIONSHIPS).expect("open table");

                let deleted = RelationshipStore::delete(
                    &mut table,
                    vault_id,
                    "doc:123",
                    "viewer",
                    "user:alice",
                )
                .expect("delete");
                assert!(deleted);

                // Deleting again should return false
                let deleted = RelationshipStore::delete(
                    &mut table,
                    vault_id,
                    "doc:123",
                    "viewer",
                    "user:alice",
                )
                .expect("delete");
                assert!(!deleted);
            }
            txn.commit().expect("commit");
        }

        // Verify deleted
        {
            let txn = engine.begin_read().expect("begin read");
            let table = txn.open_table(Tables::RELATIONSHIPS).expect("open table");

            assert!(
                !RelationshipStore::exists(&table, vault_id, "doc:123", "viewer", "user:alice")
                    .expect("exists")
            );
        }
    }

    #[test]
    fn test_list_in_vault() {
        let engine = StorageEngine::open_in_memory().expect("open engine");
        let vault_id = 1;

        // Create multiple relationships
        {
            let mut txn = engine.begin_write().expect("begin write");
            {
                let mut table = txn.open_table(Tables::RELATIONSHIPS).expect("open table");

                for i in 0..5 {
                    RelationshipStore::create(
                        &mut table,
                        vault_id,
                        &format!("doc:{}", i),
                        "viewer",
                        "user:alice",
                    )
                    .expect("create");
                }
            }
            txn.commit().expect("commit");
        }

        // List
        {
            let txn = engine.begin_read().expect("begin read");
            let table = txn.open_table(Tables::RELATIONSHIPS).expect("open table");

            let rels = RelationshipStore::list_in_vault(&table, vault_id, 3, 0).expect("list");
            assert_eq!(rels.len(), 3);

            let count = RelationshipStore::count_in_vault(&table, vault_id).expect("count");
            assert_eq!(count, 5);
        }
    }

    #[test]
    fn test_vault_isolation() {
        let engine = StorageEngine::open_in_memory().expect("open engine");

        // Create in vault 1
        {
            let mut txn = engine.begin_write().expect("begin write");
            {
                let mut table = txn.open_table(Tables::RELATIONSHIPS).expect("open table");

                RelationshipStore::create(&mut table, 1, "doc:shared", "viewer", "user:alice")
                    .expect("create");
            }
            txn.commit().expect("commit");
        }

        // Vault 2 should not see vault 1's relationships
        {
            let txn = engine.begin_read().expect("begin read");
            let table = txn.open_table(Tables::RELATIONSHIPS).expect("open table");

            assert!(
                !RelationshipStore::exists(&table, 2, "doc:shared", "viewer", "user:alice")
                    .expect("exists")
            );

            let count = RelationshipStore::count_in_vault(&table, 2).expect("count");
            assert_eq!(count, 0);
        }
    }
}
