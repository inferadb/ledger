//! Entity storage operations.
//!
//! Provides direct entity CRUD operations separate from the state layer.
//! Used for lower-level access when the full state layer isn't needed.

use inferadb_ledger_store::{ReadTransaction, StorageBackend, WriteTransaction, tables};
use inferadb_ledger_types::{CodecError, Entity, VaultId, decode, encode};
use snafu::{ResultExt, Snafu};

use crate::keys::{bucket_prefix, encode_storage_key, vault_prefix};

/// Errors returned by [`EntityStore`] operations.
#[derive(Debug, Snafu)]
pub enum EntityError {
    /// Underlying storage operation failed.
    #[snafu(display("Storage error: {source}"))]
    Storage {
        source: inferadb_ledger_store::Error,
        #[snafu(implicit)]
        location: snafu::Location,
    },

    /// Serialization or deserialization failed.
    #[snafu(display("Codec error: {source}"))]
    Codec {
        source: CodecError,
        #[snafu(implicit)]
        location: snafu::Location,
    },
}

/// Result type for entity operations.
pub type Result<T> = std::result::Result<T, EntityError>;

/// Entity storage operations.
pub struct EntityStore;

impl EntityStore {
    /// Returns an entity by key.
    ///
    /// # Errors
    ///
    /// Returns `EntityError::Storage` if the read transaction fails.
    /// Returns `EntityError::Codec` if deserialization of the stored entity fails.
    pub fn get<B: StorageBackend>(
        txn: &ReadTransaction<'_, B>,
        vault_id: VaultId,
        key: &[u8],
    ) -> Result<Option<Entity>> {
        let storage_key = encode_storage_key(vault_id, key);

        match txn.get::<tables::Entities>(&storage_key).context(StorageSnafu)? {
            Some(data) => {
                let entity = decode(&data).context(CodecSnafu)?;
                Ok(Some(entity))
            },
            None => Ok(None),
        }
    }

    /// Sets an entity value.
    ///
    /// # Errors
    ///
    /// Returns `EntityError::Codec` if serialization of the entity fails.
    /// Returns `EntityError::Storage` if the write transaction fails.
    pub fn set<B: StorageBackend>(
        txn: &mut WriteTransaction<'_, B>,
        vault_id: VaultId,
        entity: &Entity,
    ) -> Result<()> {
        let storage_key = encode_storage_key(vault_id, &entity.key);
        let encoded = encode(entity).context(CodecSnafu)?;

        txn.insert::<tables::Entities>(&storage_key, &encoded).context(StorageSnafu)?;
        Ok(())
    }

    /// Deletes an entity.
    ///
    /// # Errors
    ///
    /// Returns `EntityError::Storage` if the delete operation fails.
    pub fn delete<B: StorageBackend>(
        txn: &mut WriteTransaction<'_, B>,
        vault_id: VaultId,
        key: &[u8],
    ) -> Result<bool> {
        let storage_key = encode_storage_key(vault_id, key);
        let existed = txn.delete::<tables::Entities>(&storage_key).context(StorageSnafu)?;
        Ok(existed)
    }

    /// Checks if an entity exists.
    ///
    /// # Errors
    ///
    /// Returns `EntityError::Storage` if the read transaction fails.
    pub fn exists<B: StorageBackend>(
        txn: &ReadTransaction<'_, B>,
        vault_id: VaultId,
        key: &[u8],
    ) -> Result<bool> {
        let storage_key = encode_storage_key(vault_id, key);
        Ok(txn.get::<tables::Entities>(&storage_key).context(StorageSnafu)?.is_some())
    }

    /// Lists all entities in a vault with pagination.
    ///
    /// Returns entities sorted by key with their local keys.
    ///
    /// # Errors
    ///
    /// Returns `EntityError::Storage` if the iterator or read transaction fails.
    /// Returns `EntityError::Codec` if deserialization of any entity fails.
    pub fn list_in_vault<B: StorageBackend>(
        txn: &ReadTransaction<'_, B>,
        vault_id: VaultId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<Entity>> {
        let prefix = vault_prefix(vault_id);
        let mut entities = Vec::new();

        let iter = txn.iter::<tables::Entities>().context(StorageSnafu)?;

        let mut count = 0;
        for (key_bytes, value) in iter {
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
                if entities.len() >= limit {
                    break;
                }
                let entity = decode(&value).context(CodecSnafu)?;
                entities.push(entity);
            }
            count += 1;
        }

        Ok(entities)
    }

    /// Lists all entities in a specific bucket within a vault.
    ///
    /// Used for state root computation.
    ///
    /// # Errors
    ///
    /// Returns `EntityError::Storage` if the iterator or read transaction fails.
    /// Returns `EntityError::Codec` if deserialization of any entity fails.
    pub fn list_in_bucket<B: StorageBackend>(
        txn: &ReadTransaction<'_, B>,
        vault_id: VaultId,
        bucket_id: u8,
    ) -> Result<Vec<Entity>> {
        let _prefix = bucket_prefix(vault_id, bucket_id);
        let mut entities = Vec::new();

        let iter = txn.iter::<tables::Entities>().context(StorageSnafu)?;

        for (key_bytes, value) in iter {
            if key_bytes.len() < 9 {
                continue;
            }

            let key_vault_id = i64::from_be_bytes(key_bytes[..8].try_into().unwrap_or([0; 8]));
            if key_vault_id < vault_id.value() {
                continue;
            }
            if key_vault_id > vault_id.value() {
                break;
            }

            if key_bytes[8] < bucket_id {
                continue;
            }
            if key_bytes[8] > bucket_id {
                break;
            }

            let entity = decode(&value).context(CodecSnafu)?;
            entities.push(entity);
        }

        Ok(entities)
    }

    /// Counts entities in a vault.
    ///
    /// # Errors
    ///
    /// Returns `EntityError::Storage` if the iterator or read transaction fails.
    pub fn count_in_vault<B: StorageBackend>(
        txn: &ReadTransaction<'_, B>,
        vault_id: VaultId,
    ) -> Result<usize> {
        let prefix = vault_prefix(vault_id);
        let mut count = 0;

        let iter = txn.iter::<tables::Entities>().context(StorageSnafu)?;

        for (key_bytes, _) in iter {
            if key_bytes.len() < 8 || key_bytes[..8] != prefix[..] {
                if key_bytes[..8] < prefix[..] {
                    continue;
                }
                break;
            }
            count += 1;
        }

        Ok(count)
    }

    /// Scans entities with a key prefix within a vault.
    ///
    /// # Errors
    ///
    /// Returns `EntityError::Storage` if the iterator or read transaction fails.
    /// Returns `EntityError::Codec` if deserialization of any matching entity fails.
    pub fn scan_prefix<B: StorageBackend>(
        txn: &ReadTransaction<'_, B>,
        vault_id: VaultId,
        key_prefix: &[u8],
        limit: usize,
    ) -> Result<Vec<Entity>> {
        let prefix = vault_prefix(vault_id);
        let mut entities = Vec::new();

        let iter = txn.iter::<tables::Entities>().context(StorageSnafu)?;

        for (key_bytes, value) in iter {
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
            if local_key.starts_with(key_prefix) {
                let entity = decode(&value).context(CodecSnafu)?;
                entities.push(entity);

                if entities.len() >= limit {
                    break;
                }
            }
        }

        Ok(entities)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods, unused_mut)]
mod tests {
    use inferadb_ledger_types::VaultId;

    use super::*;
    use crate::engine::InMemoryStorageEngine;

    #[test]
    fn test_entity_crud() {
        let engine = InMemoryStorageEngine::open().expect("open engine");
        let db = engine.db();
        let vault_id = VaultId::new(1);

        {
            let mut txn = db.write().expect("begin write");

            let entity = Entity {
                key: b"test_key".to_vec(),
                value: b"test_value".to_vec(),
                expires_at: 0,
                version: 1,
            };

            EntityStore::set(&mut txn, vault_id, &entity).expect("set entity");
            txn.commit().expect("commit");
        }

        {
            let txn = db.read().expect("begin read");

            let entity = EntityStore::get(&txn, vault_id, b"test_key")
                .expect("get entity")
                .expect("entity should exist");

            assert_eq!(entity.key, b"test_key");
            assert_eq!(entity.value, b"test_value");
            assert_eq!(entity.version, 1);
        }

        {
            let mut txn = db.write().expect("begin write");

            let deleted = EntityStore::delete(&mut txn, vault_id, b"test_key").expect("delete");
            assert!(deleted);
            txn.commit().expect("commit");
        }

        {
            let txn = db.read().expect("begin read");

            let entity = EntityStore::get(&txn, vault_id, b"test_key").expect("get entity");
            assert!(entity.is_none());
        }
    }

    #[test]
    fn test_list_in_vault() {
        let engine = InMemoryStorageEngine::open().expect("open engine");
        let db = engine.db();
        let vault_id = VaultId::new(1);

        {
            let mut txn = db.write().expect("begin write");

            for i in 0..5 {
                let entity = Entity {
                    key: format!("key_{}", i).into_bytes(),
                    value: format!("value_{}", i).into_bytes(),
                    expires_at: 0,
                    version: 1,
                };
                EntityStore::set(&mut txn, vault_id, &entity).expect("set entity");
            }
            txn.commit().expect("commit");
        }

        {
            let txn = db.read().expect("begin read");

            let entities = EntityStore::list_in_vault(&txn, vault_id, 3, 0).expect("list");
            assert_eq!(entities.len(), 3);

            let entities = EntityStore::list_in_vault(&txn, vault_id, 10, 3).expect("list");
            assert_eq!(entities.len(), 2);
        }
    }

    #[test]
    fn test_vault_isolation() {
        let engine = InMemoryStorageEngine::open().expect("open engine");
        let db = engine.db();

        {
            let mut txn = db.write().expect("begin write");

            let entity = Entity {
                key: b"shared_key".to_vec(),
                value: b"vault_1_value".to_vec(),
                expires_at: 0,
                version: 1,
            };
            EntityStore::set(&mut txn, VaultId::new(1), &entity).expect("set entity");
            txn.commit().expect("commit");
        }

        {
            let txn = db.read().expect("begin read");

            let entity =
                EntityStore::get(&txn, VaultId::new(2), b"shared_key").expect("get entity");
            assert!(entity.is_none());

            let count = EntityStore::count_in_vault(&txn, VaultId::new(1)).expect("count");
            assert_eq!(count, 1);

            let count = EntityStore::count_in_vault(&txn, VaultId::new(2)).expect("count");
            assert_eq!(count, 0);
        }
    }

    // Error conversion chain tests

    #[test]
    fn test_entity_exists() {
        let engine = InMemoryStorageEngine::open().expect("open engine");
        let db = engine.db();
        let vault_id = VaultId::new(1);

        // Entity doesn't exist initially
        {
            let txn = db.read().expect("begin read");
            assert!(!EntityStore::exists(&txn, vault_id, b"missing_key").expect("exists check"));
        }

        // Create entity
        {
            let mut txn = db.write().expect("begin write");
            let entity = Entity {
                key: b"exists_key".to_vec(),
                value: b"value".to_vec(),
                expires_at: 0,
                version: 1,
            };
            EntityStore::set(&mut txn, vault_id, &entity).expect("set entity");
            txn.commit().expect("commit");
        }

        // Entity exists now
        {
            let txn = db.read().expect("begin read");
            assert!(EntityStore::exists(&txn, vault_id, b"exists_key").expect("exists check"));
            // Still doesn't exist in different vault
            assert!(
                !EntityStore::exists(&txn, VaultId::new(999), b"exists_key").expect("exists check")
            );
        }
    }

    #[test]
    fn test_scan_prefix() {
        let engine = InMemoryStorageEngine::open().expect("open engine");
        let db = engine.db();
        let vault_id = VaultId::new(1);

        // Create entities with different prefixes
        {
            let mut txn = db.write().expect("begin write");

            let entities = vec![
                Entity {
                    key: b"user:1".to_vec(),
                    value: b"alice".to_vec(),
                    expires_at: 0,
                    version: 1,
                },
                Entity {
                    key: b"user:2".to_vec(),
                    value: b"bob".to_vec(),
                    expires_at: 0,
                    version: 1,
                },
                Entity {
                    key: b"team:1".to_vec(),
                    value: b"engineering".to_vec(),
                    expires_at: 0,
                    version: 1,
                },
                Entity {
                    key: b"user:3".to_vec(),
                    value: b"charlie".to_vec(),
                    expires_at: 0,
                    version: 1,
                },
                Entity {
                    key: b"team:2".to_vec(),
                    value: b"design".to_vec(),
                    expires_at: 0,
                    version: 1,
                },
            ];

            for entity in entities {
                EntityStore::set(&mut txn, vault_id, &entity).expect("set entity");
            }
            txn.commit().expect("commit");
        }

        // Scan for "user:" prefix
        {
            let txn = db.read().expect("begin read");
            let users = EntityStore::scan_prefix(&txn, vault_id, b"user:", 10).expect("scan");
            assert_eq!(users.len(), 3);
            for user in &users {
                assert!(user.key.starts_with(b"user:"));
            }
        }

        // Scan for "team:" prefix
        {
            let txn = db.read().expect("begin read");
            let teams = EntityStore::scan_prefix(&txn, vault_id, b"team:", 10).expect("scan");
            assert_eq!(teams.len(), 2);
        }

        // Scan with limit
        {
            let txn = db.read().expect("begin read");
            let users = EntityStore::scan_prefix(&txn, vault_id, b"user:", 2).expect("scan");
            assert_eq!(users.len(), 2);
        }

        // Scan non-existent prefix
        {
            let txn = db.read().expect("begin read");
            let none = EntityStore::scan_prefix(&txn, vault_id, b"missing:", 10).expect("scan");
            assert!(none.is_empty());
        }
    }

    #[test]
    fn test_list_in_bucket() {
        let engine = InMemoryStorageEngine::open().expect("open engine");
        let db = engine.db();
        let vault_id = VaultId::new(1);

        // Create several entities
        {
            let mut txn = db.write().expect("begin write");

            for i in 0..10 {
                let entity = Entity {
                    key: format!("bucket_test_{}", i).into_bytes(),
                    value: format!("value_{}", i).into_bytes(),
                    expires_at: 0,
                    version: 1,
                };
                EntityStore::set(&mut txn, vault_id, &entity).expect("set entity");
            }
            txn.commit().expect("commit");
        }

        // List entities in bucket 0 (the first bucket based on key hashing)
        // Note: Actual bucket assignment depends on key hashing
        {
            let txn = db.read().expect("begin read");
            // Try a few buckets to find where our entities landed
            let mut found_any = false;
            for bucket_id in 0..=255u8 {
                let entities =
                    EntityStore::list_in_bucket(&txn, vault_id, bucket_id).expect("list bucket");
                if !entities.is_empty() {
                    found_any = true;
                }
            }
            assert!(found_any, "Should find entities in at least one bucket");
        }
    }

    #[test]
    fn test_count_in_vault_multiple_vaults() {
        let engine = InMemoryStorageEngine::open().expect("open engine");
        let db = engine.db();

        // Create entities in multiple vaults
        {
            let mut txn = db.write().expect("begin write");

            // Vault 1: 3 entities
            for i in 0..3 {
                let entity = Entity {
                    key: format!("v1_key_{}", i).into_bytes(),
                    value: b"value".to_vec(),
                    expires_at: 0,
                    version: 1,
                };
                EntityStore::set(&mut txn, VaultId::new(1), &entity).expect("set entity");
            }

            // Vault 2: 5 entities
            for i in 0..5 {
                let entity = Entity {
                    key: format!("v2_key_{}", i).into_bytes(),
                    value: b"value".to_vec(),
                    expires_at: 0,
                    version: 1,
                };
                EntityStore::set(&mut txn, VaultId::new(2), &entity).expect("set entity");
            }

            txn.commit().expect("commit");
        }

        // Verify counts
        {
            let txn = db.read().expect("begin read");

            assert_eq!(EntityStore::count_in_vault(&txn, VaultId::new(1)).expect("count"), 3);
            assert_eq!(EntityStore::count_in_vault(&txn, VaultId::new(2)).expect("count"), 5);
            assert_eq!(EntityStore::count_in_vault(&txn, VaultId::new(999)).expect("count"), 0);
        }
    }

    #[test]
    fn test_delete_nonexistent() {
        let engine = InMemoryStorageEngine::open().expect("open engine");
        let db = engine.db();
        let vault_id = VaultId::new(1);

        // Delete non-existent entity should return false
        {
            let mut txn = db.write().expect("begin write");
            let deleted = EntityStore::delete(&mut txn, vault_id, b"nonexistent").expect("delete");
            assert!(!deleted);
            txn.commit().expect("commit");
        }
    }

    #[test]
    fn test_list_in_vault_empty() {
        let engine = InMemoryStorageEngine::open().expect("open engine");
        let db = engine.db();

        // List from empty vault
        {
            let txn = db.read().expect("begin read");
            let entities = EntityStore::list_in_vault(&txn, VaultId::new(1), 10, 0).expect("list");
            assert!(entities.is_empty());
        }
    }

    // Test EntityError Display implementations
    #[test]
    fn test_entity_error_display() {
        use std::error::Error;

        use snafu::ResultExt;

        // Create a codec error via context selector and verify it converts properly
        fn create_entity_error() -> std::result::Result<(), EntityError> {
            let malformed: &[u8] = &[0xFF, 0xFF, 0xFF];
            let _: u64 = inferadb_ledger_types::decode(malformed).context(CodecSnafu)?;
            Ok(())
        }

        let entity_err = create_entity_error().unwrap_err();
        let display = format!("{entity_err}");

        // Should have format "Codec error: Decoding failed: <postcard error>"
        assert!(display.starts_with("Codec error:"), "Expected 'Codec error:', got: {display}");
        assert!(
            display.contains("Decoding failed"),
            "Expected to contain 'Decoding failed', got: {display}"
        );

        // Verify source chain is preserved
        assert!(entity_err.source().is_some(), "EntityError::Codec should have a source");
    }

    // Test error conversion chain: CodecError -> EntityError
    #[test]
    fn test_entity_error_from_codec_error() {
        use snafu::ResultExt;

        // Simulate what happens when a codec operation fails in EntityStore
        fn simulate_codec_failure() -> std::result::Result<(), EntityError> {
            let malformed: &[u8] = &[0xFF, 0xFF];
            let _: u64 = inferadb_ledger_types::decode(malformed).context(CodecSnafu)?;
            Ok(())
        }

        let result = simulate_codec_failure();
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert!(matches!(err, EntityError::Codec { .. }), "Should be EntityError::Codec variant");
    }
}
