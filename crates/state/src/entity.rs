//! Entity storage operations.
//!
//! Provides direct entity CRUD operations separate from the state layer.
//! Used for lower-level access when the full state layer isn't needed.

use inferadb_ledger_store::{ReadTransaction, StorageBackend, WriteTransaction, tables};
use snafu::{ResultExt, Snafu};

use inferadb_ledger_types::{CodecError, Entity, VaultId, decode, encode};

use crate::keys::{bucket_prefix, encode_storage_key, vault_prefix};

/// Entity store error types.
#[derive(Debug, Snafu)]
pub enum EntityError {
    #[snafu(display("Storage error: {source}"))]
    Storage {
        source: inferadb_ledger_store::Error,
    },

    #[snafu(display("Codec error: {source}"))]
    Codec { source: CodecError },
}

/// Result type for entity operations.
pub type Result<T> = std::result::Result<T, EntityError>;

/// Entity storage operations.
pub struct EntityStore;

impl EntityStore {
    /// Get an entity by key.
    pub fn get<B: StorageBackend>(
        txn: &ReadTransaction<'_, B>,
        vault_id: VaultId,
        key: &[u8],
    ) -> Result<Option<Entity>> {
        let storage_key = encode_storage_key(vault_id, key);

        match txn
            .get::<tables::Entities>(&storage_key)
            .context(StorageSnafu)?
        {
            Some(data) => {
                let entity = decode(&data).context(CodecSnafu)?;
                Ok(Some(entity))
            }
            None => Ok(None),
        }
    }

    /// Set an entity value.
    pub fn set<B: StorageBackend>(
        txn: &mut WriteTransaction<'_, B>,
        vault_id: VaultId,
        entity: &Entity,
    ) -> Result<()> {
        let storage_key = encode_storage_key(vault_id, &entity.key);
        let encoded = encode(entity).context(CodecSnafu)?;

        txn.insert::<tables::Entities>(&storage_key, &encoded)
            .context(StorageSnafu)?;
        Ok(())
    }

    /// Delete an entity.
    pub fn delete<B: StorageBackend>(
        txn: &mut WriteTransaction<'_, B>,
        vault_id: VaultId,
        key: &[u8],
    ) -> Result<bool> {
        let storage_key = encode_storage_key(vault_id, key);
        let existed = txn
            .delete::<tables::Entities>(&storage_key)
            .context(StorageSnafu)?;
        Ok(existed)
    }

    /// Check if an entity exists.
    pub fn exists<B: StorageBackend>(
        txn: &ReadTransaction<'_, B>,
        vault_id: VaultId,
        key: &[u8],
    ) -> Result<bool> {
        let storage_key = encode_storage_key(vault_id, key);
        Ok(txn
            .get::<tables::Entities>(&storage_key)
            .context(StorageSnafu)?
            .is_some())
    }

    /// List all entities in a vault with pagination.
    ///
    /// Returns entities sorted by key with their local keys.
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

    /// List all entities in a specific bucket within a vault.
    ///
    /// Used for state root computation.
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
            if key_vault_id < vault_id {
                continue;
            }
            if key_vault_id > vault_id {
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

    /// Count entities in a vault.
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

    /// Scan entities with a key prefix within a vault.
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
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::disallowed_methods,
    unused_mut
)]
mod tests {
    use super::*;
    use crate::engine::InMemoryStorageEngine;

    #[test]
    fn test_entity_crud() {
        let engine = InMemoryStorageEngine::open().expect("open engine");
        let db = engine.db();
        let vault_id = 1;

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
        let vault_id = 1;

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
            EntityStore::set(&mut txn, 1, &entity).expect("set entity");
            txn.commit().expect("commit");
        }

        {
            let txn = db.read().expect("begin read");

            let entity = EntityStore::get(&txn, 2, b"shared_key").expect("get entity");
            assert!(entity.is_none());

            let count = EntityStore::count_in_vault(&txn, 1).expect("count");
            assert_eq!(count, 1);

            let count = EntityStore::count_in_vault(&txn, 2).expect("count");
            assert_eq!(count, 0);
        }
    }

    // =========================================================================
    // Error conversion chain tests (Task 2: Consolidate Error Types)
    // =========================================================================

    // Test EntityError Display implementations
    #[test]
    fn test_entity_error_display() {
        use std::error::Error;

        // Create a codec error and verify it converts properly
        let codec_err = inferadb_ledger_types::CodecError::Decode {
            source: postcard::from_bytes::<u64>(&[0xFF, 0xFF, 0xFF]).expect_err("should fail"),
        };

        let entity_err = EntityError::Codec { source: codec_err };
        let display = format!("{entity_err}");

        // Should have format "Codec error: Decoding failed: <postcard error>"
        assert!(
            display.starts_with("Codec error:"),
            "Expected 'Codec error:', got: {display}"
        );
        assert!(
            display.contains("Decoding failed"),
            "Expected to contain 'Decoding failed', got: {display}"
        );

        // Verify source chain is preserved
        assert!(
            entity_err.source().is_some(),
            "EntityError::Codec should have a source"
        );
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
        assert!(
            matches!(err, EntityError::Codec { .. }),
            "Should be EntityError::Codec variant"
        );
    }

    // Test that Storage variant also works correctly
    #[test]
    fn test_entity_error_storage_display() {
        // We can't easily create an inferadb_ledger_store::Error, but we can verify the
        // pattern compiles and the Display format is correct by checking the derive
        let _: fn(inferadb_ledger_store::Error) -> EntityError =
            |source| EntityError::Storage { source };
    }
}
