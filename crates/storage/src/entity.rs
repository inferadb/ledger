//! Entity storage operations.
//!
//! Provides direct entity CRUD operations separate from the state layer.
//! Used for lower-level access when the full state layer isn't needed.

use redb::{ReadOnlyTable, Table};
use snafu::{ResultExt, Snafu};

use ledger_types::{Entity, VaultId};

use crate::keys::{bucket_prefix, encode_storage_key, vault_prefix};

/// Entity store error types.
#[derive(Debug, Snafu)]
pub enum EntityError {
    #[snafu(display("Storage error: {source}"))]
    Storage { source: redb::StorageError },

    #[snafu(display("Serialization error: {message}"))]
    Serialization { message: String },
}

/// Result type for entity operations.
pub type Result<T> = std::result::Result<T, EntityError>;

/// Entity storage operations.
pub struct EntityStore;

impl EntityStore {
    /// Get an entity by key.
    pub fn get(
        table: &ReadOnlyTable<&'static [u8], &'static [u8]>,
        vault_id: VaultId,
        key: &[u8],
    ) -> Result<Option<Entity>> {
        let storage_key = encode_storage_key(vault_id, key);

        match table.get(&storage_key[..]).context(StorageSnafu)? {
            Some(data) => {
                let entity =
                    postcard::from_bytes(data.value()).map_err(|e| EntityError::Serialization {
                        message: e.to_string(),
                    })?;
                Ok(Some(entity))
            }
            None => Ok(None),
        }
    }

    /// Set an entity value.
    pub fn set(
        table: &mut Table<'_, &'static [u8], &'static [u8]>,
        vault_id: VaultId,
        entity: &Entity,
    ) -> Result<()> {
        let storage_key = encode_storage_key(vault_id, &entity.key);
        let encoded = postcard::to_allocvec(entity).map_err(|e| EntityError::Serialization {
            message: e.to_string(),
        })?;

        table
            .insert(&storage_key[..], &encoded[..])
            .context(StorageSnafu)?;
        Ok(())
    }

    /// Delete an entity.
    pub fn delete(
        table: &mut Table<'_, &'static [u8], &'static [u8]>,
        vault_id: VaultId,
        key: &[u8],
    ) -> Result<bool> {
        let storage_key = encode_storage_key(vault_id, key);
        let existed = table.remove(&storage_key[..]).context(StorageSnafu)?;
        Ok(existed.is_some())
    }

    /// Check if an entity exists.
    pub fn exists(
        table: &ReadOnlyTable<&'static [u8], &'static [u8]>,
        vault_id: VaultId,
        key: &[u8],
    ) -> Result<bool> {
        let storage_key = encode_storage_key(vault_id, key);
        Ok(table.get(&storage_key[..]).context(StorageSnafu)?.is_some())
    }

    /// List all entities in a vault with pagination.
    ///
    /// Returns entities sorted by key with their local keys.
    pub fn list_in_vault(
        table: &ReadOnlyTable<&'static [u8], &'static [u8]>,
        vault_id: VaultId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<Entity>> {
        let prefix = vault_prefix(vault_id);
        let mut entities = Vec::new();

        for (count, result) in table
            .range(&prefix[..]..)
            .context(StorageSnafu)?
            .enumerate()
        {
            let (key, value) = result.context(StorageSnafu)?;
            let key_bytes = key.value();

            // Check we're still in the same vault
            if key_bytes.len() < 8 {
                break;
            }
            if key_bytes[..8] != prefix[..] {
                break;
            }

            if count >= offset {
                if entities.len() >= limit {
                    break;
                }
                let entity = postcard::from_bytes(value.value()).map_err(|e| {
                    EntityError::Serialization {
                        message: e.to_string(),
                    }
                })?;
                entities.push(entity);
            }
        }

        Ok(entities)
    }

    /// List all entities in a specific bucket within a vault.
    ///
    /// Used for state root computation.
    pub fn list_in_bucket(
        table: &ReadOnlyTable<&'static [u8], &'static [u8]>,
        vault_id: VaultId,
        bucket_id: u8,
    ) -> Result<Vec<Entity>> {
        let prefix = bucket_prefix(vault_id, bucket_id);
        let mut entities = Vec::new();

        // Determine end of range
        if bucket_id == 255 {
            // Scan to end of vault's keyspace
            for result in table.range(&prefix[..]..).context(StorageSnafu)? {
                let (key, value) = result.context(StorageSnafu)?;
                let key_bytes = key.value();

                // Check we're still in the same vault and bucket
                if key_bytes.len() < 9 {
                    break;
                }
                let key_vault_id = i64::from_be_bytes(key_bytes[..8].try_into().unwrap_or([0; 8]));
                if key_vault_id != vault_id || key_bytes[8] != 255 {
                    break;
                }

                let entity = postcard::from_bytes(value.value()).map_err(|e| {
                    EntityError::Serialization {
                        message: e.to_string(),
                    }
                })?;
                entities.push(entity);
            }
        } else {
            let mut end = prefix;
            end[8] = bucket_id + 1;

            for result in table.range(&prefix[..]..&end[..]).context(StorageSnafu)? {
                let (_, value) = result.context(StorageSnafu)?;
                let entity = postcard::from_bytes(value.value()).map_err(|e| {
                    EntityError::Serialization {
                        message: e.to_string(),
                    }
                })?;
                entities.push(entity);
            }
        }

        Ok(entities)
    }

    /// Count entities in a vault.
    pub fn count_in_vault(
        table: &ReadOnlyTable<&'static [u8], &'static [u8]>,
        vault_id: VaultId,
    ) -> Result<usize> {
        let prefix = vault_prefix(vault_id);
        let mut count = 0;

        for result in table.range(&prefix[..]..).context(StorageSnafu)? {
            let (key, _) = result.context(StorageSnafu)?;
            let key_bytes = key.value();

            if key_bytes.len() < 8 || key_bytes[..8] != prefix[..] {
                break;
            }
            count += 1;
        }

        Ok(count)
    }

    /// Scan entities with a key prefix within a vault.
    pub fn scan_prefix(
        table: &ReadOnlyTable<&'static [u8], &'static [u8]>,
        vault_id: VaultId,
        key_prefix: &[u8],
        limit: usize,
    ) -> Result<Vec<Entity>> {
        let prefix = vault_prefix(vault_id);
        let mut entities = Vec::new();

        for result in table.range(&prefix[..]..).context(StorageSnafu)? {
            let (key, value) = result.context(StorageSnafu)?;
            let key_bytes = key.value();

            // Check we're still in the same vault
            if key_bytes.len() < 9 {
                break;
            }
            if key_bytes[..8] != prefix[..] {
                break;
            }

            // Extract local key (skip vault_id and bucket_id)
            let local_key = &key_bytes[9..];

            // Check prefix match
            if local_key.starts_with(key_prefix) {
                let entity = postcard::from_bytes(value.value()).map_err(|e| {
                    EntityError::Serialization {
                        message: e.to_string(),
                    }
                })?;
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
    use crate::engine::StorageEngine;
    use crate::tables::Tables;

    #[test]
    fn test_entity_crud() {
        let engine = StorageEngine::open_in_memory().expect("open engine");
        let vault_id = 1;

        // Write
        {
            let mut txn = engine.begin_write().expect("begin write");
            {
                let mut table = txn.open_table(Tables::ENTITIES).expect("open table");

                let entity = Entity {
                    key: b"test_key".to_vec(),
                    value: b"test_value".to_vec(),
                    expires_at: 0,
                    version: 1,
                };

                EntityStore::set(&mut table, vault_id, &entity).expect("set entity");
            }
            txn.commit().expect("commit");
        }

        // Read
        {
            let txn = engine.begin_read().expect("begin read");
            let table = txn.open_table(Tables::ENTITIES).expect("open table");

            let entity = EntityStore::get(&table, vault_id, b"test_key")
                .expect("get entity")
                .expect("entity should exist");

            assert_eq!(entity.key, b"test_key");
            assert_eq!(entity.value, b"test_value");
            assert_eq!(entity.version, 1);
        }

        // Delete
        {
            let mut txn = engine.begin_write().expect("begin write");
            {
                let mut table = txn.open_table(Tables::ENTITIES).expect("open table");

                let deleted =
                    EntityStore::delete(&mut table, vault_id, b"test_key").expect("delete");
                assert!(deleted);
            }
            txn.commit().expect("commit");
        }

        // Verify deleted
        {
            let txn = engine.begin_read().expect("begin read");
            let table = txn.open_table(Tables::ENTITIES).expect("open table");

            let entity = EntityStore::get(&table, vault_id, b"test_key").expect("get entity");
            assert!(entity.is_none());
        }
    }

    #[test]
    fn test_list_in_vault() {
        let engine = StorageEngine::open_in_memory().expect("open engine");
        let vault_id = 1;

        // Write multiple entities
        {
            let mut txn = engine.begin_write().expect("begin write");
            {
                let mut table = txn.open_table(Tables::ENTITIES).expect("open table");

                for i in 0..5 {
                    let entity = Entity {
                        key: format!("key_{}", i).into_bytes(),
                        value: format!("value_{}", i).into_bytes(),
                        expires_at: 0,
                        version: 1,
                    };
                    EntityStore::set(&mut table, vault_id, &entity).expect("set entity");
                }
            }
            txn.commit().expect("commit");
        }

        // List with pagination
        {
            let txn = engine.begin_read().expect("begin read");
            let table = txn.open_table(Tables::ENTITIES).expect("open table");

            let entities = EntityStore::list_in_vault(&table, vault_id, 3, 0).expect("list");
            assert_eq!(entities.len(), 3);

            let entities = EntityStore::list_in_vault(&table, vault_id, 10, 3).expect("list");
            assert_eq!(entities.len(), 2);
        }
    }

    #[test]
    fn test_vault_isolation() {
        let engine = StorageEngine::open_in_memory().expect("open engine");

        // Write to vault 1
        {
            let mut txn = engine.begin_write().expect("begin write");
            {
                let mut table = txn.open_table(Tables::ENTITIES).expect("open table");

                let entity = Entity {
                    key: b"shared_key".to_vec(),
                    value: b"vault_1_value".to_vec(),
                    expires_at: 0,
                    version: 1,
                };
                EntityStore::set(&mut table, 1, &entity).expect("set entity");
            }
            txn.commit().expect("commit");
        }

        // Vault 2 should not see vault 1's data
        {
            let txn = engine.begin_read().expect("begin read");
            let table = txn.open_table(Tables::ENTITIES).expect("open table");

            let entity = EntityStore::get(&table, 2, b"shared_key").expect("get entity");
            assert!(entity.is_none());

            let count = EntityStore::count_in_vault(&table, 1).expect("count");
            assert_eq!(count, 1);

            let count = EntityStore::count_in_vault(&table, 2).expect("count");
            assert_eq!(count, 0);
        }
    }
}
