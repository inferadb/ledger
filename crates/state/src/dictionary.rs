//! Per-vault string dictionary for compact storage.
//!
//! Maps strings to compact [`InternId`] values (u16) within a given
//! [`InternCategory`], enabling space-efficient binary key encoding for
//! relationship tuples. Each vault maintains an independent dictionary
//! with write-through caching to avoid redundant storage lookups.

use std::collections::HashMap;

use inferadb_ledger_store::{StorageBackend, tables};
use inferadb_ledger_types::VaultId;
use snafu::{ResultExt, Snafu};

use crate::binary_keys::{InternCategory, InternId, split_typed_id};

/// Errors returned by [`VaultDictionary`] operations.
#[derive(Debug, Snafu)]
pub enum DictionaryError {
    /// Underlying storage operation failed.
    #[snafu(display("Storage error: {source}"))]
    Storage {
        /// The storage error that occurred.
        source: inferadb_ledger_store::Error,
        /// Source location for diagnostics.
        #[snafu(implicit)]
        location: snafu::Location,
    },
    /// The dictionary for a category has reached its maximum capacity of 65536 entries.
    #[snafu(display("Dictionary full for category {category:?}: maximum 65536 entries"))]
    Full {
        /// The category that is full.
        category: InternCategory,
    },
}

/// Per-vault string dictionary with write-through cache.
///
/// Maintains bidirectional mappings between strings and compact [`InternId`]
/// values, scoped by [`InternCategory`]. The cache is populated on [`load`](Self::load)
/// and kept in sync on [`intern`](Self::intern), so read-only lookups via
/// [`get_id`](Self::get_id) and [`resolve`](Self::resolve) never touch storage.
pub struct VaultDictionary {
    vault: VaultId,
    forward: HashMap<(InternCategory, String), InternId>,
    reverse: HashMap<(InternCategory, InternId), String>,
    next_ids: [u16; 3],
}

impl VaultDictionary {
    /// Creates an empty dictionary for the given vault.
    pub fn new(vault: VaultId) -> Self {
        Self { vault, forward: HashMap::new(), reverse: HashMap::new(), next_ids: [0; 3] }
    }

    /// Loads all dictionary entries for a vault from storage.
    ///
    /// Scans the [`StringDictionaryReverse`](tables::StringDictionaryReverse) table
    /// for keys prefixed with the vault ID, populating both forward and reverse
    /// caches. Tracks the maximum ID per category so subsequent [`intern`](Self::intern)
    /// calls continue the sequence.
    ///
    /// # Errors
    ///
    /// Returns [`DictionaryError::Storage`] if the scan fails.
    pub fn load<B: StorageBackend>(
        txn: &inferadb_ledger_store::ReadTransaction<'_, B>,
        vault: VaultId,
    ) -> Result<Self, DictionaryError> {
        let mut dict = Self::new(vault);
        let vault_bytes = vault.value().to_be_bytes();

        // Build prefix for this vault (8 bytes)
        let start_key: Vec<u8> = vault_bytes.to_vec();

        // End key: increment the vault ID prefix to scan only this vault's entries
        let end_key: Vec<u8> = (vault.value() + 1).to_be_bytes().to_vec();

        let iter = txn
            .range::<tables::StringDictionaryReverse>(Some(&start_key), Some(&end_key))
            .context(StorageSnafu)?;

        for (key, value) in iter {
            // Key format: [vault_id:8BE][category:1][intern_id:2BE]
            if key.len() < 11 {
                continue;
            }

            let category_byte = key[8];
            let category = match category_byte {
                0 => InternCategory::Relation,
                1 => InternCategory::ResourceType,
                2 => InternCategory::SubjectType,
                _ => continue,
            };

            let intern_id = InternId(u16::from_be_bytes([key[9], key[10]]));
            let string_value = String::from_utf8_lossy(&value).into_owned();

            dict.forward.insert((category, string_value.clone()), intern_id);
            dict.reverse.insert((category, intern_id), string_value);

            // Track max ID per category for next_ids
            let cat_idx = category_byte as usize;
            if intern_id.0 >= dict.next_ids[cat_idx] {
                dict.next_ids[cat_idx] = intern_id.0.saturating_add(1);
            }
        }

        Ok(dict)
    }

    /// Interns a string, returning its compact identifier.
    ///
    /// If the string is already interned in this category, returns the existing
    /// [`InternId`]. Otherwise assigns the next sequential ID and writes both
    /// forward and reverse mappings to storage.
    ///
    /// # Errors
    ///
    /// Returns [`DictionaryError::Full`] if the category already has 65536 entries.
    /// Returns [`DictionaryError::Storage`] if the storage write fails.
    pub fn intern<B: StorageBackend>(
        &mut self,
        txn: &mut inferadb_ledger_store::WriteTransaction<'_, B>,
        category: InternCategory,
        value: &str,
    ) -> Result<InternId, DictionaryError> {
        // Check cache first
        if let Some(&id) = self.forward.get(&(category, value.to_owned())) {
            return Ok(id);
        }

        let cat_idx = category as u8 as usize;
        let next = self.next_ids[cat_idx];

        // u16 overflow: next_ids is incremented after assignment, so if it wrapped
        // to 0 while entries exist, the category is full. We check before assigning.
        if next == 0 && !self.forward.is_empty() {
            // Only full if this specific category has entries
            let cat_count = self.forward.keys().filter(|(c, _)| *c == category).count();
            if cat_count >= 65536 {
                return Err(DictionaryError::Full { category });
            }
        }

        let id = InternId(next);
        // This will wrap to 0 after 65535, which is fine — the next call will
        // hit the full check above.
        self.next_ids[cat_idx] = next.wrapping_add(1);

        let vault_bytes = self.vault.value().to_be_bytes();
        let cat_byte = category as u8;

        // Forward key: [vault_id:8BE][category:1][string_bytes]
        let mut forward_key = Vec::with_capacity(9 + value.len());
        forward_key.extend_from_slice(&vault_bytes);
        forward_key.push(cat_byte);
        forward_key.extend_from_slice(value.as_bytes());

        // Forward value: [intern_id:2LE]
        let forward_value = id.0.to_le_bytes().to_vec();

        txn.insert::<tables::StringDictionary>(&forward_key, &forward_value)
            .context(StorageSnafu)?;

        // Reverse key: [vault_id:8BE][category:1][intern_id:2BE]
        let mut reverse_key = Vec::with_capacity(11);
        reverse_key.extend_from_slice(&vault_bytes);
        reverse_key.push(cat_byte);
        reverse_key.extend_from_slice(&id.0.to_be_bytes());

        // Reverse value: [string_bytes]
        let reverse_value = value.as_bytes().to_vec();

        txn.insert::<tables::StringDictionaryReverse>(&reverse_key, &reverse_value)
            .context(StorageSnafu)?;

        // Update cache
        self.forward.insert((category, value.to_owned()), id);
        self.reverse.insert((category, id), value.to_owned());

        Ok(id)
    }

    /// Looks up the [`InternId`] for a string from the cache.
    ///
    /// Returns `None` if the string has not been interned in this category.
    pub fn get_id(&self, category: InternCategory, value: &str) -> Option<InternId> {
        self.forward.get(&(category, value.to_owned())).copied()
    }

    /// Resolves an [`InternId`] back to its string from the cache.
    ///
    /// Returns `None` if the ID is not known in this category.
    pub fn resolve(&self, category: InternCategory, id: InternId) -> Option<&str> {
        self.reverse.get(&(category, id)).map(String::as_str)
    }

    /// Interns the type portion of a typed ID string.
    ///
    /// Splits `typed_id` on the first colon (via [`split_typed_id`]). The type
    /// prefix is interned; the local ID portion is returned as raw bytes. If there
    /// is no colon, the empty string is interned as the type and the full string
    /// becomes the local ID.
    ///
    /// # Errors
    ///
    /// Returns [`DictionaryError::Full`] or [`DictionaryError::Storage`] if
    /// interning the type string fails.
    pub fn intern_typed_id<B: StorageBackend>(
        &mut self,
        txn: &mut inferadb_ledger_store::WriteTransaction<'_, B>,
        category: InternCategory,
        typed_id: &str,
    ) -> Result<(InternId, Vec<u8>), DictionaryError> {
        let (type_str, local_str) = match split_typed_id(typed_id) {
            Some((t, l)) => (t, l),
            None => ("", typed_id),
        };

        let type_id = self.intern(txn, category, type_str)?;
        let local_bytes = local_str.as_bytes().to_vec();

        Ok((type_id, local_bytes))
    }

    /// Resolves a typed ID from its interned components.
    ///
    /// Reconstructs the original `"type:local_id"` string. If the type string
    /// is empty, returns just the local ID without a colon separator.
    pub fn resolve_typed_id(
        &self,
        category: InternCategory,
        type_id: InternId,
        local_id: &[u8],
    ) -> Option<String> {
        let type_str = self.resolve(category, type_id)?;
        let local_str = std::str::from_utf8(local_id).ok()?;

        if type_str.is_empty() {
            Some(local_str.to_owned())
        } else {
            Some(format!("{type_str}:{local_str}"))
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;
    use crate::engine::InMemoryStorageEngine;

    fn setup() -> (InMemoryStorageEngine, VaultId) {
        let engine = InMemoryStorageEngine::open().expect("open engine");
        (engine, VaultId::new(1))
    }

    #[test]
    fn intern_new_string_returns_sequential_ids() {
        let (engine, vault) = setup();
        let db = engine.db();
        let mut txn = db.write().expect("write txn");
        let mut dict = VaultDictionary::new(vault);

        let id0 = dict.intern(&mut txn, InternCategory::Relation, "owner").expect("intern owner");
        let id1 = dict.intern(&mut txn, InternCategory::Relation, "viewer").expect("intern viewer");

        assert_eq!(id0, InternId(0));
        assert_eq!(id1, InternId(1));
    }

    #[test]
    fn intern_existing_string_returns_same_id() {
        let (engine, vault) = setup();
        let db = engine.db();
        let mut txn = db.write().expect("write txn");
        let mut dict = VaultDictionary::new(vault);

        let id1 = dict.intern(&mut txn, InternCategory::Relation, "owner").expect("intern");
        let id2 = dict.intern(&mut txn, InternCategory::Relation, "owner").expect("intern again");

        assert_eq!(id1, id2);
    }

    #[test]
    fn different_categories_get_independent_ids() {
        let (engine, vault) = setup();
        let db = engine.db();
        let mut txn = db.write().expect("write txn");
        let mut dict = VaultDictionary::new(vault);

        let rel_id =
            dict.intern(&mut txn, InternCategory::Relation, "member").expect("intern relation");
        let res_id = dict
            .intern(&mut txn, InternCategory::ResourceType, "member")
            .expect("intern resource type");

        assert_eq!(rel_id, InternId(0));
        assert_eq!(res_id, InternId(0));
    }

    #[test]
    fn resolve_roundtrip() {
        let (engine, vault) = setup();
        let db = engine.db();
        let mut txn = db.write().expect("write txn");
        let mut dict = VaultDictionary::new(vault);

        let id = dict.intern(&mut txn, InternCategory::SubjectType, "user").expect("intern");

        assert_eq!(dict.get_id(InternCategory::SubjectType, "user"), Some(id));
        assert_eq!(dict.resolve(InternCategory::SubjectType, id), Some("user"));
    }

    #[test]
    fn load_from_storage_persists_across_instances() {
        let (engine, vault) = setup();
        let db = engine.db();

        // Intern and commit
        {
            let mut txn = db.write().expect("write txn");
            let mut dict = VaultDictionary::new(vault);
            dict.intern(&mut txn, InternCategory::Relation, "owner").expect("intern");
            dict.intern(&mut txn, InternCategory::ResourceType, "doc").expect("intern");
            txn.commit().expect("commit");
        }

        // Load from storage in a new dict
        {
            let txn = db.read().expect("read txn");
            let dict = VaultDictionary::load(&txn, vault).expect("load");

            assert_eq!(dict.get_id(InternCategory::Relation, "owner"), Some(InternId(0)));
            assert_eq!(dict.get_id(InternCategory::ResourceType, "doc"), Some(InternId(0)));
            assert_eq!(dict.resolve(InternCategory::Relation, InternId(0)), Some("owner"));
            assert_eq!(dict.resolve(InternCategory::ResourceType, InternId(0)), Some("doc"));
        }
    }

    #[test]
    fn vault_isolation() {
        let (engine, vault1) = setup();
        let vault2 = VaultId::new(2);
        let db = engine.db();

        // Intern in vault 1
        {
            let mut txn = db.write().expect("write txn");
            let mut dict1 = VaultDictionary::new(vault1);
            dict1.intern(&mut txn, InternCategory::Relation, "owner").expect("intern");
            txn.commit().expect("commit");
        }

        // Load vault 2 — should be empty
        {
            let txn = db.read().expect("read txn");
            let dict2 = VaultDictionary::load(&txn, vault2).expect("load");

            assert_eq!(dict2.get_id(InternCategory::Relation, "owner"), None);
        }
    }

    #[test]
    fn intern_typed_id_splits_on_first_colon() {
        let (engine, vault) = setup();
        let db = engine.db();
        let mut txn = db.write().expect("write txn");
        let mut dict = VaultDictionary::new(vault);

        let (type_id, local_bytes) = dict
            .intern_typed_id(&mut txn, InternCategory::ResourceType, "doc:123")
            .expect("intern typed id");

        assert_eq!(dict.resolve(InternCategory::ResourceType, type_id), Some("doc"));
        assert_eq!(local_bytes, b"123");
    }

    #[test]
    fn intern_typed_id_no_colon_uses_empty_type() {
        let (engine, vault) = setup();
        let db = engine.db();
        let mut txn = db.write().expect("write txn");
        let mut dict = VaultDictionary::new(vault);

        let (type_id, local_bytes) = dict
            .intern_typed_id(&mut txn, InternCategory::ResourceType, "foobar")
            .expect("intern typed id");

        assert_eq!(dict.resolve(InternCategory::ResourceType, type_id), Some(""));
        assert_eq!(local_bytes, b"foobar");
    }

    #[test]
    fn resolve_typed_id_roundtrip() {
        let (engine, vault) = setup();
        let db = engine.db();
        let mut txn = db.write().expect("write txn");
        let mut dict = VaultDictionary::new(vault);

        let original = "user:alice#member";
        let (type_id, local_bytes) =
            dict.intern_typed_id(&mut txn, InternCategory::SubjectType, original).expect("intern");

        let resolved = dict
            .resolve_typed_id(InternCategory::SubjectType, type_id, &local_bytes)
            .expect("resolve");

        assert_eq!(resolved, original);
    }

    #[test]
    fn intern_after_load_continues_sequence() {
        let (engine, vault) = setup();
        let db = engine.db();

        // Intern two entries and commit
        {
            let mut txn = db.write().expect("write txn");
            let mut dict = VaultDictionary::new(vault);
            dict.intern(&mut txn, InternCategory::Relation, "owner").expect("intern");
            dict.intern(&mut txn, InternCategory::Relation, "viewer").expect("intern");
            txn.commit().expect("commit");
        }

        // Load, then intern a new entry — should get ID 2
        {
            let txn = db.read().expect("read txn");
            let mut dict = VaultDictionary::load(&txn, vault).expect("load");
            drop(txn);

            let mut txn = db.write().expect("write txn");
            let id = dict.intern(&mut txn, InternCategory::Relation, "editor").expect("intern");

            assert_eq!(id, InternId(2));
        }
    }
}
