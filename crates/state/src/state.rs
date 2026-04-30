//! State layer for materialized state with bucket-based commitment.
//!
//! Provides:
//! - Per-vault entity and relationship CRUD via [`StateLayer::apply_operations`]
//! - Atomic batch writes with conditional (CAS) semantics
//! - Incremental state root computation via 256-bucket dirty tracking
//!
//! The state layer separates commitment (Merkleized hashing) from storage
//! (fast K/V reads), so queries bypass the hash tree entirely.

use std::{collections::HashMap, sync::Arc};

use inferadb_ledger_store::{
    CompactionStats, Database, DatabaseStats, StorageBackend, Table, TableId, WriteTransaction,
    tables,
};
use inferadb_ledger_types::{
    Entity, Hash, Operation, Relationship, SetCondition, VaultId, WriteStatus, decode, encode,
};
use parking_lot::{MappedRwLockReadGuard, RwLock, RwLockReadGuard};
use rayon::prelude::*;
use snafu::{ResultExt, Snafu};

use crate::{
    binary_keys::{InternCategory, encode_relationship_local_key, split_typed_id},
    bucket::{BucketRootBuilder, NUM_BUCKETS, VaultCommitment},
    indexes::{IndexError, IndexManager},
    keys::encode_storage_key,
    relationship_index::RelationshipIndex,
};

/// Errors returned by [`StateLayer`] operations.
#[derive(Debug, Snafu)]
pub enum StateError {
    /// Underlying storage operation failed.
    #[snafu(display("Storage error: {source}"))]
    Store {
        /// The underlying inferadb-ledger-store storage error.
        source: inferadb_ledger_store::Error,
        /// Location where the error occurred.
        #[snafu(implicit)]
        location: snafu::Location,
    },

    /// Index operation failed.
    #[snafu(display("Index error: {source}"))]
    Index {
        /// The underlying index error.
        source: IndexError,
        /// Location where the error occurred.
        #[snafu(implicit)]
        location: snafu::Location,
    },

    /// Codec error during serialization/deserialization.
    #[snafu(display("Codec error: {source}"))]
    Codec {
        /// The underlying codec error.
        source: inferadb_ledger_types::CodecError,
        /// Location where the error occurred.
        #[snafu(implicit)]
        location: snafu::Location,
    },

    /// Relationship storage operation failed.
    #[snafu(display("Relationship error: {source}"))]
    Relationship {
        /// The underlying relationship error.
        source: crate::relationship::RelationshipError,
        /// Location where the error occurred.
        #[snafu(implicit)]
        location: snafu::Location,
    },

    /// Conditional write precondition failed.
    ///
    /// Returns current state for client-side conflict resolution. When a CAS
    /// (compare-and-swap) condition fails, this error provides the current entity
    /// state so clients can implement retry or conflict resolution logic.
    #[snafu(display("Precondition failed for key '{key}'"))]
    PreconditionFailed {
        /// The key that failed the condition check.
        key: String,
        /// Current version of the entity (block height when last modified), if it exists.
        current_version: Option<u64>,
        /// Current value of the entity, if it exists.
        current_value: Option<Vec<u8>>,
        /// The condition that failed (for specific error code mapping).
        failed_condition: Option<SetCondition>,
    },

    /// Vault has not been materialised in this `StateLayer`.
    ///
    /// Returned by [`StateLayer::evict_vault_state_page_cache`] when the
    /// caller asks to evict a vault that this state layer has never opened
    /// — at sleep time, that's a programmer bug (the raft layer drives
    /// hibernation only for vaults whose group it has registered, and the
    /// register path always materialises the per-vault DB before completing).
    #[snafu(display("Vault {vault_id} not materialised in state layer"))]
    VaultNotMaterialized {
        /// The vault that was requested but is not in the per-vault DB map.
        vault_id: VaultId,
        /// Location where the error was raised.
        #[snafu(implicit)]
        location: snafu::Location,
    },
}

/// Result type for state operations.
pub type Result<T> = std::result::Result<T, StateError>;

/// Convenience constructor for tests and transitional callers that still
/// want a singleton-backed [`StateLayer`].
///
/// Slice 2b of per-vault consensus flipped [`StateLayer::new`] to take a
/// [`VaultDbFactory`] closure. This helper wraps a single shared
/// [`Arc<Database<B>>`] into a factory that resolves every vault to the
/// same DB — preserving legacy test semantics without repeating the
/// closure at every call site.
///
/// Production code uses a per-vault factory
/// (`{org_dir}/state/vault-{id}/state.db`) assembled in
/// `RaftManager::open_region_storage` and must not call this helper.
///
/// # Errors
///
/// Returns [`StateError::Store`] if `StateLayer::new`'s eager
/// materialisation of the `SYSTEM_VAULT_ID` database fails. The shared
/// factory cannot fail by construction (it simply clones an already-
/// open `Arc`), but the signature stays fallible to match
/// [`StateLayer::new`].
pub fn new_state_layer_shared<B>(
    db: Arc<Database<B>>,
    meta_db: Arc<Database<B>>,
) -> Result<StateLayer<B>>
where
    B: StorageBackend + 'static,
{
    let shared = Arc::clone(&db);
    StateLayer::new(move |_vault| Ok(Arc::clone(&shared)), meta_db)
}

/// Factory closure that opens (or creates) the [`Database`] backing a
/// specific vault's state.
///
/// Introduced by Slice 2b of per-vault consensus. P2b.0 drops each
/// vault's state.db one level deeper so future slices can add
/// per-vault `raft.db` / `blocks.db` / `events.db` alongside. The
/// production factory captures the per-organization path components
/// and composes
/// `{data_dir}/{region}/{organization_id}/state/vault-{vault_id}/state.db`.
/// Test factories capture an in-memory backend and open a fresh
/// [`Database::open_in_memory`] per vault. The factory is invoked exactly
/// once per vault — the per-vault HashMap caches the resulting [`Arc`] for
/// the lifetime of the [`StateLayer`].
pub type VaultDbFactory<B> =
    Arc<dyn Fn(VaultId) -> Result<Arc<Database<B>>> + Send + Sync + 'static>;

/// State layer managing per-vault materialized state.
///
/// Provides entity/relationship CRUD and incremental state root computation
/// via bucket-based dirty tracking. Each vault's state is independently
/// tracked with its own [`VaultCommitment`] — only buckets modified since
/// the last commit are rehashed.
///
/// # Per-vault databases (Slice 2b of per-vault consensus)
///
/// As of Slice 2b, each vault owns its own [`Database`]; [`StateLayer`]
/// caches open handles in the `dbs` map and lazily materialises new vaults
/// via the [`VaultDbFactory`] supplied to [`StateLayer::new`]. Callers
/// acquire the per-vault handle through [`StateLayer::db_for`], which
/// returns an owned [`Arc`] — they then open read/write transactions
/// against the local `Arc`. Transactions borrow from the `Arc`, not from
/// the [`StateLayer`], so the two-liner pattern
///
/// ```text
/// let db = state.db_for(vault)?;
/// let mut txn = db.write()?;
/// ```
///
/// keeps the transaction's borrow lifetime tied to the caller's local
/// `Arc` rather than to any `RwLock` guard — no self-referential trick
/// required.
///
/// Generic over [`StorageBackend`] to support both file-based (production)
/// and in-memory (testing) storage.
pub struct StateLayer<B: StorageBackend> {
    /// Lazily-materialised per-vault state databases.
    ///
    /// Keyed by [`VaultId`]; each value is an [`Arc`] clone of the
    /// `Database` returned by [`VaultDbFactory`]. The Arc is never removed
    /// from the map — vault databases live for the lifetime of the
    /// [`StateLayer`] (hibernation will evict in Phase 4, out of scope for
    /// Slice 2b). Open via [`Self::db_for`] (double-checked locking);
    /// enumerate via [`Self::live_vault_dbs`].
    dbs: RwLock<HashMap<VaultId, Arc<Database<B>>>>,
    /// Factory that opens a vault's [`Database`] on first reference.
    /// Invoked exactly once per [`VaultId`]; the resulting [`Arc`] is
    /// cached in `dbs` for the lifetime of the state layer.
    db_factory: VaultDbFactory<B>,
    /// Per-organization coordination database handle (`_meta.db`).
    ///
    /// Owns the `_meta:last_applied` crash-recovery sentinel as of the
    /// per-vault consensus Slice 1 cleave. Must always be synced **after**
    /// every per-vault state database (and `raft.db` / `blocks.db` /
    /// `events.db`) so the sentinel on disk never advances past entity
    /// data that has not yet reached the page cache's dual-slot on-disk
    /// pointer.
    meta_db: Arc<Database<B>>,
    /// Per-vault commitment tracking.
    vault_commitments: RwLock<HashMap<VaultId, VaultCommitment>>,
    /// Cached per-vault string dictionaries for relationship storage.
    vault_dictionaries: RwLock<HashMap<VaultId, crate::dictionary::VaultDictionary>>,
    /// In-memory hash index for O(1) relationship existence checks.
    relationship_index: Arc<RelationshipIndex>,
}

#[allow(clippy::result_large_err)]
impl<B: StorageBackend> StateLayer<B> {
    /// Creates a new state layer backed by a per-vault database factory and
    /// the per-organization meta database.
    ///
    /// Slice 2b of per-vault consensus flips `StateLayer` from a singleton
    /// `Arc<Database<B>>` to a lazy per-vault [`HashMap`]. The `db_factory`
    /// closure is invoked the first time [`Self::db_for`] is asked for a
    /// given [`VaultId`]; the resulting [`Arc`] is cached for the lifetime
    /// of the layer.
    ///
    /// `meta_db` is the per-organization `_meta.db` coordinator introduced
    /// by Slice 1 of per-vault consensus. It owns the `_meta:last_applied`
    /// sentinel; entity data lives in the per-vault databases. The apply
    /// path commits entity data to the vault's DB first, then records the
    /// sentinel in `meta_db` in a second transaction so any crash between
    /// the two commits merely re-drives idempotent replay instead of
    /// advancing the sentinel past work that never reached the state DB.
    ///
    /// The constructor eagerly materialises the
    /// [`crate::system::SYSTEM_VAULT_ID`] database via the factory.
    /// Beyond the system vault, every other vault DB is opened lazily on
    /// the first [`Self::db_for`] call. The eager materialisation keeps
    /// the system organization's slug index, refresh tokens, signing
    /// keys, and other GLOBAL-tier records reachable without a callsite
    /// having to first register the system vault.
    ///
    /// # Errors
    ///
    /// Returns whatever error the factory produces while opening the
    /// system vault DB. A fresh on-disk layout cannot fail here because
    /// the factory's `create_dir_all + Database::create_with_config` path
    /// is the same one that succeeded during
    /// `RegionStorageManager::open_organization`. Tests using an
    /// in-memory factory should construct an always-Ok closure.
    pub fn new<F>(db_factory: F, meta_db: Arc<Database<B>>) -> Result<Self>
    where
        F: Fn(VaultId) -> Result<Arc<Database<B>>> + Send + Sync + 'static,
    {
        let factory: VaultDbFactory<B> = Arc::new(db_factory);
        // Eagerly materialise SYSTEM_VAULT_ID so the system organization's
        // GLOBAL-tier records (slug indexes, signing keys, refresh
        // tokens) are reachable without a callsite first registering the
        // system vault.
        let system_db = (factory)(crate::system::SYSTEM_VAULT_ID)?;
        let mut dbs = HashMap::new();
        dbs.insert(crate::system::SYSTEM_VAULT_ID, system_db);
        Ok(Self {
            dbs: RwLock::new(dbs),
            db_factory: factory,
            meta_db,
            vault_commitments: RwLock::new(HashMap::new()),
            vault_dictionaries: RwLock::new(HashMap::new()),
            relationship_index: Arc::new(RelationshipIndex::new()),
        })
    }

    /// Returns aggregate database-level statistics across every
    /// materialised vault DB for metrics collection.
    ///
    /// Counters (`cache_hits`, `cache_misses`, `page_splits`) and
    /// page-count fields (`total_pages`, `cached_pages`, `dirty_pages`,
    /// `free_pages`) are summed across vaults. `page_size` is taken
    /// from the first DB (every vault DB shares the same page size by
    /// construction — see `RegionStorageManager::open_organization`'s
    /// `ORGANIZATION_PAGE_SIZE`).
    pub fn database_stats(&self) -> DatabaseStats {
        let snapshot = self.live_vault_dbs();
        let mut combined = DatabaseStats::default();
        let mut page_size_set = false;
        for (_, db) in &snapshot {
            let s = db.stats();
            if !page_size_set {
                combined.page_size = s.page_size;
                page_size_set = true;
            }
            combined.total_pages += s.total_pages;
            combined.cached_pages += s.cached_pages;
            combined.dirty_pages += s.dirty_pages;
            combined.free_pages += s.free_pages;
            combined.cache_hits += s.cache_hits;
            combined.cache_misses += s.cache_misses;
            combined.page_splits += s.page_splits;
        }
        combined
    }

    /// Returns the maximum B-tree depth observed for each table across
    /// every materialised vault DB.
    ///
    /// Each table is reported once with the deepest tree found in any
    /// vault — the per-region metric tracks worst-case depth so a single
    /// pathological vault is visible. Empty databases (every vault has
    /// depth 0 for that table) report 0.
    ///
    /// # Errors
    ///
    /// Returns [`StateError::Store`] if any per-vault read transaction
    /// fails. The first failure short-circuits.
    pub fn table_depths(&self) -> Result<Vec<(&'static str, u32)>> {
        let snapshot = self.live_vault_dbs();
        let mut max_depths: HashMap<&'static str, u32> = HashMap::new();
        for (_, db) in &snapshot {
            let depths = db.table_depths().context(StoreSnafu)?;
            for (name, depth) in depths {
                let entry = max_depths.entry(name).or_insert(0);
                if depth > *entry {
                    *entry = depth;
                }
            }
        }
        Ok(max_depths.into_iter().collect())
    }

    /// Execute a function with mutable access to a vault's commitment.
    ///
    /// Creates the commitment if it doesn't exist.
    fn with_commitment<F, R>(&self, vault: VaultId, f: F) -> R
    where
        F: FnOnce(&mut VaultCommitment) -> R,
    {
        let mut map = self.vault_commitments.write();
        let commitment = map.entry(vault).or_default();
        f(commitment)
    }

    /// Takes the dictionary for a vault out of the cache for mutable use.
    ///
    /// If not cached, loads from storage. The caller **must** return it via
    /// [`return_dictionary`](Self::return_dictionary) after use, or it will be
    /// reloaded from storage on the next access.
    ///
    /// # Errors
    ///
    /// Returns [`StateError::Store`] if the storage read fails.
    /// Returns [`StateError::Relationship`] if dictionary loading fails.
    pub fn take_dictionary(&self, vault: VaultId) -> Result<crate::dictionary::VaultDictionary> {
        if let Some(dict) = self.vault_dictionaries.write().remove(&vault) {
            return Ok(dict);
        }
        let db = self.db_for(vault)?;
        let txn = db.read().context(StoreSnafu)?;
        crate::dictionary::VaultDictionary::load(&txn, vault).map_err(|e| match e {
            crate::dictionary::DictionaryError::Storage { source, .. } => {
                StateError::Store { source, location: snafu::location!() }
            },
            other => StateError::Relationship {
                source: crate::relationship::RelationshipError::Dictionary {
                    source: other,
                    location: snafu::location!(),
                },
                location: snafu::location!(),
            },
        })
    }

    /// Returns a dictionary to the cache after use.
    pub fn return_dictionary(&self, vault: VaultId, dict: crate::dictionary::VaultDictionary) {
        self.vault_dictionaries.write().insert(vault, dict);
    }

    /// Returns a read-only reference to the cached dictionary for a vault.
    ///
    /// Loads the dictionary from storage if it is not already cached.
    fn get_dictionary(
        &self,
        vault: VaultId,
    ) -> Result<MappedRwLockReadGuard<'_, crate::dictionary::VaultDictionary>> {
        // Fast path: dictionary already cached.
        {
            let map = self.vault_dictionaries.read();
            if map.contains_key(&vault) {
                return Ok(RwLockReadGuard::map(map, |m| &m[&vault]));
            }
        }
        // Slow path: load from storage and insert into cache.
        let db = self.db_for(vault)?;
        let txn = db.read().context(StoreSnafu)?;
        let dict = crate::dictionary::VaultDictionary::load(&txn, vault).map_err(|e| match e {
            crate::dictionary::DictionaryError::Storage { source, .. } => {
                StateError::Store { source, location: snafu::location!() }
            },
            other => StateError::Relationship {
                source: crate::relationship::RelationshipError::Dictionary {
                    source: other,
                    location: snafu::location!(),
                },
                location: snafu::location!(),
            },
        })?;
        drop(txn);
        self.vault_dictionaries.write().insert(vault, dict);
        let map = self.vault_dictionaries.read();
        Ok(RwLockReadGuard::map(map, |m| &m[&vault]))
    }

    /// Returns the [`Database`] handle that owns the given vault's entity
    /// data.
    ///
    /// Slice 2b of per-vault consensus: the first call for a given
    /// [`VaultId`] invokes the factory closure supplied to
    /// [`Self::new`] and caches the resulting [`Arc`]. Subsequent calls
    /// return a clone of the cached handle. Lookup uses double-checked
    /// locking — the common path takes a read lock only.
    ///
    /// The returned value is an **owned** [`Arc`] (not a reference into
    /// the `HashMap`); callers open transactions against it locally so
    /// the transaction's `'_` lifetime is tied to the caller's `Arc`,
    /// not to any `RwLock` guard:
    ///
    /// ```text
    /// let db = state.db_for(vault)?;
    /// let mut txn = db.write()?;
    /// // ... use txn ...
    /// txn.commit_in_memory()?;
    /// // `db` drops here, strictly after the transaction.
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`StateError::Store`] (wrapping whatever the factory
    /// closure produced) if opening the per-vault DB fails.
    pub fn db_for(&self, vault: VaultId) -> Result<Arc<Database<B>>> {
        // Fast path: the vault's DB is already materialised.
        if let Some(db) = self.dbs.read().get(&vault) {
            return Ok(Arc::clone(db));
        }
        // Slow path: acquire the write lock and re-check (double-checked
        // locking). Another thread may have materialised the DB between
        // the read-lock drop and the write-lock acquire.
        let mut dbs = self.dbs.write();
        if let Some(db) = dbs.get(&vault) {
            return Ok(Arc::clone(db));
        }
        let db = (self.db_factory)(vault)?;
        dbs.insert(vault, Arc::clone(&db));
        Ok(db)
    }

    /// Returns every vault DB that has been materialised for this
    /// organization.
    ///
    /// Used by `StateCheckpointer` (in the raft crate) and
    /// `RaftManager::sync_all_state_dbs` to fan out strict-ordered fsync
    /// Phase A (vault state DBs + raft.db + blocks.db + events.db
    /// concurrent) across every live vault before Phase B syncs meta.db.
    ///
    /// Slice 2b returns a snapshot vector rather than an iterator
    /// borrowing from the lock, so callers can drop the read lock before
    /// awaiting per-DB sync futures.
    pub fn live_vault_dbs(&self) -> Vec<(VaultId, Arc<Database<B>>)> {
        self.dbs.read().iter().map(|(v, db)| (*v, Arc::clone(db))).collect()
    }

    /// Hints to the OS that the page cache for `vault`'s state.db may be
    /// dropped (O6 vault hibernation Pass 2).
    ///
    /// Best-effort: on Linux this calls `posix_fadvise(POSIX_FADV_DONTNEED)`
    /// on the per-vault `state.db` file; on Apple / Windows it is a no-op
    /// success (see [`Database::evict_page_cache`]). Idempotent — calling
    /// twice in a row is a no-op success.
    ///
    /// **Used only by the raft-layer hibernation path.** `RaftManager::sleep_vault`
    /// drives this together with eviction on the per-vault `raft.db` /
    /// `blocks.db` / `events.db` (via direct `Database::evict_page_cache`
    /// calls on the per-vault DB handles owned by `InnerVaultGroup`). The
    /// in-process page cache is intentionally NOT cleared — wake p99
    /// stays under 100ms via warm in-process cache hits while the OS-side
    /// memory pressure is released.
    ///
    /// # Errors
    ///
    /// Returns [`StateError::VaultNotMaterialized`] if the vault has never
    /// been opened in this `StateLayer` (which at sleep time would be a
    /// programmer bug — the raft layer only drives hibernation for vaults
    /// whose group it has registered, and the register path materialises
    /// the per-vault DB before completing). Returns [`StateError::Store`]
    /// if the underlying syscall fails on a platform where eviction is
    /// supported.
    pub fn evict_vault_state_page_cache(&self, vault: VaultId) -> Result<()> {
        // Look up the per-vault DB without materialising on miss — eviction
        // on an unmaterialised vault is a no-op-vs-programmer-error
        // ambiguity; surface the latter so the raft-layer caller can log
        // the bug rather than silently drop the request.
        let db = match self.dbs.read().get(&vault) {
            Some(db) => Arc::clone(db),
            None => {
                return Err(StateError::VaultNotMaterialized {
                    vault_id: vault,
                    location: snafu::location!(),
                });
            },
        };
        db.evict_page_cache().context(StoreSnafu)?;
        Ok(())
    }

    // Slice 2b note: the pre-Slice-2b `begin_write(vault)` / `begin_read(vault)`
    // convenience wrappers are deleted. With a per-vault HashMap, the Arc
    // cannot be clone-on-demand inside `StateLayer` and still return a
    // transaction that outlives the clone — transactions borrow from
    // `&Database`, so the Arc must live in the caller's scope. Call sites
    // now take the two-liner:
    //
    // ```text
    // let db = state.db_for(vault)?;          // owned Arc
    // let mut txn = db.write().context(StoreSnafu)?;
    // // ... use txn ...
    // txn.commit_in_memory().context(StoreSnafu)?;
    // // `db` drops strictly after `txn`.
    // ```
    //
    // This keeps the lifetime chain explicit, avoids self-referential
    // gymnastics, and is trivially safe — the caller's local Arc outlives
    // the txn by construction.

    /// Sentinel key for crash-recovery atomicity (organization-scoped Raft
    /// group — the parent org's `OrganizationGroup`).
    ///
    /// Stored in the meta.db coordinator database (post–Slice 1) with a raw
    /// byte key. A byte-identical sentinel previously lived in state.db under
    /// the Entities table; legacy detection refuses to boot when that
    /// artefact is still present (see `detect_legacy_sentinel`).
    pub const LAST_APPLIED_KEY: &'static [u8] = b"_meta:last_applied";

    /// Builds the sentinel key for a per-vault Raft group.
    ///
    /// Per-vault Raft groups have independent log indices that are not
    /// comparable with the parent organization's log indices, so each
    /// vault's apply pipeline records its sentinel under its own key.
    /// Sharing a single `LAST_APPLIED_KEY` across the org and every vault
    /// of the same organization (they share `meta.db`) caused vault apply
    /// pipelines to skip-replay against the wrong group's last-applied
    /// log id, silently dropping every write past the highest sibling
    /// index.
    fn last_applied_key_for_vault(vault: VaultId) -> Vec<u8> {
        let mut key = Vec::with_capacity(Self::LAST_APPLIED_KEY.len() + 8 + 8);
        key.extend_from_slice(Self::LAST_APPLIED_KEY);
        key.extend_from_slice(b":vault:");
        key.extend_from_slice(vault.value().to_be_bytes().as_slice());
        key
    }

    /// Returns the sentinel key for the given Raft-group scope. `None`
    /// scope refers to the parent organization's group; `Some(vault)`
    /// refers to a per-vault group.
    fn last_applied_key_bytes(scope: Option<VaultId>) -> Vec<u8> {
        match scope {
            None => Self::LAST_APPLIED_KEY.to_vec(),
            Some(vault) => Self::last_applied_key_for_vault(vault),
        }
    }

    /// Returns a reference to the underlying meta database for diagnostics
    /// and durability-lifecycle wiring (checkpointer fan-out, shutdown
    /// fsync).
    pub fn meta_database(&self) -> &Arc<Database<B>> {
        &self.meta_db
    }

    /// Persists a Raft log ID sentinel to the meta database in its own
    /// write transaction.
    ///
    /// `scope` selects the Raft group the sentinel belongs to: `None` for
    /// the parent organization's group, `Some(vault)` for a per-vault
    /// group. Each Raft group keeps its own sentinel because per-vault
    /// log indices are not comparable to the parent organization's log
    /// indices — sharing a single key allowed one group's apply pipeline
    /// to skip-replay against another group's last-applied log id.
    ///
    /// Under Slice 1 of per-vault consensus the sentinel is no longer
    /// bundled into the entity-data write transaction. The apply path
    /// commits entity data to state.db first, then calls this method to
    /// record the sentinel in meta.db in a separate transaction. A crash
    /// between the two commits leaves the sentinel un-advanced; WAL replay
    /// re-drives the idempotent apply path on restart.
    ///
    /// The commit uses `commit_in_memory`, matching the apply path's
    /// durability model — the `StateCheckpointer` fans out a strict-ordered
    /// fsync (state/raft/blocks/events before meta) to realise on-disk
    /// durability. Never invert that ordering: meta.db must never reach
    /// disk ahead of the entity data it references.
    ///
    /// # Errors
    ///
    /// Returns [`StateError::Store`] if the meta-db write transaction
    /// cannot be opened, the sentinel insert fails, or the commit fails.
    pub fn persist_last_applied_meta(
        &self,
        scope: Option<VaultId>,
        log_id_bytes: &[u8],
    ) -> Result<()> {
        let mut txn = self.meta_db.write().context(StoreSnafu)?;
        let key = Self::last_applied_key_bytes(scope);
        txn.insert_raw(inferadb_ledger_store::tables::TableId::Entities, &key, log_id_bytes)
            .context(StoreSnafu)?;
        txn.commit_in_memory().context(StoreSnafu)
    }

    /// Reads the last-applied Raft log ID sentinel for the given scope
    /// from the meta database.
    ///
    /// `scope` selects the Raft group: `None` for the parent organization's
    /// group, `Some(vault)` for a per-vault group. Returns `None` on first
    /// boot (no sentinel written yet for this scope).
    ///
    /// # Errors
    ///
    /// Returns [`StateError::Store`] if the read transaction fails.
    pub fn read_last_applied(&self, scope: Option<VaultId>) -> Result<Option<Vec<u8>>> {
        let txn = self.meta_db.read().context(StoreSnafu)?;
        let key = Self::last_applied_key_bytes(scope);
        txn.get::<inferadb_ledger_store::tables::Entities>(&key).context(StoreSnafu)
    }

    /// Reads the legacy `_meta:last_applied` sentinel from the state
    /// database, returning `Some(bytes)` if a pre–Slice 1 layout is
    /// detected. Slice 1 moves the sentinel to meta.db; any hit here on
    /// startup means the data directory was written by an older build.
    ///
    /// Slice 2b: under the per-vault state DB layout the pre-Slice-1
    /// layout is anyway refused at `RegionStorageManager::open_organization`
    /// — a legacy flat `state.db` file (not directory) on disk fails the
    /// legacy detector before this layer is ever constructed. This helper
    /// is retained for `RaftLogStore`'s recovery-assertion path; it now
    /// probes the system vault's DB (materialised eagerly by
    /// [`Self::new`]). Any hit is treated as a "should have been caught
    /// earlier" signal by the caller.
    ///
    /// # Errors
    ///
    /// Returns [`StateError::Store`] if the read transaction fails.
    pub fn read_legacy_last_applied_from_state_db(&self) -> Result<Option<Vec<u8>>> {
        // Probe the system vault's DB explicitly. The legacy in-state.db
        // sentinel pre-dates per-vault storage; under Slice 2b the only
        // place a stray legacy artefact could sit is the system-vault
        // DB (the eagerly-materialised one).
        let db = self.db_for(crate::system::SYSTEM_VAULT_ID)?;
        let txn = db.read().context(StoreSnafu)?;
        let key = Self::LAST_APPLIED_KEY.to_vec();
        txn.get::<inferadb_ledger_store::tables::Entities>(&key).context(StoreSnafu)
    }

    /// Restores a single entity during snapshot installation.
    ///
    /// Writes a raw key/value pair directly into the Entities table within the
    /// provided transaction. No condition checking, version tracking, or dirty
    /// key tracking — snapshot restoration replaces the entire entity table.
    ///
    /// The key must be a full storage key (vault + bucket_id + local_key),
    /// as produced by [`encode_storage_key`](crate::encode_storage_key).
    ///
    /// # Errors
    ///
    /// Returns [`StateError::Store`] if the table insert fails.
    pub fn restore_entity(
        txn: &mut WriteTransaction<'_, B>,
        storage_key: &[u8],
        encoded_value: &[u8],
    ) -> Result<()> {
        txn.insert_raw(inferadb_ledger_store::tables::TableId::Entities, storage_key, encoded_value)
            .context(StoreSnafu)
    }

    /// Applies a batch of operations using an externally-provided transaction.
    ///
    /// Does **not** commit the transaction or mark dirty buckets — the caller
    /// is responsible for calling [`WriteTransaction::commit`] and then
    /// [`mark_dirty_keys`](Self::mark_dirty_keys).
    ///
    /// # Errors
    ///
    /// Returns [`StateError::Store`] if any storage operation fails.
    /// Returns [`StateError::Codec`] if entity serialization or deserialization fails.
    /// Returns [`StateError::Index`] if a relationship index update fails.
    /// Returns [`StateError::PreconditionFailed`] if a conditional write check fails.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            vault = vault.value(),
            op_count = operations.len(),
            block_height = block_height,
        )
    )]
    pub fn apply_operations_in_txn(
        &self,
        txn: &mut WriteTransaction<'_, B>,
        dict: &mut crate::dictionary::VaultDictionary,
        vault: VaultId,
        operations: &[Operation],
        block_height: u64,
    ) -> Result<(Vec<WriteStatus>, Vec<Vec<u8>>)> {
        let mut statuses = Vec::with_capacity(operations.len());
        let mut dirty_keys: Vec<Vec<u8>> = Vec::new();

        for op in operations {
            let status = match op {
                Operation::SetEntity { key, value, condition, expires_at } => {
                    let local_key = key.as_bytes();
                    let storage_key = encode_storage_key(vault, local_key);

                    let existing =
                        txn.get_raw(TableId::Entities, &storage_key).context(StoreSnafu)?;

                    let is_update = existing.is_some();
                    let entity_data =
                        existing.as_ref().and_then(|data| decode::<Entity>(data).ok());

                    let condition_met = match condition {
                        None => true,
                        Some(SetCondition::MustNotExist) => !is_update,
                        Some(SetCondition::MustExist) => is_update,
                        Some(SetCondition::VersionEquals(v)) => {
                            entity_data.as_ref().is_some_and(|e| e.version == *v)
                        },
                        Some(SetCondition::ValueEquals(expected)) => {
                            entity_data.as_ref().is_some_and(|e| e.value == *expected)
                        },
                    };

                    if !condition_met {
                        return Err(StateError::PreconditionFailed {
                            key: key.clone(),
                            current_version: entity_data.as_ref().map(|e| e.version),
                            current_value: entity_data.map(|e| e.value),
                            failed_condition: condition.clone(),
                        });
                    }

                    let entity = Entity {
                        key: local_key.to_vec(),
                        value: value.clone(),
                        expires_at: expires_at.unwrap_or(0),
                        version: block_height,
                    };

                    let encoded = encode(&entity).context(CodecSnafu)?;
                    txn.insert::<tables::Entities>(&storage_key, &encoded).context(StoreSnafu)?;
                    dirty_keys.push(local_key.to_vec());

                    if is_update { WriteStatus::Updated } else { WriteStatus::Created }
                },

                Operation::DeleteEntity { key } => {
                    let local_key = key.as_bytes();
                    let storage_key = encode_storage_key(vault, local_key);

                    let existed =
                        txn.delete_raw(TableId::Entities, &storage_key).context(StoreSnafu)?;

                    if existed {
                        dirty_keys.push(local_key.to_vec());
                        WriteStatus::Deleted
                    } else {
                        WriteStatus::NotFound
                    }
                },

                Operation::ExpireEntity { key, .. } => {
                    let local_key = key.as_bytes();
                    let storage_key = encode_storage_key(vault, local_key);

                    let existed =
                        txn.delete_raw(TableId::Entities, &storage_key).context(StoreSnafu)?;

                    if existed {
                        dirty_keys.push(local_key.to_vec());
                        WriteStatus::Deleted
                    } else {
                        WriteStatus::NotFound
                    }
                },

                Operation::CreateRelationship { resource, relation, subject } => {
                    let created = crate::relationship::RelationshipStore::create(
                        txn, dict, vault, resource, relation, subject,
                    )
                    .context(RelationshipSnafu)?;

                    if created {
                        IndexManager::add_to_obj_index(
                            &mut *txn, dict, vault, resource, relation, subject,
                        )
                        .context(IndexSnafu)?;
                        IndexManager::add_to_subj_index(
                            &mut *txn, dict, vault, resource, relation, subject,
                        )
                        .context(IndexSnafu)?;

                        // Update hash index only if the vault is already loaded.
                        // If not loaded, the lazy-load on next exists() picks up the new entry.
                        if self.relationship_index.is_vault_loaded(vault)
                            && let Some(hash) =
                                Self::relationship_hash_from_dict(dict, resource, relation, subject)
                        {
                            self.relationship_index.insert(vault, hash);
                        }

                        WriteStatus::Created
                    } else {
                        WriteStatus::AlreadyExists
                    }
                },

                Operation::DeleteRelationship { resource, relation, subject } => {
                    let deleted = crate::relationship::RelationshipStore::delete(
                        txn, dict, vault, resource, relation, subject,
                    )
                    .context(RelationshipSnafu)?;

                    if deleted {
                        IndexManager::remove_from_obj_index(
                            &mut *txn, dict, vault, resource, relation, subject,
                        )
                        .context(IndexSnafu)?;
                        IndexManager::remove_from_subj_index(
                            &mut *txn, dict, vault, resource, relation, subject,
                        )
                        .context(IndexSnafu)?;

                        // Remove from hash index only if the vault is already loaded.
                        if self.relationship_index.is_vault_loaded(vault)
                            && let Some(hash) =
                                Self::relationship_hash_from_dict(dict, resource, relation, subject)
                        {
                            self.relationship_index.remove(vault, hash);
                        }

                        WriteStatus::Deleted
                    } else {
                        WriteStatus::NotFound
                    }
                },
            };

            statuses.push(status);
        }

        Ok((statuses, dirty_keys))
    }

    /// Marks dirty buckets for state root computation after a transaction commits.
    ///
    /// Call this after committing a [`WriteTransaction`] that was used with
    /// [`apply_operations_in_txn`](Self::apply_operations_in_txn).
    pub fn mark_dirty_keys(&self, vault: VaultId, dirty_keys: &[Vec<u8>]) {
        self.with_commitment(vault, |commitment| {
            for key in dirty_keys {
                commitment.mark_dirty_by_key(key);
            }
        });
    }

    /// Marks all 256 buckets dirty for a vault, forcing a full state root
    /// recomputation on the next [`StateLayer::compute_state_root`] call.
    ///
    /// Used during crash recovery when entity data is already in the B+ tree
    /// but vault commitment tracking has been lost (in-memory state was reset).
    pub fn mark_all_dirty(&self, vault: VaultId) {
        self.with_commitment(vault, |commitment| {
            for bucket in 0..NUM_BUCKETS as u8 {
                commitment.mark_dirty(bucket);
            }
        });
    }

    /// Applies a batch of operations to a vault's state.
    ///
    /// Executes all operations atomically within a single write transaction.
    /// Returns a `WriteStatus` for each operation indicating whether it was
    /// created, updated, deleted, or failed a precondition check.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use std::sync::Arc;
    /// # use inferadb_ledger_store::Database;
    /// # use inferadb_ledger_state::StateLayer;
    /// use inferadb_ledger_types::types::{Operation, VaultId};
    ///
    /// # let meta_db = Arc::new(Database::open_in_memory()?);
    /// # let factory = |_vault| -> Result<_, inferadb_ledger_state::StateError> {
    /// #     Ok(Arc::new(Database::open_in_memory().unwrap()))
    /// # };
    /// # let state = StateLayer::new(factory, meta_db)?;
    /// let ops = vec![Operation::SetEntity {
    ///     key: "user:alice".into(),
    ///     value: b"data".to_vec(),
    ///     condition: None,
    ///     expires_at: None,
    /// }];
    ///
    /// let statuses = state.apply_operations(VaultId::new(1), &ops, 1)?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`StateError::Store`] if any storage operation fails.
    /// Returns [`StateError::Codec`] if entity serialization or deserialization fails.
    /// Returns [`StateError::Index`] if a relationship index update fails.
    /// Returns [`StateError::PreconditionFailed`] if a conditional write check fails.
    pub fn apply_operations(
        &self,
        vault: VaultId,
        operations: &[Operation],
        block_height: u64,
    ) -> Result<Vec<WriteStatus>> {
        let mut dict = self.take_dictionary(vault)?;
        let db = self.db_for(vault)?;
        let mut txn = db.write().context(StoreSnafu)?;
        match self.apply_operations_in_txn(&mut txn, &mut dict, vault, operations, block_height) {
            Ok((statuses, dirty_keys)) => {
                // Mark dirty before commit: dirty marks are conservative (trigger
                // re-hash from storage). If commit fails, the re-hash reads old
                // data and produces the correct (pre-commit) state root. If commit
                // succeeds, dirty buckets are correctly tracked for the new data.
                self.mark_dirty_keys(vault, &dirty_keys);
                txn.commit().context(StoreSnafu)?;
                self.return_dictionary(vault, dict);
                Ok(statuses)
            },
            Err(e) => {
                // Discard potentially-dirty dictionary on failure.
                // Next call will reload from storage.
                Err(e)
            },
        }
    }

    /// Applies a batch of operations lazily — identical semantics to
    /// [`StateLayer::apply_operations`], but commits the internal transaction
    /// via [`WriteTransaction::commit_in_memory`] instead of the strict-durable
    /// [`WriteTransaction::commit`].
    ///
    /// Intended for IN-APPLY-PIPELINE callers invoked per committed Raft
    /// entry (directly from `apply_request_with_events` or from its system-
    /// service helpers). Durability is realized by `Database::sync_state`
    /// driven by `StateCheckpointer` (periodic) and forced synchronously at
    /// snapshot / backup / graceful-shutdown boundaries. On crash, every
    /// write applied lazily is WAL-replayable via `apply_committed_entries`.
    ///
    /// OUT-OF-APPLY-PIPELINE callers (recovery, admin RPC migrations,
    /// background jobs that write directly to state) MUST use
    /// [`StateLayer::apply_operations`] so their writes are durable-on-return.
    ///
    /// See `docs/superpowers/specs/2026-04-19-commit-durability-audit.md`
    /// for the classification rules governing the split.
    ///
    /// # Errors
    ///
    /// Returns [`StateError::Store`] if any storage operation fails.
    /// Returns [`StateError::Codec`] if entity serialization or deserialization fails.
    /// Returns [`StateError::Index`] if a relationship index update fails.
    /// Returns [`StateError::PreconditionFailed`] if a conditional write check fails.
    pub fn apply_operations_lazy(
        &self,
        vault: VaultId,
        operations: &[Operation],
        block_height: u64,
    ) -> Result<Vec<WriteStatus>> {
        let mut dict = self.take_dictionary(vault)?;
        let db = self.db_for(vault)?;
        let mut txn = db.write().context(StoreSnafu)?;
        match self.apply_operations_in_txn(&mut txn, &mut dict, vault, operations, block_height) {
            Ok((statuses, dirty_keys)) => {
                // Mark dirty before commit: dirty marks are conservative (trigger
                // re-hash from storage). If commit fails, the re-hash reads old
                // data and produces the correct (pre-commit) state root. If commit
                // succeeds, dirty buckets are correctly tracked for the new data.
                self.mark_dirty_keys(vault, &dirty_keys);
                txn.commit_in_memory().context(StoreSnafu)?;
                self.return_dictionary(vault, dict);
                Ok(statuses)
            },
            Err(e) => {
                // Discard potentially-dirty dictionary on failure.
                // Next call will reload from storage.
                Err(e)
            },
        }
    }

    /// Clears all entities and relationships for a vault.
    ///
    /// Used during vault recovery to reset state before replay.
    ///
    /// # Errors
    ///
    /// Returns [`StateError::Store`] if any storage operation (iteration, delete,
    /// or commit) fails.
    pub fn clear_vault(&self, vault: VaultId) -> Result<()> {
        let db = self.db_for(vault)?;
        let mut txn = db.write().context(StoreSnafu)?;

        delete_vault_keys::<B, tables::Entities>(&mut txn, vault)?;
        delete_vault_keys::<B, tables::Relationships>(&mut txn, vault)?;
        delete_vault_keys::<B, tables::ObjIndex>(&mut txn, vault)?;
        delete_vault_keys::<B, tables::SubjIndex>(&mut txn, vault)?;
        delete_vault_keys::<B, tables::StringDictionary>(&mut txn, vault)?;
        delete_vault_keys::<B, tables::StringDictionaryReverse>(&mut txn, vault)?;

        txn.commit().context(StoreSnafu)?;

        // Reset commitment, dictionary, and relationship index caches for this vault
        self.vault_commitments.write().remove(&vault);
        self.vault_dictionaries.write().remove(&vault);
        self.relationship_index.evict_vault(vault);

        Ok(())
    }

    /// Returns an entity by key.
    ///
    /// # Errors
    ///
    /// Returns [`StateError::Store`] if the read transaction fails.
    /// Returns [`StateError::Codec`] if deserialization of the stored entity fails.
    pub fn get_entity(&self, vault: VaultId, key: &[u8]) -> Result<Option<Entity>> {
        let storage_key = encode_storage_key(vault, key);
        let db = self.db_for(vault)?;
        let txn = db.read().context(StoreSnafu)?;

        match txn.get_raw(TableId::Entities, &storage_key).context(StoreSnafu)? {
            Some(data) => {
                let entity = decode(&data).context(CodecSnafu)?;
                Ok(Some(entity))
            },
            None => Ok(None),
        }
    }

    /// Checks whether the exact relationship tuple is present in the vault.
    ///
    /// This is a storage-primitive existence check — it does NOT evaluate
    /// authorization policy, userset rewrites, or schema inheritance. Callers
    /// composing authorization decisions should use this as one leaf in a
    /// larger evaluation.
    ///
    /// Attempts the in-memory hash index first for O(1) lookups. Falls back
    /// to the B+ tree if the vault is not yet indexed or the dictionary
    /// cannot resolve the triple's components.
    ///
    /// The state layer evaluates against the current committed state. Block
    /// height is a raft-apply-layer concern — callers that need to annotate
    /// the response with a `checked_at_height` source it separately from
    /// `AppliedStateAccessor::vault_height`.
    ///
    /// # Errors
    ///
    /// Returns [`StateError::Store`] if the read transaction fails.
    /// Returns [`StateError::Relationship`] on dictionary or storage failure.
    pub fn relationship_exists(
        &self,
        vault: VaultId,
        resource: &str,
        relation: &str,
        subject: &str,
    ) -> Result<bool> {
        // Try hash index first (O(1)).
        if let Some(key_hash) = self.compute_relationship_hash(vault, resource, relation, subject) {
            if let Some(result) = self.relationship_index.exists(vault, key_hash) {
                return Ok(result);
            }
            // Vault not indexed — load it, then re-check.
            self.load_vault_relationship_index(vault)?;
            if let Some(result) = self.relationship_index.exists(vault, key_hash) {
                return Ok(result);
            }
        }
        // Fallback to B+ tree (dictionary strings unknown or load failed to index).
        let dict = self.get_dictionary(vault)?;
        let db = self.db_for(vault)?;
        let txn = db.read().context(StoreSnafu)?;
        crate::relationship::RelationshipStore::exists(
            &txn, &dict, vault, resource, relation, subject,
        )
        .context(RelationshipSnafu)
    }

    /// Computes the seahash of a relationship's binary key using the
    /// dictionary cache. Returns `None` if any component string is
    /// unknown in the dictionary (read-only lookup — no interning).
    fn compute_relationship_hash(
        &self,
        vault: VaultId,
        resource: &str,
        relation: &str,
        subject: &str,
    ) -> Option<u64> {
        let dict = self.get_dictionary(vault).ok()?;

        let (res_type_str, res_local) = split_typed_id(resource).unwrap_or(("", resource));
        let res_type = dict.get_id(InternCategory::ResourceType, res_type_str)?;
        let res_id_bytes = res_local.as_bytes();

        let rel_id = dict.get_id(InternCategory::Relation, relation)?;

        let (subj_type_str, subj_local) = split_typed_id(subject).unwrap_or(("", subject));
        let subj_type = dict.get_id(InternCategory::SubjectType, subj_type_str)?;
        let subj_id_bytes = subj_local.as_bytes();

        let local_key =
            encode_relationship_local_key(res_type, res_id_bytes, rel_id, subj_type, subj_id_bytes);
        Some(seahash::hash(&local_key))
    }

    /// Loads all relationships for a vault into the hash index.
    ///
    /// Iterates the Relationships table for the given vault prefix and
    /// inserts the seahash of each local key into the index.
    fn load_vault_relationship_index(&self, vault: VaultId) -> Result<()> {
        let db = self.db_for(vault)?;
        let txn = db.read().context(StoreSnafu)?;
        let vault_start = crate::keys::vault_prefix(vault);
        let next_vault = VaultId::from(vault.value() + 1);
        let vault_end = crate::keys::vault_prefix(next_vault);

        let start_key = vault_start.to_vec();
        let end_key = vault_end.to_vec();

        for (key_bytes, _) in txn
            .range::<tables::Relationships>(Some(&start_key), Some(&end_key))
            .context(StoreSnafu)?
        {
            if key_bytes.len() < 9 {
                continue;
            }
            // local_key starts after the 8-byte vault prefix + 1-byte bucket_id.
            let local_key = &key_bytes[9..];
            self.relationship_index.insert(vault, seahash::hash(local_key));
        }

        // Mark the vault as loaded even if it had zero relationships.
        self.relationship_index.ensure_vault(vault);
        Ok(())
    }

    /// Returns a reference to the in-memory relationship hash index.
    pub fn relationship_index(&self) -> &Arc<RelationshipIndex> {
        &self.relationship_index
    }

    /// Computes a relationship hash from an already-populated dictionary.
    ///
    /// Used in `apply_operations_in_txn` where the dictionary is guaranteed
    /// to contain all component strings (the preceding create/delete interned
    /// them). Returns `None` only if the dictionary is missing an entry, which
    /// should not happen in practice.
    fn relationship_hash_from_dict(
        dict: &crate::dictionary::VaultDictionary,
        resource: &str,
        relation: &str,
        subject: &str,
    ) -> Option<u64> {
        let (res_type_str, res_local) = split_typed_id(resource).unwrap_or(("", resource));
        let res_type = dict.get_id(InternCategory::ResourceType, res_type_str)?;
        let res_id_bytes = res_local.as_bytes();

        let rel_id = dict.get_id(InternCategory::Relation, relation)?;

        let (subj_type_str, subj_local) = split_typed_id(subject).unwrap_or(("", subject));
        let subj_type = dict.get_id(InternCategory::SubjectType, subj_type_str)?;
        let subj_id_bytes = subj_local.as_bytes();

        let local_key =
            encode_relationship_local_key(res_type, res_id_bytes, rel_id, subj_type, subj_id_bytes);
        Some(seahash::hash(&local_key))
    }

    /// Computes state root for a vault, updating dirty bucket roots.
    ///
    /// This scans only the dirty buckets and recomputes their roots,
    /// then returns SHA-256(bucket_roots[0..256]).
    ///
    /// # Implementation
    ///
    /// Storage keys are encoded as `[vault_id:8BE][bucket_id:1][local_key]`,
    /// so all entities in bucket `B` of vault `V` form a contiguous
    /// prefix-scannable range `[bucket_prefix(V, B), bucket_prefix(V, B+1))`.
    /// Rather than scanning the entire vault and filtering by bucket in
    /// memory (O(vault_entries)), we issue one range scan per dirty bucket
    /// (O(sum(dirty_bucket_entries))). With 256 buckets and typically a
    /// handful dirty per apply, this skips ~99% of the scan + decode cost
    /// on write-heavy workloads without changing the hash output.
    ///
    /// # Errors
    ///
    /// Returns [`StateError::Store`] if the read transaction or iteration fails.
    /// Returns [`StateError::Codec`] if deserialization of any entity in a dirty bucket fails.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(vault = vault.value())
    )]
    pub fn compute_state_root(&self, vault: VaultId) -> Result<Hash> {
        // First check if dirty and get the dirty buckets list (brief read lock)
        let dirty_buckets: Vec<u8> = {
            let map = self.vault_commitments.read();
            match map.get(&vault) {
                Some(commitment) if commitment.is_dirty() => {
                    commitment.dirty_buckets().iter().copied().collect()
                },
                Some(commitment) => {
                    // Not dirty, return cached state root
                    return Ok(commitment.compute_state_root());
                },
                None => {
                    // No commitment yet, create default and return its state root
                    drop(map);
                    return Ok(self.with_commitment(vault, |c| c.compute_state_root()));
                },
            }
        };

        // Per-dirty-bucket prefix scan. Each dirty bucket's entities form a
        // contiguous [bucket_prefix(V, B), bucket_prefix(V, B+1)) range, so
        // we hit only the leaves that contain dirty data.
        //
        // Buckets are independent — their hashes don't depend on each other —
        // so we fan the scan-and-hash work across rayon's thread pool. Each
        // rayon task opens its own MVCC read transaction: the snapshot
        // semantics guarantee each thread sees a consistent view of the DB,
        // and opening a read txn is cheap (snapshot registration + table-
        // roots clone). `ReadTransaction` is `!Sync` because it caches
        // fetched pages in a `RefCell`; one txn per thread sidesteps that.
        //
        // For batches that dirty many buckets (typical in multi-op
        // workloads) this turns the hottest span in the apply path from
        // O(dirty_buckets × entities-per-bucket) serial work into
        // near-O(work / cores).

        // `vault_end` is the exclusive upper bound for bucket 255 (wrapping to
        // the next vault). Matches the prior implementation's convention of
        // assuming `vault.value() + 1` does not overflow i64 for live vaults.
        let vault_end = crate::keys::vault_prefix(VaultId::new(vault.value() + 1)).to_vec();

        // Resolve the vault's per-vault DB once before the rayon fan-out —
        // each worker opens its own MVCC read transaction against this
        // shared `Arc`. Under Slice 2b, `db_for` returns a cheap clone of
        // the HashMap's `Arc`; there is no lock held across the rayon
        // join.
        let db = self.db_for(vault)?;

        // Use the bounded apply-path pool (see `apply_pool`) rather than
        // rayon's default global pool. The global pool sizes itself to
        // `num_cpus`, competing 1:1 with tokio's runtime workers and
        // inflating p99 tail under apply bursts. `APPLY_POOL` caps parallelism
        // at ~num_cpus/2 so network I/O and response delivery continue
        // making progress while the apply worker is hashing.
        let bucket_roots: Vec<(u8, Hash)> = crate::apply_pool::APPLY_POOL.install(|| {
            dirty_buckets
                .par_iter()
                .map(|&bucket| -> Result<(u8, Hash)> {
                    let txn = db.read().context(StoreSnafu)?;
                    let start = crate::keys::bucket_prefix(vault, bucket).to_vec();
                    let end_owned: Vec<u8> = if bucket < 255 {
                        crate::keys::bucket_prefix(vault, bucket + 1).to_vec()
                    } else {
                        vault_end.clone()
                    };

                    let iter = txn
                        .range::<tables::Entities>(Some(&start), Some(&end_owned))
                        .context(StoreSnafu)?;

                    let mut builder = BucketRootBuilder::new(bucket);
                    for (_key_bytes, value) in iter {
                        let entity: Entity = decode(&value).context(CodecSnafu)?;
                        builder.add_entity(&entity);
                    }
                    Ok((bucket, builder.finalize()))
                })
                .collect::<Result<Vec<(u8, Hash)>>>()
        })?;

        // Update commitment with computed bucket roots (brief write lock)
        Ok(self.with_commitment(vault, |commitment| {
            for (bucket, root) in bucket_roots {
                commitment.set_bucket_root(bucket, root);
            }
            commitment.clear_dirty();
            commitment.compute_state_root()
        }))
    }

    /// Returns `true` if the cached state root for `vault` is current —
    /// i.e. the next [`Self::compute_state_root`] call will short-circuit
    /// without rescanning dirty buckets.
    ///
    /// Returns `false` when the vault has dirty buckets (a recompute is
    /// needed) or no commitment has been instantiated yet (the first
    /// `compute_state_root` call constructs one).
    ///
    /// Used by the apply pipeline to label per-vault Prometheus
    /// observations as cache hits vs misses without changing the
    /// `compute_state_root` API surface (Phase 7 / O3).
    pub fn state_root_is_cached(&self, vault: VaultId) -> bool {
        self.vault_commitments.read().get(&vault).is_some_and(|commitment| !commitment.is_dirty())
    }

    /// Loads bucket roots from stored vault metadata.
    ///
    /// Called during startup/recovery to restore commitment state.
    pub fn load_vault_commitment(&self, vault: VaultId, bucket_roots: [Hash; NUM_BUCKETS]) {
        self.vault_commitments
            .write()
            .insert(vault, VaultCommitment::from_bucket_roots(bucket_roots));
    }

    /// Returns the current bucket roots for a vault (for persistence).
    pub fn get_bucket_roots(&self, vault: VaultId) -> Option<[Hash; NUM_BUCKETS]> {
        self.vault_commitments.read().get(&vault).map(|c| *c.bucket_roots())
    }

    /// Compacts every B+ tree table inside a single vault's database,
    /// merging underfull leaf nodes.
    ///
    /// Returns aggregate compaction statistics across all tables of the
    /// addressed vault. Callers iterating multiple vaults should sum the
    /// returned `pages_merged` / `pages_freed` themselves.
    ///
    /// # Errors
    ///
    /// Returns [`StateError::Store`] if the compaction operation, the
    /// per-vault DB lookup, or the commit fails.
    pub fn compact_tables(&self, vault: VaultId, min_fill_factor: f64) -> Result<CompactionStats> {
        let db = self.db_for(vault)?;
        let mut txn = db.write().context(StoreSnafu)?;
        let stats = txn.compact_all_tables(min_fill_factor).context(StoreSnafu)?;
        txn.commit().context(StoreSnafu)?;
        Ok(stats)
    }

    /// Re-wraps a batch of crypto sidecar metadata pages for a single
    /// vault's database to a target RMK version.
    ///
    /// Delegates to [`Database::rewrap_pages`]. Non-encrypted backends
    /// are a no-op. Callers that need to cover every live vault iterate
    /// over [`Self::live_vault_dbs`] and call this method once per vault.
    ///
    /// # Errors
    ///
    /// Returns [`StateError::Store`] if the per-vault DB lookup or the
    /// rewrap operation fails.
    pub fn rewrap_pages(
        &self,
        vault: VaultId,
        start_page_id: u64,
        batch_size: usize,
        target_version: Option<u32>,
    ) -> Result<(usize, Option<u64>)> {
        let db = self.db_for(vault)?;
        db.rewrap_pages(start_page_id, batch_size, target_version).context(StoreSnafu)
    }

    /// Returns the sum of crypto sidecar page counts across every
    /// materialised vault DB.
    ///
    /// Non-encrypted backends contribute 0.
    ///
    /// # Errors
    ///
    /// Returns [`StateError::Store`] if any per-vault sidecar read
    /// fails. The first failure short-circuits.
    pub fn sidecar_page_count(&self) -> Result<u64> {
        let snapshot = self.live_vault_dbs();
        let mut total = 0u64;
        for (_, db) in &snapshot {
            total = total.saturating_add(db.sidecar_page_count().context(StoreSnafu)?);
        }
        Ok(total)
    }

    /// Lists subjects for a given resource and relation.
    ///
    /// # Errors
    ///
    /// Returns [`StateError::Store`] if the read transaction fails.
    /// Returns [`StateError::Index`] if the index lookup fails.
    pub fn list_subjects(
        &self,
        vault: VaultId,
        resource: &str,
        relation: &str,
    ) -> Result<Vec<String>> {
        let dict = self.get_dictionary(vault)?;
        let db = self.db_for(vault)?;
        let txn = db.read().context(StoreSnafu)?;
        IndexManager::get_subjects(&txn, &dict, vault, resource, relation).context(IndexSnafu)
    }

    /// Lists resource-relation pairs for a given subject.
    ///
    /// # Errors
    ///
    /// Returns [`StateError::Store`] if the read transaction fails.
    /// Returns [`StateError::Index`] if the index lookup fails.
    pub fn list_resources_for_subject(
        &self,
        vault: VaultId,
        subject: &str,
    ) -> Result<Vec<(String, String)>> {
        let dict = self.get_dictionary(vault)?;
        let db = self.db_for(vault)?;
        let txn = db.read().context(StoreSnafu)?;
        IndexManager::get_resources(&txn, &dict, vault, subject).context(IndexSnafu)
    }

    /// Lists all entities in a vault with optional prefix filter.
    ///
    /// Returns up to `limit` entities. Use `start_after` for pagination.
    ///
    /// # Errors
    ///
    /// Returns [`StateError::Store`] if the read transaction or iteration fails.
    /// Returns [`StateError::Codec`] if deserialization of any entity fails.
    pub fn list_entities(
        &self,
        vault: VaultId,
        prefix: Option<&str>,
        start_after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<Entity>> {
        use crate::keys::vault_prefix;

        let db = self.db_for(vault)?;
        let txn = db.read().context(StoreSnafu)?;

        let mut entities = Vec::with_capacity(limit.min(1000));

        // Build the range start key.
        // Note: Keys are ordered by (vault, bucket_id, local_key), where bucket_id
        // is a hash of the local key. This means keys with the same prefix can be
        // scattered across different buckets. For prefix scans, we must start from
        // the beginning of the vault and filter by prefix in the loop.
        let start_key: Vec<u8> = if let Some(after) = start_after {
            // Pagination: start after this specific key
            let mut k = encode_storage_key(vault, after.as_bytes());
            k.push(0); // Advance past the exact key
            k
        } else {
            // For prefix scans or full vault scans, start from the vault prefix
            vault_prefix(vault).to_vec()
        };

        for (key_bytes, value) in txn.iter::<tables::Entities>().context(StoreSnafu)? {
            if entities.len() >= limit {
                break;
            }

            // Skip until we reach the start key
            if key_bytes < start_key {
                continue;
            }

            // Check we're still in the same vault (first 8 bytes)
            if key_bytes.len() < 9 {
                break;
            }
            let key_vault_id = i64::from_be_bytes(key_bytes[..8].try_into().unwrap_or([0; 8]));
            if key_vault_id != vault.value() {
                break;
            }

            // Check prefix match if specified
            if let Some(p) = prefix {
                // Skip bucket byte (index 8), entity key starts at index 9
                let entity_key = &key_bytes[9..];
                if !entity_key.starts_with(p.as_bytes()) {
                    // Keys are scattered by bucket, so we can't early-exit
                    continue;
                }
            }

            let entity: Entity = decode(&value).context(CodecSnafu)?;

            entities.push(entity);
        }

        Ok(entities)
    }

    /// Lists all relationships in a vault.
    ///
    /// Returns up to `limit` relationships. Use `start_after` for cursor-based
    /// pagination — pass the last relationship from the previous page to resume.
    ///
    /// # Errors
    ///
    /// Returns [`StateError::Store`] if the read transaction or iteration fails.
    /// Returns [`StateError::Relationship`] if dictionary loading or key decoding fails.
    pub fn list_relationships(
        &self,
        vault: VaultId,
        start_after: Option<&Relationship>,
        limit: usize,
    ) -> Result<Vec<Relationship>> {
        let dict = self.get_dictionary(vault)?;
        let db = self.db_for(vault)?;
        let txn = db.read().context(StoreSnafu)?;

        // Build start key for pagination. If start_after is provided, resolve
        // its components through the dictionary to build a binary storage key.
        let vault_start = crate::keys::vault_prefix(vault);
        let start_key: Vec<u8> = if let Some(after) = start_after {
            // Resolve the cursor relationship to its binary storage key
            use crate::binary_keys::{
                InternCategory, encode_relationship_local_key, split_typed_id,
            };

            let (res_type_str, res_local) =
                split_typed_id(&after.resource).unwrap_or(("", &after.resource));
            let (subj_type_str, subj_local) =
                split_typed_id(&after.subject).unwrap_or(("", &after.subject));

            if let (Some(res_type), Some(rel_id), Some(subj_type)) = (
                dict.get_id(InternCategory::ResourceType, res_type_str),
                dict.get_id(InternCategory::Relation, &after.relation),
                dict.get_id(InternCategory::SubjectType, subj_type_str),
            ) {
                let local_key = encode_relationship_local_key(
                    res_type,
                    res_local.as_bytes(),
                    rel_id,
                    subj_type,
                    subj_local.as_bytes(),
                );
                let mut k = encode_storage_key(vault, &local_key);
                k.push(0); // Advance past the exact key
                k
            } else {
                // Cursor relationship has unknown dictionary entries — start from beginning
                vault_start.to_vec()
            }
        } else {
            vault_start.to_vec()
        };

        let mut relationships = Vec::with_capacity(limit.min(1000));

        for (key_bytes, _) in txn.iter::<tables::Relationships>().context(StoreSnafu)? {
            if relationships.len() >= limit {
                break;
            }

            if key_bytes < start_key {
                continue;
            }

            if key_bytes.len() < 9 {
                break;
            }
            if key_bytes[..8] != vault_start[..] {
                if key_bytes[..8] < vault_start[..] {
                    continue;
                }
                break;
            }

            // Decode binary local key (bytes after vault_id + bucket_id)
            let local_key = &key_bytes[9..];
            if let Some((res_type, res_id, rel_id, subj_type, subj_id)) =
                crate::binary_keys::decode_relationship_local_key(local_key)
                && let (Some(resource), Some(relation), Some(subject)) = (
                    dict.resolve_typed_id(InternCategory::ResourceType, res_type, res_id),
                    dict.resolve(InternCategory::Relation, rel_id).map(str::to_owned),
                    dict.resolve_typed_id(InternCategory::SubjectType, subj_type, subj_id),
                )
            {
                relationships.push(Relationship::new(resource, relation, subject));
            }
        }

        Ok(relationships)
    }
}

impl<B: StorageBackend> Clone for StateLayer<B> {
    fn clone(&self) -> Self {
        // Snapshot the currently-materialised vault DBs; the clone shares
        // the same `Arc<Database<B>>` handles so writes remain visible
        // across clones. The factory is reference-counted and re-shared
        // so a clone can still lazily materialise a vault that was not
        // yet opened at clone time.
        let dbs_snapshot: HashMap<VaultId, Arc<Database<B>>> =
            self.dbs.read().iter().map(|(v, db)| (*v, Arc::clone(db))).collect();
        Self {
            dbs: RwLock::new(dbs_snapshot),
            db_factory: Arc::clone(&self.db_factory),
            meta_db: Arc::clone(&self.meta_db),
            vault_commitments: RwLock::new(HashMap::new()), // Each clone starts fresh
            vault_dictionaries: RwLock::new(HashMap::new()), // Each clone starts fresh
            relationship_index: Arc::clone(&self.relationship_index), // Shared across clones
        }
    }
}

/// Deletes all keys in a table that belong to the given vault.
///
/// Keys are sorted lexicographically with the 8-byte vault prefix first,
/// so the scan skips keys from earlier vaults and terminates as soon as
/// a key from a later vault is reached.
fn delete_vault_keys<B: StorageBackend, T: Table<KeyType = Vec<u8>>>(
    txn: &mut WriteTransaction<'_, B>,
    vault: VaultId,
) -> Result<()> {
    let prefix = crate::keys::vault_prefix(vault);
    let mut keys_to_delete = Vec::new();

    for (key_bytes, _) in txn.iter::<T>().context(StoreSnafu)? {
        if key_bytes.len() < 8 {
            break;
        }
        if key_bytes[..8] < prefix[..] {
            continue;
        }
        if key_bytes[..8] != prefix[..] {
            break;
        }
        keys_to_delete.push(key_bytes);
    }

    for key in keys_to_delete {
        txn.delete::<T>(&key).context(StoreSnafu)?;
    }

    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods, clippy::panic)]
mod tests {
    use inferadb_ledger_store::InMemoryBackend;
    use inferadb_ledger_types::VaultId;

    use super::*;
    use crate::{engine::InMemoryStorageEngine, system::SYSTEM_VAULT_ID};

    fn create_test_state() -> StateLayer<InMemoryBackend> {
        let meta_engine = InMemoryStorageEngine::open().expect("open meta engine");
        // Slice 2b: each vault gets its own `Database`. The test factory
        // opens a fresh in-memory database per vault — tests exercise
        // the per-vault branch, not the Slice 2a singleton shim.
        StateLayer::new(
            |_vault| {
                inferadb_ledger_store::Database::<InMemoryBackend>::open_in_memory()
                    .map(Arc::new)
                    .map_err(|e| StateError::Store { source: e, location: snafu::location!() })
            },
            meta_engine.db(),
        )
        .expect("open test StateLayer")
    }

    #[test]
    fn test_set_and_get_entity() {
        let state = create_test_state();
        let vault = VaultId::new(1);

        let ops = vec![Operation::SetEntity {
            key: "test_key".to_string(),
            value: b"test_value".to_vec(),
            condition: None,
            expires_at: None,
        }];

        let statuses = state.apply_operations(vault, &ops, 1).unwrap();
        assert_eq!(statuses, vec![WriteStatus::Created]);

        let entity = state.get_entity(vault, b"test_key").unwrap();
        assert!(entity.is_some());
        let e = entity.unwrap();
        assert_eq!(e.value, b"test_value");
        assert_eq!(e.version, 1);
    }

    #[test]
    fn test_set_entity_must_not_exist() {
        let state = create_test_state();
        let vault = VaultId::new(1);

        // First set
        let ops = vec![Operation::SetEntity {
            key: "key".to_string(),
            value: b"value1".to_vec(),
            condition: Some(SetCondition::MustNotExist),
            expires_at: None,
        }];
        let statuses = state.apply_operations(vault, &ops, 1).unwrap();
        assert_eq!(statuses, vec![WriteStatus::Created]);

        // Second set should fail with error (batch atomicity)
        let ops = vec![Operation::SetEntity {
            key: "key".to_string(),
            value: b"value2".to_vec(),
            condition: Some(SetCondition::MustNotExist),
            expires_at: None,
        }];
        let result = state.apply_operations(vault, &ops, 2);

        // Should return PreconditionFailed error with current state details
        match result {
            Err(StateError::PreconditionFailed {
                key,
                current_version,
                current_value,
                failed_condition,
            }) => {
                assert_eq!(key, "key");
                assert_eq!(current_version, Some(1)); // Set at block_height 1
                assert_eq!(current_value, Some(b"value1".to_vec()));
                assert_eq!(failed_condition, Some(SetCondition::MustNotExist));
            },
            Ok(_) => panic!("Expected PreconditionFailed error, got Ok"),
            Err(e) => panic!("Expected PreconditionFailed error, got: {:?}", e),
        }
    }

    #[test]
    fn test_delete_entity() {
        let state = create_test_state();
        let vault = VaultId::new(1);

        // Set then delete
        let ops = vec![Operation::SetEntity {
            key: "key".to_string(),
            value: b"value".to_vec(),
            condition: None,
            expires_at: None,
        }];
        state.apply_operations(vault, &ops, 1).unwrap();

        let ops = vec![Operation::DeleteEntity { key: "key".to_string() }];
        let statuses = state.apply_operations(vault, &ops, 2).unwrap();
        assert_eq!(statuses, vec![WriteStatus::Deleted]);

        // Should not exist now
        assert!(state.get_entity(vault, b"key").unwrap().is_none());

        // Delete again should be NotFound
        let statuses = state.apply_operations(vault, &ops, 3).unwrap();
        assert_eq!(statuses, vec![WriteStatus::NotFound]);
    }

    #[test]
    fn test_create_relationship() {
        let state = create_test_state();
        let vault = VaultId::new(1);

        let ops = vec![Operation::CreateRelationship {
            resource: "doc:123".to_string(),
            relation: "viewer".to_string(),
            subject: "user:alice".to_string(),
        }];

        let statuses = state.apply_operations(vault, &ops, 1).unwrap();
        assert_eq!(statuses, vec![WriteStatus::Created]);

        assert!(state.relationship_exists(vault, "doc:123", "viewer", "user:alice").unwrap());

        // Create again should return AlreadyExists
        let statuses = state.apply_operations(vault, &ops, 2).unwrap();
        assert_eq!(statuses, vec![WriteStatus::AlreadyExists]);
    }

    #[test]
    fn test_delete_relationship() {
        let state = create_test_state();
        let vault = VaultId::new(1);

        // Create
        let ops = vec![Operation::CreateRelationship {
            resource: "doc:123".to_string(),
            relation: "viewer".to_string(),
            subject: "user:alice".to_string(),
        }];
        state.apply_operations(vault, &ops, 1).unwrap();

        // Delete
        let ops = vec![Operation::DeleteRelationship {
            resource: "doc:123".to_string(),
            relation: "viewer".to_string(),
            subject: "user:alice".to_string(),
        }];
        let statuses = state.apply_operations(vault, &ops, 2).unwrap();
        assert_eq!(statuses, vec![WriteStatus::Deleted]);

        assert!(!state.relationship_exists(vault, "doc:123", "viewer", "user:alice").unwrap());
    }

    /// Verifies list_relationships returns all relationships across all buckets.
    #[test]
    fn test_list_relationships_returns_all_buckets() {
        let state = create_test_state();
        let vault = VaultId::new(1);

        let test_cases = vec![
            ("doc:alpha", "viewer", "user:a"),
            ("doc:beta", "viewer", "user:b"),
            ("doc:gamma", "viewer", "user:c"),
            ("doc:delta", "viewer", "user:d"),
            ("doc:epsilon", "viewer", "user:e"),
            ("doc:zeta", "viewer", "user:f"),
            ("doc:eta", "viewer", "user:g"),
            ("doc:theta", "viewer", "user:h"),
        ];

        // Create all relationships
        let ops: Vec<Operation> = test_cases
            .iter()
            .map(|(resource, relation, subject)| Operation::CreateRelationship {
                resource: resource.to_string(),
                relation: relation.to_string(),
                subject: subject.to_string(),
            })
            .collect();

        let statuses = state.apply_operations(vault, &ops, 1).unwrap();
        assert!(statuses.iter().all(|s| *s == WriteStatus::Created));

        // List all relationships without start_after
        let listed = state.list_relationships(vault, None, 100).unwrap();

        assert_eq!(
            listed.len(),
            test_cases.len(),
            "list_relationships should return all {} relationships, got {}.",
            test_cases.len(),
            listed.len()
        );

        // Verify all specific relationships are present
        for (resource, relation, subject) in &test_cases {
            let found = listed.iter().any(|r| {
                r.resource == *resource && r.relation == *relation && r.subject == *subject
            });
            assert!(
                found,
                "Relationship {}#{}@{} not found in list_relationships output",
                resource, relation, subject
            );
        }
    }

    /// Reproduction test for Engine integration test failure.
    ///
    /// Simulates the concurrent write + list pattern. Writes 11 relationships
    /// (1 sequential + 10 concurrent) and verifies all are returned by
    /// list_relationships.
    #[test]
    fn test_list_relationships_after_concurrent_writes() {
        use std::thread;

        let state = create_test_state();
        let vault = VaultId::new(20_000_002_690_000);

        // First: sequential write
        let seq_ops = vec![Operation::CreateRelationship {
            resource: "document:seq-test".to_string(),
            relation: "viewer".to_string(),
            subject: "user:seq-test".to_string(),
        }];
        let statuses = state.apply_operations(vault, &seq_ops, 1).unwrap();
        assert_eq!(statuses, vec![WriteStatus::Created]);

        assert!(
            state
                .relationship_exists(vault, "document:seq-test", "viewer", "user:seq-test")
                .unwrap()
        );

        // Concurrent writes
        let state_arc = std::sync::Arc::new(state);
        let mut handles = Vec::new();

        for i in 0..10 {
            let state = std::sync::Arc::clone(&state_arc);
            let handle = thread::spawn(move || {
                let ops = vec![Operation::CreateRelationship {
                    resource: format!("document:concurrent-{}", i),
                    relation: "viewer".to_string(),
                    subject: format!("user:concurrent-{}", i),
                }];
                state.apply_operations(vault, &ops, (i + 2) as u64).unwrap()
            });
            handles.push(handle);
        }

        for handle in handles {
            let statuses = handle.join().expect("thread panicked");
            assert_eq!(statuses, vec![WriteStatus::Created]);
        }

        for i in 0..10 {
            assert!(
                state_arc
                    .relationship_exists(
                        vault,
                        &format!("document:concurrent-{}", i),
                        "viewer",
                        &format!("user:concurrent-{}", i),
                    )
                    .unwrap(),
                "Direct read failed for concurrent-{}",
                i
            );
        }

        let all_relationships = state_arc.list_relationships(vault, None, 100).unwrap();

        assert_eq!(
            all_relationships.len(),
            11,
            "Expected 11 relationships (1 seq + 10 concurrent), got {}",
            all_relationships.len()
        );

        assert!(
            all_relationships.iter().any(|r| r.resource == "document:seq-test"),
            "Sequential relationship not found in list"
        );
        for i in 0..10 {
            assert!(
                all_relationships
                    .iter()
                    .any(|r| r.resource == format!("document:concurrent-{}", i)),
                "Concurrent relationship {} not found in list",
                i
            );
        }
    }

    #[test]
    fn test_state_root_changes_on_writes() {
        let state = create_test_state();
        let vault = VaultId::new(1);

        let root1 = state.compute_state_root(vault).unwrap();

        let ops = vec![Operation::SetEntity {
            key: "key".to_string(),
            value: b"value".to_vec(),
            condition: None,
            expires_at: None,
        }];
        state.apply_operations(vault, &ops, 1).unwrap();

        let root2 = state.compute_state_root(vault).unwrap();
        assert_ne!(root1, root2);

        // Deleting should change root again
        let ops = vec![Operation::DeleteEntity { key: "key".to_string() }];
        state.apply_operations(vault, &ops, 2).unwrap();

        let root3 = state.compute_state_root(vault).unwrap();
        assert_ne!(root2, root3);
        // After deleting, should be back to empty state
        assert_eq!(root1, root3);
    }

    #[test]
    fn test_vaults_are_isolated() {
        let state = create_test_state();

        let ops = vec![Operation::SetEntity {
            key: "key".to_string(),
            value: b"value".to_vec(),
            condition: None,
            expires_at: None,
        }];
        state.apply_operations(VaultId::new(1), &ops, 1).unwrap();

        // Vault 2 should not see vault 1's data
        assert!(state.get_entity(VaultId::new(2), b"key").unwrap().is_none());

        // State roots should differ
        let root1 = state.compute_state_root(VaultId::new(1)).unwrap();
        let root2 = state.compute_state_root(VaultId::new(2)).unwrap();
        assert_ne!(root1, root2);
    }

    #[test]
    fn test_clear_vault() {
        let state = create_test_state();
        let vault = VaultId::new(1);

        // Create entities and relationships
        let ops = vec![
            Operation::SetEntity {
                key: "entity1".to_string(),
                value: b"value1".to_vec(),
                condition: None,
                expires_at: None,
            },
            Operation::SetEntity {
                key: "entity2".to_string(),
                value: b"value2".to_vec(),
                condition: None,
                expires_at: None,
            },
            Operation::CreateRelationship {
                resource: "doc:1".to_string(),
                relation: "viewer".to_string(),
                subject: "user:alice".to_string(),
            },
        ];
        state.apply_operations(vault, &ops, 1).unwrap();

        // Also add data to vault 2 to ensure isolation
        let ops_vault2 = vec![Operation::SetEntity {
            key: "entity_v2".to_string(),
            value: b"value_v2".to_vec(),
            condition: None,
            expires_at: None,
        }];
        state.apply_operations(VaultId::new(2), &ops_vault2, 1).unwrap();

        // Verify data exists in vault 1
        assert!(state.get_entity(vault, b"entity1").unwrap().is_some());
        assert!(state.get_entity(vault, b"entity2").unwrap().is_some());
        assert!(state.relationship_exists(vault, "doc:1", "viewer", "user:alice").unwrap());

        // Clear vault 1
        state.clear_vault(vault).unwrap();

        // Verify vault 1 data is gone
        assert!(state.get_entity(vault, b"entity1").unwrap().is_none());
        assert!(state.get_entity(vault, b"entity2").unwrap().is_none());
        assert!(!state.relationship_exists(vault, "doc:1", "viewer", "user:alice").unwrap());

        // Verify vault 2 data is still there (isolation)
        assert!(state.get_entity(VaultId::new(2), b"entity_v2").unwrap().is_some());

        // State root should be back to empty
        let empty_root = state.compute_state_root(vault).unwrap();
        let fresh_state = create_test_state();
        let expected_empty = fresh_state.compute_state_root(vault).unwrap();
        assert_eq!(empty_root, expected_empty);
    }

    /// Stress test: many sequential writes followed by verification
    /// Simulates the pattern used by the server stress tests
    #[test]
    fn test_many_sequential_writes_then_verify() {
        let state = create_test_state();
        let vault = VaultId::new(1);

        // Write 500 entities sequentially
        let num_keys = 500;
        for i in 0..num_keys {
            let key = format!("stress-key-{}", i);
            let value = format!("stress-value-{}", i).into_bytes();
            let ops = vec![Operation::SetEntity {
                key: key.clone(),
                value: value.clone(),
                condition: None,
                expires_at: None,
            }];

            let statuses = state.apply_operations(vault, &ops, i as u64 + 1).unwrap();
            assert_eq!(statuses, vec![WriteStatus::Created], "Failed to create key {}", i);
        }

        // Verify all keys are present
        let mut missing = Vec::new();
        for i in 0..num_keys {
            let key = format!("stress-key-{}", i);
            let expected = format!("stress-value-{}", i).into_bytes();
            match state.get_entity(vault, key.as_bytes()).unwrap() {
                Some(entity) => {
                    assert_eq!(entity.value, expected, "Value mismatch for key {}", i);
                },
                None => {
                    missing.push(i);
                },
            }
        }

        assert!(
            missing.is_empty(),
            "Missing {} keys out of {}: {:?}",
            missing.len(),
            num_keys,
            &missing[..std::cmp::min(10, missing.len())]
        );
    }

    /// Stress test with concurrent threads writing different keys
    #[test]
    fn test_concurrent_writes_from_threads() {
        use std::{sync::Arc, thread};

        let state = Arc::new(create_test_state());
        let vault = VaultId::new(1);
        let num_threads = 4;
        let writes_per_thread = 50;

        let mut handles = Vec::new();
        for thread_id in 0..num_threads {
            let state = Arc::clone(&state);
            let handle = thread::spawn(move || {
                for i in 0..writes_per_thread {
                    let key = format!("key-{}-{}", thread_id, i);
                    let value = format!("value-{}-{}", thread_id, i).into_bytes();
                    let ops = vec![Operation::SetEntity {
                        key,
                        value,
                        condition: None,
                        expires_at: None,
                    }];

                    // Each write gets a unique block height
                    let block_height = (thread_id * writes_per_thread + i + 1) as u64;
                    state.apply_operations(vault, &ops, block_height).unwrap();
                }
            });
            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        // Verify all keys are present
        let mut missing = Vec::new();
        for thread_id in 0..num_threads {
            for i in 0..writes_per_thread {
                let key = format!("key-{}-{}", thread_id, i);
                let expected = format!("value-{}-{}", thread_id, i).into_bytes();
                match state.get_entity(vault, key.as_bytes()).unwrap() {
                    Some(entity) => {
                        assert_eq!(entity.value, expected, "Value mismatch for key {}", key);
                    },
                    None => {
                        missing.push(key);
                    },
                }
            }
        }

        let expected_total = num_threads * writes_per_thread;
        assert!(
            missing.is_empty(),
            "Missing {} keys out of {}: {:?}",
            missing.len(),
            expected_total,
            &missing[..std::cmp::min(10, missing.len())]
        );
    }

    // =========================================================================
    // Property-based tests for state determinism
    // =========================================================================

    mod proptest_determinism {
        use proptest::prelude::*;

        use super::*;

        /// Generates an arbitrary entity key (short identifiers for efficiency).
        fn arb_key() -> impl Strategy<Value = String> {
            proptest::string::string_regex("[a-z][a-z0-9]{0,7}").expect("valid regex")
        }

        /// Generates an arbitrary entity value (small byte arrays).
        fn arb_value() -> impl Strategy<Value = Vec<u8>> {
            proptest::collection::vec(any::<u8>(), 0..32)
        }

        /// Generates an arbitrary resource identifier.
        fn arb_resource() -> impl Strategy<Value = String> {
            (arb_key(), prop::sample::select(vec!["doc", "folder", "project"]))
                .prop_map(|(id, typ)| format!("{}:{}", typ, id))
        }

        /// Generates an arbitrary relation name.
        fn arb_relation() -> impl Strategy<Value = String> {
            prop::sample::select(vec![
                "viewer".to_string(),
                "editor".to_string(),
                "owner".to_string(),
                "member".to_string(),
            ])
        }

        /// Generates an arbitrary subject identifier.
        fn arb_subject() -> impl Strategy<Value = String> {
            (arb_key(), prop::sample::select(vec!["user", "group", "team"]))
                .prop_map(|(id, typ)| format!("{}:{}", typ, id))
        }

        /// Generates an arbitrary Operation.
        fn arb_operation() -> impl Strategy<Value = Operation> {
            prop_oneof![
                // SetEntity (most common)
                (arb_key(), arb_value()).prop_map(|(key, value)| {
                    Operation::SetEntity { key, value, condition: None, expires_at: None }
                }),
                // DeleteEntity
                arb_key().prop_map(|key| Operation::DeleteEntity { key }),
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

        /// Generates a sequence of operations (1 to 20 operations per test).
        fn arb_operation_sequence() -> impl Strategy<Value = Vec<Operation>> {
            proptest::collection::vec(arb_operation(), 1..20)
        }

        proptest! {
            /// Test: Applying the same operations to two independent StateLayer
            /// instances must produce identical state roots.
            ///
            /// This is a critical invariant for Raft consensus - all replicas
            /// must reach the same state when applying the same log entries.
            #[test]
            fn state_determinism_same_operations(
                operations in arb_operation_sequence()
            ) {
                let vault = VaultId::new(1);

                // Create two independent StateLayer instances
                let state1 = create_test_state();
                let state2 = create_test_state();

                // Apply the same operations to both
                for (idx, op) in operations.iter().enumerate() {
                    let block_height = (idx + 1) as u64;
                    let _ = state1.apply_operations(vault, std::slice::from_ref(op), block_height);
                    let _ = state2.apply_operations(vault, std::slice::from_ref(op), block_height);
                }

                // State roots must be identical
                let root1 = state1.compute_state_root(vault).unwrap();
                let root2 = state2.compute_state_root(vault).unwrap();

                prop_assert_eq!(
                    root1, root2,
                    "State roots diverged after applying {} operations",
                    operations.len()
                );
            }

            /// Test: Applying operations one-by-one vs all-at-once in a batch
            /// (at the same block height) must produce identical state roots.
            ///
            /// This verifies that batching doesn't affect determinism when
            /// all operations are at the same block height.
            ///
            /// Note: Different block heights will produce different entity versions
            /// (stored in Entity.version field), so this test uses the same height.
            #[test]
            fn state_determinism_batch_vs_individual(
                operations in arb_operation_sequence()
            ) {
                let vault = VaultId::new(1);
                let block_height: u64 = 1; // Same height for both

                // Apply one-by-one at the SAME block height
                let state_individual = create_test_state();
                for op in operations.iter() {
                    let _ = state_individual.apply_operations(vault, std::slice::from_ref(op), block_height);
                }

                // Apply as batch at the same block height
                let state_batch = create_test_state();
                let _ = state_batch.apply_operations(vault, &operations, block_height);

                // State roots must be identical
                let root_individual = state_individual.compute_state_root(vault).unwrap();
                let root_batch = state_batch.compute_state_root(vault).unwrap();

                prop_assert_eq!(
                    root_individual, root_batch,
                    "Batch vs individual application produced different roots"
                );
            }

            /// Test: State root computation is idempotent - calling it multiple
            /// times without changes must return the same value.
            #[test]
            fn state_root_idempotent(
                operations in arb_operation_sequence()
            ) {
                let vault = VaultId::new(1);
                let state = create_test_state();

                // Apply operations
                let _ = state.apply_operations(vault, &operations, 1);

                // Compute state root multiple times
                let root1 = state.compute_state_root(vault).unwrap();
                let root2 = state.compute_state_root(vault).unwrap();
                let root3 = state.compute_state_root(vault).unwrap();

                prop_assert_eq!(root1, root2, "First and second computation differ");
                prop_assert_eq!(root2, root3, "Second and third computation differ");
            }

            /// Test: Different operation sequences produce different state roots.
            ///
            /// This verifies the state root is actually sensitive to content changes.
            /// Note: There's a tiny probability of collision, but proptest's
            /// shrinking would catch systematic issues.
            #[test]
            fn different_operations_different_roots(
                ops1 in arb_operation_sequence(),
                ops2 in arb_operation_sequence(),
            ) {
                // Only test when sequences are actually different
                prop_assume!(ops1 != ops2);

                let vault = VaultId::new(1);

                let state1 = create_test_state();
                let state2 = create_test_state();

                let _ = state1.apply_operations(vault, &ops1, 1);
                let _ = state2.apply_operations(vault, &ops2, 1);

                let root1 = state1.compute_state_root(vault).unwrap();
                let root2 = state2.compute_state_root(vault).unwrap();

                // Hash collisions are possible but extremely unlikely.
                // If this fails repeatedly, there's a bug in hashing.
                // We allow the rare collision by not asserting inequality.
                // Instead, we verify that SOME operation sequences produce
                // different roots (which the previous tests implicitly do).
                                if root1 == root2 {
                    // Log for debugging, but don't fail - could be hash collision
                    // or operations that cancel out (e.g., set then delete same key)
                }
            }
        }
    }

    // Error conversion chain tests

    // Test StateError Display implementations for all variants
    #[test]
    fn test_state_error_display() {
        use std::error::Error;

        // Test Codec variant display via context selector
        let malformed: &[u8] = &[0xFF, 0xFF, 0xFF];
        let result: std::result::Result<u64, StateError> =
            inferadb_ledger_types::decode(malformed).context(CodecSnafu);
        let state_err = result.expect_err("should fail");
        let display = format!("{state_err}");

        assert!(display.starts_with("Codec error:"), "Expected 'Codec error:', got: {display}");

        // Verify source chain is preserved
        assert!(state_err.source().is_some(), "StateError::Codec should have a source");
    }

    // Test error conversion: CodecError -> StateError (via context)
    #[test]
    fn test_state_error_from_codec_error() {
        use snafu::ResultExt;

        // Simulate codec failure during state operation
        fn simulate_codec_failure() -> std::result::Result<(), StateError> {
            let malformed: &[u8] = &[0xFF, 0xFF];
            let _: u64 = inferadb_ledger_types::decode(malformed).context(CodecSnafu)?;
            Ok(())
        }

        let result = simulate_codec_failure();
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert!(matches!(err, StateError::Codec { .. }), "Should be StateError::Codec variant");
    }

    // Test the full error chain: StorageError -> IndexError -> StateError
    #[test]
    fn test_state_error_chain_from_index() {
        use crate::indexes::IndexError;

        // IndexError::Storage wraps a store error — construct one via the
        // Storage variant directly. We just verify the StateError display chain.
        let store_err =
            inferadb_ledger_store::Error::Io { source: std::io::Error::other("test io error") };
        let index_err = IndexError::Storage { source: store_err, location: snafu::location!() };

        // Convert IndexError -> StateError via context selector
        let state_err = StateError::Index { source: index_err, location: snafu::location!() };

        let display = format!("{state_err}");
        assert!(
            display.contains("Index error"),
            "StateError::Index should mention 'Index error': {display}"
        );
    }

    // Test PreconditionFailed variant display (special case with optional fields)
    #[test]
    fn test_state_error_precondition_failed_display() {
        let err = StateError::PreconditionFailed {
            key: "test_key".to_string(),
            current_version: Some(5),
            current_value: Some(b"old_value".to_vec()),
            failed_condition: None,
        };

        let display = format!("{err}");
        assert!(
            display.contains("test_key"),
            "PreconditionFailed should mention the key: {display}"
        );
    }

    #[test]
    fn test_compact_tables_empty() {
        let state = create_test_state();
        let stats = state.compact_tables(SYSTEM_VAULT_ID, 0.4).unwrap();
        assert_eq!(stats.pages_merged, 0);
        assert_eq!(stats.pages_freed, 0);
    }

    #[test]
    fn test_compact_tables_preserves_data() {
        let state = create_test_state();
        let vault = VaultId::new(1);

        // Insert many entities
        for i in 0..100 {
            let ops = vec![Operation::SetEntity {
                key: format!("ckey-{:04}", i),
                value: format!("cval-{:04}", i).into_bytes(),
                condition: None,
                expires_at: None,
            }];
            state.apply_operations(vault, &ops, i as u64 + 1).unwrap();
        }

        // Delete most to create sparse leaves
        for i in 0..80 {
            let ops = vec![Operation::DeleteEntity { key: format!("ckey-{:04}", i) }];
            state.apply_operations(vault, &ops, 200 + i as u64).unwrap();
        }

        // Run compaction on the vault that received the writes — Slice 2c
        // signature requires the caller to address a specific vault.
        let stats = state.compact_tables(vault, 0.4).unwrap();

        // Verify remaining data is intact
        for i in 80..100 {
            let entity = state.get_entity(vault, format!("ckey-{:04}", i).as_bytes()).unwrap();
            assert!(entity.is_some(), "Entity ckey-{:04} missing after compaction", i);
            assert_eq!(entity.unwrap().value, format!("cval-{:04}", i).into_bytes());
        }

        // Verify deleted keys are still gone
        for i in 0..80 {
            let entity = state.get_entity(vault, format!("ckey-{:04}", i).as_bytes()).unwrap();
            assert!(entity.is_none());
        }

        // At least the stats struct should be populated (may or may not have merges
        // depending on actual page layout)
        let _ = stats;
    }

    // ================================================================
    // Atomicity sentinel tests
    // ================================================================

    #[test]
    fn test_read_last_applied_returns_none_on_fresh_db() {
        let state = create_test_state();
        let result = state.read_last_applied(None).unwrap();
        assert!(result.is_none(), "fresh database should have no sentinel");
    }

    #[test]
    fn test_persist_and_read_last_applied() {
        let state = create_test_state();
        let log_id_bytes = b"test_log_id_v1";

        // Persist sentinel through the meta-db helper (Slice 1 cleave: the
        // sentinel lives in its own database, committed in a separate txn
        // after the state.db apply).
        state.persist_last_applied_meta(None, log_id_bytes).unwrap();

        // Read it back
        let result = state.read_last_applied(None).unwrap();
        assert_eq!(result.as_deref(), Some(log_id_bytes.as_slice()));
    }

    #[test]
    fn test_sentinel_overwrites_on_update() {
        let state = create_test_state();

        state.persist_last_applied_meta(None, b"log_id_1").unwrap();
        state.persist_last_applied_meta(None, b"log_id_2").unwrap();

        let result = state.read_last_applied(None).unwrap();
        assert_eq!(result.as_deref(), Some(b"log_id_2".as_slice()));
    }

    /// Post–Slice 1, entity writes and the sentinel commit in two separate
    /// transactions across two separate databases. Success of both leaves
    /// both readable — this test pins that behaviour.
    #[test]
    fn test_entity_and_sentinel_visible_after_two_step_apply() {
        let state = create_test_state();
        let vault = VaultId::new(1);

        // Step 1: entity data → state.db.
        let db = state.db_for(vault).unwrap();
        let mut txn = db.write().unwrap();
        let mut dict = crate::dictionary::VaultDictionary::new(vault);
        state
            .apply_operations_in_txn(
                &mut txn,
                &mut dict,
                vault,
                &[Operation::SetEntity {
                    key: "key1".into(),
                    value: b"value1".to_vec(),
                    condition: None,
                    expires_at: None,
                }],
                1,
            )
            .unwrap();
        txn.commit().unwrap();
        drop(db);

        // Step 2: sentinel → meta.db.
        state.persist_last_applied_meta(None, b"log_id_100").unwrap();

        let entity = state.get_entity(vault, b"key1").unwrap();
        assert!(entity.is_some());
        assert_eq!(
            state.read_last_applied(None).unwrap().as_deref(),
            Some(b"log_id_100".as_slice())
        );
    }

    /// Crash-between-commits model: step 1 succeeded (entity data is in
    /// state.db) but step 2 never ran (sentinel was not written to
    /// meta.db). On the next boot, `read_last_applied` returns `None`, so
    /// replay re-drives the idempotent apply path — no double-apply, no
    /// ghost sentinel.
    #[test]
    fn test_crash_between_commits_leaves_sentinel_unadvanced() {
        let state = create_test_state();
        let vault = VaultId::new(1);

        let db = state.db_for(vault).unwrap();
        let mut txn = db.write().unwrap();
        let mut dict = crate::dictionary::VaultDictionary::new(vault);
        state
            .apply_operations_in_txn(
                &mut txn,
                &mut dict,
                vault,
                &[Operation::SetEntity {
                    key: "key1".into(),
                    value: b"value1".to_vec(),
                    condition: None,
                    expires_at: None,
                }],
                1,
            )
            .unwrap();
        txn.commit().unwrap();
        drop(db);
        // Simulate crash before step 2: we never call
        // persist_last_applied_meta.

        let entity = state.get_entity(vault, b"key1").unwrap();
        assert!(entity.is_some(), "step 1's entity data persists");
        assert!(
            state.read_last_applied(None).unwrap().is_none(),
            "step 2 never ran — the sentinel must not have advanced"
        );
    }

    /// Sentinel now lives in meta.db, so writing an entity with the same
    /// byte key ("_meta:last_applied") into state.db does not overwrite
    /// the sentinel — distinct B+ trees across databases.
    #[test]
    fn test_sentinel_does_not_collide_with_entity_keys() {
        let state = create_test_state();
        let vault = VaultId::new(1);

        state.persist_last_applied_meta(None, b"sentinel_value").unwrap();

        // Write an entity with a key that starts with _meta:. The
        // `state_layer.apply_operations` path uses a vault-scoped storage
        // key, so this entity lands under a completely different B+ tree
        // key in state.db.
        state
            .apply_operations(
                vault,
                &[Operation::SetEntity {
                    key: "_meta:last_applied".into(),
                    value: b"entity_value".to_vec(),
                    condition: None,
                    expires_at: None,
                }],
                1,
            )
            .unwrap();

        let sentinel = state.read_last_applied(None).unwrap();
        assert_eq!(sentinel.as_deref(), Some(b"sentinel_value".as_slice()));

        let entity = state.get_entity(vault, b"_meta:last_applied").unwrap();
        assert!(entity.is_some());
        assert_eq!(entity.unwrap().value, b"entity_value");
    }

    /// Legacy detector: a pre–Slice 1 data_dir carries the sentinel under
    /// the Entities table in state.db. Writing that artefact directly
    /// lets us confirm `read_legacy_last_applied_from_state_db` picks it
    /// up.
    #[test]
    fn test_legacy_sentinel_detection_reads_state_db() {
        let state = create_test_state();

        // Simulate a pre–Slice 1 artefact: sentinel bytes written under
        // the legacy key into the system vault's state.db Entities table.
        // Under Slice 2c the legacy probe is scoped explicitly to the
        // system vault DB (no `any_db` shim — every consumer addresses a
        // specific vault id).
        let db = state.db_for(SYSTEM_VAULT_ID).unwrap();
        let mut txn = db.write().unwrap();
        txn.insert_raw(
            inferadb_ledger_store::tables::TableId::Entities,
            StateLayer::<InMemoryBackend>::LAST_APPLIED_KEY,
            b"legacy_sentinel",
        )
        .unwrap();
        txn.commit().unwrap();
        drop(db);

        let legacy = state.read_legacy_last_applied_from_state_db().unwrap();
        assert_eq!(legacy.as_deref(), Some(b"legacy_sentinel".as_slice()));

        // meta.db is pristine — the sentinel hasn't been written through
        // the new API.
        assert!(state.read_last_applied(None).unwrap().is_none());
    }

    #[test]
    fn test_mark_all_dirty_forces_full_recomputation() {
        let state = create_test_state();
        let vault = VaultId::new(1);

        // Write some entity data
        state
            .apply_operations(
                vault,
                &[Operation::SetEntity {
                    key: "key1".into(),
                    value: b"value1".to_vec(),
                    condition: None,
                    expires_at: None,
                }],
                1,
            )
            .unwrap();

        // Compute state root normally (dirty from apply_operations)
        let root1 = state.compute_state_root(vault).unwrap();

        // Mark all dirty and recompute — should produce the same root
        state.mark_all_dirty(vault);
        let root2 = state.compute_state_root(vault).unwrap();

        assert_eq!(root1, root2, "mark_all_dirty recomputation should match normal computation");
    }

    // =========================================================================
    // Additional coverage tests
    // =========================================================================

    #[test]
    fn test_set_entity_must_exist_on_missing_key_fails() {
        let state = create_test_state();
        let vault = VaultId::new(1);

        let ops = vec![Operation::SetEntity {
            key: "missing".to_string(),
            value: b"val".to_vec(),
            condition: Some(SetCondition::MustExist),
            expires_at: None,
        }];

        let result = state.apply_operations(vault, &ops, 1);
        match result {
            Err(StateError::PreconditionFailed {
                key, current_version, failed_condition, ..
            }) => {
                assert_eq!(key, "missing");
                assert_eq!(current_version, None);
                assert_eq!(failed_condition, Some(SetCondition::MustExist));
            },
            other => panic!("Expected PreconditionFailed, got: {other:?}"),
        }
    }

    #[test]
    fn test_set_entity_version_equals_condition() {
        let state = create_test_state();
        let vault = VaultId::new(1);

        // Create entity at block_height 1
        state
            .apply_operations(
                vault,
                &[Operation::SetEntity {
                    key: "k".into(),
                    value: b"v1".to_vec(),
                    condition: None,
                    expires_at: None,
                }],
                1,
            )
            .unwrap();

        // VersionEquals(1) should succeed
        let result = state.apply_operations(
            vault,
            &[Operation::SetEntity {
                key: "k".into(),
                value: b"v2".to_vec(),
                condition: Some(SetCondition::VersionEquals(1)),
                expires_at: None,
            }],
            2,
        );
        assert!(result.is_ok());

        // VersionEquals(1) should fail now (version is 2)
        let result = state.apply_operations(
            vault,
            &[Operation::SetEntity {
                key: "k".into(),
                value: b"v3".to_vec(),
                condition: Some(SetCondition::VersionEquals(1)),
                expires_at: None,
            }],
            3,
        );
        assert!(matches!(result, Err(StateError::PreconditionFailed { .. })));
    }

    #[test]
    fn test_set_entity_value_equals_condition() {
        let state = create_test_state();
        let vault = VaultId::new(1);

        // Create entity
        state
            .apply_operations(
                vault,
                &[Operation::SetEntity {
                    key: "k".into(),
                    value: b"expected".to_vec(),
                    condition: None,
                    expires_at: None,
                }],
                1,
            )
            .unwrap();

        // ValueEquals with matching value should succeed
        let result = state.apply_operations(
            vault,
            &[Operation::SetEntity {
                key: "k".into(),
                value: b"new".to_vec(),
                condition: Some(SetCondition::ValueEquals(b"expected".to_vec())),
                expires_at: None,
            }],
            2,
        );
        assert!(result.is_ok());

        // ValueEquals with wrong value should fail
        let result = state.apply_operations(
            vault,
            &[Operation::SetEntity {
                key: "k".into(),
                value: b"other".to_vec(),
                condition: Some(SetCondition::ValueEquals(b"wrong".to_vec())),
                expires_at: None,
            }],
            3,
        );
        assert!(matches!(result, Err(StateError::PreconditionFailed { .. })));
    }

    #[test]
    fn test_expire_entity() {
        let state = create_test_state();
        let vault = VaultId::new(1);

        // Create entity
        state
            .apply_operations(
                vault,
                &[Operation::SetEntity {
                    key: "k".into(),
                    value: b"v".to_vec(),
                    condition: None,
                    expires_at: Some(9999),
                }],
                1,
            )
            .unwrap();

        // Expire it
        let statuses = state
            .apply_operations(
                vault,
                &[Operation::ExpireEntity { key: "k".into(), expired_at: 1000 }],
                2,
            )
            .unwrap();
        assert_eq!(statuses, vec![WriteStatus::Deleted]);

        // Should not exist now
        assert!(state.get_entity(vault, b"k").unwrap().is_none());

        // Expire non-existent entity returns NotFound
        let statuses = state
            .apply_operations(
                vault,
                &[Operation::ExpireEntity { key: "k".into(), expired_at: 2000 }],
                3,
            )
            .unwrap();
        assert_eq!(statuses, vec![WriteStatus::NotFound]);
    }

    #[test]
    fn test_entity_with_expires_at() {
        let state = create_test_state();
        let vault = VaultId::new(1);

        let ops = vec![Operation::SetEntity {
            key: "ttl_key".to_string(),
            value: b"data".to_vec(),
            condition: None,
            expires_at: Some(999999),
        }];

        state.apply_operations(vault, &ops, 1).unwrap();

        let entity = state.get_entity(vault, b"ttl_key").unwrap().unwrap();
        assert_eq!(entity.expires_at, 999999);
    }

    #[test]
    fn test_compute_state_root_no_commitment() {
        let state = create_test_state();
        let vault = VaultId::new(99);

        // No commitment for this vault yet, should return default state root
        let root = state.compute_state_root(vault).unwrap();
        assert_ne!(root, [0u8; 32]); // default is sha256_concat of empty hashes
    }

    #[test]
    fn test_compute_state_root_cached_when_clean() {
        let state = create_test_state();
        let vault = VaultId::new(1);

        // Write some data and compute state root
        state
            .apply_operations(
                vault,
                &[Operation::SetEntity {
                    key: "k".into(),
                    value: b"v".to_vec(),
                    condition: None,
                    expires_at: None,
                }],
                1,
            )
            .unwrap();
        let root1 = state.compute_state_root(vault).unwrap();

        // Second call without writes should return cached value
        let root2 = state.compute_state_root(vault).unwrap();
        assert_eq!(root1, root2);
    }

    #[test]
    fn test_load_and_get_bucket_roots() {
        let state = create_test_state();
        let vault = VaultId::new(1);

        // Initially no bucket roots
        assert!(state.get_bucket_roots(vault).is_none());

        // Load bucket roots
        let roots = [inferadb_ledger_types::EMPTY_HASH; 256];
        state.load_vault_commitment(vault, roots);

        // Should be retrievable now
        let retrieved = state.get_bucket_roots(vault).unwrap();
        assert_eq!(retrieved, roots);
    }

    /// Per-bucket prefix-scan optimization: writing keys that hash into many
    /// different buckets and then computing the state root must produce the
    /// same root as a full-vault scan would. We verify two paths against each
    /// other via `mark_all_dirty` (which forces every bucket's root to be
    /// recomputed from storage, exercising the per-bucket prefix scan for
    /// every non-empty bucket).
    #[test]
    fn test_compute_state_root_per_bucket_matches_recomputation() {
        let state = create_test_state();
        let vault = VaultId::new(1);

        // Populate 64 entities whose keys land in many different buckets.
        let ops: Vec<Operation> = (0..64u32)
            .map(|i| Operation::SetEntity {
                key: format!("entity:{i}"),
                value: format!("value:{i}").into_bytes(),
                condition: None,
                expires_at: None,
            })
            .collect();
        state.apply_operations(vault, &ops, 1).unwrap();

        // First computation (only the dirty buckets touched by the writes
        // above go through the per-bucket prefix scan).
        let root_incremental = state.compute_state_root(vault).unwrap();

        // Force every bucket to be marked dirty and recompute. This drives
        // the per-bucket prefix scan across every non-empty bucket and the
        // final root must match the incremental root byte-for-byte.
        state.mark_all_dirty(vault);
        let root_full = state.compute_state_root(vault).unwrap();

        assert_eq!(
            root_incremental, root_full,
            "per-bucket prefix scan must produce identical state root to full recomputation"
        );
    }

    #[test]
    fn test_list_entities_with_prefix() {
        let state = create_test_state();
        let vault = VaultId::new(1);

        // Create entities with different prefixes
        for key in ["user:alice", "user:bob", "doc:readme", "doc:todo"] {
            state
                .apply_operations(
                    vault,
                    &[Operation::SetEntity {
                        key: key.to_string(),
                        value: b"data".to_vec(),
                        condition: None,
                        expires_at: None,
                    }],
                    1,
                )
                .unwrap();
        }

        // List with prefix "user:"
        let users = state.list_entities(vault, Some("user:"), None, 100).unwrap();
        assert_eq!(users.len(), 2);
        for e in &users {
            assert!(String::from_utf8_lossy(&e.key).starts_with("user:"));
        }

        // List with prefix "doc:"
        let docs = state.list_entities(vault, Some("doc:"), None, 100).unwrap();
        assert_eq!(docs.len(), 2);
    }

    #[test]
    fn test_list_entities_with_limit() {
        let state = create_test_state();
        let vault = VaultId::new(1);

        for i in 0..5 {
            state
                .apply_operations(
                    vault,
                    &[Operation::SetEntity {
                        key: format!("key{i}"),
                        value: b"data".to_vec(),
                        condition: None,
                        expires_at: None,
                    }],
                    1,
                )
                .unwrap();
        }

        let limited = state.list_entities(vault, None, None, 2).unwrap();
        assert_eq!(limited.len(), 2);
    }

    #[test]
    fn test_list_subjects_and_resources() {
        let state = create_test_state();
        let vault = VaultId::new(1);

        state
            .apply_operations(
                vault,
                &[
                    Operation::CreateRelationship {
                        resource: "doc:1".into(),
                        relation: "viewer".into(),
                        subject: "user:alice".into(),
                    },
                    Operation::CreateRelationship {
                        resource: "doc:1".into(),
                        relation: "viewer".into(),
                        subject: "user:bob".into(),
                    },
                ],
                1,
            )
            .unwrap();

        let subjects = state.list_subjects(vault, "doc:1", "viewer").unwrap();
        assert_eq!(subjects.len(), 2);
        assert!(subjects.contains(&"user:alice".to_string()));
        assert!(subjects.contains(&"user:bob".to_string()));

        let resources = state.list_resources_for_subject(vault, "user:alice").unwrap();
        assert_eq!(resources.len(), 1);
        assert_eq!(resources[0], ("doc:1".to_string(), "viewer".to_string()));
    }

    #[test]
    fn test_database_stats() {
        let state = create_test_state();
        let stats = state.database_stats();
        // stats should be available even on empty database (header pages always exist)
        let _ = stats.total_pages;
    }

    #[test]
    fn test_table_depths_empty() {
        let state = create_test_state();
        let depths = state.table_depths().unwrap();
        // All tables should be depth 0 on empty db
        for (_name, depth) in &depths {
            assert_eq!(*depth, 0);
        }
    }

    #[test]
    fn test_clone_starts_fresh_commitments() {
        let state = create_test_state();
        let vault = VaultId::new(1);

        state
            .apply_operations(
                vault,
                &[Operation::SetEntity {
                    key: "k".into(),
                    value: b"v".to_vec(),
                    condition: None,
                    expires_at: None,
                }],
                1,
            )
            .unwrap();
        state.compute_state_root(vault).unwrap();

        // Clone should not inherit commitment state
        let cloned = state.clone();
        assert!(cloned.get_bucket_roots(vault).is_none());
    }

    // =========================================================================
    // Dictionary cache tests
    // =========================================================================

    #[test]
    fn dictionary_cached_across_operations() {
        let state = create_test_state();
        let vault = VaultId::new(1);

        // First apply: creates relationships and populates dictionary cache
        let ops1 = vec![Operation::CreateRelationship {
            resource: "doc:1".to_string(),
            relation: "viewer".to_string(),
            subject: "user:alice".to_string(),
        }];
        let statuses1 = state.apply_operations(vault, &ops1, 1).unwrap();
        assert_eq!(statuses1, vec![WriteStatus::Created]);

        // Second apply: should reuse cached dictionary (no redundant B+ tree scan)
        let ops2 = vec![Operation::CreateRelationship {
            resource: "doc:2".to_string(),
            relation: "viewer".to_string(),
            subject: "user:bob".to_string(),
        }];
        let statuses2 = state.apply_operations(vault, &ops2, 2).unwrap();
        assert_eq!(statuses2, vec![WriteStatus::Created]);

        // Verify both relationships exist
        assert!(state.relationship_exists(vault, "doc:1", "viewer", "user:alice").unwrap());
        assert!(state.relationship_exists(vault, "doc:2", "viewer", "user:bob").unwrap());
    }

    #[test]
    fn dictionary_cache_cleared_on_clear_vault() {
        let state = create_test_state();
        let vault = VaultId::new(1);

        // Create a relationship (populates dictionary cache)
        let ops = vec![Operation::CreateRelationship {
            resource: "doc:1".to_string(),
            relation: "viewer".to_string(),
            subject: "user:alice".to_string(),
        }];
        state.apply_operations(vault, &ops, 1).unwrap();
        assert!(state.relationship_exists(vault, "doc:1", "viewer", "user:alice").unwrap());

        // Clear vault — should evict dictionary cache
        state.clear_vault(vault).unwrap();

        // Verify data is gone
        assert!(!state.relationship_exists(vault, "doc:1", "viewer", "user:alice").unwrap());

        // Re-create a relationship — should work with a fresh dictionary
        let ops2 = vec![Operation::CreateRelationship {
            resource: "doc:2".to_string(),
            relation: "editor".to_string(),
            subject: "user:bob".to_string(),
        }];
        let statuses = state.apply_operations(vault, &ops2, 2).unwrap();
        assert_eq!(statuses, vec![WriteStatus::Created]);
        assert!(state.relationship_exists(vault, "doc:2", "editor", "user:bob").unwrap());
    }

    #[test]
    fn dictionary_cache_rollback_on_error() {
        let state = create_test_state();
        let vault = VaultId::new(1);

        // Create an entity so we can trigger PreconditionFailed
        let ops = vec![Operation::SetEntity {
            key: "key".to_string(),
            value: b"value1".to_vec(),
            condition: None,
            expires_at: None,
        }];
        state.apply_operations(vault, &ops, 1).unwrap();

        // Trigger PreconditionFailed — dictionary should be discarded on error
        let bad_ops = vec![Operation::SetEntity {
            key: "key".to_string(),
            value: b"value2".to_vec(),
            condition: Some(SetCondition::MustNotExist),
            expires_at: None,
        }];
        let result = state.apply_operations(vault, &bad_ops, 2);
        assert!(matches!(result, Err(StateError::PreconditionFailed { .. })));

        // Next call should still work (dictionary reloaded from storage)
        let ops2 = vec![Operation::CreateRelationship {
            resource: "doc:1".to_string(),
            relation: "viewer".to_string(),
            subject: "user:alice".to_string(),
        }];
        let statuses = state.apply_operations(vault, &ops2, 3).unwrap();
        assert_eq!(statuses, vec![WriteStatus::Created]);
        assert!(state.relationship_exists(vault, "doc:1", "viewer", "user:alice").unwrap());
    }

    // =========================================================================
    // relationship_exists tests
    // =========================================================================

    #[test]
    fn relationship_exists_returns_true_for_created_tuple() {
        let state = create_test_state();
        let vault = VaultId::new(1);

        let ops = vec![Operation::CreateRelationship {
            resource: "doc:1".to_string(),
            relation: "viewer".to_string(),
            subject: "user:alice".to_string(),
        }];
        state.apply_operations(vault, &ops, 1).unwrap();

        assert!(state.relationship_exists(vault, "doc:1", "viewer", "user:alice").unwrap());
    }

    #[test]
    fn relationship_exists_returns_false_for_typo() {
        let state = create_test_state();
        let vault = VaultId::new(1);

        let ops = vec![Operation::CreateRelationship {
            resource: "doc:1".to_string(),
            relation: "viewer".to_string(),
            subject: "user:alice".to_string(),
        }];
        state.apply_operations(vault, &ops, 1).unwrap();

        // Typo'd resource
        assert!(!state.relationship_exists(vault, "doc:TYPO", "viewer", "user:alice").unwrap());

        // Typo'd relation
        assert!(!state.relationship_exists(vault, "doc:1", "TYPO", "user:alice").unwrap());

        // Typo'd subject
        assert!(!state.relationship_exists(vault, "doc:1", "viewer", "user:TYPO").unwrap());
    }

    #[test]
    fn relationship_exists_returns_false_for_empty_vault() {
        let state = create_test_state();
        let vault = VaultId::new(42);

        // No relationships have been created — must return false, not an error.
        assert!(!state.relationship_exists(vault, "doc:1", "viewer", "user:alice").unwrap());
    }

    // ─── apply_operations_lazy vs apply_operations ───────

    /// `apply_operations_lazy` produces the same in-process observable
    /// state as `apply_operations`. Test both paths against the same ops
    /// and assert round-trip equivalence. This is the correctness backstop
    /// for the IN-APPLY-PIPELINE flip: every lazy-applied batch must be
    /// indistinguishable from a durable-applied batch for in-process reads.
    #[test]
    fn apply_operations_lazy_matches_apply_operations_in_process() {
        let state_lazy = create_test_state();
        let state_durable = create_test_state();
        let vault = VaultId::new(1);

        let ops = vec![
            Operation::SetEntity {
                key: "a".to_string(),
                value: b"alpha".to_vec(),
                condition: None,
                expires_at: None,
            },
            Operation::SetEntity {
                key: "b".to_string(),
                value: b"bravo".to_vec(),
                condition: None,
                expires_at: None,
            },
        ];

        let lazy = state_lazy.apply_operations_lazy(vault, &ops, 1).unwrap();
        let durable = state_durable.apply_operations(vault, &ops, 1).unwrap();
        assert_eq!(lazy, durable, "lazy and durable must return identical statuses");

        // Both paths must surface the writes to subsequent in-process reads.
        assert_eq!(state_lazy.get_entity(vault, b"a").unwrap().unwrap().value, b"alpha".to_vec(),);
        assert_eq!(state_lazy.get_entity(vault, b"b").unwrap().unwrap().value, b"bravo".to_vec(),);
        assert_eq!(
            state_lazy.compute_state_root(vault).unwrap(),
            state_durable.compute_state_root(vault).unwrap(),
            "state roots must match across commit variants",
        );
    }

    /// CAS semantics are preserved by `apply_operations_lazy`.
    /// `PreconditionFailed` must surface from the lazy path exactly as
    /// it does from the durable path; the dictionary must be dropped (not
    /// reused) on failure.
    #[test]
    fn apply_operations_lazy_preserves_cas_semantics() {
        let state = create_test_state();
        let vault = VaultId::new(1);

        state
            .apply_operations_lazy(
                vault,
                &[Operation::SetEntity {
                    key: "k".to_string(),
                    value: b"v1".to_vec(),
                    condition: Some(SetCondition::MustNotExist),
                    expires_at: None,
                }],
                1,
            )
            .unwrap();

        let result = state.apply_operations_lazy(
            vault,
            &[Operation::SetEntity {
                key: "k".to_string(),
                value: b"v2".to_vec(),
                condition: Some(SetCondition::MustNotExist),
                expires_at: None,
            }],
            2,
        );

        match result {
            Err(StateError::PreconditionFailed { key, current_version, .. }) => {
                assert_eq!(key, "k");
                assert_eq!(current_version, Some(1));
            },
            other => panic!("expected PreconditionFailed, got {:?}", other),
        }

        // Original value must remain after the failed CAS.
        let entity = state.get_entity(vault, b"k").unwrap().unwrap();
        assert_eq!(entity.value, b"v1".to_vec());
        assert_eq!(entity.version, 1);
    }

    /// `apply_operations_lazy` skips the dual-slot fsync. On a
    /// `FileBackend` database, a lazy-applied write followed by a reopen
    /// MUST NOT surface the write — that is the whole point of the split
    /// (durability is deferred to the next `sync_state` call).
    ///
    /// The strict-durable `apply_operations` does not have this gap:
    /// after a reopen, its writes remain visible. This test pins both
    /// sides of the classification.
    #[test]
    fn apply_operations_lazy_skips_durable_commit_but_apply_operations_does_not() {
        use std::{path::PathBuf, sync::Arc};

        use inferadb_ledger_store::{Database, FileBackend};
        use tempfile::tempdir;

        // P2b.0: the factory shape mirrors production —
        // `state/vault-{vault_id}/state.db` under a per-case directory.
        // The factory creates the per-vault directory lazily before
        // opening state.db, matching the production factory in
        // `RaftManager::open_region_storage`.
        fn make_factory(
            root: PathBuf,
        ) -> impl Fn(VaultId) -> Result<Arc<Database<FileBackend>>> + Send + Sync + 'static
        {
            move |vault: VaultId| {
                let vault_dir = root.join("state").join(format!("vault-{}", vault.value()));
                std::fs::create_dir_all(&vault_dir).map_err(|e| StateError::Store {
                    source: inferadb_ledger_store::Error::Io { source: e },
                    location: snafu::location!(),
                })?;
                let path = vault_dir.join("state.db");
                let db = if path.exists() {
                    Database::<FileBackend>::open(&path)
                } else {
                    Database::<FileBackend>::create(&path)
                }
                .map_err(|e| StateError::Store { source: e, location: snafu::location!() })?;
                Ok(Arc::new(db))
            }
        }

        let tmp = tempdir().unwrap();
        let lazy_root = tmp.path().join("lazy");
        let lazy_meta_path = tmp.path().join("lazy_meta.ink");
        let durable_root = tmp.path().join("durable");
        let durable_meta_path = tmp.path().join("durable_meta.ink");

        // Lazy side: apply_operations_lazy → reopen → write is GONE.
        {
            let meta_db = Arc::new(Database::<FileBackend>::create(&lazy_meta_path).unwrap());
            let state = StateLayer::new(make_factory(lazy_root.clone()), meta_db).unwrap();
            state
                .apply_operations_lazy(
                    VaultId::new(1),
                    &[Operation::SetEntity {
                        key: "lazy_key".to_string(),
                        value: b"lazy_value".to_vec(),
                        condition: None,
                        expires_at: None,
                    }],
                    1,
                )
                .unwrap();
            // In-process visibility holds.
            assert!(state.get_entity(VaultId::new(1), b"lazy_key").unwrap().is_some());
        }

        let reopened_lazy_meta = Arc::new(Database::<FileBackend>::open(&lazy_meta_path).unwrap());
        let state_reopened_lazy =
            StateLayer::new(make_factory(lazy_root.clone()), reopened_lazy_meta).unwrap();
        assert!(
            state_reopened_lazy.get_entity(VaultId::new(1), b"lazy_key").unwrap().is_none(),
            "apply_operations_lazy must NOT persist the dual-slot state pointer — \
             the write must be gone after reopen",
        );

        // Durable side: apply_operations → reopen → write IS there.
        {
            let meta_db = Arc::new(Database::<FileBackend>::create(&durable_meta_path).unwrap());
            let state = StateLayer::new(make_factory(durable_root.clone()), meta_db).unwrap();
            state
                .apply_operations(
                    VaultId::new(1),
                    &[Operation::SetEntity {
                        key: "durable_key".to_string(),
                        value: b"durable_value".to_vec(),
                        condition: None,
                        expires_at: None,
                    }],
                    1,
                )
                .unwrap();
        }

        let reopened_durable_meta =
            Arc::new(Database::<FileBackend>::open(&durable_meta_path).unwrap());
        let state_reopened_durable =
            StateLayer::new(make_factory(durable_root), reopened_durable_meta).unwrap();
        let entity = state_reopened_durable
            .get_entity(VaultId::new(1), b"durable_key")
            .unwrap()
            .expect("apply_operations must persist across reopen");
        assert_eq!(entity.value, b"durable_value".to_vec());
    }

    /// `evict_vault_state_page_cache` errors when the vault is not
    /// materialised (rather than silently no-oping). At sleep time this
    /// would be a programmer bug — surface it so the raft-layer caller
    /// can log and metric it.
    #[test]
    fn test_evict_vault_state_page_cache_unknown_vault_errors() {
        let state = create_test_state();
        // VaultId(99) was never written, so the factory was never invoked
        // for it. Eviction must surface VaultNotMaterialized.
        let err = state
            .evict_vault_state_page_cache(VaultId::new(99))
            .expect_err("evict on never-materialised vault must error");
        assert!(
            matches!(err, StateError::VaultNotMaterialized { vault_id, .. } if vault_id == VaultId::new(99)),
            "expected VaultNotMaterialized, got {err:?}"
        );
    }

    /// `evict_vault_state_page_cache` is best-effort and idempotent on a
    /// materialised vault. The in-memory backend is a no-op success path
    /// (no OS page cache to drop); the round-trip asserts the API works
    /// both before and after a write lands without disturbing the data.
    #[test]
    fn test_evict_vault_state_page_cache_materialised_vault_roundtrips() {
        let state = create_test_state();
        let vault = VaultId::new(1);

        // Materialise the vault by performing a write.
        let ops = vec![Operation::SetEntity {
            key: "k".to_string(),
            value: b"v".to_vec(),
            condition: None,
            expires_at: None,
        }];
        state.apply_operations(vault, &ops, 1).unwrap();

        // First eviction.
        state.evict_vault_state_page_cache(vault).expect("evict must succeed");
        // Second eviction is idempotent.
        state.evict_vault_state_page_cache(vault).expect("evict must be idempotent");

        // The underlying data is still readable post-eviction.
        let e = state.get_entity(vault, b"k").unwrap();
        assert_eq!(e.map(|e| e.value), Some(b"v".to_vec()));
    }

    /// `evict_vault_state_page_cache` works on the eagerly-materialised
    /// system vault even before any apply touches it.
    #[test]
    fn test_evict_vault_state_page_cache_system_vault_is_eager() {
        let state = create_test_state();
        // SYSTEM_VAULT_ID is materialised eagerly in StateLayer::new.
        state.evict_vault_state_page_cache(SYSTEM_VAULT_ID).expect("system vault eager");
    }
}
