//! State management for InferaDB Ledger.
//!
//! This crate sits between the raw B+ tree storage engine (`inferadb-ledger-store`)
//! and the Raft consensus layer (`inferadb-ledger-raft`). It owns every domain
//! write path: keys are constructed, tier-validated, and written here; no crate
//! above calls `store` directly.
//!
//! # Core abstractions
//!
//! - [`StorageEngine`] — file-backed B+ tree wrapper; [`InMemoryStorageEngine`] for tests.
//! - [`StateLayer`] — per-vault entity/relationship CRUD with incremental state-root computation
//!   (256-bucket dirty tracking) and two apply entry points: [`StateLayer::apply_operations`]
//!   (strict-durable, for admin/recovery callers) and `apply_operations_lazy` (lazy via
//!   `commit_in_memory`, for the in-apply-pipeline path).
//! - [`system::SystemKeys`] — all storage-key builders for the `_system` organization. Keys are
//!   classified by [`system::KeyTier`] (Global vs Regional) and validated at write time by
//!   [`system::SystemKeys::validate_key_tier`].
//! - [`ShardManager`] — per-organization coordinator: block archive, snapshots, and vault-level
//!   state routing. Rebuilt from GLOBAL directory state on each start.
//!
//! # Data-residency patterns
//!
//! Every key written through this crate targets exactly one tier:
//!
//! - **Pattern 1** — REGIONAL-only bare key; no GLOBAL counterpart (e.g. `user:`, `team:`).
//! - **Pattern 2** — GLOBAL skeleton with no PII + REGIONAL `{entity}_profile:` overlay; merged on
//!   read (e.g. `app:` + `app_profile:`, `org:` + `org_profile:`).
//! - **Pattern 3** — GLOBAL-only, no PII (e.g. `signing_key:`, `refresh_token:`).
//!
//! PII must always go to REGIONAL. When residency is unclear, treat it as REGIONAL.
//! See [`system::SystemKeys::KEY_REGISTRY`] for the authoritative per-constant classification.
//!
//! # Other subsystems
//!
//! - [`BlockArchive`] — append-only per-vault block store with compaction support.
//! - [`SnapshotManager`] — point-in-time state snapshots (zstd-compressed).
//! - [`RelationshipIndex`] — in-memory O(1) relationship existence index, evicted on hibernate.
//! - [`EventStore`] / [`EventsDatabase`] — audit-event storage in a dedicated `events.db`.

#![deny(unsafe_code)]
#![warn(missing_docs)]

pub mod apply_pool;
pub mod binary_keys;
mod block_archive;
mod bucket;
pub mod dictionary;
mod engine;
mod entity;
mod events;
mod events_keys;
mod indexes;
mod keys;
mod relationship;
/// In-memory hash index for O(1) relationship existence checks.
pub mod relationship_index;
mod shard;
mod snapshot;
mod state;
/// System organization (`_system`) for global cluster data.
///
/// Contains types for user accounts, organization routing, cluster membership,
/// and cross-organization sagas. See [`system::SystemOrganizationService`] for the
/// primary service interface.
pub mod system;
mod tiered_storage;

pub use binary_keys::{InternCategory, InternId};
pub use block_archive::{BlockArchive, BlockArchiveError};
pub use bucket::{NUM_BUCKETS, VaultCommitment};
pub use dictionary::{DictionaryError, VaultDictionary};
pub use engine::{InMemoryStorageEngine, StorageEngine};
pub use entity::EntityStore;
pub use events::{
    EventIndex, EventStore, EventStoreError, Events, EventsDatabase, EventsDatabaseError,
};
pub use events_keys::encode_event_key;
pub use indexes::IndexManager;
pub use keys::{StorageKey, decode_storage_key, encode_index_key, encode_storage_key};
pub use relationship::RelationshipStore;
pub use relationship_index::RelationshipIndex;
pub use shard::ShardManager;
pub use snapshot::{
    Snapshot, SnapshotChainParams, SnapshotError, SnapshotManager, SnapshotStateData,
    VaultSnapshotMeta,
};
pub use state::{StateError, StateLayer, VaultDbFactory, new_state_layer_shared};
pub use tiered_storage::{
    LocalBackend, ObjectStorageBackend, StorageBackend, StorageTier, TieredSnapshotManager,
    TieredStorageConfig, TieredStorageError,
};
