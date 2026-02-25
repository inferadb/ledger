//! State management for InferaDB Ledger.
//!
//! This crate sits between the raw B+ tree storage engine (`inferadb-ledger-store`)
//! and the Raft consensus layer (`inferadb-ledger-raft`), providing:
//!
//! - State layer with bucket-based incremental hashing (256 buckets per vault)
//! - Entity and relationship CRUD with conditional writes
//! - Snapshot creation and restoration
//! - Dual indexes for relationship queries (object-centric and subject-centric)
//! - System organization types for organization routing and cluster membership
//! - Time-travel index for historical queries
//! - Block archive for committed block storage

#![deny(unsafe_code)]
#![warn(missing_docs)]

mod block_archive;
mod bucket;
mod engine;
mod entity;
mod events;
mod events_keys;
mod indexes;
mod keys;
mod relationship;
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

pub use block_archive::{BlockArchive, BlockArchiveError};
pub use bucket::{NUM_BUCKETS, VaultCommitment};
pub use engine::{InMemoryStorageEngine, StorageEngine};
pub use entity::EntityStore;
pub use events::{
    EventIndex, EventStore, EventStoreError, Events, EventsDatabase, EventsDatabaseError,
};
pub use events_keys::{
    DecodedEventKey, EVENT_INDEX_KEY_LEN, EVENT_INDEX_VALUE_LEN, EVENT_KEY_LEN, decode_event_key,
    encode_event_index_key, encode_event_index_value, encode_event_key, org_prefix,
    org_time_prefix, primary_key_from_index_value,
};
pub use indexes::IndexManager;
// Re-export inferadb-ledger-store's tables for convenience
pub use inferadb_ledger_store::tables;
pub use keys::{StorageKey, decode_storage_key, encode_storage_key};
pub use relationship::RelationshipStore;
pub use shard::ShardManager;
pub use snapshot::{
    Snapshot, SnapshotChainParams, SnapshotError, SnapshotManager, SnapshotStateData,
    VaultSnapshotMeta,
};
pub use state::{StateError, StateLayer};
pub use tiered_storage::{
    LocalBackend, ObjectStorageBackend, StorageBackend, StorageTier, TieredSnapshotManager,
    TieredStorageConfig, TieredStorageError,
};
