//! State management for InferaDB Ledger.
//!
//! This crate sits between the raw B+ tree storage engine (`inferadb-ledger-store`)
//! and the Raft consensus layer (`inferadb-ledger-raft`), providing:
//!
//! - State layer with bucket-based incremental hashing (256 buckets per vault)
//! - Entity and relationship CRUD with conditional writes
//! - Snapshot creation and restoration
//! - Dual indexes for relationship queries (object-centric and subject-centric)
//! - System namespace types and cluster membership
//! - Time-travel index for historical queries
//! - Block archive for committed block storage

#![deny(unsafe_code)]

mod block_archive;
mod bucket;
mod engine;
mod entity;
mod indexes;
mod keys;
mod relationship;
mod shard;
mod snapshot;
mod state;
pub mod system;
mod tiered_storage;
mod time_travel;

pub use block_archive::{BlockArchive, BlockArchiveError};
pub use bucket::VaultCommitment;
pub use engine::{InMemoryStorageEngine, StorageEngine};
pub use entity::EntityStore;
pub use indexes::IndexManager;
// Re-export inferadb-ledger-store's tables for convenience
pub use inferadb_ledger_store::tables;
pub use keys::{StorageKey, decode_storage_key, encode_storage_key};
pub use relationship::RelationshipStore;
pub use shard::ShardManager;
pub use snapshot::{Snapshot, SnapshotManager};
pub use state::{StateError, StateLayer};
pub use tiered_storage::{
    LocalBackend, ObjectStorageBackend, StorageBackend, StorageTier, TieredConfig,
    TieredSnapshotManager, TieredStorageError,
};
pub use time_travel::{
    TimeTravelConfig, TimeTravelEntry, TimeTravelError, TimeTravelIndex, TimeTravelStats,
};
