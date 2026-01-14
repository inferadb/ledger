//! Storage engine for InferaDB Ledger.
//!
//! This crate provides:
//! - inkwell B+ tree-based persistent storage
//! - State layer with bucket-based hashing
//! - Snapshot creation and restoration
//! - Dual indexes for relationship queries
//! - System namespace types and cluster membership
//! - Time-travel index for historical queries

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
pub use keys::{StorageKey, decode_storage_key, encode_storage_key};
pub use relationship::RelationshipStore;
pub use shard::ShardManager;
pub use snapshot::{Snapshot, SnapshotManager};
pub use state::{StateError, StateLayer};
// Re-export inkwell's tables for convenience
pub use inkwell::tables;
pub use tiered_storage::{
    LocalBackend, ObjectStorageBackend, StorageBackend, StorageTier, TieredConfig,
    TieredSnapshotManager, TieredStorageError,
};
pub use time_travel::{
    TimeTravelConfig, TimeTravelEntry, TimeTravelError, TimeTravelIndex, TimeTravelStats,
};
