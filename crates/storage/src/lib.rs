//! Storage engine for InferaDB Ledger.
//!
//! This crate provides:
//! - redb-based persistent storage
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
mod tables;
mod tiered_storage;
mod time_travel;

pub use block_archive::{BlockArchive, BlockArchiveError};
pub use bucket::VaultCommitment;
pub use engine::{ReadTxn, StorageEngine, WriteTxn};
pub use entity::EntityStore;
pub use indexes::IndexManager;
pub use keys::{StorageKey, decode_storage_key, encode_storage_key};
pub use relationship::RelationshipStore;
pub use shard::ShardManager;
pub use snapshot::{Snapshot, SnapshotManager};
pub use state::{StateError, StateLayer};
pub use tables::Tables;
pub use tiered_storage::{
    LocalBackend, ObjectStorageBackend, StorageBackend, StorageTier, TieredConfig,
    TieredSnapshotManager, TieredStorageError,
};
pub use time_travel::{
    TimeTravelConfig, TimeTravelEntry, TimeTravelError, TimeTravelIndex, TimeTravelStats,
};
