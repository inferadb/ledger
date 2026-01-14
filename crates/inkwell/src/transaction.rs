//! Transaction tracking and snapshot management for Copy-on-Write semantics.
//!
//! This module provides the infrastructure for MVCC-style snapshot isolation:
//! - `SnapshotId`: Unique identifier for a committed database state
//! - `CommittedState`: The visible state (table roots) at a snapshot
//! - `TransactionTracker`: Reference counting for safe page deallocation

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;

use crate::error::PageId;
use crate::tables::TableId;

/// Unique identifier for a committed database snapshot.
///
/// Each successful commit increments the snapshot ID. Read transactions
/// capture the current snapshot ID to ensure they see a consistent view.
#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Debug, Default)]
pub struct SnapshotId(pub u64);

impl SnapshotId {
    /// Create a new snapshot ID with the given value.
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    /// Get the raw u64 value.
    pub fn raw(&self) -> u64 {
        self.0
    }

    /// Increment and return the next snapshot ID.
    pub fn next(&self) -> Self {
        Self(self.0 + 1)
    }
}

/// The committed state that readers snapshot.
///
/// Contains the root page IDs for all tables at a specific point in time.
/// This is what gets atomically swapped on commit.
#[derive(Clone, Debug)]
pub struct CommittedState {
    /// Root page IDs for each table (0 means empty table).
    pub table_roots: [PageId; TableId::COUNT],
    /// The snapshot ID for this committed state.
    pub snapshot_id: SnapshotId,
}

impl Default for CommittedState {
    fn default() -> Self {
        Self {
            table_roots: [0; TableId::COUNT],
            snapshot_id: SnapshotId::default(),
        }
    }
}

/// Tracks active transactions for safe page deallocation.
///
/// This is the key to Copy-on-Write: we can't free old pages until
/// all readers that might reference them have finished.
///
/// # How it works
///
/// 1. When a read transaction starts, it registers with its snapshot ID
/// 2. The tracker maintains a reference count for each snapshot ID
/// 3. When a read transaction ends, it decrements the count
/// 4. Pages freed by a commit can only be deallocated when no readers
///    reference that snapshot ID or earlier
///
/// # Example
///
/// ```text
/// Time    Action                          live_readers
/// ----    ------                          ------------
/// T1      Commit snapshot 1               {}
/// T2      Read starts (snapshot 1)        {1: 1}
/// T3      Commit snapshot 2               {1: 1}
/// T4      Read starts (snapshot 2)        {1: 1, 2: 1}
/// T5      First read ends                 {2: 1}
/// T6      Pages from snapshot 1 freed     {2: 1}
/// ```
pub struct TransactionTracker {
    state: Mutex<TrackerState>,
    /// Next snapshot ID to assign (atomic for fast reads).
    next_snapshot_id: AtomicU64,
}

struct TrackerState {
    /// Reference count of read transactions per snapshot ID.
    live_read_transactions: BTreeMap<SnapshotId, u64>,
    /// Current write transaction snapshot ID (at most one).
    live_write_transaction: Option<SnapshotId>,
}

impl TransactionTracker {
    /// Create a new transaction tracker.
    pub fn new(initial_snapshot_id: SnapshotId) -> Self {
        Self {
            state: Mutex::new(TrackerState {
                live_read_transactions: BTreeMap::new(),
                live_write_transaction: None,
            }),
            next_snapshot_id: AtomicU64::new(initial_snapshot_id.0),
        }
    }

    /// Register a new read transaction at the given snapshot.
    ///
    /// Increments the reference count for that snapshot.
    pub fn register_read_transaction(&self, snapshot_id: SnapshotId) {
        let mut state = self.state.lock();
        state
            .live_read_transactions
            .entry(snapshot_id)
            .and_modify(|count| *count += 1)
            .or_insert(1);
    }

    /// Unregister a read transaction.
    ///
    /// Decrements the reference count. If it reaches zero, removes the entry.
    pub fn unregister_read_transaction(&self, snapshot_id: SnapshotId) {
        let mut state = self.state.lock();
        if let Some(count) = state.live_read_transactions.get_mut(&snapshot_id) {
            *count -= 1;
            if *count == 0 {
                state.live_read_transactions.remove(&snapshot_id);
            }
        }
    }

    /// Start a write transaction and return its snapshot ID.
    ///
    /// There can only be one write transaction at a time.
    /// This is enforced by the Database's write_lock mutex.
    pub fn start_write_transaction(&self) -> SnapshotId {
        let snapshot_id = SnapshotId(self.next_snapshot_id.fetch_add(1, Ordering::SeqCst));
        let mut state = self.state.lock();
        debug_assert!(
            state.live_write_transaction.is_none(),
            "Only one write transaction allowed at a time"
        );
        state.live_write_transaction = Some(snapshot_id);
        snapshot_id
    }

    /// End the current write transaction.
    pub fn end_write_transaction(&self, snapshot_id: SnapshotId) {
        let mut state = self.state.lock();
        debug_assert_eq!(
            state.live_write_transaction,
            Some(snapshot_id),
            "Ending wrong write transaction"
        );
        state.live_write_transaction = None;
    }

    /// Get the oldest snapshot ID that still has active readers.
    ///
    /// Pages freed by snapshots OLDER than this can be safely deallocated.
    /// Returns `None` if there are no active readers.
    pub fn oldest_live_read_transaction(&self) -> Option<SnapshotId> {
        let state = self.state.lock();
        state.live_read_transactions.keys().next().copied()
    }

    /// Get the current snapshot ID (without incrementing).
    pub fn current_snapshot_id(&self) -> SnapshotId {
        SnapshotId(self.next_snapshot_id.load(Ordering::SeqCst))
    }

    /// Check if there are any active read transactions.
    pub fn has_active_readers(&self) -> bool {
        let state = self.state.lock();
        !state.live_read_transactions.is_empty()
    }

    /// Get the count of active read transactions.
    #[cfg(test)]
    pub fn active_reader_count(&self) -> usize {
        let state = self.state.lock();
        state.live_read_transactions.values().sum::<u64>() as usize
    }
}

impl Default for TransactionTracker {
    fn default() -> Self {
        Self::new(SnapshotId::new(1))
    }
}

/// Tracks pages pending deallocation.
///
/// When a write transaction commits, old pages that were replaced by COW
/// are added here. They can only be freed when no reader references them.
#[derive(Default)]
pub struct PendingFrees {
    /// Maps snapshot_id -> pages freed by that commit.
    /// Pages can be freed when oldest_live_read > snapshot_id.
    freed_by_snapshot: BTreeMap<SnapshotId, Vec<PageId>>,
}

impl PendingFrees {
    /// Create a new empty pending frees tracker.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record pages to be freed from a commit.
    pub fn record_freed_pages(&mut self, snapshot_id: SnapshotId, pages: Vec<PageId>) {
        if !pages.is_empty() {
            self.freed_by_snapshot.insert(snapshot_id, pages);
        }
    }

    /// Get pages that can be safely freed.
    ///
    /// Returns pages from snapshots older than `oldest_reader`.
    /// If `oldest_reader` is `None`, all pending pages can be freed.
    pub fn drain_freeable(&mut self, oldest_reader: Option<SnapshotId>) -> Vec<PageId> {
        let cutoff = oldest_reader.unwrap_or(SnapshotId(u64::MAX));

        let to_free: Vec<SnapshotId> = self
            .freed_by_snapshot
            .range(..cutoff)
            .map(|(k, _)| *k)
            .collect();

        let mut pages = Vec::new();
        for snapshot_id in to_free {
            if let Some(mut freed) = self.freed_by_snapshot.remove(&snapshot_id) {
                pages.append(&mut freed);
            }
        }
        pages
    }

    /// Check if there are any pending frees.
    pub fn is_empty(&self) -> bool {
        self.freed_by_snapshot.is_empty()
    }

    /// Get total count of pages pending free.
    #[cfg(test)]
    pub fn pending_count(&self) -> usize {
        self.freed_by_snapshot.values().map(|v| v.len()).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snapshot_id_ordering() {
        let s1 = SnapshotId::new(1);
        let s2 = SnapshotId::new(2);
        let s3 = s2.next();

        assert!(s1 < s2);
        assert!(s2 < s3);
        assert_eq!(s3.raw(), 3);
    }

    #[test]
    fn test_tracker_read_registration() {
        let tracker = TransactionTracker::default();
        let snapshot = SnapshotId::new(1);

        assert!(!tracker.has_active_readers());

        tracker.register_read_transaction(snapshot);
        assert!(tracker.has_active_readers());
        assert_eq!(tracker.oldest_live_read_transaction(), Some(snapshot));

        tracker.unregister_read_transaction(snapshot);
        assert!(!tracker.has_active_readers());
        assert_eq!(tracker.oldest_live_read_transaction(), None);
    }

    #[test]
    fn test_tracker_multiple_readers() {
        let tracker = TransactionTracker::default();
        let s1 = SnapshotId::new(1);
        let s2 = SnapshotId::new(2);

        tracker.register_read_transaction(s1);
        tracker.register_read_transaction(s1); // Same snapshot twice
        tracker.register_read_transaction(s2);

        assert_eq!(tracker.active_reader_count(), 3);
        assert_eq!(tracker.oldest_live_read_transaction(), Some(s1));

        tracker.unregister_read_transaction(s1);
        assert_eq!(tracker.active_reader_count(), 2);
        assert_eq!(tracker.oldest_live_read_transaction(), Some(s1)); // Still has one reader

        tracker.unregister_read_transaction(s1);
        assert_eq!(tracker.active_reader_count(), 1);
        assert_eq!(tracker.oldest_live_read_transaction(), Some(s2)); // Now s2 is oldest
    }

    #[test]
    fn test_pending_frees() {
        let mut pending = PendingFrees::new();
        let s1 = SnapshotId::new(1);
        let s2 = SnapshotId::new(2);
        let s3 = SnapshotId::new(3);

        pending.record_freed_pages(s1, vec![10, 11]);
        pending.record_freed_pages(s2, vec![20, 21, 22]);
        pending.record_freed_pages(s3, vec![30]);

        assert_eq!(pending.pending_count(), 6);

        // Free pages from snapshots < s2
        let freed = pending.drain_freeable(Some(s2));
        assert_eq!(freed, vec![10, 11]);
        assert_eq!(pending.pending_count(), 4);

        // Free all remaining (no active readers)
        let freed = pending.drain_freeable(None);
        assert_eq!(freed.len(), 4);
        assert!(pending.is_empty());
    }
}
