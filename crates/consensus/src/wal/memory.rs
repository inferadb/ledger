//! In-memory WAL backend for testing and simulation.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use crate::{
    types::ShardId,
    wal_backend::{CHECKPOINT_SHARD_ID, CheckpointFrame, WalBackend, WalError, WalFrame},
};

/// An in-memory WAL backend that separates pending (unsynced) from durable
/// (synced) frames. Supports fault injection for testing sync and append failures.
pub struct InMemoryWalBackend {
    /// Frames that have been appended but not yet synced.
    pending: Vec<WalFrame>,
    /// Frames that have been synced (durable).
    durable: Vec<WalFrame>,
    /// When set, the next `sync` call will fail with this error message
    /// and the flag will be cleared.
    inject_sync_error: Option<String>,
    /// Number of successful sync operations.
    sync_count: u64,
    /// When `true`, the next `append` call will return an error.
    /// Uses `AtomicBool` so callers can inject errors via `&self`
    /// without requiring mutable access.
    inject_append_error: AtomicBool,
}

impl InMemoryWalBackend {
    /// Creates a new empty in-memory WAL backend.
    pub fn new() -> Self {
        Self {
            pending: Vec::new(),
            durable: Vec::new(),
            inject_sync_error: None,
            sync_count: 0,
            inject_append_error: AtomicBool::new(false),
        }
    }

    /// Injects a sync error that will trigger on the next `sync` call.
    /// The error is consumed on use — subsequent syncs succeed unless
    /// another error is injected.
    pub fn inject_sync_error(&mut self, message: impl Into<String>) {
        self.inject_sync_error = Some(message.into());
    }

    /// Causes the next `append` call to return an error when `fail` is `true`.
    /// Pass `false` to restore normal behavior.
    ///
    /// Uses `AtomicBool` internally so this can be called via shared reference,
    /// which is useful when the reactor holds mutable ownership of the backend.
    pub fn inject_append_error(&self, fail: bool) {
        self.inject_append_error.store(fail, Ordering::Release);
    }

    /// Returns the number of successful sync operations performed.
    pub fn sync_count(&self) -> u64 {
        self.sync_count
    }

    /// Returns the number of pending (unsynced) frames.
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// Returns the number of durable (synced) frames.
    pub fn durable_count(&self) -> usize {
        self.durable.len()
    }
}

impl Default for InMemoryWalBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl WalBackend for InMemoryWalBackend {
    fn append(&mut self, frames: &[WalFrame]) -> Result<(), WalError> {
        if self.inject_append_error.load(Ordering::Acquire) {
            return Err(WalError::Io {
                kind: std::io::ErrorKind::Other,
                message: "injected append error".to_string(),
            });
        }
        self.pending.extend(frames.iter().cloned());
        Ok(())
    }

    fn sync(&mut self) -> Result<(), WalError> {
        if let Some(message) = self.inject_sync_error.take() {
            return Err(WalError::Io { kind: std::io::ErrorKind::Other, message });
        }
        self.durable.append(&mut self.pending);
        self.sync_count += 1;
        Ok(())
    }

    fn read_frames(&self, from_offset: u64) -> Result<Vec<WalFrame>, WalError> {
        let offset = from_offset as usize;
        if offset >= self.durable.len() {
            return Ok(Vec::new());
        }
        Ok(self.durable[offset..].to_vec())
    }

    fn shred_frames(&mut self, shard_id: ShardId) -> Result<u64, WalError> {
        let mut count = 0u64;
        for frame in &mut self.pending {
            if frame.shard_id == shard_id {
                frame.data = vec![0u8; frame.data.len()].into();
                count += 1;
            }
        }
        for frame in &mut self.durable {
            if frame.shard_id == shard_id {
                frame.data = vec![0u8; frame.data.len()].into();
                count += 1;
            }
        }
        Ok(count)
    }

    fn truncate_before(&mut self, offset: u64) -> Result<(), WalError> {
        let offset = offset as usize;
        if offset >= self.durable.len() {
            self.durable.clear();
        } else {
            self.durable.drain(..offset);
        }
        Ok(())
    }

    fn write_checkpoint(&mut self, checkpoint: &CheckpointFrame) -> Result<(), WalError> {
        let frame = WalFrame {
            shard_id: CHECKPOINT_SHARD_ID,
            index: 0,
            term: 0,
            data: Arc::from(checkpoint.encode().as_slice()),
        };
        self.append(&[frame])
    }

    fn last_checkpoint(&self) -> Result<Option<CheckpointFrame>, WalError> {
        for frame in self.durable.iter().rev() {
            if frame.shard_id == CHECKPOINT_SHARD_ID
                && let Some(cp) = CheckpointFrame::decode(&frame.data)
            {
                return Ok(Some(cp));
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::disallowed_methods)]
mod tests {
    use super::*;
    use crate::types::ShardId;

    fn frame(shard: u64, data: &[u8]) -> WalFrame {
        WalFrame { shard_id: ShardId(shard), index: 0, term: 0, data: Arc::from(data) }
    }

    // --- append / sync / read_frames ---

    #[test]
    fn test_append_frames_are_pending_until_sync() {
        let mut wal = InMemoryWalBackend::new();

        wal.append(&[frame(1, b"a"), frame(1, b"b")]).unwrap();

        assert_eq!(wal.pending_count(), 2);
        assert_eq!(wal.durable_count(), 0);
        assert!(wal.read_frames(0).unwrap().is_empty());
    }

    #[test]
    fn test_sync_moves_pending_to_durable() {
        let mut wal = InMemoryWalBackend::new();

        wal.append(&[frame(1, b"a"), frame(1, b"b")]).unwrap();
        wal.sync().unwrap();

        assert_eq!(wal.pending_count(), 0);
        assert_eq!(wal.durable_count(), 2);
        assert_eq!(wal.sync_count(), 1);

        let frames = wal.read_frames(0).unwrap();
        assert_eq!(frames.len(), 2);
        assert_eq!(&*frames[0].data, b"a");
        assert_eq!(&*frames[1].data, b"b");
    }

    #[test]
    fn test_append_empty_batch_succeeds() {
        let mut wal = InMemoryWalBackend::new();

        wal.append(&[]).unwrap();

        assert_eq!(wal.pending_count(), 0);
        assert_eq!(wal.durable_count(), 0);
    }

    #[test]
    fn test_read_frames_from_offset_returns_suffix() {
        let mut wal = InMemoryWalBackend::new();
        wal.append(&[frame(1, b"a"), frame(1, b"b"), frame(2, b"c")]).unwrap();
        wal.sync().unwrap();

        let cases: &[(u64, &[&[u8]])] =
            &[(0, &[b"a", b"b", b"c"]), (1, &[b"b", b"c"]), (2, &[b"c"]), (3, &[]), (100, &[])];
        for &(offset, expected) in cases {
            let frames = wal.read_frames(offset).unwrap();
            assert_eq!(frames.len(), expected.len(), "offset={offset}");
            for (i, exp) in expected.iter().enumerate() {
                assert_eq!(&*frames[i].data, *exp, "offset={offset}, i={i}");
            }
        }
    }

    // --- truncate_before ---

    #[test]
    fn test_truncate_before_removes_prefix() {
        let mut wal = InMemoryWalBackend::new();
        wal.append(&[frame(1, b"a"), frame(1, b"b"), frame(2, b"c")]).unwrap();
        wal.sync().unwrap();

        wal.truncate_before(1).unwrap();

        assert_eq!(wal.durable_count(), 2);
        let frames = wal.read_frames(0).unwrap();
        assert_eq!(&*frames[0].data, b"b");
        assert_eq!(&*frames[1].data, b"c");
    }

    #[test]
    fn test_truncate_before_beyond_length_clears_all() {
        let mut wal = InMemoryWalBackend::new();
        wal.append(&[frame(1, b"a")]).unwrap();
        wal.sync().unwrap();

        wal.truncate_before(100).unwrap();

        assert_eq!(wal.durable_count(), 0);
    }

    // --- inject_sync_error ---

    #[test]
    fn test_inject_sync_error_fails_then_recovers() {
        let mut wal = InMemoryWalBackend::new();
        wal.append(&[frame(1, b"a")]).unwrap();
        wal.inject_sync_error("disk full");

        // First sync fails.
        let err = wal.sync().unwrap_err();
        assert!(err.to_string().contains("disk full"));
        assert_eq!(wal.pending_count(), 1);
        assert_eq!(wal.durable_count(), 0);
        assert_eq!(wal.sync_count(), 0);

        // Error consumed: next sync succeeds.
        wal.sync().unwrap();
        assert_eq!(wal.pending_count(), 0);
        assert_eq!(wal.durable_count(), 1);
        assert_eq!(wal.sync_count(), 1);
    }

    // --- inject_append_error ---

    #[test]
    fn test_inject_append_error_fails_then_recovers() {
        let mut wal = InMemoryWalBackend::new();

        wal.inject_append_error(true);

        let err = wal.append(&[frame(1, b"a")]).unwrap_err();
        assert!(err.to_string().contains("injected append error"));
        assert_eq!(wal.pending_count(), 0, "no frames should be appended on error");

        // Clear the error flag — next append succeeds.
        wal.inject_append_error(false);

        wal.append(&[frame(1, b"b")]).unwrap();
        assert_eq!(wal.pending_count(), 1);
        assert_eq!(&*wal.pending[0].data, b"b");
    }

    #[test]
    fn test_inject_append_error_via_shared_reference() {
        let mut wal = InMemoryWalBackend::new();

        // Inject via &self (shared reference).
        let wal_ref: &InMemoryWalBackend = &wal;
        wal_ref.inject_append_error(true);

        let err = wal.append(&[frame(1, b"a")]).unwrap_err();
        assert!(err.to_string().contains("injected append error"));
    }

    // --- checkpoint ---

    #[test]
    fn test_checkpoint_write_then_read_roundtrips() {
        let mut wal = InMemoryWalBackend::new();

        let cp = CheckpointFrame { committed_index: 42, term: 3 };
        wal.write_checkpoint(&cp).unwrap();
        wal.sync().unwrap();

        assert_eq!(wal.last_checkpoint().unwrap(), Some(cp));
    }

    #[test]
    fn test_checkpoint_multiple_writes_returns_last() {
        let mut wal = InMemoryWalBackend::new();

        wal.write_checkpoint(&CheckpointFrame { committed_index: 1, term: 1 }).unwrap();
        wal.write_checkpoint(&CheckpointFrame { committed_index: 5, term: 2 }).unwrap();
        wal.write_checkpoint(&CheckpointFrame { committed_index: 10, term: 2 }).unwrap();
        wal.sync().unwrap();

        assert_eq!(
            wal.last_checkpoint().unwrap(),
            Some(CheckpointFrame { committed_index: 10, term: 2 })
        );
    }

    #[test]
    fn test_checkpoint_none_when_no_checkpoint_written() {
        let wal = InMemoryWalBackend::new();

        assert!(wal.last_checkpoint().unwrap().is_none());
    }

    #[test]
    fn test_checkpoint_interleaved_with_entries_returns_last() {
        let mut wal = InMemoryWalBackend::new();

        wal.append(&[frame(1, b"entry-1"), frame(2, b"entry-2")]).unwrap();
        wal.write_checkpoint(&CheckpointFrame { committed_index: 2, term: 1 }).unwrap();
        wal.append(&[frame(1, b"entry-3")]).unwrap();
        wal.write_checkpoint(&CheckpointFrame { committed_index: 3, term: 1 }).unwrap();
        wal.sync().unwrap();

        assert_eq!(
            wal.last_checkpoint().unwrap(),
            Some(CheckpointFrame { committed_index: 3, term: 1 })
        );

        // Regular frames are still all readable.
        let entry_count = wal
            .read_frames(0)
            .unwrap()
            .iter()
            .filter(|f| f.shard_id != CHECKPOINT_SHARD_ID)
            .count();
        assert_eq!(entry_count, 3);
    }

    #[test]
    fn test_checkpoint_unsync_not_visible() {
        let mut wal = InMemoryWalBackend::new();

        wal.write_checkpoint(&CheckpointFrame { committed_index: 10, term: 1 }).unwrap();
        // Checkpoint is pending (not synced), so last_checkpoint scans durable only.
        assert!(wal.last_checkpoint().unwrap().is_none());
    }

    // --- shred_frames ---

    #[test]
    fn test_shred_frames_zeros_matching_shard_preserves_others() {
        let mut wal = InMemoryWalBackend::new();
        wal.append(&[frame(1, b"shard-one-a"), frame(1, b"shard-one-b"), frame(1, b"shard-one-c")])
            .unwrap();
        wal.append(&[frame(2, b"shard-two-a"), frame(2, b"shard-two-b")]).unwrap();
        wal.sync().unwrap();

        let zeroed = wal.shred_frames(ShardId(1)).unwrap();
        assert_eq!(zeroed, 3);

        let frames = wal.read_frames(0).unwrap();
        for f in frames.iter().filter(|f| f.shard_id == ShardId(1)) {
            assert!(f.data.iter().all(|&b| b == 0), "shard 1 frame not zeroed");
        }
        let shard2: Vec<_> = frames.iter().filter(|f| f.shard_id == ShardId(2)).collect();
        assert_eq!(&*shard2[0].data, b"shard-two-a");
        assert_eq!(&*shard2[1].data, b"shard-two-b");
    }

    #[test]
    fn test_shred_frames_zeros_both_pending_and_durable() {
        let mut wal = InMemoryWalBackend::new();

        wal.append(&[frame(1, b"durable")]).unwrap();
        wal.sync().unwrap();
        wal.append(&[frame(1, b"pending")]).unwrap();

        let zeroed = wal.shred_frames(ShardId(1)).unwrap();
        assert_eq!(zeroed, 2);

        // Durable frame zeroed.
        assert!(wal.read_frames(0).unwrap()[0].data.iter().all(|&b| b == 0));

        // Sync pending, then verify both zeroed.
        wal.sync().unwrap();
        let all_frames = wal.read_frames(0).unwrap();
        assert_eq!(all_frames.len(), 2);
        for f in &all_frames {
            assert!(f.data.iter().all(|&b| b == 0));
        }
    }

    #[test]
    fn test_shred_frames_unknown_shard_returns_zero_preserves_data() {
        let mut wal = InMemoryWalBackend::new();
        wal.append(&[frame(1, b"data")]).unwrap();
        wal.sync().unwrap();

        assert_eq!(wal.shred_frames(ShardId(99)).unwrap(), 0);
        assert_eq!(&*wal.read_frames(0).unwrap()[0].data, b"data");
    }
}
