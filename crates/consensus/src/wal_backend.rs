//! WAL backend trait for injectable persistence.
//! Implementations provided in Phase 3.

use std::sync::Arc;

use crate::types::ShardId;

/// Sentinel shard ID used to mark checkpoint frames in the WAL.
///
/// No real shard uses `u64::MAX`, so this value is safe as a reserved marker.
pub const CHECKPOINT_SHARD_ID: ShardId = ShardId(u64::MAX);

/// A checkpoint frame recording the committed index at a point in time.
///
/// Written by the reactor after each flush cycle. On recovery, the last
/// checkpoint determines which entries need replay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointFrame {
    /// The committed log index at checkpoint time.
    pub committed_index: u64,
    /// The Raft term at checkpoint time.
    pub term: u64,
}

impl CheckpointFrame {
    /// Encodes the checkpoint as 16 bytes: `[committed_index:8LE][term:8LE]`.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(16);
        buf.extend_from_slice(&self.committed_index.to_le_bytes());
        buf.extend_from_slice(&self.term.to_le_bytes());
        buf
    }

    /// Decodes a checkpoint from 16 bytes. Returns `None` if the buffer is
    /// too short.
    pub fn decode(data: &[u8]) -> Option<Self> {
        if data.len() < 16 {
            return None;
        }
        let committed_index = u64::from_le_bytes(data[..8].try_into().ok()?);
        let term = u64::from_le_bytes(data[8..16].try_into().ok()?);
        Some(Self { committed_index, term })
    }
}

/// A single WAL frame ready for persistence.
#[derive(Clone, Debug)]
pub struct WalFrame {
    /// Shard this frame belongs to.
    pub shard_id: ShardId,
    /// Log index of this entry (0 for checkpoint frames).
    pub index: u64,
    /// Raft term of this entry (0 for checkpoint frames).
    pub term: u64,
    /// Raw entry bytes (shared via Arc to avoid copies on WAL frame creation).
    pub data: Arc<[u8]>,
}

/// Abstraction over WAL persistence.
pub trait WalBackend: Send + Sync + 'static {
    /// Appends frames to the WAL (no fsync yet).
    fn append(&mut self, frames: &[WalFrame]) -> Result<(), WalError>;
    /// Fsyncs all appended frames.
    fn sync(&mut self) -> Result<(), WalError>;
    /// Reads frames from a given offset.
    fn read_frames(&self, from_offset: u64) -> Result<Vec<WalFrame>, WalError>;
    /// Truncates frames before the given offset.
    fn truncate_before(&mut self, offset: u64) -> Result<(), WalError>;

    /// Writes a commit checkpoint frame to the WAL.
    ///
    /// The checkpoint is encoded as a regular [`WalFrame`] with
    /// [`CHECKPOINT_SHARD_ID`] as the shard identifier. It is appended without
    /// an immediate fsync — the checkpoint becomes durable with the next
    /// [`WalBackend::sync`] call.
    fn write_checkpoint(&mut self, checkpoint: &CheckpointFrame) -> Result<(), WalError> {
        let frame = WalFrame {
            shard_id: CHECKPOINT_SHARD_ID,
            index: 0,
            term: 0,
            data: Arc::from(checkpoint.encode().as_slice()),
        };
        self.append(&[frame])
    }

    /// Zeros out all WAL frames for a specific shard.
    ///
    /// Called during crypto-shredding after the vault's DEK is destroyed.
    /// The frame headers (shard_id, index, term, length) are preserved
    /// but the data payload is overwritten with zeros.
    ///
    /// Returns the number of frames zeroed.
    fn shred_frames(&mut self, shard_id: ShardId) -> Result<u64, WalError>;

    /// Returns whether this backend supports async fsync (io_uring).
    ///
    /// When `true`, the reactor uses the non-blocking fsync path: it calls
    /// [`submit_async_fsync`](WalBackend::submit_async_fsync) and polls
    /// [`poll_fsync_completion`](WalBackend::poll_fsync_completion) before
    /// resolving responses. Synchronous backends always return `false`.
    fn supports_async_fsync(&self) -> bool {
        false
    }

    /// Submits an async fsync request. Returns immediately without blocking.
    ///
    /// Call [`poll_fsync_completion`](WalBackend::poll_fsync_completion) to
    /// check whether the fsync has completed. For synchronous backends, the
    /// default implementation falls back to [`WalBackend::sync`].
    ///
    /// Only meaningful when [`supports_async_fsync`](WalBackend::supports_async_fsync)
    /// returns `true`.
    ///
    /// # Errors
    ///
    /// Returns [`WalError`] if submitting or (for sync fallback) executing the
    /// fsync fails.
    fn submit_async_fsync(&mut self) -> Result<(), WalError> {
        self.sync()
    }

    /// Polls for async fsync completion.
    ///
    /// Returns `true` if the previously submitted fsync has completed.
    /// Synchronous backends always return `true` immediately because
    /// [`submit_async_fsync`](WalBackend::submit_async_fsync) blocks until
    /// the fsync is durable.
    fn poll_fsync_completion(&self) -> bool {
        true
    }

    /// Reads the most recent checkpoint by scanning frames in reverse.
    ///
    /// Returns `None` if no checkpoint has been written yet. The default
    /// implementation reads all frames and scans backward; backends may
    /// override for efficiency.
    fn last_checkpoint(&self) -> Result<Option<CheckpointFrame>, WalError> {
        let frames = self.read_frames(0)?;
        for frame in frames.iter().rev() {
            if frame.shard_id == CHECKPOINT_SHARD_ID
                && let Some(cp) = CheckpointFrame::decode(&frame.data)
            {
                return Ok(Some(cp));
            }
        }
        Ok(None)
    }
}

/// Tracks the async fsync lifecycle in the reactor.
///
/// Used by the reactor to decide when to resolve pending proposal responses.
/// Synchronous backends always complete in the `Submitted` step and return
/// immediately to `Idle`. Async backends (io_uring) stay in `Submitted` until
/// [`WalBackend::poll_fsync_completion`] returns `true`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsyncPhase {
    /// No fsync pending — ready for new writes.
    Idle,
    /// Entries written to WAL buffer, fsync submitted (or completed synchronously).
    Submitted,
}

/// WAL operation errors.
#[derive(Debug, snafu::Snafu)]
pub enum WalError {
    /// WAL I/O error.
    #[snafu(display("WAL I/O error ({kind:?}): {message}"))]
    Io {
        /// The I/O error kind.
        kind: std::io::ErrorKind,
        /// Description of the I/O error.
        message: String,
    },
    /// WAL frame corruption detected.
    #[snafu(display("WAL frame corrupted at offset {offset}"))]
    Corrupted {
        /// Byte offset where corruption was detected.
        offset: u64,
    },
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    /// Minimal backend that only implements required methods, exercising
    /// all default trait method bodies.
    struct DefaultsOnlyBackend {
        frames: Vec<WalFrame>,
    }

    impl DefaultsOnlyBackend {
        fn new() -> Self {
            Self { frames: Vec::new() }
        }
    }

    impl WalBackend for DefaultsOnlyBackend {
        fn append(&mut self, frames: &[WalFrame]) -> Result<(), WalError> {
            self.frames.extend(frames.iter().cloned());
            Ok(())
        }

        fn sync(&mut self) -> Result<(), WalError> {
            Ok(())
        }

        fn read_frames(&self, from_offset: u64) -> Result<Vec<WalFrame>, WalError> {
            let offset = from_offset as usize;
            if offset >= self.frames.len() {
                return Ok(Vec::new());
            }
            Ok(self.frames[offset..].to_vec())
        }

        fn truncate_before(&mut self, offset: u64) -> Result<(), WalError> {
            let offset = offset as usize;
            if offset >= self.frames.len() {
                self.frames.clear();
            } else {
                self.frames.drain(..offset);
            }
            Ok(())
        }

        fn shred_frames(&mut self, shard_id: ShardId) -> Result<u64, WalError> {
            let mut count = 0u64;
            for frame in &mut self.frames {
                if frame.shard_id == shard_id {
                    frame.data = vec![0u8; frame.data.len()].into();
                    count += 1;
                }
            }
            Ok(count)
        }
    }

    // ── Default: write_checkpoint ─────────────────────────────────

    #[test]
    fn test_default_write_checkpoint_appends_checkpoint_frame() {
        let mut wal = DefaultsOnlyBackend::new();

        let cp = CheckpointFrame { committed_index: 10, term: 2 };

        wal.write_checkpoint(&cp).unwrap();

        assert_eq!(wal.frames.len(), 1);
        assert_eq!(wal.frames[0].shard_id, CHECKPOINT_SHARD_ID);
        assert_eq!(wal.frames[0].index, 0);
        assert_eq!(wal.frames[0].term, 0);
    }

    // ── Default: supports_async_fsync ─────────────────────────────

    #[test]
    fn test_default_supports_async_fsync_returns_false() {
        let wal = DefaultsOnlyBackend::new();

        assert!(!wal.supports_async_fsync());
    }

    // ── Default: submit_async_fsync ───────────────────────────────

    #[test]
    fn test_default_submit_async_fsync_delegates_to_sync() {
        let mut wal = DefaultsOnlyBackend::new();

        // submit_async_fsync default calls self.sync().
        let result = wal.submit_async_fsync();

        assert!(result.is_ok());
    }

    // ── Default: poll_fsync_completion ─────────────────────────────

    #[test]
    fn test_default_poll_fsync_completion_returns_true() {
        let wal = DefaultsOnlyBackend::new();

        assert!(wal.poll_fsync_completion());
    }

    // ── Default: last_checkpoint ──────────────────────────────────

    #[test]
    fn test_default_last_checkpoint_none_when_empty() {
        let wal = DefaultsOnlyBackend::new();

        assert!(wal.last_checkpoint().unwrap().is_none());
    }

    #[test]
    fn test_default_last_checkpoint_returns_most_recent() {
        let mut wal = DefaultsOnlyBackend::new();

        // Write two checkpoints interleaved with a regular frame.
        wal.write_checkpoint(&CheckpointFrame { committed_index: 3, term: 1 }).unwrap();
        wal.append(&[WalFrame {
            shard_id: ShardId(1),
            index: 4,
            term: 1,
            data: Arc::from(b"entry" as &[u8]),
        }])
        .unwrap();
        wal.write_checkpoint(&CheckpointFrame { committed_index: 7, term: 2 }).unwrap();

        let cp = wal.last_checkpoint().unwrap().expect("should find a checkpoint");

        assert_eq!(cp.committed_index, 7);
        assert_eq!(cp.term, 2);
    }

    #[test]
    fn test_default_last_checkpoint_ignores_truncated_data() {
        let mut wal = DefaultsOnlyBackend::new();

        // Append a frame with CHECKPOINT_SHARD_ID but too-short data (< 16 bytes).
        wal.frames.push(WalFrame {
            shard_id: CHECKPOINT_SHARD_ID,
            index: 0,
            term: 0,
            data: Arc::from(b"short" as &[u8]),
        });

        assert!(
            wal.last_checkpoint().unwrap().is_none(),
            "truncated checkpoint data should be skipped"
        );
    }

    // ── CheckpointFrame encode/decode ─────────────────────────────

    #[test]
    fn test_checkpoint_frame_roundtrip() {
        let cases: &[(u64, u64)] = &[(0, 0), (1, 1), (u64::MAX, u64::MAX), (42, 7)];

        for &(committed_index, term) in cases {
            let cp = CheckpointFrame { committed_index, term };

            let encoded = cp.encode();
            let decoded =
                CheckpointFrame::decode(&encoded).expect("should decode valid checkpoint");

            assert_eq!(decoded.committed_index, committed_index);
            assert_eq!(decoded.term, term);
        }
    }

    #[test]
    fn test_checkpoint_frame_decode_rejects_short_data() {
        let cases: &[&[u8]] = &[b"", b"short", &[0u8; 15]];

        for data in cases {
            assert!(
                CheckpointFrame::decode(data).is_none(),
                "should reject {}-byte data",
                data.len()
            );
        }
    }

    // ── WalError display ──────────────────────────────────────────

    #[test]
    fn test_wal_error_io_display() {
        let err =
            WalError::Io { kind: std::io::ErrorKind::Other, message: "disk full".to_string() };

        assert_eq!(err.to_string(), "WAL I/O error (Other): disk full");
    }

    #[test]
    fn test_wal_error_corrupted_display() {
        let err = WalError::Corrupted { offset: 1024 };

        assert_eq!(err.to_string(), "WAL frame corrupted at offset 1024");
    }
}
