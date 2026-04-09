//! State machine trait for consensus engine integration.
//!
//! The [`StateMachine`] trait defines how the application layer integrates with
//! the consensus engine. Implementations receive committed log entries and manage
//! snapshot lifecycle.

use std::path::{Path, PathBuf};

/// Result of applying a single entry.
#[derive(Debug, Clone)]
pub enum ApplyResult {
    /// The entry was applied successfully.
    Ok,
    /// The entry was a no-op (e.g., already applied or empty).
    Noop,
}

/// Snapshot operation errors.
#[derive(Debug, snafu::Snafu)]
pub enum SnapshotError {
    /// An I/O error occurred during a snapshot operation.
    #[snafu(display("Snapshot I/O error: {message}"))]
    Io {
        /// Description of the I/O failure.
        message: String,
    },
}

/// Interface between the consensus engine and application state.
///
/// Implementations apply committed log entries to persistent state and
/// handle snapshot creation and restoration for catch-up and recovery.
pub trait StateMachine: Send + Sync + 'static {
    /// Apply committed entries. Each entry is raw bytes.
    fn apply(&mut self, entries: &[Vec<u8>]) -> Vec<ApplyResult>;

    /// The last applied log index.
    fn last_applied(&self) -> u64;

    /// Create a snapshot. Returns path to consistent state file.
    fn snapshot(&self) -> Result<PathBuf, SnapshotError>;

    /// Restore state from a snapshot file.
    fn restore(&mut self, path: &Path) -> Result<(), SnapshotError>;

    /// Flush dirty pages to durable storage (without fsync).
    ///
    /// Called by the apply worker after mutations are complete. Implementations
    /// that buffer writes to a page cache should flush pending pages here.
    fn flush_dirty_pages(&mut self) -> Result<(), SnapshotError> {
        Ok(())
    }

    /// Compute the state root hash for a vault.
    ///
    /// Returns `None` if state roots are not supported by this implementation.
    /// When supported, the hash covers all live entries in the vault at the
    /// current applied index and can be used for integrity verification.
    fn state_root(&self, _vault_id: u64) -> Option<[u8; 32]> {
        None
    }

    /// Validate that an operation is permitted before it enters consensus.
    ///
    /// Called before proposing to consensus (pre-proposal access control).
    /// Implementations may inspect `data` to check shard routing, region
    /// affinity, or authorization. Returns `Ok(())` if the proposal is
    /// accepted, or `Err(reason)` to reject it with a description.
    fn validate_access(&self, _data: &[u8]) -> Result<(), String> {
        Ok(())
    }
}

/// A no-op state machine for testing.
///
/// Counts applied entries but does not persist any state. Snapshot and
/// restore operations always return an error.
#[derive(Debug, Default)]
pub struct NoopStateMachine {
    applied_count: u64,
}

impl NoopStateMachine {
    /// Create a new `NoopStateMachine` with zero applied entries.
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of entries applied so far.
    pub fn applied_count(&self) -> u64 {
        self.applied_count
    }
}

impl StateMachine for NoopStateMachine {
    fn apply(&mut self, entries: &[Vec<u8>]) -> Vec<ApplyResult> {
        self.applied_count += entries.len() as u64;
        entries.iter().map(|_| ApplyResult::Ok).collect()
    }

    fn last_applied(&self) -> u64 {
        self.applied_count
    }

    fn snapshot(&self) -> Result<PathBuf, SnapshotError> {
        Err(SnapshotError::Io { message: "NoopStateMachine does not support snapshots".into() })
    }

    fn restore(&mut self, _path: &Path) -> Result<(), SnapshotError> {
        Err(SnapshotError::Io { message: "NoopStateMachine does not support restore".into() })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn noop_starts_at_zero() {
        let sm = NoopStateMachine::new();
        assert_eq!(sm.applied_count(), 0);
        assert_eq!(sm.last_applied(), 0);
    }

    #[test]
    fn noop_apply_counts_entries_and_returns_ok_per_entry() {
        let mut sm = NoopStateMachine::new();
        let results = sm.apply(&[b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);

        assert_eq!(results.len(), 3);
        assert!(results.iter().all(|r| matches!(r, ApplyResult::Ok)));
        assert_eq!(sm.applied_count(), 3);
        assert_eq!(sm.last_applied(), 3);
    }

    #[test]
    fn noop_apply_accumulates_across_calls() {
        let mut sm = NoopStateMachine::new();
        sm.apply(&[b"x".to_vec()]);
        sm.apply(&[b"y".to_vec(), b"z".to_vec()]);
        assert_eq!(sm.applied_count(), 3);
        assert_eq!(sm.last_applied(), 3);
    }

    #[test]
    fn noop_apply_empty_batch() {
        let mut sm = NoopStateMachine::new();
        let results = sm.apply(&[]);
        assert!(results.is_empty());
        assert_eq!(sm.applied_count(), 0);
    }

    #[test]
    fn noop_snapshot_returns_error() {
        let sm = NoopStateMachine::new();
        let err = sm.snapshot().unwrap_err();
        assert!(err.to_string().contains("snapshot"), "error message: {err}");
    }

    #[test]
    fn noop_restore_returns_error() {
        let mut sm = NoopStateMachine::new();
        let err = sm.restore(std::path::Path::new("/fake")).unwrap_err();
        assert!(err.to_string().contains("restore"), "error message: {err}");
    }

    #[test]
    fn noop_flush_dirty_pages_returns_ok() {
        let mut sm = NoopStateMachine::new();
        assert!(sm.flush_dirty_pages().is_ok());
    }

    #[test]
    fn noop_state_root_returns_none() {
        let sm = NoopStateMachine::new();
        assert!(sm.state_root(0).is_none());
        assert!(sm.state_root(1).is_none());
        assert!(sm.state_root(u64::MAX).is_none());
    }

    #[test]
    fn noop_validate_access_accepts_all() {
        let sm = NoopStateMachine::new();
        assert!(sm.validate_access(b"").is_ok());
        assert!(sm.validate_access(b"any payload").is_ok());
    }

    #[test]
    fn snapshot_error_display() {
        let err = SnapshotError::Io { message: "disk full".into() };
        assert_eq!(err.to_string(), "Snapshot I/O error: disk full");
    }
}
