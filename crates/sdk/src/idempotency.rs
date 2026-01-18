//! Client-side sequence tracking for idempotency.
//!
//! Maintains monotonically increasing sequence numbers per vault
//! scope for duplicate request detection and crash recovery.
//!
//! # Idempotency Model
//!
//! The server tracks the last committed sequence per `(namespace_id, vault_id, client_id)`.
//! When a write arrives:
//! - `seq == last_committed + 1`: Accept and commit
//! - `seq <= last_committed`: Duplicate, return cached response
//! - `seq > last_committed + 1`: Gap error, client must recover
//!
//! The [`SequenceTracker`] maintains client-side counters to ensure correct
//! sequence assignment and provides recovery support when gaps are detected.

use std::{collections::HashMap, sync::Arc};

use parking_lot::RwLock;

/// Key for per-vault sequence tracking.
///
/// Each `(namespace_id, vault_id)` pair has an independent sequence counter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct VaultKey {
    /// Namespace identifier.
    pub namespace_id: i64,
    /// Vault identifier within the namespace.
    pub vault_id: i64,
}

impl VaultKey {
    /// Creates a new vault key.
    #[must_use]
    pub fn new(namespace_id: i64, vault_id: i64) -> Self {
        Self { namespace_id, vault_id }
    }
}

impl From<(i64, i64)> for VaultKey {
    fn from((namespace_id, vault_id): (i64, i64)) -> Self {
        Self::new(namespace_id, vault_id)
    }
}

/// Thread-safe sequence tracker for client-side idempotency.
///
/// Maintains monotonically increasing sequence numbers per vault scope.
/// Sequences start at 1 and increment by 1 for each write operation.
///
/// # Thread Safety
///
/// All operations are thread-safe. The tracker uses a read-write lock
/// that allows concurrent reads while serializing writes.
///
/// # Example
///
/// ```
/// use inferadb_ledger_sdk::SequenceTracker;
///
/// let tracker = SequenceTracker::new("client-123");
///
/// // Get next sequence for a vault (starts at 1)
/// let seq1 = tracker.next_sequence(1, 0);
/// assert_eq!(seq1, 1);
///
/// // Subsequent calls increment the sequence
/// let seq2 = tracker.next_sequence(1, 0);
/// assert_eq!(seq2, 2);
///
/// // Different vaults have independent sequences
/// let other_vault_seq = tracker.next_sequence(1, 1);
/// assert_eq!(other_vault_seq, 1);
/// ```
#[derive(Debug)]
pub struct SequenceTracker {
    /// Client identifier used in idempotency keys.
    client_id: String,
    /// Per-vault sequence state.
    sequences: Arc<RwLock<HashMap<VaultKey, u64>>>,
}

impl SequenceTracker {
    /// Creates a new sequence tracker for the given client.
    ///
    /// # Arguments
    ///
    /// * `client_id` - Unique identifier for this client instance. Used by the server to track
    ///   per-client sequences.
    #[must_use]
    pub fn new(client_id: impl Into<String>) -> Self {
        Self { client_id: client_id.into(), sequences: Arc::new(RwLock::new(HashMap::new())) }
    }

    /// Returns the client identifier.
    #[must_use]
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Gets and increments the sequence number for a vault.
    ///
    /// Returns the next sequence number to use for a write operation.
    /// The first call for a vault returns 1.
    ///
    /// # Arguments
    ///
    /// * `namespace_id` - Namespace containing the vault.
    /// * `vault_id` - Vault within the namespace.
    ///
    /// # Returns
    ///
    /// The next sequence number (1-indexed, monotonically increasing).
    pub fn next_sequence(&self, namespace_id: i64, vault_id: i64) -> u64 {
        let key = VaultKey::new(namespace_id, vault_id);
        let mut sequences = self.sequences.write();
        let entry = sequences.entry(key).or_insert(0);
        *entry += 1;
        *entry
    }

    /// Sets the sequence number for a vault.
    ///
    /// Used during recovery to sync with the server's last committed sequence.
    /// After calling this, the next call to [`next_sequence`](Self::next_sequence)
    /// will return `seq + 1`.
    ///
    /// # Arguments
    ///
    /// * `namespace_id` - Namespace containing the vault.
    /// * `vault_id` - Vault within the namespace.
    /// * `seq` - The sequence number to set (typically from server recovery).
    pub fn set_sequence(&self, namespace_id: i64, vault_id: i64, seq: u64) {
        let key = VaultKey::new(namespace_id, vault_id);
        let mut sequences = self.sequences.write();
        sequences.insert(key, seq);
    }

    /// Gets the current sequence number for a vault without incrementing.
    ///
    /// Returns the last sequence number that was assigned, or 0 if
    /// no sequences have been assigned for this vault.
    ///
    /// # Arguments
    ///
    /// * `namespace_id` - Namespace containing the vault.
    /// * `vault_id` - Vault within the namespace.
    ///
    /// # Returns
    ///
    /// The current sequence number (0 if no writes have been made).
    #[must_use]
    pub fn current_sequence(&self, namespace_id: i64, vault_id: i64) -> u64 {
        let key = VaultKey::new(namespace_id, vault_id);
        let sequences = self.sequences.read();
        sequences.get(&key).copied().unwrap_or(0)
    }

    /// Returns the number of vaults being tracked.
    ///
    /// Useful for diagnostics and testing.
    #[must_use]
    pub fn vault_count(&self) -> usize {
        self.sequences.read().len()
    }

    /// Clears all sequence state.
    ///
    /// Use with caution - this will cause sequence gaps if writes
    /// are in progress. Primarily intended for testing.
    pub fn clear(&self) {
        self.sequences.write().clear();
    }

    /// Returns a snapshot of all current sequence state.
    ///
    /// Use this to save state for persistence.
    #[must_use]
    pub fn snapshot(&self) -> HashMap<VaultKey, u64> {
        self.sequences.read().clone()
    }

    /// Restores sequence state from a loaded snapshot.
    ///
    /// Typically called after loading state from persistence.
    pub fn restore(&self, sequences: HashMap<VaultKey, u64>) {
        let mut guard = self.sequences.write();
        guard.clear();
        guard.extend(sequences);
    }
}

impl Clone for SequenceTracker {
    fn clone(&self) -> Self {
        Self { client_id: self.client_id.clone(), sequences: Arc::clone(&self.sequences) }
    }
}

// ============================================================================
// Sequence Persistence
// ============================================================================

/// Storage format version for forward compatibility.
const STORAGE_VERSION: u32 = 1;

/// Trait for sequence state persistence.
///
/// Implementations handle loading and saving sequence state to durable storage
/// for crash recovery.
pub trait SequenceStorage: Send + Sync {
    /// Loads persisted sequence state.
    ///
    /// Returns an empty map if no state exists or if the state is corrupted.
    /// Implementations should log warnings for corrupted state.
    ///
    /// # Errors
    ///
    /// Returns an error if the storage is inaccessible (permissions, I/O).
    fn load(&self) -> crate::Result<HashMap<VaultKey, u64>>;

    /// Saves sequence state to durable storage.
    ///
    /// Must be atomic - either all state is saved or none is.
    /// Implementations should use write-temp-fsync-rename pattern.
    ///
    /// # Errors
    ///
    /// Returns an error if the save operation fails.
    fn save(&self, sequences: &HashMap<VaultKey, u64>) -> crate::Result<()>;
}

/// Persisted sequence state format.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct PersistedState {
    /// Format version for forward compatibility.
    version: u32,
    /// Client identifier for validation.
    client_id: String,
    /// Sequence state as "namespace_id:vault_id" -> sequence.
    sequences: HashMap<String, u64>,
    /// Timestamp of last save.
    updated_at: String,
}

/// File-based sequence storage with atomic writes.
///
/// Uses write-temp-fsync-rename pattern to prevent corruption:
/// 1. Write to temporary file in same directory
/// 2. Sync file to disk (fsync)
/// 3. Rename over target file (atomic on POSIX)
///
/// # Storage Location
///
/// Default: `~/.ledger/sequences/{client_id}.json`
///
/// # Example
///
/// ```no_run
/// # use inferadb_ledger_sdk::FileSequenceStorage;
/// # use std::path::PathBuf;
/// let storage = FileSequenceStorage::new("client-123", PathBuf::from("/var/lib/ledger"));
/// ```
#[derive(Debug)]
pub struct FileSequenceStorage {
    /// Client identifier for file naming and validation.
    client_id: String,
    /// Directory containing sequence files.
    storage_dir: std::path::PathBuf,
}

impl FileSequenceStorage {
    /// Creates a new file-based sequence storage.
    ///
    /// # Arguments
    ///
    /// * `client_id` - Client identifier (used in filename).
    /// * `storage_dir` - Directory for sequence files.
    #[must_use]
    pub fn new(client_id: impl Into<String>, storage_dir: impl Into<std::path::PathBuf>) -> Self {
        Self { client_id: client_id.into(), storage_dir: storage_dir.into() }
    }

    /// Creates storage using the default directory.
    ///
    /// Default: `~/.ledger/sequences/`
    ///
    /// # Errors
    ///
    /// Returns an error if the home directory cannot be determined.
    pub fn with_default_dir(client_id: impl Into<String>) -> crate::Result<Self> {
        let home = dirs::home_dir().ok_or_else(|| crate::SdkError::Config {
            message: "Cannot determine home directory".to_string(),
        })?;
        let storage_dir = home.join(".ledger").join("sequences");
        Ok(Self::new(client_id, storage_dir))
    }

    /// Returns the path to the sequence file.
    fn file_path(&self) -> std::path::PathBuf {
        self.storage_dir.join(format!("{}.json", self.client_id))
    }

    /// Parses a vault key from its string representation.
    fn parse_vault_key(s: &str) -> Option<VaultKey> {
        let parts: Vec<&str> = s.split(':').collect();
        if parts.len() != 2 {
            return None;
        }
        let namespace_id = parts[0].parse().ok()?;
        let vault_id = parts[1].parse().ok()?;
        Some(VaultKey::new(namespace_id, vault_id))
    }

    /// Formats a vault key as a string.
    fn format_vault_key(key: &VaultKey) -> String {
        format!("{}:{}", key.namespace_id, key.vault_id)
    }
}

impl SequenceStorage for FileSequenceStorage {
    fn load(&self) -> crate::Result<HashMap<VaultKey, u64>> {
        let path = self.file_path();

        // No file = fresh start
        if !path.exists() {
            return Ok(HashMap::new());
        }

        // Read and parse
        let content = std::fs::read_to_string(&path).map_err(|e| crate::SdkError::Config {
            message: format!("Failed to read sequence file: {e}"),
        })?;

        let state: PersistedState = match serde_json::from_str(&content) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(path = %path.display(), error = %e, "Corrupted sequence file, starting fresh");
                return Ok(HashMap::new());
            },
        };

        // Version check
        if state.version != STORAGE_VERSION {
            tracing::warn!(
                file_version = state.version,
                expected = STORAGE_VERSION,
                "Sequence file version mismatch, starting fresh"
            );
            return Ok(HashMap::new());
        }

        // Client ID validation
        if state.client_id != self.client_id {
            tracing::warn!(
                file_client = %state.client_id,
                expected = %self.client_id,
                "Sequence file client mismatch, starting fresh"
            );
            return Ok(HashMap::new());
        }

        // Convert string keys to VaultKey
        let mut result = HashMap::new();
        for (key_str, seq) in state.sequences {
            if let Some(key) = Self::parse_vault_key(&key_str) {
                result.insert(key, seq);
            } else {
                tracing::warn!(key = %key_str, "Invalid vault key in sequence file, skipping");
            }
        }

        Ok(result)
    }

    fn save(&self, sequences: &HashMap<VaultKey, u64>) -> crate::Result<()> {
        // Ensure directory exists
        std::fs::create_dir_all(&self.storage_dir).map_err(|e| crate::SdkError::Config {
            message: format!("Failed to create storage directory: {e}"),
        })?;

        // Build state
        let state = PersistedState {
            version: STORAGE_VERSION,
            client_id: self.client_id.clone(),
            sequences: sequences.iter().map(|(k, v)| (Self::format_vault_key(k), *v)).collect(),
            updated_at: chrono::Utc::now().to_rfc3339(),
        };

        let content = serde_json::to_string_pretty(&state).map_err(|e| {
            crate::SdkError::Config { message: format!("Failed to serialize sequence state: {e}") }
        })?;

        let path = self.file_path();
        let temp_path = path.with_extension("json.tmp");

        // Write to temp file
        {
            use std::io::Write;
            let mut file = std::fs::File::create(&temp_path).map_err(|e| {
                crate::SdkError::Config { message: format!("Failed to create temp file: {e}") }
            })?;
            file.write_all(content.as_bytes()).map_err(|e| crate::SdkError::Config {
                message: format!("Failed to write temp file: {e}"),
            })?;
            file.sync_all().map_err(|e| crate::SdkError::Config {
                message: format!("Failed to sync temp file: {e}"),
            })?;
        }

        // Atomic rename
        std::fs::rename(&temp_path, &path).map_err(|e| crate::SdkError::Config {
            message: format!("Failed to rename temp file: {e}"),
        })?;

        Ok(())
    }
}

// ============================================================================
// Persistent Sequence Tracker
// ============================================================================

/// A sequence tracker with automatic background persistence.
///
/// Wraps [`SequenceTracker`] with lazy persistence that spawns background
/// tasks to save state without blocking the hot path.
///
/// # Construction
///
/// Use [`PersistentSequenceTracker::new`] which loads existing state if available.
///
/// # Persistence Behavior
///
/// - Saves are triggered on every N increments (configurable, default 10)
/// - Saves run in the background and don't block increment operations
/// - Manual flush is available via [`flush`](Self::flush)
///
/// # Example
///
/// ```no_run
/// # use inferadb_ledger_sdk::{PersistentSequenceTracker, FileSequenceStorage};
/// # use std::path::PathBuf;
/// # fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let storage = FileSequenceStorage::new("my-client", PathBuf::from("/var/lib/ledger"));
/// let tracker = PersistentSequenceTracker::new("my-client", storage)?;
///
/// // Sequences persist automatically
/// let seq = tracker.next_sequence(1, 0);
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct PersistentSequenceTracker<S: SequenceStorage + 'static> {
    /// The underlying sequence tracker.
    tracker: SequenceTracker,
    /// Storage backend.
    storage: std::sync::Arc<S>,
    /// Number of increments since last save.
    pending_saves: std::sync::atomic::AtomicU32,
    /// Threshold for triggering background save.
    save_threshold: u32,
    /// Handle to the tokio runtime for spawning background tasks.
    runtime_handle: Option<tokio::runtime::Handle>,
}

impl<S: SequenceStorage + 'static> PersistentSequenceTracker<S> {
    /// Creates a new persistent sequence tracker, loading existing state.
    ///
    /// # Arguments
    ///
    /// * `client_id` - Client identifier.
    /// * `storage` - Storage backend for persistence.
    ///
    /// # Errors
    ///
    /// Returns an error if loading existing state fails due to I/O error
    /// (not for corrupted data, which starts fresh).
    pub fn new(client_id: impl Into<String>, storage: S) -> crate::Result<Self> {
        Self::with_threshold(client_id, storage, 10)
    }

    /// Creates a new persistent tracker with custom save threshold.
    ///
    /// # Arguments
    ///
    /// * `client_id` - Client identifier.
    /// * `storage` - Storage backend.
    /// * `save_threshold` - Number of increments before triggering background save.
    pub fn with_threshold(
        client_id: impl Into<String>,
        storage: S,
        save_threshold: u32,
    ) -> crate::Result<Self> {
        let tracker = SequenceTracker::new(client_id);

        // Load existing state
        let loaded = storage.load()?;
        if !loaded.is_empty() {
            tracker.restore(loaded);
            tracing::info!(
                client_id = %tracker.client_id(),
                vault_count = tracker.vault_count(),
                "Restored sequence state from persistence"
            );
        }

        // Try to get runtime handle for background saves
        let runtime_handle = tokio::runtime::Handle::try_current().ok();

        Ok(Self {
            tracker,
            storage: std::sync::Arc::new(storage),
            pending_saves: std::sync::atomic::AtomicU32::new(0),
            save_threshold,
            runtime_handle,
        })
    }

    /// Gets and increments the sequence number for a vault.
    ///
    /// May trigger a background save if the threshold is reached.
    pub fn next_sequence(&self, namespace_id: i64, vault_id: i64) -> u64 {
        let seq = self.tracker.next_sequence(namespace_id, vault_id);

        // Increment pending saves counter
        let pending = self.pending_saves.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;

        // Trigger background save if threshold reached
        if pending >= self.save_threshold {
            self.trigger_background_save();
        }

        seq
    }

    /// Triggers a background save operation.
    fn trigger_background_save(&self) {
        // Reset counter
        self.pending_saves.store(0, std::sync::atomic::Ordering::Relaxed);

        // Get snapshot
        let snapshot = self.tracker.snapshot();
        let storage = std::sync::Arc::clone(&self.storage);

        // Spawn background task if runtime is available
        if let Some(handle) = &self.runtime_handle {
            handle.spawn(async move {
                if let Err(e) = storage.save(&snapshot) {
                    tracing::warn!(error = %e, "Background sequence save failed");
                }
            });
        } else {
            // Fallback: synchronous save (shouldn't happen in async context)
            if let Err(e) = self.storage.save(&snapshot) {
                tracing::warn!(error = %e, "Synchronous sequence save failed");
            }
        }
    }

    /// Synchronously flushes all pending changes to storage.
    ///
    /// Call this before shutdown to ensure no data is lost.
    ///
    /// # Errors
    ///
    /// Returns an error if the save operation fails.
    pub fn flush(&self) -> crate::Result<()> {
        self.pending_saves.store(0, std::sync::atomic::Ordering::Relaxed);
        self.storage.save(&self.tracker.snapshot())
    }

    /// Returns the client identifier.
    #[must_use]
    pub fn client_id(&self) -> &str {
        self.tracker.client_id()
    }

    /// Gets the current sequence number for a vault without incrementing.
    #[must_use]
    pub fn current_sequence(&self, namespace_id: i64, vault_id: i64) -> u64 {
        self.tracker.current_sequence(namespace_id, vault_id)
    }

    /// Sets the sequence number for a vault (for recovery).
    pub fn set_sequence(&self, namespace_id: i64, vault_id: i64, seq: u64) {
        self.tracker.set_sequence(namespace_id, vault_id, seq);
    }

    /// Returns the number of vaults being tracked.
    #[must_use]
    pub fn vault_count(&self) -> usize {
        self.tracker.vault_count()
    }

    /// Returns a reference to the underlying tracker.
    #[must_use]
    pub fn inner(&self) -> &SequenceTracker {
        &self.tracker
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]
mod tests {
    use std::{
        sync::atomic::{AtomicU64, Ordering},
        thread,
    };

    use super::*;

    #[test]
    fn test_new_tracker() {
        let tracker = SequenceTracker::new("test-client");
        assert_eq!(tracker.client_id(), "test-client");
        assert_eq!(tracker.vault_count(), 0);
    }

    #[test]
    fn test_sequence_starts_at_one() {
        let tracker = SequenceTracker::new("client");
        let seq = tracker.next_sequence(1, 0);
        assert_eq!(seq, 1);
    }

    #[test]
    fn test_sequence_increments_correctly() {
        let tracker = SequenceTracker::new("client");

        assert_eq!(tracker.next_sequence(1, 0), 1);
        assert_eq!(tracker.next_sequence(1, 0), 2);
        assert_eq!(tracker.next_sequence(1, 0), 3);
        assert_eq!(tracker.next_sequence(1, 0), 4);
        assert_eq!(tracker.next_sequence(1, 0), 5);
    }

    #[test]
    fn test_different_vaults_independent_sequences() {
        let tracker = SequenceTracker::new("client");

        // Vault (1, 0) gets sequences 1, 2, 3
        assert_eq!(tracker.next_sequence(1, 0), 1);
        assert_eq!(tracker.next_sequence(1, 0), 2);
        assert_eq!(tracker.next_sequence(1, 0), 3);

        // Vault (1, 1) starts fresh at 1
        assert_eq!(tracker.next_sequence(1, 1), 1);
        assert_eq!(tracker.next_sequence(1, 1), 2);

        // Vault (2, 0) also starts fresh
        assert_eq!(tracker.next_sequence(2, 0), 1);

        // Original vault continues from 3
        assert_eq!(tracker.next_sequence(1, 0), 4);
    }

    #[test]
    fn test_current_sequence_without_increment() {
        let tracker = SequenceTracker::new("client");

        // Before any writes, current is 0
        assert_eq!(tracker.current_sequence(1, 0), 0);

        // After writes, current reflects last assigned
        tracker.next_sequence(1, 0);
        assert_eq!(tracker.current_sequence(1, 0), 1);

        tracker.next_sequence(1, 0);
        assert_eq!(tracker.current_sequence(1, 0), 2);

        // Reading current doesn't change it
        assert_eq!(tracker.current_sequence(1, 0), 2);
        assert_eq!(tracker.current_sequence(1, 0), 2);
    }

    #[test]
    fn test_set_sequence_for_recovery() {
        let tracker = SequenceTracker::new("client");

        // Simulate recovery: server says last committed was 42
        tracker.set_sequence(1, 0, 42);

        // Current shows the set value
        assert_eq!(tracker.current_sequence(1, 0), 42);

        // Next sequence continues from there
        assert_eq!(tracker.next_sequence(1, 0), 43);
        assert_eq!(tracker.next_sequence(1, 0), 44);
    }

    #[test]
    fn test_set_sequence_overwrites_existing() {
        let tracker = SequenceTracker::new("client");

        // Use some sequences
        tracker.next_sequence(1, 0);
        tracker.next_sequence(1, 0);
        assert_eq!(tracker.current_sequence(1, 0), 2);

        // Recovery overwrites to server state
        tracker.set_sequence(1, 0, 100);
        assert_eq!(tracker.current_sequence(1, 0), 100);
        assert_eq!(tracker.next_sequence(1, 0), 101);
    }

    #[test]
    fn test_vault_count() {
        let tracker = SequenceTracker::new("client");

        assert_eq!(tracker.vault_count(), 0);

        tracker.next_sequence(1, 0);
        assert_eq!(tracker.vault_count(), 1);

        tracker.next_sequence(1, 1);
        assert_eq!(tracker.vault_count(), 2);

        tracker.next_sequence(2, 0);
        assert_eq!(tracker.vault_count(), 3);

        // Same vault doesn't increase count
        tracker.next_sequence(1, 0);
        assert_eq!(tracker.vault_count(), 3);
    }

    #[test]
    fn test_clear() {
        let tracker = SequenceTracker::new("client");

        tracker.next_sequence(1, 0);
        tracker.next_sequence(1, 1);
        assert_eq!(tracker.vault_count(), 2);

        tracker.clear();
        assert_eq!(tracker.vault_count(), 0);
        assert_eq!(tracker.current_sequence(1, 0), 0);
    }

    #[test]
    fn test_clone_shares_state() {
        let tracker1 = SequenceTracker::new("client");

        tracker1.next_sequence(1, 0);
        assert_eq!(tracker1.current_sequence(1, 0), 1);

        let tracker2 = tracker1.clone();

        // Clone sees same state
        assert_eq!(tracker2.current_sequence(1, 0), 1);
        assert_eq!(tracker2.client_id(), "client");

        // Modifications via one affect the other
        tracker2.next_sequence(1, 0);
        assert_eq!(tracker1.current_sequence(1, 0), 2);
        assert_eq!(tracker2.current_sequence(1, 0), 2);
    }

    #[test]
    fn test_vault_key_from_tuple() {
        let key: VaultKey = (1, 2).into();
        assert_eq!(key.namespace_id, 1);
        assert_eq!(key.vault_id, 2);
    }

    #[test]
    fn test_thread_safety_concurrent_increments() {
        let tracker = SequenceTracker::new("client");
        let final_value = Arc::new(AtomicU64::new(0));

        const NUM_THREADS: usize = 10;
        const INCREMENTS_PER_THREAD: usize = 1000;

        let handles: Vec<_> = (0..NUM_THREADS)
            .map(|_| {
                let t = tracker.clone();
                let f = Arc::clone(&final_value);
                thread::spawn(move || {
                    for _ in 0..INCREMENTS_PER_THREAD {
                        let seq = t.next_sequence(1, 0);
                        // Track max sequence seen
                        f.fetch_max(seq, Ordering::SeqCst);
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().expect("thread panicked");
        }

        // All increments should have been applied
        let expected = (NUM_THREADS * INCREMENTS_PER_THREAD) as u64;
        assert_eq!(tracker.current_sequence(1, 0), expected);
        assert_eq!(final_value.load(Ordering::SeqCst), expected);
    }

    #[test]
    fn test_thread_safety_no_duplicates() {
        let tracker = SequenceTracker::new("client");
        let seen_sequences = Arc::new(RwLock::new(Vec::new()));

        const NUM_THREADS: usize = 8;
        const INCREMENTS_PER_THREAD: usize = 500;

        let handles: Vec<_> = (0..NUM_THREADS)
            .map(|_| {
                let t = tracker.clone();
                let seen = Arc::clone(&seen_sequences);
                thread::spawn(move || {
                    let mut local_seqs = Vec::with_capacity(INCREMENTS_PER_THREAD);
                    for _ in 0..INCREMENTS_PER_THREAD {
                        local_seqs.push(t.next_sequence(1, 0));
                    }
                    seen.write().extend(local_seqs);
                })
            })
            .collect();

        for handle in handles {
            handle.join().expect("thread panicked");
        }

        let mut all_seqs = seen_sequences.read().clone();
        all_seqs.sort_unstable();

        // Check no duplicates
        let unique_count = all_seqs.windows(2).filter(|w| w[0] != w[1]).count() + 1;
        assert_eq!(unique_count, all_seqs.len());

        // Check all sequences from 1 to N are present
        let expected: Vec<u64> = (1..=((NUM_THREADS * INCREMENTS_PER_THREAD) as u64)).collect();
        assert_eq!(all_seqs, expected);
    }

    // ========================================================================
    // FileSequenceStorage Tests
    // ========================================================================

    #[test]
    fn test_file_storage_load_empty_directory() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let storage = FileSequenceStorage::new("test-client", temp_dir.path());

        let loaded = storage.load().expect("Load should succeed");
        assert!(loaded.is_empty());
    }

    #[test]
    fn test_file_storage_save_and_load_roundtrip() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let storage = FileSequenceStorage::new("test-client", temp_dir.path());

        let mut sequences = HashMap::new();
        sequences.insert(VaultKey::new(1, 0), 42);
        sequences.insert(VaultKey::new(1, 5), 128);
        sequences.insert(VaultKey::new(2, 0), 1);

        storage.save(&sequences).expect("Save should succeed");

        let loaded = storage.load().expect("Load should succeed");
        assert_eq!(loaded.len(), 3);
        assert_eq!(loaded.get(&VaultKey::new(1, 0)), Some(&42));
        assert_eq!(loaded.get(&VaultKey::new(1, 5)), Some(&128));
        assert_eq!(loaded.get(&VaultKey::new(2, 0)), Some(&1));
    }

    #[test]
    fn test_file_storage_creates_directory() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let nested_path = temp_dir.path().join("deeply").join("nested").join("path");
        let storage = FileSequenceStorage::new("test-client", &nested_path);

        let mut sequences = HashMap::new();
        sequences.insert(VaultKey::new(1, 0), 42);

        storage.save(&sequences).expect("Save should create directories");

        assert!(nested_path.exists());
        assert!(nested_path.join("test-client.json").exists());
    }

    #[test]
    fn test_file_storage_atomic_write_leaves_no_temp_file() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let storage = FileSequenceStorage::new("test-client", temp_dir.path());

        let mut sequences = HashMap::new();
        sequences.insert(VaultKey::new(1, 0), 42);

        storage.save(&sequences).expect("Save should succeed");

        // Check no temp file remains
        let temp_file = temp_dir.path().join("test-client.json.tmp");
        assert!(!temp_file.exists(), "Temp file should be renamed, not left behind");

        // Check actual file exists
        let actual_file = temp_dir.path().join("test-client.json");
        assert!(actual_file.exists(), "Final file should exist");
    }

    #[test]
    fn test_file_storage_corrupted_json_returns_empty() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let storage = FileSequenceStorage::new("test-client", temp_dir.path());

        // Write corrupted JSON
        let path = temp_dir.path().join("test-client.json");
        std::fs::write(&path, "{ invalid json }").expect("Write should succeed");

        // Load should succeed but return empty (start fresh on corruption)
        let loaded = storage.load().expect("Load should succeed");
        assert!(loaded.is_empty(), "Corrupted JSON should return empty map");
    }

    #[test]
    fn test_file_storage_wrong_version_returns_empty() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let storage = FileSequenceStorage::new("test-client", temp_dir.path());

        // Write valid JSON with wrong version
        let path = temp_dir.path().join("test-client.json");
        let content = r#"{"version": 999, "client_id": "test-client", "sequences": {}, "updated_at": "2026-01-15T00:00:00Z"}"#;
        std::fs::write(&path, content).expect("Write should succeed");

        let loaded = storage.load().expect("Load should succeed with wrong version");
        assert!(loaded.is_empty(), "Wrong version should return empty map");
    }

    #[test]
    fn test_file_storage_wrong_client_id_returns_empty() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let storage = FileSequenceStorage::new("test-client", temp_dir.path());

        // Write valid JSON with wrong client_id
        let path = temp_dir.path().join("test-client.json");
        let content = r#"{"version": 1, "client_id": "wrong-client", "sequences": {"1:0": 42}, "updated_at": "2026-01-15T00:00:00Z"}"#;
        std::fs::write(&path, content).expect("Write should succeed");

        let loaded = storage.load().expect("Load should succeed");
        assert!(loaded.is_empty(), "Wrong client_id should return empty map");
    }

    #[test]
    fn test_file_storage_invalid_vault_key_skipped() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let storage = FileSequenceStorage::new("test-client", temp_dir.path());

        // Write valid JSON with some invalid vault keys
        let path = temp_dir.path().join("test-client.json");
        let content = r#"{"version": 1, "client_id": "test-client", "sequences": {"1:0": 42, "invalid": 100, "not:a:valid:key": 200}, "updated_at": "2026-01-15T00:00:00Z"}"#;
        std::fs::write(&path, content).expect("Write should succeed");

        let loaded = storage.load().expect("Load should succeed");
        assert_eq!(loaded.len(), 1, "Only valid vault key should be loaded");
        assert_eq!(loaded.get(&VaultKey::new(1, 0)), Some(&42));
    }

    #[test]
    fn test_file_storage_overwrites_previous_state() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let storage = FileSequenceStorage::new("test-client", temp_dir.path());

        // First save
        let mut sequences1 = HashMap::new();
        sequences1.insert(VaultKey::new(1, 0), 10);
        sequences1.insert(VaultKey::new(1, 1), 20);
        storage.save(&sequences1).expect("First save should succeed");

        // Second save with different data
        let mut sequences2 = HashMap::new();
        sequences2.insert(VaultKey::new(1, 0), 100);
        sequences2.insert(VaultKey::new(2, 0), 50);
        storage.save(&sequences2).expect("Second save should succeed");

        // Load should reflect second save
        let loaded = storage.load().expect("Load should succeed");
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded.get(&VaultKey::new(1, 0)), Some(&100));
        assert_eq!(loaded.get(&VaultKey::new(2, 0)), Some(&50));
        assert!(!loaded.contains_key(&VaultKey::new(1, 1)));
    }

    #[test]
    fn test_file_storage_empty_sequences_saves_valid_json() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let storage = FileSequenceStorage::new("test-client", temp_dir.path());

        let sequences = HashMap::new();
        storage.save(&sequences).expect("Save should succeed");

        let loaded = storage.load().expect("Load should succeed");
        assert!(loaded.is_empty());
    }

    #[test]
    fn test_file_storage_file_path() {
        let storage = FileSequenceStorage::new("my-client-123", "/var/lib/ledger");
        let path = storage.file_path();
        assert_eq!(path.to_str().expect("valid path"), "/var/lib/ledger/my-client-123.json");
    }

    #[test]
    fn test_vault_key_format_and_parse_roundtrip() {
        let key = VaultKey::new(123, 456);
        let formatted = FileSequenceStorage::format_vault_key(&key);
        assert_eq!(formatted, "123:456");

        let parsed = FileSequenceStorage::parse_vault_key(&formatted);
        assert_eq!(parsed, Some(key));
    }

    #[test]
    fn test_vault_key_parse_invalid() {
        assert!(FileSequenceStorage::parse_vault_key("").is_none());
        assert!(FileSequenceStorage::parse_vault_key("123").is_none());
        assert!(FileSequenceStorage::parse_vault_key("a:b").is_none());
        assert!(FileSequenceStorage::parse_vault_key("1:2:3").is_none());
        assert!(FileSequenceStorage::parse_vault_key(":").is_none());
    }

    #[test]
    fn test_vault_key_parse_negative_numbers() {
        let parsed = FileSequenceStorage::parse_vault_key("-1:-2");
        assert_eq!(parsed, Some(VaultKey::new(-1, -2)));
    }

    #[test]
    fn test_snapshot_returns_current_state() {
        let tracker = SequenceTracker::new("client");

        tracker.next_sequence(1, 0);
        tracker.next_sequence(1, 0);
        tracker.next_sequence(2, 0);

        let snapshot = tracker.snapshot();
        assert_eq!(snapshot.len(), 2);
        assert_eq!(snapshot.get(&VaultKey::new(1, 0)), Some(&2));
        assert_eq!(snapshot.get(&VaultKey::new(2, 0)), Some(&1));
    }

    #[test]
    fn test_restore_replaces_all_state() {
        let tracker = SequenceTracker::new("client");

        // Initial state
        tracker.next_sequence(1, 0);
        tracker.next_sequence(1, 0);

        // Restore with different state
        let mut new_state = HashMap::new();
        new_state.insert(VaultKey::new(5, 5), 100);
        tracker.restore(new_state);

        // Old state is gone
        assert_eq!(tracker.current_sequence(1, 0), 0);

        // New state is active
        assert_eq!(tracker.current_sequence(5, 5), 100);
        assert_eq!(tracker.next_sequence(5, 5), 101);
    }

    #[test]
    fn test_sequence_persistence_survives_reload() {
        // This simulates a client restart scenario
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let storage_path = temp_dir.path().to_path_buf();

        // First session: use some sequences and save
        {
            let storage = FileSequenceStorage::new("persistent-client", &storage_path);
            let tracker = SequenceTracker::new("persistent-client");

            // Generate some sequences
            assert_eq!(tracker.next_sequence(1, 0), 1);
            assert_eq!(tracker.next_sequence(1, 0), 2);
            assert_eq!(tracker.next_sequence(1, 0), 3);
            assert_eq!(tracker.next_sequence(2, 0), 1);
            assert_eq!(tracker.next_sequence(2, 0), 2);

            // Save state (simulating clean shutdown)
            storage.save(&tracker.snapshot()).expect("Save should succeed");
        }

        // Second session: load and continue
        {
            let storage = FileSequenceStorage::new("persistent-client", &storage_path);
            let loaded = storage.load().expect("Load should succeed");

            // Create new tracker and restore state
            let tracker = SequenceTracker::new("persistent-client");
            tracker.restore(loaded);

            // Sequences should continue from where they left off
            assert_eq!(tracker.next_sequence(1, 0), 4);
            assert_eq!(tracker.next_sequence(1, 0), 5);
            assert_eq!(tracker.next_sequence(2, 0), 3);

            // New vault still starts at 1
            assert_eq!(tracker.next_sequence(3, 0), 1);
        }
    }

    // ========================================================================
    // PersistentSequenceTracker Tests
    // ========================================================================

    #[test]
    fn test_persistent_tracker_loads_on_creation() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let storage = FileSequenceStorage::new("test-client", temp_dir.path());

        // Pre-populate storage
        let mut sequences = HashMap::new();
        sequences.insert(VaultKey::new(1, 0), 42);
        sequences.insert(VaultKey::new(2, 0), 100);
        storage.save(&sequences).expect("Save should succeed");

        // Create persistent tracker - should load existing state
        let tracker =
            PersistentSequenceTracker::new("test-client", storage).expect("Should create tracker");

        // Should continue from loaded state
        assert_eq!(tracker.current_sequence(1, 0), 42);
        assert_eq!(tracker.current_sequence(2, 0), 100);
        assert_eq!(tracker.next_sequence(1, 0), 43);
    }

    #[test]
    fn test_persistent_tracker_starts_fresh_when_empty() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let storage = FileSequenceStorage::new("test-client", temp_dir.path());

        let tracker =
            PersistentSequenceTracker::new("test-client", storage).expect("Should create tracker");

        assert_eq!(tracker.vault_count(), 0);
        assert_eq!(tracker.next_sequence(1, 0), 1);
    }

    #[test]
    fn test_persistent_tracker_flush() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let storage_path = temp_dir.path().to_path_buf();

        // First tracker: create some sequences and flush
        {
            let storage = FileSequenceStorage::new("flush-client", &storage_path);
            let tracker = PersistentSequenceTracker::new("flush-client", storage)
                .expect("Should create tracker");

            tracker.next_sequence(1, 0);
            tracker.next_sequence(1, 0);
            tracker.next_sequence(1, 0);

            tracker.flush().expect("Flush should succeed");
        }

        // Second tracker: verify state was persisted
        {
            let storage = FileSequenceStorage::new("flush-client", &storage_path);
            let tracker = PersistentSequenceTracker::new("flush-client", storage)
                .expect("Should create tracker");

            assert_eq!(tracker.current_sequence(1, 0), 3);
            assert_eq!(tracker.next_sequence(1, 0), 4);
        }
    }

    #[test]
    fn test_persistent_tracker_accessors() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let storage = FileSequenceStorage::new("accessor-client", temp_dir.path());

        let tracker = PersistentSequenceTracker::new("accessor-client", storage)
            .expect("Should create tracker");

        assert_eq!(tracker.client_id(), "accessor-client");
        assert_eq!(tracker.vault_count(), 0);

        tracker.next_sequence(1, 0);
        assert_eq!(tracker.vault_count(), 1);
        assert_eq!(tracker.current_sequence(1, 0), 1);

        tracker.set_sequence(2, 0, 50);
        assert_eq!(tracker.current_sequence(2, 0), 50);
    }

    #[test]
    fn test_persistent_tracker_inner() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let storage = FileSequenceStorage::new("inner-client", temp_dir.path());

        let tracker =
            PersistentSequenceTracker::new("inner-client", storage).expect("Should create tracker");

        tracker.next_sequence(1, 0);

        // Can access inner tracker directly
        let inner = tracker.inner();
        assert_eq!(inner.current_sequence(1, 0), 1);
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]
mod proptest_tests {
    use std::collections::HashSet;

    use proptest::prelude::*;

    use super::*;

    proptest! {
        /// Property: Sequence always increments by exactly 1
        ///
        /// For any starting sequence and any number of increments,
        /// each call to next_sequence returns exactly previous + 1.
        #[test]
        fn prop_sequence_increments_by_one(
            namespace_id in any::<i64>(),
            vault_id in any::<i64>(),
            initial_seq in 0u64..1_000_000,
            num_increments in 1usize..100
        ) {
            let tracker = SequenceTracker::new("prop-test-client");

            // Set initial sequence
            if initial_seq > 0 {
                tracker.set_sequence(namespace_id, vault_id, initial_seq);
            }

            let mut expected = initial_seq;
            for _ in 0..num_increments {
                expected += 1;
                let actual = tracker.next_sequence(namespace_id, vault_id);
                prop_assert_eq!(
                    actual, expected,
                    "Expected sequence {} but got {} after {} increments from initial {}",
                    expected, actual, num_increments, initial_seq
                );
            }
        }

        /// Property: No duplicate sequences within a vault
        ///
        /// When calling next_sequence multiple times for the same vault,
        /// all returned sequences are unique.
        #[test]
        fn prop_no_duplicate_sequences(
            namespace_id in any::<i64>(),
            vault_id in any::<i64>(),
            num_increments in 1usize..500
        ) {
            let tracker = SequenceTracker::new("prop-test-client");
            let mut seen = HashSet::new();

            for i in 0..num_increments {
                let seq = tracker.next_sequence(namespace_id, vault_id);
                prop_assert!(
                    seen.insert(seq),
                    "Duplicate sequence {} detected on increment {}",
                    seq, i
                );
            }

            // All sequences should be consecutive starting from 1
            let expected: HashSet<u64> = (1..=(num_increments as u64)).collect();
            prop_assert_eq!(seen, expected);
        }

        /// Property: Recovery sets correct state
        ///
        /// After set_sequence(n), the next call to next_sequence returns n+1.
        #[test]
        fn prop_recovery_reaches_correct_state(
            namespace_id in any::<i64>(),
            vault_id in any::<i64>(),
            recovery_seq in 0u64..1_000_000
        ) {
            let tracker = SequenceTracker::new("prop-test-client");

            // Simulate recovery: server says last committed was recovery_seq
            tracker.set_sequence(namespace_id, vault_id, recovery_seq);

            // Current should reflect the set value
            prop_assert_eq!(
                tracker.current_sequence(namespace_id, vault_id),
                recovery_seq
            );

            // Next should be recovery_seq + 1
            let next = tracker.next_sequence(namespace_id, vault_id);
            prop_assert_eq!(
                next, recovery_seq + 1,
                "After recovery to {}, expected next {} but got {}",
                recovery_seq, recovery_seq + 1, next
            );
        }

        /// Property: Different vaults have independent sequences
        ///
        /// Operations on one vault don't affect sequences in other vaults.
        #[test]
        fn prop_vaults_are_independent(
            ns1 in any::<i64>(),
            v1 in any::<i64>(),
            ns2 in any::<i64>(),
            v2 in any::<i64>(),
            ops1 in 1usize..50,
            ops2 in 1usize..50
        ) {
            // Skip if same vault
            prop_assume!(ns1 != ns2 || v1 != v2);

            let tracker = SequenceTracker::new("prop-test-client");

            // Operate on vault 1
            for _ in 0..ops1 {
                tracker.next_sequence(ns1, v1);
            }

            // Operate on vault 2
            for _ in 0..ops2 {
                tracker.next_sequence(ns2, v2);
            }

            // Each vault should have its own sequence
            prop_assert_eq!(
                tracker.current_sequence(ns1, v1),
                ops1 as u64
            );
            prop_assert_eq!(
                tracker.current_sequence(ns2, v2),
                ops2 as u64
            );
        }

        /// Property: Snapshot and restore are inverse operations
        ///
        /// Restoring a snapshot to a fresh tracker reproduces the original state.
        #[test]
        fn prop_snapshot_restore_roundtrip(
            operations in prop::collection::vec(
                (any::<i64>(), any::<i64>(), 1usize..10),
                1..10
            )
        ) {
            let tracker1 = SequenceTracker::new("tracker1");

            // Perform operations
            for (ns, v, count) in &operations {
                for _ in 0..*count {
                    tracker1.next_sequence(*ns, *v);
                }
            }

            // Snapshot and restore to a new tracker
            let snapshot = tracker1.snapshot();
            let tracker2 = SequenceTracker::new("tracker2");
            tracker2.restore(snapshot.clone());

            // Verify all sequences match
            for (ns, v, _) in &operations {
                prop_assert_eq!(
                    tracker1.current_sequence(*ns, *v),
                    tracker2.current_sequence(*ns, *v),
                    "Sequence mismatch for vault ({}, {})",
                    ns, v
                );
            }
        }
    }
}
