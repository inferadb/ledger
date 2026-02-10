//! Background resource saturation metrics collector.
//!
//! Periodically samples infrastructure-level metrics that indicate resource
//! utilization and saturation. These complement the application-level metrics
//! already in `metrics.rs` and enable the USE method (Utilization, Saturation,
//! Errors) for capacity planning.
//!
//! Collected metrics:
//! - Disk space (total/free/used bytes)
//! - Page cache hit/miss rates and size
//! - B-tree page split counts
//! - Compaction lag (free pages as reclaimable space)
//! - Snapshot disk usage

use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use inferadb_ledger_state::StateLayer;
use inferadb_ledger_store::StorageBackend;
use tokio::time::interval;
use tracing::debug;

use crate::metrics;

/// Default interval between resource metric collection cycles.
const DEFAULT_COLLECTION_INTERVAL: Duration = Duration::from_secs(30);

/// Background collector for resource saturation metrics.
///
/// Samples disk space, page cache statistics, B-tree health, and snapshot
/// disk usage at a configurable interval.
#[derive(bon::Builder)]
#[builder(on(_, required))]
pub struct ResourceMetricsCollector<B: StorageBackend + 'static> {
    /// State layer providing database statistics.
    state: Arc<StateLayer<B>>,
    /// Path to the data directory (for disk space metrics).
    data_dir: PathBuf,
    /// Path to the snapshot directory (for snapshot disk usage).
    snapshot_dir: PathBuf,
    /// Collection interval.
    #[builder(default = DEFAULT_COLLECTION_INTERVAL)]
    interval: Duration,
    /// Watchdog heartbeat handle. Updated each cycle to prove liveness.
    #[builder(default)]
    watchdog_handle: Option<Arc<std::sync::atomic::AtomicU64>>,
}

impl<B: StorageBackend + 'static> ResourceMetricsCollector<B> {
    /// Runs a single collection cycle.
    fn collect(&self) {
        self.collect_disk_metrics();
        self.collect_database_metrics();
        self.collect_snapshot_metrics();
    }

    /// Collect disk space metrics for the data directory's filesystem.
    fn collect_disk_metrics(&self) {
        match disk_space(&self.data_dir) {
            Some((total, free)) => {
                metrics::set_disk_bytes(total, free);
            },
            None => {
                debug!(
                    data_dir = %self.data_dir.display(),
                    "Could not read disk space metrics"
                );
            },
        }
    }

    /// Collect database-level metrics (page cache, splits, compaction lag, btree depth).
    fn collect_database_metrics(&self) {
        let stats = self.state.database_stats();

        metrics::set_page_cache_metrics(stats.cache_hits, stats.cache_misses, stats.cached_pages);
        metrics::set_btree_page_splits(stats.page_splits);
        metrics::set_compaction_lag_blocks(stats.free_pages);

        match self.state.table_depths() {
            Ok(depths) => {
                for (table, depth) in depths {
                    metrics::set_btree_depth(table, depth);
                }
            },
            Err(e) => {
                debug!(error = %e, "Could not read B-tree depths");
            },
        }
    }

    /// Collect snapshot directory disk usage.
    fn collect_snapshot_metrics(&self) {
        match snapshot_disk_bytes(&self.snapshot_dir) {
            Some(bytes) => metrics::set_snapshot_disk_bytes(bytes),
            None => {
                debug!(
                    snapshot_dir = %self.snapshot_dir.display(),
                    "Could not read snapshot directory size"
                );
            },
        }
    }

    /// Starts the background collection task.
    ///
    /// Returns a handle that can be used to abort the task.
    pub fn start(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = interval(self.interval);

            loop {
                ticker.tick().await;
                if let Some(ref handle) = self.watchdog_handle {
                    handle.store(
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs(),
                        std::sync::atomic::Ordering::Relaxed,
                    );
                }
                self.collect();
            }
        })
    }
}

/// Returns disk space (total bytes, free bytes) for the filesystem containing `path`.
///
/// Uses `fs2` for safe, cross-platform filesystem space queries.
fn disk_space(path: &Path) -> Option<(u64, u64)> {
    use fs2::{available_space, total_space};

    let total = total_space(path).ok()?;
    let free = available_space(path).ok()?;

    Some((total, free))
}

/// Sum file sizes in the snapshot directory.
fn snapshot_disk_bytes(snapshot_dir: &Path) -> Option<u64> {
    if !snapshot_dir.exists() {
        return Some(0);
    }

    let mut total = 0u64;
    let entries = std::fs::read_dir(snapshot_dir).ok()?;

    for entry in entries {
        let entry = entry.ok()?;
        let metadata = entry.metadata().ok()?;
        if metadata.is_file() {
            total += metadata.len();
        }
    }

    Some(total)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn test_disk_space_returns_values() {
        let dir = tempfile::tempdir().unwrap();
        let result = disk_space(dir.path());
        assert!(result.is_some(), "disk_space should work for a tempdir");
        let (total, free) = result.unwrap();
        assert!(total > 0, "total disk space should be positive");
        assert!(free <= total, "free should not exceed total");
    }

    #[test]
    fn test_disk_space_nonexistent_path() {
        let result = disk_space(Path::new("/nonexistent/path/that/does/not/exist"));
        // Should return None for nonexistent paths
        assert!(result.is_none());
    }

    #[test]
    fn test_snapshot_disk_bytes_empty_dir() {
        let dir = tempfile::tempdir().unwrap();
        let result = snapshot_disk_bytes(dir.path());
        assert_eq!(result, Some(0));
    }

    #[test]
    fn test_snapshot_disk_bytes_nonexistent_dir() {
        let result = snapshot_disk_bytes(Path::new("/nonexistent/snapshot/dir"));
        assert_eq!(result, Some(0));
    }

    #[test]
    fn test_snapshot_disk_bytes_with_files() {
        let dir = tempfile::tempdir().unwrap();
        let file1 = dir.path().join("snapshot_1.bin");
        let file2 = dir.path().join("snapshot_2.bin");

        std::fs::write(&file1, vec![0u8; 1024]).unwrap();
        std::fs::write(&file2, vec![0u8; 2048]).unwrap();

        let result = snapshot_disk_bytes(dir.path());
        assert_eq!(result, Some(3072));
    }

    #[test]
    fn test_resource_metrics_collector_database_metrics() {
        // Verify collection doesn't panic with in-memory database
        let db = inferadb_ledger_store::Database::open_in_memory().unwrap();
        let state = Arc::new(StateLayer::new(Arc::new(db)));
        let dir = tempfile::tempdir().unwrap();

        let collector = ResourceMetricsCollector::builder()
            .state(state)
            .data_dir(dir.path().to_path_buf())
            .snapshot_dir(dir.path().join("snapshots"))
            .build();

        // Should not panic
        collector.collect();
    }

    #[test]
    fn test_table_depths_after_inserts() {
        use inferadb_ledger_store::tables::Entities;

        let db = inferadb_ledger_store::Database::open_in_memory().unwrap();

        // Insert enough rows to create a btree with depth >= 1
        {
            let mut txn = db.write().unwrap();
            for i in 0..100u64 {
                let key = i.to_be_bytes().to_vec();
                let value = vec![0u8; 64];
                txn.insert::<Entities>(&key, &value).unwrap();
            }
            txn.commit().unwrap();
        }

        let depths = db.table_depths().unwrap();
        // Entities table should have depth >= 1
        let entities_depth = depths.iter().find(|(name, _)| *name == "entities");
        assert!(entities_depth.is_some(), "entities table should be in depths after inserts");
        assert!(entities_depth.unwrap().1 >= 1, "entities depth should be at least 1");
    }

    #[test]
    fn test_table_depths_empty_database() {
        let db = inferadb_ledger_store::Database::open_in_memory().unwrap();
        let depths = db.table_depths().unwrap();
        assert!(depths.is_empty(), "empty database should have no btree depths");
    }

    #[test]
    fn test_resource_metrics_no_panic_without_recorder() {
        // Metrics crate is safe to call without a recorder
        metrics::set_disk_bytes(1_000_000_000, 500_000_000);
        metrics::set_page_cache_metrics(100, 50, 75);
        metrics::set_btree_depth("entities", 3);
        metrics::set_btree_page_splits(42);
        metrics::set_compaction_lag_blocks(10);
        metrics::set_snapshot_disk_bytes(1_048_576);
    }
}
