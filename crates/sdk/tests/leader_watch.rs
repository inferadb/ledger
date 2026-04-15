//! End-to-end test: `WatchLeader` streaming resolver applies server-pushed
//! leader updates to the SDK's `RegionLeaderCache`.
//!
//! Uses [`MockLedgerServer`], which implements `WatchLeader` by broadcasting
//! updates sent via [`MockLedgerServer::push_leader_update`] to all active
//! subscribers. The test:
//!
//! 1. Starts a mock server and a `ConnectionPool` with `preferred_region`.
//! 2. Triggers `get_or_create_gateway_channel` (via the first RPC path) so the pool lazily spawns
//!    the watcher task.
//! 3. Pushes a leader update.
//! 4. Asserts the update lands in the cache and the SDK metric is bumped.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use inferadb_ledger_sdk::{
    ClientConfig, ConnectionPool, SdkMetrics, ServerSource, mock::MockLedgerServer,
};
use inferadb_ledger_types::Region;

/// Counting metrics that records watch update / reconnect events by region.
#[derive(Debug, Default)]
struct CountingMetrics {
    updates: AtomicU64,
    reconnects: AtomicU64,
}

impl SdkMetrics for CountingMetrics {
    fn leader_watch_update(&self, _region: &str) {
        self.updates.fetch_add(1, Ordering::SeqCst);
    }
    fn leader_watch_reconnect(&self, _region: &str) {
        self.reconnects.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watch_leader_pushes_updates_into_cache() {
    let server = MockLedgerServer::start().await.expect("start mock server");
    let metrics = Arc::new(CountingMetrics::default());

    let config = ClientConfig::builder()
        .servers(ServerSource::from_static([server.endpoint()]))
        .client_id("watch-leader-test")
        .preferred_region(Region::US_EAST_VA)
        .metrics(Arc::clone(&metrics) as Arc<dyn SdkMetrics>)
        .build()
        .expect("build client config");

    let pool = ConnectionPool::new(config);

    // Touch the pool so the gateway channel is created and the watcher task
    // spawns lazily. `get_channel` goes through `get_region_channel`, which
    // on cache miss calls `get_or_create_gateway_channel` — triggering the
    // lazy watcher spawn.
    let _ = pool.get_channel().await;

    // Give the watcher time to open the stream and consume the initial
    // empty update.
    let mut update_pushed = false;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        if server.push_leader_update("http://leader.example:5000", 42, 7) {
            update_pushed = true;
            break;
        }
    }
    assert!(update_pushed, "watcher should have subscribed by now");

    // Wait for the push to land in the cache.
    let mut cached: Option<String> = None;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        if let Some(ep) = pool.region_cached_endpoint() {
            cached = Some(String::from(ep.as_ref()));
            break;
        }
    }
    assert_eq!(cached.as_deref(), Some("http://leader.example:5000"));

    // At least two updates observed: one initial empty, one pushed.
    assert!(
        metrics.updates.load(Ordering::SeqCst) >= 2,
        "expected >= 2 watch updates, got {}",
        metrics.updates.load(Ordering::SeqCst)
    );
    // No reconnects: the stream stayed open.
    assert_eq!(metrics.reconnects.load(Ordering::SeqCst), 0);

    // Dropping the pool must not hang — Drop cancels + aborts the task.
    drop(pool);
}
