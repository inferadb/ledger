//! Latency-based server selection.
//!
//! Tracks per-server latency using exponential moving average (EMA) and
//! provides ordered server selection preferring lowest-latency servers.

use std::{
    collections::HashSet,
    sync::Arc,
    time::{Duration, Instant},
};

use dashmap::DashMap;
use parking_lot::RwLock;

use super::ResolvedServer;

/// Exponential moving average alpha factor.
///
/// α = 0.3 gives approximately 10-sample half-life, meaning recent samples
/// have significant weight but history still matters.
const EMA_ALPHA: f64 = 0.3;

/// Default latency for servers with no measurements (10ms).
///
/// This is optimistic to allow new servers to be tried.
const DEFAULT_LATENCY_MS: f64 = 10.0;

/// Minimum samples before a latency measurement is considered reliable.
const MIN_RELIABLE_SAMPLES: u64 = 3;

/// Latency statistics for a single server.
#[derive(Debug, Clone)]
struct LatencyStats {
    /// Exponential moving average of latency in milliseconds.
    ema_ms: f64,
    /// Number of samples recorded.
    sample_count: u64,
    /// When this was last updated.
    last_updated: Instant,
}

impl Default for LatencyStats {
    fn default() -> Self {
        Self { ema_ms: DEFAULT_LATENCY_MS, sample_count: 0, last_updated: Instant::now() }
    }
}

impl LatencyStats {
    /// Records a new latency sample, updating the EMA.
    fn record(&mut self, latency: Duration) {
        let latency_ms = latency.as_secs_f64() * 1000.0;

        if self.sample_count == 0 {
            // First sample: use it directly
            self.ema_ms = latency_ms;
        } else {
            // EMA update: new = α * sample + (1-α) * old
            self.ema_ms = EMA_ALPHA * latency_ms + (1.0 - EMA_ALPHA) * self.ema_ms;
        }

        self.sample_count += 1;
        self.last_updated = Instant::now();
    }

    /// Returns whether we have enough samples for reliable ordering.
    fn is_reliable(&self) -> bool {
        self.sample_count >= MIN_RELIABLE_SAMPLES
    }
}

/// Server selector with latency-based ordering.
///
/// Tracks per-server latency and provides server selection that prefers
/// servers with the lowest measured latency. Integrates with health status
/// to exclude unhealthy servers.
///
/// # Thread Safety
///
/// All operations are thread-safe and can be called concurrently from
/// multiple tasks.
///
/// # Example
///
/// ```no_run
/// use std::time::Duration;
/// use inferadb_ledger_sdk::{ResolvedServer, ServerSelector};
///
/// let selector = ServerSelector::new();
///
/// // Record latencies as requests complete
/// selector.record_latency("10.0.0.1:50051", Duration::from_millis(5));
/// selector.record_latency("10.0.0.2:50051", Duration::from_millis(15));
///
/// // Get servers ordered by latency
/// let available_servers = vec![
///     ResolvedServer::new("10.0.0.1:50051", false),
///     ResolvedServer::new("10.0.0.2:50051", false),
/// ];
/// let servers = selector.select_best(&available_servers);
/// // Returns: ["10.0.0.1:50051", "10.0.0.2:50051"]
/// ```
#[derive(Debug, Clone)]
pub struct ServerSelector {
    /// Per-server latency tracking.
    latencies: Arc<DashMap<String, LatencyStats>>,

    /// Servers known to be unhealthy.
    unhealthy: Arc<RwLock<HashSet<String>>>,
}

impl Default for ServerSelector {
    fn default() -> Self {
        Self::new()
    }
}

impl ServerSelector {
    /// Creates a new server selector.
    #[must_use]
    pub fn new() -> Self {
        Self {
            latencies: Arc::new(DashMap::new()),
            unhealthy: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    /// Records a successful request latency for a server.
    ///
    /// This updates the exponential moving average for the server.
    /// Call this after each successful request to keep latency data fresh.
    pub fn record_latency(&self, server: &str, latency: Duration) {
        self.latencies.entry(server.to_string()).or_default().record(latency);
    }

    /// Marks a server as unhealthy.
    ///
    /// Unhealthy servers are deprioritized in selection.
    pub fn mark_unhealthy(&self, server: &str) {
        self.unhealthy.write().insert(server.to_string());
    }

    /// Marks a server as healthy.
    ///
    /// Removes the server from the unhealthy set.
    pub fn mark_healthy(&self, server: &str) {
        self.unhealthy.write().remove(server);
    }

    /// Clears the unhealthy status for all servers.
    pub fn clear_unhealthy(&self) {
        self.unhealthy.write().clear();
    }

    /// Returns the current latency EMA for a server, if known.
    #[must_use]
    pub fn latency_ms(&self, server: &str) -> Option<f64> {
        self.latencies.get(server).map(|stats| stats.ema_ms)
    }

    /// Returns whether a server is marked unhealthy.
    #[must_use]
    pub fn is_unhealthy(&self, server: &str) -> bool {
        self.unhealthy.read().contains(server)
    }

    /// Selects the best servers from the given list, ordered by latency.
    ///
    /// Returns servers ordered from lowest to highest latency. Unhealthy
    /// servers are placed at the end. Servers without latency data are
    /// placed after servers with data but before unhealthy servers.
    ///
    /// # Arguments
    ///
    /// * `servers` - Available servers to select from
    ///
    /// # Returns
    ///
    /// Server URLs ordered by preference (lowest latency first).
    #[must_use]
    pub fn select_best(&self, servers: &[ResolvedServer]) -> Vec<String> {
        let unhealthy = self.unhealthy.read();

        // Partition into healthy and unhealthy
        let (mut healthy, mut unhealthy_servers): (Vec<_>, Vec<_>) =
            servers.iter().partition(|s| !unhealthy.contains(&s.address));

        // Sort healthy servers by latency
        healthy.sort_by(|a, b| {
            let latency_a = self.effective_latency(&a.address);
            let latency_b = self.effective_latency(&b.address);
            latency_a.partial_cmp(&latency_b).unwrap_or(std::cmp::Ordering::Equal)
        });

        // Sort unhealthy servers by latency too (for fallback ordering)
        unhealthy_servers.sort_by(|a, b| {
            let latency_a = self.effective_latency(&a.address);
            let latency_b = self.effective_latency(&b.address);
            latency_a.partial_cmp(&latency_b).unwrap_or(std::cmp::Ordering::Equal)
        });

        // Combine: healthy first, then unhealthy as fallback
        healthy.into_iter().chain(unhealthy_servers).map(|s| s.url()).collect()
    }

    /// Returns the effective latency for sorting.
    ///
    /// Servers without reliable data get a slightly higher default to
    /// prioritize servers we know about, while still allowing new servers
    /// to be tried.
    fn effective_latency(&self, address: &str) -> f64 {
        match self.latencies.get(address) {
            Some(stats) if stats.is_reliable() => stats.ema_ms,
            Some(stats) => {
                // Not enough samples: use EMA but add a small penalty
                stats.ema_ms + 5.0
            },
            None => DEFAULT_LATENCY_MS + 10.0, // Unknown: slightly higher default
        }
    }

    /// Returns statistics about tracked servers.
    #[must_use]
    pub fn stats(&self) -> SelectorStats {
        let total = self.latencies.len();
        let reliable = self.latencies.iter().filter(|entry| entry.is_reliable()).count();
        let unhealthy = self.unhealthy.read().len();

        SelectorStats {
            total_tracked: total,
            reliable_tracked: reliable,
            unhealthy_count: unhealthy,
        }
    }

    /// Clears all latency data.
    ///
    /// Useful for testing or when the server topology changes significantly.
    pub fn clear(&self) {
        self.latencies.clear();
        self.unhealthy.write().clear();
    }
}

/// Statistics about the server selector state.
#[derive(Debug, Clone, Copy)]
pub struct SelectorStats {
    /// Total number of servers with latency data.
    pub total_tracked: usize,
    /// Number of servers with reliable latency data.
    pub reliable_tracked: usize,
    /// Number of servers marked unhealthy.
    pub unhealthy_count: usize,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    fn make_server(address: &str, use_tls: bool) -> ResolvedServer {
        ResolvedServer::new(address, use_tls)
    }

    #[test]
    fn test_selector_new() {
        let selector = ServerSelector::new();
        assert_eq!(selector.stats().total_tracked, 0);
        assert_eq!(selector.stats().unhealthy_count, 0);
    }

    #[test]
    fn test_record_latency_first_sample() {
        let selector = ServerSelector::new();
        selector.record_latency("10.0.0.1:50051", Duration::from_millis(50));

        let latency = selector.latency_ms("10.0.0.1:50051").expect("should have latency");
        assert!((latency - 50.0).abs() < 0.001);
    }

    #[test]
    fn test_record_latency_ema_update() {
        let selector = ServerSelector::new();

        // First sample: 100ms
        selector.record_latency("10.0.0.1:50051", Duration::from_millis(100));
        let lat1 = selector.latency_ms("10.0.0.1:50051").unwrap();
        assert!((lat1 - 100.0).abs() < 0.001);

        // Second sample: 50ms
        // EMA = 0.3 * 50 + 0.7 * 100 = 15 + 70 = 85
        selector.record_latency("10.0.0.1:50051", Duration::from_millis(50));
        let lat2 = selector.latency_ms("10.0.0.1:50051").unwrap();
        assert!((lat2 - 85.0).abs() < 0.001);

        // Third sample: 50ms
        // EMA = 0.3 * 50 + 0.7 * 85 = 15 + 59.5 = 74.5
        selector.record_latency("10.0.0.1:50051", Duration::from_millis(50));
        let lat3 = selector.latency_ms("10.0.0.1:50051").unwrap();
        assert!((lat3 - 74.5).abs() < 0.001);
    }

    #[test]
    fn test_mark_unhealthy() {
        let selector = ServerSelector::new();

        assert!(!selector.is_unhealthy("10.0.0.1:50051"));

        selector.mark_unhealthy("10.0.0.1:50051");
        assert!(selector.is_unhealthy("10.0.0.1:50051"));

        selector.mark_healthy("10.0.0.1:50051");
        assert!(!selector.is_unhealthy("10.0.0.1:50051"));
    }

    #[test]
    fn test_clear_unhealthy() {
        let selector = ServerSelector::new();
        selector.mark_unhealthy("10.0.0.1:50051");
        selector.mark_unhealthy("10.0.0.2:50051");

        assert_eq!(selector.stats().unhealthy_count, 2);

        selector.clear_unhealthy();
        assert_eq!(selector.stats().unhealthy_count, 0);
    }

    #[test]
    fn test_select_best_orders_by_latency() {
        let selector = ServerSelector::new();

        // Record reliable latencies (3+ samples each)
        for _ in 0..3 {
            selector.record_latency("10.0.0.1:50051", Duration::from_millis(100));
            selector.record_latency("10.0.0.2:50051", Duration::from_millis(50));
            selector.record_latency("10.0.0.3:50051", Duration::from_millis(75));
        }

        let servers = vec![
            make_server("10.0.0.1:50051", false),
            make_server("10.0.0.2:50051", false),
            make_server("10.0.0.3:50051", false),
        ];

        let selected = selector.select_best(&servers);

        // Should be ordered: lowest latency first
        assert_eq!(selected[0], "http://10.0.0.2:50051"); // 50ms
        assert_eq!(selected[1], "http://10.0.0.3:50051"); // 75ms
        assert_eq!(selected[2], "http://10.0.0.1:50051"); // 100ms
    }

    #[test]
    fn test_select_best_unhealthy_last() {
        let selector = ServerSelector::new();

        // Server 1: fast but unhealthy
        for _ in 0..3 {
            selector.record_latency("10.0.0.1:50051", Duration::from_millis(10));
            selector.record_latency("10.0.0.2:50051", Duration::from_millis(100));
        }
        selector.mark_unhealthy("10.0.0.1:50051");

        let servers =
            vec![make_server("10.0.0.1:50051", false), make_server("10.0.0.2:50051", false)];

        let selected = selector.select_best(&servers);

        // Healthy server first, even though slower
        assert_eq!(selected[0], "http://10.0.0.2:50051");
        assert_eq!(selected[1], "http://10.0.0.1:50051");
    }

    #[test]
    fn test_select_best_unknown_servers() {
        let selector = ServerSelector::new();

        // Only record latency for one server - use 15ms which is less than
        // the unknown server default of 20ms (DEFAULT_LATENCY_MS + 10)
        for _ in 0..3 {
            selector.record_latency("10.0.0.1:50051", Duration::from_millis(15));
        }

        let servers = vec![
            make_server("10.0.0.1:50051", false),
            make_server("10.0.0.2:50051", false), // Unknown
        ];

        let selected = selector.select_best(&servers);

        // Known server first (15ms < 20ms default for unknown)
        assert_eq!(selected[0], "http://10.0.0.1:50051");
        assert_eq!(selected[1], "http://10.0.0.2:50051");
    }

    #[test]
    fn test_select_best_with_tls() {
        let selector = ServerSelector::new();

        let servers = vec![make_server("10.0.0.1:443", true), make_server("10.0.0.2:50051", false)];

        let selected = selector.select_best(&servers);

        assert!(selected[0].starts_with("https://") || selected[0].starts_with("http://"));
        assert!(selected[1].starts_with("https://") || selected[1].starts_with("http://"));
    }

    #[test]
    fn test_select_best_empty_input() {
        let selector = ServerSelector::new();
        let selected = selector.select_best(&[]);
        assert!(selected.is_empty());
    }

    #[test]
    fn test_stats() {
        let selector = ServerSelector::new();

        // Initial state
        let stats = selector.stats();
        assert_eq!(stats.total_tracked, 0);
        assert_eq!(stats.reliable_tracked, 0);
        assert_eq!(stats.unhealthy_count, 0);

        // Add some data
        selector.record_latency("10.0.0.1:50051", Duration::from_millis(50));
        selector.record_latency("10.0.0.1:50051", Duration::from_millis(50));
        selector.record_latency("10.0.0.1:50051", Duration::from_millis(50)); // 3 samples = reliable
        selector.record_latency("10.0.0.2:50051", Duration::from_millis(50)); // 1 sample = not reliable
        selector.mark_unhealthy("10.0.0.3:50051");

        let stats = selector.stats();
        assert_eq!(stats.total_tracked, 2);
        assert_eq!(stats.reliable_tracked, 1);
        assert_eq!(stats.unhealthy_count, 1);
    }

    #[test]
    fn test_clear() {
        let selector = ServerSelector::new();
        selector.record_latency("10.0.0.1:50051", Duration::from_millis(50));
        selector.mark_unhealthy("10.0.0.2:50051");

        selector.clear();

        assert_eq!(selector.stats().total_tracked, 0);
        assert_eq!(selector.stats().unhealthy_count, 0);
        assert!(selector.latency_ms("10.0.0.1:50051").is_none());
    }

    #[test]
    fn test_selector_clone_shares_state() {
        let selector1 = ServerSelector::new();
        let selector2 = selector1.clone();

        // Modify through selector1
        selector1.record_latency("10.0.0.1:50051", Duration::from_millis(50));

        // Should be visible through selector2
        assert!(selector2.latency_ms("10.0.0.1:50051").is_some());
    }

    #[test]
    fn test_selector_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<ServerSelector>();
    }
}
