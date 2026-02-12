//! Peer tracking with staleness detection and pruning.
//!
//! This module tracks when peers were last seen and provides:
//! - Recording peer announcements with timestamps
//! - Querying last-seen time for health monitoring
//! - Pruning stale peers not seen within a configurable threshold (default: 1 hour)
//!
//! ## Usage
//!
//! The peer tracker should be periodically maintained via `prune_stale()` to prevent
//! unbounded memory growth from departed peers.

use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use prost_types::Timestamp;

/// Default staleness threshold: peers not seen in 1 hour are considered stale.
pub const DEFAULT_STALENESS_THRESHOLD: Duration = Duration::from_secs(60 * 60);

/// Default maintenance interval: run pruning every 5 minutes.
pub const DEFAULT_MAINTENANCE_INTERVAL: Duration = Duration::from_secs(5 * 60);

/// Configuration for peer tracking behavior.
#[derive(Debug, Clone, bon::Builder)]
pub struct PeerTrackerConfig {
    /// Duration after which a peer is considered stale if not seen.
    #[builder(default = DEFAULT_STALENESS_THRESHOLD)]
    pub staleness_threshold: Duration,
    /// How often to run maintenance (pruning).
    #[builder(default = DEFAULT_MAINTENANCE_INTERVAL)]
    pub maintenance_interval: Duration,
}

impl Default for PeerTrackerConfig {
    fn default() -> Self {
        Self {
            staleness_threshold: DEFAULT_STALENESS_THRESHOLD,
            maintenance_interval: DEFAULT_MAINTENANCE_INTERVAL,
        }
    }
}

/// Tracks when peers were last seen for health monitoring and staleness detection.
#[derive(Debug)]
pub struct PeerTracker {
    /// Map from node ID string to last seen instant.
    last_seen: HashMap<String, Instant>,
    /// Configuration for staleness thresholds.
    config: PeerTrackerConfig,
}

impl Default for PeerTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl PeerTracker {
    /// Creates a new peer tracker with default configuration.
    pub fn new() -> Self {
        Self::with_config(PeerTrackerConfig::default())
    }

    /// Creates a new peer tracker with custom configuration.
    pub fn with_config(config: PeerTrackerConfig) -> Self {
        Self { last_seen: HashMap::new(), config }
    }

    /// Records that a peer was seen now.
    pub fn record_seen(&mut self, node_id: &str) {
        self.last_seen.insert(node_id.to_string(), Instant::now());
    }

    /// Returns the last seen timestamp for a peer as a proto Timestamp.
    ///
    /// Returns None if the peer has never been seen.
    pub fn get_last_seen(&self, node_id: &str) -> Option<Timestamp> {
        self.last_seen.get(node_id).map(|instant| {
            // Convert Instant to wall-clock time
            let elapsed_since_seen = instant.elapsed();
            let now = std::time::SystemTime::now();
            let seen_time = now - elapsed_since_seen;

            // Convert to proto Timestamp
            match seen_time.duration_since(std::time::UNIX_EPOCH) {
                Ok(duration) => Timestamp {
                    seconds: duration.as_secs() as i64,
                    nanos: duration.subsec_nanos() as i32,
                },
                Err(_) => Timestamp { seconds: 0, nanos: 0 },
            }
        })
    }

    /// Checks if a peer is considered stale (not seen within threshold).
    pub fn is_stale(&self, node_id: &str) -> bool {
        match self.last_seen.get(node_id) {
            Some(instant) => instant.elapsed() > self.config.staleness_threshold,
            None => true, // Never seen = stale
        }
    }

    /// Returns the time since a peer was last seen.
    ///
    /// Returns None if the peer has never been seen.
    pub fn time_since_seen(&self, node_id: &str) -> Option<Duration> {
        self.last_seen.get(node_id).map(|instant| instant.elapsed())
    }

    /// Prunes all stale peers that haven't been seen within the staleness threshold.
    ///
    /// Returns the number of peers pruned.
    pub fn prune_stale(&mut self) -> usize {
        let threshold = self.config.staleness_threshold;
        let before_count = self.last_seen.len();

        self.last_seen.retain(|_, instant| instant.elapsed() <= threshold);

        let pruned = before_count - self.last_seen.len();

        if pruned > 0 {
            tracing::info!(
                pruned_count = pruned,
                remaining = self.last_seen.len(),
                "Pruned stale peers"
            );
        }

        pruned
    }

    /// Prunes peers not seen within a custom duration.
    ///
    /// Useful for testing or one-off cleanup with different thresholds.
    pub fn prune_older_than(&mut self, max_age: Duration) -> usize {
        let before_count = self.last_seen.len();

        self.last_seen.retain(|_, instant| instant.elapsed() <= max_age);

        before_count - self.last_seen.len()
    }

    /// Returns the number of tracked peers.
    pub fn peer_count(&self) -> usize {
        self.last_seen.len()
    }

    /// Returns all tracked peer IDs.
    pub fn peer_ids(&self) -> impl Iterator<Item = &str> {
        self.last_seen.keys().map(|s| s.as_str())
    }

    /// Returns the staleness threshold from config.
    pub fn staleness_threshold(&self) -> Duration {
        self.config.staleness_threshold
    }

    /// Returns the maintenance interval from config.
    pub fn maintenance_interval(&self) -> Duration {
        self.config.maintenance_interval
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn test_new_tracker_is_empty() {
        let tracker = PeerTracker::new();
        assert_eq!(tracker.peer_count(), 0);
    }

    #[test]
    fn test_record_seen() {
        let mut tracker = PeerTracker::new();

        tracker.record_seen("node-1");
        assert_eq!(tracker.peer_count(), 1);
        assert!(tracker.get_last_seen("node-1").is_some());
    }

    #[test]
    fn test_unknown_peer_returns_none() {
        let tracker = PeerTracker::new();
        assert!(tracker.get_last_seen("unknown").is_none());
        assert!(tracker.time_since_seen("unknown").is_none());
    }

    #[test]
    fn test_is_stale_never_seen() {
        let tracker = PeerTracker::new();
        assert!(tracker.is_stale("never-seen"));
    }

    #[test]
    fn test_is_stale_recently_seen() {
        let mut tracker = PeerTracker::new();
        tracker.record_seen("node-1");
        assert!(!tracker.is_stale("node-1"));
    }

    #[test]
    fn test_prune_stale_removes_old_peers() {
        // Create tracker with very short staleness threshold for testing
        let config = PeerTrackerConfig {
            staleness_threshold: Duration::from_millis(10),
            maintenance_interval: DEFAULT_MAINTENANCE_INTERVAL,
        };
        let mut tracker = PeerTracker::with_config(config);

        tracker.record_seen("node-1");
        tracker.record_seen("node-2");
        assert_eq!(tracker.peer_count(), 2);

        // Wait for peers to become stale
        std::thread::sleep(Duration::from_millis(20));

        let pruned = tracker.prune_stale();
        assert_eq!(pruned, 2);
        assert_eq!(tracker.peer_count(), 0);
    }

    #[test]
    fn test_prune_keeps_recent_peers() {
        let config = PeerTrackerConfig {
            staleness_threshold: Duration::from_secs(60),
            maintenance_interval: DEFAULT_MAINTENANCE_INTERVAL,
        };
        let mut tracker = PeerTracker::with_config(config);

        tracker.record_seen("node-1");
        tracker.record_seen("node-2");

        // Prune immediately - nothing should be removed
        let pruned = tracker.prune_stale();
        assert_eq!(pruned, 0);
        assert_eq!(tracker.peer_count(), 2);
    }

    #[test]
    fn test_prune_older_than_custom_duration() {
        let mut tracker = PeerTracker::new();

        tracker.record_seen("node-1");
        std::thread::sleep(Duration::from_millis(20));
        tracker.record_seen("node-2");

        // Prune peers older than 10ms - should remove node-1
        let pruned = tracker.prune_older_than(Duration::from_millis(10));
        assert_eq!(pruned, 1);
        assert!(tracker.get_last_seen("node-1").is_none());
        assert!(tracker.get_last_seen("node-2").is_some());
    }

    #[test]
    fn test_updates_timestamp_on_repeated_seen() {
        let mut tracker = PeerTracker::new();

        tracker.record_seen("node-1");
        let first = tracker.time_since_seen("node-1").unwrap();

        std::thread::sleep(Duration::from_millis(10));

        tracker.record_seen("node-1");
        let second = tracker.time_since_seen("node-1").unwrap();

        // Second should be shorter (more recent)
        assert!(second < first + Duration::from_millis(5));
    }

    #[test]
    fn test_peer_ids_iteration() {
        let mut tracker = PeerTracker::new();

        tracker.record_seen("node-1");
        tracker.record_seen("node-2");
        tracker.record_seen("node-3");

        let ids: Vec<_> = tracker.peer_ids().collect();
        assert_eq!(ids.len(), 3);
        assert!(ids.contains(&"node-1"));
        assert!(ids.contains(&"node-2"));
        assert!(ids.contains(&"node-3"));
    }

    #[test]
    fn test_default_config_values() {
        let config = PeerTrackerConfig::default();
        assert_eq!(config.staleness_threshold, Duration::from_secs(60 * 60));
        assert_eq!(config.maintenance_interval, Duration::from_secs(5 * 60));
    }

    #[test]
    fn test_peer_tracker_config_builder_with_defaults() {
        let config = PeerTrackerConfig::builder().build();
        assert_eq!(config.staleness_threshold, Duration::from_secs(60 * 60));
        assert_eq!(config.maintenance_interval, Duration::from_secs(5 * 60));
    }

    #[test]
    fn test_peer_tracker_config_builder_with_custom_values() {
        let config = PeerTrackerConfig::builder()
            .staleness_threshold(Duration::from_secs(120))
            .maintenance_interval(Duration::from_secs(30))
            .build();
        assert_eq!(config.staleness_threshold, Duration::from_secs(120));
        assert_eq!(config.maintenance_interval, Duration::from_secs(30));
    }

    #[test]
    fn test_peer_tracker_config_builder_matches_default() {
        let from_builder = PeerTrackerConfig::builder().build();
        let from_default = PeerTrackerConfig::default();
        assert_eq!(from_builder.staleness_threshold, from_default.staleness_threshold);
        assert_eq!(from_builder.maintenance_interval, from_default.maintenance_interval);
    }

    #[test]
    fn test_timestamp_is_reasonable() {
        let mut tracker = PeerTracker::new();

        tracker.record_seen("node-1");
        let ts = tracker.get_last_seen("node-1").unwrap();

        // Timestamp should be recent (within last minute)
        let now =
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()
                as i64;

        assert!(
            ts.seconds >= now - 60 && ts.seconds <= now + 1,
            "Timestamp should be within last minute: got {}, now {}",
            ts.seconds,
            now
        );
    }
}
