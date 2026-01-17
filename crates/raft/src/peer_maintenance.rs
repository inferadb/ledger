//! Background maintenance task for peer tracking.
//!
//! Per DESIGN.md §3.6, this module provides:
//! - Periodic pruning of stale peers (not seen in >1 hour)
//! - Maintenance runs every 5 minutes by default
//!
//! ## Usage
//!
//! ```ignore
//! let maintenance = PeerMaintenance::new(discovery_service);
//! let handle = maintenance.start();
//! // ... later ...
//! handle.abort(); // to stop maintenance
//! ```

use std::{sync::Arc, time::Duration};

use tokio::time::interval;
use tracing::{debug, info};

use crate::services::DiscoveryServiceImpl;

/// Default maintenance interval: run pruning every 5 minutes.
pub const DEFAULT_MAINTENANCE_INTERVAL: Duration = Duration::from_secs(5 * 60);

/// Background maintenance task for peer tracking.
///
/// Periodically prunes stale peers from the discovery service's tracker.
pub struct PeerMaintenance {
    /// The discovery service to maintain.
    discovery: Arc<DiscoveryServiceImpl>,
    /// Interval between maintenance cycles.
    interval: Duration,
}

impl PeerMaintenance {
    /// Create a new peer maintenance task.
    pub fn new(discovery: Arc<DiscoveryServiceImpl>) -> Self {
        let interval = discovery.maintenance_interval();
        Self { discovery, interval }
    }

    /// Create with a custom interval (for testing).
    #[cfg(test)]
    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    /// Run a single maintenance cycle.
    fn run_cycle(&self) {
        let pruned = self.discovery.prune_stale_peers();
        let remaining = self.discovery.peer_count();

        if pruned > 0 {
            info!(
                pruned_count = pruned,
                remaining_peers = remaining,
                "Peer maintenance: pruned stale peers"
            );
        } else {
            debug!(remaining_peers = remaining, "Peer maintenance: no stale peers");
        }
    }

    /// Start the maintenance background task.
    ///
    /// Returns a handle that can be used to abort the task.
    pub fn start(self) -> tokio::task::JoinHandle<()> {
        info!(interval_secs = self.interval.as_secs(), "Starting peer maintenance task");

        tokio::spawn(async move {
            let mut ticker = interval(self.interval);

            loop {
                ticker.tick().await;
                self.run_cycle();
            }
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods, clippy::panic)]
mod tests {
    // Note: Full tests require setting up DiscoveryServiceImpl with mocked Raft.
    // The maintenance logic is simple enough that it's exercised through
    // integration tests.

    use super::*;

    #[test]
    fn test_default_interval() {
        assert_eq!(DEFAULT_MAINTENANCE_INTERVAL.as_secs(), 5 * 60);
    }
}
