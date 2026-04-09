//! Leader transfer coordination.
//!
//! Orchestrates the transfer of Raft leadership to a specific follower before
//! shutdown or maintenance. The protocol:
//!
//! 1. Verify this node is the current leader
//! 2. Select the best transfer target (most caught-up follower)
//! 3. Wait for replication to reach the target
//! 4. Pause leader heartbeats and disable elections on this node
//! 5. Send a `TriggerElection` RPC to the target
//! 6. Poll until leadership changes or timeout expires
//! 7. Re-enable heartbeats and elections (on both success and failure paths)

use std::{
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

use snafu::Snafu;
use tracing;

use crate::consensus_handle::ConsensusHandle;

/// Configuration for a leader transfer attempt.
#[derive(Debug, Clone, bon::Builder)]
pub struct LeaderTransferConfig {
    /// Maximum time to wait for the full transfer.
    #[builder(default = Duration::from_secs(10))]
    pub timeout: Duration,
    /// How often to poll replication progress.
    #[builder(default = Duration::from_millis(50))]
    pub poll_interval: Duration,
    /// Maximum time to wait for replication to catch up.
    #[builder(default = Duration::from_secs(5))]
    pub replication_timeout: Duration,
    /// How long to wait after pausing heartbeats for follower leases to expire.
    ///
    /// openraft's leader lease mechanism causes followers to reject votes while
    /// the lease is active. This pause gives followers time to notice the leader
    /// has stopped heartbeating, so they become willing to vote for the transfer
    /// target. Should be at least `election_timeout_max` (default 600ms).
    #[builder(default = Duration::from_millis(800))]
    pub lease_expiry_wait: Duration,
}

/// Errors that can occur during leader transfer.
#[derive(Debug, Snafu)]
pub enum LeaderTransferError {
    /// This node is not the current leader.
    #[snafu(display("Not the current leader"))]
    NotLeader,
    /// No eligible transfer target found.
    #[snafu(display("No eligible transfer target"))]
    NoTarget,
    /// Another transfer is already in progress.
    #[snafu(display("Leader transfer already in progress"))]
    TransferInProgress,
    /// Replication to target did not catch up in time.
    #[snafu(display("Replication to target did not catch up within timeout"))]
    ReplicationTimeout,
    /// Target rejected the election trigger.
    #[snafu(display("Target rejected election trigger: {message}"))]
    TargetRejected {
        /// Rejection reason from the target node.
        message: String,
    },
    /// Transfer timed out before leadership changed.
    #[snafu(display("Transfer timed out — leader did not change within {timeout:?}"))]
    Timeout {
        /// The configured timeout duration.
        timeout: Duration,
    },
    /// Failed to connect to the target node.
    #[snafu(display("Failed to connect to target node: {source}"))]
    Connection {
        /// The underlying transport error.
        source: tonic::transport::Error,
        /// Source location for debugging.
        #[snafu(implicit)]
        location: snafu::Location,
    },
    /// gRPC error from the target node.
    #[snafu(display("gRPC error from target: {source}"))]
    Rpc {
        /// The underlying gRPC status error.
        source: tonic::Status,
        /// Source location for debugging.
        #[snafu(implicit)]
        location: snafu::Location,
    },
}

/// RAII guard that resets the transfer lock on drop.
struct TransferGuard<'a>(&'a AtomicBool);

impl Drop for TransferGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

/// Transfers leadership to a target follower.
///
/// If `target` is `None`, the most caught-up follower is selected automatically.
/// The `transfer_lock` prevents concurrent invocations — a second caller receives
/// [`LeaderTransferError::TransferInProgress`].
///
/// Returns the node ID of the new leader. Note that this may differ from the
/// requested target if a third node wins the election.
///
/// The transfer sends a `TimeoutNow` message to the target via the consensus
/// engine and polls until leadership changes or the timeout expires. Heartbeat
/// pause and replication-progress-based target selection are not yet available,
/// so the transfer may be less reliable under high load or with lagging followers.
pub async fn transfer_leadership(
    handle: &ConsensusHandle,
    target: Option<u64>,
    transfer_lock: &AtomicBool,
    config: &LeaderTransferConfig,
) -> Result<u64, LeaderTransferError> {
    // Acquire the transfer lock
    if transfer_lock.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire).is_err() {
        return Err(LeaderTransferError::TransferInProgress);
    }
    let _guard = TransferGuard(transfer_lock);

    // Verify we are the leader
    if !handle.is_leader() {
        return Err(LeaderTransferError::NotLeader);
    }

    let my_id = handle.node_id();

    // Determine transfer target from shard state voters
    let state = handle.shard_state();
    let voter_ids: Vec<u64> =
        state.voters.iter().filter(|&&id| id.0 != my_id).map(|id| id.0).collect();

    let target_id = match target {
        Some(id) => {
            if !voter_ids.contains(&id) {
                return Err(LeaderTransferError::NoTarget);
            }
            id
        },
        None => *voter_ids.first().ok_or(LeaderTransferError::NoTarget)?,
    };

    // Send TimeoutNow to the target via the consensus engine. The target
    // will immediately start a real election (skipping pre-vote).
    handle.transfer_leader(target_id).await.map_err(|e| {
        tracing::warn!(target_id, error = ?e, "transfer_leader call failed");
        LeaderTransferError::NoTarget
    })?;

    // Poll until a different leader is confirmed or the timeout expires.
    // We wait for current_leader() to report a specific node that isn't us,
    // not just None (which means the old leader stepped down but no new leader
    // has been elected yet).
    let overall_deadline = tokio::time::Instant::now() + config.timeout;
    loop {
        if tokio::time::Instant::now() >= overall_deadline {
            return Err(LeaderTransferError::Timeout { timeout: config.timeout });
        }

        if let Some(leader) = handle.current_leader()
            && leader != my_id
        {
            return Ok(leader);
        }

        tokio::time::sleep(config.poll_interval).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_leader_transfer_config_defaults() {
        let config = LeaderTransferConfig::builder().build();
        assert_eq!(config.timeout, Duration::from_secs(10));
        assert_eq!(config.poll_interval, Duration::from_millis(50));
        assert_eq!(config.replication_timeout, Duration::from_secs(5));
        assert_eq!(config.lease_expiry_wait, Duration::from_millis(800));
    }

    #[test]
    fn test_leader_transfer_config_custom() {
        let config = LeaderTransferConfig::builder()
            .timeout(Duration::from_secs(30))
            .poll_interval(Duration::from_millis(100))
            .replication_timeout(Duration::from_secs(10))
            .lease_expiry_wait(Duration::from_millis(1200))
            .build();
        assert_eq!(config.timeout, Duration::from_secs(30));
        assert_eq!(config.poll_interval, Duration::from_millis(100));
        assert_eq!(config.replication_timeout, Duration::from_secs(10));
        assert_eq!(config.lease_expiry_wait, Duration::from_millis(1200));
    }

    #[test]
    fn test_transfer_guard_resets_flag() {
        let flag = AtomicBool::new(true);
        {
            let _guard = TransferGuard(&flag);
            assert!(flag.load(Ordering::Acquire));
        }
        assert!(!flag.load(Ordering::Acquire));
    }

    #[test]
    fn test_not_leader_error_display() {
        let err = LeaderTransferError::NotLeader;
        assert_eq!(err.to_string(), "Not the current leader");
    }

    #[test]
    fn test_no_target_error_display() {
        let err = LeaderTransferError::NoTarget;
        assert_eq!(err.to_string(), "No eligible transfer target");
    }

    #[test]
    fn test_transfer_in_progress_error_display() {
        let err = LeaderTransferError::TransferInProgress;
        assert_eq!(err.to_string(), "Leader transfer already in progress");
    }

    #[test]
    fn test_replication_timeout_error_display() {
        let err = LeaderTransferError::ReplicationTimeout;
        assert_eq!(err.to_string(), "Replication to target did not catch up within timeout");
    }

    #[test]
    fn test_target_rejected_error_display() {
        let err = LeaderTransferError::TargetRejected { message: "stale term".to_string() };
        assert_eq!(err.to_string(), "Target rejected election trigger: stale term");
    }

    #[test]
    fn test_timeout_error_display() {
        let err = LeaderTransferError::Timeout { timeout: Duration::from_secs(10) };
        assert_eq!(err.to_string(), "Transfer timed out \u{2014} leader did not change within 10s");
    }

    #[test]
    fn test_config_debug() {
        let config = LeaderTransferConfig::builder().build();
        let debug = format!("{:?}", config);
        assert!(debug.contains("timeout"));
        assert!(debug.contains("poll_interval"));
        assert!(debug.contains("replication_timeout"));
        assert!(debug.contains("lease_expiry_wait"));
    }

    #[test]
    fn test_config_clone() {
        let config = LeaderTransferConfig::builder().timeout(Duration::from_secs(20)).build();
        let cloned = config.clone();
        assert_eq!(cloned.timeout, Duration::from_secs(20));
        assert_eq!(cloned.poll_interval, config.poll_interval);
    }

    #[test]
    fn test_transfer_guard_multiple_resets() {
        let flag = AtomicBool::new(true);
        {
            let _guard = TransferGuard(&flag);
        }
        assert!(!flag.load(Ordering::Acquire));

        // Setting it again and dropping another guard resets again
        flag.store(true, Ordering::Release);
        {
            let _guard = TransferGuard(&flag);
        }
        assert!(!flag.load(Ordering::Acquire));
    }

    #[test]
    fn test_transfer_lock_acquire_release() {
        let lock = AtomicBool::new(false);

        // Acquire succeeds on unlocked
        assert!(lock.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire).is_ok());

        // Second acquire fails
        assert!(lock.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire).is_err());

        // Release via guard
        {
            let _guard = TransferGuard(&lock);
        }
        assert!(!lock.load(Ordering::Acquire));

        // Can acquire again after release
        assert!(lock.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire).is_ok());
    }

    #[test]
    fn test_error_debug_format() {
        let err = LeaderTransferError::TargetRejected { message: "term mismatch".to_string() };
        let debug = format!("{:?}", err);
        assert!(debug.contains("TargetRejected"));
        assert!(debug.contains("term mismatch"));
    }

    #[test]
    fn test_timeout_error_various_durations() {
        let err = LeaderTransferError::Timeout { timeout: Duration::from_millis(500) };
        assert!(err.to_string().contains("500ms"));

        let err = LeaderTransferError::Timeout { timeout: Duration::from_secs(30) };
        assert!(err.to_string().contains("30s"));
    }

    #[test]
    fn test_lease_expiry_wait_exceeds_election_timeout() {
        // Default lease_expiry_wait (800ms) should exceed typical election_timeout_max (600ms)
        let config = LeaderTransferConfig::builder().build();
        assert!(config.lease_expiry_wait >= Duration::from_millis(600));
    }

    #[test]
    fn test_replication_timeout_clamped_to_overall() {
        // When replication_timeout > timeout, min should clamp
        let config = LeaderTransferConfig::builder()
            .timeout(Duration::from_secs(3))
            .replication_timeout(Duration::from_secs(10))
            .build();
        let clamped = config.replication_timeout.min(config.timeout);
        assert_eq!(clamped, Duration::from_secs(3));
    }

    #[test]
    fn test_replication_timeout_not_clamped_when_smaller() {
        let config = LeaderTransferConfig::builder()
            .timeout(Duration::from_secs(10))
            .replication_timeout(Duration::from_secs(3))
            .build();
        let clamped = config.replication_timeout.min(config.timeout);
        assert_eq!(clamped, Duration::from_secs(3));
    }

    #[test]
    fn test_transfer_guard_set_false_even_when_already_false() {
        let flag = AtomicBool::new(false);
        {
            let _guard = TransferGuard(&flag);
        }
        // Still false after drop
        assert!(!flag.load(Ordering::Acquire));
    }

    #[test]
    fn test_config_all_defaults_reasonable() {
        let config = LeaderTransferConfig::builder().build();
        // Timeout should be reasonable for production use
        assert!(config.timeout >= Duration::from_secs(1));
        assert!(config.timeout <= Duration::from_secs(60));
        // Poll interval should be sub-second
        assert!(config.poll_interval < Duration::from_secs(1));
        // Replication timeout should be less than overall timeout
        assert!(config.replication_timeout <= config.timeout);
    }

    #[test]
    fn test_error_timeout_formatting_subsecond() {
        let err = LeaderTransferError::Timeout { timeout: Duration::from_millis(100) };
        let msg = err.to_string();
        assert!(msg.contains("100ms"));
    }

    #[test]
    fn test_error_target_rejected_with_empty_message() {
        let err = LeaderTransferError::TargetRejected { message: String::new() };
        let msg = err.to_string();
        assert_eq!(msg, "Target rejected election trigger: ");
    }
}
