//! Leader transfer coordination.
//!
//! Orchestrates the transfer of Raft leadership to a specific follower before
//! shutdown or maintenance. The protocol:
//!
//! 1. Verify this node is the current leader
//! 2. Select the best transfer target (most caught-up follower)
//! 3. Wait for replication to reach the target
//! 4. Send a `TriggerElection` RPC to the target
//! 5. Poll until leadership changes or timeout expires

use std::{
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

use inferadb_ledger_proto::proto::{
    TriggerElectionRequest, raft_service_client::RaftServiceClient,
};
use openraft::Raft;
use snafu::{ResultExt, Snafu};
use tonic::transport::Channel;
use tracing::info;

use crate::types::LedgerTypeConfig;

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
pub async fn transfer_leadership(
    raft: &Raft<LedgerTypeConfig>,
    target: Option<u64>,
    transfer_lock: &AtomicBool,
    config: &LeaderTransferConfig,
) -> Result<u64, LeaderTransferError> {
    // Acquire the transfer lock
    if transfer_lock.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire).is_err() {
        return Err(LeaderTransferError::TransferInProgress);
    }
    let _guard = TransferGuard(transfer_lock);

    let overall_deadline = tokio::time::Instant::now() + config.timeout;

    // Step 1: Verify we are the leader and extract metrics
    let metrics = raft.metrics().borrow().clone();
    let my_id = metrics.id;

    if metrics.current_leader != Some(my_id) {
        return Err(LeaderTransferError::NotLeader);
    }

    let current_term = metrics.current_term;
    let last_log_index = metrics.last_log_index.unwrap_or(0);

    // Step 2: Determine transfer target
    let replication = metrics.replication.as_ref().ok_or(LeaderTransferError::NotLeader)?;

    let target_id = match target {
        Some(id) => {
            // Verify the specified target is in the replication map
            if !replication.contains_key(&id) {
                return Err(LeaderTransferError::NoTarget);
            }
            id
        },
        None => {
            // Pick the most caught-up follower
            replication
                .iter()
                .filter(|(id, _)| **id != my_id)
                .filter_map(|(id, log_id)| log_id.as_ref().map(|lid| (*id, lid.index)))
                .max_by_key(|(_, index)| *index)
                .map(|(id, _)| id)
                .ok_or(LeaderTransferError::NoTarget)?
        },
    };

    info!(target_id, last_log_index, "Starting leader transfer");

    // Step 3: Wait for replication to catch up
    let repl_deadline =
        tokio::time::Instant::now() + config.replication_timeout.min(config.timeout);

    loop {
        let fresh = raft.metrics().borrow().clone();
        if let Some(repl) = &fresh.replication
            && let Some(Some(log_id)) = repl.get(&target_id)
            && log_id.index >= last_log_index
        {
            break;
        }

        if tokio::time::Instant::now() >= repl_deadline {
            return Err(LeaderTransferError::ReplicationTimeout);
        }

        tokio::time::sleep(config.poll_interval).await;
    }

    // Step 4: Connect to target and send TriggerElection
    // Re-read metrics to get fresh membership data (may have changed during replication wait).
    let fresh_metrics = raft.metrics().borrow().clone();
    let membership = fresh_metrics.membership_config.membership();
    let target_node = membership
        .nodes()
        .find(|(id, _)| **id == target_id)
        .map(|(_, node)| node)
        .ok_or(LeaderTransferError::NoTarget)?;

    let endpoint = format!("http://{}", target_node.addr);
    let channel: Channel = Channel::from_shared(endpoint)
        .map_err(|e| LeaderTransferError::TargetRejected {
            message: format!("Invalid endpoint: {e}"),
        })?
        .connect()
        .await
        .context(ConnectionSnafu)?;

    let mut client: RaftServiceClient<Channel> = RaftServiceClient::new(channel);
    let response = client
        .trigger_election(TriggerElectionRequest { leader_term: current_term, leader_id: my_id })
        .await
        .context(RpcSnafu)?;

    let inner = response.into_inner();
    if !inner.accepted {
        return Err(LeaderTransferError::TargetRejected { message: inner.message });
    }

    // Step 5: Poll until a different leader is confirmed.
    // During the election transition there is a brief window where
    // `current_leader` is `None`. We must wait through that window until
    // a concrete new leader ID appears, otherwise we return 0.
    loop {
        let fresh = raft.metrics().borrow().clone();
        if let Some(leader) = fresh.current_leader
            && leader != my_id
        {
            info!(new_leader = leader, target_id, "Leadership transferred");
            return Ok(leader);
        }

        if tokio::time::Instant::now() >= overall_deadline {
            return Err(LeaderTransferError::Timeout { timeout: config.timeout });
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
    }

    #[test]
    fn test_leader_transfer_config_custom() {
        let config = LeaderTransferConfig::builder()
            .timeout(Duration::from_secs(30))
            .poll_interval(Duration::from_millis(100))
            .replication_timeout(Duration::from_secs(10))
            .build();
        assert_eq!(config.timeout, Duration::from_secs(30));
        assert_eq!(config.poll_interval, Duration::from_millis(100));
        assert_eq!(config.replication_timeout, Duration::from_secs(10));
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
    fn test_transfer_lock_prevents_concurrent() {
        let lock = AtomicBool::new(true); // already locked
        let result = lock.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire);
        assert!(result.is_err());
    }

    #[test]
    fn test_error_display() {
        let err = LeaderTransferError::NotLeader;
        assert_eq!(err.to_string(), "Not the current leader");

        let err = LeaderTransferError::NoTarget;
        assert_eq!(err.to_string(), "No eligible transfer target");

        let err = LeaderTransferError::TransferInProgress;
        assert_eq!(err.to_string(), "Leader transfer already in progress");

        let err = LeaderTransferError::ReplicationTimeout;
        assert_eq!(err.to_string(), "Replication to target did not catch up within timeout");

        let err = LeaderTransferError::TargetRejected { message: "stale term".to_string() };
        assert_eq!(err.to_string(), "Target rejected election trigger: stale term");

        let err = LeaderTransferError::Timeout { timeout: Duration::from_secs(10) };
        assert_eq!(err.to_string(), "Transfer timed out \u{2014} leader did not change within 10s");
    }
}
