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

use inferadb_ledger_proto::proto::{
    GetClusterInfoRequest, TriggerElectionRequest, admin_service_client::AdminServiceClient,
    raft_service_client::RaftServiceClient,
};
use openraft::Raft;
use snafu::{ResultExt, Snafu};
use tonic::transport::Channel;
use tracing::{debug, info};

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

/// RAII guard that re-enables leader heartbeats and elections on drop.
///
/// During leader transfer, heartbeats must be paused so follower leases expire,
/// and elections must be disabled on the old leader to prevent it from winning
/// re-election (which creates a livelock since it can't heartbeat). This guard
/// ensures both are always restored, even if the transfer fails.
struct TransferModeGuard<'a>(&'a Raft<LedgerTypeConfig>);

impl Drop for TransferModeGuard<'_> {
    fn drop(&mut self) {
        self.0.runtime_config().heartbeat(true);
        self.0.runtime_config().elect(true);
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

    // Step 4: Connect to target (while heartbeats are still running so the
    // target stays healthy). Resolve the address before pausing heartbeats
    // to minimize the window where the cluster has no heartbeats.
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

    let mut raft_client = RaftServiceClient::new(channel.clone());
    let mut admin_client = AdminServiceClient::new(channel);

    // Step 5: Pause heartbeats and disable elections on this node.
    // Heartbeat pause: followers' leases expire after ~election_timeout_max
    // (~600ms), making them willing to vote for the transfer target.
    // Election disable: prevents this node from winning re-election after it
    // steps down (which would create a livelock since it can't heartbeat).
    info!(target_id, "Pausing heartbeats and disabling elections for transfer");
    raft.runtime_config().heartbeat(false);
    raft.runtime_config().elect(false);
    let _transfer_guard = TransferModeGuard(raft);

    tokio::time::sleep(config.lease_expiry_wait).await;

    // Steps 6-7: Repeatedly trigger election on the target until leadership
    // changes or the overall deadline expires. A single trigger().elect() may
    // fail due to split votes or term races, so we retry periodically.
    //
    // We poll leadership via GetClusterInfo on the target node rather than
    // local raft metrics, because the local RaftCore may become unresponsive
    // after stepping down (e.g., assertion failures in debug builds when
    // receiving AppendEntries from the new leader).
    let mut attempt = 0u32;
    loop {
        if tokio::time::Instant::now() >= overall_deadline {
            return Err(LeaderTransferError::Timeout { timeout: config.timeout });
        }

        // Read current term from local metrics (may be stale if core panicked,
        // but best-effort for the TriggerElection term check).
        let local = raft.metrics().borrow().clone();
        let trigger_term = local.current_term;

        // Check local metrics first — works when the core is healthy.
        if let Some(leader) = local.current_leader
            && leader != my_id
        {
            info!(new_leader = leader, target_id, attempt, "Leadership transferred (local)");
            return Ok(leader);
        }

        attempt += 1;
        info!(target_id, leader_term = trigger_term, attempt, "Sending TriggerElection RPC");

        match raft_client
            .trigger_election(TriggerElectionRequest {
                leader_term: trigger_term,
                leader_id: my_id,
            })
            .await
        {
            Ok(response) => {
                let inner = response.into_inner();
                if !inner.accepted {
                    debug!(message = %inner.message, "Target rejected election trigger");
                }
            },
            Err(status) => {
                // Stale term or transient error — log and continue.
                debug!(code = ?status.code(), message = %status.message(), "TriggerElection RPC failed, retrying");
            },
        }

        // Poll the target via GetClusterInfo to confirm leadership change.
        // This is more reliable than local metrics after the old leader steps
        // down, since the target's RaftCore is healthy and sees the new leader.
        let retry_deadline =
            (tokio::time::Instant::now() + Duration::from_secs(2)).min(overall_deadline);

        loop {
            if let Ok(resp) = admin_client.get_cluster_info(GetClusterInfoRequest {}).await {
                let info = resp.into_inner();
                if info.leader_id != 0 && info.leader_id != my_id {
                    info!(
                        new_leader = info.leader_id,
                        target_id, attempt, "Leadership transferred (confirmed via target)"
                    );
                    return Ok(info.leader_id);
                }
            }

            if tokio::time::Instant::now() >= retry_deadline {
                break;
            }

            tokio::time::sleep(config.poll_interval).await;
        }
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
