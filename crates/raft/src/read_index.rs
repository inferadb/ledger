//! ReadIndex protocol helpers for follower linearizable reads.
//!
//! The ReadIndex protocol allows followers to serve linearizable reads:
//! 1. Follower calls `CommittedIndex` RPC on the leader to get the committed index.
//! 2. Follower waits until its local applied index catches up to the committed index.
//! 3. Follower serves the read from local state.
//!
//! This module provides the wait step (step 2).

use std::time::Duration;

use tonic::Status;

/// Waits until the local applied index reaches the target committed index.
///
/// Returns `Ok(())` once the applied index (observed via the watch channel)
/// reaches or exceeds `target_index`. Returns an error if the timeout expires
/// or the watch channel is closed.
///
/// # Errors
///
/// - `Status::deadline_exceeded` if the timeout elapses before the index is reached.
/// - `Status::internal` if the watch channel sender is dropped.
pub async fn wait_for_apply(
    watch: &mut tokio::sync::watch::Receiver<u64>,
    target_index: u64,
    timeout: Duration,
) -> Result<(), Status> {
    if *watch.borrow() >= target_index {
        return Ok(());
    }
    tokio::time::timeout(timeout, watch.wait_for(|&idx| idx >= target_index))
        .await
        .map_err(|_| Status::deadline_exceeded("Timed out waiting for apply"))?
        .map_err(|_| Status::internal("Applied index watch closed"))?;
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::disallowed_methods)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn already_caught_up() {
        let (tx, mut rx) = tokio::sync::watch::channel(10u64);
        drop(tx);
        let result = wait_for_apply(&mut rx, 5, Duration::from_millis(100)).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn catches_up_after_send() {
        let (tx, mut rx) = tokio::sync::watch::channel(0u64);
        let handle = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            let _ = tx.send(10);
        });
        let result = wait_for_apply(&mut rx, 10, Duration::from_secs(1)).await;
        assert!(result.is_ok());
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn timeout_when_index_not_reached() {
        let (_tx, mut rx) = tokio::sync::watch::channel(0u64);
        let result = wait_for_apply(&mut rx, 100, Duration::from_millis(10)).await;
        assert!(result.is_err());
        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::DeadlineExceeded);
    }

    #[tokio::test]
    async fn channel_closed() {
        let (tx, mut rx) = tokio::sync::watch::channel(0u64);
        drop(tx);
        let result = wait_for_apply(&mut rx, 10, Duration::from_millis(100)).await;
        assert!(result.is_err());
        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::Internal);
    }
}
