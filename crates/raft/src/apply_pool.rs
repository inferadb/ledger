//! Pool of apply workers for cross-shard parallelism.
//!
//! Routes committed batches to workers based on `shard_id % num_workers`,
//! guaranteeing in-order apply per shard while enabling parallel apply
//! across shards.

use inferadb_ledger_consensus::committed::CommittedBatch;
use tokio::{sync::mpsc, task::JoinHandle};

/// Distributes committed batches across a pool of apply workers.
pub struct ApplyPool {
    /// Per-worker send channels.
    worker_txs: Vec<mpsc::Sender<CommittedBatch>>,
    /// Worker task handles for health monitoring.
    worker_handles: Vec<JoinHandle<()>>,
}

impl ApplyPool {
    /// Creates a new pool. Returns the pool and per-worker receivers.
    ///
    /// The caller spawns one `ApplyWorker::run` per receiver.
    pub fn new(num_workers: usize) -> (Self, Vec<mpsc::Receiver<CommittedBatch>>) {
        assert!(num_workers > 0, "apply pool requires at least 1 worker");
        let mut txs = Vec::with_capacity(num_workers);
        let mut rxs = Vec::with_capacity(num_workers);
        for _ in 0..num_workers {
            let (tx, rx) = mpsc::channel(1000);
            txs.push(tx);
            rxs.push(rx);
        }
        (Self { worker_txs: txs, worker_handles: Vec::new() }, rxs)
    }

    /// Registers worker task handles for health monitoring.
    pub fn set_handles(&mut self, handles: Vec<JoinHandle<()>>) {
        self.worker_handles = handles;
    }

    /// Routes a committed batch to the appropriate worker.
    ///
    /// Worker selection: `shard_id % num_workers` ensures all batches
    /// for a given shard go to the same worker (in-order guarantee).
    pub async fn dispatch(&self, batch: CommittedBatch) {
        if self.worker_txs.is_empty() {
            return;
        }
        let worker_idx = batch.shard.0 as usize % self.worker_txs.len();
        if self.worker_txs[worker_idx].send(batch).await.is_err() {
            tracing::error!(worker = worker_idx, "Apply worker channel closed");
        }
    }

    /// Returns the number of workers.
    pub fn num_workers(&self) -> usize {
        self.worker_txs.len()
    }

    /// Checks if any worker has terminated (panicked or completed).
    pub fn check_health(&self) -> Vec<usize> {
        self.worker_handles
            .iter()
            .enumerate()
            .filter(|(_, h)| h.is_finished())
            .map(|(i, _)| i)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use inferadb_ledger_consensus::{committed::CommittedBatch, types::ConsensusStateId};

    use super::*;

    #[tokio::test]
    async fn dispatch_routes_by_shard_id() {
        let (pool, mut rxs) = ApplyPool::new(3);

        // ConsensusState 0 → worker 0, ConsensusState 1 → worker 1, ConsensusState 3 → worker 0
        pool.dispatch(CommittedBatch {
            shard: ConsensusStateId(0),
            entries: vec![],
            leader_node: None,
        })
        .await;
        pool.dispatch(CommittedBatch {
            shard: ConsensusStateId(1),
            entries: vec![],
            leader_node: None,
        })
        .await;
        pool.dispatch(CommittedBatch {
            shard: ConsensusStateId(3),
            entries: vec![],
            leader_node: None,
        })
        .await;

        // Worker 0 got 2 batches (shard 0 and 3)
        assert!(rxs[0].try_recv().is_ok());
        assert!(rxs[0].try_recv().is_ok());
        assert!(rxs[0].try_recv().is_err());

        // Worker 1 got 1 batch (shard 1)
        assert!(rxs[1].try_recv().is_ok());
        assert!(rxs[1].try_recv().is_err());

        // Worker 2 got nothing
        assert!(rxs[2].try_recv().is_err());
    }

    #[test]
    fn num_workers_matches_creation() {
        let (pool, _rxs) = ApplyPool::new(4);
        assert_eq!(pool.num_workers(), 4);
    }

    #[test]
    fn health_check_empty_when_no_handles() {
        let (pool, _rxs) = ApplyPool::new(2);
        assert!(pool.check_health().is_empty());
    }
}
