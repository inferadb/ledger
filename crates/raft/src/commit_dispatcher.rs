//! Commit dispatcher — fans `CommittedBatch` values out from a single
//! [`ConsensusEngine`](inferadb_ledger_consensus::ConsensusEngine) commit
//! channel to per-shard apply-worker channels.
//!
//! ## Why this exists
//!
//! `ConsensusEngine::start` returns one `mpsc::Receiver<CommittedBatch>` for
//! the entire engine. After P2a's `add_shard` / `remove_shard` landed, an
//! engine can host more than one shard — but every shard's committed batches
//! still arrive on that single receiver. Plumbing the raw receiver into a
//! single apply worker (the org's worker) makes batches for newly added
//! shards land on the wrong apply pipeline.
//!
//! [`CommitDispatcher`] sits between the engine's commit receiver and the
//! per-shard apply workers: the dispatcher's background task reads each
//! [`CommittedBatch`], looks up the registered downstream by
//! [`ConsensusStateId`], and forwards. Per-shard apply workers register on
//! shard start and deregister on shard stop.
//!
//! ## Lifecycle invariants
//!
//! - **One dispatcher per engine** (i.e. per region's `ConsensusEngine`).
//! - **Register before propose**: a shard must call [`register`] before its
//!   first batch can arrive on the engine's commit channel — in practice
//!   that means before
//!   [`ConsensusEngine::add_shard`](inferadb_ledger_consensus::ConsensusEngine::add_shard)
//!   completes.
//! - **Deregister before remove**: a shard must call [`deregister`] before
//!   [`ConsensusEngine::remove_shard`](inferadb_ledger_consensus::ConsensusEngine::remove_shard)
//!   returns. After deregister, any in-flight batch for the shard is logged
//!   and dropped — Raft semantics guarantee a removed shard's state is no
//!   longer authoritative, so the drop is safe.
//! - **The dispatcher task exits when the engine's commit channel closes**
//!   (engine shutdown). No explicit teardown is required at engine shutdown.
//!
//! [`register`]: CommitDispatcher::register
//! [`deregister`]: CommitDispatcher::deregister

use std::{collections::HashMap, sync::Arc};

use inferadb_ledger_consensus::{committed::CommittedBatch, types::ConsensusStateId};
use parking_lot::RwLock;
use tokio::sync::mpsc;

/// Registration table mapping each shard to its per-shard downstream sender.
type Routes = Arc<RwLock<HashMap<ConsensusStateId, mpsc::Sender<CommittedBatch>>>>;

/// Routes [`CommittedBatch`] values from a single engine commit channel to
/// per-shard apply-worker channels.
///
/// See the [module documentation](self) for the lifecycle contract.
pub struct CommitDispatcher {
    routes: Routes,
    /// Background dispatcher task. Held to keep ownership; the task ends
    /// when the source `commit_rx` closes (engine shutdown).
    _task: tokio::task::JoinHandle<()>,
}

impl CommitDispatcher {
    /// Spawns a dispatcher task that drains `commit_rx` until the channel
    /// closes (engine shutdown), forwarding each batch to the registered
    /// per-shard sender.
    ///
    /// Initial routing table is empty; callers must [`register`] each shard
    /// before its batches can be observed.
    ///
    /// [`register`]: CommitDispatcher::register
    pub fn new(commit_rx: mpsc::Receiver<CommittedBatch>) -> Self {
        let routes: Routes = Arc::new(RwLock::new(HashMap::new()));
        let routes_for_task = Arc::clone(&routes);
        let task = tokio::spawn(async move {
            dispatcher_loop(commit_rx, routes_for_task).await;
        });
        Self { routes, _task: task }
    }

    /// Registers a per-shard downstream sender.
    ///
    /// Replacing a previous registration for the same shard drops the old
    /// sender; any batch in flight to the old sender is lost. In practice
    /// callers register exactly once per shard during `start_region` (org's
    /// own shard) or `start_vault_group` (vault's shard).
    pub fn register(&self, shard_id: ConsensusStateId, tx: mpsc::Sender<CommittedBatch>) {
        self.routes.write().insert(shard_id, tx);
    }

    /// Deregisters a per-shard downstream sender.
    ///
    /// Must be called **before**
    /// [`ConsensusEngine::remove_shard`](inferadb_ledger_consensus::ConsensusEngine::remove_shard)
    /// returns. In-flight batches for this shard after deregister are
    /// dropped with a warning log — safe because Raft semantics guarantee
    /// the shard's state is no longer authoritative after `remove_shard`.
    pub fn deregister(&self, shard_id: ConsensusStateId) {
        self.routes.write().remove(&shard_id);
    }
}

/// Background dispatch loop. Exits when the source channel closes.
async fn dispatcher_loop(mut commit_rx: mpsc::Receiver<CommittedBatch>, routes: Routes) {
    while let Some(batch) = commit_rx.recv().await {
        let shard_id = batch.shard;
        // Clone the sender out of the map under a short-lived read lock so
        // the await on `send` doesn't hold the lock across suspension.
        let sender_opt = routes.read().get(&shard_id).cloned();
        match sender_opt {
            Some(sender) => {
                if let Err(e) = sender.send(batch).await {
                    tracing::error!(
                        shard = shard_id.0,
                        error = %e,
                        "CommitDispatcher: downstream channel closed; batch dropped",
                    );
                }
            }
            None => {
                tracing::warn!(
                    shard = shard_id.0,
                    entry_count = batch.entries.len(),
                    "CommitDispatcher: no downstream registered; batch dropped",
                );
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use inferadb_ledger_consensus::{
        committed::{CommittedBatch, CommittedEntry},
        types::{ConsensusStateId, EntryKind},
    };
    use tokio::sync::mpsc;

    use super::CommitDispatcher;

    /// Builds a minimal `CommittedBatch` whose single entry's `data` carries
    /// the supplied tag — lets tests assert "batch B was routed to shard X"
    /// by reading the entry payload.
    fn batch_for(shard: ConsensusStateId, tag: &str) -> CommittedBatch {
        let entry = CommittedEntry {
            index: 1,
            term: 1,
            data: Arc::from(tag.as_bytes()),
            kind: EntryKind::Normal,
        };
        CommittedBatch { shard, entries: vec![entry], leader_node: None }
    }

    fn tag_of(batch: &CommittedBatch) -> String {
        let entry = batch.entries.first().expect("batch has at least one entry");
        String::from_utf8(entry.data.to_vec()).expect("tag is valid UTF-8")
    }

    #[tokio::test]
    async fn routes_batches_to_correct_downstream() {
        let (src_tx, src_rx) = mpsc::channel::<CommittedBatch>(16);
        let dispatcher = CommitDispatcher::new(src_rx);

        let (shard_a_tx, mut shard_a_rx) = mpsc::channel::<CommittedBatch>(16);
        let (shard_b_tx, mut shard_b_rx) = mpsc::channel::<CommittedBatch>(16);

        dispatcher.register(ConsensusStateId(1), shard_a_tx);
        dispatcher.register(ConsensusStateId(2), shard_b_tx);

        src_tx.send(batch_for(ConsensusStateId(1), "a1")).await.expect("src send a1");
        src_tx.send(batch_for(ConsensusStateId(2), "b1")).await.expect("src send b1");
        src_tx.send(batch_for(ConsensusStateId(1), "a2")).await.expect("src send a2");

        // Shard A receives its two batches in order.
        let a_first = shard_a_rx.recv().await.expect("shard A first batch");
        assert_eq!(a_first.shard, ConsensusStateId(1));
        assert_eq!(tag_of(&a_first), "a1");
        let a_second = shard_a_rx.recv().await.expect("shard A second batch");
        assert_eq!(a_second.shard, ConsensusStateId(1));
        assert_eq!(tag_of(&a_second), "a2");

        // Shard B receives its single batch.
        let b_first = shard_b_rx.recv().await.expect("shard B first batch");
        assert_eq!(b_first.shard, ConsensusStateId(2));
        assert_eq!(tag_of(&b_first), "b1");
    }

    #[tokio::test]
    async fn unknown_shard_is_dropped_without_blocking() {
        let (src_tx, src_rx) = mpsc::channel::<CommittedBatch>(16);
        let dispatcher = CommitDispatcher::new(src_rx);

        // Register one shard; send a batch for a different one.
        let (registered_tx, mut registered_rx) = mpsc::channel::<CommittedBatch>(16);
        dispatcher.register(ConsensusStateId(1), registered_tx);

        // Orphan batch — no downstream. Dispatcher must drop it (with a log)
        // and not block subsequent batches.
        src_tx
            .send(batch_for(ConsensusStateId(42), "orphan"))
            .await
            .expect("src send orphan");

        // A subsequent batch for the registered shard must still flow through.
        src_tx
            .send(batch_for(ConsensusStateId(1), "after-orphan"))
            .await
            .expect("src send after-orphan");

        let received = registered_rx.recv().await.expect("registered batch arrives");
        assert_eq!(received.shard, ConsensusStateId(1));
        assert_eq!(tag_of(&received), "after-orphan");
    }

    #[tokio::test]
    async fn deregister_drops_subsequent_batches() {
        let (src_tx, src_rx) = mpsc::channel::<CommittedBatch>(16);
        let dispatcher = CommitDispatcher::new(src_rx);

        let (tx, mut rx) = mpsc::channel::<CommittedBatch>(16);
        dispatcher.register(ConsensusStateId(7), tx);

        // First batch routes successfully.
        src_tx.send(batch_for(ConsensusStateId(7), "first")).await.expect("src send first");
        let first = rx.recv().await.expect("first batch");
        assert_eq!(tag_of(&first), "first");

        dispatcher.deregister(ConsensusStateId(7));

        // Subsequent batches for the deregistered shard are dropped.
        src_tx
            .send(batch_for(ConsensusStateId(7), "after-deregister"))
            .await
            .expect("src send after-deregister");

        // Give the dispatcher time to consume and drop the orphan.
        tokio::time::sleep(Duration::from_millis(50)).await;
        // The downstream must have nothing waiting.
        assert!(rx.try_recv().is_err());
    }
}
