//! Public API for the consensus engine.
//!
//! [`ConsensusEngine`] spawns a [`Reactor`] as a
//! background tokio task and communicates with it via channels. Callers
//! interact through the engine handle; committed batches are delivered on a
//! separate receiver channel.

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

/// Type alias for the pre-proposal access validator function.
type ValidatorFn = dyn Fn(&[u8]) -> Result<(), String> + Send + Sync;

static PROPOSE_COUNT: AtomicU64 = AtomicU64::new(0);

use tokio::sync::{mpsc, oneshot, watch};

use crate::{
    clock::Clock,
    committed::CommittedBatch,
    error::ConsensusError,
    leadership::ShardState,
    message::Message,
    reactor::{Reactor, ReactorEvent},
    rng::RngSource,
    shard::Shard,
    transport::NetworkTransport,
    types::*,
    wal_backend::WalBackend,
};

/// Handle to the consensus engine.
///
/// Created via [`ConsensusEngine::start`], which spawns a background reactor
/// task. The engine communicates with the reactor through an mpsc channel.
/// Dropping all clones of the sender (i.e. dropping the engine) causes the
/// reactor to shut down on the next event loop iteration.
pub struct ConsensusEngine {
    inbox: mpsc::Sender<ReactorEvent>,
    reactor_handle: tokio::task::JoinHandle<()>,
    /// Pre-proposal idempotency cache. Checked before sending to reactor.
    idempotency: parking_lot::Mutex<crate::idempotency::IdempotencyCache>,
    /// Optional pre-proposal access control validator.
    ///
    /// When set, every [`propose`](ConsensusEngine::propose) call runs this
    /// function before touching the idempotency cache or the reactor. A
    /// returned `Err(reason)` rejects the proposal immediately.
    validator: Option<Arc<ValidatorFn>>,
}

impl ConsensusEngine {
    /// Starts the engine by spawning the reactor as a background task.
    ///
    /// Returns:
    /// - The engine handle.
    /// - A receiver for committed entry batches.
    /// - A map from shard ID to a watch receiver for observable shard state. Each receiver reflects
    ///   the latest [`ShardState`] for that shard and is updated after every event that touches the
    ///   shard.
    ///
    /// Each shard is registered with the reactor before the event loop begins.
    pub fn start<C, R, W, T>(
        shards: Vec<Shard<C, R>>,
        wal: W,
        clock: C,
        transport: T,
        flush_interval: Duration,
    ) -> (Self, mpsc::Receiver<CommittedBatch>, HashMap<ShardId, watch::Receiver<ShardState>>)
    where
        C: Clock + Clone + Send + 'static,
        R: RngSource + Send + 'static,
        W: WalBackend + Send + 'static,
        T: NetworkTransport,
    {
        let (inbox_tx, inbox_rx) = mpsc::channel(10_000);
        let (commit_tx, commit_rx) = mpsc::channel(10_000);

        let mut reactor = Reactor::new(inbox_rx, wal, clock, transport, commit_tx, flush_interval);
        let mut state_receivers = HashMap::new();

        for shard in shards {
            let id = shard.id();
            let initial_state = shard.state_snapshot();
            let (state_tx, state_rx) = watch::channel(initial_state);
            reactor.add_shard(id, shard);
            reactor.add_state_watcher(id, state_tx);
            state_receivers.insert(id, state_rx);
        }

        let reactor_handle = tokio::spawn(async move {
            reactor.run().await;
        });

        let idempotency = parking_lot::Mutex::new(crate::idempotency::IdempotencyCache::new(
            std::time::Duration::from_secs(60),
        ));

        (
            Self { inbox: inbox_tx, reactor_handle, idempotency, validator: None },
            commit_rx,
            state_receivers,
        )
    }

    /// Sets a pre-proposal access validator.
    ///
    /// The validator is called before every [`propose`](Self::propose) call.
    /// If it returns `Err(reason)`, the proposal is rejected without entering
    /// consensus. Replaces any previously set validator.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_consensus::ConsensusEngine;
    /// # fn example(mut engine: ConsensusEngine) {
    /// engine.set_validator(|data| {
    ///     if data.starts_with(b"BLOCKED") {
    ///         return Err("blocked prefix".into());
    ///     }
    ///     Ok(())
    /// });
    /// # }
    /// ```
    pub fn set_validator(
        &mut self,
        f: impl Fn(&[u8]) -> Result<(), String> + Send + Sync + 'static,
    ) {
        self.validator = Some(Arc::new(f));
    }

    /// Proposes an entry to a shard. Returns when the WAL is synced (consensus
    /// durable).
    ///
    /// If a validator is set via [`set_validator`](Self::set_validator), it is
    /// checked first. A rejected proposal returns
    /// [`ConsensusError::ShardUnavailable`] without touching the idempotency
    /// cache or the reactor.
    ///
    /// Duplicate proposals (same content within the 60-second idempotency
    /// window) are short-circuited before reaching the reactor, returning the
    /// cached commit index immediately.
    pub async fn propose(&self, shard: ShardId, data: Vec<u8>) -> Result<u64, ConsensusError> {
        // Pre-proposal access control.
        if let Some(validator) = &self.validator {
            validator(&data).map_err(|_reason| ConsensusError::ShardUnavailable { shard })?;
        }

        // Pre-proposal idempotency check.
        // Use seahash of the data as the 16-byte key.
        let hash1 = seahash::hash(&data);
        let hash2 = hash1 ^ (data.len() as u64).wrapping_mul(0x517cc1b727220a95);
        let mut key = [0u8; 16];
        key[..8].copy_from_slice(&hash1.to_le_bytes());
        key[8..].copy_from_slice(&hash2.to_le_bytes());

        {
            let cache = self.idempotency.lock();
            if let crate::idempotency::IdempotencyCheck::Duplicate { commit_index } =
                cache.check(&key)
            {
                return Ok(commit_index);
            }
        }

        // Not a duplicate — proceed with consensus.
        let (tx, rx) = oneshot::channel();
        self.inbox
            .send(ReactorEvent::Propose { shard, data, response: tx })
            .await
            .map_err(|_| ConsensusError::ProposalQueueFull)?;
        let result = rx.await.map_err(|_| ConsensusError::ProposalQueueFull)?;

        // Cache successful proposals.
        if let Ok(commit_index) = &result {
            self.idempotency.lock().insert(key, *commit_index);
        }

        if PROPOSE_COUNT.fetch_add(1, Ordering::Relaxed).is_multiple_of(1024) {
            self.idempotency.lock().evict_expired();
        }

        result
    }

    /// Proposes a batch of entries to a shard. Returns when the WAL is synced.
    ///
    /// If a validator is set via [`set_validator`](Self::set_validator), it is
    /// checked against each entry before the batch reaches the reactor. Any
    /// rejected entry causes the entire batch to return
    /// [`ConsensusError::ShardUnavailable`] immediately.
    pub async fn propose_batch(
        &self,
        shard: ShardId,
        entries: Vec<Vec<u8>>,
    ) -> Result<u64, ConsensusError> {
        // Pre-proposal validation on each entry.
        if let Some(validator) = &self.validator {
            for entry in &entries {
                validator(entry).map_err(|_| ConsensusError::ShardUnavailable { shard })?;
            }
        }

        let (tx, rx) = oneshot::channel();
        self.inbox
            .send(ReactorEvent::ProposeBatch { shard, entries, response: tx })
            .await
            .map_err(|_| ConsensusError::ProposalQueueFull)?;
        rx.await.map_err(|_| ConsensusError::ProposalQueueFull)?
    }

    /// Delivers a peer message to the reactor for the given shard.
    pub async fn peer_message(
        &self,
        shard: ShardId,
        from: NodeId,
        message: Message,
    ) -> Result<(), ConsensusError> {
        self.inbox
            .send(ReactorEvent::PeerMessage { shard, from, message })
            .await
            .map_err(|_| ConsensusError::ProposalQueueFull)
    }

    /// Adds a learner to a shard.
    pub async fn add_learner(
        &self,
        shard: ShardId,
        node: NodeId,
        promotable: bool,
    ) -> Result<(), ConsensusError> {
        let (tx, rx) = oneshot::channel();
        self.inbox
            .send(ReactorEvent::MembershipChange {
                shard,
                change: MembershipChange::AddLearner {
                    node_id: node,
                    promotable,
                    expected_conf_epoch: None,
                },
                response: tx,
            })
            .await
            .map_err(|_| ConsensusError::ProposalQueueFull)?;
        rx.await.map_err(|_| ConsensusError::ProposalQueueFull)?
    }

    /// Promotes a learner to voter.
    pub async fn promote_voter(&self, shard: ShardId, node: NodeId) -> Result<(), ConsensusError> {
        let (tx, rx) = oneshot::channel();
        self.inbox
            .send(ReactorEvent::MembershipChange {
                shard,
                change: MembershipChange::PromoteVoter { node_id: node, expected_conf_epoch: None },
                response: tx,
            })
            .await
            .map_err(|_| ConsensusError::ProposalQueueFull)?;
        rx.await.map_err(|_| ConsensusError::ProposalQueueFull)?
    }

    /// Removes a node from a shard.
    pub async fn remove_node(&self, shard: ShardId, node: NodeId) -> Result<(), ConsensusError> {
        let (tx, rx) = oneshot::channel();
        self.inbox
            .send(ReactorEvent::MembershipChange {
                shard,
                change: MembershipChange::RemoveNode { node_id: node, expected_conf_epoch: None },
                response: tx,
            })
            .await
            .map_err(|_| ConsensusError::ProposalQueueFull)?;
        rx.await.map_err(|_| ConsensusError::ProposalQueueFull)?
    }

    /// Transfers leadership of a shard to the specified target voter.
    ///
    /// Sends a `TimeoutNow` message to the target node, causing it to
    /// immediately start a real election (skipping the pre-vote phase).
    /// This node must be the leader and the target must be a voter.
    ///
    /// # Errors
    ///
    /// Returns [`ConsensusError::NotLeader`] if this node is not the leader,
    /// [`ConsensusError::ShardUnavailable`] if the shard is not registered
    /// or the target is not a voter, or [`ConsensusError::ProposalQueueFull`]
    /// if the reactor inbox is full.
    pub async fn transfer_leader(
        &self,
        shard: ShardId,
        target: NodeId,
    ) -> Result<(), ConsensusError> {
        let (tx, rx) = oneshot::channel();
        self.inbox
            .send(ReactorEvent::TransferLeader { shard, target, response: tx })
            .await
            .map_err(|_| ConsensusError::ProposalQueueFull)?;
        rx.await.map_err(|_| ConsensusError::ProposalQueueFull)?
    }

    /// Triggers a snapshot on a shard.
    ///
    /// Returns `(last_included_index, last_included_term)` if a snapshot was
    /// triggered, or `(0, 0)` if no snapshot was needed (commit index is zero
    /// or already at the last snapshot index).
    ///
    /// # Errors
    ///
    /// Returns [`ConsensusError::ShardUnavailable`] if the shard is not
    /// registered, or [`ConsensusError::ProposalQueueFull`] if the reactor
    /// inbox is full.
    pub async fn trigger_snapshot(&self, shard: ShardId) -> Result<(u64, u64), ConsensusError> {
        let (tx, rx) = oneshot::channel();
        self.inbox
            .send(ReactorEvent::TriggerSnapshot { shard, response: tx })
            .await
            .map_err(|_| ConsensusError::ProposalQueueFull)?;
        rx.await.map_err(|_| ConsensusError::ProposalQueueFull)?
    }

    /// Returns the current committed index for a shard.
    ///
    /// Reads the shard's `commit_index` without going through the Raft log.
    /// Suitable for monotonic reads or for linearizable reads when combined
    /// with an out-of-band leader-lease check.
    ///
    /// # Errors
    ///
    /// Returns [`ConsensusError::ShardUnavailable`] if the shard is not
    /// registered with this engine, or [`ConsensusError::ProposalQueueFull`]
    /// if the reactor inbox is full or the reactor has shut down.
    pub async fn read_index(&self, shard: ShardId) -> Result<u64, ConsensusError> {
        let (tx, rx) = oneshot::channel();
        self.inbox
            .send(ReactorEvent::ReadIndex { shard, response: tx })
            .await
            .map_err(|_| ConsensusError::ProposalQueueFull)?;
        rx.await.map_err(|_| ConsensusError::ProposalQueueFull)?
    }

    /// Queries a peer's match_index for a shard.
    ///
    /// Returns `None` if the shard or peer is not tracked by the reactor.
    ///
    /// # Errors
    ///
    /// Returns [`ConsensusError::ProposalQueueFull`] if the reactor inbox is
    /// full or the reactor has shut down.
    pub async fn query_peer_state(
        &self,
        shard: ShardId,
        node: NodeId,
        response: oneshot::Sender<Option<u64>>,
    ) -> Result<(), ConsensusError> {
        self.inbox
            .send(ReactorEvent::QueryPeerState { shard, node, response })
            .await
            .map_err(|_| ConsensusError::ProposalQueueFull)
    }

    /// Gracefully shuts down the reactor and waits for it to finish.
    pub async fn shutdown(self) {
        let _ = self.inbox.send(ReactorEvent::Shutdown).await;
        let _ = self.reactor_handle.await;
    }

    /// Requests the reactor to shut down gracefully.
    ///
    /// Unlike `shutdown()` which takes ownership, this can be called through
    /// shared references (e.g., when the engine is behind an Arc).
    pub async fn request_shutdown(&self) {
        let _ = self.inbox.send(ReactorEvent::Shutdown).await;
    }

    /// Returns the number of entries currently held in the idempotency cache.
    ///
    /// Primarily for observability and testing. The count includes unexpired
    /// entries only; expired entries are evicted lazily on `check()`.
    pub fn idempotency_cache_len(&self) -> usize {
        self.idempotency.lock().len()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use std::sync::Arc;

    use tokio::sync::mpsc;

    use super::*;
    use crate::{
        clock::SimulatedClock,
        config::ShardConfig,
        rng::SimulatedRng,
        shard::Shard,
        transport::InMemoryTransport,
        types::{Membership, NodeId},
        wal::InMemoryWalBackend,
    };

    fn make_engine() -> (ConsensusEngine, mpsc::Receiver<crate::committed::CommittedBatch>) {
        let clock = Arc::new(SimulatedClock::new());
        let shard_id = ShardId(1);
        let shard = Shard::new(
            shard_id,
            NodeId(1),
            Membership::new([NodeId(1)]),
            ShardConfig::default(),
            clock.clone(),
            SimulatedRng::new(42),
        );
        let (engine, commit_rx, _state_rx) = ConsensusEngine::start(
            vec![shard],
            InMemoryWalBackend::new(),
            clock,
            InMemoryTransport::new(),
            std::time::Duration::from_millis(1),
        );
        (engine, commit_rx)
    }

    /// Seeds the idempotency cache with a key derived from `data`, as if the
    /// data had been committed at `commit_index`.
    fn seed_cache(engine: &ConsensusEngine, data: &[u8], commit_index: u64) {
        let hash1 = seahash::hash(data);
        let hash2 = hash1 ^ (data.len() as u64).wrapping_mul(0x517cc1b727220a95);
        let mut key = [0u8; 16];
        key[..8].copy_from_slice(&hash1.to_le_bytes());
        key[8..].copy_from_slice(&hash2.to_le_bytes());
        engine.idempotency.lock().insert(key, commit_index);
    }

    /// Duplicate proposals are served from the cache before reaching the
    /// reactor. The cache should not grow after the short-circuit.
    #[tokio::test]
    async fn duplicate_propose_returns_cached_commit_index() {
        let (engine, _commit_rx) = make_engine();

        let data = b"idempotent-payload".to_vec();
        let expected_commit_index: u64 = 42;

        // Simulate a prior successful commit by pre-seeding the cache.
        seed_cache(&engine, &data, expected_commit_index);
        assert_eq!(engine.idempotency_cache_len(), 1);

        // Proposing the same data should short-circuit and return the cached
        // commit index without sending anything to the reactor.
        let result = engine.propose(ShardId(1), data.clone()).await.unwrap();
        assert_eq!(
            result, expected_commit_index,
            "duplicate propose should return cached commit index"
        );

        // Cache must not grow — the entry was served, not re-inserted.
        assert_eq!(engine.idempotency_cache_len(), 1);

        engine.shutdown().await;
    }

    /// A validator that rejects proposals with a specific prefix blocks them
    /// before the idempotency cache or reactor are consulted.
    #[tokio::test]
    async fn validator_rejects_blocked_proposals() {
        let (mut engine, _commit_rx) = make_engine();

        engine.set_validator(|data| {
            if data.starts_with(b"BLOCKED") {
                return Err("blocked prefix".into());
            }
            Ok(())
        });

        // Blocked proposal must return an error.
        let result = engine.propose(ShardId(1), b"BLOCKED:payload".to_vec()).await;
        assert!(result.is_err(), "validator-rejected proposal must fail");

        // Cache must remain empty — rejected proposals are never cached.
        assert_eq!(engine.idempotency_cache_len(), 0, "rejected proposal must not enter cache");

        // Non-blocked proposal reaches the reactor (fails: no leader), but is
        // not rejected by the validator.
        let result = engine.propose(ShardId(1), b"allowed-payload".to_vec()).await;
        assert!(result.is_err(), "non-blocked proposal to leaderless shard should fail");
        assert_eq!(engine.idempotency_cache_len(), 0, "failed propose must not be cached");

        engine.shutdown().await;
    }

    /// A validator set on the engine also rejects `propose_batch` entries.
    #[tokio::test]
    async fn validator_rejects_propose_batch_entries() {
        let (mut engine, _commit_rx) = make_engine();

        engine.set_validator(|data| {
            if data.starts_with(b"BLOCKED") {
                return Err("blocked prefix".into());
            }
            Ok(())
        });

        // A batch containing a blocked entry must be rejected before the reactor.
        let result = engine
            .propose_batch(ShardId(1), vec![b"allowed".to_vec(), b"BLOCKED:bad".to_vec()])
            .await;
        assert!(
            matches!(result, Err(ConsensusError::ShardUnavailable { .. })),
            "batch with blocked entry must return ShardUnavailable, got {result:?}",
        );

        // A batch with only allowed entries reaches the reactor (fails: no leader).
        let result = engine.propose_batch(ShardId(1), vec![b"allowed".to_vec()]).await;
        assert!(result.is_err(), "propose_batch to leaderless shard should fail");

        engine.shutdown().await;
    }

    /// `read_index` returns the current committed index (0 for a fresh shard).
    #[tokio::test]
    async fn read_index_returns_commit_index_for_known_shard() {
        let (engine, _commit_rx) = make_engine();

        let result = engine.read_index(ShardId(1)).await;
        assert!(result.is_ok(), "expected Ok, got {result:?}");
        assert_eq!(result.unwrap(), 0, "fresh shard has commit_index == 0");

        engine.shutdown().await;
    }

    /// `read_index` on an unknown shard returns `ShardUnavailable`.
    #[tokio::test]
    async fn read_index_returns_shard_unavailable_for_unknown_shard() {
        let (engine, _commit_rx) = make_engine();

        let result = engine.read_index(ShardId(99)).await;
        assert!(
            matches!(result, Err(ConsensusError::ShardUnavailable { shard: ShardId(99) })),
            "expected ShardUnavailable, got {result:?}",
        );

        engine.shutdown().await;
    }

    /// Different data produces a different cache key and reaches the reactor.
    /// Here the reactor returns an error (no leader), which must not be cached.
    #[tokio::test]
    async fn different_data_bypasses_cache() {
        let (engine, _commit_rx) = make_engine();

        let cached_data = b"cached-entry".to_vec();
        let other_data = b"different-entry".to_vec();

        seed_cache(&engine, &cached_data, 7);

        // `other_data` is not in the cache — reaches the reactor and fails
        // (no leader), so the cache should still have exactly one entry.
        let result = engine.propose(ShardId(1), other_data).await;
        assert!(result.is_err(), "non-cached propose to leaderless shard should fail");
        assert_eq!(engine.idempotency_cache_len(), 1, "failed propose must not be cached");

        engine.shutdown().await;
    }
}
