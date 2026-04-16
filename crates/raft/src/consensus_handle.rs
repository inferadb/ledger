//! High-level handle for interacting with the consensus engine.
//!
//! Provides propose, leadership check, and membership operations
//! backed by the custom consensus engine.

use std::{collections::HashMap, sync::Arc, time::Duration};

use inferadb_ledger_consensus::{
    ConsensusEngine, ConsensusError,
    leadership::ShardState,
    types::{NodeId, ShardId},
};
use parking_lot::Mutex;
use snafu::{ResultExt, Snafu};
use tokio::sync::{oneshot, watch};

use crate::types::{LedgerNodeId, LedgerResponse, RaftPayload};

/// Error type for [`ConsensusHandle`] operations.
#[derive(Debug, Snafu)]
pub enum HandleError {
    /// Underlying consensus engine returned an error.
    #[snafu(display("Consensus error: {source}"))]
    Consensus {
        /// The consensus engine error.
        source: ConsensusError,
        /// Error location for stack-like diagnostics.
        #[snafu(implicit)]
        location: snafu::Location,
    },
    /// Payload serialization failed.
    #[snafu(display("Serialization error: {message}"))]
    Serialization {
        /// Error description.
        message: String,
    },
    /// Proposal or apply wait exceeded the given timeout.
    #[snafu(display("Proposal timed out after {timeout:?}"))]
    Timeout {
        /// The timeout duration that elapsed.
        timeout: Duration,
    },
    /// The apply worker dropped the response channel before delivering a result.
    #[snafu(display("Apply worker dropped response channel"))]
    ApplyDropped,
}

/// Maps commit indices to response oneshot senders.
///
/// Shared between [`ConsensusHandle`] (inserts on propose) and `ApplyWorker`
/// (removes and sends on apply).
pub type ResponseMap = Arc<Mutex<HashMap<u64, oneshot::Sender<LedgerResponse>>>>;

/// Buffer for responses that arrived before the proposer registered a waiter.
///
/// When the `ApplyWorker` processes a committed entry and finds no waiter in
/// [`ResponseMap`], it stores the response here. The proposer checks this
/// buffer after registering its oneshot sender, closing the TOCTOU race
/// between `engine.propose()` returning and the waiter being inserted.
pub type SpilloverMap = Arc<Mutex<HashMap<u64, LedgerResponse>>>;

/// High-level handle for a single shard in the consensus engine.
///
/// Each region gets its own `ConsensusHandle` bound to a specific shard.
pub struct ConsensusHandle {
    engine: ConsensusEngine,
    shard: ShardId,
    node_id: LedgerNodeId,
    state_rx: watch::Receiver<ShardState>,
    response_map: ResponseMap,
    spillover: SpilloverMap,
}

impl ConsensusHandle {
    /// Creates a new handle bound to the given shard.
    pub fn new(
        engine: ConsensusEngine,
        shard: ShardId,
        node_id: LedgerNodeId,
        state_rx: watch::Receiver<ShardState>,
        response_map: ResponseMap,
    ) -> Self {
        Self {
            engine,
            shard,
            node_id,
            state_rx,
            response_map,
            spillover: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Proposes a payload to the shard and returns the commit index.
    pub async fn propose(&self, payload: RaftPayload) -> Result<u64, HandleError> {
        let bytes = postcard::to_allocvec(&payload)
            .map_err(|e| HandleError::Serialization { message: e.to_string() })?;
        self.engine.propose(self.shard, bytes).await.context(ConsensusSnafu)
    }

    /// Proposes a payload and waits for the apply result.
    pub async fn propose_and_wait(
        &self,
        payload: RaftPayload,
        timeout: Duration,
    ) -> Result<LedgerResponse, HandleError> {
        let bytes = postcard::to_allocvec(&payload)
            .map_err(|e| HandleError::Serialization { message: e.to_string() })?;

        let start = tokio::time::Instant::now();

        // Submit to engine and get commit index.
        let commit_index = tokio::time::timeout(timeout, self.engine.propose(self.shard, bytes))
            .await
            .map_err(|_| HandleError::Timeout { timeout })?
            .context(ConsensusSnafu)?;

        // Register response waiter.
        let (tx, rx) = oneshot::channel();
        self.response_map.lock().insert(commit_index, tx);

        // Check if the apply worker already delivered before we registered.
        // This closes the TOCTOU race: engine.propose() returns the commit_index,
        // the apply worker processes it before we insert into response_map, and
        // stores the result in spillover instead.
        if let Some(response) = self.spillover.lock().remove(&commit_index) {
            self.response_map.lock().remove(&commit_index);
            return Ok(response);
        }

        // Compute remaining budget so the total wait never exceeds the caller's timeout.
        let remaining = timeout.saturating_sub(start.elapsed());

        // Wait for apply worker to deliver the response.
        tokio::time::timeout(remaining, rx)
            .await
            .map_err(|_| {
                // Clean up the pending entry on timeout.
                self.response_map.lock().remove(&commit_index);
                HandleError::Timeout { timeout }
            })?
            .map_err(|_| HandleError::ApplyDropped)
    }

    /// Returns `true` if this node is currently the leader of its shard.
    pub fn is_leader(&self) -> bool {
        let state = self.state_rx.borrow();
        state.leader == Some(NodeId(self.node_id))
    }

    /// Returns the current leader node ID, if known.
    pub fn current_leader(&self) -> Option<LedgerNodeId> {
        self.state_rx.borrow().leader.map(|n| n.0)
    }

    /// Returns the current Raft term.
    pub fn current_term(&self) -> u64 {
        self.state_rx.borrow().term
    }

    /// Returns the highest log index known to be committed.
    pub fn commit_index(&self) -> u64 {
        self.state_rx.borrow().commit_index
    }

    /// Returns a snapshot of the current shard state.
    pub fn shard_state(&self) -> ShardState {
        self.state_rx.borrow().clone()
    }

    /// Returns `true` if a membership change is currently in-flight.
    pub fn has_pending_membership(&self) -> bool {
        self.state_rx.borrow().pending_membership
    }

    /// Returns the shard ID this handle is bound to.
    pub fn shard_id(&self) -> ShardId {
        self.shard
    }

    /// Returns this node's ID.
    pub fn node_id(&self) -> LedgerNodeId {
        self.node_id
    }

    /// Adds a learner to the shard.
    pub async fn add_learner(
        &self,
        node: LedgerNodeId,
        promotable: bool,
    ) -> Result<(), HandleError> {
        self.engine.add_learner(self.shard, NodeId(node), promotable).await.context(ConsensusSnafu)
    }

    /// Promotes a learner to voter in the shard.
    pub async fn promote_voter(&self, node: LedgerNodeId) -> Result<(), HandleError> {
        self.engine.promote_voter(self.shard, NodeId(node)).await.context(ConsensusSnafu)
    }

    /// Removes a node from the shard membership.
    pub async fn remove_node(&self, node: LedgerNodeId) -> Result<(), HandleError> {
        self.engine.remove_node(self.shard, NodeId(node)).await.context(ConsensusSnafu)
    }

    /// Transfers leadership of this shard to the specified target voter.
    ///
    /// Sends a `TimeoutNow` message to the target node, causing it to
    /// immediately start a real election. This node must be the leader
    /// and the target must be a voter in the current membership.
    pub async fn transfer_leader(&self, target: LedgerNodeId) -> Result<(), HandleError> {
        self.engine.transfer_leader(self.shard, NodeId(target)).await.context(ConsensusSnafu)
    }

    /// Triggers a snapshot on this shard.
    ///
    /// Returns `(last_included_index, last_included_term)` if a snapshot was
    /// triggered, or `(0, 0)` if no snapshot was needed.
    pub async fn trigger_snapshot(&self) -> Result<(u64, u64), HandleError> {
        self.engine.trigger_snapshot(self.shard).await.context(ConsensusSnafu)
    }

    /// Delivers a peer message from another node to this shard.
    pub async fn peer_message(
        &self,
        from: LedgerNodeId,
        message: inferadb_ledger_consensus::Message,
    ) -> Result<(), HandleError> {
        self.engine.peer_message(self.shard, NodeId(from), message).await.context(ConsensusSnafu)
    }

    /// Returns a clone of the state watch receiver.
    pub fn state_watch(&self) -> watch::Receiver<ShardState> {
        self.state_rx.clone()
    }

    /// Returns a reference to the shared response map.
    pub fn response_map(&self) -> &ResponseMap {
        &self.response_map
    }

    /// Returns a reference to the spillover buffer.
    ///
    /// The apply worker stores responses here when no waiter is found in the
    /// response map, allowing proposers that register late to retrieve them.
    pub fn spillover(&self) -> &SpilloverMap {
        &self.spillover
    }

    /// Returns a peer's match_index for this shard.
    ///
    /// Queries the reactor synchronously via the inbox. Returns `None` if the
    /// peer is not tracked or the reactor is unavailable.
    pub async fn peer_match_index(&self, node: u64) -> Option<u64> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.engine
            .query_peer_state(self.shard, inferadb_ledger_consensus::types::NodeId(node), tx)
            .await
            .ok()?;
        rx.await.ok()?
    }

    /// Confirms leadership via a quorum heartbeat round and returns
    /// the committed index.
    ///
    /// Use this as a fallback when the leader lease has expired (idle
    /// cluster, freshly elected leader, etc.).
    pub async fn engine_read_index(&self) -> Result<u64, HandleError> {
        self.engine.read_index(self.shard).await.context(ConsensusSnafu)
    }

    /// Requests consensus engine shutdown for this shard.
    ///
    /// The reactor will flush pending work and exit its event loop.
    pub async fn request_shutdown(&self) {
        self.engine.request_shutdown().await;
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods, clippy::panic)]
mod tests {
    use inferadb_ledger_consensus::{
        InMemoryTransport, Shard,
        clock::SystemClock,
        config::ShardConfig,
        rng::SystemRng,
        types::{Membership, NodeId, NodeState, ShardId},
        wal::InMemoryWalBackend,
    };

    use super::*;

    fn make_state(
        shard: ShardId,
        state: NodeState,
        leader: Option<NodeId>,
        term: u64,
    ) -> ShardState {
        ShardState {
            shard,
            term,
            state,
            leader,
            commit_index: 0,
            voters: Default::default(),
            learners: Default::default(),
            conf_epoch: 0,
            pending_membership: false,
            last_log_index: 0,
        }
    }

    fn make_handle(
        node_id: LedgerNodeId,
        initial_state: ShardState,
    ) -> (ConsensusHandle, ResponseMap) {
        let shard_id = initial_state.shard;
        let config = ShardConfig::default();
        let membership = Membership::new([NodeId(node_id)]);
        let shard = Shard::<SystemClock, SystemRng>::new(
            shard_id,
            NodeId(node_id),
            membership,
            config,
            SystemClock,
            SystemRng,
            0,
            None,
            0,
        );
        let transport = InMemoryTransport::new();
        let wal = InMemoryWalBackend::new();

        let (engine, _commit_rx, _state_map) = ConsensusEngine::start(
            vec![shard],
            wal,
            SystemClock,
            transport,
            Duration::from_millis(10),
        );

        let (tx, rx) = watch::channel(initial_state);
        std::mem::forget(tx);

        let response_map: ResponseMap = Arc::new(Mutex::new(HashMap::new()));
        let handle = ConsensusHandle::new(engine, shard_id, node_id, rx, Arc::clone(&response_map));
        (handle, response_map)
    }

    #[tokio::test]
    async fn is_leader_when_state_shows_leader() {
        let node_id: LedgerNodeId = 1;
        let state = make_state(ShardId(0), NodeState::Leader, Some(NodeId(1)), 3);
        let (handle, _map) = make_handle(node_id, state);
        assert!(handle.is_leader());
    }

    #[tokio::test]
    async fn is_leader_returns_false_for_follower() {
        let node_id: LedgerNodeId = 1;
        let state = make_state(ShardId(0), NodeState::Follower, Some(NodeId(2)), 3);
        let (handle, _map) = make_handle(node_id, state);
        assert!(!handle.is_leader());
    }

    #[tokio::test]
    async fn current_leader_returns_node_id() {
        let node_id: LedgerNodeId = 5;
        let state = make_state(ShardId(0), NodeState::Follower, Some(NodeId(3)), 1);
        let (handle, _map) = make_handle(node_id, state);
        assert_eq!(handle.current_leader(), Some(3));
    }

    #[tokio::test]
    async fn current_term_returns_from_state() {
        let node_id: LedgerNodeId = 1;
        let state = make_state(ShardId(0), NodeState::Follower, None, 42);
        let (handle, _map) = make_handle(node_id, state);
        assert_eq!(handle.current_term(), 42);
    }

    #[tokio::test]
    async fn response_map_starts_empty() {
        let node_id: LedgerNodeId = 1;
        let state = make_state(ShardId(0), NodeState::Follower, None, 1);
        let (handle, _map) = make_handle(node_id, state);
        assert!(handle.response_map().lock().is_empty());
    }

    #[tokio::test]
    async fn shard_id_and_node_id_accessors() {
        let node_id: LedgerNodeId = 7;
        let state = make_state(ShardId(4), NodeState::Follower, None, 1);
        let (handle, _map) = make_handle(node_id, state);
        assert_eq!(handle.shard_id(), ShardId(4));
        assert_eq!(handle.node_id(), 7);
    }

    #[test]
    fn spillover_delivers_early_response() {
        let spillover: SpilloverMap = Arc::new(Mutex::new(HashMap::new()));
        let response_map: ResponseMap = Arc::new(Mutex::new(HashMap::new()));

        // Simulate apply worker delivering before proposer registers.
        spillover.lock().insert(42, LedgerResponse::Empty);

        // Proposer registers a waiter...
        let (tx, _rx) = oneshot::channel();
        response_map.lock().insert(42, tx);

        // ...then checks spillover and finds the early response.
        let response = spillover.lock().remove(&42);
        assert!(response.is_some());
        assert_eq!(response.unwrap(), LedgerResponse::Empty);

        // Clean up the now-unnecessary waiter.
        response_map.lock().remove(&42);
    }

    #[test]
    fn normal_path_bypasses_spillover() {
        let response_map: ResponseMap = Arc::new(Mutex::new(HashMap::new()));
        let spillover: SpilloverMap = Arc::new(Mutex::new(HashMap::new()));

        // Proposer registers before apply worker delivers.
        let (tx, mut rx) = oneshot::channel();
        response_map.lock().insert(42, tx);

        // Apply worker finds waiter — delivers directly, spillover stays empty.
        if let Some(sender) = response_map.lock().remove(&42) {
            let _ = sender.send(LedgerResponse::Empty);
        }

        assert!(spillover.lock().is_empty());
        assert_eq!(rx.try_recv().unwrap(), LedgerResponse::Empty);
    }

    #[tokio::test]
    async fn spillover_starts_empty() {
        let node_id: LedgerNodeId = 1;
        let state = make_state(ShardId(0), NodeState::Follower, None, 1);
        let (handle, _map) = make_handle(node_id, state);
        assert!(handle.spillover().lock().is_empty());
    }

    #[tokio::test]
    async fn spillover_and_response_map_share_via_handle() {
        let node_id: LedgerNodeId = 1;
        let state = make_state(ShardId(0), NodeState::Follower, None, 1);
        let (handle, _map) = make_handle(node_id, state);

        // Insert into spillover through one reference.
        handle.spillover().lock().insert(99, LedgerResponse::Empty);

        // Read through a cloned reference — same underlying map.
        let spillover_clone = handle.spillover().clone();
        assert_eq!(spillover_clone.lock().remove(&99), Some(LedgerResponse::Empty));
        assert!(handle.spillover().lock().is_empty());
    }
}
