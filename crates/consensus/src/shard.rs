//! Core Raft state machine.
//!
//! The [`Shard`] receives events and returns [`Action`] values — it never
//! performs I/O directly. Parameterized by [`Clock`] and [`RngSource`] for
//! deterministic simulation testing.

use std::{collections::VecDeque, sync::Arc, time::Duration};

use crate::{
    action::Action,
    clock::Clock,
    closed_ts::ClosedTimestampTracker,
    config::ShardConfig,
    error::ConsensusError,
    lease::LeaderLease,
    message::Message,
    rng::RngSource,
    types::{
        Entry, EntryKind, Membership, MembershipChange, NodeId, NodeState, PeerState, ShardId,
        TimerKind,
    },
};

/// Core Raft state machine for a single shard (consensus group).
///
/// All mutation happens through event handlers that return [`Action`] vectors.
/// The reactor is responsible for executing those actions (sending messages,
/// persisting entries, scheduling timers).
pub struct Shard<C: Clock, R: RngSource> {
    id: ShardId,
    node_id: NodeId,
    state: NodeState,
    current_term: u64,
    last_committed_term: u64,
    voted_for: Option<NodeId>,
    log: VecDeque<Entry>,
    commit_index: u64,
    last_applied: u64,
    peer_states: Vec<PeerState>,
    self_match_index: u64,
    membership: Membership,
    pending_membership: bool,
    conf_epoch: u64,
    #[allow(dead_code)]
    pending_split: bool,
    last_snapshot_index: u64,
    /// Set when a `TriggerSnapshot` action is emitted; cleared by
    /// `handle_snapshot_completed`. Prevents duplicate trigger actions while
    /// a snapshot is being persisted.
    snapshot_in_flight: bool,
    config: ShardConfig,
    clock: C,
    rng: R,
    pre_votes_received: usize,
    votes_received: usize,
    election_deadline: std::time::Instant,
    lease: LeaderLease<C>,
    closed_ts: ClosedTimestampTracker,
    /// Last known leader for this shard. `None` until the first AppendEntries
    /// is received or this node wins an election.
    leader_id: Option<NodeId>,
    /// Tracks whether the local node has ever appeared in this shard's
    /// membership. During log replay a node that was added after the
    /// shard's initial creation will see historical membership entries
    /// that do not include it. Without this guard, `ShardRemoved` would
    /// fire spuriously, shutting down a shard the node is actually a
    /// member of at the log tip.
    was_ever_member: bool,
    /// How this shard determines its leader.
    ///
    /// In [`LeadershipMode::SelfElect`] (the default), the shard runs
    /// standard Raft elections — election-timeout fires transition to
    /// PreCandidate, vote-collection promotes to Leader.
    ///
    /// In [`LeadershipMode::Delegated`], elections are disabled. The
    /// shard's leader is set externally via [`Shard::adopt_leader`] —
    /// typically driven by another shard's leader changes (the region
    /// coordinator's leader becomes every per-organization shard's
    /// leader under the B.1 unified-leadership model).
    leadership_mode: LeadershipMode,
}

/// Determines how a [`Shard`] establishes its leader.
///
/// The default mode is `SelfElect` — standard Raft elections. The
/// `Delegated` mode is used by per-organization shards under the B.1
/// unified-leadership model: the region coordinator's elected leader is
/// adopted as the org shard's leader without a separate election.
///
/// # Safety
///
/// In `Delegated` mode, the shard must NOT initiate elections (election
/// timeouts no-op) and must accept externally-asserted leadership via
/// [`Shard::adopt_leader`]. The caller (the consensus engine + reactor)
/// is responsible for ensuring that adopted leaders actually have a quorum
/// — typically by deriving them from another shard whose Raft elections
/// already established quorum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeadershipMode {
    /// Standard Raft elections.
    SelfElect,
    /// Leader is asserted externally via [`Shard::adopt_leader`].
    /// Election timeouts no-op.
    Delegated,
}

impl Default for LeadershipMode {
    fn default() -> Self {
        Self::SelfElect
    }
}

impl<C: Clock + Clone, R: RngSource> Shard<C, R> {
    /// Creates a new shard starting as a [`NodeState::Follower`].
    ///
    /// For first boot pass `initial_term = 0, initial_voted_for = None,
    /// initial_committed_index = 0`.
    /// On restart, pass the values recovered from the WAL checkpoint to
    /// satisfy the Raft durability requirement (Figure 2).
    ///
    /// `initial_committed_index` tells the shard (and its reactor) where
    /// the state machine was last durably applied. On restart this is the
    /// `committed_index` from the WAL checkpoint.  The reactor uses this
    /// value to avoid re-dispatching already-applied entries to the apply
    /// worker.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: ShardId,
        node_id: NodeId,
        membership: Membership,
        config: ShardConfig,
        clock: C,
        rng: R,
        initial_term: u64,
        initial_voted_for: Option<NodeId>,
        initial_committed_index: u64,
    ) -> Self {
        // Set the initial election deadline to the past so the first pre-vote
        // request from any node is immediately granted. For brand-new shards
        // there is no existing leader to protect, and using a future deadline
        // causes pre-vote denials (peers haven't expired their own timers yet),
        // delaying the first election by multiple timeout rounds.
        let deadline = clock.now();
        let lease = LeaderLease::new(config.heartbeat_interval * 3, clock.clone());
        let closed_ts = ClosedTimestampTracker::new(Duration::from_secs(3));
        let was_ever_member = membership.is_voter(node_id) || membership.is_learner(node_id);
        Self {
            id,
            node_id,
            state: NodeState::Follower,
            current_term: initial_term,
            last_committed_term: 0,
            voted_for: initial_voted_for,
            log: VecDeque::new(),
            commit_index: initial_committed_index,
            last_applied: initial_committed_index,
            peer_states: Vec::new(),
            self_match_index: 0,
            membership,
            pending_membership: false,
            conf_epoch: 0,
            pending_split: false,
            last_snapshot_index: 0,
            snapshot_in_flight: false,
            config,
            clock,
            rng,
            pre_votes_received: 0,
            votes_received: 0,
            election_deadline: deadline,
            lease,
            closed_ts,
            leader_id: None,
            was_ever_member,
            leadership_mode: LeadershipMode::default(),
        }
    }

    /// Sets the shard's leadership mode.
    ///
    /// Called by the reactor at shard construction time to configure
    /// per-organization shards as [`LeadershipMode::Delegated`]. Once a
    /// shard has run any elections or accepted any AppendEntries,
    /// switching modes mid-flight is unsafe — set the mode immediately
    /// after construction and before the shard accepts any events.
    pub fn set_leadership_mode(&mut self, mode: LeadershipMode) {
        self.leadership_mode = mode;
    }

    /// Returns the shard's current leadership mode.
    #[inline]
    pub fn leadership_mode(&self) -> LeadershipMode {
        self.leadership_mode
    }

    /// Externally asserts a leader for this shard.
    ///
    /// Used in [`LeadershipMode::Delegated`] mode to install the leader
    /// derived from another shard's elected leader (the region
    /// coordinator's leader becomes every per-organization shard's
    /// leader). The caller is responsible for ensuring the asserted
    /// leader has a quorum — typically by deriving it from a shard whose
    /// Raft elections have already established quorum.
    ///
    /// Behavior:
    /// - If `term < self.current_term`: rejected (stale assertion).
    ///   Returns no actions.
    /// - If `term > self.current_term`: term is advanced. State
    ///   transitions to Leader if `leader == self.node_id`, otherwise
    ///   Follower. Voted_for is cleared. Election timer is reset.
    /// - If `term == self.current_term`: leader_id is updated (or
    ///   confirmed). State adjusts as above. Election timer is reset.
    ///
    /// Calling this in [`LeadershipMode::SelfElect`] is permitted but
    /// not recommended — it pre-empts the normal Raft election flow and
    /// can cause split-brain if combined with concurrent elections.
    pub fn adopt_leader(&mut self, leader: NodeId, term: u64) -> Vec<Action> {
        if self.state == NodeState::Failed || self.state == NodeState::Shutdown {
            return Vec::new();
        }
        if term < self.current_term {
            return Vec::new();
        }
        // Idempotent: if already in the same term with the same leader,
        // nothing to do.
        if term == self.current_term
            && self.leader_id == Some(leader)
            && ((leader == self.node_id && self.state == NodeState::Leader)
                || (leader != self.node_id && self.state == NodeState::Follower))
        {
            return Vec::new();
        }
        let mut actions = Vec::new();
        if term > self.current_term {
            self.current_term = term;
            self.voted_for = None;
        }
        self.leader_id = Some(leader);
        if leader == self.node_id {
            // Local node is the new leader. Initialize per-peer match-index
            // tracking and schedule heartbeats so followers learn about the
            // leadership through standard AppendEntries flow. Skip the
            // standard new-leader no-op entry: under unified leadership,
            // term advancement comes from the data-region group's
            // election, and the no-op pattern (forcing a quorum ack to
            // resolve term-T-1 commits) is satisfied by the data-region
            // group's own no-op entry on its term transition.
            self.state = NodeState::Leader;
            let next = self.log.len() as u64 + 1;
            self.peer_states = self
                .membership
                .voters
                .iter()
                .filter(|&&id| id != self.node_id)
                .map(|&id| PeerState::voter(id, next))
                .chain(
                    self.membership
                        .learners
                        .iter()
                        .map(|&id| PeerState::learner(id, next, false)),
                )
                .collect();
            self.self_match_index = self.log.len() as u64;
            actions.push(Action::ScheduleTimer {
                shard: self.id,
                kind: TimerKind::Heartbeat,
                deadline: self.clock.now() + self.config.heartbeat_interval,
            });
            actions.extend(self.send_append_entries_to_all());
        } else {
            self.state = NodeState::Follower;
        }
        self.reset_election_timer(&mut actions);
        actions
    }

    /// Returns the actions that should be processed when this shard is first
    /// registered with the reactor.
    ///
    /// Currently this emits a [`Action::ScheduleTimer`] for the initial
    /// election deadline so the reactor's timer wheel can fire the first
    /// election timeout.
    pub fn initial_actions(&self) -> Vec<Action> {
        vec![Action::ScheduleTimer {
            shard: self.id,
            kind: TimerKind::Election,
            deadline: self.election_deadline,
        }]
    }

    // ── Public accessors ────────────────────────────────────────────

    /// Returns the shard ID.
    #[inline]
    pub fn id(&self) -> ShardId {
        self.id
    }

    /// Returns the current node state (role).
    #[inline]
    pub fn state(&self) -> NodeState {
        self.state
    }

    /// Returns `true` if this shard has been marked as failed.
    ///
    /// A failed shard no longer processes events and must not enter
    /// `catch_unwind` dispatch paths.
    #[inline]
    pub fn is_failed(&self) -> bool {
        self.state == NodeState::Failed
    }

    /// Returns the current term.
    #[inline]
    pub fn current_term(&self) -> u64 {
        self.current_term
    }

    /// Returns the candidate this node voted for in the current term.
    #[inline]
    pub fn voted_for(&self) -> Option<NodeId> {
        self.voted_for
    }

    /// Returns the current commit index.
    #[inline]
    pub fn commit_index(&self) -> u64 {
        self.commit_index
    }

    /// Returns a peer's match_index, or `None` if the peer is not tracked.
    pub fn peer_match_index(&self, node: NodeId) -> Option<u64> {
        self.peer_states.iter().find(|ps| ps.id == node).map(|ps| ps.match_index)
    }

    /// Returns the number of entries in the log.
    #[inline]
    pub fn log_len(&self) -> u64 {
        self.log.len() as u64
    }

    /// Returns log entries in the range `[from_index, to_index]` inclusive.
    ///
    /// Uses index arithmetic for O(1) slice computation rather than a linear
    /// scan. Entries outside the in-memory log window are silently omitted. The
    /// caller is responsible for tracking which entries have already been
    /// consumed to avoid re-applying.
    pub fn log_entries(&self, from_index: u64, to_index: u64) -> Vec<&Entry> {
        let Some(first) = self.log.front() else { return vec![] };
        let first_index = first.index;
        if from_index > to_index || to_index < first_index {
            return vec![];
        }
        let start = from_index.saturating_sub(first_index) as usize;
        let end = ((to_index - first_index) as usize + 1).min(self.log.len());
        if start >= self.log.len() {
            return vec![];
        }
        self.log.range(start..end).collect()
    }

    /// Truncates the in-memory log, removing entries before the given index.
    ///
    /// Called after a snapshot is confirmed durable to bound memory usage.
    pub fn truncate_log_before(&mut self, index: u64) {
        while let Some(front) = self.log.front() {
            if front.index < index {
                self.log.pop_front();
            } else {
                break;
            }
        }
    }

    /// Returns a reference to the closed timestamp tracker.
    #[inline]
    pub fn closed_ts(&self) -> &ClosedTimestampTracker {
        &self.closed_ts
    }

    /// Returns the known leader for this shard.
    ///
    /// When this node is the leader, returns `Some(self.node_id)`. Otherwise
    /// returns the last leader observed via [`Message::AppendEntries`], or
    /// `None` if no leader has been seen yet.
    #[inline]
    pub fn leader_id(&self) -> Option<NodeId> {
        if self.state == NodeState::Leader { Some(self.node_id) } else { self.leader_id }
    }

    /// Returns a snapshot of the current observable shard state.
    pub fn state_snapshot(&self) -> crate::leadership::ShardState {
        crate::leadership::ShardState {
            shard: self.id,
            term: self.current_term,
            state: self.state,
            leader: self.leader_id(),
            commit_index: self.commit_index,
            voters: self.membership.voters.clone(),
            learners: self.membership.learners.clone(),
            conf_epoch: self.conf_epoch,
            pending_membership: self.pending_membership,
            last_log_index: self.log.back().map_or(0, |e| e.index),
        }
    }

    // ── Event handlers ──────────────────────────────────────────────

    /// Marks this shard as failed after a panic.
    ///
    /// Once marked, all event handlers return immediately with
    /// [`ConsensusError::ShardUnavailable`] or an empty action list.
    pub fn mark_failed(&mut self) {
        self.state = NodeState::Failed;
    }

    /// Marks this shard as shutdown (removed from membership).
    ///
    /// The shard continues processing in-flight messages during a grace
    /// period but will not initiate elections or heartbeats.
    pub fn mark_shutdown(&mut self) {
        self.state = NodeState::Shutdown;
    }

    /// Restores a shard from `Shutdown` state to `Follower`.
    ///
    /// Used when the cleanup timer discovers the local node is back in the
    /// shard's membership (e.g., because later log entries re-added it after
    /// a spurious `ShardRemoved` during log replay).
    pub fn restore_from_shutdown(&mut self) {
        if self.state == NodeState::Shutdown {
            self.state = NodeState::Follower;
        }
    }

    /// Returns the current membership for this shard.
    pub fn membership(&self) -> &Membership {
        &self.membership
    }

    /// Returns the local node ID for this shard.
    pub fn local_node_id(&self) -> NodeId {
        self.node_id
    }

    /// Called when the election timer fires.
    ///
    /// Followers transition to [`NodeState::PreCandidate`] and send
    /// [`Message::PreVoteRequest`] to all voter peers. Pre-candidates
    /// that time out again also restart pre-voting.
    pub fn handle_election_timeout(&mut self) -> Vec<Action> {
        if self.state == NodeState::Failed {
            return Vec::new();
        }
        // Delegated-leadership shards never initiate elections — their
        // leader is set externally via `adopt_leader`, derived from
        // another shard's elected leader (the region coordinator's
        // leader becomes every per-organization shard's leader under the
        // B.1 unified-leadership model). The timer is rescheduled so the
        // event loop continues to wake at the configured cadence; the
        // wake is just a no-op for election purposes.
        if self.leadership_mode == LeadershipMode::Delegated {
            let mut actions = Vec::new();
            self.reset_election_timer(&mut actions);
            return actions;
        }
        // Non-voters must not start elections. This prevents a node that was
        // added to a data region (but isn't in the voter set yet) from winning
        // an isolated election on its own shard.
        if !self.membership.voters.contains(&self.node_id) {
            let mut actions = Vec::new();
            self.reset_election_timer(&mut actions);
            return actions;
        }
        match self.state {
            NodeState::Follower | NodeState::PreCandidate => self.start_pre_vote(),
            NodeState::Candidate => self.start_pre_vote(),
            NodeState::Leader => Vec::new(),
            NodeState::Failed | NodeState::Shutdown => Vec::new(),
        }
    }

    /// Called when the heartbeat timer fires (leader only).
    ///
    /// Sends empty [`Message::AppendEntries`] heartbeats to all peers.
    pub fn handle_heartbeat_timeout(&mut self) -> Vec<Action> {
        if self.state == NodeState::Failed {
            return Vec::new();
        }
        if self.state != NodeState::Leader {
            return Vec::new();
        }
        let mut actions = self.send_append_entries_to_all();
        actions.push(Action::ScheduleTimer {
            shard: self.id,
            kind: TimerKind::Heartbeat,
            deadline: self.clock.now() + self.config.heartbeat_interval,
        });
        actions
    }

    /// Processes an inbound Raft message from `from`.
    pub fn handle_message(&mut self, from: NodeId, msg: Message) -> Vec<Action> {
        if self.state == NodeState::Failed {
            return Vec::new();
        }
        match msg {
            Message::PreVoteRequest { term, candidate_id, last_log_index, last_log_term } => self
                .handle_pre_vote_request(from, term, candidate_id, last_log_index, last_log_term),
            Message::PreVoteResponse { term, vote_granted } => {
                self.handle_pre_vote_response(term, vote_granted)
            },
            Message::VoteRequest { term, candidate_id, last_log_index, last_log_term } => {
                self.handle_vote_request(from, term, candidate_id, last_log_index, last_log_term)
            },
            Message::VoteResponse { term, vote_granted } => {
                self.handle_vote_response(term, vote_granted)
            },
            Message::AppendEntries {
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
                closed_ts_nanos,
            } => self.handle_append_entries(
                from,
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
                closed_ts_nanos,
            ),
            Message::AppendEntriesResponse { term, success, match_index } => {
                self.handle_append_entries_response(from, term, success, match_index)
            },
            Message::InstallSnapshot {
                term,
                leader_id,
                last_included_index,
                last_included_term,
                ..
            } => self.handle_install_snapshot(
                from,
                term,
                leader_id,
                last_included_index,
                last_included_term,
            ),
            Message::InstallSnapshotResponse { term } => {
                self.handle_install_snapshot_response(from, term)
            },
            Message::TimeoutNow => self.handle_timeout_now(),
        }
    }

    /// Proposes a single entry. Only valid on the leader.
    pub fn handle_propose(&mut self, data: Vec<u8>) -> Result<Vec<Action>, ConsensusError> {
        if self.state == NodeState::Failed {
            return Err(ConsensusError::ShardUnavailable { shard: self.id });
        }
        if self.state != NodeState::Leader {
            return Err(ConsensusError::NotLeader);
        }
        let entry = self.append_entry(data);
        let mut actions = vec![Action::PersistEntries { shard: self.id, entries: vec![entry] }];
        actions.extend(self.send_append_entries_to_all());
        // Single-node cluster: self_match_index already forms quorum — commit immediately
        self.try_advance_commit(&mut actions);
        Ok(actions)
    }

    /// Proposes a batch of entries. Only valid on the leader.
    pub fn handle_propose_batch(
        &mut self,
        batch: Vec<Vec<u8>>,
    ) -> Result<Vec<Action>, ConsensusError> {
        if self.state == NodeState::Failed {
            return Err(ConsensusError::ShardUnavailable { shard: self.id });
        }
        if self.state != NodeState::Leader {
            return Err(ConsensusError::NotLeader);
        }
        let entries: Vec<Entry> = batch.into_iter().map(|data| self.append_entry(data)).collect();
        let mut actions = vec![Action::PersistEntries { shard: self.id, entries }];
        actions.extend(self.send_append_entries_to_all());
        // Single-node cluster: self_match_index already forms quorum — commit immediately
        self.try_advance_commit(&mut actions);
        Ok(actions)
    }

    // ── Pre-vote ────────────────────────────────────────────────────

    fn start_pre_vote(&mut self) -> Vec<Action> {
        if !self.membership.is_voter(self.node_id) {
            let mut actions = Vec::new();
            self.reset_election_timer(&mut actions);
            return actions;
        }
        self.state = NodeState::PreCandidate;
        self.pre_votes_received = 1; // count self
        self.votes_received = 0;

        let (last_log_index, last_log_term) = self.last_log_info();
        // Prospective term: current_term + 1 (not actually incremented yet)
        let prospective_term = self.current_term + 1;

        let mut actions: Vec<Action> = self
            .voter_peers()
            .map(|peer_id| Action::Send {
                to: peer_id,
                shard: self.id,
                msg: Message::PreVoteRequest {
                    term: prospective_term,
                    candidate_id: self.node_id,
                    last_log_index,
                    last_log_term,
                },
            })
            .collect();

        // Single-node cluster: immediately become candidate
        if self.pre_votes_received >= self.membership.quorum() {
            actions.extend(self.start_election());
            return actions;
        }

        self.reset_election_timer(&mut actions);
        actions
    }

    fn handle_pre_vote_request(
        &mut self,
        from: NodeId,
        term: u64,
        _candidate_id: NodeId,
        last_log_index: u64,
        last_log_term: u64,
    ) -> Vec<Action> {
        // Pre-vote does NOT cause term advancement or step-down.
        // Grant if:
        //   1) prospective term >= our term
        //   2) candidate's log is at least as up-to-date
        //   3) our election timer has elapsed (we haven't heard from a valid leader recently)
        let (my_last_index, my_last_term) = self.last_log_info();
        let log_ok = last_log_term > my_last_term
            || (last_log_term == my_last_term && last_log_index >= my_last_index);

        // If we've never heard from a leader (term 0, no leader_id), always
        // consider the election elapsed. This allows fast initial elections for
        // brand-new regions where no leader exists to protect.
        let election_elapsed =
            self.leader_id.is_none() || self.clock.now() >= self.election_deadline;

        let grant = term >= self.current_term && log_ok && election_elapsed;

        vec![Action::Send {
            to: from,
            shard: self.id,
            msg: Message::PreVoteResponse { term, vote_granted: grant },
        }]
    }

    fn handle_pre_vote_response(&mut self, term: u64, vote_granted: bool) -> Vec<Action> {
        if self.state != NodeState::PreCandidate {
            return Vec::new();
        }
        // Pre-vote responses with a higher real term don't step us down
        // (the term in pre-vote is prospective). But if someone reveals
        // a higher *actual* term via a different message, we'd step down there.
        if term > self.current_term + 1 {
            // The responder has a term higher than our prospective term;
            // our election cannot succeed.
            return Vec::new();
        }

        if vote_granted {
            self.pre_votes_received += 1;
            if self.pre_votes_received >= self.membership.quorum() {
                return self.start_election();
            }
        }
        Vec::new()
    }

    // ── Election ────────────────────────────────────────────────────

    fn start_election(&mut self) -> Vec<Action> {
        if !self.membership.is_voter(self.node_id) {
            let mut actions = Vec::new();
            self.reset_election_timer(&mut actions);
            return actions;
        }
        self.current_term += 1;
        self.state = NodeState::Candidate;
        self.voted_for = Some(self.node_id);
        self.votes_received = 1; // self-vote
        self.pre_votes_received = 0;
        // Leadership is contested while we're a candidate.
        self.leader_id = None;

        let (last_log_index, last_log_term) = self.last_log_info();

        // Raft Figure 2: persist currentTerm + votedFor before sending VoteRequests.
        // Placed before Send actions so the reactor persists before transmitting.
        let mut actions: Vec<Action> = vec![Action::PersistTermState {
            shard: self.id,
            term: self.current_term,
            voted_for: self.voted_for,
        }];

        actions.extend(self.voter_peers().map(|peer_id| Action::Send {
            to: peer_id,
            shard: self.id,
            msg: Message::VoteRequest {
                term: self.current_term,
                candidate_id: self.node_id,
                last_log_index,
                last_log_term,
            },
        }));

        // Single-node cluster: immediately become leader
        if self.votes_received >= self.membership.quorum() {
            actions.extend(self.become_leader());
            return actions;
        }

        self.reset_election_timer(&mut actions);
        actions
    }

    fn handle_vote_request(
        &mut self,
        from: NodeId,
        term: u64,
        _candidate_id: NodeId,
        last_log_index: u64,
        last_log_term: u64,
    ) -> Vec<Action> {
        let mut actions = Vec::new();

        if term > self.current_term {
            self.become_follower(term, &mut actions);
        }

        let (my_last_index, my_last_term) = self.last_log_info();
        let log_ok = last_log_term > my_last_term
            || (last_log_term == my_last_term && last_log_index >= my_last_index);

        let can_vote = term == self.current_term
            && (self.voted_for.is_none() || self.voted_for == Some(from))
            && log_ok;

        if can_vote {
            self.voted_for = Some(from);
            // Raft Figure 2: persist votedFor before sending VoteResponse.
            actions.push(Action::PersistTermState {
                shard: self.id,
                term: self.current_term,
                voted_for: self.voted_for,
            });
            self.reset_election_timer(&mut actions);
        }

        actions.push(Action::Send {
            to: from,
            shard: self.id,
            msg: Message::VoteResponse { term: self.current_term, vote_granted: can_vote },
        });
        actions
    }

    fn handle_vote_response(&mut self, term: u64, vote_granted: bool) -> Vec<Action> {
        let mut actions = Vec::new();

        if term > self.current_term {
            self.become_follower(term, &mut actions);
            return actions;
        }

        if self.state != NodeState::Candidate || term != self.current_term {
            return Vec::new();
        }

        if vote_granted {
            self.votes_received += 1;
            if self.votes_received >= self.membership.quorum() {
                actions.extend(self.become_leader());
            }
        }
        actions
    }

    // ── AppendEntries (follower side) ───────────────────────────────

    #[allow(clippy::too_many_arguments)]
    fn handle_append_entries(
        &mut self,
        from: NodeId,
        term: u64,
        leader_id: NodeId,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Arc<[Entry]>,
        leader_commit: u64,
        closed_ts_nanos: u64,
    ) -> Vec<Action> {
        let mut actions = Vec::new();

        // Reject stale term.
        if term < self.current_term {
            actions.push(Action::Send {
                to: from,
                shard: self.id,
                msg: Message::AppendEntriesResponse {
                    term: self.current_term,
                    success: false,
                    match_index: 0,
                },
            });
            return actions;
        }

        // Step down if higher or equal term from a leader.
        if term > self.current_term || self.state != NodeState::Follower {
            self.become_follower(term, &mut actions);
        }

        // Track the leader we're hearing from.
        self.leader_id = Some(leader_id);

        // Valid leader heartbeat — reset election timer.
        self.reset_election_timer(&mut actions);

        // Update closed timestamp from leader.
        self.closed_ts.update(closed_ts_nanos);

        // Check prev_log consistency.
        if prev_log_index > 0 {
            let log_len = self.log.len() as u64;
            if prev_log_index > log_len {
                actions.push(Action::Send {
                    to: from,
                    shard: self.id,
                    msg: Message::AppendEntriesResponse {
                        term: self.current_term,
                        success: false,
                        match_index: log_len,
                    },
                });
                return actions;
            }
            let prev_entry = &self.log[(prev_log_index - 1) as usize];
            if prev_entry.term != prev_log_term {
                // Truncate conflicting suffix.
                self.log.truncate((prev_log_index - 1) as usize);
                actions.push(Action::Send {
                    to: from,
                    shard: self.id,
                    msg: Message::AppendEntriesResponse {
                        term: self.current_term,
                        success: false,
                        match_index: self.log.len() as u64,
                    },
                });
                return actions;
            }
        }

        // Append new entries (skip overlapping entries with matching terms).
        let mut new_entries = Vec::new();
        for entry in entries.iter() {
            let idx = entry.index as usize;
            if idx <= self.log.len() {
                if idx > 0 && self.log[idx - 1].term != entry.term {
                    self.log.truncate(idx - 1);
                    self.log.push_back(entry.clone());
                    new_entries.push(entry.clone());
                }
            } else {
                self.log.push_back(entry.clone());
                new_entries.push(entry.clone());
            }
        }

        if !new_entries.is_empty() {
            actions.push(Action::PersistEntries { shard: self.id, entries: new_entries });
        }

        // Advance commit index.
        if leader_commit > self.commit_index {
            let new_commit = leader_commit.min(self.log.len() as u64);
            if new_commit > self.commit_index {
                let old_commit = self.commit_index;
                self.commit_index = new_commit;
                self.last_applied = new_commit;
                actions.push(Action::Committed { shard: self.id, up_to: new_commit });

                // Check for newly committed membership entries.
                self.apply_committed_membership(old_commit, new_commit, &mut actions);

                // Check if snapshot threshold exceeded.
                self.maybe_trigger_snapshot(&mut actions);
            }
        }

        let current_match = self.log.len() as u64;
        actions.push(Action::Send {
            to: from,
            shard: self.id,
            msg: Message::AppendEntriesResponse {
                term: self.current_term,
                success: true,
                match_index: current_match,
            },
        });
        actions
    }

    // ── AppendEntries response (leader side) ────────────────────────

    fn handle_append_entries_response(
        &mut self,
        from: NodeId,
        term: u64,
        success: bool,
        peer_match_index: u64,
    ) -> Vec<Action> {
        let mut actions = Vec::new();

        if term > self.current_term {
            self.become_follower(term, &mut actions);
            return actions;
        }

        if self.state != NodeState::Leader || term != self.current_term {
            return Vec::new();
        }

        if success {
            if let Some(ps) = self.peer_state_mut(from) {
                ps.match_index = peer_match_index;
                ps.next_index = peer_match_index + 1;
                ps.in_flight = ps.in_flight.saturating_sub(1);
            }
            self.try_advance_commit(&mut actions);
        } else {
            // NACK: rewind next_index to the follower's reported match_index.
            tracing::info!(
                shard = self.id.0,
                from = from.0,
                peer_match_index,
                "AppendEntries NACK — rewinding next_index"
            );
            if let Some(ps) = self.peer_state_mut(from) {
                ps.next_index = peer_match_index + 1;
                ps.in_flight = 0;
            }
            // Resend from the corrected position.
            if let Some(send_actions) = self.send_append_entries_to(from) {
                actions.extend(send_actions);
            }
        }
        actions
    }

    // ── TimeoutNow (leadership transfer) ────────────────────────────

    fn handle_timeout_now(&mut self) -> Vec<Action> {
        // Skip pre-vote, go directly to real election.
        self.start_election()
    }

    /// Transfers leadership to a target node by sending `TimeoutNow`.
    ///
    /// The target must be a voter in the current membership. This node
    /// must be the leader. The target receives a `TimeoutNow` message
    /// causing it to immediately start a real election (skipping pre-vote).
    pub fn handle_transfer_leader(
        &mut self,
        target: NodeId,
    ) -> Result<Vec<Action>, ConsensusError> {
        if self.state == NodeState::Failed {
            return Err(ConsensusError::ShardUnavailable { shard: self.id });
        }
        if self.state != NodeState::Leader {
            return Err(ConsensusError::NotLeader);
        }
        if target == self.node_id {
            return Err(ConsensusError::NotLeader);
        }
        if !self.membership.is_voter(target) {
            return Err(ConsensusError::ShardUnavailable { shard: self.id });
        }

        // Step down to follower BEFORE sending TimeoutNow. This prevents
        // the old leader from continuing to send heartbeats (which would
        // reset followers' election timers and block the target's election)
        // and from winning re-election itself.
        let mut actions = Vec::new();
        self.become_follower(self.current_term, &mut actions);

        // Push the old leader's election deadline far enough into the future
        // that the transfer target has time to win. The default become_follower
        // sets a 300-600ms timeout, which is too aggressive — the old leader
        // would start its own election and compete with the target.
        let transfer_grace = self.config.election_timeout_max * 3;
        self.election_deadline = self.clock.now() + transfer_grace;
        // Replace the ScheduleTimer action with the extended deadline.
        actions.retain(|a| !matches!(a, Action::ScheduleTimer { kind: TimerKind::Election, .. }));
        actions.push(Action::ScheduleTimer {
            shard: self.id,
            kind: TimerKind::Election,
            deadline: self.election_deadline,
        });

        actions.push(Action::Send { to: target, shard: self.id, msg: Message::TimeoutNow });
        Ok(actions)
    }

    /// Triggers a snapshot at the current commit index.
    ///
    /// Returns a [`Action::TriggerSnapshot`] action if there are committed
    /// entries to snapshot. Returns an empty action list if the commit index
    /// is zero or already at the last snapshot index.
    pub fn handle_trigger_snapshot(&self) -> Vec<Action> {
        if self.state == NodeState::Failed {
            return Vec::new();
        }
        if self.commit_index == 0 || self.commit_index <= self.last_snapshot_index {
            return Vec::new();
        }
        let last_included_index = self.commit_index;
        let last_included_term = self
            .log
            .iter()
            .rfind(|e| e.index <= last_included_index)
            .map_or(self.current_term, |e| e.term);
        vec![Action::TriggerSnapshot { shard: self.id, last_included_index, last_included_term }]
    }

    /// Proposes a membership change. Only valid on the leader.
    pub fn handle_membership_change(
        &mut self,
        change: MembershipChange,
    ) -> Result<Vec<Action>, ConsensusError> {
        if self.state == NodeState::Failed {
            return Err(ConsensusError::ShardUnavailable { shard: self.id });
        }
        if self.state != NodeState::Leader {
            return Err(ConsensusError::NotLeader);
        }
        if self.pending_membership {
            return Err(ConsensusError::MembershipChangePending);
        }
        // Ongaro §4.1: reject membership changes until the leader has committed
        // an entry in its current term. Without this, uncommitted config entries
        // from a previous term can create non-overlapping quorums.
        if self.last_committed_term != self.current_term {
            return Err(ConsensusError::LeaderNotReady);
        }

        // Epoch fencing: reject if the caller's expected epoch doesn't match.
        let expected_epoch = match &change {
            MembershipChange::AddLearner { expected_conf_epoch, .. }
            | MembershipChange::PromoteVoter { expected_conf_epoch, .. }
            | MembershipChange::RemoveNode { expected_conf_epoch, .. } => *expected_conf_epoch,
        };
        if let Some(expected) = expected_epoch
            && expected != self.conf_epoch
        {
            return Err(ConsensusError::StaleEpoch { expected, actual: self.conf_epoch });
        }

        // Self-removal is allowed — the leader commits the membership change
        // with the current membership (including itself), then steps down after
        // the new membership takes effect. The remaining voters elect a new leader.

        let mut new_membership = self.membership.clone();
        match &change {
            MembershipChange::AddLearner { node_id, .. } => {
                new_membership.add_learner(*node_id);
            },
            MembershipChange::PromoteVoter { node_id, .. } => {
                new_membership.promote_learner(*node_id);
            },
            MembershipChange::RemoveNode { node_id, .. } => {
                new_membership.remove_voter(*node_id);
                new_membership.remove_learner(*node_id);
            },
        }

        // Reject no-op changes — if the membership didn't actually change,
        // proposing it would create a pending entry that blocks subsequent
        // real membership changes.
        if new_membership == self.membership {
            return Err(ConsensusError::InvalidMembershipChange {
                reason: "membership change is a no-op".into(),
            });
        }

        let index = self.log.len() as u64 + 1;
        let entry = Entry {
            term: self.current_term,
            index,
            data: Arc::from(Vec::new()),
            kind: EntryKind::Membership(new_membership),
        };
        self.log.push_back(entry.clone());
        self.self_match_index = index;
        self.pending_membership = true;

        // Add peer state for new learner/voter if needed.
        match &change {
            MembershipChange::AddLearner { node_id, promotable, .. } => {
                if !self.peer_states.iter().any(|p| p.id == *node_id) {
                    self.peer_states.push(PeerState::learner(
                        *node_id,
                        self.log.len() as u64 + 1,
                        *promotable,
                    ));
                }
            },
            MembershipChange::PromoteVoter { node_id, .. } => {
                if let Some(ps) = self.peer_state_mut(*node_id) {
                    ps.is_learner = false;
                }
            },
            MembershipChange::RemoveNode { .. } => {},
        }

        let mut actions = vec![Action::PersistEntries { shard: self.id, entries: vec![entry] }];
        actions.extend(self.send_append_entries_to_all());
        // Single-node cluster: self_match_index already forms quorum — commit immediately
        self.try_advance_commit(&mut actions);
        Ok(actions)
    }

    // ── InstallSnapshot ────────────────────────────────────────────

    fn handle_install_snapshot(
        &mut self,
        from: NodeId,
        term: u64,
        leader_id: NodeId,
        _last_included_index: u64,
        _last_included_term: u64,
    ) -> Vec<Action> {
        let mut actions = Vec::new();

        // Reject stale term.
        if term < self.current_term {
            actions.push(Action::Send {
                to: from,
                shard: self.id,
                msg: Message::InstallSnapshotResponse { term: self.current_term },
            });
            return actions;
        }

        // Step down if higher term.
        if term > self.current_term || self.state != NodeState::Follower {
            self.become_follower(term, &mut actions);
        }

        // Track the leader sending this snapshot.
        self.leader_id = Some(leader_id);

        // Valid leader contact — reset election timer.
        self.reset_election_timer(&mut actions);

        // Acknowledge receipt. Actual snapshot application is handled by the
        // reactor/apply worker, not the shard state machine.
        actions.push(Action::Send {
            to: from,
            shard: self.id,
            msg: Message::InstallSnapshotResponse { term: self.current_term },
        });
        actions
    }

    fn handle_install_snapshot_response(&mut self, from: NodeId, term: u64) -> Vec<Action> {
        let mut actions = Vec::new();
        if term > self.current_term {
            self.become_follower(term, &mut actions);
            return actions;
        }
        // Peer has applied the snapshot — resume replication from after the snapshot.
        if self.state == NodeState::Leader
            && let Some(ps) = self.peer_states.iter_mut().find(|p| p.id == from)
        {
            ps.in_flight = ps.in_flight.saturating_sub(1);
            if ps.next_index <= self.last_snapshot_index {
                ps.next_index = self.last_snapshot_index + 1;
                ps.match_index = self.last_snapshot_index;
            }
        }
        actions
    }

    // ── State transitions ───────────────────────────────────────────

    fn become_follower(&mut self, term: u64, actions: &mut Vec<Action>) {
        self.current_term = term;
        self.state = NodeState::Follower;
        self.voted_for = None;
        self.votes_received = 0;
        self.pre_votes_received = 0;
        self.peer_states.clear();
        self.lease.invalidate();
        // Raft Figure 2: persist currentTerm + votedFor before responding.
        actions.push(Action::PersistTermState {
            shard: self.id,
            term: self.current_term,
            voted_for: self.voted_for,
        });
        self.reset_election_timer(actions);
    }

    fn become_leader(&mut self) -> Vec<Action> {
        self.state = NodeState::Leader;
        let next = self.log.len() as u64 + 1;

        // Initialize peer states for all other voters + learners.
        self.peer_states = self
            .membership
            .voters
            .iter()
            .filter(|&&id| id != self.node_id)
            .map(|&id| PeerState::voter(id, next))
            .chain(self.membership.learners.iter().map(|&id| PeerState::learner(id, next, false)))
            .collect();

        // Commit a no-op entry at the new term (Raft paper §5.4.2).
        // This establishes the new leader's commit_index by forcing a quorum
        // acknowledgement. Without it, pending entries from previous terms
        // (including membership changes) may never be applied because the
        // new leader's commit_index is stale — especially if the old leader
        // crashed before broadcasting the updated leader_commit.
        let noop = self.append_entry(Vec::new());
        self.self_match_index = noop.index;

        let mut actions = vec![Action::PersistEntries { shard: self.id, entries: vec![noop] }];

        // Schedule heartbeat timer.
        actions.push(Action::ScheduleTimer {
            shard: self.id,
            kind: TimerKind::Heartbeat,
            deadline: self.clock.now() + self.config.heartbeat_interval,
        });

        // Send initial heartbeats (includes the no-op entry).
        actions.extend(self.send_append_entries_to_all());

        // For single-node clusters, advance commit immediately
        // (the self_match_index already satisfies quorum).
        self.try_advance_commit(&mut actions);

        actions
    }

    // ── Replication helpers ─────────────────────────────────────────

    fn send_append_entries_to_all(&mut self) -> Vec<Action> {
        let peer_ids: Vec<NodeId> = self.peer_states.iter().map(|ps| ps.id).collect();
        let mut actions = Vec::new();
        for peer_id in peer_ids {
            if let Some(peer_actions) = self.send_append_entries_to(peer_id) {
                actions.extend(peer_actions);
            }
        }
        actions
    }

    fn send_append_entries_to(&mut self, peer_id: NodeId) -> Option<Vec<Action>> {
        // BUGGIFY: 1% chance to skip sending entries (simulates message loss).
        if crate::buggify::buggify(0.01) {
            return None;
        }

        // Collect peer state we need before any other borrows.
        let (in_flight, next) = {
            let ps = self.peer_states.iter().find(|p| p.id == peer_id)?;
            (ps.in_flight, ps.next_index)
        };

        // Respect pipeline depth.
        if in_flight >= self.config.pipeline_depth {
            return None;
        }

        // If the follower needs entries that have been compacted (behind the
        // snapshot boundary), delegate snapshot transfer to the reactor via
        // Action::SendSnapshot. The reactor routes this to a SnapshotSender
        // callback when available, falling back to an InstallSnapshot message.
        if self.last_snapshot_index > 0 && next <= self.last_snapshot_index {
            let last_included_term = self.log.front().map_or(self.current_term, |e| e.term);
            if let Some(ps) = self.peer_states.iter_mut().find(|p| p.id == peer_id) {
                ps.in_flight += 1;
            }
            return Some(vec![Action::SendSnapshot {
                to: peer_id,
                shard: self.id,
                term: self.current_term,
                leader_id: self.node_id,
                last_included_index: self.last_snapshot_index,
                last_included_term,
            }]);
        }

        let prev_log_index = next.saturating_sub(1);
        let prev_log_term = if prev_log_index > 0 {
            self.log.get((prev_log_index - 1) as usize).map_or(0, |e| e.term)
        } else {
            0
        };

        let start = (next - 1) as usize;
        let log_len = self.log.len();
        let end = log_len.min(start + self.config.max_entries_per_rpc as usize);
        let entries: Arc<[Entry]> = if start < log_len {
            Arc::from(self.log.range(start..end).cloned().collect::<Vec<_>>())
        } else {
            Arc::from(Vec::new())
        };

        // Temporary: log catch-up progress for lagging peers.
        let (match_index, is_learner) = self
            .peer_states
            .iter()
            .find(|p| p.id == peer_id)
            .map_or((0, false), |p| (p.match_index, p.is_learner));
        if match_index == 0 && log_len > 0 {
            tracing::info!(
                shard = self.id.0,
                peer = peer_id.0,
                next_index = next,
                prev_log_index,
                log_len,
                entries_count = entries.len(),
                in_flight,
                is_learner,
                "Sending AppendEntries to lagging peer"
            );
        }

        if let Some(ps) = self.peer_states.iter_mut().find(|p| p.id == peer_id) {
            ps.in_flight += 1;
        }

        Some(vec![Action::Send {
            to: peer_id,
            shard: self.id,
            msg: Message::AppendEntries {
                term: self.current_term,
                leader_id: self.node_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: self.commit_index,
                closed_ts_nanos: self.closed_ts.compute_leader_closed_ts(),
            },
        }])
    }

    // ── Commit logic ────────────────────────────────────────────────

    fn try_advance_commit(&mut self, actions: &mut Vec<Action>) {
        let mut matches: Vec<u64> = self.peer_states.iter().map(|p| p.match_index).collect();
        matches.push(self.self_match_index);
        matches.sort_unstable();

        let quorum_idx = matches.len() - self.membership.quorum();
        let quorum_match = matches[quorum_idx];

        if quorum_match > self.commit_index {
            // Only commit entries from the current term (Raft safety property).
            let log_len = self.log.len() as u64;
            if quorum_match <= log_len
                && self.log[(quorum_match - 1) as usize].term == self.current_term
            {
                let old_commit = self.commit_index;
                self.commit_index = quorum_match;
                self.last_committed_term = self.log[(quorum_match - 1) as usize].term;
                self.last_applied = quorum_match;
                actions.push(Action::Committed { shard: self.id, up_to: quorum_match });
                // Renew leader lease on commit advance.
                self.lease.renew();
                actions.push(Action::RenewLease { shard: self.id });

                // Check for newly committed membership entries.
                self.apply_committed_membership(old_commit, quorum_match, actions);

                // Check if snapshot threshold exceeded.
                self.maybe_trigger_snapshot(actions);
            }
        }

        // Check for auto-promotable learners (only when shard manages its own promotions).
        if self.config.auto_promote {
            let log_tip = self.log.back().map_or(0, |e| e.index);
            if !self.pending_membership {
                for ps in &self.peer_states {
                    if ps.is_learner
                        && ps.promotable
                        && log_tip > 0
                        && ps.match_index + self.config.auto_promote_threshold >= log_tip
                    {
                        // Learner is caught up — propose auto-promotion via membership change.
                        let mut new_membership = self.membership.clone();
                        new_membership.promote_learner(ps.id);
                        actions.push(Action::MembershipChanged {
                            shard: self.id,
                            membership: new_membership,
                        });
                        break; // One at a time
                    }
                }
            }
        }
    }

    /// Checks newly committed entries for membership changes and applies them.
    ///
    /// Processes each membership entry in the committed range, updating
    /// `self.membership`, emitting a per-entry `MembershipChanged` action,
    /// and stepping down if the local leader was removed. A single
    /// `ShardRemoved` action is emitted at most once per call, and only
    /// when the **final** membership after processing the entire batch
    /// excludes the local node.
    ///
    /// Checking only the final membership — rather than every intermediate
    /// configuration — prevents spurious `Shutdown` cycles on late-joiners
    /// catching up through a history of configuration churn. A late-joiner
    /// typically replays entries shaped like
    /// `{A} → {A,B} → {A,B,C-learner} → {A,B,C-voter}`: intermediate
    /// entries transiently exclude node C before later entries add it
    /// back. Emitting `ShardRemoved` per entry would mark the shard
    /// `Shutdown` for a 30s grace period even though the final
    /// configuration includes C, blocking replication until the cleanup
    /// timer expires.
    fn apply_committed_membership(
        &mut self,
        old_commit: u64,
        new_commit: u64,
        actions: &mut Vec<Action>,
    ) {
        for idx in (old_commit + 1)..=new_commit {
            if let Some(entry) = self.log.get((idx - 1) as usize)
                && let EntryKind::Membership(ref new_membership) = entry.kind
            {
                self.membership = new_membership.clone();
                self.pending_membership = false;
                self.conf_epoch += 1;

                // If this node was removed from the voter set, send one final
                // heartbeat so followers learn the updated commit_index (which
                // includes this membership change), then step down.
                if self.state == NodeState::Leader && !self.membership.is_voter(self.node_id) {
                    actions.extend(self.send_append_entries_to_all());
                    self.become_follower(self.current_term, actions);
                }

                // Rebuild peer_states if we are leader.
                if self.state == NodeState::Leader {
                    let next = self.log.len() as u64 + 1;
                    self.peer_states = self
                        .membership
                        .voters
                        .iter()
                        .filter(|&&id| id != self.node_id)
                        .map(|&id| PeerState::voter(id, next))
                        .chain(self.membership.learners.iter().map(|&id| {
                            // Preserve the promotable flag from any existing peer state.
                            let promotable = self
                                .peer_states
                                .iter()
                                .find(|p| p.id == id)
                                .is_some_and(|p| p.promotable);
                            PeerState::learner(id, next, promotable)
                        }))
                        .collect();
                }

                actions.push(Action::MembershipChanged {
                    shard: self.id,
                    membership: self.membership.clone(),
                });

                // Track whether we have ever been part of this shard.
                if self.membership.is_voter(self.node_id)
                    || self.membership.is_learner(self.node_id)
                {
                    self.was_ever_member = true;
                }
            }
        }

        // Emit ShardRemoved once, based on the final membership after
        // processing the entire batch. Guarded by `was_ever_member` so log
        // replay on a brand-new node (which has never been a member) does
        // not signal removal.
        if self.was_ever_member
            && !self.membership.is_voter(self.node_id)
            && !self.membership.is_learner(self.node_id)
        {
            actions.push(Action::ShardRemoved { shard: self.id });
        }
    }

    /// Emits a `TriggerSnapshot` action if the log has grown past the snapshot threshold.
    ///
    /// This method does **not** advance `last_snapshot_index`. The caller
    /// (reactor / external coordinator) must call [`handle_snapshot_completed`]
    /// after the snapshot has been persisted successfully. A
    /// `snapshot_in_flight` guard prevents duplicate trigger actions while a
    /// snapshot is being persisted.
    fn maybe_trigger_snapshot(&mut self, actions: &mut Vec<Action>) {
        if self.snapshot_in_flight {
            return;
        }
        let entries_since_snapshot = self.commit_index.saturating_sub(self.last_snapshot_index);
        if entries_since_snapshot >= self.config.snapshot_threshold
            && let Some(entry) = self.log.get((self.commit_index - 1) as usize)
        {
            let term = entry.term;
            self.snapshot_in_flight = true;
            actions.push(Action::TriggerSnapshot {
                shard: self.id,
                last_included_index: self.commit_index,
                last_included_term: term,
            });
        }
    }

    /// Advances `last_snapshot_index` after a snapshot has been successfully persisted.
    ///
    /// The reactor calls this once the external coordinator confirms the snapshot
    /// at `last_included_index` is durable. This ensures the shard never believes
    /// it has snapshotted beyond what is actually on disk.
    pub fn handle_snapshot_completed(&mut self, last_included_index: u64) {
        self.snapshot_in_flight = false;
        if last_included_index > self.last_snapshot_index {
            self.last_snapshot_index = last_included_index;
        }
    }

    // ── Log helpers ─────────────────────────────────────────────────

    fn append_entry(&mut self, data: Vec<u8>) -> Entry {
        let index = self.log.len() as u64 + 1;
        let entry = Entry {
            term: self.current_term,
            index,
            data: Arc::from(data),
            kind: EntryKind::Normal,
        };
        self.log.push_back(entry.clone());
        self.self_match_index = index;
        entry
    }

    fn last_log_info(&self) -> (u64, u64) {
        match self.log.back() {
            Some(entry) => (entry.index, entry.term),
            None => (0, 0),
        }
    }

    // ── Peer helpers ────────────────────────────────────────────────

    fn peer_state_mut(&mut self, id: NodeId) -> Option<&mut PeerState> {
        self.peer_states.iter_mut().find(|p| p.id == id)
    }

    /// Returns an iterator over voter peer IDs (excluding self).
    fn voter_peers(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.membership.voters.iter().copied().filter(|&id| id != self.node_id)
    }

    // ── Timer helpers ───────────────────────────────────────────────

    fn reset_election_timer(&mut self, actions: &mut Vec<Action>) {
        let timeout = self.rng.election_timeout(
            self.config.election_timeout_min.as_millis() as u64,
            self.config.election_timeout_max.as_millis() as u64,
        );
        let deadline = self.clock.now() + timeout;
        self.election_deadline = deadline;
        actions.push(Action::ScheduleTimer { shard: self.id, kind: TimerKind::Election, deadline });
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods, clippy::panic)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::{clock::SimulatedClock, rng::SimulatedRng};

    fn make_clock() -> Arc<SimulatedClock> {
        Arc::new(SimulatedClock::new())
    }

    fn make_rng() -> SimulatedRng {
        SimulatedRng::new(42)
    }

    fn make_membership_3() -> Membership {
        Membership::new([NodeId(1), NodeId(2), NodeId(3)])
    }

    fn make_shard(
        node_id: NodeId,
        clock: Arc<SimulatedClock>,
        rng: SimulatedRng,
        membership: Membership,
    ) -> Shard<Arc<SimulatedClock>, SimulatedRng> {
        Shard::new(ShardId(1), node_id, membership, ShardConfig::default(), clock, rng, 0, None, 0)
    }

    fn make_3node_shard(
        node_id: u64,
    ) -> (Arc<SimulatedClock>, Shard<Arc<SimulatedClock>, SimulatedRng>) {
        let clock = make_clock();
        let shard = make_shard(NodeId(node_id), clock.clone(), make_rng(), make_membership_3());
        (clock, shard)
    }

    /// Extract Send actions targeting a specific node.
    fn sends_to(actions: &[Action], to: NodeId) -> Vec<&Message> {
        actions
            .iter()
            .filter_map(|a| match a {
                Action::Send { to: t, msg, .. } if *t == to => Some(msg),
                _ => None,
            })
            .collect()
    }

    /// Extract all Send actions.
    fn all_sends(actions: &[Action]) -> Vec<(NodeId, &Message)> {
        actions
            .iter()
            .filter_map(|a| match a {
                Action::Send { to, msg, .. } => Some((*to, msg)),
                _ => None,
            })
            .collect()
    }

    /// Check if actions contain a RenewLease.
    fn has_renew_lease(actions: &[Action]) -> bool {
        actions.iter().any(|a| matches!(a, Action::RenewLease { .. }))
    }

    /// Elect a node as leader by simulating pre-vote + vote rounds.
    fn elect_leader(shard: &mut Shard<Arc<SimulatedClock>, SimulatedRng>, clock: &SimulatedClock) {
        // Advance past election deadline.
        clock.advance(std::time::Duration::from_secs(1));
        let _actions = shard.handle_election_timeout();

        // Should be PreCandidate, sending PreVoteRequests.
        assert_eq!(shard.state(), NodeState::PreCandidate);

        // Grant pre-votes from both peers (quorum = 2, self counts as 1).
        let prospective_term = shard.current_term() + 1;
        let _actions = shard.handle_message(
            NodeId(2),
            Message::PreVoteResponse { term: prospective_term, vote_granted: true },
        );

        // Should now be Candidate.
        assert_eq!(shard.state(), NodeState::Candidate);

        // Grant real vote from one peer (self-vote + 1 = quorum of 2).
        let term = shard.current_term();
        let _actions =
            shard.handle_message(NodeId(2), Message::VoteResponse { term, vote_granted: true });

        assert_eq!(shard.state(), NodeState::Leader);

        // Commit the no-op by simulating an AppendEntriesResponse from node 2.
        // Without this, last_committed_term remains 0 and handle_membership_change
        // would return LeaderNotReady (Ongaro §4.1 guard).
        let noop_index = shard.log_len();
        let current_term = shard.current_term();
        let _ = shard.handle_message(
            NodeId(2),
            Message::AppendEntriesResponse {
                term: current_term,
                success: true,
                match_index: noop_index,
            },
        );

        assert_eq!(
            shard.commit_index(),
            noop_index,
            "no-op must commit before leaving elect_leader"
        );
    }

    // ── Tests ───────────────────────────────────────────────────────

    // ── Construction & accessors ───────────────────────────────────

    #[test]
    fn test_new_shard_initial_state_is_follower_at_term_zero() {
        let (_clock, shard) = make_3node_shard(1);

        assert_eq!(shard.state(), NodeState::Follower);
        assert_eq!(shard.current_term(), 0);
    }

    #[test]
    fn test_state_snapshot_reflects_current_shard_state() {
        let (clock, mut shard) = make_3node_shard(1);

        let snap = shard.state_snapshot();
        assert_eq!(snap.shard, ShardId(1));
        assert_eq!(snap.term, 0);
        assert_eq!(snap.state, NodeState::Follower);
        assert!(snap.leader.is_none());
        assert_eq!(snap.commit_index, 0);

        elect_leader(&mut shard, &clock);

        let snap = shard.state_snapshot();
        assert_eq!(snap.state, NodeState::Leader);
        assert_eq!(snap.leader, Some(NodeId(1)));
    }

    #[test]
    fn test_leader_id_returns_self_when_leader() {
        let (clock, mut shard) = make_3node_shard(1);
        elect_leader(&mut shard, &clock);

        assert_eq!(shard.leader_id(), Some(NodeId(1)));
    }

    #[test]
    fn test_leader_id_returns_tracked_leader_from_append_entries() {
        let (_clock, mut shard) = make_3node_shard(2);

        assert_eq!(shard.leader_id(), None);

        shard.handle_message(
            NodeId(1),
            Message::AppendEntries {
                term: 1,
                leader_id: NodeId(1),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: Arc::from(Vec::new()),
                leader_commit: 0,
                closed_ts_nanos: 0,
            },
        );

        assert_eq!(shard.leader_id(), Some(NodeId(1)));
    }

    // ── Election timeout ───────────────────────────────────────────

    #[test]
    fn test_election_timeout_follower_transitions_to_pre_candidate() {
        let (clock, mut shard) = make_3node_shard(1);

        clock.advance(std::time::Duration::from_secs(1));
        let actions = shard.handle_election_timeout();

        assert_eq!(shard.state(), NodeState::PreCandidate);
        assert_eq!(shard.current_term(), 0);

        let pre_vote_count = all_sends(&actions)
            .iter()
            .filter(|(_, msg)| matches!(msg, Message::PreVoteRequest { .. }))
            .count();
        assert_eq!(pre_vote_count, 2);
    }

    #[test]
    fn test_election_timeout_leader_is_noop() {
        let (clock, mut shard) = make_3node_shard(1);
        elect_leader(&mut shard, &clock);

        clock.advance(std::time::Duration::from_secs(1));
        let actions = shard.handle_election_timeout();

        assert_eq!(shard.state(), NodeState::Leader);
        assert!(actions.is_empty());
    }

    #[test]
    fn test_election_timeout_failed_shard_returns_empty() {
        let (clock, mut shard) = make_3node_shard(1);
        clock.advance(std::time::Duration::from_secs(1));
        shard.mark_failed();

        let actions = shard.handle_election_timeout();

        assert!(actions.is_empty());
        assert_eq!(shard.state(), NodeState::Failed);
    }

    // ── Heartbeat timeout ──────────────────────────────────────────

    #[test]
    fn test_heartbeat_timeout_leader_sends_append_entries() {
        let (clock, mut shard) = make_3node_shard(1);
        elect_leader(&mut shard, &clock);

        let actions = shard.handle_heartbeat_timeout();

        let ae_count = all_sends(&actions)
            .iter()
            .filter(|(_, msg)| matches!(msg, Message::AppendEntries { .. }))
            .count();
        assert_eq!(ae_count, 2, "leader should send heartbeats to 2 peers");

        let timer_count = actions
            .iter()
            .filter(|a| matches!(a, Action::ScheduleTimer { kind: TimerKind::Heartbeat, .. }))
            .count();
        assert_eq!(timer_count, 1, "should reschedule heartbeat timer");
    }

    #[test]
    fn test_heartbeat_timeout_follower_is_noop() {
        let (_clock, mut shard) = make_3node_shard(1);

        let actions = shard.handle_heartbeat_timeout();

        assert!(actions.is_empty());
    }

    // ── Pre-vote ───────────────────────────────────────────────────

    #[test]
    fn test_pre_vote_quorum_transitions_to_candidate() {
        let (clock, mut shard) = make_3node_shard(1);

        clock.advance(std::time::Duration::from_secs(1));
        let _actions = shard.handle_election_timeout();
        assert_eq!(shard.state(), NodeState::PreCandidate);

        let prospective_term = shard.current_term() + 1;
        let actions = shard.handle_message(
            NodeId(2),
            Message::PreVoteResponse { term: prospective_term, vote_granted: true },
        );

        assert_eq!(shard.state(), NodeState::Candidate);
        assert_eq!(shard.current_term(), 1);

        let vote_request_count = all_sends(&actions)
            .iter()
            .filter(|(_, msg)| matches!(msg, Message::VoteRequest { .. }))
            .count();
        assert_eq!(vote_request_count, 2);
    }

    #[test]
    fn test_pre_vote_rejected_remains_pre_candidate() {
        let (clock, mut shard) = make_3node_shard(1);

        clock.advance(std::time::Duration::from_secs(1));
        let _actions = shard.handle_election_timeout();
        assert_eq!(shard.state(), NodeState::PreCandidate);

        let prospective_term = shard.current_term() + 1;
        shard.handle_message(
            NodeId(2),
            Message::PreVoteResponse { term: prospective_term, vote_granted: false },
        );
        shard.handle_message(
            NodeId(3),
            Message::PreVoteResponse { term: prospective_term, vote_granted: false },
        );

        assert_eq!(shard.state(), NodeState::PreCandidate);
        assert_eq!(shard.current_term(), 0);
    }

    // ── Vote / leader election ─────────────────────────────────────

    #[test]
    fn test_vote_quorum_transitions_to_leader() {
        let (clock, mut shard) = make_3node_shard(1);

        elect_leader(&mut shard, &clock);

        assert_eq!(shard.state(), NodeState::Leader);
        assert_eq!(shard.current_term(), 1);
    }

    #[test]
    fn test_single_node_cluster_immediately_becomes_leader() {
        let clock = make_clock();
        let membership = Membership::new([NodeId(1)]);
        let mut shard = make_shard(NodeId(1), clock.clone(), make_rng(), membership);

        clock.advance(std::time::Duration::from_secs(1));
        let _actions = shard.handle_election_timeout();

        assert_eq!(shard.state(), NodeState::Leader);
        assert_eq!(shard.current_term(), 1);
    }

    // ── Step-down ──────────────────────────────────────────────────

    #[test]
    fn test_higher_term_append_entries_causes_step_down() {
        let (clock, mut shard) = make_3node_shard(1);
        elect_leader(&mut shard, &clock);
        assert_eq!(shard.state(), NodeState::Leader);

        shard.handle_message(
            NodeId(2),
            Message::AppendEntries {
                term: 10,
                leader_id: NodeId(2),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: Arc::from(Vec::new()),
                leader_commit: 0,
                closed_ts_nanos: 0,
            },
        );

        assert_eq!(shard.state(), NodeState::Follower);
        assert_eq!(shard.current_term(), 10);
    }

    // ── Propose ────────────────────────────────────────────────────

    #[test]
    fn test_propose_leader_appends_and_replicates() {
        let (clock, mut shard) = make_3node_shard(1);
        elect_leader(&mut shard, &clock);

        let noop_len = shard.log_len(); // no-op from become_leader
        let actions = shard.handle_propose(b"hello".to_vec()).unwrap();

        assert_eq!(shard.log_len(), noop_len + 1);

        let persist_count =
            actions.iter().filter(|a| matches!(a, Action::PersistEntries { .. })).count();
        assert_eq!(persist_count, 1);

        let send_count = actions.iter().filter(|a| matches!(a, Action::Send { .. })).count();
        assert!(send_count > 0);
    }

    #[test]
    fn test_propose_follower_returns_not_leader() {
        let (_clock, mut shard) = make_3node_shard(1);

        let result = shard.handle_propose(b"hello".to_vec());

        assert!(matches!(result, Err(ConsensusError::NotLeader)));
    }

    #[test]
    fn test_propose_failed_shard_returns_shard_unavailable() {
        let (_clock, mut shard) = make_3node_shard(1);
        shard.mark_failed();

        let result = shard.handle_propose(b"hello".to_vec());

        assert!(matches!(result, Err(ConsensusError::ShardUnavailable { .. })));
    }

    // ── Propose batch ──────────────────────────────────────────────

    #[test]
    fn test_propose_batch_leader_appends_all_entries() {
        let (clock, mut shard) = make_3node_shard(1);
        elect_leader(&mut shard, &clock);

        let noop_len = shard.log_len();
        let batch = vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()];
        let actions = shard.handle_propose_batch(batch).unwrap();

        assert_eq!(shard.log_len(), noop_len + 3);

        let persist: Vec<_> = actions
            .iter()
            .filter_map(|a| match a {
                Action::PersistEntries { entries, .. } => Some(entries),
                _ => None,
            })
            .collect();
        assert_eq!(persist.len(), 1);
        assert_eq!(persist[0].len(), 3);
    }

    #[test]
    fn test_propose_batch_follower_returns_not_leader() {
        let (_clock, mut shard) = make_3node_shard(1);

        let result = shard.handle_propose_batch(vec![b"a".to_vec()]);

        assert!(matches!(result, Err(ConsensusError::NotLeader)));
    }

    #[test]
    fn test_propose_batch_failed_shard_returns_shard_unavailable() {
        let (_clock, mut shard) = make_3node_shard(1);
        shard.mark_failed();

        let result = shard.handle_propose_batch(vec![b"a".to_vec()]);

        assert!(matches!(result, Err(ConsensusError::ShardUnavailable { .. })));
    }

    // ── AppendEntries ──────────────────────────────────────────────

    #[test]
    fn test_append_entries_valid_advances_commit_index() {
        let (_clock, mut shard) = make_3node_shard(2);

        let entries: Arc<[Entry]> = Arc::from(vec![Entry {
            term: 1,
            index: 1,
            data: Arc::from(b"data".to_vec()),
            kind: EntryKind::Normal,
        }]);

        let actions = shard.handle_message(
            NodeId(1),
            Message::AppendEntries {
                term: 1,
                leader_id: NodeId(1),
                prev_log_index: 0,
                prev_log_term: 0,
                entries,
                leader_commit: 1,
                closed_ts_nanos: 0,
            },
        );

        assert_eq!(shard.commit_index(), 1);
        assert_eq!(shard.log_len(), 1);

        let sends = all_sends(&actions);
        let response = sends.iter().find(|(to, _)| *to == NodeId(1));
        assert!(response.is_some());
        match response.unwrap().1 {
            Message::AppendEntriesResponse { success, match_index, .. } => {
                assert!(success);
                assert_eq!(*match_index, 1);
            },
            _ => panic!("expected AppendEntriesResponse"),
        }
    }

    #[test]
    fn test_append_entries_stale_term_rejected() {
        let (_clock, mut shard) = make_3node_shard(2);
        {
            let mut actions = Vec::new();
            shard.become_follower(5, &mut actions);
        }

        let actions = shard.handle_message(
            NodeId(1),
            Message::AppendEntries {
                term: 3,
                leader_id: NodeId(1),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: Arc::from(Vec::new()),
                leader_commit: 0,
                closed_ts_nanos: 0,
            },
        );

        let sends = all_sends(&actions);
        match sends[0].1 {
            Message::AppendEntriesResponse { success, term, .. } => {
                assert!(!success);
                assert_eq!(*term, 5);
            },
            _ => panic!("expected AppendEntriesResponse"),
        }
    }

    // ── AppendEntries response (leader-side NACK) ──────────────────

    #[test]
    fn test_nack_rewinds_next_index_and_resends() {
        let (clock, mut shard) = make_3node_shard(1);
        elect_leader(&mut shard, &clock);

        let noop_len = shard.log_len(); // no-op from become_leader

        for i in 0..3 {
            let _ = shard.handle_propose(format!("entry{i}").into_bytes()).unwrap();
        }

        // NACK: follower only has entries up to the no-op
        let actions = shard.handle_message(
            NodeId(2),
            Message::AppendEntriesResponse {
                term: shard.current_term(),
                success: false,
                match_index: noop_len,
            },
        );

        let ps = shard.peer_states.iter().find(|p| p.id == NodeId(2)).unwrap();
        assert_eq!(ps.next_index, noop_len + 1);
        assert_eq!(ps.in_flight, 1);

        let sends = sends_to(&actions, NodeId(2));
        assert!(!sends.is_empty());
        match sends[0] {
            Message::AppendEntries { prev_log_index, entries, .. } => {
                assert_eq!(*prev_log_index, noop_len);
                assert_eq!(entries.len(), 3); // 3 proposed entries
            },
            _ => panic!("expected AppendEntries"),
        }
    }

    // ── Pipeline depth ─────────────────────────────────────────────

    #[test]
    fn test_pipeline_depth_limits_in_flight_sends() {
        let (clock, mut shard) = make_3node_shard(1);
        shard.config.pipeline_depth = 1;
        elect_leader(&mut shard, &clock);

        // First proposal fills the one available pipeline slot.
        let first_actions = shard.handle_propose(b"first".to_vec()).unwrap();
        let first_sends = sends_to(&first_actions, NodeId(2));
        assert!(!first_sends.is_empty(), "first proposal should be sent");

        // Second proposal must not be sent — pipeline depth is 1 and the first
        // AppendEntries is still in flight (no AppendEntriesResponse delivered).
        let second_actions = shard.handle_propose(b"second".to_vec()).unwrap();
        let second_sends = sends_to(&second_actions, NodeId(2));
        assert!(
            second_sends.is_empty(),
            "pipeline full, no new sends expected for second proposal"
        );
    }

    // ── Commit / lease ─────────────────────────────────────────────

    #[test]
    fn test_quorum_ack_advances_commit_and_renews_lease() {
        let (clock, mut shard) = make_3node_shard(1);
        elect_leader(&mut shard, &clock);

        // No-op committed at index 1 by elect_leader. Data proposal goes to index 2.
        let _ = shard.handle_propose(b"data".to_vec()).unwrap();

        let actions = shard.handle_message(
            NodeId(2),
            Message::AppendEntriesResponse {
                term: shard.current_term(),
                success: true,
                match_index: 2,
            },
        );

        assert_eq!(shard.commit_index(), 2);
        assert!(has_renew_lease(&actions));
    }

    // ── Membership changes ─────────────────────────────────────────

    #[test]
    fn test_membership_add_learner_creates_entry_and_peer_state() {
        let (clock, mut shard) = make_3node_shard(1);
        elect_leader(&mut shard, &clock);

        let actions = shard
            .handle_membership_change(MembershipChange::AddLearner {
                node_id: NodeId(4),
                promotable: true,
                expected_conf_epoch: None,
            })
            .unwrap();

        let last_entry = shard.log.back().unwrap();
        assert!(matches!(last_entry.kind, EntryKind::Membership(_)));

        let persist_count =
            actions.iter().filter(|a| matches!(a, Action::PersistEntries { .. })).count();
        assert_eq!(persist_count, 1);

        let learner_peer = shard.peer_states.iter().find(|p| p.id == NodeId(4));
        assert!(learner_peer.is_some());
        assert!(learner_peer.unwrap().is_learner);
        assert!(learner_peer.unwrap().promotable);
    }

    #[test]
    fn test_membership_change_follower_returns_not_leader() {
        let (_clock, mut shard) = make_3node_shard(1);

        let result = shard.handle_membership_change(MembershipChange::AddLearner {
            node_id: NodeId(4),
            promotable: false,
            expected_conf_epoch: None,
        });

        assert!(matches!(result, Err(ConsensusError::NotLeader)));
    }

    #[test]
    fn test_membership_change_while_pending_returns_error() {
        let (clock, mut shard) = make_3node_shard(1);
        elect_leader(&mut shard, &clock);

        let _actions = shard
            .handle_membership_change(MembershipChange::AddLearner {
                node_id: NodeId(4),
                promotable: false,
                expected_conf_epoch: None,
            })
            .unwrap();

        let result = shard.handle_membership_change(MembershipChange::AddLearner {
            node_id: NodeId(5),
            promotable: false,
            expected_conf_epoch: None,
        });

        assert!(matches!(result, Err(ConsensusError::MembershipChangePending)));
    }

    #[test]
    fn test_membership_change_failed_shard_returns_shard_unavailable() {
        let (_clock, mut shard) = make_3node_shard(1);
        shard.mark_failed();

        let result = shard.handle_membership_change(MembershipChange::AddLearner {
            node_id: NodeId(4),
            promotable: false,
            expected_conf_epoch: None,
        });

        assert!(matches!(result, Err(ConsensusError::ShardUnavailable { .. })));
    }

    #[test]
    fn test_membership_promote_learner_to_voter() {
        let (clock, mut shard) = make_3node_shard(1);
        elect_leader(&mut shard, &clock);

        let _actions = shard
            .handle_membership_change(MembershipChange::AddLearner {
                node_id: NodeId(4),
                promotable: true,
                expected_conf_epoch: None,
            })
            .unwrap();

        // Commit the membership entry.
        let match_idx = shard.log_len();
        shard.handle_message(
            NodeId(2),
            Message::AppendEntriesResponse {
                term: shard.current_term(),
                success: true,
                match_index: match_idx,
            },
        );
        assert!(!shard.pending_membership);
        assert!(shard.membership.is_learner(NodeId(4)));

        let actions = shard
            .handle_membership_change(MembershipChange::PromoteVoter {
                node_id: NodeId(4),
                expected_conf_epoch: None,
            })
            .unwrap();

        let last_entry = shard.log.back().unwrap();
        match &last_entry.kind {
            EntryKind::Membership(m) => {
                assert!(m.is_voter(NodeId(4)));
                assert!(!m.is_learner(NodeId(4)));
            },
            _ => panic!("expected Membership entry"),
        }
        assert!(actions.iter().any(|a| matches!(a, Action::PersistEntries { .. })));
    }

    // ── Install snapshot ───────────────────────────────────────────

    #[test]
    fn test_install_snapshot_valid_resets_election_timer() {
        let (_clock, mut shard) = make_3node_shard(2);
        let initial_deadline = shard.election_deadline;

        let actions = shard.handle_message(
            NodeId(1),
            Message::InstallSnapshot {
                term: 1,
                leader_id: NodeId(1),
                last_included_index: 10,
                last_included_term: 1,
                offset: 0,
                data: vec![1, 2, 3],
                done: true,
            },
        );

        assert_eq!(shard.state(), NodeState::Follower);
        assert_eq!(shard.current_term(), 1);
        assert_ne!(shard.election_deadline, initial_deadline);

        let sends = all_sends(&actions);
        assert!(
            sends.iter().any(|(to, msg)| *to == NodeId(1)
                && matches!(msg, Message::InstallSnapshotResponse { .. }))
        );
    }

    #[test]
    fn test_install_snapshot_stale_term_rejected() {
        let (_clock, mut shard) = make_3node_shard(2);
        {
            let mut actions = Vec::new();
            shard.become_follower(5, &mut actions);
        }

        let actions = shard.handle_message(
            NodeId(1),
            Message::InstallSnapshot {
                term: 3,
                leader_id: NodeId(1),
                last_included_index: 10,
                last_included_term: 3,
                offset: 0,
                data: vec![1, 2, 3],
                done: true,
            },
        );

        let sends = all_sends(&actions);
        match sends[0].1 {
            Message::InstallSnapshotResponse { term } => {
                assert_eq!(*term, 5);
            },
            _ => panic!("expected InstallSnapshotResponse"),
        }
    }

    // ── Snapshot trigger (leader) ───────────────────────────────────

    #[test]
    fn test_lagging_peer_receives_install_snapshot() {
        let (clock, mut shard) = make_3node_shard(1);
        elect_leader(&mut shard, &clock);

        // Simulate log compaction: entries before index 5 have been snapshotted.
        shard.last_snapshot_index = 5;

        // Reset peer next_index to 1 to simulate a lagging peer that needs entries
        // the leader no longer has (they were compacted into the snapshot).
        for ps in shard.peer_states.iter_mut() {
            ps.next_index = 1;
            ps.match_index = 0;
            ps.in_flight = 0;
        }

        // Trigger replication — the leader should send SendSnapshot to lagging peers.
        let actions = shard.send_append_entries_to_all();

        let has_send_snapshot = actions.iter().any(|a| {
            matches!(
                a,
                Action::SendSnapshot { last_included_index, .. }
                if *last_included_index == 5
            )
        });
        assert!(has_send_snapshot, "Expected SendSnapshot for lagging peer; got: {actions:?}");
    }

    #[test]
    fn test_install_snapshot_response_advances_peer_next_index() {
        let (clock, mut shard) = make_3node_shard(1);
        elect_leader(&mut shard, &clock);

        // Simulate log compaction.
        shard.last_snapshot_index = 10;

        // Simulate a peer that is behind the snapshot.
        for ps in shard.peer_states.iter_mut() {
            if ps.id == NodeId(2) {
                ps.next_index = 1;
                ps.match_index = 0;
                ps.in_flight = 1; // outstanding InstallSnapshot
            }
        }

        let current_term = shard.current_term();
        let actions = shard
            .handle_message(NodeId(2), Message::InstallSnapshotResponse { term: current_term });

        // No term change expected.
        assert_eq!(shard.state(), NodeState::Leader);
        assert!(actions.is_empty());

        // Peer's next_index should now be last_snapshot_index + 1.
        let ps = shard.peer_states.iter().find(|p| p.id == NodeId(2)).unwrap();
        assert_eq!(ps.next_index, 11, "next_index should advance past snapshot");
        assert_eq!(ps.match_index, 10, "match_index should be updated to snapshot index");
        assert_eq!(ps.in_flight, 0, "in_flight should be decremented");
    }

    #[test]
    fn test_peer_ahead_of_snapshot_boundary_gets_append_entries() {
        let (clock, mut shard) = make_3node_shard(1);
        elect_leader(&mut shard, &clock);

        // Snapshot at index 3; peer is at next_index=4 (ahead of snapshot).
        shard.last_snapshot_index = 3;
        for ps in shard.peer_states.iter_mut() {
            ps.next_index = 4;
            ps.match_index = 3;
            ps.in_flight = 0;
        }

        let actions = shard.send_append_entries_to_all();

        // Should send AppendEntries (heartbeat), not SendSnapshot.
        let has_send_snapshot = actions.iter().any(|a| matches!(a, Action::SendSnapshot { .. }));
        assert!(!has_send_snapshot, "Peer ahead of snapshot should not receive SendSnapshot");
        let has_append_entries = actions
            .iter()
            .any(|a| matches!(a, Action::Send { msg: Message::AppendEntries { .. }, .. }));
        assert!(has_append_entries, "Peer ahead of snapshot should receive AppendEntries");
    }

    // ── TimeoutNow (leadership transfer) ───────────────────────────

    #[test]
    fn test_timeout_now_skips_pre_vote_and_starts_election() {
        let (clock, mut shard) = make_3node_shard(1);
        clock.advance(std::time::Duration::from_secs(1));

        let actions = shard.handle_message(NodeId(2), Message::TimeoutNow);

        assert_eq!(shard.state(), NodeState::Candidate);
        assert_eq!(shard.current_term(), 1);

        let vote_request_count = all_sends(&actions)
            .iter()
            .filter(|(_, msg)| matches!(msg, Message::VoteRequest { .. }))
            .count();
        assert_eq!(vote_request_count, 2);
    }

    // ── Transfer leader ────────────────────────────────────────────

    #[test]
    fn test_transfer_leader_sends_timeout_now_to_target() {
        let (clock, mut shard) = make_3node_shard(1);
        elect_leader(&mut shard, &clock);

        let actions = shard.handle_transfer_leader(NodeId(2)).unwrap();

        let sends = sends_to(&actions, NodeId(2));
        assert_eq!(sends.len(), 1);
        assert!(matches!(sends[0], Message::TimeoutNow));
    }

    #[test]
    fn test_transfer_leader_not_leader_returns_error() {
        let (_clock, mut shard) = make_3node_shard(1);

        let result = shard.handle_transfer_leader(NodeId(2));

        assert!(matches!(result, Err(ConsensusError::NotLeader)));
    }

    #[test]
    fn test_transfer_leader_to_self_returns_error() {
        let (clock, mut shard) = make_3node_shard(1);
        elect_leader(&mut shard, &clock);

        let result = shard.handle_transfer_leader(NodeId(1));

        assert!(matches!(result, Err(ConsensusError::NotLeader)));
    }

    #[test]
    fn test_transfer_leader_non_voter_target_returns_shard_unavailable() {
        let (clock, mut shard) = make_3node_shard(1);
        elect_leader(&mut shard, &clock);

        let result = shard.handle_transfer_leader(NodeId(99));

        assert!(matches!(result, Err(ConsensusError::ShardUnavailable { .. })));
    }

    #[test]
    fn test_transfer_leader_failed_shard_returns_shard_unavailable() {
        let (_clock, mut shard) = make_3node_shard(1);
        shard.mark_failed();

        let result = shard.handle_transfer_leader(NodeId(2));

        assert!(matches!(result, Err(ConsensusError::ShardUnavailable { .. })));
    }

    // ── Trigger snapshot ───────────────────────────────────────────

    #[test]
    fn test_trigger_snapshot_with_committed_entries_returns_action() {
        let (clock, mut shard) = make_3node_shard(1);
        elect_leader(&mut shard, &clock);

        // Propose and commit an entry.
        let _ = shard.handle_propose(b"data".to_vec()).unwrap();
        shard.handle_message(
            NodeId(2),
            Message::AppendEntriesResponse {
                term: shard.current_term(),
                success: true,
                match_index: 1,
            },
        );
        assert_eq!(shard.commit_index(), 1);

        let actions = shard.handle_trigger_snapshot();

        let snapshot = actions.iter().find(|a| matches!(a, Action::TriggerSnapshot { .. }));
        assert!(snapshot.is_some(), "expected TriggerSnapshot action");
    }

    #[test]
    fn test_trigger_snapshot_zero_commit_returns_empty() {
        let (_clock, shard) = make_3node_shard(1);

        let actions = shard.handle_trigger_snapshot();

        assert!(actions.is_empty());
    }

    #[test]
    fn test_trigger_snapshot_failed_shard_returns_empty() {
        let (_clock, mut shard) = make_3node_shard(1);
        shard.mark_failed();

        let actions = shard.handle_trigger_snapshot();

        assert!(actions.is_empty());
    }

    #[test]
    fn test_maybe_trigger_snapshot_does_not_advance_last_snapshot_index() {
        let clock = make_clock();
        let config = ShardConfig { snapshot_threshold: 3, ..ShardConfig::default() };
        let mut shard = Shard::new(
            ShardId(1),
            NodeId(1),
            make_membership_3(),
            config,
            clock.clone(),
            make_rng(),
            0,
            None,
            0,
        );
        elect_leader(&mut shard, &clock);

        // Propose and commit 3 entries to cross the snapshot threshold.
        for _ in 0..3 {
            let _ = shard.handle_propose(b"data".to_vec()).unwrap();
        }
        let match_index = shard.log_len();
        let term = shard.current_term();
        let actions = shard.handle_message(
            NodeId(2),
            Message::AppendEntriesResponse { term, success: true, match_index },
        );

        // A TriggerSnapshot action should have been emitted.
        let has_snapshot = actions.iter().any(|a| matches!(a, Action::TriggerSnapshot { .. }));
        assert!(has_snapshot, "expected TriggerSnapshot action after threshold crossed");

        // But last_snapshot_index must NOT have advanced yet.
        assert_eq!(
            shard.last_snapshot_index, 0,
            "last_snapshot_index should not advance on trigger"
        );
    }

    #[test]
    fn test_handle_snapshot_completed_advances_last_snapshot_index() {
        let clock = make_clock();
        let config = ShardConfig { snapshot_threshold: 3, ..ShardConfig::default() };
        let mut shard = Shard::new(
            ShardId(1),
            NodeId(1),
            make_membership_3(),
            config,
            clock.clone(),
            make_rng(),
            0,
            None,
            0,
        );
        elect_leader(&mut shard, &clock);

        // Propose and commit 3 entries.
        for _ in 0..3 {
            let _ = shard.handle_propose(b"data".to_vec()).unwrap();
        }
        let match_index = shard.log_len();
        let term = shard.current_term();
        let _ = shard.handle_message(
            NodeId(2),
            Message::AppendEntriesResponse { term, success: true, match_index },
        );

        assert_eq!(shard.last_snapshot_index, 0);

        // Simulate the reactor confirming the snapshot completed.
        shard.handle_snapshot_completed(match_index);
        assert_eq!(
            shard.last_snapshot_index, match_index,
            "last_snapshot_index should advance after completion"
        );
    }

    #[test]
    fn test_handle_snapshot_completed_ignores_stale_index() {
        let (_clock, mut shard) = make_3node_shard(1);

        // Manually set a snapshot index.
        shard.last_snapshot_index = 10;

        // A stale completion (lower index) should be ignored.
        shard.handle_snapshot_completed(5);
        assert_eq!(shard.last_snapshot_index, 10, "stale snapshot completion should be ignored");

        // Equal index should also be a no-op (not strictly stale, but not an advance).
        shard.handle_snapshot_completed(10);
        assert_eq!(shard.last_snapshot_index, 10);
    }

    #[test]
    fn test_snapshot_in_flight_suppresses_duplicate_triggers() {
        let clock = make_clock();
        let config = ShardConfig { snapshot_threshold: 2, ..ShardConfig::default() };
        let mut shard = Shard::new(
            ShardId(1),
            NodeId(1),
            make_membership_3(),
            config,
            clock.clone(),
            make_rng(),
            0,
            None,
            0,
        );
        elect_leader(&mut shard, &clock);

        // Propose and commit enough entries to cross the threshold.
        for _ in 0..2 {
            let _ = shard.handle_propose(b"data".to_vec()).unwrap();
        }
        let match_index = shard.log_len();
        let term = shard.current_term();
        let actions = shard.handle_message(
            NodeId(2),
            Message::AppendEntriesResponse { term, success: true, match_index },
        );

        let trigger_count =
            actions.iter().filter(|a| matches!(a, Action::TriggerSnapshot { .. })).count();
        assert_eq!(
            trigger_count, 1,
            "first commit past threshold should trigger exactly one snapshot"
        );
        assert!(shard.snapshot_in_flight, "snapshot_in_flight should be set");

        // Propose and commit more entries — no new trigger while in-flight.
        for _ in 0..3 {
            let _ = shard.handle_propose(b"data".to_vec()).unwrap();
        }
        let match_index2 = shard.log_len();
        let actions2 = shard.handle_message(
            NodeId(2),
            Message::AppendEntriesResponse { term, success: true, match_index: match_index2 },
        );
        let trigger_count2 =
            actions2.iter().filter(|a| matches!(a, Action::TriggerSnapshot { .. })).count();
        assert_eq!(trigger_count2, 0, "no snapshot trigger while in-flight");

        // Complete the snapshot — clears in-flight flag.
        shard.handle_snapshot_completed(match_index);
        assert!(!shard.snapshot_in_flight, "snapshot_in_flight should be cleared");

        // Now a new trigger should fire on the next commit.
        let _ = shard.handle_propose(b"data".to_vec()).unwrap();
        let match_index3 = shard.log_len();
        let actions3 = shard.handle_message(
            NodeId(2),
            Message::AppendEntriesResponse { term, success: true, match_index: match_index3 },
        );
        let trigger_count3 =
            actions3.iter().filter(|a| matches!(a, Action::TriggerSnapshot { .. })).count();
        assert_eq!(trigger_count3, 1, "new trigger should fire after completion");
    }

    // ── Auto-promotion ─────────────────────────────────────────────

    #[test]
    fn test_auto_promote_caught_up_learner_emits_membership_changed() {
        let (clock, mut shard) = make_3node_shard(1);
        elect_leader(&mut shard, &clock);

        let _actions = shard
            .handle_membership_change(MembershipChange::AddLearner {
                node_id: NodeId(4),
                promotable: true,
                expected_conf_epoch: None,
            })
            .unwrap();

        // Commit the AddLearner membership entry.
        let match_idx = shard.log_len();
        shard.handle_message(
            NodeId(2),
            Message::AppendEntriesResponse {
                term: shard.current_term(),
                success: true,
                match_index: match_idx,
            },
        );
        assert!(!shard.pending_membership);
        assert!(shard.membership.is_learner(NodeId(4)));

        // Set the learner's match_index to the log tip.
        let log_tip = shard.log_len();
        if let Some(ps) = shard.peer_states.iter_mut().find(|p| p.id == NodeId(4)) {
            ps.match_index = log_tip;
        }

        let actions = shard.handle_message(
            NodeId(2),
            Message::AppendEntriesResponse {
                term: shard.current_term(),
                success: true,
                match_index: log_tip,
            },
        );

        let promotion = actions.iter().find_map(|a| match a {
            Action::MembershipChanged { membership, .. } => Some(membership),
            _ => None,
        });
        assert!(promotion.is_some(), "expected MembershipChanged action for auto-promotion");
        let new_membership = promotion.unwrap();
        assert!(new_membership.is_voter(NodeId(4)));
        assert!(!new_membership.is_learner(NodeId(4)));
    }

    #[test]
    fn test_auto_promote_suppressed_when_learner_lagging() {
        let (clock, mut shard) = make_3node_shard(1);
        elect_leader(&mut shard, &clock);

        let _actions = shard
            .handle_membership_change(MembershipChange::AddLearner {
                node_id: NodeId(4),
                promotable: true,
                expected_conf_epoch: None,
            })
            .unwrap();

        let match_idx = shard.log_len();
        shard.handle_message(
            NodeId(2),
            Message::AppendEntriesResponse {
                term: shard.current_term(),
                success: true,
                match_index: match_idx,
            },
        );
        assert!(shard.membership.is_learner(NodeId(4)));

        let _actions = shard.handle_propose(vec![1, 2, 3]);
        let log_tip = shard.log_len();

        if let Some(ps) = shard.peer_states.iter_mut().find(|p| p.id == NodeId(4)) {
            ps.match_index = 0;
        }
        // Lower threshold so the learner is genuinely lagging.
        shard.config.auto_promote_threshold = 0;

        let actions = shard.handle_message(
            NodeId(2),
            Message::AppendEntriesResponse {
                term: shard.current_term(),
                success: true,
                match_index: log_tip,
            },
        );

        let promotion = actions.iter().find(|a| matches!(a, Action::MembershipChanged { .. }));
        assert!(promotion.is_none(), "should not auto-promote a lagging learner");
    }

    #[test]
    fn test_auto_promote_suppressed_when_pending_membership() {
        let (clock, mut shard) = make_3node_shard(1);
        elect_leader(&mut shard, &clock);

        let _actions = shard
            .handle_membership_change(MembershipChange::AddLearner {
                node_id: NodeId(4),
                promotable: true,
                expected_conf_epoch: None,
            })
            .unwrap();
        let match_idx = shard.log_len();
        shard.handle_message(
            NodeId(2),
            Message::AppendEntriesResponse {
                term: shard.current_term(),
                success: true,
                match_index: match_idx,
            },
        );
        assert!(!shard.pending_membership);

        let log_tip = shard.log_len();
        if let Some(ps) = shard.peer_states.iter_mut().find(|p| p.id == NodeId(4)) {
            ps.match_index = log_tip;
        }

        shard.pending_membership = true;

        let mut actions = Vec::new();
        shard.try_advance_commit(&mut actions);

        let auto_promoted = actions.iter().any(|a| matches!(a, Action::MembershipChanged { .. }));
        assert!(!auto_promoted, "pending membership should suppress auto-promotion");
    }

    // ── log_entries / truncate_log_before ───────────────────────────

    fn push_entry(shard: &mut Shard<Arc<SimulatedClock>, SimulatedRng>, index: u64) {
        shard.log.push_back(Entry {
            term: 1,
            index,
            data: Arc::from(vec![index as u8]),
            kind: EntryKind::Normal,
        });
    }

    #[test]
    fn test_log_entries_returns_correct_inclusive_range() {
        let (_, mut shard) = make_3node_shard(1);

        for i in 1..=5 {
            push_entry(&mut shard, i);
        }

        let entries = shard.log_entries(2, 4);

        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].index, 2);
        assert_eq!(entries[1].index, 3);
        assert_eq!(entries[2].index, 4);
    }

    #[test]
    fn test_log_entries_empty_log_returns_empty() {
        let (_, shard) = make_3node_shard(1);

        assert!(shard.log_entries(1, 3).is_empty());
    }

    #[test]
    fn test_log_entries_beyond_range_returns_empty() {
        let (_, mut shard) = make_3node_shard(1);
        for i in 1..=3 {
            push_entry(&mut shard, i);
        }

        assert!(shard.log_entries(10, 20).is_empty());
    }

    #[test]
    fn test_log_entries_inverted_range_returns_empty() {
        let (_, mut shard) = make_3node_shard(1);
        for i in 1..=3 {
            push_entry(&mut shard, i);
        }

        assert!(shard.log_entries(3, 1).is_empty());
    }

    #[test]
    fn test_log_entries_after_truncation_uses_index_arithmetic() {
        let (_, mut shard) = make_3node_shard(1);

        for i in 1..=10 {
            push_entry(&mut shard, i);
        }

        shard.truncate_log_before(5);
        assert_eq!(shard.log.front().unwrap().index, 5);

        let entries = shard.log_entries(6, 8);

        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].index, 6);
        assert_eq!(entries[2].index, 8);
    }

    #[test]
    fn test_truncate_log_before_removes_earlier_entries() {
        let (_, mut shard) = make_3node_shard(1);

        for i in 1..=5 {
            push_entry(&mut shard, i);
        }

        shard.truncate_log_before(3);

        assert_eq!(shard.log.len(), 3);
        assert_eq!(shard.log.front().unwrap().index, 3);
        assert_eq!(shard.log.back().unwrap().index, 5);
    }

    #[test]
    fn test_truncate_log_before_index_zero_is_noop() {
        let (_, mut shard) = make_3node_shard(1);
        for i in 1..=3 {
            push_entry(&mut shard, i);
        }

        shard.truncate_log_before(0);

        assert_eq!(shard.log.len(), 3);
    }

    #[test]
    fn test_truncate_log_before_beyond_end_clears_log() {
        let (_, mut shard) = make_3node_shard(1);
        for i in 1..=3 {
            push_entry(&mut shard, i);
        }

        shard.truncate_log_before(100);

        assert!(shard.log.is_empty());
    }

    // ── handle_message on failed shard ─────────────────────────────

    #[test]
    fn test_handle_message_failed_shard_returns_empty() {
        let (_clock, mut shard) = make_3node_shard(1);
        shard.mark_failed();

        let actions = shard.handle_message(
            NodeId(2),
            Message::AppendEntries {
                term: 1,
                leader_id: NodeId(2),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: Arc::from(Vec::new()),
                leader_commit: 0,
                closed_ts_nanos: 0,
            },
        );

        assert!(actions.is_empty());
    }

    // ── ShardRemoved emission on final membership only ─────────────

    #[test]
    fn test_catchup_through_historical_configs_does_not_emit_spurious_shard_removed() {
        // Regression test for the late-joiner shutdown cycle. A node whose
        // shard is initialized with a membership that includes itself
        // (`was_ever_member = true`) must not be marked Shutdown while
        // replaying a history of intermediate configurations — even if some
        // of those configurations transiently exclude the node — so long as
        // the *final* membership after the batch includes it.
        //
        // Before the fix, each intermediate membership entry in the catch-up
        // batch that excluded Node 3 emitted its own Action::ShardRemoved.
        // The reactor then marked the shard Shutdown and scheduled a 30s
        // cleanup timer, blocking replication until the timer expired —
        // even though the final entry in the same batch re-added the node.
        let clock = make_clock();
        // Shard is created with all three nodes in the initial membership,
        // so `was_ever_member` starts true on Node 3.
        let mut shard = make_shard(NodeId(3), clock.clone(), make_rng(), make_membership_3());

        // Catch-up history replayed from the leader. The intermediate
        // configs exclude Node 3; the final config re-adds it as a voter.
        let entries: Arc<[Entry]> = Arc::from(vec![
            Entry {
                term: 1,
                index: 1,
                data: Arc::from(Vec::<u8>::new()),
                kind: EntryKind::Membership(Membership::new([NodeId(1)])),
            },
            Entry {
                term: 1,
                index: 2,
                data: Arc::from(Vec::<u8>::new()),
                kind: EntryKind::Membership(Membership::new([NodeId(1), NodeId(2)])),
            },
            Entry {
                term: 1,
                index: 3,
                data: Arc::from(Vec::<u8>::new()),
                kind: EntryKind::Membership({
                    let mut m = Membership::new([NodeId(1), NodeId(2)]);
                    m.add_learner(NodeId(3));
                    m
                }),
            },
            Entry {
                term: 1,
                index: 4,
                data: Arc::from(Vec::<u8>::new()),
                kind: EntryKind::Membership(Membership::new([NodeId(1), NodeId(2), NodeId(3)])),
            },
        ]);

        let actions = shard.handle_message(
            NodeId(1),
            Message::AppendEntries {
                term: 1,
                leader_id: NodeId(1),
                prev_log_index: 0,
                prev_log_term: 0,
                entries,
                leader_commit: 4,
                closed_ts_nanos: 0,
            },
        );

        // Log + commit index advanced as expected.
        assert_eq!(shard.commit_index(), 4);
        assert_eq!(shard.log_len(), 4);

        // Final membership includes Node 3 as voter.
        assert!(shard.membership().is_voter(NodeId(3)));

        // No spurious ShardRemoved, even though intermediate configs
        // excluded Node 3.
        let shard_removed_count =
            actions.iter().filter(|a| matches!(a, Action::ShardRemoved { .. })).count();
        assert_eq!(
            shard_removed_count, 0,
            "catch-up through intermediate configs must not emit ShardRemoved when \
             the final membership includes the local node"
        );
    }

    #[test]
    fn test_genuine_removal_still_emits_shard_removed() {
        // Counterpart to the above: when the batch's *final* membership
        // actually excludes the local node, ShardRemoved must still fire.
        let clock = make_clock();
        let mut shard = make_shard(NodeId(3), clock.clone(), make_rng(), make_membership_3());

        let entries: Arc<[Entry]> = Arc::from(vec![Entry {
            term: 1,
            index: 1,
            data: Arc::from(Vec::<u8>::new()),
            kind: EntryKind::Membership(Membership::new([NodeId(1), NodeId(2)])),
        }]);

        let actions = shard.handle_message(
            NodeId(1),
            Message::AppendEntries {
                term: 1,
                leader_id: NodeId(1),
                prev_log_index: 0,
                prev_log_term: 0,
                entries,
                leader_commit: 1,
                closed_ts_nanos: 0,
            },
        );

        assert!(!shard.membership().is_voter(NodeId(3)));
        assert!(!shard.membership().is_learner(NodeId(3)));
        let shard_removed_count =
            actions.iter().filter(|a| matches!(a, Action::ShardRemoved { .. })).count();
        assert_eq!(
            shard_removed_count, 1,
            "genuine removal where the final membership excludes the local node \
             must still emit exactly one ShardRemoved action"
        );
    }

    // ── Self-removal guard ────────────────────────────────────────

    #[test]
    fn test_shard_remove_self_succeeds() {
        let (clock, mut shard) = make_3node_shard(1);
        elect_leader(&mut shard, &clock);

        // Self-removal is allowed — the leader commits the change then steps down.
        let result = shard.handle_membership_change(MembershipChange::RemoveNode {
            node_id: NodeId(1),
            expected_conf_epoch: None,
        });
        assert!(result.is_ok(), "leader should be able to remove itself");

        // Removing a different node should also succeed.
        let (clock2, mut shard2) = make_3node_shard(1);
        elect_leader(&mut shard2, &clock2);
        let result = shard2.handle_membership_change(MembershipChange::RemoveNode {
            node_id: NodeId(2),
            expected_conf_epoch: None,
        });
        assert!(result.is_ok(), "removing a different node should succeed");
    }

    // ── Pre-vote rejection with recent heartbeat ──────────────────

    #[test]
    fn test_pre_vote_rejected_when_election_deadline_not_expired() {
        // Node 2 is a follower in a 3-node cluster.
        let (clock, mut follower) = make_3node_shard(2);

        // Receive AppendEntries from leader (node 1) to renew election deadline.
        follower.handle_message(
            NodeId(1),
            Message::AppendEntries {
                term: 1,
                leader_id: NodeId(1),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: Arc::from(Vec::new()),
                leader_commit: 0,
                closed_ts_nanos: 0,
            },
        );

        // Immediately send a PreVoteRequest from node 3.
        // The follower's election deadline has NOT expired, so it should reject.
        let actions = follower.handle_message(
            NodeId(3),
            Message::PreVoteRequest {
                term: 2,
                candidate_id: NodeId(3),
                last_log_index: 0,
                last_log_term: 0,
            },
        );

        let response = sends_to(&actions, NodeId(3));
        assert_eq!(response.len(), 1);
        match response[0] {
            Message::PreVoteResponse { vote_granted, .. } => {
                assert!(
                    !vote_granted,
                    "should reject pre-vote when election deadline has not expired"
                );
            },
            _ => unreachable!("expected PreVoteResponse"),
        }

        // Advance clock past the election timeout (max is 600ms).
        clock.advance(std::time::Duration::from_secs(1));

        // Resend the same PreVoteRequest — now it should be granted.
        let actions = follower.handle_message(
            NodeId(3),
            Message::PreVoteRequest {
                term: 2,
                candidate_id: NodeId(3),
                last_log_index: 0,
                last_log_term: 0,
            },
        );

        let response = sends_to(&actions, NodeId(3));
        assert_eq!(response.len(), 1);
        match response[0] {
            Message::PreVoteResponse { vote_granted, .. } => {
                assert!(vote_granted, "should grant pre-vote after election deadline has expired");
            },
            _ => unreachable!("expected PreVoteResponse"),
        }
    }

    // ── Log conflict resolution ───────────────────────────────────

    #[test]
    fn test_log_conflict_resolution_truncates_and_accepts_leader_entries() {
        let (_clock, mut follower) = make_3node_shard(2);

        // Simulate the follower having stale entries at term 1.
        // Entries at indices 1, 2, 3 with term 1.
        for i in 1..=3 {
            follower.log.push_back(Entry {
                term: 1,
                index: i,
                data: Arc::from(vec![i as u8]),
                kind: EntryKind::Normal,
            });
        }
        assert_eq!(follower.log_len(), 3);

        // Leader sends entries at indices 2 and 3 but with term 2 (conflict at index 2).
        // prev_log_index=1, prev_log_term=1 — matches the follower's entry 1.
        let leader_entries: Arc<[Entry]> = Arc::from(vec![
            Entry { term: 2, index: 2, data: Arc::from(vec![20]), kind: EntryKind::Normal },
            Entry { term: 2, index: 3, data: Arc::from(vec![30]), kind: EntryKind::Normal },
        ]);

        let actions = follower.handle_message(
            NodeId(1),
            Message::AppendEntries {
                term: 2,
                leader_id: NodeId(1),
                prev_log_index: 1,
                prev_log_term: 1,
                entries: leader_entries,
                leader_commit: 0,
                closed_ts_nanos: 0,
            },
        );

        // Follower should have 3 entries: original index 1 (term 1), plus leader's
        // index 2 and 3 (term 2). The stale entries at indices 2-3 should be replaced.
        assert_eq!(follower.log_len(), 3);
        assert_eq!(follower.log[0].term, 1); // index 1, original
        assert_eq!(follower.log[0].index, 1);
        assert_eq!(follower.log[1].term, 2); // index 2, from leader
        assert_eq!(follower.log[1].index, 2);
        assert_eq!(&*follower.log[1].data, &[20]);
        assert_eq!(follower.log[2].term, 2); // index 3, from leader
        assert_eq!(follower.log[2].index, 3);
        assert_eq!(&*follower.log[2].data, &[30]);

        // The response should indicate success.
        let response = sends_to(&actions, NodeId(1));
        assert_eq!(response.len(), 1);
        match response[0] {
            Message::AppendEntriesResponse { success, match_index, .. } => {
                assert!(success, "follower should accept after resolving conflict");
                assert_eq!(*match_index, 3);
            },
            _ => unreachable!("expected AppendEntriesResponse"),
        }

        // New entries should have been persisted.
        let persist_count =
            actions.iter().filter(|a| matches!(a, Action::PersistEntries { .. })).count();
        assert_eq!(persist_count, 1, "conflicting entries should be persisted");
    }

    #[test]
    fn test_log_conflict_at_prev_log_triggers_truncation_and_nack() {
        let (_clock, mut follower) = make_3node_shard(2);

        // Follower has entries at indices 1-3, all at term 1.
        for i in 1..=3 {
            follower.log.push_back(Entry {
                term: 1,
                index: i,
                data: Arc::from(vec![i as u8]),
                kind: EntryKind::Normal,
            });
        }

        // Leader sends with prev_log_index=2, prev_log_term=5 — mismatch at index 2.
        // The follower's entry 2 has term 1, not term 5. This triggers a truncation
        // of the conflicting suffix and a NACK.
        let actions = follower.handle_message(
            NodeId(1),
            Message::AppendEntries {
                term: 5,
                leader_id: NodeId(1),
                prev_log_index: 2,
                prev_log_term: 5,
                entries: Arc::from(Vec::new()),
                leader_commit: 0,
                closed_ts_nanos: 0,
            },
        );

        // Log should be truncated to index 1 (everything from index 2 onward removed).
        assert_eq!(follower.log_len(), 1);
        assert_eq!(follower.log[0].index, 1);

        // Response should be a NACK with match_index=1.
        let response = sends_to(&actions, NodeId(1));
        assert_eq!(response.len(), 1);
        match response[0] {
            Message::AppendEntriesResponse { success, match_index, .. } => {
                assert!(!success, "should NACK due to prev_log mismatch");
                assert_eq!(*match_index, 1);
            },
            _ => unreachable!("expected AppendEntriesResponse"),
        }
    }

    // ── Term + votedFor persistence (Raft Figure 2) ────────────────

    #[test]
    fn term_survives_simulated_restart() {
        let clock = make_clock();
        let (_, mut shard) = make_3node_shard(1);

        // Advance to term 10 by becoming follower at that term.
        let mut actions = Vec::new();
        shard.become_follower(10, &mut actions);
        assert_eq!(shard.current_term(), 10);

        // Extract PersistTermState from actions.
        let persist = actions.iter().find(|a| matches!(a, Action::PersistTermState { .. }));
        assert!(persist.is_some(), "become_follower must emit PersistTermState");
        let (persisted_term, persisted_voted_for) = match persist {
            Some(Action::PersistTermState { term, voted_for, .. }) => (*term, *voted_for),
            _ => unreachable!(),
        };
        assert_eq!(persisted_term, 10);
        assert_eq!(persisted_voted_for, None);

        // Simulate restart: create a NEW shard with the persisted values.
        let restarted = make_shard(NodeId(1), clock.clone(), make_rng(), make_membership_3());
        // Default shard starts at term 0 — this is the bug we're fixing.
        assert_eq!(restarted.current_term(), 0);

        // Now create with recovered state.
        let recovered = Shard::new(
            ShardId(1),
            NodeId(1),
            make_membership_3(),
            ShardConfig::default(),
            clock,
            make_rng(),
            persisted_term,
            persisted_voted_for,
            0,
        );
        assert_eq!(recovered.current_term(), 10);
        assert_eq!(recovered.voted_for(), None);
    }

    #[test]
    fn voted_for_survives_simulated_restart() {
        let clock = make_clock();
        let (_, mut shard) = make_3node_shard(1);

        // Advance clock so election timeout can fire.
        clock.advance(std::time::Duration::from_secs(1));
        let _actions = shard.handle_election_timeout();
        assert_eq!(shard.state(), NodeState::PreCandidate);

        // Grant pre-votes to become Candidate.
        let prospective_term = shard.current_term() + 1;
        let _ = shard.handle_message(
            NodeId(2),
            Message::PreVoteResponse { term: prospective_term, vote_granted: true },
        );
        assert_eq!(shard.state(), NodeState::Candidate);

        // Now the shard has voted for itself at term 1.
        assert_eq!(shard.current_term(), 1);
        assert_eq!(shard.voted_for(), Some(NodeId(1)));

        // Simulate restart with recovered state.
        let mut recovered = Shard::new(
            ShardId(1),
            NodeId(1),
            make_membership_3(),
            ShardConfig::default(),
            clock.clone(),
            make_rng(),
            shard.current_term(),
            shard.voted_for(),
            0,
        );
        assert_eq!(recovered.current_term(), 1);
        assert_eq!(recovered.voted_for(), Some(NodeId(1)));

        // The recovered shard must reject a vote request from NodeId(3)
        // at the same term (already voted for NodeId(1)).
        let actions = recovered.handle_message(
            NodeId(3),
            Message::VoteRequest {
                term: 1,
                candidate_id: NodeId(3),
                last_log_index: 0,
                last_log_term: 0,
            },
        );
        let vote_granted = actions
            .iter()
            .find_map(|a| match a {
                Action::Send { msg: Message::VoteResponse { vote_granted, .. }, .. } => {
                    Some(*vote_granted)
                },
                _ => None,
            })
            .expect("should produce VoteResponse");
        assert!(!vote_granted, "must reject vote — already voted for NodeId(1) at term 1");
    }

    /// Helper: verify a vote-granted action produces PersistTermState.
    #[test]
    fn vote_grant_emits_persist_term_state() {
        let (_clock, mut shard) = make_3node_shard(1);

        // Receive a VoteRequest from NodeId(2) at term 1.
        let actions = shard.handle_message(
            NodeId(2),
            Message::VoteRequest {
                term: 1,
                candidate_id: NodeId(2),
                last_log_index: 0,
                last_log_term: 0,
            },
        );

        // Verify PersistTermState was emitted. There may be two: one from
        // become_follower (term advance) and one from the vote grant itself.
        // The LAST one carries the final voted_for state.
        let persist_actions: Vec<_> =
            actions.iter().filter(|a| matches!(a, Action::PersistTermState { .. })).collect();
        assert!(!persist_actions.is_empty(), "vote grant must emit at least one PersistTermState");
        // The final PersistTermState must carry the vote.
        match persist_actions.last() {
            Some(Action::PersistTermState { term, voted_for, .. }) => {
                assert_eq!(*term, 1);
                assert_eq!(*voted_for, Some(NodeId(2)));
            },
            _ => unreachable!(),
        }

        // Verify PersistTermState appears BEFORE the VoteResponse Send.
        let persist_pos = actions
            .iter()
            .position(|a| matches!(a, Action::PersistTermState { .. }))
            .expect("PersistTermState must be present");
        let send_pos = actions
            .iter()
            .position(|a| matches!(a, Action::Send { msg: Message::VoteResponse { .. }, .. }))
            .expect("VoteResponse must be present");
        assert!(
            persist_pos < send_pos,
            "PersistTermState (pos {persist_pos}) must precede VoteResponse (pos {send_pos})"
        );
    }

    #[test]
    fn start_election_emits_persist_term_state() {
        let clock = make_clock();
        let (_, mut shard) = make_3node_shard(1);

        // Advance clock, trigger election timeout -> pre-vote.
        clock.advance(std::time::Duration::from_secs(1));
        let _ = shard.handle_election_timeout();

        // Grant pre-votes to transition to Candidate (which calls start_election).
        let prospective_term = shard.current_term() + 1;
        let actions = shard.handle_message(
            NodeId(2),
            Message::PreVoteResponse { term: prospective_term, vote_granted: true },
        );

        // start_election was called internally. Verify PersistTermState.
        let persist_actions: Vec<_> =
            actions.iter().filter(|a| matches!(a, Action::PersistTermState { .. })).collect();
        assert!(!persist_actions.is_empty(), "start_election must emit PersistTermState");
        match &persist_actions[0] {
            Action::PersistTermState { term, voted_for, .. } => {
                assert_eq!(*term, 1, "term should be incremented to 1");
                assert_eq!(*voted_for, Some(NodeId(1)), "should vote for self");
            },
            _ => unreachable!(),
        }
    }

    #[test]
    fn become_follower_emits_persist_term_state() {
        let (_, mut shard) = make_3node_shard(1);

        let mut actions = Vec::new();
        shard.become_follower(5, &mut actions);

        let persist = actions.iter().find(|a| matches!(a, Action::PersistTermState { .. }));
        assert!(persist.is_some(), "become_follower must emit PersistTermState");
        match persist {
            Some(Action::PersistTermState { term, voted_for, .. }) => {
                assert_eq!(*term, 5);
                assert_eq!(*voted_for, None, "voted_for reset on term change");
            },
            _ => unreachable!(),
        }
    }

    #[test]
    fn restarted_shard_rejects_duplicate_vote_at_same_term() {
        let clock = make_clock();

        // Create a shard that has already voted for NodeId(2) at term 5.
        let mut shard = Shard::new(
            ShardId(1),
            NodeId(1),
            make_membership_3(),
            ShardConfig::default(),
            clock.clone(),
            make_rng(),
            5,
            Some(NodeId(2)),
            0,
        );

        // NodeId(3) requests a vote at term 5. Should be rejected because
        // we already voted for NodeId(2).
        let actions = shard.handle_message(
            NodeId(3),
            Message::VoteRequest {
                term: 5,
                candidate_id: NodeId(3),
                last_log_index: 0,
                last_log_term: 0,
            },
        );

        let vote_response = actions
            .iter()
            .find_map(|a| match a {
                Action::Send { msg: Message::VoteResponse { vote_granted, .. }, .. } => {
                    Some(*vote_granted)
                },
                _ => None,
            })
            .expect("should produce VoteResponse");
        assert!(!vote_response, "must reject vote — already voted for NodeId(2) at term 5");
    }

    #[test]
    fn restarted_shard_cannot_win_election_at_lower_term() {
        let clock = make_clock();

        // Shard B was at term 5. On restart, if term were lost (reset to 0),
        // it could start an election at term 1 which should be rejected.
        // With the fix, it correctly starts at term 5.
        let mut shard_a = Shard::new(
            ShardId(1),
            NodeId(1),
            make_membership_3(),
            ShardConfig::default(),
            clock.clone(),
            make_rng(),
            5,
            None,
            0,
        );

        // Simulate shard_b starting a vote request at term 3 (stale).
        let actions = shard_a.handle_message(
            NodeId(2),
            Message::VoteRequest {
                term: 3,
                candidate_id: NodeId(2),
                last_log_index: 0,
                last_log_term: 0,
            },
        );

        // shard_a at term 5 must reject the stale term 3 request.
        let vote_response = actions
            .iter()
            .find_map(|a| match a {
                Action::Send { msg: Message::VoteResponse { vote_granted, term }, .. } => {
                    Some((*vote_granted, *term))
                },
                _ => None,
            })
            .expect("should produce VoteResponse");
        assert!(!vote_response.0, "must reject vote from stale term");
        assert_eq!(vote_response.1, 5, "response should carry current term 5");
    }

    #[test]
    fn initial_committed_index_sets_commit_and_last_applied() {
        let clock = make_clock();
        let shard = Shard::new(
            ShardId(1),
            NodeId(1),
            make_membership_3(),
            ShardConfig::default(),
            clock,
            make_rng(),
            5,
            None,
            44,
        );
        assert_eq!(shard.commit_index(), 44);
        assert_eq!(shard.last_applied, 44);
        assert_eq!(shard.current_term(), 5);
    }

    /// A restarted shard with `initial_committed_index > 0` only emits
    /// `Action::Committed` for entries BEYOND that index when processing
    /// AppendEntries from the leader. This prevents the apply worker from
    /// re-applying entries already in the state machine.
    #[test]
    fn recovered_shard_only_commits_beyond_initial_committed_index() {
        let clock = make_clock();
        // Simulate a follower that crashed with committed_index=3.
        let mut follower = Shard::new(
            ShardId(1),
            NodeId(2),
            make_membership_3(),
            ShardConfig::default(),
            clock.clone(),
            make_rng(),
            1,
            None,
            3,
        );

        // The leader sends entries 1-5 (catching the follower up) with
        // leader_commit=5. Entries 1-3 were already committed before the
        // crash; 4-5 are new.
        let entries: Vec<Entry> = (1..=5)
            .map(|i| Entry {
                index: i,
                term: 1,
                data: Arc::from(format!("entry-{i}").into_bytes().as_slice()),
                kind: EntryKind::Normal,
            })
            .collect();

        let actions = follower.handle_message(
            NodeId(1),
            Message::AppendEntries {
                term: 1,
                leader_id: NodeId(1),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: Arc::from(entries.as_slice()),
                leader_commit: 5,
                closed_ts_nanos: 0,
            },
        );

        // The Committed action should commit up to 5 (starting from the
        // previous commit_index of 3).
        let committed = actions.iter().find_map(|a| match a {
            Action::Committed { up_to, .. } => Some(*up_to),
            _ => None,
        });
        assert_eq!(committed, Some(5), "should commit up to 5");
        assert_eq!(follower.commit_index(), 5);

        // The shard's log should contain all 5 entries.
        assert_eq!(follower.log_len(), 5);
    }

    /// When the leader sends entries that conflict with uncommitted entries
    /// beyond the initial_committed_index, the shard truncates and replaces
    /// them with the leader's version.
    #[test]
    fn recovered_shard_replaces_conflicting_uncommitted_entries() {
        let clock = make_clock();
        // Simulate a follower with committed_index=3. It will receive entries
        // 1-3 (matching) and 4-5 (new from leader, possibly different from
        // what was in the WAL before crash).
        let mut follower = Shard::new(
            ShardId(1),
            NodeId(2),
            make_membership_3(),
            ShardConfig::default(),
            clock.clone(),
            make_rng(),
            1,
            None,
            3,
        );

        // First, populate the log with entries 1-5 (simulating what the
        // follower had received before the crash). In practice the log is
        // empty on restart, but if WAL replay populated it, entries 4-5
        // would be stale.
        let initial_entries: Vec<Entry> = (1..=5)
            .map(|i| Entry {
                index: i,
                term: 1,
                data: Arc::from(format!("old-{i}").into_bytes().as_slice()),
                kind: EntryKind::Normal,
            })
            .collect();
        // Manually push entries into the log to simulate WAL replay.
        for entry in &initial_entries {
            follower.log.push_back(entry.clone());
        }

        // Leader sends entries 4-5 at term 2 (different term = conflict).
        let leader_entries: Vec<Entry> = (4..=5)
            .map(|i| Entry {
                index: i,
                term: 2,
                data: Arc::from(format!("leader-{i}").into_bytes().as_slice()),
                kind: EntryKind::Normal,
            })
            .collect();

        let actions = follower.handle_message(
            NodeId(1),
            Message::AppendEntries {
                term: 2,
                leader_id: NodeId(1),
                prev_log_index: 3,
                prev_log_term: 1,
                entries: Arc::from(leader_entries.as_slice()),
                leader_commit: 5,
                closed_ts_nanos: 0,
            },
        );

        // The shard should have truncated entries 4-5 and replaced them.
        assert_eq!(follower.log_len(), 5);

        // Entries 4 and 5 should now have term 2 (leader's version).
        assert_eq!(follower.log[3].term, 2);
        assert_eq!(follower.log[4].term, 2);
        assert_eq!(&*follower.log[3].data, b"leader-4");
        assert_eq!(&*follower.log[4].data, b"leader-5");

        // The committed action should advance to 5.
        let committed = actions.iter().find_map(|a| match a {
            Action::Committed { up_to, .. } => Some(*up_to),
            _ => None,
        });
        assert_eq!(committed, Some(5));
    }
}
