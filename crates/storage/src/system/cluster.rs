//! Cluster membership and Voter/Learner scaling model.
//!
//! Per DESIGN.md lines 1966-1996.

use std::collections::HashMap;

use chrono::{DateTime, Utc};

use ledger_types::NodeId;

use super::types::{NodeInfo, NodeRole};

/// Maximum number of voters in a `_system` Raft group.
///
/// Beyond this limit, additional nodes become Learners.
/// 5 voters provides good fault tolerance (can lose 2 nodes) while
/// keeping election/heartbeat latency low.
pub const MAX_VOTERS: usize = 5;

/// Node's role in the `_system` Raft group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SystemRole {
    /// Voter: Participates in elections.
    Voter,
    /// Learner: Replicates but doesn't vote.
    Learner,
}

/// Cluster membership state for the `_system` namespace.
///
/// Manages the Voter/Learner model where:
/// - Up to 5 nodes are Voters (participate in Raft elections)
/// - Additional nodes are Learners (replicate but don't vote)
/// - When a Voter leaves, the oldest healthy Learner is promoted
#[derive(Debug, Clone)]
pub struct ClusterMembership {
    /// All nodes in the cluster.
    nodes: HashMap<NodeId, NodeInfo>,
}

impl Default for ClusterMembership {
    fn default() -> Self {
        Self::new()
    }
}

impl ClusterMembership {
    /// Create a new empty cluster membership.
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
        }
    }

    /// Create cluster membership from a list of nodes.
    pub fn from_nodes(nodes: Vec<NodeInfo>) -> Self {
        let nodes = nodes.into_iter().map(|n| (n.node_id.clone(), n)).collect();
        Self { nodes }
    }

    /// Get the number of nodes in the cluster.
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Check if the cluster is empty.
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Get a node by ID.
    pub fn get(&self, node_id: &NodeId) -> Option<&NodeInfo> {
        self.nodes.get(node_id)
    }

    /// Get all nodes.
    pub fn nodes(&self) -> impl Iterator<Item = &NodeInfo> {
        self.nodes.values()
    }

    /// Count the number of Voter nodes.
    pub fn voter_count(&self) -> usize {
        self.nodes
            .values()
            .filter(|n| matches!(n.role, NodeRole::Voter))
            .count()
    }

    /// Count the number of Learner nodes.
    pub fn learner_count(&self) -> usize {
        self.nodes
            .values()
            .filter(|n| matches!(n.role, NodeRole::Learner))
            .count()
    }

    /// Get all Voter node IDs.
    pub fn voters(&self) -> Vec<NodeId> {
        self.nodes
            .values()
            .filter(|n| matches!(n.role, NodeRole::Voter))
            .map(|n| n.node_id.clone())
            .collect()
    }

    /// Get all Learner node IDs.
    pub fn learners(&self) -> Vec<NodeId> {
        self.nodes
            .values()
            .filter(|n| matches!(n.role, NodeRole::Learner))
            .map(|n| n.node_id.clone())
            .collect()
    }

    /// Determine the role for a new node joining the cluster.
    ///
    /// If there are fewer than MAX_VOTERS voters, the new node becomes a Voter.
    /// Otherwise, it becomes a Learner.
    pub fn determine_role(&self, _new_node: &NodeId) -> NodeRole {
        if self.voter_count() < MAX_VOTERS {
            NodeRole::Voter
        } else {
            NodeRole::Learner
        }
    }

    /// Add a new node to the cluster.
    ///
    /// The node's role is automatically determined based on current voter count.
    pub fn add_node(&mut self, mut node: NodeInfo) {
        node.role = self.determine_role(&node.node_id);
        self.nodes.insert(node.node_id.clone(), node);
    }

    /// Remove a node from the cluster.
    ///
    /// Returns the removed node if it existed.
    pub fn remove_node(&mut self, node_id: &NodeId) -> Option<NodeInfo> {
        self.nodes.remove(node_id)
    }

    /// Update a node's heartbeat timestamp.
    pub fn update_heartbeat(&mut self, node_id: &NodeId, timestamp: DateTime<Utc>) {
        if let Some(node) = self.nodes.get_mut(node_id) {
            node.last_heartbeat = timestamp;
        }
    }

    /// Promote the oldest healthy Learner to Voter.
    ///
    /// Called when a Voter is removed and we need to maintain voter count.
    /// Returns the promoted node ID if a Learner was promoted.
    ///
    /// "Oldest" means the Learner with the earliest join time, as a heuristic
    /// for stability.
    pub fn promote_learner_if_needed(&mut self) -> Option<NodeId> {
        // Only promote if we're below the voter ceiling
        if self.voter_count() >= MAX_VOTERS {
            return None;
        }

        // Find the oldest healthy Learner (by join time)
        let learner_to_promote = self
            .nodes
            .iter()
            .filter(|(_, n)| matches!(n.role, NodeRole::Learner))
            .min_by_key(|(_, n)| n.joined_at)
            .map(|(id, _)| id.clone());

        if let Some(ref node_id) = learner_to_promote {
            if let Some(node) = self.nodes.get_mut(node_id) {
                node.role = NodeRole::Voter;
            }
        }

        learner_to_promote
    }

    /// Demote the newest Voter to Learner.
    ///
    /// Used when rebalancing the cluster or when a higher-priority node joins.
    /// Returns the demoted node ID if a Voter was demoted.
    pub fn demote_voter_if_needed(&mut self) -> Option<NodeId> {
        // Only demote if we're above the voter ceiling
        if self.voter_count() <= MAX_VOTERS {
            return None;
        }

        // Find the newest Voter (by join time) to demote
        let voter_to_demote = self
            .nodes
            .iter()
            .filter(|(_, n)| matches!(n.role, NodeRole::Voter))
            .max_by_key(|(_, n)| n.joined_at)
            .map(|(id, _)| id.clone());

        if let Some(ref node_id) = voter_to_demote {
            if let Some(node) = self.nodes.get_mut(node_id) {
                node.role = NodeRole::Learner;
            }
        }

        voter_to_demote
    }

    /// Find nodes that haven't sent a heartbeat within the timeout period.
    pub fn find_stale_nodes(&self, timeout: chrono::Duration, now: DateTime<Utc>) -> Vec<NodeId> {
        self.nodes
            .iter()
            .filter(|(_, n)| now.signed_duration_since(n.last_heartbeat) > timeout)
            .map(|(id, _)| id.clone())
            .collect()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use std::net::SocketAddr;

    use super::*;

    fn make_node(id: &str, role: NodeRole, joined_at: DateTime<Utc>) -> NodeInfo {
        NodeInfo {
            node_id: id.to_string(),
            addresses: vec!["127.0.0.1:5000".parse::<SocketAddr>().unwrap()],
            grpc_port: 5001,
            role,
            last_heartbeat: Utc::now(),
            joined_at,
        }
    }

    #[test]
    fn test_empty_cluster() {
        let cluster = ClusterMembership::new();
        assert!(cluster.is_empty());
        assert_eq!(cluster.voter_count(), 0);
        assert_eq!(cluster.learner_count(), 0);
    }

    #[test]
    fn test_determine_role_below_max() {
        let cluster = ClusterMembership::new();
        // First 5 nodes should be Voters
        assert_eq!(
            cluster.determine_role(&"node-1".to_string()),
            NodeRole::Voter
        );
    }

    #[test]
    fn test_determine_role_at_max() {
        let now = Utc::now();
        let nodes: Vec<NodeInfo> = (0..5)
            .map(|i| make_node(&format!("node-{i}"), NodeRole::Voter, now))
            .collect();
        let cluster = ClusterMembership::from_nodes(nodes);

        // 6th node should be Learner
        assert_eq!(cluster.voter_count(), 5);
        assert_eq!(
            cluster.determine_role(&"node-5".to_string()),
            NodeRole::Learner
        );
    }

    #[test]
    fn test_add_node_assigns_role() {
        let mut cluster = ClusterMembership::new();
        let now = Utc::now();

        // Add 6 nodes - first 5 should be Voters, 6th should be Learner
        for i in 0..6 {
            let node = make_node(&format!("node-{i}"), NodeRole::Learner, now);
            cluster.add_node(node);
        }

        assert_eq!(cluster.voter_count(), 5);
        assert_eq!(cluster.learner_count(), 1);
    }

    #[test]
    fn test_promote_learner_on_voter_removal() {
        let mut cluster = ClusterMembership::new();
        let now = Utc::now();

        // Add 6 nodes
        for i in 0..6 {
            let joined = now + chrono::Duration::seconds(i);
            let node = make_node(&format!("node-{i}"), NodeRole::Learner, joined);
            cluster.add_node(node);
        }

        assert_eq!(cluster.voter_count(), 5);
        assert_eq!(cluster.learner_count(), 1);

        // Remove a voter
        cluster.remove_node(&"node-0".to_string());
        assert_eq!(cluster.voter_count(), 4);
        assert_eq!(cluster.learner_count(), 1);

        // Promote learner
        let promoted = cluster.promote_learner_if_needed();
        assert!(promoted.is_some());
        assert_eq!(cluster.voter_count(), 5);
        assert_eq!(cluster.learner_count(), 0);
    }

    #[test]
    fn test_no_promotion_at_max_voters() {
        let now = Utc::now();
        let nodes: Vec<NodeInfo> = (0..5)
            .map(|i| make_node(&format!("node-{i}"), NodeRole::Voter, now))
            .collect();
        let mut cluster = ClusterMembership::from_nodes(nodes);

        // Add a learner
        cluster.add_node(make_node("node-5", NodeRole::Learner, now));

        // Should not promote because we already have 5 voters
        let promoted = cluster.promote_learner_if_needed();
        assert!(promoted.is_none());
    }

    #[test]
    fn test_find_stale_nodes() {
        let mut cluster = ClusterMembership::new();
        let now = Utc::now();
        let stale_time = now - chrono::Duration::seconds(60);

        cluster.add_node(make_node("node-fresh", NodeRole::Voter, now));

        // Add a node with old heartbeat
        let mut stale_node = make_node("node-stale", NodeRole::Voter, now);
        stale_node.last_heartbeat = stale_time;
        cluster.nodes.insert(stale_node.node_id.clone(), stale_node);

        let stale = cluster.find_stale_nodes(chrono::Duration::seconds(30), now);
        assert_eq!(stale.len(), 1);
        assert_eq!(stale[0], "node-stale");
    }
}
