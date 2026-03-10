//! Cluster membership and per-group Voter/Learner scaling model.
//!
//! ## Per-Group Role Tracking
//!
//! A node's voter/learner status is per-Raft-group, not global. The same node
//! can be a Voter in `GLOBAL`, a Learner in `US_EAST_VA`, and not a member of
//! `CN_NORTH_BEIJING`. [`GroupMembership`] tracks this mapping, derived from
//! openraft's per-group `StoredMembership` at runtime.
//!
//! ## Learner Staleness Handling
//!
//! Learners receive replication but may lag during network issues.
//! The design specifies:
//! - `learner_cache_ttl`: Maximum age before learner considers cache stale (default: 5s)
//! - `learner_refresh_interval`: Interval for polling voter freshness (default: 1s)
//!
//! ### Current Implementation
//!
//! Learners maintain cache freshness through two mechanisms:
//!
//! 1. **OpenRaft Built-in Replication**: Learners receive log entries from the leader
//!    automatically, keeping their state reasonably fresh under normal conditions.
//!
//! 2. **Background Refresh Job** (`LearnerRefreshJob`): Learners periodically poll voters via
//!    `GetSystemState` RPC to refresh their cached system state (organization registry, routing
//!    info). This provides an additional layer of freshness beyond Raft replication.
//!
//! Staleness checks can be integrated into read paths if needed.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use inferadb_ledger_types::{NodeId, Region};

use super::types::{NodeInfo, NodeRole};

/// Maximum number of voters per Raft group.
///
/// Beyond this limit, additional nodes become Learners within that group.
/// 5 voters provides good fault tolerance (can lose 2 nodes) while
/// keeping election/heartbeat latency low. Applied independently per group.
pub const MAX_VOTERS: usize = 5;

/// Minimum number of in-region nodes required to form a protected region's
/// Raft group.
///
/// Protected regions (where `requires_residency() == true`) reject group
/// formation if fewer than this many nodes are tagged with the region.
pub const MIN_NODES_PER_PROTECTED_REGION: usize = 3;

/// Node's role in the `_system` Raft group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SystemRole {
    /// Voter: Participates in elections.
    Voter,
    /// Learner: Replicates but doesn't vote.
    Learner,
}

/// Per-Raft-group role assignments.
///
/// Tracks voter/learner status for each node within each region's Raft group.
/// Derived from openraft's per-group `StoredMembership` — not independently
/// persisted. Built in memory on startup by querying each active Raft group's
/// membership.
#[derive(Debug, Clone, Default)]
pub struct GroupMembership {
    /// Per-group role map: `Region → (NodeId → NodeRole)`.
    groups: HashMap<Region, HashMap<NodeId, NodeRole>>,
}

impl GroupMembership {
    /// Creates an empty group membership.
    pub fn new() -> Self {
        Self { groups: HashMap::new() }
    }

    /// Returns the role map for a specific region's group.
    pub fn group(&self, region: Region) -> Option<&HashMap<NodeId, NodeRole>> {
        self.groups.get(&region)
    }

    /// Returns all regions that have group membership entries.
    pub fn regions(&self) -> impl Iterator<Item = Region> + '_ {
        self.groups.keys().copied()
    }

    /// Returns the number of voters in a region's group.
    pub fn voter_count(&self, region: Region) -> usize {
        self.groups
            .get(&region)
            .map(|g| g.values().filter(|r| matches!(r, NodeRole::Voter)).count())
            .unwrap_or(0)
    }

    /// Returns the number of learners in a region's group.
    pub fn learner_count(&self, region: Region) -> usize {
        self.groups
            .get(&region)
            .map(|g| g.values().filter(|r| matches!(r, NodeRole::Learner)).count())
            .unwrap_or(0)
    }

    /// Returns the total member count (voters + learners) in a region's group.
    pub fn member_count(&self, region: Region) -> usize {
        self.groups.get(&region).map(HashMap::len).unwrap_or(0)
    }

    /// Returns all voter node IDs in a region's group.
    pub fn voters(&self, region: Region) -> Vec<NodeId> {
        self.groups
            .get(&region)
            .map(|g| {
                g.iter()
                    .filter(|(_, r)| matches!(r, NodeRole::Voter))
                    .map(|(id, _)| id.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Returns all learner node IDs in a region's group.
    pub fn learners(&self, region: Region) -> Vec<NodeId> {
        self.groups
            .get(&region)
            .map(|g| {
                g.iter()
                    .filter(|(_, r)| matches!(r, NodeRole::Learner))
                    .map(|(id, _)| id.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Returns all member node IDs in a region's group.
    pub fn members(&self, region: Region) -> Vec<NodeId> {
        self.groups.get(&region).map(|g| g.keys().cloned().collect()).unwrap_or_default()
    }

    /// Returns the role of a node in a specific region's group, if it is a member.
    pub fn role_of(&self, region: Region, node_id: &NodeId) -> Option<NodeRole> {
        self.groups.get(&region).and_then(|g| g.get(node_id)).copied()
    }

    /// Checks whether a node is a member of a region's group.
    pub fn is_member(&self, region: Region, node_id: &NodeId) -> bool {
        self.groups.get(&region).is_some_and(|g| g.contains_key(node_id))
    }

    /// Determines the role for a new node joining a region's group.
    ///
    /// If the group has fewer than [`MAX_VOTERS`] voters, the node becomes a
    /// Voter. Otherwise, it becomes a Learner.
    pub fn determine_role(&self, region: Region) -> NodeRole {
        if self.voter_count(region) < MAX_VOTERS { NodeRole::Voter } else { NodeRole::Learner }
    }

    /// Sets a node's role in a region's group.
    ///
    /// Creates the group if it does not exist.
    pub fn set_role(&mut self, region: Region, node_id: NodeId, role: NodeRole) {
        self.groups.entry(region).or_default().insert(node_id, role);
    }

    /// Removes a node from a region's group.
    ///
    /// Returns the removed role, or `None` if the node was not a member.
    pub fn remove_from_group(&mut self, region: Region, node_id: &NodeId) -> Option<NodeRole> {
        let removed = self.groups.get_mut(&region).and_then(|g| g.remove(node_id));
        // Clean up empty groups
        if let Some(g) = self.groups.get(&region)
            && g.is_empty()
        {
            self.groups.remove(&region);
        }
        removed
    }

    /// Removes a node from all groups.
    ///
    /// Returns the regions the node was removed from.
    pub fn remove_from_all_groups(&mut self, node_id: &NodeId) -> Vec<Region> {
        let mut removed_from = Vec::new();
        let regions: Vec<Region> = self.groups.keys().copied().collect();
        for region in regions {
            if self.remove_from_group(region, node_id).is_some() {
                removed_from.push(region);
            }
        }
        removed_from
    }

    /// Promotes the oldest healthy Learner to Voter in a region's group.
    ///
    /// Called when a Voter is removed and we need to maintain voter count.
    /// Requires access to node info (for join time ordering).
    /// Returns the promoted node ID if a Learner was promoted.
    pub fn promote_learner_if_needed(
        &mut self,
        region: Region,
        nodes: &HashMap<NodeId, NodeInfo>,
    ) -> Option<NodeId> {
        if self.voter_count(region) >= MAX_VOTERS {
            return None;
        }

        let group = self.groups.get(&region)?;
        let learner_to_promote = group
            .iter()
            .filter(|(_, r)| matches!(r, NodeRole::Learner))
            .filter_map(|(id, _)| nodes.get(id).map(|n| (id.clone(), n.joined_at)))
            .min_by_key(|(_, joined)| *joined)
            .map(|(id, _)| id);

        if let Some(ref node_id) = learner_to_promote {
            self.set_role(region, node_id.clone(), NodeRole::Voter);
        }

        learner_to_promote
    }

    /// Demotes the newest Voter to Learner in a region's group.
    ///
    /// Used when rebalancing the group or when a higher-priority node joins.
    /// Requires access to node info (for join time ordering).
    /// Returns the demoted node ID if a Voter was demoted.
    pub fn demote_voter_if_needed(
        &mut self,
        region: Region,
        nodes: &HashMap<NodeId, NodeInfo>,
    ) -> Option<NodeId> {
        if self.voter_count(region) <= MAX_VOTERS {
            return None;
        }

        let group = self.groups.get(&region)?;
        let voter_to_demote = group
            .iter()
            .filter(|(_, r)| matches!(r, NodeRole::Voter))
            .filter_map(|(id, _)| nodes.get(id).map(|n| (id.clone(), n.joined_at)))
            .max_by_key(|(_, joined)| *joined)
            .map(|(id, _)| id);

        if let Some(ref node_id) = voter_to_demote {
            self.set_role(region, node_id.clone(), NodeRole::Learner);
        }

        voter_to_demote
    }
}

/// Cluster membership state.
///
/// Manages physical node information (addresses, region, heartbeat) and
/// per-group role assignments. A node's voter/learner status is tracked by
/// [`GroupMembership`], not stored on [`NodeInfo`].
#[derive(Debug, Clone)]
pub struct ClusterMembership {
    /// All nodes in the cluster (physical info only).
    nodes: HashMap<NodeId, NodeInfo>,
    /// Per-group role assignments (voter/learner per Raft group).
    group_roles: GroupMembership,
}

impl Default for ClusterMembership {
    fn default() -> Self {
        Self::new()
    }
}

impl ClusterMembership {
    /// Creates a new empty cluster membership.
    pub fn new() -> Self {
        Self { nodes: HashMap::new(), group_roles: GroupMembership::new() }
    }

    /// Creates cluster membership from a list of nodes.
    ///
    /// Nodes are registered but not assigned to any groups.
    pub fn from_nodes(nodes: Vec<NodeInfo>) -> Self {
        let nodes = nodes.into_iter().map(|n| (n.node_id.clone(), n)).collect();
        Self { nodes, group_roles: GroupMembership::new() }
    }

    /// Returns the number of nodes in the cluster.
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Checks if the cluster is empty.
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Returns a node by ID.
    pub fn get(&self, node_id: &NodeId) -> Option<&NodeInfo> {
        self.nodes.get(node_id)
    }

    /// Returns all nodes.
    pub fn nodes(&self) -> impl Iterator<Item = &NodeInfo> {
        self.nodes.values()
    }

    /// Returns a reference to the per-group role assignments.
    pub fn group_roles(&self) -> &GroupMembership {
        &self.group_roles
    }

    /// Returns a mutable reference to the per-group role assignments.
    pub fn group_roles_mut(&mut self) -> &mut GroupMembership {
        &mut self.group_roles
    }

    /// Adds a new node to the cluster registry.
    ///
    /// The node is registered but not added to any groups. Use
    /// [`add_node_to_group`](Self::add_node_to_group) to assign group
    /// membership.
    pub fn add_node(&mut self, node: NodeInfo) {
        self.nodes.insert(node.node_id.clone(), node);
    }

    /// Adds a node to a region's Raft group with an auto-determined role.
    ///
    /// If the node is not yet registered, it is added to the node registry
    /// first. Returns the assigned role.
    pub fn add_node_to_group(&mut self, node: NodeInfo, region: Region) -> NodeRole {
        let node_id = node.node_id.clone();
        self.nodes.entry(node_id.clone()).or_insert(node);
        let role = self.group_roles.determine_role(region);
        self.group_roles.set_role(region, node_id, role);
        role
    }

    /// Removes a node from the cluster entirely.
    ///
    /// Removes from the node registry and all group memberships.
    /// Returns the removed node if it existed.
    pub fn remove_node(&mut self, node_id: &NodeId) -> Option<NodeInfo> {
        self.group_roles.remove_from_all_groups(node_id);
        self.nodes.remove(node_id)
    }

    /// Updates a node's heartbeat timestamp.
    pub fn update_heartbeat(&mut self, node_id: &NodeId, timestamp: DateTime<Utc>) {
        if let Some(node) = self.nodes.get_mut(node_id) {
            node.last_heartbeat = timestamp;
        }
    }

    /// Promotes the oldest healthy Learner to Voter in a region's group.
    ///
    /// Delegates to [`GroupMembership::promote_learner_if_needed`].
    pub fn promote_learner_if_needed(&mut self, region: Region) -> Option<NodeId> {
        self.group_roles.promote_learner_if_needed(region, &self.nodes)
    }

    /// Demotes the newest Voter to Learner in a region's group.
    ///
    /// Delegates to [`GroupMembership::demote_voter_if_needed`].
    pub fn demote_voter_if_needed(&mut self, region: Region) -> Option<NodeId> {
        self.group_roles.demote_voter_if_needed(region, &self.nodes)
    }

    /// Finds nodes that haven't sent a heartbeat within the timeout period.
    pub fn find_stale_nodes(&self, timeout: chrono::Duration, now: DateTime<Utc>) -> Vec<NodeId> {
        self.nodes
            .iter()
            .filter(|(_, n)| now.signed_duration_since(n.last_heartbeat) > timeout)
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Returns nodes eligible for a region's Raft group.
    ///
    /// - **Non-protected + GLOBAL**: all cluster nodes.
    /// - **Protected**: only nodes tagged with the matching region.
    pub fn eligible_nodes_for_region(&self, region: Region) -> Vec<&NodeInfo> {
        if region.requires_residency() {
            self.nodes.values().filter(|n| n.region == region).collect()
        } else {
            self.nodes.values().collect()
        }
    }

    /// Returns the number of in-region nodes for a given region.
    pub fn in_region_node_count(&self, region: Region) -> usize {
        self.nodes.values().filter(|n| n.region == region).count()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use std::net::SocketAddr;

    use inferadb_ledger_types::Region;

    use super::*;

    fn make_node(id: &str, region: Region, joined_at: DateTime<Utc>) -> NodeInfo {
        NodeInfo {
            node_id: NodeId::new(id),
            addresses: vec!["127.0.0.1:5000".parse::<SocketAddr>().unwrap()],
            grpc_port: 5001,
            region,
            last_heartbeat: Utc::now(),
            joined_at,
        }
    }

    #[test]
    fn test_empty_cluster() {
        let cluster = ClusterMembership::new();
        assert!(cluster.is_empty());
        assert_eq!(cluster.group_roles().voter_count(Region::GLOBAL), 0);
        assert_eq!(cluster.group_roles().learner_count(Region::GLOBAL), 0);
    }

    #[test]
    fn test_determine_role_below_max() {
        let groups = GroupMembership::new();
        assert_eq!(groups.determine_role(Region::GLOBAL), NodeRole::Voter);
    }

    #[test]
    fn test_determine_role_at_max() {
        let mut groups = GroupMembership::new();
        for i in 0..5 {
            groups.set_role(Region::GLOBAL, NodeId::new(format!("node-{i}")), NodeRole::Voter);
        }
        assert_eq!(groups.voter_count(Region::GLOBAL), 5);
        assert_eq!(groups.determine_role(Region::GLOBAL), NodeRole::Learner);
    }

    #[test]
    fn test_add_node_to_group_assigns_role() {
        let mut cluster = ClusterMembership::new();
        let now = Utc::now();

        for i in 0..6 {
            let node = make_node(&format!("node-{i}"), Region::GLOBAL, now);
            cluster.add_node_to_group(node, Region::GLOBAL);
        }

        assert_eq!(cluster.group_roles().voter_count(Region::GLOBAL), 5);
        assert_eq!(cluster.group_roles().learner_count(Region::GLOBAL), 1);
    }

    #[test]
    fn test_promote_learner_on_voter_removal() {
        let mut cluster = ClusterMembership::new();
        let now = Utc::now();

        for i in 0..6 {
            let joined = now + chrono::Duration::seconds(i);
            let node = make_node(&format!("node-{i}"), Region::GLOBAL, joined);
            cluster.add_node_to_group(node, Region::GLOBAL);
        }

        assert_eq!(cluster.group_roles().voter_count(Region::GLOBAL), 5);
        assert_eq!(cluster.group_roles().learner_count(Region::GLOBAL), 1);

        // Remove a voter from the group
        cluster.group_roles_mut().remove_from_group(Region::GLOBAL, &NodeId::new("node-0"));
        cluster.nodes.remove(&NodeId::new("node-0"));
        assert_eq!(cluster.group_roles().voter_count(Region::GLOBAL), 4);
        assert_eq!(cluster.group_roles().learner_count(Region::GLOBAL), 1);

        // Promote learner
        let promoted = cluster.promote_learner_if_needed(Region::GLOBAL);
        assert!(promoted.is_some());
        assert_eq!(cluster.group_roles().voter_count(Region::GLOBAL), 5);
        assert_eq!(cluster.group_roles().learner_count(Region::GLOBAL), 0);
    }

    #[test]
    fn test_no_promotion_at_max_voters() {
        let mut cluster = ClusterMembership::new();
        let now = Utc::now();

        for i in 0..5 {
            let node = make_node(&format!("node-{i}"), Region::GLOBAL, now);
            cluster.add_node_to_group(node, Region::GLOBAL);
        }
        // Add a 6th as learner
        let node = make_node("node-5", Region::GLOBAL, now);
        cluster.add_node_to_group(node, Region::GLOBAL);

        // Should not promote because we already have 5 voters
        let promoted = cluster.promote_learner_if_needed(Region::GLOBAL);
        assert!(promoted.is_none());
    }

    #[test]
    fn test_find_stale_nodes() {
        let mut cluster = ClusterMembership::new();
        let now = Utc::now();
        let stale_time = now - chrono::Duration::seconds(60);

        cluster.add_node(make_node("node-fresh", Region::GLOBAL, now));

        let mut stale_node = make_node("node-stale", Region::GLOBAL, now);
        stale_node.last_heartbeat = stale_time;
        cluster.nodes.insert(stale_node.node_id.clone(), stale_node);

        let stale = cluster.find_stale_nodes(chrono::Duration::seconds(30), now);
        assert_eq!(stale.len(), 1);
        assert_eq!(stale[0], NodeId::new("node-stale"));
    }

    // =========================================================================
    // Per-group membership tests
    // =========================================================================

    #[test]
    fn test_group_membership_independent_per_region() {
        let mut groups = GroupMembership::new();

        groups.set_role(Region::GLOBAL, NodeId::new("node-1"), NodeRole::Voter);
        groups.set_role(Region::US_EAST_VA, NodeId::new("node-1"), NodeRole::Learner);

        assert_eq!(groups.role_of(Region::GLOBAL, &NodeId::new("node-1")), Some(NodeRole::Voter));
        assert_eq!(
            groups.role_of(Region::US_EAST_VA, &NodeId::new("node-1")),
            Some(NodeRole::Learner)
        );
        assert_eq!(groups.role_of(Region::IE_EAST_DUBLIN, &NodeId::new("node-1")), None);
    }

    #[test]
    fn test_group_membership_remove_from_group() {
        let mut groups = GroupMembership::new();

        groups.set_role(Region::GLOBAL, NodeId::new("node-1"), NodeRole::Voter);
        groups.set_role(Region::US_EAST_VA, NodeId::new("node-1"), NodeRole::Learner);

        let removed = groups.remove_from_group(Region::GLOBAL, &NodeId::new("node-1"));
        assert_eq!(removed, Some(NodeRole::Voter));
        assert!(!groups.is_member(Region::GLOBAL, &NodeId::new("node-1")));
        assert!(groups.is_member(Region::US_EAST_VA, &NodeId::new("node-1")));
    }

    #[test]
    fn test_group_membership_remove_from_all_groups() {
        let mut groups = GroupMembership::new();

        groups.set_role(Region::GLOBAL, NodeId::new("node-1"), NodeRole::Voter);
        groups.set_role(Region::US_EAST_VA, NodeId::new("node-1"), NodeRole::Learner);
        groups.set_role(Region::IE_EAST_DUBLIN, NodeId::new("node-1"), NodeRole::Voter);

        let removed_from = groups.remove_from_all_groups(&NodeId::new("node-1"));
        assert_eq!(removed_from.len(), 3);
        assert!(!groups.is_member(Region::GLOBAL, &NodeId::new("node-1")));
        assert!(!groups.is_member(Region::US_EAST_VA, &NodeId::new("node-1")));
        assert!(!groups.is_member(Region::IE_EAST_DUBLIN, &NodeId::new("node-1")));
    }

    #[test]
    fn test_eligible_nodes_for_non_protected_region() {
        let mut cluster = ClusterMembership::new();
        let now = Utc::now();

        cluster.add_node(make_node("node-us", Region::US_EAST_VA, now));
        cluster.add_node(make_node("node-ie", Region::IE_EAST_DUBLIN, now));
        cluster.add_node(make_node("node-global", Region::GLOBAL, now));

        // US_EAST_VA is non-protected — all nodes eligible
        let eligible = cluster.eligible_nodes_for_region(Region::US_EAST_VA);
        assert_eq!(eligible.len(), 3);
    }

    #[test]
    fn test_eligible_nodes_for_protected_region() {
        let mut cluster = ClusterMembership::new();
        let now = Utc::now();

        cluster.add_node(make_node("node-ie-1", Region::IE_EAST_DUBLIN, now));
        cluster.add_node(make_node("node-ie-2", Region::IE_EAST_DUBLIN, now));
        cluster.add_node(make_node("node-us", Region::US_EAST_VA, now));
        cluster.add_node(make_node("node-global", Region::GLOBAL, now));

        // IE_EAST_DUBLIN requires residency — only IE nodes eligible
        let eligible = cluster.eligible_nodes_for_region(Region::IE_EAST_DUBLIN);
        assert_eq!(eligible.len(), 2);
        assert!(eligible.iter().all(|n| n.region == Region::IE_EAST_DUBLIN));
    }

    #[test]
    fn test_per_group_voter_promotion_independent() {
        let mut cluster = ClusterMembership::new();
        let now = Utc::now();

        // Add 6 nodes to GLOBAL and 3 to US_EAST_VA
        for i in 0..6 {
            let joined = now + chrono::Duration::seconds(i);
            let node = make_node(&format!("node-{i}"), Region::US_EAST_VA, joined);
            cluster.add_node_to_group(node.clone(), Region::GLOBAL);
            if i < 3 {
                cluster.group_roles_mut().set_role(
                    Region::US_EAST_VA,
                    node.node_id.clone(),
                    NodeRole::Voter,
                );
            }
        }

        // GLOBAL: 5 voters + 1 learner; US_EAST_VA: 3 voters
        assert_eq!(cluster.group_roles().voter_count(Region::GLOBAL), 5);
        assert_eq!(cluster.group_roles().voter_count(Region::US_EAST_VA), 3);

        // Promoting in US_EAST_VA should not affect GLOBAL
        cluster.group_roles_mut().remove_from_group(Region::US_EAST_VA, &NodeId::new("node-0"));
        assert_eq!(cluster.group_roles().voter_count(Region::GLOBAL), 5);
        assert_eq!(cluster.group_roles().voter_count(Region::US_EAST_VA), 2);
    }

    #[test]
    fn test_max_voters_enforced_per_group() {
        let mut groups = GroupMembership::new();

        // Fill GLOBAL to max
        for i in 0..5 {
            groups.set_role(Region::GLOBAL, NodeId::new(format!("node-{i}")), NodeRole::Voter);
        }

        // US_EAST_VA still has room
        assert_eq!(groups.determine_role(Region::GLOBAL), NodeRole::Learner);
        assert_eq!(groups.determine_role(Region::US_EAST_VA), NodeRole::Voter);
    }

    #[test]
    fn test_in_region_node_count() {
        let mut cluster = ClusterMembership::new();
        let now = Utc::now();

        cluster.add_node(make_node("node-ie-1", Region::IE_EAST_DUBLIN, now));
        cluster.add_node(make_node("node-ie-2", Region::IE_EAST_DUBLIN, now));
        cluster.add_node(make_node("node-ie-3", Region::IE_EAST_DUBLIN, now));
        cluster.add_node(make_node("node-us", Region::US_EAST_VA, now));

        assert_eq!(cluster.in_region_node_count(Region::IE_EAST_DUBLIN), 3);
        assert_eq!(cluster.in_region_node_count(Region::US_EAST_VA), 1);
        assert_eq!(cluster.in_region_node_count(Region::JP_EAST_TOKYO), 0);
    }

    #[test]
    fn test_min_nodes_per_protected_region_constant() {
        assert_eq!(MIN_NODES_PER_PROTECTED_REGION, 3);
    }

    #[test]
    fn test_group_membership_members() {
        let mut groups = GroupMembership::new();

        groups.set_role(Region::GLOBAL, NodeId::new("node-1"), NodeRole::Voter);
        groups.set_role(Region::GLOBAL, NodeId::new("node-2"), NodeRole::Learner);
        groups.set_role(Region::GLOBAL, NodeId::new("node-3"), NodeRole::Voter);

        let members = groups.members(Region::GLOBAL);
        assert_eq!(members.len(), 3);
        assert_eq!(groups.member_count(Region::GLOBAL), 3);
        assert_eq!(groups.member_count(Region::US_EAST_VA), 0);
    }

    #[test]
    fn test_empty_group_cleaned_up_after_last_removal() {
        let mut groups = GroupMembership::new();

        groups.set_role(Region::US_EAST_VA, NodeId::new("node-1"), NodeRole::Voter);
        assert!(groups.group(Region::US_EAST_VA).is_some());

        groups.remove_from_group(Region::US_EAST_VA, &NodeId::new("node-1"));
        assert!(groups.group(Region::US_EAST_VA).is_none());
    }

    #[test]
    fn test_demote_voter_when_over_max() {
        let mut cluster = ClusterMembership::new();
        let base = Utc::now();

        // Add MAX_VOTERS + 1 = 6 nodes, all as voters initially
        for i in 0..6 {
            let joined = base + chrono::Duration::seconds(i64::from(i));
            let node = make_node(&format!("node-{i}"), Region::GLOBAL, joined);
            cluster.add_node_to_group(node, Region::GLOBAL);
        }

        // First 5 are voters, 6th is learner (because MAX_VOTERS=5)
        assert_eq!(cluster.group_roles().voter_count(Region::GLOBAL), 5);
        assert_eq!(cluster.group_roles().learner_count(Region::GLOBAL), 1);

        // Force the 6th to voter to simulate an over-limit state
        cluster.group_roles_mut().set_role(Region::GLOBAL, NodeId::new("node-5"), NodeRole::Voter);
        assert_eq!(cluster.group_roles().voter_count(Region::GLOBAL), 6);

        // Demote should remove the newest voter (node-5, joined last)
        let demoted = cluster.demote_voter_if_needed(Region::GLOBAL);
        assert_eq!(demoted, Some(NodeId::new("node-5")));
        assert_eq!(cluster.group_roles().voter_count(Region::GLOBAL), 5);
    }

    #[test]
    fn test_demote_voter_noop_at_max() {
        let mut cluster = ClusterMembership::new();
        let now = Utc::now();

        // Add exactly MAX_VOTERS nodes
        for i in 0..5 {
            let node = make_node(&format!("node-{i}"), Region::GLOBAL, now);
            cluster.add_node_to_group(node, Region::GLOBAL);
        }

        assert_eq!(cluster.group_roles().voter_count(Region::GLOBAL), 5);

        // At exactly MAX_VOTERS, demotion should not trigger
        let demoted = cluster.demote_voter_if_needed(Region::GLOBAL);
        assert!(demoted.is_none());
        assert_eq!(cluster.group_roles().voter_count(Region::GLOBAL), 5);
    }
}
