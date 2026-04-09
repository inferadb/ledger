//! Thread-safe map from node ID to network address.
//!
//! Populated during bootstrap from `initial_members` and updated dynamically
//! via `announce_peer` RPCs. Shared between [`RaftManager`](crate::raft_manager::RaftManager)
//! and gRPC services so that any component can resolve a peer's address
//! without reaching into the consensus transport layer.

use std::{collections::HashMap, sync::Arc};

use parking_lot::RwLock;

/// Thread-safe map from node ID to network address.
///
/// Clone is cheap (inner `Arc`). All clones share the same underlying map,
/// so an insert on one handle is immediately visible to all others.
///
/// # Examples
///
/// ```no_run
/// # use inferadb_ledger_raft::peer_address_map::PeerAddressMap;
/// let map = PeerAddressMap::new();
/// map.insert(1, "10.0.0.1:50051".to_string());
/// assert_eq!(map.get(1), Some("10.0.0.1:50051".to_string()));
/// ```
#[derive(Debug, Clone)]
pub struct PeerAddressMap {
    inner: Arc<RwLock<HashMap<u64, String>>>,
}

impl PeerAddressMap {
    /// Creates an empty peer address map.
    pub fn new() -> Self {
        Self { inner: Arc::new(RwLock::new(HashMap::new())) }
    }

    /// Inserts or updates the address for a node.
    pub fn insert(&self, node_id: u64, address: String) {
        self.inner.write().insert(node_id, address);
    }

    /// Removes a node's address, returning it if it existed.
    pub fn remove(&self, node_id: u64) -> Option<String> {
        self.inner.write().remove(&node_id)
    }

    /// Returns the address for a node, if present.
    pub fn get(&self, node_id: u64) -> Option<String> {
        self.inner.read().get(&node_id).cloned()
    }

    /// Returns all `(node_id, address)` pairs.
    pub fn iter_peers(&self) -> Vec<(u64, String)> {
        self.inner.read().iter().map(|(&id, addr)| (id, addr.clone())).collect()
    }

    /// Returns the number of entries.
    pub fn len(&self) -> usize {
        self.inner.read().len()
    }

    /// Returns `true` if the map contains no entries.
    pub fn is_empty(&self) -> bool {
        self.inner.read().is_empty()
    }

    /// Inserts multiple entries at once, holding the write lock for the duration.
    pub fn insert_many(&self, peers: impl IntoIterator<Item = (u64, String)>) {
        let mut map = self.inner.write();
        for (id, addr) in peers {
            map.insert(id, addr);
        }
    }
}

impl Default for PeerAddressMap {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_get() {
        let map = PeerAddressMap::new();
        map.insert(1, "10.0.0.1:50051".to_string());
        assert_eq!(map.get(1), Some("10.0.0.1:50051".to_string()));
        assert_eq!(map.get(2), None);
    }

    #[test]
    fn update_overwrites() {
        let map = PeerAddressMap::new();
        map.insert(1, "10.0.0.1:50051".to_string());
        map.insert(1, "10.0.0.2:50051".to_string());
        assert_eq!(map.get(1), Some("10.0.0.2:50051".to_string()));
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn remove_returns_old_value() {
        let map = PeerAddressMap::new();
        map.insert(1, "10.0.0.1:50051".to_string());
        assert_eq!(map.remove(1), Some("10.0.0.1:50051".to_string()));
        assert_eq!(map.remove(1), None);
        assert!(map.is_empty());
    }

    #[test]
    fn insert_many_adds_all() {
        let map = PeerAddressMap::new();
        map.insert_many(vec![
            (1, "10.0.0.1:50051".to_string()),
            (2, "10.0.0.2:50051".to_string()),
            (3, "10.0.0.3:50051".to_string()),
        ]);
        assert_eq!(map.len(), 3);
        assert_eq!(map.get(2), Some("10.0.0.2:50051".to_string()));
    }

    #[test]
    fn iter_peers_returns_all() {
        let map = PeerAddressMap::new();
        map.insert(1, "a".to_string());
        map.insert(2, "b".to_string());
        let mut peers = map.iter_peers();
        peers.sort_by_key(|(id, _)| *id);
        assert_eq!(peers, vec![(1, "a".to_string()), (2, "b".to_string())]);
    }

    #[test]
    fn default_is_empty() {
        let map = PeerAddressMap::default();
        assert!(map.is_empty());
        assert_eq!(map.len(), 0);
    }

    #[test]
    fn clone_shares_state() {
        let map = PeerAddressMap::new();
        let map2 = map.clone();
        map.insert(1, "addr".to_string());
        assert_eq!(map2.get(1), Some("addr".to_string()));
    }
}
