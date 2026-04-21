//! Cluster bootstrap: start + init pattern.
//!
//! Implements the CockroachDB-style two-phase startup where nodes start
//! in an uninitialized state and transition to running only after an
//! explicit cluster init command. This prevents accidental split-brain
//! by requiring operator intent before forming a cluster.

use crate::types::{NodeId, ConsensusStateId};

/// Initializes a new cluster.
///
/// Generates a unique `cluster_id` and transitions `state` from
/// `Uninitialized` → `Running`. Called exactly once during cluster setup.
/// Subsequent calls return [`crate::error::ConsensusError::AlreadyInitialized`].
///
/// # Errors
///
/// Returns `AlreadyInitialized` if `state` is already `Running`.
pub fn init_cluster(
    state: &mut BootstrapState,
    _config: &BootstrapConfig,
) -> Result<InitResult, crate::error::ConsensusError> {
    if state.is_running() {
        return Err(crate::error::ConsensusError::AlreadyInitialized);
    }

    let cluster_id = generate_cluster_id();
    *state = BootstrapState::Running { cluster_id };

    Ok(InitResult { cluster_id, global_shard: ConsensusStateId(0) })
}

/// Generates a unique cluster identifier using random bytes.
fn generate_cluster_id() -> u64 {
    rand::random::<u64>()
}

/// Node lifecycle state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BootstrapState {
    /// Waiting for init. Rejects client traffic.
    Uninitialized,
    /// Cluster initialized. Normal operation.
    Running {
        /// The unique identifier for this cluster.
        cluster_id: u64,
    },
}

/// Bootstrap configuration from CLI flags.
#[derive(Debug, Clone)]
pub struct BootstrapConfig {
    /// This node's generated ID.
    pub node_id: NodeId,
    /// Seed addresses for discovery (--join).
    pub join_addrs: Vec<String>,
    /// Regulated regions this node serves (--region).
    pub regions: Vec<String>,
    /// Advertise address override (--advertise).
    pub advertise_addr: Option<String>,
    /// Data directory (--data).
    pub data_dir: String,
}

/// Result of cluster initialization.
#[derive(Debug, Clone)]
pub struct InitResult {
    /// Generated cluster ID.
    pub cluster_id: u64,
    /// The GLOBAL shard created during init.
    pub global_shard: ConsensusStateId,
}

impl BootstrapState {
    /// Whether this node is ready to serve traffic.
    pub fn is_running(&self) -> bool {
        matches!(self, Self::Running { .. })
    }

    /// The cluster ID if initialized.
    pub fn cluster_id(&self) -> Option<u64> {
        match self {
            Self::Running { cluster_id } => Some(*cluster_id),
            Self::Uninitialized => None,
        }
    }

    /// Validates that an incoming message's `cluster_id` matches this cluster.
    ///
    /// Returns an error if the IDs don't match (cross-cluster message). When
    /// `Uninitialized`, all cluster IDs are accepted (no local ID to compare).
    pub fn validate_cluster_id(&self, incoming: u64) -> Result<(), crate::error::ConsensusError> {
        match self {
            Self::Running { cluster_id } if *cluster_id != incoming => {
                Err(crate::error::ConsensusError::ShardUnavailable {
                    shard: crate::types::ConsensusStateId(0),
                })
            },
            _ => Ok(()),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;
    use crate::error::ConsensusError;

    fn default_config() -> BootstrapConfig {
        BootstrapConfig {
            node_id: crate::types::NodeId(1),
            join_addrs: vec![],
            regions: vec![],
            advertise_addr: None,
            data_dir: "/tmp/test".to_string(),
        }
    }

    // --- BootstrapState accessors ---

    #[test]
    fn uninitialized_state_is_not_running_and_has_no_cluster_id() {
        let state = BootstrapState::Uninitialized;
        assert!(!state.is_running());
        assert_eq!(state.cluster_id(), None);
    }

    #[test]
    fn running_state_is_running_and_exposes_cluster_id() {
        let state = BootstrapState::Running { cluster_id: 42 };
        assert!(state.is_running());
        assert_eq!(state.cluster_id(), Some(42));
    }

    // --- init_cluster ---

    #[test]
    fn init_cluster_transitions_to_running_with_global_shard() {
        let config = default_config();
        let mut state = BootstrapState::Uninitialized;
        let result = init_cluster(&mut state, &config).expect("init_cluster");
        assert!(state.is_running());
        assert_eq!(state.cluster_id(), Some(result.cluster_id));
        assert_eq!(result.global_shard, ConsensusStateId(0));
    }

    #[test]
    fn init_cluster_generates_nonzero_cluster_id_with_high_probability() {
        // Random u64 being exactly 0 has probability 2^-64. Run a few times
        // to confirm the generator produces varied values.
        let config = default_config();
        let mut ids = Vec::new();
        for _ in 0..5 {
            let mut state = BootstrapState::Uninitialized;
            let result = init_cluster(&mut state, &config).expect("init_cluster");
            ids.push(result.cluster_id);
        }
        // At least 2 distinct IDs out of 5 runs.
        ids.sort_unstable();
        ids.dedup();
        assert!(ids.len() >= 2, "expected distinct cluster IDs, got: {ids:?}");
    }

    #[test]
    fn double_init_returns_already_initialized() {
        let config = default_config();
        let mut state = BootstrapState::Uninitialized;
        init_cluster(&mut state, &config).expect("first init");
        let err = init_cluster(&mut state, &config).expect_err("second init");
        assert!(matches!(err, ConsensusError::AlreadyInitialized));
    }

    // --- validate_cluster_id ---

    #[test]
    fn validate_cluster_id_accepts_matching_id() {
        let state = BootstrapState::Running { cluster_id: 42 };
        assert!(state.validate_cluster_id(42).is_ok());
    }

    #[test]
    fn validate_cluster_id_rejects_mismatched_id() {
        let state = BootstrapState::Running { cluster_id: 42 };
        let err = state.validate_cluster_id(99).expect_err("mismatch");
        assert!(matches!(err, ConsensusError::ShardUnavailable { .. }));
    }

    #[test]
    fn validate_cluster_id_accepts_any_when_uninitialized() {
        let state = BootstrapState::Uninitialized;
        assert!(state.validate_cluster_id(0).is_ok());
        assert!(state.validate_cluster_id(u64::MAX).is_ok());
    }

    #[test]
    fn validate_cluster_id_accepts_self_match() {
        // Running state with incoming == cluster_id should always succeed,
        // even for boundary values.
        for id in [0, 1, u64::MAX] {
            let state = BootstrapState::Running { cluster_id: id };
            assert!(state.validate_cluster_id(id).is_ok());
        }
    }

    // --- BootstrapState equality ---

    #[test]
    fn bootstrap_state_equality() {
        assert_eq!(BootstrapState::Uninitialized, BootstrapState::Uninitialized);
        assert_eq!(
            BootstrapState::Running { cluster_id: 1 },
            BootstrapState::Running { cluster_id: 1 },
        );
        assert_ne!(BootstrapState::Uninitialized, BootstrapState::Running { cluster_id: 1 });
        assert_ne!(
            BootstrapState::Running { cluster_id: 1 },
            BootstrapState::Running { cluster_id: 2 },
        );
    }
}
