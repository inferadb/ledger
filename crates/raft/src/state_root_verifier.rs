//! State root divergence handler.
//!
//! Receives [`StateRootDivergence`] events from the apply path and proposes
//! [`OrganizationRequest::UpdateVaultHealth`] via Raft to halt diverged vaults
//! cluster-wide.
//!
//! ## Architecture
//!
//! ```text
//! apply_to_state_machine()                   StateRootDivergenceHandler
//!   │ verify_state_root_commitment()              │
//!   │   mismatch → send(divergence)  ──────────►  │ recv(divergence)
//!   │                                              │   → raft.client_write(UpdateVaultHealth)
//! ```
//!
//! The handler runs as a background task alongside `AutoRecoveryJob` and
//! other per-region async workers.

use std::sync::Arc;

use tokio::sync::mpsc;
use tracing::{error, info, warn};

use crate::{
    consensus_handle::ConsensusHandle,
    types::{OrganizationRequest, RaftPayload, StateRootDivergence},
};

/// Async task that receives divergence events and halts diverged vaults.
///
/// Spawned once per region group. On receiving a divergence event, proposes
/// `UpdateVaultHealth { healthy: false }` through Raft to record the
/// divergence in the state machine (visible to all nodes).
pub struct StateRootDivergenceHandler {
    /// Consensus handle for proposing vault health updates.
    handle: Arc<ConsensusHandle>,
    /// Receiver for divergence events from the apply path.
    receiver: mpsc::UnboundedReceiver<StateRootDivergence>,
    /// Region name for logging.
    region: String,
}

impl StateRootDivergenceHandler {
    /// Creates a new divergence handler.
    pub fn new(
        handle: Arc<ConsensusHandle>,
        receiver: mpsc::UnboundedReceiver<StateRootDivergence>,
        region: String,
    ) -> Self {
        Self { handle, receiver, region }
    }

    /// Runs the handler loop until the channel is closed.
    ///
    /// For each divergence event, proposes `UpdateVaultHealth { healthy: false }`
    /// via Raft. Proposal failures are logged but do not stop the handler —
    /// the divergence is already recorded in metrics and logs by the apply path.
    pub async fn run(mut self) {
        info!(region = %self.region, "State root divergence handler started");

        while let Some(divergence) = self.receiver.recv().await {
            warn!(
                region = %self.region,
                organization = divergence.organization.value(),
                vault = divergence.vault.value(),
                vault_height = divergence.vault_height,
                "Proposing vault halt due to state root divergence"
            );

            let payload = RaftPayload::system(OrganizationRequest::UpdateVaultHealth {
                organization: divergence.organization,
                vault: divergence.vault,
                healthy: false,
                expected_root: Some(divergence.leader_state_root),
                computed_root: Some(divergence.local_state_root),
                diverged_at_height: Some(divergence.vault_height),
                recovery_attempt: None,
                recovery_started_at: None,
            });

            match self.handle.propose(payload).await {
                Ok(_) => {
                    info!(
                        region = %self.region,
                        organization = divergence.organization.value(),
                        vault = divergence.vault.value(),
                        "Vault halted due to state root divergence"
                    );
                },
                Err(e) => {
                    // Proposal may fail if this node is not leader — that's fine,
                    // the leader will also detect the divergence independently.
                    error!(
                        region = %self.region,
                        organization = divergence.organization.value(),
                        vault = divergence.vault.value(),
                        error = %e,
                        "Failed to propose vault halt for state root divergence"
                    );
                },
            }
        }

        info!(region = %self.region, "State root divergence handler stopped");
    }

    /// Builds the `RaftPayload` for a divergence event.
    ///
    /// Extracted for testability — the `run()` loop cannot be unit-tested
    /// without a live Raft handle.
    #[cfg(test)]
    fn build_halt_payload(divergence: &StateRootDivergence) -> RaftPayload<OrganizationRequest> {
        RaftPayload::system(OrganizationRequest::UpdateVaultHealth {
            organization: divergence.organization,
            vault: divergence.vault,
            healthy: false,
            expected_root: Some(divergence.leader_state_root),
            computed_root: Some(divergence.local_state_root),
            diverged_at_height: Some(divergence.vault_height),
            recovery_attempt: None,
            recovery_started_at: None,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods, clippy::panic)]
mod tests {
    use inferadb_ledger_types::{OrganizationId, VaultId};
    use tokio::sync::mpsc;

    use super::*;

    fn sample_divergence() -> StateRootDivergence {
        StateRootDivergence {
            organization: OrganizationId::new(42),
            vault: VaultId::new(7),
            vault_height: 100,
            local_state_root: [0xAA; 32],
            leader_state_root: [0xBB; 32],
        }
    }

    #[test]
    fn test_build_halt_payload_fields() {
        let divergence = sample_divergence();
        let payload = StateRootDivergenceHandler::build_halt_payload(&divergence);

        match &payload.request {
            OrganizationRequest::UpdateVaultHealth {
                organization,
                vault,
                healthy,
                expected_root,
                computed_root,
                diverged_at_height,
                recovery_attempt,
                recovery_started_at,
            } => {
                assert_eq!(*organization, OrganizationId::new(42));
                assert_eq!(*vault, VaultId::new(7));
                assert!(!healthy, "should mark vault as unhealthy");
                assert_eq!(*expected_root, Some([0xBB; 32]), "expected = leader's root");
                assert_eq!(*computed_root, Some([0xAA; 32]), "computed = local root");
                assert_eq!(*diverged_at_height, Some(100));
                assert!(recovery_attempt.is_none());
                assert!(recovery_started_at.is_none());
            },
            _ => panic!("expected UpdateVaultHealth request"),
        }

        assert!(payload.state_root_commitments.is_empty());
    }

    #[test]
    fn test_build_halt_payload_zero_height() {
        let divergence = StateRootDivergence {
            organization: OrganizationId::new(1),
            vault: VaultId::new(1),
            vault_height: 0,
            local_state_root: [0u8; 32],
            leader_state_root: [1u8; 32],
        };

        let payload = StateRootDivergenceHandler::build_halt_payload(&divergence);

        match &payload.request {
            OrganizationRequest::UpdateVaultHealth { diverged_at_height, .. } => {
                assert_eq!(*diverged_at_height, Some(0));
            },
            _ => panic!("expected UpdateVaultHealth"),
        }
    }

    #[tokio::test]
    async fn test_channel_sender_drop_stops_receiver() {
        // When the sender side is dropped (e.g., region shutdown),
        // the receiver loop should terminate.
        let (sender, receiver) = mpsc::unbounded_channel::<StateRootDivergence>();

        // Send one event then drop
        sender.send(sample_divergence()).expect("send");
        drop(sender);

        // Receiver should get the event, then `recv()` returns None
        let mut rx = receiver;
        let event = rx.recv().await;
        assert!(event.is_some());
        assert_eq!(event.unwrap().organization, OrganizationId::new(42));

        let none = rx.recv().await;
        assert!(none.is_none(), "should return None after sender dropped");
    }
}
