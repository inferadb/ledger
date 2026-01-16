//! Saga orchestrator for cross-namespace operations.
//!
//! Per DESIGN.md §4.6: Sagas coordinate operations spanning multiple namespaces
//! using eventual consistency. The orchestrator polls for pending sagas and
//! drives state transitions.
//!
//! ## Saga Storage
//!
//! Sagas are stored in `_system` namespace under `saga:{saga_id}` keys.
//! The orchestrator polls every 30 seconds for incomplete sagas.
//!
//! ## Execution Model
//!
//! - Only the leader executes sagas (followers skip)
//! - Each saga step is idempotent for crash recovery
//! - Exponential backoff on failures (1s, 2s, 4s... up to 5 min)
//! - Max 10 retries before marking saga as permanently failed

use std::sync::Arc;
use std::time::Duration;

use openraft::Raft;
use snafu::{GenerateImplicitData, ResultExt};
use tokio::time::interval;
use tracing::{debug, info, warn};

use ledger_db::StorageBackend;
use ledger_state::StateLayer;
use ledger_state::system::{
    CreateOrgSaga, CreateOrgSagaState, DeleteUserSaga, DeleteUserSagaState, SAGA_POLL_INTERVAL,
    Saga,
};
use ledger_types::{Operation, Transaction};

use crate::error::{DeserializationSnafu, SagaError, SerializationSnafu, StateReadSnafu};
use crate::log_storage::AppliedStateAccessor;
use crate::types::{LedgerNodeId, LedgerRequest, LedgerTypeConfig};

/// Key prefix for saga records in _system namespace.
const SAGA_KEY_PREFIX: &str = "saga:";

/// Actor identifier for saga operations.
const SAGA_ACTOR: &str = "system:saga";

/// System namespace ID.
const SYSTEM_NAMESPACE_ID: i64 = 0;

/// Saga orchestrator for cross-namespace operations.
///
/// Runs as a background task, periodically polling for pending sagas
/// and driving their state transitions through Raft consensus.
pub struct SagaOrchestrator<B: StorageBackend + 'static> {
    /// The Raft instance.
    raft: Arc<Raft<LedgerTypeConfig>>,
    /// This node's ID.
    node_id: LedgerNodeId,
    /// The shared state layer (internally thread-safe via ledger-db MVCC).
    state: Arc<StateLayer<B>>,
    /// Accessor for applied state.
    /// Reserved for future saga state queries.
    #[allow(dead_code)]
    applied_state: AppliedStateAccessor,
    /// Poll interval.
    interval: Duration,
}

impl<B: StorageBackend + 'static> SagaOrchestrator<B> {
    /// Create a new saga orchestrator.
    pub fn new(
        raft: Arc<Raft<LedgerTypeConfig>>,
        node_id: LedgerNodeId,
        state: Arc<StateLayer<B>>,
        applied_state: AppliedStateAccessor,
    ) -> Self {
        Self {
            raft,
            node_id,
            state,
            applied_state,
            interval: SAGA_POLL_INTERVAL,
        }
    }

    /// Create with custom interval (for testing).
    #[cfg(test)]
    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    /// Check if this node is the current leader.
    fn is_leader(&self) -> bool {
        let metrics = self.raft.metrics().borrow().clone();
        metrics.current_leader == Some(self.node_id)
    }

    /// Load all pending sagas from _system namespace.
    fn load_pending_sagas(&self) -> Vec<Saga> {
        // StateLayer is internally thread-safe via ledger-db MVCC

        // List all entities with saga: prefix in _system (vault_id=0)
        let entities = match self
            .state
            .list_entities(0, Some(SAGA_KEY_PREFIX), None, 1000)
        {
            Ok(e) => e,
            Err(e) => {
                warn!(error = %e, "Failed to list sagas");
                return Vec::new();
            }
        };

        entities
            .into_iter()
            .filter_map(|entity| {
                // Deserialize saga record
                match serde_json::from_slice::<Saga>(&entity.value) {
                    Ok(saga) => {
                        if !saga.is_terminal() && saga.is_ready_for_retry() {
                            Some(saga)
                        } else {
                            None
                        }
                    }
                    Err(e) => {
                        let key = String::from_utf8_lossy(&entity.key);
                        warn!(key = %key, error = %e, "Failed to deserialize saga");
                        None
                    }
                }
            })
            .collect()
    }

    /// Save a saga back to storage.
    async fn save_saga(&self, saga: &Saga) -> Result<(), SagaError> {
        let key = format!("{}{}", SAGA_KEY_PREFIX, saga.id());
        let value = serde_json::to_vec(saga).context(SerializationSnafu)?;

        let operation = Operation::SetEntity {
            key,
            value,
            expires_at: None,
            condition: None,
        };

        let transaction = Transaction {
            id: *uuid::Uuid::new_v4().as_bytes(),
            client_id: SAGA_ACTOR.to_string(),
            sequence: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0),
            operations: vec![operation],
            timestamp: chrono::Utc::now(),
            actor: SAGA_ACTOR.to_string(),
        };

        let request = LedgerRequest::Write {
            namespace_id: SYSTEM_NAMESPACE_ID,
            vault_id: 0, // _system entities are in vault 0
            transactions: vec![transaction],
        };

        self.raft
            .client_write(request)
            .await
            .map_err(|e| SagaError::SagaRaftWrite {
                message: format!("{:?}", e),
                backtrace: snafu::Backtrace::generate(),
            })?;

        Ok(())
    }

    /// Execute a single step of a CreateOrg saga.
    // Allow: serde_json::json! macro uses unwrap internally for key insertion,
    // but with string literal keys this is infallible.
    #[allow(clippy::disallowed_methods)]
    async fn execute_create_org_step(&self, saga: &mut CreateOrgSaga) -> Result<(), SagaError> {
        // Clone state to avoid borrow conflicts with saga.transition()
        match saga.state.clone() {
            CreateOrgSagaState::Pending => {
                // Step 1: Create user in _system (if not using existing)
                if let Some(user_id) = saga.input.existing_user_id {
                    // Skip user creation, use existing
                    saga.transition(CreateOrgSagaState::UserCreated { user_id });
                    info!(saga_id = %saga.id, user_id, "CreateOrg: using existing user");
                } else {
                    // Create new user - allocate ID and write user entity
                    // Note: In production, this would call through the proper user creation flow
                    // For now, we simulate by writing to _system
                    let user_id = self.allocate_sequence_id("user").await?;

                    let user_key = format!("user:{}", user_id);
                    let user_value = serde_json::json!({
                        "id": user_id,
                        "name": saga.input.user_name,
                        "email": saga.input.user_email,
                        "created_at": chrono::Utc::now().to_rfc3339(),
                    });

                    self.write_entity(SYSTEM_NAMESPACE_ID, 0, &user_key, &user_value)
                        .await?;

                    // Also write email index
                    let email_idx_key = format!("_idx:user:email:{}", saga.input.user_email);
                    let email_idx_value = serde_json::json!({ "user_id": user_id });
                    self.write_entity(SYSTEM_NAMESPACE_ID, 0, &email_idx_key, &email_idx_value)
                        .await?;

                    saga.transition(CreateOrgSagaState::UserCreated { user_id });
                    info!(saga_id = %saga.id, user_id, "CreateOrg: user created");
                }
                Ok(())
            }

            CreateOrgSagaState::UserCreated { user_id } => {
                // Step 2: Create namespace
                let namespace_id = self.allocate_sequence_id("namespace").await?;

                let ns_key = format!("namespace:{}", namespace_id);
                let ns_value = serde_json::json!({
                    "id": namespace_id,
                    "name": saga.input.org_name,
                    "owner_user_id": user_id,
                    "created_at": chrono::Utc::now().to_rfc3339(),
                });

                self.write_entity(SYSTEM_NAMESPACE_ID, 0, &ns_key, &ns_value)
                    .await?;

                // Write namespace name index
                let name_idx_key = format!("_idx:namespace:name:{}", saga.input.org_name);
                let name_idx_value = serde_json::json!({ "namespace_id": namespace_id });
                self.write_entity(SYSTEM_NAMESPACE_ID, 0, &name_idx_key, &name_idx_value)
                    .await?;

                saga.transition(CreateOrgSagaState::NamespaceCreated {
                    user_id,
                    namespace_id,
                });
                info!(saga_id = %saga.id, namespace_id, "CreateOrg: namespace created");
                Ok(())
            }

            CreateOrgSagaState::NamespaceCreated {
                user_id,
                namespace_id,
            } => {
                // Step 3: Create membership record in the new namespace
                let member_key = format!("member:{}", user_id);
                let member_value = serde_json::json!({
                    "user_id": user_id,
                    "role": "owner",
                    "created_at": chrono::Utc::now().to_rfc3339(),
                });

                // Write to the new namespace (not _system)
                self.write_entity(namespace_id, 0, &member_key, &member_value)
                    .await?;

                saga.transition(CreateOrgSagaState::Completed {
                    user_id,
                    namespace_id,
                });
                info!(
                    saga_id = %saga.id,
                    user_id,
                    namespace_id,
                    "CreateOrg: saga completed"
                );
                Ok(())
            }

            CreateOrgSagaState::Completed { .. }
            | CreateOrgSagaState::Failed { .. }
            | CreateOrgSagaState::Compensated { .. } => {
                // Terminal states - nothing to do
                Ok(())
            }
        }
    }

    /// Execute a single step of a DeleteUser saga.
    // Allow: serde_json::json! macro uses unwrap internally for key insertion,
    // but with string literal keys this is infallible.
    #[allow(clippy::disallowed_methods)]
    async fn execute_delete_user_step(&self, saga: &mut DeleteUserSaga) -> Result<(), SagaError> {
        match &saga.state.clone() {
            DeleteUserSagaState::Pending => {
                // Step 1: Mark user as deleting
                let user_key = format!("user:{}", saga.input.user_id);

                // Read current user value (StateLayer is internally thread-safe)
                let user_entity =
                    self.state
                        .get_entity(0, user_key.as_bytes())
                        .context(StateReadSnafu {
                            entity_type: "User".to_string(),
                        })?;

                if let Some(entity) = user_entity {
                    let mut user_data: serde_json::Value = serde_json::from_slice(&entity.value)
                        .context(DeserializationSnafu {
                            entity_type: "User".to_string(),
                        })?;

                    // Set deleted_at timestamp
                    user_data["deleted_at"] = serde_json::json!(chrono::Utc::now().to_rfc3339());
                    user_data["status"] = serde_json::json!("DELETING");

                    self.write_entity(SYSTEM_NAMESPACE_ID, 0, &user_key, &user_data)
                        .await?;

                    saga.transition(DeleteUserSagaState::MarkingDeleted {
                        user_id: saga.input.user_id,
                        remaining_namespaces: saga.input.namespace_ids.clone(),
                    });
                    info!(
                        saga_id = %saga.id,
                        user_id = saga.input.user_id,
                        "DeleteUser: marked as deleting"
                    );
                } else {
                    // User already doesn't exist - skip to completed
                    saga.transition(DeleteUserSagaState::Completed {
                        user_id: saga.input.user_id,
                    });
                }
                Ok(())
            }

            DeleteUserSagaState::MarkingDeleted {
                user_id,
                remaining_namespaces,
            } => {
                // Step 2: Remove memberships from each namespace
                if let Some(namespace_id) = remaining_namespaces.first() {
                    let member_key = format!("member:{}", user_id);

                    // Delete membership in this namespace
                    self.delete_entity(*namespace_id, 0, &member_key).await?;

                    // Also delete the index
                    let idx_key = format!("_idx:member:user:{}", user_id);
                    let _ = self.delete_entity(*namespace_id, 0, &idx_key).await;

                    // Update remaining namespaces
                    let remaining: Vec<_> = remaining_namespaces[1..].to_vec();
                    if remaining.is_empty() {
                        saga.transition(DeleteUserSagaState::MembershipsRemoved {
                            user_id: *user_id,
                        });
                    } else {
                        saga.transition(DeleteUserSagaState::MarkingDeleted {
                            user_id: *user_id,
                            remaining_namespaces: remaining,
                        });
                    }
                    info!(
                        saga_id = %saga.id,
                        user_id,
                        namespace_id,
                        "DeleteUser: removed membership"
                    );
                } else {
                    saga.transition(DeleteUserSagaState::MembershipsRemoved { user_id: *user_id });
                }
                Ok(())
            }

            DeleteUserSagaState::MembershipsRemoved { user_id } => {
                // Step 3: Delete user record
                let user_key = format!("user:{}", user_id);
                self.delete_entity(SYSTEM_NAMESPACE_ID, 0, &user_key)
                    .await?;

                // Delete email index (need to look up email first)
                // In production, we'd read the user first to get their email
                // For now, we leave the index cleanup to the orphan cleanup job

                saga.transition(DeleteUserSagaState::Completed { user_id: *user_id });
                info!(saga_id = %saga.id, user_id, "DeleteUser: saga completed");
                Ok(())
            }

            DeleteUserSagaState::Completed { .. } | DeleteUserSagaState::Failed { .. } => {
                // Terminal states
                Ok(())
            }
        }
    }

    /// Allocate a new sequence ID from _system.
    async fn allocate_sequence_id(&self, entity_type: &str) -> Result<i64, SagaError> {
        let seq_key = format!("_meta:seq:{}", entity_type);

        // Read current value (StateLayer is internally thread-safe)
        let current = self
            .state
            .get_entity(0, seq_key.as_bytes())
            .context(StateReadSnafu {
                entity_type: "Sequence".to_string(),
            })?
            .and_then(|e| e.value.get(..8)?.try_into().ok().map(i64::from_le_bytes))
            .unwrap_or(1);

        let next_id = current;
        let new_seq = (current + 1).to_le_bytes().to_vec();

        // Write incremented value
        let operation = Operation::SetEntity {
            key: seq_key,
            value: new_seq,
            expires_at: None,
            condition: None,
        };

        let transaction = Transaction {
            id: *uuid::Uuid::new_v4().as_bytes(),
            client_id: SAGA_ACTOR.to_string(),
            sequence: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0),
            operations: vec![operation],
            timestamp: chrono::Utc::now(),
            actor: SAGA_ACTOR.to_string(),
        };

        let request = LedgerRequest::Write {
            namespace_id: SYSTEM_NAMESPACE_ID,
            vault_id: 0,
            transactions: vec![transaction],
        };

        self.raft
            .client_write(request)
            .await
            .map_err(|e| SagaError::SequenceAllocation {
                message: format!("{:?}", e),
                backtrace: snafu::Backtrace::generate(),
            })?;

        Ok(next_id)
    }

    /// Write an entity to storage through Raft.
    async fn write_entity(
        &self,
        namespace_id: i64,
        vault_id: i64,
        key: &str,
        value: &serde_json::Value,
    ) -> Result<(), SagaError> {
        let value_bytes = serde_json::to_vec(value).context(SerializationSnafu)?;

        let operation = Operation::SetEntity {
            key: key.to_string(),
            value: value_bytes,
            expires_at: None,
            condition: None,
        };

        let transaction = Transaction {
            id: *uuid::Uuid::new_v4().as_bytes(),
            client_id: SAGA_ACTOR.to_string(),
            sequence: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0),
            operations: vec![operation],
            timestamp: chrono::Utc::now(),
            actor: SAGA_ACTOR.to_string(),
        };

        let request = LedgerRequest::Write {
            namespace_id,
            vault_id,
            transactions: vec![transaction],
        };

        self.raft
            .client_write(request)
            .await
            .map_err(|e| SagaError::SagaRaftWrite {
                message: format!("{:?}", e),
                backtrace: snafu::Backtrace::generate(),
            })?;

        Ok(())
    }

    /// Delete an entity from storage through Raft.
    async fn delete_entity(
        &self,
        namespace_id: i64,
        vault_id: i64,
        key: &str,
    ) -> Result<(), SagaError> {
        let operation = Operation::DeleteEntity {
            key: key.to_string(),
        };

        let transaction = Transaction {
            id: *uuid::Uuid::new_v4().as_bytes(),
            client_id: SAGA_ACTOR.to_string(),
            sequence: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0),
            operations: vec![operation],
            timestamp: chrono::Utc::now(),
            actor: SAGA_ACTOR.to_string(),
        };

        let request = LedgerRequest::Write {
            namespace_id,
            vault_id,
            transactions: vec![transaction],
        };

        self.raft
            .client_write(request)
            .await
            .map_err(|e| SagaError::SagaRaftWrite {
                message: format!("{:?}", e),
                backtrace: snafu::Backtrace::generate(),
            })?;

        Ok(())
    }

    /// Execute a single saga.
    async fn execute_saga(&self, mut saga: Saga) {
        let saga_id = saga.id().to_string();
        let saga_type = saga.saga_type();

        debug!(saga_id = %saga_id, saga_type = ?saga_type, "Executing saga step");

        let result = match &mut saga {
            Saga::CreateOrg(s) => self.execute_create_org_step(s).await,
            Saga::DeleteUser(s) => self.execute_delete_user_step(s).await,
        };

        // Handle result
        match result {
            Ok(()) => {
                // Save updated saga state
                if let Err(e) = self.save_saga(&saga).await {
                    warn!(saga_id = %saga_id, error = %e, "Failed to save saga state");
                }
            }
            Err(e) => {
                warn!(saga_id = %saga_id, error = %e, "Saga step failed");

                // Mark failure and schedule retry
                let step = match &saga {
                    Saga::CreateOrg(s) => match &s.state {
                        CreateOrgSagaState::Pending => 0,
                        CreateOrgSagaState::UserCreated { .. } => 1,
                        CreateOrgSagaState::NamespaceCreated { .. } => 2,
                        _ => 0,
                    },
                    Saga::DeleteUser(s) => match &s.state {
                        DeleteUserSagaState::Pending => 0,
                        DeleteUserSagaState::MarkingDeleted { .. } => 1,
                        DeleteUserSagaState::MembershipsRemoved { .. } => 2,
                        _ => 0,
                    },
                };

                let error_msg = e.to_string();
                match &mut saga {
                    Saga::CreateOrg(s) => s.fail(step, error_msg.clone()),
                    Saga::DeleteUser(s) => s.fail(step, error_msg),
                }

                // Save failed state
                if let Err(save_err) = self.save_saga(&saga).await {
                    warn!(
                        saga_id = %saga_id,
                        error = %save_err,
                        "Failed to save saga failure state"
                    );
                }
            }
        }
    }

    /// Run a single poll cycle.
    async fn run_cycle(&self) {
        // Only leader executes sagas
        if !self.is_leader() {
            debug!("Skipping saga poll (not leader)");
            return;
        }

        debug!("Starting saga poll cycle");

        let sagas = self.load_pending_sagas();
        if sagas.is_empty() {
            debug!("No pending sagas");
            return;
        }

        info!(count = sagas.len(), "Found pending sagas");

        for saga in sagas {
            self.execute_saga(saga).await;
        }
    }

    /// Start the saga orchestrator background task.
    ///
    /// Returns a handle that can be used to abort the task.
    pub fn start(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = interval(self.interval);

            loop {
                ticker.tick().await;
                self.run_cycle().await;
            }
        })
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::disallowed_methods,
    clippy::panic
)]
mod tests {
    use super::*;
    use ledger_state::system::{CreateOrgInput, DeleteUserInput};

    #[test]
    fn test_saga_key_format() {
        let saga_id = "test-saga-123";
        let key = format!("{}{}", SAGA_KEY_PREFIX, saga_id);
        assert_eq!(key, "saga:test-saga-123");
    }

    #[test]
    fn test_create_org_saga_serialization() {
        let input = CreateOrgInput {
            user_name: "Alice".to_string(),
            user_email: "alice@example.com".to_string(),
            org_name: "acme".to_string(),
            existing_user_id: None,
        };

        let saga = CreateOrgSaga::new("test-123".to_string(), input);
        let wrapped = Saga::CreateOrg(saga);

        let serialized = serde_json::to_vec(&wrapped).unwrap();
        let deserialized: Saga = serde_json::from_slice(&serialized).unwrap();

        assert_eq!(deserialized.id(), "test-123");
        assert!(!deserialized.is_terminal());
    }

    #[test]
    fn test_delete_user_saga_serialization() {
        let input = DeleteUserInput {
            user_id: 42,
            namespace_ids: vec![1, 2, 3],
        };

        let saga = DeleteUserSaga::new("delete-456".to_string(), input);
        let wrapped = Saga::DeleteUser(saga);

        let serialized = serde_json::to_vec(&wrapped).unwrap();
        let deserialized: Saga = serde_json::from_slice(&serialized).unwrap();

        assert_eq!(deserialized.id(), "delete-456");
        assert!(!deserialized.is_terminal());
    }
}
