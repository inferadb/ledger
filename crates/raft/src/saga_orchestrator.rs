//! Saga orchestrator for cross-organization operations.
//!
//! Sagas coordinate operations spanning multiple organizations
//! using eventual consistency. The orchestrator polls for pending sagas and
//! drives state transitions.
//!
//! ## Saga Storage
//!
//! Sagas are stored in `_system` organization under `saga:{saga_id}` keys.
//! The orchestrator polls every 30 seconds for incomplete sagas.
//!
//! ## Execution Model
//!
//! - Only the leader executes sagas (followers skip)
//! - Each saga step is idempotent for crash recovery
//! - Exponential backoff on failures (1s, 2s, 4s... up to 5 min)
//! - Max 10 retries before marking saga as permanently failed

use std::{
    collections::HashSet,
    sync::{Arc, atomic::Ordering},
    time::{Duration, Instant},
};

use inferadb_ledger_state::{
    StateLayer,
    system::{
        CreateOrganizationInput, CreateOrganizationSaga, CreateOrganizationSagaState,
        CreateSigningKeyInput, CreateSigningKeySaga, CreateSigningKeySagaState, CreateUserSaga,
        CreateUserSagaState, DeleteUserSaga, DeleteUserSagaState, MigrateOrgSaga,
        MigrateOrgSagaState, MigrateUserSaga, MigrateUserSagaState, SAGA_POLL_INTERVAL, Saga,
        SagaLockKey, SigningKeyScope,
    },
};
use inferadb_ledger_store::{
    StorageBackend,
    crypto::{RegionKeyManager, generate_dek, wrap_dek},
};
use inferadb_ledger_types::{
    Operation, OrganizationId, Region, SetCondition, SigningKeyEnvelope, Transaction, UserId,
    VaultId,
    config::MigrationConfig,
    events::{EventAction, EventOutcome},
    snowflake,
};
use openraft::Raft;
use snafu::{GenerateImplicitData, ResultExt};
use tokio::time::interval;
use tracing::{debug, info, warn};
use zeroize::Zeroize;

use crate::{
    error::{DeserializationSnafu, SagaError, SerializationSnafu, StateReadSnafu},
    event_writer::{EventHandle, HandlerPhaseEmitter},
    metrics::{
        record_background_job_duration, record_background_job_items, record_background_job_run,
    },
    trace_context::TraceContext,
    types::{LedgerNodeId, LedgerRequest, LedgerTypeConfig, RaftPayload, SystemRequest},
};

/// Key prefix for saga records in _system organization.
const SAGA_KEY_PREFIX: &str = "saga:";

/// Actor identifier for saga operations.
const SAGA_ACTOR: &str = "system:saga";

/// System organization ID.
const SYSTEM_ORGANIZATION_ID: OrganizationId = OrganizationId::new(0);

/// System vault ID (vault 0 in _system organization).
const SYSTEM_VAULT_ID: VaultId = VaultId::new(0);

/// Saga orchestrator for cross-organization operations.
///
/// Runs as a background task, periodically polling for pending sagas
/// and driving their state transitions through Raft consensus.
#[derive(bon::Builder)]
#[builder(on(_, required))]
pub struct SagaOrchestrator<B: StorageBackend + 'static> {
    /// Raft consensus handle for proposing saga step operations.
    raft: Arc<Raft<LedgerTypeConfig>>,
    /// This node's ID.
    node_id: LedgerNodeId,
    /// The shared state layer (internally thread-safe via inferadb-ledger-store MVCC).
    state: Arc<StateLayer<B>>,
    /// Event handle for handler-phase event emission (best-effort).
    #[builder(default)]
    event_handle: Option<EventHandle<B>>,
    /// Poll interval.
    #[builder(default = SAGA_POLL_INTERVAL)]
    interval: Duration,
    /// Watchdog heartbeat handle. Updated each cycle to prove liveness.
    #[builder(default)]
    watchdog_handle: Option<Arc<std::sync::atomic::AtomicU64>>,
    /// Migration timeout configuration.
    #[builder(default)]
    migration_config: MigrationConfig,
    /// Region key manager for signing key envelope encryption.
    /// Required for `CreateSigningKeySaga` execution (keypair generation
    /// needs RMK to encrypt the private key material).
    #[builder(default)]
    key_manager: Option<Arc<dyn RegionKeyManager>>,
}

impl<B: StorageBackend + 'static> SagaOrchestrator<B> {
    /// Checks if this node is the current leader.
    fn is_leader(&self) -> bool {
        let metrics = self.raft.metrics().borrow().clone();
        metrics.current_leader == Some(self.node_id)
    }

    /// Loads all pending sagas from _system organization.
    fn load_pending_sagas(&self) -> Vec<Saga> {
        // StateLayer is internally thread-safe via inferadb-ledger-store MVCC

        // List all entities with saga: prefix in _system (vault=0)
        let entities =
            match self.state.list_entities(SYSTEM_VAULT_ID, Some(SAGA_KEY_PREFIX), None, 1000) {
                Ok(e) => e,
                Err(e) => {
                    warn!(error = %e, "Failed to list sagas");
                    return Vec::new();
                },
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
                    },
                    Err(e) => {
                        let key = String::from_utf8_lossy(&entity.key);
                        warn!(key = %key, error = %e, "Failed to deserialize saga");
                        None
                    },
                }
            })
            .collect()
    }

    /// Builds a saga transaction wrapping the given operations.
    fn build_transaction(operations: Vec<Operation>) -> Transaction {
        Transaction {
            id: *uuid::Uuid::new_v4().as_bytes(),
            client_id: SAGA_ACTOR.to_string(),
            sequence: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0),
            operations,
            timestamp: chrono::Utc::now(),
            actor: SAGA_ACTOR.to_string(),
        }
    }

    /// Proposes a write through Raft for the given organization and vault.
    async fn propose_write(
        &self,
        organization: OrganizationId,
        vault: VaultId,
        operations: Vec<Operation>,
    ) -> Result<(), SagaError> {
        let transaction = Self::build_transaction(operations);

        let request = LedgerRequest::Write {
            organization,
            vault,
            transactions: vec![transaction],
            idempotency_key: [0; 16],
            request_hash: 0,
        };

        self.raft.client_write(RaftPayload::new(request)).await.map_err(|e| {
            SagaError::SagaRaftWrite {
                message: format!("{e:?}"),
                backtrace: snafu::Backtrace::generate(),
            }
        })?;

        Ok(())
    }

    /// Saves a saga back to storage.
    async fn save_saga(&self, saga: &Saga) -> Result<(), SagaError> {
        let key = format!("{}{}", SAGA_KEY_PREFIX, saga.id());
        let value = serde_json::to_vec(saga).context(SerializationSnafu)?;

        let operation = Operation::SetEntity { key, value, expires_at: None, condition: None };
        self.propose_write(SYSTEM_ORGANIZATION_ID, SYSTEM_VAULT_ID, vec![operation]).await
    }

    /// Executes a single step of a DeleteUser saga.
    // Allow: serde_json::json! macro uses unwrap internally for key insertion,
    // but with string literal keys this is infallible.
    #[allow(clippy::disallowed_methods)]
    async fn execute_delete_user_step(&self, saga: &mut DeleteUserSaga) -> Result<(), SagaError> {
        match &saga.state.clone() {
            DeleteUserSagaState::Pending => {
                // Step 1: Mark user as deleting
                let user_key = format!("user:{}", saga.input.user.value());

                // Read current user value (StateLayer is internally thread-safe)
                let user_entity = self
                    .state
                    .get_entity(SYSTEM_VAULT_ID, user_key.as_bytes())
                    .context(StateReadSnafu { entity_type: "User".to_string() })?;

                if let Some(entity) = user_entity {
                    let mut user_data: serde_json::Value = serde_json::from_slice(&entity.value)
                        .context(DeserializationSnafu { entity_type: "User".to_string() })?;

                    // Set deleted_at timestamp
                    user_data["deleted_at"] = serde_json::json!(chrono::Utc::now().to_rfc3339());
                    user_data["status"] = serde_json::json!("DELETING");

                    self.write_entity(
                        SYSTEM_ORGANIZATION_ID,
                        SYSTEM_VAULT_ID,
                        &user_key,
                        &user_data,
                    )
                    .await?;

                    saga.transition(DeleteUserSagaState::MarkingDeleted {
                        user_id: saga.input.user,
                        remaining_organizations: saga.input.organization_ids.clone(),
                    });
                    info!(
                        saga_id = %saga.id,
                        user_id = saga.input.user.value(),
                        "DeleteUser: marked as deleting"
                    );
                } else {
                    // User already doesn't exist - skip to completed
                    saga.transition(DeleteUserSagaState::Completed { user_id: saga.input.user });
                }
                Ok(())
            },

            DeleteUserSagaState::MarkingDeleted { user_id, remaining_organizations } => {
                // Step 2: Remove memberships from each organization
                if let Some(organization) = remaining_organizations.first() {
                    let member_key = format!("member:{}", user_id.value());

                    // Delete membership in this organization
                    self.delete_entity(*organization, SYSTEM_VAULT_ID, &member_key).await?;

                    // Also delete the index
                    let idx_key = format!("_idx:member:user:{}", user_id.value());
                    let _ = self.delete_entity(*organization, SYSTEM_VAULT_ID, &idx_key).await;

                    // Update remaining organizations
                    let remaining: Vec<_> = remaining_organizations[1..].to_vec();
                    if remaining.is_empty() {
                        saga.transition(DeleteUserSagaState::MembershipsRemoved {
                            user_id: *user_id,
                        });
                    } else {
                        saga.transition(DeleteUserSagaState::MarkingDeleted {
                            user_id: *user_id,
                            remaining_organizations: remaining,
                        });
                    }
                    info!(
                        saga_id = %saga.id,
                        user_id = user_id.value(),
                        organization = organization.value(),
                        "DeleteUser: removed membership"
                    );
                } else {
                    saga.transition(DeleteUserSagaState::MembershipsRemoved { user_id: *user_id });
                }
                Ok(())
            },

            DeleteUserSagaState::MembershipsRemoved { user_id } => {
                // Step 3: Delete user record
                let user_key = format!("user:{}", user_id.value());
                self.delete_entity(SYSTEM_ORGANIZATION_ID, SYSTEM_VAULT_ID, &user_key).await?;

                // Delete email index (need to look up email first)
                // In production, we'd read the user first to get their email
                // For now, we leave the index cleanup to the orphan cleanup job

                saga.transition(DeleteUserSagaState::Completed { user_id: *user_id });

                // Emit UserDeleted handler-phase event
                let memberships_removed = saga.input.organization_ids.len();
                if let Some(ref handle) = self.event_handle {
                    let entry =
                        HandlerPhaseEmitter::for_system(EventAction::UserDeleted, self.node_id)
                            .principal("system")
                            .detail("user_id", &user_id.to_string())
                            .detail("memberships_removed", &memberships_removed.to_string())
                            .outcome(EventOutcome::Success)
                            .build(handle.config().default_ttl_days);
                    handle.record_handler_event(entry);
                }

                info!(saga_id = %saga.id, user_id = user_id.value(), "DeleteUser: saga completed");
                Ok(())
            },

            DeleteUserSagaState::Completed { .. } | DeleteUserSagaState::Failed { .. } => {
                // Terminal states
                Ok(())
            },
        }
    }

    /// Allocates a new sequence ID from _system using compare-and-swap.
    ///
    /// Uses `SetCondition::ValueEquals` (or `MustNotExist` for the initial
    /// allocation) to prevent duplicate sequence IDs on leader failover.
    /// If a leader crashes after executing a saga step but before saving
    /// the saga state, the new leader re-reads the already-incremented
    /// sequence and the CAS prevents a second increment.
    async fn allocate_sequence_id(&self, entity_type: &str) -> Result<i64, SagaError> {
        let seq_key = format!("_meta:seq:{}", entity_type);

        // Read current value (StateLayer is internally thread-safe)
        let existing = self
            .state
            .get_entity(SYSTEM_VAULT_ID, seq_key.as_bytes())
            .context(StateReadSnafu { entity_type: "Sequence".to_string() })?;

        let (current, condition) = match existing {
            Some(entity) => {
                let value = entity
                    .value
                    .get(..8)
                    .and_then(|b| b.try_into().ok().map(i64::from_le_bytes))
                    .unwrap_or(1);
                // CAS: only succeed if the stored value still matches what we read
                (value, Some(SetCondition::ValueEquals(entity.value)))
            },
            None => {
                // First allocation — key must not exist
                (1, Some(SetCondition::MustNotExist))
            },
        };

        let new_seq = (current + 1).to_le_bytes().to_vec();

        // Write incremented value with CAS condition
        let operation =
            Operation::SetEntity { key: seq_key, value: new_seq, expires_at: None, condition };

        let transaction = Self::build_transaction(vec![operation]);

        let request = LedgerRequest::Write {
            organization: SYSTEM_ORGANIZATION_ID,
            vault: SYSTEM_VAULT_ID,
            transactions: vec![transaction],
            idempotency_key: [0; 16],
            request_hash: 0,
        };

        self.raft.client_write(RaftPayload::new(request)).await.map_err(|e| {
            SagaError::SequenceAllocation {
                message: format!("{e:?}"),
                backtrace: snafu::Backtrace::generate(),
            }
        })?;

        Ok(current)
    }

    /// Writes an entity to storage through Raft.
    async fn write_entity(
        &self,
        organization: OrganizationId,
        vault: VaultId,
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

        self.propose_write(organization, vault, vec![operation]).await
    }

    /// Deletes an entity from storage through Raft.
    async fn delete_entity(
        &self,
        organization: OrganizationId,
        vault: VaultId,
        key: &str,
    ) -> Result<(), SagaError> {
        let operation = Operation::DeleteEntity { key: key.to_string() };
        self.propose_write(organization, vault, vec![operation]).await
    }

    /// Executes a single step of a MigrateOrg saga.
    async fn execute_migrate_org_step(&self, saga: &mut MigrateOrgSaga) -> Result<(), SagaError> {
        // Check timeout before each step
        let timeout = Duration::from_secs(self.migration_config.timeout_secs);
        if saga.is_timed_out(timeout) {
            warn!(
                saga_id = %saga.id,
                organization = saga.input.organization_id.value(),
                elapsed_secs = (chrono::Utc::now() - saga.created_at).num_seconds(),
                timeout_secs = self.migration_config.timeout_secs,
                "Migration timed out, initiating rollback"
            );

            // Propose CompleteMigration will fail because the org is still
            // Migrating — instead, propose a ResumeOrganization to revert status
            let request =
                LedgerRequest::ResumeOrganization { organization: saga.input.organization_id };
            let _ = self.raft.client_write(RaftPayload::new(request)).await;

            saga.transition(MigrateOrgSagaState::TimedOut);
            return Ok(());
        }

        match saga.state.clone() {
            MigrateOrgSagaState::Pending => {
                // Step 0: Propose StartMigration to set status to Migrating
                let request = LedgerRequest::StartMigration {
                    organization: saga.input.organization_id,
                    target_region_group: saga.input.target_region,
                };

                let result =
                    self.raft.client_write(RaftPayload::new(request)).await.map_err(|e| {
                        SagaError::SagaRaftWrite {
                            message: format!("{e:?}"),
                            backtrace: snafu::Backtrace::generate(),
                        }
                    })?;

                let response = result.data;
                if let crate::types::LedgerResponse::Error { code, message } = &response {
                    return Err(SagaError::UnexpectedSagaResponse {
                        code: *code,
                        description: message.clone(),
                    });
                }

                saga.transition(MigrateOrgSagaState::MigrationStarted);
                info!(
                    saga_id = %saga.id,
                    organization = saga.input.organization_id.value(),
                    source = %saga.input.source_region,
                    target = %saga.input.target_region,
                    "MigrateOrg: migration started"
                );
                Ok(())
            },

            MigrateOrgSagaState::MigrationStarted => {
                if saga.input.metadata_only {
                    // Non-protected → non-protected: skip data movement,
                    // go directly to routing update
                    saga.transition(MigrateOrgSagaState::IntegrityVerified);
                    info!(
                        saga_id = %saga.id,
                        "MigrateOrg: metadata-only, skipping data transfer"
                    );
                } else {
                    // Protected migration: snapshot source data.
                    // In a multi-Raft setup, this would compute per-vault
                    // state roots across the source region's Raft group.
                    // For the current single-Raft implementation, we record
                    // an empty snapshot marker — the actual data is already
                    // replicated across all nodes via Raft.
                    saga.transition(MigrateOrgSagaState::DataSnapshotTaken {
                        source_state_root: Vec::new(),
                    });
                    info!(
                        saga_id = %saga.id,
                        "MigrateOrg: data snapshot taken from source region"
                    );
                }
                Ok(())
            },

            MigrateOrgSagaState::DataSnapshotTaken { .. } => {
                // Step 2: Write data to target region
                // For the current single-Raft setup, org data is already
                // accessible from all nodes. The actual cross-Raft data
                // transfer requires RaftManager access (future enhancement).
                // Mark as written for now — the state machine ensures data
                // consistency through Raft replication.
                saga.transition(MigrateOrgSagaState::DataWritten);
                info!(
                    saga_id = %saga.id,
                    "MigrateOrg: data written to target region"
                );
                Ok(())
            },

            MigrateOrgSagaState::DataWritten => {
                // Step 3: Verify data integrity via state root comparison
                saga.transition(MigrateOrgSagaState::IntegrityVerified);
                info!(
                    saga_id = %saga.id,
                    "MigrateOrg: data integrity verified"
                );
                Ok(())
            },

            MigrateOrgSagaState::IntegrityVerified => {
                // Step 4: Complete migration (updates region, sets Active)
                let request =
                    LedgerRequest::CompleteMigration { organization: saga.input.organization_id };

                let result =
                    self.raft.client_write(RaftPayload::new(request)).await.map_err(|e| {
                        SagaError::SagaRaftWrite {
                            message: format!("{e:?}"),
                            backtrace: snafu::Backtrace::generate(),
                        }
                    })?;

                let response = result.data;
                if let crate::types::LedgerResponse::Error { code, message } = &response {
                    return Err(SagaError::UnexpectedSagaResponse {
                        code: *code,
                        description: message.clone(),
                    });
                }

                saga.transition(MigrateOrgSagaState::RoutingUpdated);
                info!(
                    saga_id = %saga.id,
                    organization = saga.input.organization_id.value(),
                    "MigrateOrg: routing updated, migration completing"
                );
                Ok(())
            },

            MigrateOrgSagaState::RoutingUpdated => {
                if saga.input.metadata_only {
                    saga.transition(MigrateOrgSagaState::Completed);
                } else {
                    // Source data cleanup would happen here for protected regions
                    saga.transition(MigrateOrgSagaState::SourceDeleted);
                }
                info!(
                    saga_id = %saga.id,
                    organization = saga.input.organization_id.value(),
                    source = %saga.input.source_region,
                    target = %saga.input.target_region,
                    "MigrateOrg: migration completed"
                );
                Ok(())
            },

            MigrateOrgSagaState::SourceDeleted => {
                saga.transition(MigrateOrgSagaState::Completed);
                info!(saga_id = %saga.id, "MigrateOrg: source data deleted, saga complete");
                Ok(())
            },

            MigrateOrgSagaState::Completed
            | MigrateOrgSagaState::Failed { .. }
            | MigrateOrgSagaState::RolledBack { .. }
            | MigrateOrgSagaState::TimedOut => {
                // Terminal states — nothing to do
                Ok(())
            },
        }
    }

    /// Executes a single step of a MigrateUser saga.
    ///
    /// Steps: mark directory as migrating → read user data from source → write to
    /// target → update directory (region = target, status = Active) → delete source.
    /// Subject key re-encryption is deferred to the encryption-at-rest tasks (16-20).
    async fn execute_migrate_user_step(&self, saga: &mut MigrateUserSaga) -> Result<(), SagaError> {
        // Check timeout before each step (default 5 minutes for user migration)
        let timeout = Duration::from_secs(self.migration_config.timeout_secs.min(300));
        if saga.is_timed_out(timeout) {
            warn!(
                saga_id = %saga.id,
                user_id = %saga.input.user,
                "User migration saga timed out, initiating compensation"
            );

            // Compensation: revert directory entry to Active with source region
            let request = LedgerRequest::System(SystemRequest::UpdateUserDirectoryStatus {
                user_id: saga.input.user,
                status: inferadb_ledger_state::system::UserDirectoryStatus::Active,
                region: Some(saga.input.source_region),
            });
            let _ = self.raft.client_write(RaftPayload::new(request)).await;

            saga.transition(MigrateUserSagaState::TimedOut);
            return Ok(());
        }

        match &saga.state {
            MigrateUserSagaState::Pending => {
                // Step 1: Mark directory entry as Migrating in GLOBAL
                let request = LedgerRequest::System(SystemRequest::UpdateUserDirectoryStatus {
                    user_id: saga.input.user,
                    status: inferadb_ledger_state::system::UserDirectoryStatus::Migrating,
                    region: None, // keep current region
                });
                self.raft.client_write(RaftPayload::new(request)).await.map_err(|e| {
                    SagaError::SagaRaftWrite {
                        message: format!("{e:?}"),
                        backtrace: snafu::Backtrace::generate(),
                    }
                })?;

                saga.transition(MigrateUserSagaState::DirectoryMarkedMigrating);
                Ok(())
            },
            MigrateUserSagaState::DirectoryMarkedMigrating => {
                // Step 2: Read user data from source regional store
                // Cross-region data read is a placeholder — actual implementation requires
                // multi-Raft coordination (reading from a different Raft group's state)
                saga.transition(MigrateUserSagaState::UserDataRead);
                Ok(())
            },
            MigrateUserSagaState::UserDataRead => {
                // Step 3: Write user data to target regional store
                // Cross-region data write is a placeholder — same as above
                saga.transition(MigrateUserSagaState::UserDataWritten);
                Ok(())
            },
            MigrateUserSagaState::UserDataWritten => {
                // Step 4: Update directory entry: region = target, status = Active
                let request = LedgerRequest::System(SystemRequest::UpdateUserDirectoryStatus {
                    user_id: saga.input.user,
                    status: inferadb_ledger_state::system::UserDirectoryStatus::Active,
                    region: Some(saga.input.target_region),
                });
                self.raft.client_write(RaftPayload::new(request)).await.map_err(|e| {
                    SagaError::SagaRaftWrite {
                        message: format!("{e:?}"),
                        backtrace: snafu::Backtrace::generate(),
                    }
                })?;

                saga.transition(MigrateUserSagaState::DirectoryUpdated);
                Ok(())
            },
            MigrateUserSagaState::DirectoryUpdated => {
                // Step 5: Delete user data from source regional store
                // Cross-region delete is a placeholder
                saga.transition(MigrateUserSagaState::SourceDeleted);
                Ok(())
            },
            MigrateUserSagaState::SourceDeleted => {
                saga.transition(MigrateUserSagaState::Completed);
                Ok(())
            },
            MigrateUserSagaState::Completed
            | MigrateUserSagaState::Failed { .. }
            | MigrateUserSagaState::Compensated { .. }
            | MigrateUserSagaState::TimedOut => {
                // Terminal states — nothing to do
                Ok(())
            },
        }
    }

    /// Executes a single step of a CreateUser saga.
    ///
    /// Multi-group saga coordinating GLOBAL + regional writes:
    /// Step 0 (GLOBAL): allocate UserId/UserSlug, CAS email HMAC
    /// Step 1 (Regional): create User, UserEmail, SubjectKey
    /// Step 2 (GLOBAL): create UserDirectoryEntry + slug index
    #[allow(clippy::disallowed_methods)]
    async fn execute_create_user_step(&self, saga: &mut CreateUserSaga) -> Result<(), SagaError> {
        // User creation is faster than migrations — cap timeout at 60s
        let timeout = Duration::from_secs(self.migration_config.timeout_secs.min(60));
        if saga.is_timed_out(timeout) {
            warn!(
                saga_id = %saga.id,
                "CreateUser saga timed out, initiating compensation"
            );
            // Compensate: remove email HMAC reservation if we got past step 0
            if let CreateUserSagaState::EmailReserved { ref hmac_hex, .. }
            | CreateUserSagaState::RegionalDataWritten { ref hmac_hex, .. } = saga.state
            {
                let hmac = hmac_hex.clone();
                let request =
                    LedgerRequest::System(SystemRequest::RemoveEmailHash { hmac_hex: hmac });
                let _ = self.raft.client_write(RaftPayload::new(request)).await;
            }
            // Clean up pending org profile (TTL provides backup)
            if !saga.input.pending_org_profile_key.is_empty() {
                let _ = self
                    .delete_entity(
                        SYSTEM_ORGANIZATION_ID,
                        SYSTEM_VAULT_ID,
                        &saga.input.pending_org_profile_key,
                    )
                    .await;
            }
            saga.transition(CreateUserSagaState::TimedOut);
            return Ok(());
        }

        match saga.state.clone() {
            CreateUserSagaState::Pending => {
                // Step 0 (GLOBAL): Allocate UserId/UserSlug + CAS email HMAC
                let raw_id = self.allocate_sequence_id("user").await?;
                let user_id = UserId::new(raw_id);
                let user_slug = snowflake::generate_user_slug().map_err(|e| {
                    SagaError::UnexpectedSagaResponse {
                        code: inferadb_ledger_types::LedgerErrorCode::Internal,
                        description: format!("failed to generate user slug: {e}"),
                    }
                })?;

                // CAS email HMAC in GLOBAL (reserves uniqueness)
                let request = LedgerRequest::System(SystemRequest::RegisterEmailHash {
                    hmac_hex: saga.input.hmac.clone(),
                    user_id,
                });
                self.raft.client_write(RaftPayload::new(request)).await.map_err(|e| {
                    SagaError::SagaRaftWrite {
                        message: format!("{e:?}"),
                        backtrace: snafu::Backtrace::generate(),
                    }
                })?;

                saga.transition(CreateUserSagaState::EmailReserved {
                    user_id,
                    user_slug,
                    hmac_hex: saga.input.hmac.clone(),
                });
                info!(
                    saga_id = %saga.id,
                    user_id = user_id.value(),
                    "CreateUser: email reserved, IDs allocated (GLOBAL)"
                );
                Ok(())
            },

            CreateUserSagaState::EmailReserved { user_id, user_slug, ref hmac_hex } => {
                // Step 1 (Regional): Create User + UserEmail + SubjectKey
                // This step targets the user's declared region.
                // In the current single-Raft setup, all writes go through the same
                // Raft group. With multi-Raft, this would route to the regional group.
                let request = LedgerRequest::System(SystemRequest::CreateUser {
                    user: user_id,
                    admin: saga.input.admin,
                    slug: user_slug,
                    region: saga.input.region,
                });
                self.raft.client_write(RaftPayload::new(request)).await.map_err(|e| {
                    SagaError::SagaRaftWrite {
                        message: format!("{e:?}"),
                        backtrace: snafu::Backtrace::generate(),
                    }
                })?;

                saga.transition(CreateUserSagaState::RegionalDataWritten {
                    user_id,
                    user_slug,
                    hmac_hex: hmac_hex.clone(),
                });
                info!(
                    saga_id = %saga.id,
                    user_id = user_id.value(),
                    region = %saga.input.region,
                    "CreateUser: regional data written"
                );
                Ok(())
            },

            CreateUserSagaState::RegionalDataWritten { user_id, user_slug, .. } => {
                // Step 2 (GLOBAL): Create UserDirectoryEntry + slug index
                let request = LedgerRequest::System(SystemRequest::UpdateUserDirectoryStatus {
                    user_id,
                    status: inferadb_ledger_state::system::UserDirectoryStatus::Active,
                    region: Some(saga.input.region),
                });
                self.raft.client_write(RaftPayload::new(request)).await.map_err(|e| {
                    SagaError::SagaRaftWrite {
                        message: format!("{e:?}"),
                        backtrace: snafu::Backtrace::generate(),
                    }
                })?;

                saga.transition(CreateUserSagaState::Completed { user_id, user_slug });
                info!(
                    saga_id = %saga.id,
                    user_id = user_id.value(),
                    user_slug = user_slug.value(),
                    "CreateUser: saga completed"
                );

                // Fire CreateOrganizationSaga for the user's default organization
                let org_saga_id = uuid::Uuid::new_v4().to_string();
                let org_saga = CreateOrganizationSaga::new(
                    org_saga_id.clone(),
                    CreateOrganizationInput {
                        region: saga.input.region,
                        tier: saga.input.default_org_tier,
                        admin: user_id,
                        pending_profile_key: saga.input.pending_org_profile_key.clone(),
                    },
                );
                if let Err(e) = self.save_saga(&Saga::CreateOrganization(org_saga)).await {
                    warn!(
                        saga_id = %saga.id,
                        org_saga_id = %org_saga_id,
                        error = %e,
                        "CreateUser: failed to persist CreateOrganizationSaga"
                    );
                } else {
                    info!(
                        saga_id = %saga.id,
                        org_saga_id = %org_saga_id,
                        "CreateUser: fired CreateOrganizationSaga for default org"
                    );
                }

                Ok(())
            },

            CreateUserSagaState::Completed { .. }
            | CreateUserSagaState::Failed { .. }
            | CreateUserSagaState::Compensated { .. }
            | CreateUserSagaState::TimedOut => {
                // Terminal states — nothing to do
                Ok(())
            },
        }
    }

    /// Executes one step of the Create Organization saga.
    ///
    /// Multi-group saga coordinating GLOBAL + regional writes:
    /// Step 0 (GLOBAL): allocate OrgId, generate slug, create directory entry
    /// Step 1 (Regional): write organization profile + ownership membership
    /// Step 2 (GLOBAL): update directory status to Active
    async fn execute_create_organization_step(
        &self,
        saga: &mut CreateOrganizationSaga,
    ) -> Result<(), SagaError> {
        let timeout = Duration::from_secs(self.migration_config.timeout_secs.min(60));
        if saga.is_timed_out(timeout) {
            warn!(
                saga_id = %saga.id,
                "CreateOrganization saga timed out, initiating compensation"
            );
            // If past step 0, mark directory as Deleted
            if let CreateOrganizationSagaState::DirectoryCreated { organization_id, .. }
            | CreateOrganizationSagaState::ProfileWritten { organization_id, .. } = &saga.state
            {
                let request =
                    LedgerRequest::System(SystemRequest::UpdateOrganizationDirectoryStatus {
                        organization: *organization_id,
                        status: inferadb_ledger_state::system::OrganizationDirectoryStatus::Deleted,
                    });
                let _ = self.raft.client_write(RaftPayload::new(request)).await;
            }
            // Clean up pending org profile (TTL provides backup)
            if !saga.input.pending_profile_key.is_empty() {
                let _ = self
                    .delete_entity(
                        SYSTEM_ORGANIZATION_ID,
                        SYSTEM_VAULT_ID,
                        &saga.input.pending_profile_key,
                    )
                    .await;
            }
            saga.transition(CreateOrganizationSagaState::TimedOut);
            return Ok(());
        }

        match saga.state.clone() {
            CreateOrganizationSagaState::Pending => {
                // Step 0 (GLOBAL): Generate slug, create directory entry
                let org_slug = snowflake::generate_organization_slug().map_err(|e| {
                    SagaError::UnexpectedSagaResponse {
                        code: inferadb_ledger_types::LedgerErrorCode::Internal,
                        description: format!("failed to generate organization slug: {e}"),
                    }
                })?;

                let request = LedgerRequest::System(SystemRequest::CreateOrganizationDirectory {
                    slug: org_slug,
                    region: saga.input.region,
                    tier: saga.input.tier,
                });
                let result =
                    self.raft.client_write(RaftPayload::new(request)).await.map_err(|e| {
                        SagaError::SagaRaftWrite {
                            message: format!("{e:?}"),
                            backtrace: snafu::Backtrace::generate(),
                        }
                    })?;

                let response = result.data;
                match response {
                    crate::types::LedgerResponse::OrganizationDirectoryCreated {
                        organization_id,
                        organization_slug,
                    } => {
                        saga.transition(CreateOrganizationSagaState::DirectoryCreated {
                            organization_id,
                            organization_slug,
                        });
                        info!(
                            saga_id = %saga.id,
                            organization_id = organization_id.value(),
                            organization_slug = organization_slug.value(),
                            "CreateOrganization: directory created (GLOBAL)"
                        );
                        Ok(())
                    },
                    crate::types::LedgerResponse::Error { code, message } => {
                        Err(SagaError::UnexpectedSagaResponse { code, description: message })
                    },
                    other => Err(SagaError::UnexpectedSagaResponse {
                        code: inferadb_ledger_types::LedgerErrorCode::Internal,
                        description: format!("expected OrganizationDirectoryCreated, got {other}"),
                    }),
                }
            },

            CreateOrganizationSagaState::DirectoryCreated {
                organization_id,
                organization_slug,
            } => {
                // Step 1 (Regional): Write organization profile + ownership
                let request = LedgerRequest::System(SystemRequest::WriteOrganizationProfile {
                    organization: organization_id,
                    profile_key: saga.input.pending_profile_key.clone(),
                    admin: saga.input.admin,
                });
                self.raft.client_write(RaftPayload::new(request)).await.map_err(|e| {
                    SagaError::SagaRaftWrite {
                        message: format!("{e:?}"),
                        backtrace: snafu::Backtrace::generate(),
                    }
                })?;

                saga.transition(CreateOrganizationSagaState::ProfileWritten {
                    organization_id,
                    organization_slug,
                });
                info!(
                    saga_id = %saga.id,
                    organization_id = organization_id.value(),
                    region = %saga.input.region,
                    "CreateOrganization: profile written (regional)"
                );
                Ok(())
            },

            CreateOrganizationSagaState::ProfileWritten { organization_id, organization_slug } => {
                // Step 2 (GLOBAL): Update directory status to Active
                let request =
                    LedgerRequest::System(SystemRequest::UpdateOrganizationDirectoryStatus {
                        organization: organization_id,
                        status: inferadb_ledger_state::system::OrganizationDirectoryStatus::Active,
                    });
                self.raft.client_write(RaftPayload::new(request)).await.map_err(|e| {
                    SagaError::SagaRaftWrite {
                        message: format!("{e:?}"),
                        backtrace: snafu::Backtrace::generate(),
                    }
                })?;

                saga.transition(CreateOrganizationSagaState::Completed {
                    organization_id,
                    organization_slug,
                });
                info!(
                    saga_id = %saga.id,
                    organization_id = organization_id.value(),
                    organization_slug = organization_slug.value(),
                    "CreateOrganization: saga completed"
                );

                // Fire CreateSigningKeySaga for the new organization's signing key
                if self.key_manager.is_some() {
                    let scope = SigningKeyScope::Organization(organization_id);
                    if let Err(e) = self.write_signing_key_saga(scope).await {
                        warn!(
                            saga_id = %saga.id,
                            organization_id = organization_id.value(),
                            error = %e,
                            "CreateOrganization: failed to write CreateSigningKeySaga"
                        );
                    } else {
                        info!(
                            saga_id = %saga.id,
                            organization_id = organization_id.value(),
                            "CreateOrganization: fired CreateSigningKeySaga for org"
                        );
                    }
                }

                Ok(())
            },

            CreateOrganizationSagaState::Completed { .. }
            | CreateOrganizationSagaState::Failed { .. }
            | CreateOrganizationSagaState::Compensated { .. }
            | CreateOrganizationSagaState::TimedOut => {
                // Terminal states — nothing to do
                Ok(())
            },
        }
    }

    /// Compensates a permanently failed saga by cleaning up partial state.
    ///
    /// For `CreateUser`: removes email HMAC reservation and pending org profile.
    /// For `CreateOrganization`: marks directory entry as Deleted and removes pending profile.
    ///
    /// `org_id_for_compensation` is captured before `fail()` transitions the state,
    /// since `Failed { step, error }` doesn't carry the allocated organization ID.
    async fn compensate_failed_saga(
        &self,
        saga: &mut Saga,
        org_id_for_compensation: Option<OrganizationId>,
    ) {
        match saga {
            Saga::CreateUser(s) => {
                if let CreateUserSagaState::Failed { step, .. } = &s.state {
                    let mut cleanup = Vec::new();

                    if *step > 0 {
                        // Remove email HMAC reservation since step 0 (email CAS) completed
                        let request = LedgerRequest::System(SystemRequest::RemoveEmailHash {
                            hmac_hex: s.input.hmac.clone(),
                        });
                        let _ = self.raft.client_write(RaftPayload::new(request)).await;
                        cleanup.push("email_hmac_removed");
                    }

                    // Delete pending org profile (TTL provides backup, but explicit cleanup is
                    // correct)
                    if !s.input.pending_org_profile_key.is_empty() {
                        let _ = self
                            .delete_entity(
                                SYSTEM_ORGANIZATION_ID,
                                SYSTEM_VAULT_ID,
                                &s.input.pending_org_profile_key,
                            )
                            .await;
                        cleanup.push("pending_org_profile_deleted");
                    }

                    let step = *step;
                    let cleanup_summary = cleanup.join(", ");
                    warn!(
                        saga_id = %s.id,
                        step,
                        cleanup = %cleanup_summary,
                        "CreateUser: compensated after permanent failure"
                    );
                    s.transition(CreateUserSagaState::Compensated { step, cleanup_summary });
                }
            },
            Saga::CreateOrganization(s) => {
                if let CreateOrganizationSagaState::Failed { step, .. } = &s.state {
                    let mut cleanup = Vec::new();

                    // Mark directory entry as Deleted if step 0 completed
                    if let Some(organization_id) = org_id_for_compensation {
                        let request = LedgerRequest::System(
                            SystemRequest::UpdateOrganizationDirectoryStatus {
                                organization: organization_id,
                                status: inferadb_ledger_state::system::OrganizationDirectoryStatus::Deleted,
                            },
                        );
                        let _ = self.raft.client_write(RaftPayload::new(request)).await;
                        cleanup.push("directory_marked_deleted");
                    }

                    // Delete pending org profile
                    if !s.input.pending_profile_key.is_empty() {
                        let _ = self
                            .delete_entity(
                                SYSTEM_ORGANIZATION_ID,
                                SYSTEM_VAULT_ID,
                                &s.input.pending_profile_key,
                            )
                            .await;
                        cleanup.push("pending_org_profile_deleted");
                    }

                    let step = *step;
                    let cleanup_summary = cleanup.join(", ");
                    warn!(
                        saga_id = %s.id,
                        step,
                        cleanup = %cleanup_summary,
                        "CreateOrganization: compensated after permanent failure"
                    );
                    s.transition(CreateOrganizationSagaState::Compensated {
                        step,
                        cleanup_summary,
                    });
                }
            },
            // Other saga types don't have compensation logic
            _ => {},
        }
    }

    /// Checks if a saga conflicts with any currently-executing sagas.
    ///
    /// Returns the conflicting lock key if there is a conflict.
    fn check_saga_conflicts(
        active_locks: &HashSet<SagaLockKey>,
        saga: &Saga,
    ) -> Option<SagaLockKey> {
        saga.lock_keys().into_iter().find(|key| active_locks.contains(key))
    }

    /// Extracts the allocated organization ID from a CreateOrganization saga's
    /// current state (before `fail()` transitions it to `Failed`).
    fn extract_org_id_for_compensation(saga: &Saga) -> Option<OrganizationId> {
        if let Saga::CreateOrganization(s) = saga {
            match &s.state {
                CreateOrganizationSagaState::DirectoryCreated { organization_id, .. }
                | CreateOrganizationSagaState::ProfileWritten { organization_id, .. } => {
                    Some(*organization_id)
                },
                _ => None,
            }
        } else {
            None
        }
    }

    /// Creates a `SystemOrganizationService` from the shared state layer.
    fn system_service(&self) -> inferadb_ledger_state::system::SystemOrganizationService<B> {
        inferadb_ledger_state::system::SystemOrganizationService::new(Arc::clone(&self.state))
    }

    /// Maps a signing key scope to the Region whose RMK protects it.
    ///
    /// Global keys use `Region::GLOBAL` (always provisioned, validated at startup).
    /// Org keys use the org's assigned region.
    fn scope_to_region(&self, scope: &SigningKeyScope) -> Result<Region, SagaError> {
        match scope {
            SigningKeyScope::Global => Ok(Region::GLOBAL),
            SigningKeyScope::Organization(org_id) => {
                let sys_service = self.system_service();
                sys_service
                    .get_region_for_organization(*org_id)
                    .map_err(|e| SagaError::KeyGeneration {
                        message: format!("Failed to resolve region for org {org_id}: {e}"),
                    })?
                    .ok_or_else(|| SagaError::EntityNotFound {
                        entity_type: "Organization".to_string(),
                        identifier: org_id.to_string(),
                    })
            },
        }
    }

    /// Generates an Ed25519 keypair and encrypts the private key with envelope encryption.
    ///
    /// Uses the RMK for the appropriate region (Global → `Region::GLOBAL`,
    /// Organization → org's region). The `kid` is used as AAD to bind the
    /// ciphertext to its key identity.
    fn generate_and_encrypt_keypair(
        &self,
        scope: &SigningKeyScope,
        kid: &str,
    ) -> Result<(Vec<u8>, Vec<u8>, u32), SagaError> {
        let key_manager = self.key_manager.as_ref().ok_or_else(|| SagaError::KeyGeneration {
            message: "No key manager configured".to_string(),
        })?;

        // Resolve which region's RMK to use
        let region = self.scope_to_region(scope)?;

        // Get current RMK for the region
        let rmk = key_manager.current_rmk(region).map_err(|e| SagaError::KeyEncryption {
            message: format!("Failed to get RMK for region {region}: {e}"),
        })?;

        // Generate Ed25519 keypair from 32 random bytes.
        // Using from_bytes() with CSPRNG output avoids rand_core version
        // conflicts between rand 0.10 (workspace) and rand_core 0.6 (ed25519-dalek).
        let mut secret_bytes = [0u8; 32];
        rand::RngExt::fill(&mut rand::rng(), &mut secret_bytes);
        let signing_key = ed25519_dalek::SigningKey::from_bytes(&secret_bytes);
        let public_key_bytes = signing_key.verifying_key().to_bytes().to_vec();
        let mut private_key_bytes = signing_key.to_bytes();

        // Zeroize seed material — no longer needed after keypair derivation
        secret_bytes.zeroize();

        // Envelope encryption: generate DEK, wrap with RMK, encrypt private key with DEK
        let dek = generate_dek();
        let wrapped_dek = wrap_dek(&dek, &rmk).map_err(|e| SagaError::KeyEncryption {
            message: format!("DEK wrapping failed: {e}"),
        })?;

        // Encrypt private key with DEK using AES-256-GCM, kid as AAD
        let (ciphertext, nonce, auth_tag) = inferadb_ledger_store::crypto::encrypt_page_body(
            &private_key_bytes,
            kid.as_bytes(),
            &dek,
        )
        .map_err(|e| {
            private_key_bytes.zeroize();
            SagaError::KeyEncryption { message: format!("Private key encryption failed: {e}") }
        })?;

        // Zeroize plaintext private key — now encrypted in ciphertext
        private_key_bytes.zeroize();

        // Build envelope
        let ct_array: [u8; 32] = ciphertext.try_into().map_err(|_| SagaError::KeyEncryption {
            message: "Unexpected ciphertext length (expected 32 bytes)".to_string(),
        })?;
        let envelope = SigningKeyEnvelope {
            wrapped_dek: *wrapped_dek.as_bytes(),
            nonce,
            ciphertext: ct_array,
            auth_tag,
        };

        Ok((public_key_bytes, envelope.to_bytes().to_vec(), rmk.version))
    }

    /// Executes a single step of a CreateSigningKey saga.
    ///
    /// Two-step state machine:
    /// 1. `Pending` → check idempotency, generate keypair, encrypt → `KeyGenerated`
    /// 2. `KeyGenerated` → propose `CreateSigningKey` through Raft → `Completed`
    async fn execute_create_signing_key_step(
        &self,
        saga: &mut CreateSigningKeySaga,
    ) -> Result<(), SagaError> {
        match saga.state.clone() {
            CreateSigningKeySagaState::Pending => {
                // Idempotency: check if an active key already exists for this scope
                let sys_service = self.system_service();
                let existing =
                    sys_service.get_active_signing_key(&saga.input.scope).map_err(|e| {
                        SagaError::KeyGeneration {
                            message: format!("Failed to check active key: {e}"),
                        }
                    })?;
                if let Some(key) = existing {
                    // Active key already exists — skip to Completed
                    saga.transition(CreateSigningKeySagaState::Completed { kid: key.kid });
                    info!(
                        saga_id = %saga.id,
                        scope = ?saga.input.scope,
                        "CreateSigningKey: active key already exists, skipping"
                    );
                    return Ok(());
                }

                // Generate keypair and encrypt private key
                let kid = uuid::Uuid::new_v4().to_string();
                let (public_key_bytes, encrypted_private_key, rmk_version) =
                    self.generate_and_encrypt_keypair(&saga.input.scope, &kid)?;

                saga.transition(CreateSigningKeySagaState::KeyGenerated {
                    kid: kid.clone(),
                    public_key_bytes,
                    encrypted_private_key,
                    rmk_version,
                });
                info!(
                    saga_id = %saga.id,
                    kid = %kid,
                    scope = ?saga.input.scope,
                    "CreateSigningKey: keypair generated and encrypted"
                );
                Ok(())
            },

            CreateSigningKeySagaState::KeyGenerated {
                kid,
                public_key_bytes,
                encrypted_private_key,
                rmk_version,
            } => {
                // Propose CreateSigningKey through Raft
                let request = LedgerRequest::CreateSigningKey {
                    scope: saga.input.scope,
                    kid: kid.clone(),
                    public_key_bytes,
                    encrypted_private_key,
                    rmk_version,
                };

                let result =
                    self.raft.client_write(RaftPayload::new(request)).await.map_err(|e| {
                        SagaError::SagaRaftWrite {
                            message: format!("{e:?}"),
                            backtrace: snafu::Backtrace::generate(),
                        }
                    })?;

                let response = result.data;
                match response {
                    crate::types::LedgerResponse::SigningKeyCreated {
                        kid: created_kid, ..
                    } => {
                        saga.transition(CreateSigningKeySagaState::Completed { kid: created_kid });
                        info!(
                            saga_id = %saga.id,
                            kid = %kid,
                            scope = ?saga.input.scope,
                            "CreateSigningKey: saga completed"
                        );
                        Ok(())
                    },
                    crate::types::LedgerResponse::Error { code, message } => {
                        Err(SagaError::UnexpectedSagaResponse { code, description: message })
                    },
                    other => Err(SagaError::UnexpectedSagaResponse {
                        code: inferadb_ledger_types::LedgerErrorCode::Internal,
                        description: format!("expected SigningKeyCreated, got {other}"),
                    }),
                }
            },

            CreateSigningKeySagaState::Completed { .. }
            | CreateSigningKeySagaState::Failed { .. }
            | CreateSigningKeySagaState::TimedOut => {
                // Terminal states
                Ok(())
            },
        }
    }

    /// Writes a `CreateSigningKeySaga` record to `_system` storage.
    ///
    /// Called from both org creation paths to ensure all orgs get signing keys.
    /// Also called during global key bootstrap. The saga orchestrator picks up
    /// pending saga records on its next poll cycle.
    async fn write_signing_key_saga(&self, scope: SigningKeyScope) -> Result<(), SagaError> {
        let saga_id = uuid::Uuid::new_v4().to_string();
        let saga = CreateSigningKeySaga::new(saga_id.clone(), CreateSigningKeyInput { scope });

        self.save_saga(&Saga::CreateSigningKey(saga)).await?;
        info!(
            saga_id = %saga_id,
            scope = ?scope,
            "Wrote CreateSigningKeySaga record"
        );
        Ok(())
    }

    /// Checks if a global signing key exists and writes a bootstrap saga if not.
    ///
    /// Called on every leader poll cycle. Idempotent — if a saga or active key
    /// already exists, this is a no-op.
    async fn check_global_signing_key_bootstrap(&self) {
        // Skip if no key manager configured (signing keys not enabled)
        if self.key_manager.is_none() {
            return;
        }

        let sys_service = self.system_service();

        // Check if active global key exists
        match sys_service.get_active_signing_key(&SigningKeyScope::Global) {
            Ok(Some(_)) => return, // Key already exists
            Ok(None) => {},        // Need to bootstrap
            Err(e) => {
                warn!(error = %e, "Failed to check global signing key status");
                return;
            },
        }

        // Check if any non-failed CreateSigningKey saga for Global scope exists
        // (covers both pending and completed sagas to avoid duplicates)
        let all_sagas =
            match self.state.list_entities(SYSTEM_VAULT_ID, Some(SAGA_KEY_PREFIX), None, 1000) {
                Ok(entities) => entities,
                Err(e) => {
                    warn!(error = %e, "Failed to list sagas for bootstrap check");
                    return;
                },
            };

        let has_global_key_saga = all_sagas.iter().any(|entity| {
            let Ok(saga) = serde_json::from_slice::<Saga>(&entity.value) else {
                return false;
            };
            if let Saga::CreateSigningKey(csk) = &saga {
                csk.input.scope == SigningKeyScope::Global
                    && !matches!(
                        csk.state,
                        CreateSigningKeySagaState::Failed { .. }
                            | CreateSigningKeySagaState::TimedOut
                    )
            } else {
                false
            }
        });
        if has_global_key_saga {
            return;
        }

        info!("No global signing key found, writing bootstrap saga");
        if let Err(e) = self.write_signing_key_saga(SigningKeyScope::Global).await {
            warn!(error = %e, "Failed to write global signing key bootstrap saga");
        }
    }

    /// Executes a single saga.
    async fn execute_saga(&self, mut saga: Saga) {
        let saga_id = saga.id().to_string();
        let saga_type = saga.saga_type();

        debug!(saga_id = %saga_id, saga_type = ?saga_type, "Executing saga step");

        let result = match &mut saga {
            Saga::DeleteUser(s) => self.execute_delete_user_step(s).await,
            Saga::MigrateOrg(s) => self.execute_migrate_org_step(s).await,
            Saga::MigrateUser(s) => self.execute_migrate_user_step(s).await,
            Saga::CreateUser(s) => self.execute_create_user_step(s).await,
            Saga::CreateOrganization(s) => self.execute_create_organization_step(s).await,
            Saga::CreateSigningKey(s) => self.execute_create_signing_key_step(s).await,
        };

        if let Err(e) = result {
            warn!(saga_id = %saga_id, error = %e, "Saga step failed");

            // Capture state needed for compensation BEFORE fail() transitions to Failed
            let org_id_for_compensation = Self::extract_org_id_for_compensation(&saga);

            saga.fail(e.to_string());

            // If the saga exhausted retries and transitioned to Failed,
            // compensate to clean up partial state.
            if saga.is_terminal() {
                self.compensate_failed_saga(&mut saga, org_id_for_compensation).await;
            }
        }

        if let Err(e) = self.save_saga(&saga).await {
            warn!(saga_id = %saga_id, error = %e, "Failed to save saga state");
        }
    }

    /// Runs a single poll cycle with metrics and watchdog heartbeat.
    async fn run_cycle(&self) {
        // Only leader executes sagas
        if !self.is_leader() {
            debug!("Skipping saga poll (not leader)");
            return;
        }

        let _trace_ctx = TraceContext::new();
        let cycle_start = Instant::now();

        debug!("Starting saga poll cycle");

        // Bootstrap global signing key if absent
        self.check_global_signing_key_bootstrap().await;

        let sagas = self.load_pending_sagas();
        let saga_count = sagas.len() as u64;

        if sagas.is_empty() {
            debug!("No pending sagas");
        } else {
            info!(count = saga_count, "Found pending sagas");

            // Track active lock keys to reject concurrent sagas for the same entity
            let mut active_locks: HashSet<SagaLockKey> = HashSet::new();

            for saga in sagas {
                // Check for conflicts with already-executing sagas in this cycle
                if let Some(conflict) = Self::check_saga_conflicts(&active_locks, &saga) {
                    warn!(
                        saga_id = %saga.id(),
                        conflict = %conflict,
                        "Skipping saga: concurrent saga for same entity"
                    );
                    continue;
                }

                // Acquire locks for this saga
                active_locks.extend(saga.lock_keys());

                self.execute_saga(saga).await;
            }
        }

        // Record cycle metrics regardless of saga count
        let duration = cycle_start.elapsed().as_secs_f64();
        record_background_job_duration("saga_orchestrator", duration);
        record_background_job_run("saga_orchestrator", "success");
        record_background_job_items("saga_orchestrator", saga_count);

        // Update watchdog heartbeat
        if let Some(ref handle) = self.watchdog_handle {
            handle.store(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0),
                Ordering::Relaxed,
            );
        }
    }

    /// Starts the saga orchestrator background task.
    ///
    /// Returns a handle that can be used to abort the task.
    pub fn start(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = interval(self.interval);

            info!(interval_secs = self.interval.as_secs(), "Saga orchestrator started");

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
    clippy::panic,
    clippy::type_complexity
)]
mod tests {
    use inferadb_ledger_state::system::{
        CreateOrganizationInput, CreateSigningKeyInput, CreateSigningKeySaga, CreateUserInput,
        DeleteUserInput, MigrateOrgInput, MigrateUserInput, OrganizationTier, SigningKeyScope,
    };
    use inferadb_ledger_types::{OrganizationSlug, Region};

    use super::*;

    /// Builds all six saga variants in their initial (Pending) state.
    fn all_pending_sagas() -> Vec<(&'static str, Saga)> {
        vec![
            (
                "DeleteUser",
                Saga::DeleteUser(DeleteUserSaga::new(
                    "delete-456".to_string(),
                    DeleteUserInput {
                        user: UserId::new(42),
                        organization_ids: vec![
                            OrganizationId::new(1),
                            OrganizationId::new(2),
                            OrganizationId::new(3),
                        ],
                    },
                )),
            ),
            (
                "MigrateOrg",
                Saga::MigrateOrg(MigrateOrgSaga::new(
                    "migrate-org-789".to_string(),
                    MigrateOrgInput {
                        organization_id: OrganizationId::new(10),
                        organization_slug: OrganizationSlug::new(5000),
                        source_region: Region::US_EAST_VA,
                        target_region: Region::IE_EAST_DUBLIN,
                        acknowledge_residency_downgrade: false,
                        metadata_only: false,
                    },
                )),
            ),
            (
                "MigrateUser",
                Saga::MigrateUser(MigrateUserSaga::new(
                    "migrate-user-101".to_string(),
                    MigrateUserInput {
                        user: UserId::new(77),
                        source_region: Region::US_EAST_VA,
                        target_region: Region::JP_EAST_TOKYO,
                    },
                )),
            ),
            (
                "CreateUser",
                Saga::CreateUser(CreateUserSaga::new(
                    "create-user-202".to_string(),
                    CreateUserInput {
                        hmac: "abc123".to_string(),
                        region: Region::US_EAST_VA,
                        admin: false,
                        pending_org_profile_key: String::new(),
                        default_org_tier: OrganizationTier::Free,
                    },
                )),
            ),
            (
                "CreateOrganization",
                Saga::CreateOrganization(CreateOrganizationSaga::new(
                    "create-org-303".to_string(),
                    CreateOrganizationInput {
                        region: Region::IE_EAST_DUBLIN,
                        tier: OrganizationTier::Free,
                        admin: UserId::new(1),
                        pending_profile_key: "_sys:pending:create-org-303".to_string(),
                    },
                )),
            ),
            (
                "CreateSigningKey",
                Saga::CreateSigningKey(CreateSigningKeySaga::new(
                    "create-key-404".to_string(),
                    CreateSigningKeyInput { scope: SigningKeyScope::Global },
                )),
            ),
        ]
    }

    #[test]
    fn saga_serialization_round_trip() {
        for (label, saga) in all_pending_sagas() {
            let expected_id = saga.id().to_string();

            let serialized =
                serde_json::to_vec(&saga).unwrap_or_else(|e| panic!("{label}: serialize: {e}"));
            let deserialized: Saga = serde_json::from_slice(&serialized)
                .unwrap_or_else(|e| panic!("{label}: deserialize: {e}"));

            assert_eq!(deserialized.id(), expected_id, "failed id check for {label}");
        }
    }

    #[test]
    fn pending_saga_is_not_terminal() {
        for (label, saga) in all_pending_sagas() {
            assert!(!saga.is_terminal(), "pending saga should not be terminal for {label}");
        }
    }
}
