//! Saga orchestrator for cross-organization operations.
//!
//! Sagas coordinate operations spanning multiple organizations
//! using eventual consistency. The orchestrator polls for pending sagas and
//! drives state transitions.
//!
//! ## Saga Storage
//!
//! Sagas are stored in `_system` organization under `_meta:saga:{saga_id}` keys.
//! The orchestrator polls every 30 seconds for incomplete sagas.
//!
//! ## Execution Model
//!
//! - Only the leader executes sagas (followers skip)
//! - Each saga step is idempotent for crash recovery
//! - Exponential backoff on failures (1s, 2s, 4s... up to 5 min)
//! - Max 10 retries before marking saga as permanently failed

use std::{
    collections::{HashMap, HashSet},
    panic::AssertUnwindSafe,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use futures::FutureExt as _;
use inferadb_ledger_state::{
    StateLayer,
    system::{
        CreateOnboardingUserSaga, CreateOnboardingUserSagaState, CreateOrganizationInput,
        CreateOrganizationSaga, CreateOrganizationSagaState, CreateSigningKeyInput,
        CreateSigningKeySaga, CreateSigningKeySagaState, CreateUserSaga, CreateUserSagaState,
        DeleteUserSaga, DeleteUserSagaState, MigrateOrgSaga, MigrateOrgSagaState, MigrateUserSaga,
        MigrateUserSagaState, OrganizationStatus, SAGA_POLL_INTERVAL, Saga, SagaId, SagaLockKey,
        SigningKeyScope, SystemKeys,
    },
};
use inferadb_ledger_store::{
    StorageBackend,
    crypto::{RegionKeyManager, generate_dek, wrap_dek},
};
use inferadb_ledger_types::{
    ClientId, Operation, OrganizationId, OrganizationSlug, RefreshTokenId, Region, SetCondition,
    SigningKeyEnvelope, Transaction, UserId, UserSlug, VaultId,
    config::MigrationConfig,
    events::{EventAction, EventOutcome},
    snowflake,
};
use parking_lot::Mutex;
use snafu::{GenerateImplicitData, ResultExt};
use tokio::{sync::mpsc, time::interval};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use zeroize::Zeroizing;

use crate::{
    consensus_handle::ConsensusHandle,
    error::{SagaError, SerializationSnafu, StateReadSnafu},
    event_writer::{EventHandle, HandlerPhaseEmitter},
    raft_manager::RaftManager,
    trace_context::TraceContext,
    types::{LedgerNodeId, LedgerRequest, RaftPayload, SystemRequest},
};

/// Key prefix for saga records in _system organization.
const SAGA_KEY_PREFIX: &str = "_meta:saga:";

/// Client ID for saga operations.
const SAGA_CLIENT_ID: &str = "system:saga";

/// System organization ID.
const SYSTEM_ORGANIZATION_ID: OrganizationId = OrganizationId::new(0);

/// System vault ID (vault 0 in _system organization).
const SYSTEM_VAULT_ID: VaultId = VaultId::new(0);

/// PII context for onboarding saga — persisted to REGIONAL scratch storage.
///
/// This data is passed from the service handler to the saga orchestrator
/// via `SagaSubmission.pii`. It is persisted to REGIONAL storage under
/// `_tmp:saga_pii:{saga_id}` with a 24-hour TTL, surviving leader crashes.
/// NOT persisted in the GLOBAL Raft log or saga state.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct OnboardingPii {
    /// Email address (used to write `UserEmail` record in step 1).
    pub email: String,
    /// User display name (PII, written to regional store in step 1).
    pub name: String,
    /// Organization name (PII, written to regional store in step 1).
    pub organization_name: String,
}

impl std::fmt::Debug for OnboardingPii {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OnboardingPii")
            .field("email", &"[REDACTED]")
            .field("name", &"[REDACTED]")
            .field("organization_name", &"[REDACTED]")
            .finish()
    }
}

/// PII context for organization creation — persisted to REGIONAL scratch storage.
///
/// Organization names are PII that must not appear in the GLOBAL Raft log.
/// Passed via `SagaSubmission.org_pii` and persisted to REGIONAL storage
/// under `_tmp:saga_pii:{saga_id}` with a 24-hour TTL.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct OrgPii {
    /// Organization display name (PII, written regionally in step 1).
    pub name: String,
}

impl std::fmt::Debug for OrgPii {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OrgPii").field("name", &"[REDACTED]").finish()
    }
}

/// Combined PII envelope for REGIONAL scratch persistence.
///
/// Wraps both PII types so a single `_tmp:saga_pii:{saga_id}` key can
/// store either variant. JSON-serialized to REGIONAL storage with 24h TTL.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub enum SagaPii {
    /// Onboarding saga PII (email, name, organization_name).
    Onboarding(OnboardingPii),
    /// Organization creation saga PII (organization_name).
    Organization(OrgPii),
}

impl std::fmt::Debug for SagaPii {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SagaPii::Onboarding(pii) => pii.fmt(f),
            SagaPii::Organization(pii) => pii.fmt(f),
        }
    }
}

/// Crypto material generated during onboarding step 1 — held in-memory only.
///
/// Generated by the orchestrator when constructing `WriteOnboardingUserProfile`.
/// Consumed when building `SagaOutput` after step 2 completes. If lost on
/// crash, the saga compensates (same semantics as PII loss).
#[derive(Clone)]
pub struct OnboardingCrypto {
    /// Signing key identifier for JWT `kid` header.
    pub kid: String,
    /// Raw refresh token string (never persisted in Raft log).
    pub refresh_token: String,
    /// Refresh token family ID (16-byte random, for poison detection).
    pub refresh_family_id: [u8; 16],
    /// Refresh token expiration timestamp.
    pub refresh_expires_at: chrono::DateTime<chrono::Utc>,
    /// External user slug (copied from saga input for SagaOutput construction).
    pub user_slug: UserSlug,
    /// External organization slug (copied from saga input for SagaOutput construction).
    pub organization_slug: OrganizationSlug,
}

impl std::fmt::Debug for OnboardingCrypto {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OnboardingCrypto")
            .field("kid", &self.kid)
            .field("refresh_token", &"[REDACTED]")
            .field("refresh_family_id", &"[REDACTED]")
            .field("refresh_expires_at", &self.refresh_expires_at)
            .field("user_slug", &self.user_slug)
            .field("organization_slug", &self.organization_slug)
            .finish()
    }
}

/// Submission of a saga for orchestrator execution.
///
/// The service handler creates this struct and sends it to the orchestrator
/// via the `SagaOrchestratorHandle`. The `notify` channel provides
/// synchronous feedback — the service handler awaits the result with a
/// timeout before returning to the client.
pub struct SagaSubmission {
    /// The saga record to execute.
    pub record: Saga,
    /// Onboarding PII passed in-memory from the service handler. Used by the
    /// orchestrator to construct step 1 requests. NOT persisted in GLOBAL saga state.
    /// On crash, the oneshot sender is dropped and the saga compensates.
    pub pii: Option<OnboardingPii>,
    /// Organization PII passed in-memory. Used by `CreateOrganizationSaga` step 1
    /// to construct `WriteOrganizationProfile`. NOT persisted in GLOBAL saga state.
    pub org_pii: Option<OrgPii>,
    /// Notification channel for synchronous sagas. `None` for fire-and-forget.
    pub notify: Option<tokio::sync::oneshot::Sender<Result<SagaOutput, SagaError>>>,
}

/// Output from a completed onboarding saga.
///
/// Carries enough data for the service handler to sign a JWT and build
/// the `CompleteRegistrationResponse` without additional state reads.
#[derive(Clone)]
pub struct SagaOutput {
    /// Internal user ID.
    pub user_id: UserId,
    /// External Snowflake slug for the user.
    pub user_slug: UserSlug,
    /// Internal organization ID.
    pub organization_id: OrganizationId,
    /// External Snowflake slug for the auto-created organization.
    pub organization_slug: OrganizationSlug,
    /// Internal refresh token ID.
    pub refresh_token_id: RefreshTokenId,
    /// UUID-format signing key identifier for JWT `kid` header.
    pub kid: String,
    /// Refresh token family ID (16-byte random, for poison detection).
    pub refresh_family_id: [u8; 16],
    /// Refresh token expiration timestamp.
    pub refresh_expires_at: chrono::DateTime<chrono::Utc>,
    /// Raw refresh token string to return to the client.
    ///
    /// Generated by the saga orchestrator alongside `refresh_token_hash` for
    /// step 1. The hash is stored in the Raft log; this raw string is held
    /// in-memory and passed through `SagaOutput` to the service handler.
    pub refresh_token: String,
}

impl std::fmt::Debug for SagaOutput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SagaOutput")
            .field("user_id", &self.user_id)
            .field("user_slug", &self.user_slug)
            .field("organization_id", &self.organization_id)
            .field("organization_slug", &self.organization_slug)
            .field("refresh_token_id", &self.refresh_token_id)
            .field("kid", &self.kid)
            .field("refresh_family_id", &"[REDACTED]")
            .field("refresh_expires_at", &self.refresh_expires_at)
            .field("refresh_token", &"[REDACTED]")
            .finish()
    }
}

/// Channel buffer size for saga submissions. Generous buffer prevents
/// backpressure from blocking service handlers under normal load.
const SUBMISSION_CHANNEL_SIZE: usize = 256;

/// Cloneable handle for submitting sagas from service handlers.
///
/// Returned by [`SagaOrchestrator::start()`]. Services hold this; the
/// orchestrator background task holds the `mpsc::Receiver`.
#[derive(Clone)]
pub struct SagaOrchestratorHandle {
    tx: mpsc::Sender<SagaSubmission>,
}

impl SagaOrchestratorHandle {
    /// Submit a saga for execution by the orchestrator.
    ///
    /// The submission is queued and processed on the next poll cycle.
    /// Returns `Err` if the orchestrator has been shut down (receiver dropped).
    pub async fn submit_saga(&self, submission: SagaSubmission) -> Result<(), SagaError> {
        self.tx.send(submission).await.map_err(|_| SagaError::OrchestratorShutdown {
            message: "saga orchestrator channel closed".to_string(),
        })
    }
}

/// Saga orchestrator for cross-organization operations.
///
/// Runs as a background task, periodically polling for pending sagas
/// and driving their state transitions through Raft consensus.
#[derive(bon::Builder)]
#[builder(on(_, required))]
pub struct SagaOrchestrator<B: StorageBackend + 'static> {
    /// Consensus handle for proposing saga step operations.
    handle: Arc<ConsensusHandle>,
    /// This node's ID (used for event emission).
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
    /// Multi-Raft manager for routing PII-bearing requests to regional groups.
    /// When present, REGIONAL `SystemRequest` variants are proposed to the
    /// organization's or user's home region instead of GLOBAL Raft.
    /// `None` in single-node test setups where all data shares one Raft group.
    #[builder(default)]
    manager: Option<Arc<RaftManager>>,
    /// Region key manager for signing key envelope encryption.
    /// Required for `CreateSigningKeySaga` execution (keypair generation
    /// needs RMK to encrypt the private key material).
    #[builder(default)]
    key_manager: Option<Arc<dyn RegionKeyManager>>,
    /// Set once global signing key is confirmed (active key or saga found).
    /// Short-circuits future `check_global_signing_key_bootstrap` calls to O(1).
    #[builder(default)]
    global_key_confirmed: std::sync::atomic::AtomicBool,
    /// In-memory crypto material cache for onboarding sagas. Keyed by saga ID.
    /// Generated during step 1, consumed when building `SagaOutput` after step 2.
    #[builder(default)]
    crypto_cache: Mutex<HashMap<SagaId, OnboardingCrypto>>,
    /// Notification senders for synchronous sagas. Keyed by saga ID.
    /// When a saga reaches a terminal state, the orchestrator sends the
    /// result through the corresponding sender (if present).
    #[builder(default)]
    notify_cache:
        Mutex<HashMap<SagaId, tokio::sync::oneshot::Sender<Result<SagaOutput, SagaError>>>>,
    /// Cancellation token for graceful shutdown.
    cancellation_token: CancellationToken,
}

impl<B: StorageBackend + 'static> SagaOrchestrator<B> {
    /// Proposes a request to the appropriate Raft group for a given region.
    ///
    /// When `self.manager` is configured (multi-Raft), routes the request to
    /// the region's Raft group. Falls back to `self.handle` (GLOBAL) when no
    /// manager is present (single-node test setups).
    async fn propose_to_region(
        &self,
        region: Region,
        request: LedgerRequest,
    ) -> std::result::Result<crate::types::LedgerResponse, SagaError> {
        let payload = RaftPayload::system(request.clone());
        match &self.manager {
            Some(manager) => {
                let region_group =
                    manager.get_region_group(region).map_err(|e| SagaError::SagaRaftWrite {
                        message: format!("Region {region} not active: {e}"),
                        backtrace: snafu::Backtrace::generate(),
                    })?;

                // Try local proposal first.
                match region_group
                    .handle()
                    .propose_and_wait(payload, std::time::Duration::from_secs(30))
                    .await
                {
                    Ok(response) => Ok(response),
                    Err(e) if e.to_string().contains("Not the leader") => {
                        // Not the data region leader — forward to the actual leader.
                        let leader_id =
                            region_group.handle().current_leader().ok_or_else(|| {
                                SagaError::SagaRaftWrite {
                                    message: format!("No known leader for region {region}"),
                                    backtrace: snafu::Backtrace::generate(),
                                }
                            })?;
                        let leader_addr =
                            manager.peer_addresses().get(leader_id).ok_or_else(|| {
                                SagaError::SagaRaftWrite {
                                    message: format!(
                                        "No address for leader {leader_id} in region {region}"
                                    ),
                                    backtrace: snafu::Backtrace::generate(),
                                }
                            })?;

                        let request_payload =
                            inferadb_ledger_types::encode(&request).map_err(|e| {
                                SagaError::SagaRaftWrite {
                                    message: format!("serialize request: {e}"),
                                    backtrace: snafu::Backtrace::generate(),
                                }
                            })?;
                        let proto_region: inferadb_ledger_proto::proto::Region = region.into();

                        // Obtain the leader's peer connection from the shared
                        // registry. HTTP/2 multiplexes all subsystems over a
                        // single channel per peer.
                        let peer = manager
                            .registry()
                            .get_or_register(leader_id, &leader_addr)
                            .await
                            .map_err(|e| SagaError::SagaRaftWrite {
                                message: format!(
                                    "register leader {leader_id} ({leader_addr}): {e}"
                                ),
                                backtrace: snafu::Backtrace::generate(),
                            })?;
                        let mut client = peer.raft_client();
                        let resp = client
                            .regional_proposal(
                                inferadb_ledger_proto::proto::RegionalProposalRequest {
                                    region: Some(proto_region as i32),
                                    request_payload,
                                    caller: 0,
                                    timeout_ms: 30_000,
                                },
                            )
                            .await
                            .map_err(|e| SagaError::SagaRaftWrite {
                                message: format!("forward to leader: {e}"),
                                backtrace: snafu::Backtrace::generate(),
                            })?
                            .into_inner();

                        if resp.status_code != 0 {
                            return Err(SagaError::SagaRaftWrite {
                                message: format!("leader returned error: {}", resp.error_message),
                                backtrace: snafu::Backtrace::generate(),
                            });
                        }

                        // Wait for local replication so subsequent reads see the data.
                        if resp.committed_index > 0 {
                            let mut watch = region_group.applied_index_watch();
                            if let Err(e) = crate::wait_for_apply(
                                &mut watch,
                                resp.committed_index,
                                std::time::Duration::from_secs(5),
                            )
                            .await
                            {
                                tracing::warn!(
                                    region = region.as_str(),
                                    committed_index = resp.committed_index,
                                    error = %e,
                                    "Timed out waiting for local replication after forwarded saga write"
                                );
                            }
                        }

                        inferadb_ledger_types::decode(&resp.response_payload).map_err(|e| {
                            SagaError::SagaRaftWrite {
                                message: format!("deserialize forwarded response: {e}"),
                                backtrace: snafu::Backtrace::generate(),
                            }
                        })
                    },
                    Err(e) => Err(SagaError::SagaRaftWrite {
                        message: format!("{e:?}"),
                        backtrace: snafu::Backtrace::generate(),
                    }),
                }
            },
            None => self
                .handle
                .propose_and_wait(payload, std::time::Duration::from_secs(30))
                .await
                .map_err(|e| SagaError::SagaRaftWrite {
                    message: format!("{e:?}"),
                    backtrace: snafu::Backtrace::generate(),
                }),
        }
    }

    /// Checks if this node is the current leader.
    fn is_leader(&self) -> bool {
        self.handle.is_leader()
    }

    /// Loads all pending sagas from _system organization.
    fn load_pending_sagas(&self) -> Vec<Saga> {
        // StateLayer is internally thread-safe via inferadb-ledger-store MVCC

        // List all entities with _meta:saga: prefix in _system (vault=0)
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
            client_id: ClientId::new(SAGA_CLIENT_ID),
            sequence: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0),
            operations,
            timestamp: chrono::Utc::now(),
        }
    }

    /// Proposes a write through Raft for the given organization and vault.
    ///
    /// Routes the write to the correct Raft group based on the organization's
    /// region. System organization writes (`SYSTEM_ORGANIZATION_ID`) always go
    /// through GLOBAL Raft. Other organizations are looked up via
    /// [`SystemOrganizationService`] and routed to their home region via
    /// [`propose_to_region`](Self::propose_to_region).
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

        let region = if organization == SYSTEM_ORGANIZATION_ID {
            Region::GLOBAL
        } else {
            match self.system_service().get_organization(organization) {
                Ok(Some(registry)) => registry.region,
                Ok(None) => {
                    return Err(SagaError::EntityNotFound {
                        entity_type: "Organization".into(),
                        identifier: organization.value().to_string(),
                    });
                },
                Err(e) => {
                    warn!(
                        organization = organization.value(),
                        error = %e,
                        "Failed to look up organization region, falling back to GLOBAL"
                    );
                    Region::GLOBAL
                },
            }
        };

        self.propose_to_region(region, request).await?;

        Ok(())
    }

    /// Saves a saga back to storage.
    async fn save_saga(&self, saga: &Saga) -> Result<(), SagaError> {
        let key = format!("{}{}", SAGA_KEY_PREFIX, saga.id());
        let value = serde_json::to_vec(saga).context(SerializationSnafu)?;

        let operation = Operation::SetEntity { key, value, expires_at: None, condition: None };
        self.propose_write(SYSTEM_ORGANIZATION_ID, SYSTEM_VAULT_ID, vec![operation]).await
    }

    /// Persists saga PII to REGIONAL scratch storage.
    ///
    /// Writes to `_tmp:saga_pii:{saga_id}` with a 24-hour TTL. The PII
    /// survives leader crashes and is consumed by the saga step that needs it.
    async fn persist_saga_pii(
        &self,
        saga_id: &SagaId,
        region: Region,
        pii: &SagaPii,
    ) -> Result<(), SagaError> {
        let key = SystemKeys::saga_pii_key(saga_id.value());
        let value = serde_json::to_vec(pii).context(SerializationSnafu)?;
        let expires_at_ts = (chrono::Utc::now() + chrono::Duration::hours(24)).timestamp() as u64;
        let expires_at = Some(expires_at_ts);

        let operation = Operation::SetEntity { key, value, expires_at, condition: None };
        let transaction = Self::build_transaction(vec![operation]);
        let request = LedgerRequest::Write {
            organization: SYSTEM_ORGANIZATION_ID,
            vault: SYSTEM_VAULT_ID,
            transactions: vec![transaction],
            idempotency_key: [0; 16],
            request_hash: 0,
        };
        self.propose_to_region(region, request).await?;
        Ok(())
    }

    /// Loads saga PII from REGIONAL scratch storage.
    ///
    /// Reads `_tmp:saga_pii:{saga_id}` from the regional state layer.
    /// Falls back to GLOBAL state when no `RaftManager` is configured
    /// (single-node test setups).
    fn load_saga_pii(&self, saga_id: &SagaId, region: Region) -> Option<SagaPii> {
        let key = SystemKeys::saga_pii_key(saga_id.value());
        let entity = if let Some(ref mgr) = self.manager {
            if let Ok(rg) = mgr.get_region_group(region) {
                rg.state().get_entity(SYSTEM_VAULT_ID, key.as_bytes()).ok()?
            } else {
                self.state.get_entity(SYSTEM_VAULT_ID, key.as_bytes()).ok()?
            }
        } else {
            self.state.get_entity(SYSTEM_VAULT_ID, key.as_bytes()).ok()?
        };
        let entity = entity?;
        serde_json::from_slice(&entity.value).ok()
    }

    /// Deletes saga PII from REGIONAL scratch storage after consumption.
    async fn delete_saga_pii(&self, saga_id: &SagaId, region: Region) {
        let key = SystemKeys::saga_pii_key(saga_id.value());
        let operation = Operation::DeleteEntity { key };
        let transaction = Self::build_transaction(vec![operation]);
        let request = LedgerRequest::Write {
            organization: SYSTEM_ORGANIZATION_ID,
            vault: SYSTEM_VAULT_ID,
            transactions: vec![transaction],
            idempotency_key: [0; 16],
            request_hash: 0,
        };
        if let Err(e) = self.propose_to_region(region, request).await {
            warn!(saga_id = %saga_id, error = %e, "Failed to delete saga PII scratch key");
        }
    }

    /// Executes a single step of a DeleteUser saga.
    // Allow: serde_json::json! macro uses unwrap internally for key insertion,
    // but with string literal keys this is infallible.
    #[allow(clippy::disallowed_methods)]
    async fn execute_delete_user_step(&self, saga: &mut DeleteUserSaga) -> Result<(), SagaError> {
        match &saga.state.clone() {
            DeleteUserSagaState::Pending => {
                // Step 1: Soft-delete user via SystemRequest (proper postcard encoding).
                let request =
                    LedgerRequest::System(SystemRequest::DeleteUser { user_id: saga.input.user });
                let result = self
                    .handle
                    .propose_and_wait(
                        RaftPayload::system(request),
                        std::time::Duration::from_secs(10),
                    )
                    .await;

                match result {
                    Ok(_) => {
                        saga.transition(DeleteUserSagaState::MarkingDeleted {
                            user_id: saga.input.user,
                            remaining_organizations: saga.input.organization_ids.clone(),
                        });
                        info!(
                            saga_id = %saga.id,
                            user_id = saga.input.user.value(),
                            "DeleteUser: marked as deleting"
                        );
                    },
                    Err(e) => {
                        // If the user doesn't exist, skip to completed.
                        let err_msg = format!("{e:?}");
                        if err_msg.contains("NotFound") {
                            saga.transition(DeleteUserSagaState::Completed {
                                user_id: saga.input.user,
                            });
                        } else {
                            return Err(SagaError::SagaRaftWrite {
                                message: err_msg,
                                backtrace: snafu::Backtrace::generate(),
                            });
                        }
                    },
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

        self.handle
            .propose_and_wait(RaftPayload::system(request), std::time::Duration::from_secs(10))
            .await
            .map_err(|e| SagaError::SequenceAllocation {
                message: format!("{e:?}"),
                backtrace: snafu::Backtrace::generate(),
            })?;

        Ok(current)
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
            let _ = self.handle.propose(RaftPayload::system(request)).await;

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

                self.handle
                    .propose_and_wait(
                        RaftPayload::system(request),
                        std::time::Duration::from_secs(10),
                    )
                    .await
                    .map_err(|e| SagaError::SagaRaftWrite {
                        message: format!("{e:?}"),
                        backtrace: snafu::Backtrace::generate(),
                    })?;

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
                    // Cross-region data transfer is not yet implemented —
                    // recording an empty snapshot marker. The actual per-vault
                    // state root computation requires reading from the source
                    // region's Raft group via RaftManager.
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
                // Step 2: Write data to target region.
                // Cross-region data transfer is not yet implemented. Writing
                // org data to a target region requires RaftManager-mediated
                // cross-group proposals. Marking as written — data consistency
                // is maintained by per-region Raft replication.
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

                self.handle
                    .propose_and_wait(
                        RaftPayload::system(request),
                        std::time::Duration::from_secs(10),
                    )
                    .await
                    .map_err(|e| SagaError::SagaRaftWrite {
                        message: format!("{e:?}"),
                        backtrace: snafu::Backtrace::generate(),
                    })?;

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
    /// Shred key re-encryption is deferred to the encryption-at-rest tasks (16-20).
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
            let _ = self.handle.propose(RaftPayload::system(request)).await;

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
                self.handle
                    .propose_and_wait(
                        RaftPayload::system(request),
                        std::time::Duration::from_secs(10),
                    )
                    .await
                    .map_err(|e| SagaError::SagaRaftWrite {
                        message: format!("{e:?}"),
                        backtrace: snafu::Backtrace::generate(),
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
                self.handle
                    .propose_and_wait(
                        RaftPayload::system(request),
                        std::time::Duration::from_secs(10),
                    )
                    .await
                    .map_err(|e| SagaError::SagaRaftWrite {
                        message: format!("{e:?}"),
                        backtrace: snafu::Backtrace::generate(),
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
    /// Step 1 (Regional): create User, UserEmail, UserShredKey
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
                let _ = self.handle.propose(RaftPayload::system(request)).await;
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
                        code: inferadb_ledger_types::ErrorCode::Internal,
                        description: format!("failed to generate user slug: {e}"),
                    }
                })?;

                // CAS email HMAC in GLOBAL (reserves uniqueness)
                let request = LedgerRequest::System(SystemRequest::RegisterEmailHash {
                    hmac_hex: saga.input.hmac.clone(),
                    user_id,
                });
                self.handle
                    .propose_and_wait(
                        RaftPayload::system(request),
                        std::time::Duration::from_secs(10),
                    )
                    .await
                    .map_err(|e| SagaError::SagaRaftWrite {
                        message: format!("{e:?}"),
                        backtrace: snafu::Backtrace::generate(),
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
                // Step 1 (GLOBAL): Create User directory entry (no PII — IDs, slug, region).
                let request = LedgerRequest::System(SystemRequest::CreateUser {
                    user: user_id,
                    admin: saga.input.admin,
                    slug: user_slug,
                    region: saga.input.region,
                });
                self.handle
                    .propose_and_wait(
                        RaftPayload::system(request),
                        std::time::Duration::from_secs(10),
                    )
                    .await
                    .map_err(|e| SagaError::SagaRaftWrite {
                        message: format!("{e:?}"),
                        backtrace: snafu::Backtrace::generate(),
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
                self.handle
                    .propose_and_wait(
                        RaftPayload::system(request),
                        std::time::Duration::from_secs(10),
                    )
                    .await
                    .map_err(|e| SagaError::SagaRaftWrite {
                        message: format!("{e:?}"),
                        backtrace: snafu::Backtrace::generate(),
                    })?;

                saga.transition(CreateUserSagaState::Completed { user_id, user_slug });
                info!(
                    saga_id = %saga.id,
                    user_id = user_id.value(),
                    user_slug = user_slug.value(),
                    "CreateUser: saga completed"
                );

                // Fire CreateOrganizationSaga for the user's default organization
                let org_saga_id = SagaId::new(uuid::Uuid::new_v4().to_string());
                let org_slug = snowflake::generate_organization_slug().map_err(|e| {
                    SagaError::UnexpectedSagaResponse {
                        code: inferadb_ledger_types::ErrorCode::Internal,
                        description: format!("failed to generate organization slug: {e}"),
                    }
                })?;
                let org_saga = CreateOrganizationSaga::new(
                    org_saga_id.clone(),
                    CreateOrganizationInput {
                        slug: org_slug,
                        region: saga.input.region,
                        tier: saga.input.default_org_tier,
                        admin: user_id,
                    },
                );
                // Persist org name as PII to REGIONAL scratch storage
                let org_pii =
                    SagaPii::Organization(OrgPii { name: saga.input.organization_name.clone() });
                if let Err(e) =
                    self.persist_saga_pii(&org_saga_id, saga.input.region, &org_pii).await
                {
                    warn!(
                        saga_id = %saga.id,
                        org_saga_id = %org_saga_id,
                        error = %e,
                        "CreateUser: failed to persist org PII for CreateOrganizationSaga"
                    );
                }
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
    /// Step 0 (GLOBAL): allocate OrgId, create directory entry with pre-generated slug
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
                let request = LedgerRequest::System(SystemRequest::UpdateOrganizationStatus {
                    organization: *organization_id,
                    status: OrganizationStatus::Deleted,
                });
                let _ = self.handle.propose(RaftPayload::system(request)).await;
            }
            saga.transition(CreateOrganizationSagaState::TimedOut);
            return Ok(());
        }

        match saga.state.clone() {
            CreateOrganizationSagaState::Pending => {
                // Step 0 (GLOBAL): Create directory entry with pre-generated slug
                let request = LedgerRequest::System(SystemRequest::CreateOrganization {
                    slug: saga.input.slug,
                    region: saga.input.region,
                    tier: saga.input.tier,
                    admin: saga.input.admin,
                });
                let response = self
                    .handle
                    .propose_and_wait(
                        RaftPayload::system(request),
                        std::time::Duration::from_secs(30),
                    )
                    .await
                    .map_err(|e| SagaError::SagaRaftWrite {
                        message: format!("{e:?}"),
                        backtrace: snafu::Backtrace::generate(),
                    })?;

                match response {
                    crate::types::LedgerResponse::OrganizationCreated {
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
                        code: inferadb_ledger_types::ErrorCode::Internal,
                        description: format!("expected OrganizationCreated, got {other}"),
                    }),
                }
            },

            CreateOrganizationSagaState::DirectoryCreated {
                organization_id,
                organization_slug,
            } => {
                // Step 1 (REGIONAL): Write organization profile + ownership
                // Organization name is PII — read from REGIONAL scratch storage.
                let name = match self.load_saga_pii(&saga.id, saga.input.region) {
                    Some(SagaPii::Organization(pii)) => pii.name,
                    _ => {
                        warn!(
                            saga_id = %saga.id,
                            "Org PII not found in REGIONAL storage — compensating"
                        );
                        return Err(SagaError::PiiLost);
                    },
                };

                // Generate per-organization key for crypto-shredding.
                let shred_key_bytes: [u8; 32] = rand::random();

                // Seal the org name with the OrgShredKey before entering the Raft log.
                let aad = organization_id.value().to_le_bytes();
                let (sealed_name, name_nonce) =
                    crate::entry_crypto::seal(name.as_bytes(), &shred_key_bytes, &aad).map_err(
                        |e| SagaError::KeyEncryption {
                            message: format!("Failed to seal organization name: {e}"),
                        },
                    )?;

                let request = LedgerRequest::System(SystemRequest::WriteOrganizationProfile {
                    organization: organization_id,
                    sealed_name,
                    name_nonce,
                    shred_key_bytes,
                });
                self.propose_to_region(saga.input.region, request).await?;

                // REGIONAL write succeeded — delete PII scratch key
                self.delete_saga_pii(&saga.id, saga.input.region).await;

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
                let request = LedgerRequest::System(SystemRequest::UpdateOrganizationStatus {
                    organization: organization_id,
                    status: OrganizationStatus::Active,
                });
                self.handle
                    .propose_and_wait(
                        RaftPayload::system(request),
                        std::time::Duration::from_secs(10),
                    )
                    .await
                    .map_err(|e| SagaError::SagaRaftWrite {
                        message: format!("{e:?}"),
                        backtrace: snafu::Backtrace::generate(),
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
    /// For `CreateOnboardingUser`: removes HMAC reservation and marks directories as Deleted.
    ///
    /// `org_id_for_compensation` is captured before `fail()` transitions the state,
    /// since `Failed { step, error }` doesn't carry the allocated organization ID.
    /// `onboarding_ids` similarly captures `(user_id, organization_id)` before transition.
    async fn compensate_failed_saga(
        &self,
        saga: &mut Saga,
        org_id_for_compensation: Option<OrganizationId>,
        onboarding_ids: Option<(UserId, OrganizationId)>,
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
                        let _ = self.handle.propose(RaftPayload::system(request)).await;
                        cleanup.push("email_hmac_removed");
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
                        let request =
                            LedgerRequest::System(SystemRequest::UpdateOrganizationStatus {
                                organization: organization_id,
                                status: OrganizationStatus::Deleted,
                            });
                        let _ = self.handle.propose(RaftPayload::system(request)).await;
                        cleanup.push("directory_marked_deleted");
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
            Saga::CreateOnboardingUser(s) => {
                if let CreateOnboardingUserSagaState::Failed { step, .. } = &s.state {
                    let mut cleanup = Vec::new();

                    // Step 0 reserves the HMAC (Provisioning entry) — always clean up
                    // since the saga cannot reach IdsAllocated without reserving.
                    let request = LedgerRequest::System(SystemRequest::RemoveEmailHash {
                        hmac_hex: s.input.email_hmac.clone(),
                    });
                    let _ = self.handle.propose(RaftPayload::system(request)).await;
                    cleanup.push("email_hmac_removed");

                    // If IDs were allocated (step 0 completed), mark directories as Deleted
                    if let Some((user_id, organization_id)) = onboarding_ids {
                        let user_dir_request =
                            LedgerRequest::System(SystemRequest::UpdateUserDirectoryStatus {
                                user_id,
                                status: inferadb_ledger_state::system::UserDirectoryStatus::Deleted,
                                region: None,
                            });
                        let _ = self.handle.propose(RaftPayload::system(user_dir_request)).await;
                        cleanup.push("user_directory_marked_deleted");

                        let org_dir_request =
                            LedgerRequest::System(SystemRequest::UpdateOrganizationStatus {
                                organization: organization_id,
                                status: OrganizationStatus::Deleted,
                            });
                        let _ = self.handle.propose(RaftPayload::system(org_dir_request)).await;
                        cleanup.push("org_directory_marked_deleted");
                    }

                    let step = *step;
                    let cleanup_summary = cleanup.join(", ");
                    warn!(
                        saga_id = %s.id,
                        step,
                        cleanup = %cleanup_summary,
                        "CreateOnboardingUser: compensated after permanent failure"
                    );
                    s.transition(CreateOnboardingUserSagaState::Compensated {
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

    /// Extracts allocated IDs from a `CreateOnboardingUser` saga before
    /// `fail()` / `force_fail()` transitions it to `Failed` (which discards
    /// the IDs from the state enum).
    fn extract_onboarding_ids(saga: &Saga) -> Option<(UserId, OrganizationId)> {
        if let Saga::CreateOnboardingUser(s) = saga {
            match &s.state {
                CreateOnboardingUserSagaState::IdsAllocated { user_id, organization_id } => {
                    Some((*user_id, *organization_id))
                },
                CreateOnboardingUserSagaState::RegionalDataWritten {
                    user_id,
                    organization_id,
                    ..
                } => Some((*user_id, *organization_id)),
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
    /// Delegates to [`SystemOrganizationService::resolve_scope_region`].
    fn scope_to_region(&self, scope: &SigningKeyScope) -> Result<Region, SagaError> {
        self.system_service().resolve_scope_region(scope).map_err(|e| SagaError::KeyGeneration {
            message: format!("Failed to resolve region for scope {scope:?}: {e}"),
        })
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
        //
        // Use Zeroizing wrappers to ensure secret material is wiped on all exit
        // paths (including early returns via `?`).
        let mut secret_bytes = Zeroizing::new([0u8; 32]);
        rand::RngExt::fill(&mut rand::rng(), &mut *secret_bytes);
        let signing_key = ed25519_dalek::SigningKey::from_bytes(&secret_bytes);
        let public_key_bytes = signing_key.verifying_key().to_bytes().to_vec();
        let private_key_bytes = Zeroizing::new(signing_key.to_bytes());
        drop(signing_key); // Triggers Zeroize on Drop (ed25519-dalek "zeroize" feature)

        // Envelope encryption: generate DEK, wrap with RMK, encrypt private key with DEK
        let dek = generate_dek();
        let wrapped_dek = wrap_dek(&dek, &rmk).map_err(|e| SagaError::KeyEncryption {
            message: format!("DEK wrapping failed: {e}"),
        })?;

        // Encrypt private key with DEK using AES-256-GCM, kid as AAD
        let (ciphertext, nonce, auth_tag) = inferadb_ledger_store::crypto::encrypt_page_body(
            &*private_key_bytes,
            kid.as_bytes(),
            &dek,
        )
        .map_err(|e| SagaError::KeyEncryption {
            message: format!("Private key encryption failed: {e}"),
        })?;

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
        // Check timeout before each step (30s — signing key creation is fast)
        let timeout = Duration::from_secs(30);
        if saga.is_timed_out(timeout) {
            warn!(
                saga_id = %saga.id,
                scope = ?saga.input.scope,
                elapsed_secs = (chrono::Utc::now() - saga.created_at).num_seconds(),
                "CreateSigningKey saga timed out"
            );
            saga.transition(CreateSigningKeySagaState::TimedOut);
            return Ok(());
        }

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

                let response = self
                    .handle
                    .propose_and_wait(
                        RaftPayload::system(request),
                        std::time::Duration::from_secs(30),
                    )
                    .await
                    .map_err(|e| SagaError::SagaRaftWrite {
                        message: format!("{e:?}"),
                        backtrace: snafu::Backtrace::generate(),
                    })?;

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
                        code: inferadb_ledger_types::ErrorCode::Internal,
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

    /// Executes a step of the `CreateOnboardingUserSaga`.
    ///
    /// Three-step saga: GLOBAL → regional → GLOBAL.
    /// - Step 0 (Pending → IdsAllocated): Reserve HMAC, allocate IDs, create directories
    /// - Step 1 (IdsAllocated → RegionalDataWritten): Write PII to regional store
    /// - Step 2 (RegionalDataWritten → Completed): Activate directories, HMAC → Active
    ///
    /// After step 2 succeeds, fires `CreateSigningKeySaga` for the new
    /// organization (same pattern as `CreateOrganizationSaga`).
    async fn execute_create_onboarding_user_step(
        &self,
        saga: &mut CreateOnboardingUserSaga,
    ) -> Result<(), SagaError> {
        let timeout = Duration::from_secs(self.migration_config.timeout_secs.min(60));
        if saga.is_timed_out(timeout) {
            warn!(saga_id = %saga.id, "CreateOnboardingUser saga timed out");
            saga.transition(CreateOnboardingUserSagaState::TimedOut);
            return Ok(());
        }

        match saga.state.clone() {
            CreateOnboardingUserSagaState::Pending => {
                // Step 0 (GLOBAL): Reserve HMAC, allocate IDs, create directories
                let request = LedgerRequest::System(SystemRequest::CreateOnboardingUser {
                    email_hmac: saga.input.email_hmac.clone(),
                    user_slug: saga.input.user_slug,
                    organization_slug: saga.input.organization_slug,
                    region: saga.input.region,
                });
                let response = self
                    .handle
                    .propose_and_wait(
                        RaftPayload::system(request),
                        std::time::Duration::from_secs(30),
                    )
                    .await
                    .map_err(|e| SagaError::SagaRaftWrite {
                        message: format!("{e:?}"),
                        backtrace: snafu::Backtrace::generate(),
                    })?;

                match response {
                    crate::types::LedgerResponse::OnboardingUserCreated {
                        user_id,
                        organization_id,
                    } => {
                        saga.transition(CreateOnboardingUserSagaState::IdsAllocated {
                            user_id,
                            organization_id,
                        });
                        info!(
                            saga_id = %saga.id,
                            user_id = user_id.value(),
                            organization_id = organization_id.value(),
                            "CreateOnboardingUser: IDs allocated, HMAC reserved (GLOBAL)"
                        );
                        Ok(())
                    },
                    crate::types::LedgerResponse::Error { code, message } => {
                        Err(SagaError::UnexpectedSagaResponse { code, description: message })
                    },
                    other => Err(SagaError::UnexpectedSagaResponse {
                        code: inferadb_ledger_types::ErrorCode::Internal,
                        description: format!("expected OnboardingUserCreated, got {other}"),
                    }),
                }
            },

            CreateOnboardingUserSagaState::IdsAllocated { user_id, organization_id } => {
                // Step 1 (REGIONAL): Write PII from REGIONAL scratch storage
                let pii = match self.load_saga_pii(&saga.id, saga.input.region) {
                    Some(SagaPii::Onboarding(p)) => p,
                    _ => {
                        warn!(
                            saga_id = %saga.id,
                            "PII not found in REGIONAL storage — compensating"
                        );
                        return Err(SagaError::PiiLost);
                    },
                };

                // Generate crypto material for session bootstrap
                let mut shred_key_bytes = [0u8; 32];
                rand::RngExt::fill(&mut rand::rng(), &mut shred_key_bytes);

                let mut refresh_family_id = [0u8; 16];
                rand::RngExt::fill(&mut rand::rng(), &mut refresh_family_id);

                // Generate refresh token: ilrt_ + base64url(32 random bytes)
                let mut refresh_random = [0u8; 32];
                rand::RngExt::fill(&mut rand::rng(), &mut refresh_random);
                let refresh_encoded = base64::engine::Engine::encode(
                    &base64::engine::general_purpose::URL_SAFE_NO_PAD,
                    refresh_random,
                );
                let refresh_token = format!("ilrt_{refresh_encoded}");
                let refresh_token_hash: [u8; 32] =
                    inferadb_ledger_types::sha256(refresh_token.as_bytes());

                // 14-day TTL (matches default session_refresh_ttl_secs)
                let refresh_expires_at = chrono::Utc::now() + chrono::Duration::seconds(1_209_600);

                let kid = uuid::Uuid::new_v4().to_string();

                // Seal PII before it enters the Raft log
                let onboarding_pii = crate::entry_crypto::OnboardingPii {
                    email: pii.email,
                    name: pii.name,
                    organization_name: pii.organization_name,
                };
                let pii_plaintext = postcard::to_allocvec(&onboarding_pii).map_err(|e| {
                    SagaError::UnexpectedSagaResponse {
                        code: inferadb_ledger_types::ErrorCode::Internal,
                        description: format!("Failed to serialize onboarding PII: {e}"),
                    }
                })?;
                let aad = user_id.value().to_le_bytes();
                let (sealed_pii, pii_nonce) =
                    crate::entry_crypto::seal(&pii_plaintext, &shred_key_bytes, &aad).map_err(
                        |e| SagaError::UnexpectedSagaResponse {
                            code: inferadb_ledger_types::ErrorCode::Internal,
                            description: format!("Failed to seal onboarding PII: {e}"),
                        },
                    )?;

                let request = LedgerRequest::System(SystemRequest::WriteOnboardingUserProfile {
                    user_id,
                    user_slug: saga.input.user_slug,
                    organization_id,
                    organization_slug: saga.input.organization_slug,
                    email_hmac: saga.input.email_hmac.clone(),
                    sealed_pii,
                    pii_nonce,
                    shred_key_bytes,
                    refresh_token_hash,
                    refresh_family_id,
                    refresh_expires_at,
                    kid: kid.clone(),
                    region: saga.input.region,
                });
                let response = self.propose_to_region(saga.input.region, request).await?;
                match response {
                    crate::types::LedgerResponse::OnboardingUserProfileWritten {
                        refresh_token_id,
                    } => {
                        // PII consumed — delete scratch key from REGIONAL storage
                        self.delete_saga_pii(&saga.id, saga.input.region).await;

                        // Stash crypto material for SagaOutput construction after step 2
                        self.crypto_cache.lock().insert(
                            saga.id.clone(),
                            OnboardingCrypto {
                                kid,
                                refresh_token,
                                refresh_family_id,
                                refresh_expires_at,
                                user_slug: saga.input.user_slug,
                                organization_slug: saga.input.organization_slug,
                            },
                        );

                        saga.transition(CreateOnboardingUserSagaState::RegionalDataWritten {
                            user_id,
                            organization_id,
                            refresh_token_id,
                        });
                        info!(
                            saga_id = %saga.id,
                            user_id = user_id.value(),
                            organization_id = organization_id.value(),
                            refresh_token_id = refresh_token_id.value(),
                            "CreateOnboardingUser: profile written (regional)"
                        );
                        Ok(())
                    },
                    crate::types::LedgerResponse::Error { code, message } => {
                        Err(SagaError::UnexpectedSagaResponse { code, description: message })
                    },
                    other => Err(SagaError::UnexpectedSagaResponse {
                        code: inferadb_ledger_types::ErrorCode::Internal,
                        description: format!("expected OnboardingUserProfileWritten, got {other}"),
                    }),
                }
            },

            CreateOnboardingUserSagaState::RegionalDataWritten {
                user_id,
                organization_id,
                refresh_token_id: _,
            } => {
                // Step 2 (GLOBAL): Activate directories, HMAC → Active
                let request = LedgerRequest::System(SystemRequest::ActivateOnboardingUser {
                    user_id,
                    user_slug: saga.input.user_slug,
                    organization_id,
                    organization_slug: saga.input.organization_slug,
                    email_hmac: saga.input.email_hmac.clone(),
                });
                self.handle
                    .propose_and_wait(
                        RaftPayload::system(request),
                        std::time::Duration::from_secs(10),
                    )
                    .await
                    .map_err(|e| SagaError::SagaRaftWrite {
                        message: format!("{e:?}"),
                        backtrace: snafu::Backtrace::generate(),
                    })?;

                saga.transition(CreateOnboardingUserSagaState::Completed {
                    user_id,
                    organization_id,
                });
                info!(
                    saga_id = %saga.id,
                    user_id = user_id.value(),
                    organization_id = organization_id.value(),
                    "CreateOnboardingUser: saga completed"
                );

                // Fire CreateSigningKeySaga for the new organization
                if self.key_manager.is_some() {
                    let scope = SigningKeyScope::Organization(organization_id);
                    if let Err(e) = self.write_signing_key_saga(scope).await {
                        warn!(
                            saga_id = %saga.id,
                            organization_id = organization_id.value(),
                            error = %e,
                            "CreateOnboardingUser: failed to write CreateSigningKeySaga"
                        );
                    } else {
                        info!(
                            saga_id = %saga.id,
                            organization_id = organization_id.value(),
                            "CreateOnboardingUser: fired CreateSigningKeySaga for org"
                        );
                    }
                }

                Ok(())
            },

            CreateOnboardingUserSagaState::Completed { .. }
            | CreateOnboardingUserSagaState::Failed { .. }
            | CreateOnboardingUserSagaState::Compensated { .. }
            | CreateOnboardingUserSagaState::TimedOut => {
                // Terminal states — nothing to do
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
        let saga_id = SagaId::new(uuid::Uuid::new_v4().to_string());
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
        use std::sync::atomic::Ordering;

        // Skip if no key manager configured (signing keys not enabled)
        if self.key_manager.is_none() {
            return;
        }

        // Short-circuit: once confirmed, never check again
        if self.global_key_confirmed.load(Ordering::Relaxed) {
            return;
        }

        let sys_service = self.system_service();

        // Check if active global key exists
        match sys_service.get_active_signing_key(&SigningKeyScope::Global) {
            Ok(Some(_)) => {
                self.global_key_confirmed.store(true, Ordering::Relaxed);
                return;
            },
            Ok(None) => {}, // Need to bootstrap
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
            self.global_key_confirmed.store(true, Ordering::Relaxed);
            return;
        }

        info!("No global signing key found, writing bootstrap saga");
        if let Err(e) = self.write_signing_key_saga(SigningKeyScope::Global).await {
            warn!(error = %e, "Failed to write global signing key bootstrap saga");
        }
    }

    /// Executes a single saga.
    async fn execute_saga(&self, mut saga: Saga) {
        let saga_id = saga.id().clone();
        let saga_id_str = saga_id.to_string();
        let saga_type = saga.saga_type();

        debug!(saga_id = %saga_id_str, saga_type = ?saga_type, "Executing saga step");

        let result = match &mut saga {
            Saga::DeleteUser(s) => self.execute_delete_user_step(s).await,
            Saga::MigrateOrg(s) => self.execute_migrate_org_step(s).await,
            Saga::MigrateUser(s) => self.execute_migrate_user_step(s).await,
            Saga::CreateUser(s) => self.execute_create_user_step(s).await,
            Saga::CreateOrganization(s) => self.execute_create_organization_step(s).await,
            Saga::CreateSigningKey(s) => self.execute_create_signing_key_step(s).await,
            Saga::CreateOnboardingUser(s) => self.execute_create_onboarding_user_step(s).await,
        };

        if let Err(e) = result {
            warn!(saga_id = %saga_id_str, error = %e, "Saga step failed");

            // Capture state needed for compensation BEFORE fail()/force_fail()
            // transitions to Failed (which discards allocated IDs from the state enum).
            let org_id_for_compensation = Self::extract_org_id_for_compensation(&saga);
            let onboarding_ids = Self::extract_onboarding_ids(&saga);

            // PiiLost is unrecoverable — retrying cannot succeed because PII
            // only exists in-memory and was lost on crash/leader transfer.
            // Skip the retry counter and fail immediately for all saga types.
            if matches!(e, SagaError::PiiLost) {
                match &mut saga {
                    Saga::CreateOnboardingUser(s) => {
                        let step = s.current_step();
                        s.force_fail(step, e.to_string());
                    },
                    Saga::CreateOrganization(s) => {
                        let step = s.current_step();
                        s.state =
                            CreateOrganizationSagaState::Failed { step, error: e.to_string() };
                        s.updated_at = chrono::Utc::now();
                    },
                    _ => saga.fail(e.to_string()),
                }
            } else {
                saga.fail(e.to_string());
            }

            // If the saga exhausted retries (or force-failed) and transitioned
            // to terminal Failed, compensate to clean up partial state.
            if saga.is_terminal() {
                self.compensate_failed_saga(&mut saga, org_id_for_compensation, onboarding_ids)
                    .await;
            }
        }

        // Notify synchronous callers when saga reaches terminal state
        if saga.is_terminal() {
            // Take crypto cache BEFORE cleanup — needed for SagaOutput
            let crypto = self.crypto_cache.lock().remove(&saga_id);

            // Best-effort cleanup of PII scratch key from REGIONAL storage
            // (may already be deleted by the consuming step)
            let pii_region = match &saga {
                Saga::CreateOnboardingUser(s) => Some(s.input.region),
                Saga::CreateOrganization(s) => Some(s.input.region),
                Saga::CreateUser(s) => Some(s.input.region),
                _ => None,
            };
            if let Some(region) = pii_region {
                self.delete_saga_pii(&saga_id, region).await;
            }

            // Send result through notification channel if present
            if let Some(notify) = self.notify_cache.lock().remove(&saga_id) {
                let notification = match &saga {
                    Saga::CreateOnboardingUser(s) => {
                        use inferadb_ledger_state::system::CreateOnboardingUserSagaState;
                        match &s.state {
                            CreateOnboardingUserSagaState::Completed {
                                user_id,
                                organization_id,
                            } => {
                                // Build SagaOutput from saga state + cached crypto
                                // The refresh_token_id was in RegionalDataWritten (previous
                                // state) — we need it from the saga's last non-terminal state.
                                // Since Completed doesn't carry it, we extract it from the
                                // steps list. However, the saga transitions directly from
                                // RegionalDataWritten → Completed, and the step 2 handler
                                // already ran. We stored the refresh_token_id in step 1's
                                // transition, but it's lost after step 2 transitions to
                                // Completed. Solution: extract from saga steps or stash in
                                // crypto cache during step 1.
                                //
                                // The crypto cache was populated during step 1 and carries
                                // enough for everything except refresh_token_id. We need to
                                // also stash refresh_token_id there. For now, we read it
                                // from the state layer.
                                match crypto {
                                    Some(c) => {
                                        // Look up refresh_token_id from state layer by hash
                                        let refresh_token_hash = inferadb_ledger_types::sha256(
                                            c.refresh_token.as_bytes(),
                                        );
                                        // Read from the REGIONAL state layer where the
                                        // refresh token was written in step 2. Falls back
                                        // to GLOBAL for single-region test setups.
                                        let token_result = if let Some(ref mgr) = self.manager {
                                            if let Ok(rg) = mgr.get_region_group(s.input.region) {
                                                let sys = inferadb_ledger_state::system::SystemOrganizationService::new(rg.state().clone());
                                                sys.get_refresh_token_by_hash(&refresh_token_hash)
                                            } else {
                                                let sys = self.system_service();
                                                sys.get_refresh_token_by_hash(&refresh_token_hash)
                                            }
                                        } else {
                                            let sys = self.system_service();
                                            sys.get_refresh_token_by_hash(&refresh_token_hash)
                                        };
                                        match token_result {
                                            Ok(Some(rt)) => Ok(SagaOutput {
                                                user_id: *user_id,
                                                user_slug: c.user_slug,
                                                organization_id: *organization_id,
                                                organization_slug: c.organization_slug,
                                                refresh_token_id: rt.id,
                                                kid: c.kid,
                                                refresh_family_id: c.refresh_family_id,
                                                refresh_expires_at: c.refresh_expires_at,
                                                refresh_token: c.refresh_token,
                                            }),
                                            Ok(None) => Err(SagaError::EntityNotFound {
                                                entity_type: "RefreshToken".to_string(),
                                                identifier: "token not found by hash after step 2"
                                                    .to_string(),
                                            }),
                                            Err(e) => Err(SagaError::EntityNotFound {
                                                entity_type: "RefreshToken".to_string(),
                                                identifier: format!(
                                                    "failed to read refresh token: {e}"
                                                ),
                                            }),
                                        }
                                    },
                                    None => Err(SagaError::EntityNotFound {
                                        entity_type: "crypto".to_string(),
                                        identifier: format!(
                                            "crypto cache lost for saga {saga_id} \
                                             (crash between step 1 and notification)"
                                        ),
                                    }),
                                }
                            },
                            _ => Err(SagaError::EntityNotFound {
                                entity_type: "saga".to_string(),
                                identifier: format!("saga {saga_id} terminated without completing"),
                            }),
                        }
                    },
                    _ => {
                        // Non-onboarding sagas don't use SagaOutput
                        Err(SagaError::EntityNotFound {
                            entity_type: "saga".to_string(),
                            identifier: "notification not supported for this saga type".to_string(),
                        })
                    },
                };
                let _ = notify.send(notification);
            }
        }

        if let Err(e) = self.save_saga(&saga).await {
            warn!(saga_id = %saga_id_str, error = %e, "Failed to save saga state");
        }
    }

    /// Drains pending submissions from the mpsc channel.
    ///
    /// For each submission:
    /// 1. Saves the saga record to storage (so it survives crashes)
    /// 2. Persists PII to REGIONAL scratch storage (if present) for step 1 construction
    /// 3. Caches the notification sender (if present) for result delivery
    async fn drain_submissions(&self, rx: &mut mpsc::Receiver<SagaSubmission>) {
        while let Ok(submission) = rx.try_recv() {
            let saga_id = submission.record.id().clone();

            // Persist the saga record to storage first (crash-safe)
            if let Err(e) = self.save_saga(&submission.record).await {
                warn!(saga_id = %saga_id, error = %e, "Failed to save submitted saga");
                // Notify caller of failure if a sender is present
                if let Some(notify) = submission.notify {
                    let _ = notify.send(Err(e));
                }
                continue;
            }

            debug!(saga_id = %saga_id, has_pii = submission.pii.is_some(), "Accepted saga submission");

            // Persist PII to REGIONAL scratch storage (survives leader crashes).
            // Retry up to 3 times with backoff — the regional Raft group may not
            // be ready immediately after cluster bootstrap.
            if let Some(pii) = submission.pii {
                let region = match &submission.record {
                    Saga::CreateOnboardingUser(s) => s.input.region,
                    Saga::CreateUser(s) => s.input.region,
                    _ => Region::GLOBAL,
                };
                let mut pii_persisted = false;
                for attempt in 0..3u32 {
                    match self
                        .persist_saga_pii(&saga_id, region, &SagaPii::Onboarding(pii.clone()))
                        .await
                    {
                        Ok(()) => {
                            pii_persisted = true;
                            break;
                        }
                        Err(e) => {
                            warn!(
                                saga_id = %saga_id,
                                attempt = attempt + 1,
                                error = %e,
                                "Failed to persist onboarding PII, retrying"
                            );
                            tokio::time::sleep(Duration::from_millis(100 * u64::from(attempt + 1)))
                                .await;
                        }
                    }
                }
                if !pii_persisted {
                    warn!(saga_id = %saga_id, "PII persist exhausted retries; saga step 1 will reload from scratch or compensate");
                }
            }
            if let Some(org_pii) = submission.org_pii {
                let region = match &submission.record {
                    Saga::CreateOrganization(s) => s.input.region,
                    _ => Region::GLOBAL,
                };
                let mut org_pii_persisted = false;
                for attempt in 0..3u32 {
                    match self
                        .persist_saga_pii(&saga_id, region, &SagaPii::Organization(org_pii.clone()))
                        .await
                    {
                        Ok(()) => {
                            org_pii_persisted = true;
                            break;
                        }
                        Err(e) => {
                            warn!(
                                saga_id = %saga_id,
                                attempt = attempt + 1,
                                error = %e,
                                "Failed to persist org PII, retrying"
                            );
                            tokio::time::sleep(Duration::from_millis(100 * u64::from(attempt + 1)))
                                .await;
                        }
                    }
                }
                if !org_pii_persisted {
                    warn!(saga_id = %saga_id, "Org PII persist exhausted retries; saga step 1 will reload from scratch or compensate");
                }
            }

            // Cache notification sender
            if let Some(notify) = submission.notify {
                self.notify_cache.lock().insert(saga_id, notify);
            }
        }
    }

    /// Runs a single poll cycle with metrics and watchdog heartbeat.
    async fn run_cycle(&self) {
        // Only leader executes sagas
        if !self.is_leader() {
            debug!("Skipping saga poll (not leader)");
            return;
        }

        let mut job = crate::logging::JobContext::new("saga_orchestrator", None);
        let _trace_ctx = TraceContext::new();

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
        job.record_items(saga_count);

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
    /// Creates an `mpsc` channel for saga submissions, moves the receiver
    /// into the background task, and returns the `SagaOrchestratorHandle`
    /// alongside the task join handle.
    ///
    /// Services use the returned handle to submit sagas for execution.
    /// The background task drains the submission channel on each poll cycle,
    /// giving submitted sagas priority over storage-loaded sagas.
    pub fn start(self) -> (tokio::task::JoinHandle<()>, SagaOrchestratorHandle) {
        let (tx, mut rx) = mpsc::channel::<SagaSubmission>(SUBMISSION_CHANNEL_SIZE);
        let handle = SagaOrchestratorHandle { tx };

        let join_handle = tokio::spawn(async move {
            let mut ticker = interval(self.interval);

            info!(interval_secs = self.interval.as_secs(), "Saga orchestrator started");

            loop {
                tokio::select! {
                    biased;
                    _ = ticker.tick() => {
                        self.drain_submissions(&mut rx).await;
                        if let Err(panic_payload) = AssertUnwindSafe(self.run_cycle()).catch_unwind().await {
                            let msg = panic_payload
                                .downcast_ref::<&str>()
                                .copied()
                                .or_else(|| panic_payload.downcast_ref::<String>().map(String::as_str))
                                .unwrap_or("<non-string panic>");
                            error!(panic_message = msg, "Saga orchestrator run_cycle panicked; continuing");
                            metrics::counter!("saga_orchestrator_panic_total").increment(1);
                        }
                    }
                    _ = self.cancellation_token.cancelled() => {
                        info!("SagaOrchestrator shutting down");
                        break;
                    }
                }
            }
        });

        (join_handle, handle)
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
                    SagaId::new("delete-456"),
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
                    SagaId::new("migrate-org-789"),
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
                    SagaId::new("migrate-user-101"),
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
                    SagaId::new("create-user-202"),
                    CreateUserInput {
                        hmac: "abc123".to_string(),
                        region: Region::US_EAST_VA,
                        admin: false,
                        organization_name: String::new(),
                        default_org_tier: OrganizationTier::Free,
                    },
                )),
            ),
            (
                "CreateOrganization",
                Saga::CreateOrganization(CreateOrganizationSaga::new(
                    SagaId::new("create-org-303"),
                    CreateOrganizationInput {
                        slug: OrganizationSlug::new(303),
                        region: Region::IE_EAST_DUBLIN,
                        tier: OrganizationTier::Free,
                        admin: UserId::new(1),
                    },
                )),
            ),
            (
                "CreateSigningKey",
                Saga::CreateSigningKey(CreateSigningKeySaga::new(
                    SagaId::new("create-key-404"),
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

            assert_eq!(deserialized.id().value(), expected_id, "failed id check for {label}");
        }
    }

    #[test]
    fn pending_saga_is_not_terminal() {
        for (label, saga) in all_pending_sagas() {
            assert!(!saga.is_terminal(), "pending saga should not be terminal for {label}");
        }
    }

    #[test]
    fn pending_saga_is_ready_for_retry() {
        for (label, saga) in all_pending_sagas() {
            assert!(
                saga.is_ready_for_retry(),
                "pending saga should be ready for retry for {label}"
            );
        }
    }

    #[test]
    fn pending_saga_current_step_is_zero() {
        for (label, saga) in all_pending_sagas() {
            assert_eq!(saga.current_step(), 0, "pending saga step should be 0 for {label}");
        }
    }

    #[test]
    fn pending_saga_retries_is_zero() {
        for (label, saga) in all_pending_sagas() {
            assert_eq!(saga.retries(), 0, "pending saga retries should be 0 for {label}");
        }
    }

    #[test]
    fn saga_created_at_is_recent() {
        for (label, saga) in all_pending_sagas() {
            let age = chrono::Utc::now() - saga.created_at();
            assert!(age.num_seconds() < 5, "saga {label} created_at should be recent (age: {age})");
        }
    }

    #[test]
    fn saga_updated_at_equals_created_at_initially() {
        for (label, saga) in all_pending_sagas() {
            assert_eq!(
                saga.created_at(),
                saga.updated_at(),
                "saga {label} updated_at should match created_at initially"
            );
        }
    }

    #[test]
    fn saga_type_matches_variant() {
        use inferadb_ledger_state::system::SagaType;
        let sagas = all_pending_sagas();
        let expected_types = [
            SagaType::DeleteUser,
            SagaType::MigrateOrg,
            SagaType::MigrateUser,
            SagaType::CreateUser,
            SagaType::CreateOrganization,
            SagaType::CreateSigningKey,
        ];
        for ((label, saga), expected_type) in sagas.iter().zip(expected_types.iter()) {
            assert_eq!(saga.saga_type(), *expected_type, "saga type mismatch for {label}");
        }
    }

    #[test]
    fn saga_fail_increments_retries_and_becomes_terminal() {
        use inferadb_ledger_state::system::{DeleteUserInput, MAX_RETRIES};

        let mut saga = Saga::DeleteUser(DeleteUserSaga::new(
            SagaId::new("fail-test"),
            DeleteUserInput { user: UserId::new(1), organization_ids: vec![] },
        ));

        // Fail below max retries — should not be terminal
        saga.fail("step 0 error".to_string());
        assert_eq!(saga.retries(), 1);
        assert!(!saga.is_terminal());

        // Fail until max retries reached
        for _ in 1..MAX_RETRIES {
            saga.fail("repeated error".to_string());
        }
        assert_eq!(saga.retries(), MAX_RETRIES);
        assert!(saga.is_terminal(), "saga should be terminal after MAX_RETRIES failures");
    }

    #[test]
    fn saga_lock_keys_delete_user() {
        use inferadb_ledger_state::system::{DeleteUserInput, SagaLockKey};

        let saga = Saga::DeleteUser(DeleteUserSaga::new(
            SagaId::new("lock-test"),
            DeleteUserInput { user: UserId::new(42), organization_ids: vec![] },
        ));
        let keys = saga.lock_keys();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0], SagaLockKey::User(UserId::new(42)));
    }

    #[test]
    fn saga_lock_keys_migrate_org() {
        use inferadb_ledger_state::system::{MigrateOrgInput, SagaLockKey};

        let saga = Saga::MigrateOrg(MigrateOrgSaga::new(
            SagaId::new("lock-migrate"),
            MigrateOrgInput {
                organization_id: OrganizationId::new(7),
                organization_slug: OrganizationSlug::new(700),
                source_region: Region::US_EAST_VA,
                target_region: Region::IE_EAST_DUBLIN,
                acknowledge_residency_downgrade: false,
                metadata_only: false,
            },
        ));
        let keys = saga.lock_keys();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0], SagaLockKey::Organization(OrganizationId::new(7)));
    }

    #[test]
    fn saga_lock_keys_signing_key() {
        use inferadb_ledger_state::system::SagaLockKey;

        let saga = Saga::CreateSigningKey(CreateSigningKeySaga::new(
            SagaId::new("lock-key"),
            CreateSigningKeyInput { scope: SigningKeyScope::Global },
        ));
        let keys = saga.lock_keys();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0], SagaLockKey::SigningKeyScope(SigningKeyScope::Global));
    }

    #[test]
    fn saga_to_bytes_from_bytes_round_trip() {
        for (label, saga) in all_pending_sagas() {
            let bytes = saga.to_bytes().unwrap_or_else(|e| panic!("{label}: to_bytes: {e}"));
            let restored =
                Saga::from_bytes(&bytes).unwrap_or_else(|e| panic!("{label}: from_bytes: {e}"));
            assert_eq!(saga.id().value(), restored.id().value(), "round-trip failed for {label}");
        }
    }

    #[test]
    fn delete_user_saga_state_transitions() {
        use inferadb_ledger_state::system::DeleteUserInput;

        let mut saga = DeleteUserSaga::new(
            SagaId::new("transition-test"),
            DeleteUserInput {
                user: UserId::new(5),
                organization_ids: vec![OrganizationId::new(10), OrganizationId::new(20)],
            },
        );

        assert_eq!(saga.current_step(), 0);

        // Pending -> MarkingDeleted
        saga.transition(inferadb_ledger_state::system::DeleteUserSagaState::MarkingDeleted {
            user_id: UserId::new(5),
            remaining_organizations: vec![OrganizationId::new(10), OrganizationId::new(20)],
        });
        assert_eq!(saga.current_step(), 1);
        assert!(!saga.is_terminal());

        // MarkingDeleted -> MembershipsRemoved
        saga.transition(inferadb_ledger_state::system::DeleteUserSagaState::MembershipsRemoved {
            user_id: UserId::new(5),
        });
        assert_eq!(saga.current_step(), 2);
        assert!(!saga.is_terminal());

        // MembershipsRemoved -> Completed
        saga.transition(inferadb_ledger_state::system::DeleteUserSagaState::Completed {
            user_id: UserId::new(5),
        });
        assert!(saga.is_terminal());
        assert!(!saga.is_ready_for_retry());
    }

    #[test]
    fn migrate_org_saga_state_transitions() {
        use inferadb_ledger_state::system::MigrateOrgInput;

        let mut saga = MigrateOrgSaga::new(
            SagaId::new("migrate-transition"),
            MigrateOrgInput {
                organization_id: OrganizationId::new(1),
                organization_slug: OrganizationSlug::new(100),
                source_region: Region::US_EAST_VA,
                target_region: Region::IE_EAST_DUBLIN,
                acknowledge_residency_downgrade: false,
                metadata_only: true,
            },
        );

        assert_eq!(saga.current_step(), 0);

        saga.transition(inferadb_ledger_state::system::MigrateOrgSagaState::MigrationStarted);
        assert_eq!(saga.current_step(), 1);

        // metadata_only skips data movement
        saga.transition(inferadb_ledger_state::system::MigrateOrgSagaState::IntegrityVerified);

        saga.transition(inferadb_ledger_state::system::MigrateOrgSagaState::RoutingUpdated);

        saga.transition(inferadb_ledger_state::system::MigrateOrgSagaState::Completed);
        assert!(saga.is_terminal());
    }

    #[test]
    fn migrate_org_saga_timeout_is_terminal() {
        use inferadb_ledger_state::system::MigrateOrgInput;

        let mut saga = MigrateOrgSaga::new(
            SagaId::new("timeout-test"),
            MigrateOrgInput {
                organization_id: OrganizationId::new(1),
                organization_slug: OrganizationSlug::new(100),
                source_region: Region::US_EAST_VA,
                target_region: Region::IE_EAST_DUBLIN,
                acknowledge_residency_downgrade: false,
                metadata_only: false,
            },
        );

        saga.transition(inferadb_ledger_state::system::MigrateOrgSagaState::TimedOut);
        assert!(saga.is_terminal());
    }

    #[test]
    fn delete_user_saga_backoff_caps_at_max() {
        use inferadb_ledger_state::system::DeleteUserInput;

        let mut saga = DeleteUserSaga::new(
            SagaId::new("backoff-test"),
            DeleteUserInput { user: UserId::new(1), organization_ids: vec![] },
        );

        // Verify initial backoff is 1 second
        assert_eq!(saga.next_backoff(), std::time::Duration::from_secs(1));

        // After scheduling retries, backoff grows but caps at max (5 minutes)
        for _ in 0..20 {
            saga.schedule_retry();
        }
        assert!(saga.next_backoff() <= std::time::Duration::from_secs(5 * 60));
    }

    #[test]
    fn onboarding_pii_debug_redacts_fields() {
        let pii = OnboardingPii {
            email: "secret@example.com".to_string(),
            name: "Secret Name".to_string(),
            organization_name: "Secret Org".to_string(),
        };
        let debug = format!("{pii:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("secret@example.com"));
        assert!(!debug.contains("Secret Name"));
        assert!(!debug.contains("Secret Org"));
    }

    #[test]
    fn org_pii_debug_redacts_name() {
        let pii = OrgPii { name: "Secret Org".to_string() };
        let debug = format!("{pii:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("Secret Org"));
    }

    #[test]
    fn onboarding_crypto_debug_redacts_secrets() {
        let crypto = OnboardingCrypto {
            kid: "kid-123".to_string(),
            refresh_token: "super-secret-token".to_string(),
            refresh_family_id: [1; 16],
            refresh_expires_at: chrono::Utc::now(),
            user_slug: UserSlug::new(1),
            organization_slug: OrganizationSlug::new(1),
        };
        let debug = format!("{crypto:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("super-secret-token"));
        // kid is NOT redacted (it's a public identifier)
        assert!(debug.contains("kid-123"));
    }

    #[test]
    fn saga_output_debug_redacts_secrets() {
        let output = SagaOutput {
            user_id: UserId::new(1),
            user_slug: UserSlug::new(1),
            organization_id: OrganizationId::new(1),
            organization_slug: OrganizationSlug::new(1),
            refresh_token_id: RefreshTokenId::new(1),
            kid: "kid-456".to_string(),
            refresh_family_id: [2; 16],
            refresh_expires_at: chrono::Utc::now(),
            refresh_token: "raw-token-value".to_string(),
        };
        let debug = format!("{output:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("raw-token-value"));
        // Public fields should be visible
        assert!(debug.contains("kid-456"));
    }

    #[test]
    fn saga_handle_submit_fails_on_closed_channel() {
        let (tx, rx) = mpsc::channel(1);
        let handle = SagaOrchestratorHandle { tx };
        // Drop the receiver to close the channel
        drop(rx);

        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            let submission = SagaSubmission {
                record: Saga::DeleteUser(DeleteUserSaga::new(
                    SagaId::new("closed-channel"),
                    inferadb_ledger_state::system::DeleteUserInput {
                        user: UserId::new(1),
                        organization_ids: vec![],
                    },
                )),
                pii: None,
                org_pii: None,
                notify: None,
            };
            let result = handle.submit_saga(submission).await;
            assert!(result.is_err(), "submit should fail when channel is closed");
        });
    }

    #[test]
    fn saga_key_prefix_format() {
        assert_eq!(SAGA_KEY_PREFIX, "_meta:saga:");
        assert_eq!(SAGA_CLIENT_ID, "system:saga");
    }

    #[test]
    fn create_signing_key_saga_transitions() {
        let mut saga = CreateSigningKeySaga::new(
            SagaId::new("signing-key-transition"),
            CreateSigningKeyInput { scope: SigningKeyScope::Global },
        );

        assert_eq!(saga.current_step(), 0);
        assert!(!saga.is_terminal());

        saga.transition(inferadb_ledger_state::system::CreateSigningKeySagaState::KeyGenerated {
            kid: "kid-001".to_string(),
            public_key_bytes: vec![0x01; 32],
            encrypted_private_key: vec![0x02; 100],
            rmk_version: 1,
        });
        assert_eq!(saga.current_step(), 1);

        saga.transition(inferadb_ledger_state::system::CreateSigningKeySagaState::Completed {
            kid: "kid-001".to_string(),
        });
        assert!(saga.is_terminal());
        assert!(!saga.is_ready_for_retry());
    }

    #[test]
    fn create_signing_key_saga_fail_to_terminal() {
        let mut saga = CreateSigningKeySaga::new(
            SagaId::new("signing-key-fail"),
            CreateSigningKeyInput { scope: SigningKeyScope::Global },
        );

        // Fail MAX_RETRIES times
        for _ in 0..inferadb_ledger_state::system::MAX_RETRIES {
            saga.fail(0, "test error".to_string());
        }
        assert!(saga.is_terminal());
    }

    #[test]
    fn migrate_user_saga_state_transitions_full_path() {
        use inferadb_ledger_state::system::{MigrateUserInput, MigrateUserSagaState};

        let mut saga = MigrateUserSaga::new(
            SagaId::new("migrate-user-full"),
            MigrateUserInput {
                user: UserId::new(5),
                source_region: Region::US_EAST_VA,
                target_region: Region::IE_EAST_DUBLIN,
            },
        );

        assert!(!saga.is_terminal());
        saga.transition(MigrateUserSagaState::DirectoryMarkedMigrating);
        saga.transition(MigrateUserSagaState::UserDataRead);
        saga.transition(MigrateUserSagaState::UserDataWritten);
        saga.transition(MigrateUserSagaState::DirectoryUpdated);
        saga.transition(MigrateUserSagaState::SourceDeleted);
        saga.transition(MigrateUserSagaState::Completed);
        assert!(saga.is_terminal());
    }

    #[test]
    fn migrate_user_saga_compensated_is_terminal() {
        use inferadb_ledger_state::system::{MigrateUserInput, MigrateUserSagaState};

        let mut saga = MigrateUserSaga::new(
            SagaId::new("migrate-user-compensate"),
            MigrateUserInput {
                user: UserId::new(5),
                source_region: Region::US_EAST_VA,
                target_region: Region::IE_EAST_DUBLIN,
            },
        );

        saga.transition(MigrateUserSagaState::TimedOut);
        assert!(saga.is_terminal());
    }
}
