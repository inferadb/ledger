//! State machine apply logic for Raft log entries.
//!
//! Transforms committed log entries into state mutations via the storage engine.

use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use inferadb_ledger_state::{
    StateError, StateLayer,
    system::{
        App, AppCredentialType, AppCredentials, AppVaultConnection, ClientAssertionEntry,
        CreateSigningKeyInput, CreateSigningKeySaga, OrganizationMember, OrganizationMemberRole,
        OrganizationProfile, OrganizationRegistry, OrganizationStatus, PendingOrganizationProfile,
        RefreshToken, SYSTEM_VAULT_ID, Saga, SigningKey, SigningKeyScope, SigningKeyStatus,
        SystemKeys, SystemOrganizationService, TeamMember, TeamProfile, User,
    },
};
use inferadb_ledger_store::StorageBackend;
use inferadb_ledger_types::{
    AppId, AppSlug, Hash, LedgerErrorCode, Operation, OrganizationId, SetCondition, TeamId,
    TeamSlug, TokenSubject, TokenType, VaultEntry, VaultId, compute_tx_merkle_root, decode, encode,
    events::{EventAction, EventEntry, EventOutcome},
};

use super::{
    store::RaftLogStore,
    types::{
        AppliedState, ClientSequenceEntry, OrganizationMeta, PendingExternalWrites,
        VaultHealthStatus, VaultMeta, estimate_write_storage_delta,
    },
};
use crate::{
    event_writer::ApplyPhaseEmitter,
    types::{LedgerRequest, LedgerResponse, SystemRequest},
};

/// Encodes a value for state persistence, logging an error if serialization fails.
///
/// In the Raft state machine, `encode()` failures are effectively impossible
/// for well-formed domain types (bincode serialization of simple structs).
/// However, silently swallowing failures would cause state divergence between
/// in-memory and persisted state. This helper ensures any failure is logged
/// at `error` level for immediate operational visibility.
fn try_encode<T: serde::Serialize>(value: &T, context: &str) -> Option<Vec<u8>> {
    match encode(value) {
        Ok(blob) => Some(blob),
        Err(e) => {
            tracing::error!(context, error = %e, "Failed to encode state — persistence skipped, potential state divergence");
            None
        },
    }
}

/// Writes a `CreateSigningKeySaga` record to `_system` storage.
///
/// Called from the `CreateOrganizationWithProfile` apply handler to ensure
/// newly created orgs get a signing key. The saga orchestrator picks up
/// the record on its next poll cycle.
///
/// This is a synchronous write through the state layer (not Raft) because
/// the apply handler is already inside a Raft commit. The saga ID is derived
/// deterministically from the scope to ensure all replicas produce identical
/// state when applying the same log entry.
fn write_signing_key_saga<B: StorageBackend>(
    state_layer: &Arc<StateLayer<B>>,
    scope: SigningKeyScope,
) {
    let saga_id = match &scope {
        SigningKeyScope::Global => "create-signing-key-global".to_owned(),
        SigningKeyScope::Organization(org_id) => {
            format!("create-signing-key-org-{}", org_id.value())
        },
    };
    let saga = CreateSigningKeySaga::new(saga_id.clone(), CreateSigningKeyInput { scope });
    let wrapped = Saga::CreateSigningKey(saga);

    let key = format!("saga:{saga_id}");
    match serde_json::to_vec(&wrapped) {
        Ok(value) => {
            let op = Operation::SetEntity {
                key: key.clone(),
                value,
                condition: Some(SetCondition::MustNotExist),
                expires_at: None,
            };
            match state_layer.apply_operations(SYSTEM_VAULT_ID, &[op], 0) {
                Ok(_) => {
                    tracing::info!(
                        saga_id = %saga_id,
                        scope = ?scope,
                        "Wrote CreateSigningKeySaga from apply handler"
                    );
                },
                Err(StateError::PreconditionFailed { .. }) => {
                    // Expected on log replay — saga already exists from prior apply.
                    tracing::info!(
                        saga_id = %saga_id,
                        scope = ?scope,
                        "CreateSigningKeySaga already exists (idempotent replay)"
                    );
                },
                Err(e) => {
                    tracing::error!(
                        saga_id = %saga_id,
                        scope = ?scope,
                        error = %e,
                        "Failed to write CreateSigningKeySaga record"
                    );
                },
            }
        },
        Err(e) => {
            tracing::error!(
                saga_id = %saga_id,
                error = %e,
                "Failed to serialize CreateSigningKeySaga"
            );
        },
    }
}

/// Constructs a `LedgerResponse::Error` with the given code and message.
fn ledger_error(code: LedgerErrorCode, message: impl Into<String>) -> LedgerResponse {
    LedgerResponse::Error { code, message: message.into() }
}

/// Constructs an early-return error tuple for the apply handler.
fn error_result(
    code: LedgerErrorCode,
    message: impl Into<String>,
) -> (LedgerResponse, Option<VaultEntry>) {
    (ledger_error(code, message), None)
}

/// Loads and decodes an `OrganizationProfile` from the state layer.
fn load_org_profile<B: StorageBackend>(
    state_layer: &inferadb_ledger_state::StateLayer<B>,
    organization: OrganizationId,
) -> Result<OrganizationProfile, LedgerResponse> {
    let key = SystemKeys::organization_profile_key(organization);
    let entity = state_layer
        .get_entity(SYSTEM_VAULT_ID, key.as_bytes())
        .map_err(|e| {
            ledger_error(
                LedgerErrorCode::Internal,
                format!("Failed to read organization profile: {e}"),
            )
        })?
        .ok_or_else(|| {
            ledger_error(
                LedgerErrorCode::NotFound,
                format!("Organization profile not found for {organization}"),
            )
        })?;
    decode::<OrganizationProfile>(&entity.value).map_err(|e| {
        ledger_error(
            LedgerErrorCode::Internal,
            format!("Failed to decode organization profile: {e}"),
        )
    })
}

/// Encodes and writes an `OrganizationProfile` back to the state layer.
fn save_org_profile<B: StorageBackend>(
    state_layer: &inferadb_ledger_state::StateLayer<B>,
    organization: OrganizationId,
    profile: &OrganizationProfile,
) -> Result<(), LedgerResponse> {
    let key = SystemKeys::organization_profile_key(organization);
    let bytes = encode(profile).map_err(|e| {
        ledger_error(
            LedgerErrorCode::Internal,
            format!("Failed to encode organization profile: {e}"),
        )
    })?;
    let ops = vec![Operation::SetEntity { key, value: bytes, condition: None, expires_at: None }];
    state_layer.apply_operations(SYSTEM_VAULT_ID, &ops, 0).map(|_| ()).map_err(|e| {
        ledger_error(
            LedgerErrorCode::Internal,
            format!("Failed to write organization profile: {e}"),
        )
    })
}

/// Bumps the config_version in an organization's registry and re-serializes org meta.
fn bump_org_config_version<B: StorageBackend>(
    state_layer: &inferadb_ledger_state::StateLayer<B>,
    organization: OrganizationId,
    state: &mut AppliedState,
    pending: &mut PendingExternalWrites,
) -> Result<(), LedgerResponse> {
    if let Some(org_mut) = state.organizations.get_mut(&organization) {
        let reg_key = SystemKeys::organization_key(organization);
        if let Ok(Some(reg_entity)) = state_layer
            .get_entity(SYSTEM_VAULT_ID, reg_key.as_bytes())
            .inspect_err(|e| tracing::error!(error = %e, "Failed to read org registry"))
            && let Ok(mut registry) = decode::<OrganizationRegistry>(&reg_entity.value)
                .inspect_err(|e| tracing::error!(error = %e, "Failed to decode org registry"))
        {
            registry.config_version += 1;
            if let Some(reg_bytes) = try_encode(&registry, "org_registry") {
                let ops = vec![Operation::SetEntity {
                    key: reg_key,
                    value: reg_bytes,
                    condition: None,
                    expires_at: None,
                }];
                if let Err(e) = state_layer.apply_operations(SYSTEM_VAULT_ID, &ops, 0) {
                    return Err(ledger_error(
                        LedgerErrorCode::Internal,
                        format!("Failed to bump org config_version: {e}"),
                    ));
                }
            }
        }
        if let Some(blob) = try_encode(org_mut, "organization") {
            pending.organizations.push((organization, blob));
        }
    }
    Ok(())
}

/// Checks whether an app name already exists within an organization.
///
/// Uses the in-memory `app_name_index` for O(1) lookups.
/// The `exclude_app` parameter allows renaming an app to the same name.
fn has_app_name_conflict(
    state: &AppliedState,
    organization: OrganizationId,
    name: &str,
    exclude_app: Option<AppId>,
) -> bool {
    let key = (organization, name.to_string());
    match state.app_name_index.get(&key) {
        Some(existing_app) => exclude_app.is_none_or(|a| *existing_app != a),
        None => false,
    }
}

/// Loads an `App` from the state layer by its storage key.
fn load_app<B: StorageBackend>(
    state_layer: &inferadb_ledger_state::StateLayer<B>,
    organization: OrganizationId,
    app: AppId,
) -> Result<App, LedgerResponse> {
    let key = SystemKeys::app_key(organization, app);
    let entity = state_layer
        .get_entity(SYSTEM_VAULT_ID, key.as_bytes())
        .map_err(|e| ledger_error(LedgerErrorCode::Internal, format!("Failed to read app: {e}")))?
        .ok_or_else(|| {
            ledger_error(
                LedgerErrorCode::NotFound,
                format!("App {} not found in organization {}", app, organization),
            )
        })?;
    decode::<App>(&entity.value)
        .map_err(|e| ledger_error(LedgerErrorCode::Internal, format!("Failed to decode app: {e}")))
}

/// Saves an `App` to the state layer.
fn save_app<B: StorageBackend>(
    state_layer: &inferadb_ledger_state::StateLayer<B>,
    organization: OrganizationId,
    app: &App,
) -> Result<(), LedgerResponse> {
    let key = SystemKeys::app_key(organization, app.id);
    let bytes = encode(app).map_err(|e| {
        ledger_error(LedgerErrorCode::Internal, format!("Failed to encode app: {e}"))
    })?;
    let ops = vec![Operation::SetEntity { key, value: bytes, condition: None, expires_at: None }];
    state_layer
        .apply_operations(SYSTEM_VAULT_ID, &ops, 0)
        .map(|_| ())
        .map_err(|e| ledger_error(LedgerErrorCode::Internal, format!("Failed to write app: {e}")))
}

/// Collects all entities matching a prefix and appends `DeleteEntity` operations for each.
///
/// Uses paginated `list_entities` calls to handle any number of sub-resources
/// without a hard cap.
fn collect_all_entities_for_deletion<B: StorageBackend>(
    state_layer: &inferadb_ledger_state::StateLayer<B>,
    prefix: &str,
    ops: &mut Vec<Operation>,
) {
    const PAGE_SIZE: usize = 1000;
    let mut cursor: Option<String> = None;
    loop {
        match state_layer.list_entities(SYSTEM_VAULT_ID, Some(prefix), cursor.as_deref(), PAGE_SIZE)
        {
            Ok(entities) => {
                let is_last_page = entities.len() < PAGE_SIZE;
                for entity in &entities {
                    let key = String::from_utf8_lossy(&entity.key).into_owned();
                    cursor = Some(key.clone());
                    ops.push(Operation::DeleteEntity { key });
                }
                if is_last_page {
                    break;
                }
            },
            Err(e) => {
                tracing::error!(prefix = %prefix, error = %e, "Failed to list entities for deletion");
                break;
            },
        }
    }
}

/// Checks whether a team name already exists within an organization.
///
/// Uses the in-memory `team_name_index` for O(1) lookups instead of
/// scanning all team profiles. The `exclude_team` parameter allows
/// renaming a team to the same name (self-conflict).
fn has_team_name_conflict(
    state: &AppliedState,
    organization: OrganizationId,
    name: &str,
    exclude_team: Option<TeamId>,
) -> bool {
    let key = (organization, name.to_string());
    match state.team_name_index.get(&key) {
        Some(existing_team) => exclude_team.is_none_or(|t| *existing_team != t),
        None => false,
    }
}

/// Loads and decodes a team profile from the state layer.
///
/// Returns the decoded `TeamProfile` or a structured `LedgerResponse::Error`
/// suitable for direct return from the apply handler.
fn load_team_profile<B: StorageBackend>(
    state_layer: &inferadb_ledger_state::StateLayer<B>,
    organization: OrganizationId,
    team: TeamId,
) -> Result<TeamProfile, LedgerResponse> {
    let profile_key = SystemKeys::team_profile_key(organization, team);
    let entity = state_layer
        .get_entity(SYSTEM_VAULT_ID, profile_key.as_bytes())
        .map_err(|e| LedgerResponse::Error {
            code: LedgerErrorCode::Internal,
            message: format!("Failed to read team profile: {e}"),
        })?
        .ok_or_else(|| LedgerResponse::Error {
            code: LedgerErrorCode::NotFound,
            message: format!("Team {} not found", team),
        })?;
    decode::<TeamProfile>(&entity.value).map_err(|e| LedgerResponse::Error {
        code: LedgerErrorCode::Internal,
        message: format!("Failed to decode team profile: {e}"),
    })
}

/// Validates that an organization exists, is not deleted, and returns the state layer.
///
/// Consolidates the three-step guard (org exists → not deleted → state layer available)
/// that repeats across most organization-scoped operations. Returns a reference to the
/// state layer on success.
///
/// The `action` parameter is used in error messages (e.g., "modify", "create vaults in").
fn require_active_org_with_state<'a, B: StorageBackend>(
    organization: &OrganizationId,
    state: &AppliedState,
    state_layer: &'a Option<Arc<inferadb_ledger_state::StateLayer<B>>>,
    action: &str,
) -> Result<&'a Arc<inferadb_ledger_state::StateLayer<B>>, LedgerResponse> {
    let org = state.organizations.get(organization).ok_or_else(|| LedgerResponse::Error {
        code: LedgerErrorCode::NotFound,
        message: format!("Organization {} not found", organization),
    })?;
    if org.status == OrganizationStatus::Deleted {
        return Err(LedgerResponse::Error {
            code: LedgerErrorCode::FailedPrecondition,
            message: format!("Cannot {} deleted organization {}", action, organization),
        });
    }
    state_layer.as_ref().ok_or_else(|| LedgerResponse::Error {
        code: LedgerErrorCode::Internal,
        message: "State layer not configured".to_string(),
    })
}

/// Validates that an organization is in Active status specifically, rejecting
/// Provisioning, Suspended, Migrating, and Deleted states.
///
/// Used for write and vault creation operations that require a fully ready org.
fn require_fully_active_org(
    organization: &OrganizationId,
    state: &AppliedState,
) -> Result<(), LedgerResponse> {
    if let Some(org_meta) = state.organizations.get(organization) {
        match org_meta.status {
            OrganizationStatus::Active => Ok(()),
            other => Err(LedgerResponse::Error {
                code: LedgerErrorCode::FailedPrecondition,
                message: format!(
                    "Organization {} is {} and not accepting this operation",
                    organization,
                    match other {
                        OrganizationStatus::Provisioning => "still being provisioned",
                        OrganizationStatus::Suspended => "suspended",
                        OrganizationStatus::Migrating => "migrating",
                        OrganizationStatus::Deleted => "deleted",
                        OrganizationStatus::Active => "active",
                    }
                ),
            }),
        }
    } else {
        Err(LedgerResponse::Error {
            code: LedgerErrorCode::NotFound,
            message: format!("Organization {} not found", organization),
        })
    }
}

/// Migrates members from one team to another, preserving roles and join dates.
///
/// Skips members already present in the target team (deduplication by user_id).
fn migrate_team_members<B: StorageBackend>(
    state_layer: &inferadb_ledger_state::StateLayer<B>,
    organization: OrganizationId,
    source_profile: &TeamProfile,
    target_team: TeamId,
    block_timestamp: DateTime<Utc>,
) -> Result<(), LedgerResponse> {
    let mut target_profile = load_team_profile(state_layer, organization, target_team)?;
    for member in &source_profile.members {
        if !target_profile.members.iter().any(|m| m.user_id == member.user_id) {
            target_profile.members.push(TeamMember {
                user_id: member.user_id,
                role: member.role,
                joined_at: member.joined_at,
            });
        }
    }
    target_profile.updated_at = block_timestamp;
    let target_bytes = encode(&target_profile).map_err(|e| LedgerResponse::Error {
        code: LedgerErrorCode::Internal,
        message: format!("Failed to encode target team profile: {e}"),
    })?;
    let target_key = SystemKeys::team_profile_key(organization, target_team);
    let ops = vec![Operation::SetEntity {
        key: target_key,
        value: target_bytes,
        condition: None,
        expires_at: None,
    }];
    state_layer
        .apply_operations(SYSTEM_VAULT_ID, &ops, 0)
        .map_err(|e| LedgerResponse::Error {
            code: LedgerErrorCode::Internal,
            message: format!("Failed to migrate members to target team: {e}"),
        })
        .map(|_| ())
}

/// Converts a `u64` seconds value into a `chrono::Duration`, saturating at `i64::MAX`.
fn saturating_duration_secs(secs: u64) -> Duration {
    Duration::seconds(i64::try_from(secs).unwrap_or(i64::MAX))
}

/// Looks up a signing key by kid, returning the key or an early-return error tuple.
fn require_signing_key<B: StorageBackend>(
    sys: &SystemOrganizationService<B>,
    kid: &str,
) -> Result<SigningKey, (LedgerResponse, Option<VaultEntry>)> {
    match sys.get_signing_key_by_kid(kid) {
        Ok(Some(k)) => Ok(k),
        Ok(None) => {
            Err(error_result(LedgerErrorCode::NotFound, format!("Signing key not found: {kid}")))
        },
        Err(e) => Err(error_result(
            LedgerErrorCode::Internal,
            format!("Failed to look up signing key: {e}"),
        )),
    }
}

#[allow(clippy::result_large_err)]
impl<B: StorageBackend> RaftLogStore<B> {
    /// Applies a single request and returns the response plus optional vault entry.
    ///
    /// For Write requests, this also returns a VaultEntry that should be included
    /// in the RegionBlock. The caller is responsible for collecting these entries
    /// and creating the RegionBlock.
    ///
    /// This is the backward-compatible entry point used by tests. Events are
    /// discarded. For event-aware apply, use [`apply_request_with_events`].
    #[cfg(test)]
    pub(super) fn apply_request(
        &self,
        request: &LedgerRequest,
        state: &mut AppliedState,
    ) -> (LedgerResponse, Option<VaultEntry>) {
        let mut events = Vec::new();
        let mut op_index = 0u32;
        let mut pending = PendingExternalWrites::default();
        self.apply_request_with_events(
            request,
            state,
            Utc::now(),
            &mut op_index,
            &mut events,
            0,
            &mut pending,
        )
    }

    /// Applies a single request with event emission support.
    ///
    /// Like [`apply_request`], but additionally accumulates deterministic
    /// apply-phase events into `events` and external table writes into
    /// `pending`. The `op_index` counter is incremented for each event
    /// emitted, ensuring unique UUID v5 event IDs across a batch.
    ///
    /// `ttl_days` controls event expiry (from [`EventConfig::default_ttl_days`]).
    pub(super) fn apply_request_with_events(
        &self,
        request: &LedgerRequest,
        state: &mut AppliedState,
        block_timestamp: DateTime<Utc>,
        op_index: &mut u32,
        events: &mut Vec<EventEntry>,
        ttl_days: u32,
        pending: &mut PendingExternalWrites,
    ) -> (LedgerResponse, Option<VaultEntry>) {
        // Block height for event emission (from region chain state)
        let block_height = self.region_chain.read().height + 1;

        match request {
            LedgerRequest::Write {
                organization,
                vault,
                transactions,
                idempotency_key,
                request_hash,
            } => {
                if let Err(resp) = require_fully_active_org(organization, state) {
                    return (resp, None);
                }

                let key = (*organization, *vault);
                if let Some(VaultHealthStatus::Diverged { .. }) = state.vault_health.get(&key) {
                    return (
                        LedgerResponse::Error {
                            code: LedgerErrorCode::FailedPrecondition,
                            message: format!(
                                "Vault {}:{} is diverged and not accepting writes",
                                organization, vault
                            ),
                        },
                        None,
                    );
                }

                let current_height = state.vault_heights.get(&key).copied().unwrap_or(0);
                let new_height = current_height + 1;

                // Get previous vault hash (ZERO_HASH for genesis)
                let previous_vault_hash = state
                    .previous_vault_hashes
                    .get(&key)
                    .copied()
                    .unwrap_or(inferadb_ledger_types::ZERO_HASH);

                // Apply transactions to state layer if configured
                let state_root = if let Some(state_layer) = &self.state_layer {
                    // Apply all transactions' operations in a single storage transaction
                    // for atomicity: either all operations in the block commit, or none do.
                    let mut write_txn = match state_layer.begin_write() {
                        Ok(txn) => txn,
                        Err(e) => {
                            return (
                                LedgerResponse::Error {
                                    code: LedgerErrorCode::Internal,
                                    message: format!("Failed to begin write txn: {e}"),
                                },
                                None,
                            );
                        },
                    };
                    let mut all_dirty_keys = Vec::new();
                    for tx in transactions.iter() {
                        match state_layer.apply_operations_in_txn(
                            &mut write_txn,
                            *vault,
                            &tx.operations,
                            new_height,
                        ) {
                            Ok((_statuses, dirty_keys)) => {
                                all_dirty_keys.extend(dirty_keys);
                            },
                            Err(e) => {
                                // On CAS failure, return current state for conflict resolution.
                                // Write txn is dropped (rolled back) automatically.
                                return match e {
                                    StateError::PreconditionFailed {
                                        key,
                                        current_version,
                                        current_value,
                                        failed_condition,
                                    } => (
                                        LedgerResponse::PreconditionFailed {
                                            key,
                                            current_version,
                                            current_value,
                                            failed_condition,
                                        },
                                        None,
                                    ),
                                    other => (
                                        LedgerResponse::Error {
                                            code: LedgerErrorCode::Internal,
                                            message: format!("Failed to apply operations: {other}"),
                                        },
                                        None,
                                    ),
                                };
                            },
                        }
                    }
                    if let Err(e) = write_txn.commit() {
                        return (
                            LedgerResponse::Error {
                                code: LedgerErrorCode::Internal,
                                message: format!("Failed to commit write txn: {e}"),
                            },
                            None,
                        );
                    }
                    state_layer.mark_dirty_keys(*vault, &all_dirty_keys);

                    // Compute state root
                    match state_layer.compute_state_root(*vault) {
                        Ok(root) => root,
                        Err(e) => {
                            return (
                                LedgerResponse::Error {
                                    code: LedgerErrorCode::Internal,
                                    message: format!("Failed to compute state root: {}", e),
                                },
                                None,
                            );
                        },
                    }
                } else {
                    // No state layer configured, use placeholder
                    inferadb_ledger_types::EMPTY_HASH
                };

                // Compute tx merkle root
                let tx_merkle_root = compute_tx_merkle_root(transactions);

                // Update vault height in applied state
                state.vault_heights.insert(key, new_height);
                pending.vault_heights.push((key, new_height));

                // Update last write timestamp from latest transaction (deterministic)
                if let Some(last_tx) = transactions.last()
                    && let Some(vault_meta) = state.vaults.get_mut(&key)
                {
                    vault_meta.last_write_timestamp = last_tx.timestamp.timestamp() as u64;
                    // Re-serialize after in-place mutation
                    if let Some(blob) = try_encode(vault_meta, "vault_meta") {
                        pending.vaults.push((vault_meta.vault, blob));
                    }
                }

                // Server-assigned sequences: assign monotonic sequence to each transaction.
                // Each write request typically contains a single transaction from a single client.
                // The assigned_sequence returned is the sequence assigned to the first transaction.
                let mut assigned_sequence = 0u64;
                let transactions_with_sequences: Vec<_> = transactions
                    .iter()
                    .map(|tx| {
                        let client_key = (*organization, *vault, tx.client_id.clone());
                        let current_seq =
                            state.client_sequences.get(&client_key).map_or(0, |e| e.sequence);
                        let new_sequence = current_seq + 1;
                        let entry = ClientSequenceEntry {
                            sequence: new_sequence,
                            last_seen: block_timestamp.timestamp(),
                            last_idempotency_key: *idempotency_key,
                            last_request_hash: *request_hash,
                        };
                        state.client_sequences.insert(client_key, entry);

                        // Mirror to pending external writes
                        let cs_key = PendingExternalWrites::client_sequence_key(
                            *organization,
                            *vault,
                            tx.client_id.as_bytes(),
                        );
                        let cs_entry = ClientSequenceEntry {
                            sequence: new_sequence,
                            last_seen: block_timestamp.timestamp(),
                            last_idempotency_key: *idempotency_key,
                            last_request_hash: *request_hash,
                        };
                        if let Some(value) = try_encode(&cs_entry, "client_sequence") {
                            pending.client_sequences.push((cs_key, value));
                        }

                        // Record the first transaction's assigned sequence for the response
                        if assigned_sequence == 0 {
                            assigned_sequence = new_sequence;
                        }

                        // Clone and update the sequence
                        let mut tx_with_seq = tx.clone();
                        tx_with_seq.sequence = new_sequence;
                        tx_with_seq
                    })
                    .collect();

                // Build VaultEntry for RegionBlock with server-assigned sequences
                let vault_entry = VaultEntry {
                    organization: *organization,
                    vault: *vault,
                    vault_height: new_height,
                    previous_vault_hash,
                    transactions: transactions_with_sequences,
                    tx_merkle_root,
                    state_root,
                };

                // Update organization storage accounting.
                // Increment for sets/creates, decrement for deletes.
                let storage_delta = estimate_write_storage_delta(transactions);
                let entry = state.organization_storage_bytes.entry(*organization).or_insert(0);
                if storage_delta >= 0 {
                    *entry = entry.saturating_add(storage_delta as u64);
                } else {
                    *entry = entry.saturating_sub(storage_delta.unsigned_abs());
                }
                crate::metrics::set_organization_storage_bytes(*organization, *entry);
                crate::metrics::record_organization_operation(*organization, "write");

                // Mirror updated OrganizationMeta (with new storage_bytes) to pending
                if let Some(org_meta) = state.organizations.get_mut(organization) {
                    org_meta.storage_bytes = *entry;
                    if let Some(blob) = try_encode(org_meta, "org_meta") {
                        pending.organizations.push((*organization, blob));
                    }
                }

                // Compute block hash from vault entry (for response)
                // We temporarily build a BlockHeader to compute the hash
                let block_hash = self.compute_vault_block_hash(&vault_entry);

                // Emit WriteCommitted event
                let org_slug = state.id_to_slug.get(organization).copied();
                let vault_slug = state.vault_id_to_slug.get(vault).copied();
                let ops_count: u32 = transactions.iter().map(|tx| tx.operations.len() as u32).sum();
                let mut emitter = ApplyPhaseEmitter::for_organization(
                    EventAction::WriteCommitted,
                    *organization,
                    org_slug,
                )
                .outcome(EventOutcome::Success)
                .operations_count(ops_count);
                if let Some(vs) = vault_slug {
                    emitter = emitter.vault(vs);
                }
                events.push(emitter.build(block_height, *op_index, block_timestamp, ttl_days));
                *op_index += 1;

                // Emit individual EntityExpired events for each ExpireEntity operation
                for tx in transactions {
                    for op in &tx.operations {
                        if let Operation::ExpireEntity { key, .. } = op {
                            let mut exp_emitter = ApplyPhaseEmitter::for_organization(
                                EventAction::EntityExpired,
                                *organization,
                                org_slug,
                            )
                            .detail("key", key)
                            .outcome(EventOutcome::Success);
                            if let Some(vs) = vault_slug {
                                exp_emitter = exp_emitter.vault(vs);
                            }
                            events.push(exp_emitter.build(
                                block_height,
                                *op_index,
                                block_timestamp,
                                ttl_days,
                            ));
                            *op_index += 1;
                        }
                    }
                }

                (
                    LedgerResponse::Write {
                        block_height: new_height,
                        block_hash,
                        assigned_sequence,
                    },
                    Some(vault_entry),
                )
            },

            LedgerRequest::CreateVault { organization, slug, name, retention_policy } => {
                if let Err(resp) = require_fully_active_org(organization, state) {
                    return (resp, None);
                }

                let vault_id = state.sequences.next_vault();
                let key = (*organization, vault_id);
                state.vault_heights.insert(key, 0);
                pending.vault_heights.push((key, 0));
                state.vault_health.insert(key, VaultHealthStatus::Healthy);
                pending.vault_health.push((key, VaultHealthStatus::Healthy));
                let vault_meta = VaultMeta {
                    organization: *organization,
                    vault: vault_id,
                    slug: *slug,
                    name: name.clone(),
                    deleted: false,
                    last_write_timestamp: 0, // No writes yet
                    retention_policy: retention_policy.unwrap_or_default(),
                };
                if let Some(blob) = try_encode(&vault_meta, "vault_meta") {
                    pending.vaults.push((vault_id, blob));
                }
                state.vaults.insert(key, vault_meta);

                // Insert into bidirectional vault slug index
                state.vault_slug_index.insert(*slug, vault_id);
                state.vault_id_to_slug.insert(vault_id, *slug);
                pending.vault_slug_index.push((*slug, vault_id));

                // Emit VaultCreated event
                let org_slug = state.id_to_slug.get(organization).copied();
                events.push(
                    ApplyPhaseEmitter::for_organization(
                        EventAction::VaultCreated,
                        *organization,
                        org_slug,
                    )
                    .vault(*slug)
                    .detail("vault_name", name.as_deref().unwrap_or(""))
                    .outcome(EventOutcome::Success)
                    .build(block_height, *op_index, block_timestamp, ttl_days),
                );
                *op_index += 1;

                (LedgerResponse::VaultCreated { vault: vault_id, slug: *slug }, None)
            },

            LedgerRequest::DeleteOrganization { organization } => {
                // Capture slug before potential state changes
                let org_slug = state.id_to_slug.get(organization).copied();

                let response = if let Some(org) = state.organizations.get_mut(organization) {
                    if org.status == OrganizationStatus::Deleted {
                        LedgerResponse::Error {
                            code: LedgerErrorCode::FailedPrecondition,
                            message: format!("Organization {} is already deleted", organization),
                        }
                    } else {
                        // Soft-delete: set status and timestamp, keep slug index
                        // (slug cleanup deferred to PurgeOrganization)
                        let deleted_at = block_timestamp;
                        let retention_days = org.region.retention_days();
                        org.status = OrganizationStatus::Deleted;
                        // Re-serialize after in-place mutation
                        if let Some(blob) = try_encode(org, "organization") {
                            pending.organizations.push((*organization, blob));
                        }

                        // Update OrganizationRegistry in state layer so the purge
                        // job can discover this org's deleted_at timestamp.
                        if let Some(state_layer) = &self.state_layer {
                            let reg_key = SystemKeys::organization_key(*organization);
                            if let Ok(Some(reg_entity)) = state_layer
                                .get_entity(SYSTEM_VAULT_ID, reg_key.as_bytes())
                                .inspect_err(|e| tracing::error!(error = %e, "Failed to read org registry for deletion"))
                                && let Ok(mut registry) = decode::<OrganizationRegistry>(
                                    &reg_entity.value,
                                )
                                .inspect_err(|e| tracing::error!(error = %e, "Failed to decode org registry for deletion"))
                            {
                                registry.status = OrganizationStatus::Deleted;
                                registry.deleted_at = Some(deleted_at);
                                if let Some(reg_bytes) = try_encode(&registry, "org_registry") {
                                    let ops = vec![Operation::SetEntity {
                                        key: reg_key,
                                        value: reg_bytes,
                                        condition: None,
                                        expires_at: None,
                                    }];
                                    if let Err(e) =
                                        state_layer.apply_operations(SYSTEM_VAULT_ID, &ops, 0)
                                    {
                                        tracing::error!(
                                            organization_id = organization.value(),
                                            error = %e,
                                            "Failed to update org registry on soft-delete"
                                        );
                                    }
                                }
                            }

                            // Cascade: revoke refresh tokens on soft-delete.
                            // Signing keys are retained until PurgeOrganization for
                            // in-flight token validation during the retention window.
                            let sys = SystemOrganizationService::new(state_layer.clone());
                            match sys.revoke_all_org_refresh_tokens(*organization, block_timestamp)
                            {
                                Ok(result) => {
                                    tracing::info!(
                                        organization_id = organization.value(),
                                        revoked_count = result.revoked_count,
                                        "Cascade-revoked refresh tokens on org soft-delete"
                                    );
                                },
                                Err(e) => {
                                    tracing::error!(
                                        organization_id = organization.value(),
                                        error = %e,
                                        "Failed to cascade-revoke tokens on org soft-delete"
                                    );
                                },
                            }
                        }

                        LedgerResponse::OrganizationDeleted {
                            organization_id: *organization,
                            deleted_at,
                            retention_days,
                        }
                    }
                } else {
                    LedgerResponse::Error {
                        code: LedgerErrorCode::NotFound,
                        message: format!("Organization {} not found", organization),
                    }
                };

                // Emit OrganizationDeleted event on successful soft-delete
                if matches!(response, LedgerResponse::OrganizationDeleted { .. }) {
                    // Count active (non-deleted) vaults for this organization
                    let active_vault_count = state
                        .vaults
                        .iter()
                        .filter(|((org, _), meta)| *org == *organization && !meta.deleted)
                        .count();
                    let mut emitter =
                        ApplyPhaseEmitter::for_system(EventAction::OrganizationDeleted)
                            .detail("organization_id", &organization.to_string())
                            .detail("vault_count", &active_vault_count.to_string())
                            .outcome(EventOutcome::Success);
                    if let Some(slug) = org_slug {
                        emitter = emitter.detail("organization_slug", &slug.to_string());
                    }
                    events.push(emitter.build(block_height, *op_index, block_timestamp, ttl_days));
                    *op_index += 1;
                }

                (response, None)
            },

            LedgerRequest::DeleteVault { organization, vault } => {
                let key = (*organization, *vault);
                // Mark vault as deleted (keep heights for historical queries)
                let response = if let Some(vault_meta) = state.vaults.get_mut(&key) {
                    vault_meta.deleted = true;
                    // Re-serialize vault meta after in-place mutation
                    if let Some(blob) = try_encode(vault_meta, "vault_meta") {
                        pending.vaults.push((vault_meta.vault, blob));
                    }

                    // Clean up vault slug index
                    if let Some(slug) = state.vault_id_to_slug.remove(vault) {
                        state.vault_slug_index.remove(&slug);
                        pending.vault_slug_index_deleted.push(slug);
                    }

                    LedgerResponse::VaultDeleted { success: true }
                } else {
                    LedgerResponse::Error {
                        code: LedgerErrorCode::NotFound,
                        message: format!("Vault {}:{} not found", organization, vault),
                    }
                };

                // Emit VaultDeleted event on successful deletion
                if matches!(response, LedgerResponse::VaultDeleted { success: true }) {
                    let org_slug = state.id_to_slug.get(organization).copied();
                    events.push(
                        ApplyPhaseEmitter::for_organization(
                            EventAction::VaultDeleted,
                            *organization,
                            org_slug,
                        )
                        .detail("vault_id", &vault.to_string())
                        .outcome(EventOutcome::Success)
                        .build(
                            block_height,
                            *op_index,
                            block_timestamp,
                            ttl_days,
                        ),
                    );
                    *op_index += 1;
                }

                (response, None)
            },

            LedgerRequest::UpdateOrganization { organization, name } => {
                let org_slug = state.id_to_slug.get(organization).copied();

                let response = match require_active_org_with_state(
                    organization,
                    state,
                    &self.state_layer,
                    "update",
                ) {
                    Ok(state_layer) => match load_org_profile(state_layer, *organization) {
                        Ok(mut profile) => {
                            if let Some(new_name) = name {
                                profile.name = new_name.clone();
                            }
                            profile.updated_at = block_timestamp;
                            if let Err(e) = save_org_profile(state_layer, *organization, &profile) {
                                return (e, None);
                            }
                            if let Err(e) =
                                bump_org_config_version(state_layer, *organization, state, pending)
                            {
                                return (e, None);
                            }
                            LedgerResponse::OrganizationUpdated { organization_id: *organization }
                        },
                        Err(e) => e,
                    },
                    Err(err_response) => err_response,
                };

                // Emit OrganizationUpdated event on success
                if matches!(response, LedgerResponse::OrganizationUpdated { .. }) {
                    let mut emitter =
                        ApplyPhaseEmitter::for_system(EventAction::OrganizationUpdated)
                            .detail("organization_id", &organization.to_string())
                            .outcome(EventOutcome::Success);
                    if let Some(slug) = org_slug {
                        emitter = emitter.detail("organization_slug", &slug.to_string());
                    }
                    events.push(emitter.build(block_height, *op_index, block_timestamp, ttl_days));
                    *op_index += 1;
                }

                (response, None)
            },

            LedgerRequest::RemoveOrganizationMember { organization, target } => {
                let response = match require_active_org_with_state(
                    organization,
                    state,
                    &self.state_layer,
                    "modify",
                ) {
                    Ok(state_layer) => match load_org_profile(state_layer, *organization) {
                        Ok(mut profile) => {
                            let Some(pos) =
                                profile.members.iter().position(|m| m.user_id == *target)
                            else {
                                return error_result(
                                    LedgerErrorCode::FailedPrecondition,
                                    format!(
                                        "User {} is not a member of organization {}",
                                        target, organization
                                    ),
                                );
                            };
                            // Last-admin safety (authoritative at apply time, prevents TOCTOU)
                            let is_last_admin = profile.members[pos].role
                                == OrganizationMemberRole::Admin
                                && profile
                                    .members
                                    .iter()
                                    .filter(|m| m.role == OrganizationMemberRole::Admin)
                                    .count()
                                    <= 1;
                            if is_last_admin {
                                return error_result(
                                    LedgerErrorCode::FailedPrecondition,
                                    format!(
                                        "Cannot remove the last administrator from organization {}",
                                        organization
                                    ),
                                );
                            }
                            profile.members.remove(pos);
                            profile.updated_at = block_timestamp;
                            if let Err(e) = save_org_profile(state_layer, *organization, &profile) {
                                return (e, None);
                            }
                            // Update user→org index
                            if let Some(orgs) = state.user_org_index.get_mut(target) {
                                orgs.remove(organization);
                                if orgs.is_empty() {
                                    state.user_org_index.remove(target);
                                }
                            }
                            // Re-serialize org meta
                            if let Some(org_mut) = state.organizations.get_mut(organization)
                                && let Ok(blob) = encode(org_mut)
                            {
                                pending.organizations.push((*organization, blob));
                            }
                            LedgerResponse::OrganizationMemberRemoved {
                                organization_id: *organization,
                            }
                        },
                        Err(e) => e,
                    },
                    Err(err_response) => err_response,
                };

                if matches!(response, LedgerResponse::OrganizationMemberRemoved { .. }) {
                    events.push(
                        ApplyPhaseEmitter::for_system(EventAction::OrganizationMemberRemoved)
                            .detail("organization_id", &organization.to_string())
                            .detail("target_user_id", &target.to_string())
                            .outcome(EventOutcome::Success)
                            .build(block_height, *op_index, block_timestamp, ttl_days),
                    );
                    *op_index += 1;
                }

                (response, None)
            },

            LedgerRequest::UpdateOrganizationMemberRole { organization, target, role } => {
                let response = match require_active_org_with_state(
                    organization,
                    state,
                    &self.state_layer,
                    "modify",
                ) {
                    Ok(state_layer) => match load_org_profile(state_layer, *organization) {
                        Ok(mut profile) => {
                            let Some(pos) =
                                profile.members.iter().position(|m| m.user_id == *target)
                            else {
                                return error_result(
                                    LedgerErrorCode::FailedPrecondition,
                                    format!(
                                        "User {} is not a member of organization {}",
                                        target, organization
                                    ),
                                );
                            };
                            // Last-admin safety (authoritative at apply time, prevents TOCTOU)
                            if profile.members[pos].role == OrganizationMemberRole::Admin
                                && *role == OrganizationMemberRole::Member
                                && profile
                                    .members
                                    .iter()
                                    .filter(|m| m.role == OrganizationMemberRole::Admin)
                                    .count()
                                    <= 1
                            {
                                return error_result(
                                    LedgerErrorCode::FailedPrecondition,
                                    format!(
                                        "Cannot demote the last administrator of organization {}",
                                        organization
                                    ),
                                );
                            }
                            profile.members[pos].role = *role;
                            profile.updated_at = block_timestamp;
                            if let Err(e) = save_org_profile(state_layer, *organization, &profile) {
                                return (e, None);
                            }
                            // Re-serialize org meta
                            if let Some(org_mut) = state.organizations.get_mut(organization)
                                && let Ok(blob) = encode(org_mut)
                            {
                                pending.organizations.push((*organization, blob));
                            }
                            LedgerResponse::OrganizationMemberRoleUpdated {
                                organization_id: *organization,
                            }
                        },
                        Err(e) => e,
                    },
                    Err(err_response) => err_response,
                };

                if matches!(response, LedgerResponse::OrganizationMemberRoleUpdated { .. }) {
                    events.push(
                        ApplyPhaseEmitter::for_system(EventAction::OrganizationMemberRoleUpdated)
                            .detail("organization_id", &organization.to_string())
                            .detail("target_user_id", &target.to_string())
                            .detail("new_role", &format!("{role:?}"))
                            .outcome(EventOutcome::Success)
                            .build(block_height, *op_index, block_timestamp, ttl_days),
                    );
                    *op_index += 1;
                }

                (response, None)
            },

            LedgerRequest::CreateOrganizationTeam { organization, slug, name } => {
                let response = match require_active_org_with_state(
                    organization,
                    state,
                    &self.state_layer,
                    "modify",
                ) {
                    Ok(state_layer) => {
                        // Check name uniqueness via in-memory index
                        if has_team_name_conflict(state, *organization, name, None) {
                            LedgerResponse::Error {
                                code: LedgerErrorCode::AlreadyExists,
                                message: format!(
                                    "Team name '{}' already exists in organization {}",
                                    name, organization
                                ),
                            }
                        } else {
                            // Check slug uniqueness
                            if state.team_slug_index.contains_key(slug) {
                                return (
                                    LedgerResponse::Error {
                                        code: LedgerErrorCode::AlreadyExists,
                                        message: format!("Team slug '{}' already exists", slug),
                                    },
                                    None,
                                );
                            }
                            let team_id = state.sequences.next_team();
                            let profile = TeamProfile {
                                team: team_id,
                                organization: *organization,
                                slug: *slug,
                                name: name.clone(),
                                members: Vec::new(),
                                created_at: block_timestamp,
                                updated_at: block_timestamp,
                            };
                            let profile_bytes = match encode(&profile) {
                                Ok(bytes) => bytes,
                                Err(e) => {
                                    return (
                                        LedgerResponse::Error {
                                            code: LedgerErrorCode::Internal,
                                            message: format!("Failed to encode team profile: {e}"),
                                        },
                                        None,
                                    );
                                },
                            };
                            let profile_key = SystemKeys::team_profile_key(*organization, team_id);
                            let slug_key = SystemKeys::team_slug_key(*slug);
                            let slug_value =
                                format!("{}:{}", organization.value(), team_id.value());
                            let ops = vec![
                                Operation::SetEntity {
                                    key: profile_key,
                                    value: profile_bytes,
                                    condition: None,
                                    expires_at: None,
                                },
                                Operation::SetEntity {
                                    key: slug_key,
                                    value: slug_value.into_bytes(),
                                    condition: None,
                                    expires_at: None,
                                },
                            ];
                            if let Err(e) = state_layer.apply_operations(SYSTEM_VAULT_ID, &ops, 0) {
                                return (
                                    LedgerResponse::Error {
                                        code: LedgerErrorCode::Internal,
                                        message: format!("Failed to write team profile: {e}"),
                                    },
                                    None,
                                );
                            }
                            // Update slug indices only after successful storage write
                            state.team_slug_index.insert(*slug, (*organization, team_id));
                            state.team_id_to_slug.insert(team_id, *slug);
                            pending.team_slug_index.push((*slug, (*organization, team_id)));

                            // Update team name index
                            state.team_name_index.insert((*organization, name.clone()), team_id);

                            LedgerResponse::OrganizationTeamCreated { team_id, team_slug: *slug }
                        }
                    },
                    Err(err_response) => err_response,
                };

                if matches!(response, LedgerResponse::OrganizationTeamCreated { .. }) {
                    events.push(
                        ApplyPhaseEmitter::for_system(EventAction::TeamCreated)
                            .detail("organization_id", &organization.to_string())
                            .detail("team_slug", &slug.to_string())
                            .detail("team_name", name)
                            .outcome(EventOutcome::Success)
                            .build(block_height, *op_index, block_timestamp, ttl_days),
                    );
                    *op_index += 1;
                }

                (response, None)
            },

            LedgerRequest::DeleteOrganizationTeam { organization, team, move_members_to } => {
                let response = match require_active_org_with_state(
                    organization,
                    state,
                    &self.state_layer,
                    "modify",
                ) {
                    Ok(state_layer) => {
                        match load_team_profile(state_layer, *organization, *team) {
                            Ok(team_profile) => {
                                // Move members if requested
                                if let Some(target_team) = move_members_to {
                                    if *target_team == *team {
                                        return (
                                            LedgerResponse::Error {
                                                code: LedgerErrorCode::FailedPrecondition,
                                                message: "Cannot move members to the same team being deleted".to_string(),
                                            },
                                            None,
                                        );
                                    }
                                    if let Err(resp) = migrate_team_members(
                                        state_layer,
                                        *organization,
                                        &team_profile,
                                        *target_team,
                                        block_timestamp,
                                    ) {
                                        return (resp, None);
                                    }
                                }

                                // Delete team profile and slug index
                                let profile_key =
                                    SystemKeys::team_profile_key(*organization, *team);
                                let slug_key = SystemKeys::team_slug_key(team_profile.slug);
                                let ops = vec![
                                    Operation::DeleteEntity { key: profile_key },
                                    Operation::DeleteEntity { key: slug_key },
                                ];
                                if let Err(e) =
                                    state_layer.apply_operations(SYSTEM_VAULT_ID, &ops, 0)
                                {
                                    return (
                                        LedgerResponse::Error {
                                            code: LedgerErrorCode::Internal,
                                            message: format!("Failed to delete team profile: {e}"),
                                        },
                                        None,
                                    );
                                }

                                // Remove from slug and name indices only after successful
                                // storage write
                                state.team_slug_index.remove(&team_profile.slug);
                                state.team_id_to_slug.remove(team);
                                state
                                    .team_name_index
                                    .remove(&(*organization, team_profile.name.clone()));
                                pending.team_slug_index_deleted.push(team_profile.slug);

                                LedgerResponse::OrganizationTeamDeleted {
                                    organization_id: *organization,
                                }
                            },
                            Err(err_response) => err_response,
                        }
                    },
                    Err(err_response) => err_response,
                };

                if matches!(response, LedgerResponse::OrganizationTeamDeleted { .. }) {
                    events.push(
                        ApplyPhaseEmitter::for_system(EventAction::TeamDeleted)
                            .detail("organization_id", &organization.to_string())
                            .detail("team_id", &team.to_string())
                            .outcome(EventOutcome::Success)
                            .build(block_height, *op_index, block_timestamp, ttl_days),
                    );
                    *op_index += 1;
                }

                (response, None)
            },

            LedgerRequest::UpdateOrganizationTeam { organization, team, name } => {
                let response = match require_active_org_with_state(
                    organization,
                    state,
                    &self.state_layer,
                    "modify",
                ) {
                    Ok(state_layer) => {
                        match load_team_profile(state_layer, *organization, *team) {
                            Ok(mut profile) => {
                                // Track old name for index maintenance after write
                                let renamed = if let Some(new_name) = name {
                                    // Check uniqueness of new name via index
                                    if has_team_name_conflict(
                                        state,
                                        *organization,
                                        new_name,
                                        Some(*team),
                                    ) {
                                        return (
                                            LedgerResponse::Error {
                                                code: LedgerErrorCode::AlreadyExists,
                                                message: format!(
                                                    "Team name '{}' already exists in organization {}",
                                                    new_name, organization
                                                ),
                                            },
                                            None,
                                        );
                                    }
                                    let old_name = profile.name.clone();
                                    profile.name.clone_from(new_name);
                                    Some((old_name, new_name.clone()))
                                } else {
                                    None
                                };
                                profile.updated_at = block_timestamp;
                                let profile_key =
                                    SystemKeys::team_profile_key(*organization, *team);
                                let profile_bytes = match encode(&profile) {
                                    Ok(bytes) => bytes,
                                    Err(e) => {
                                        return (
                                            LedgerResponse::Error {
                                                code: LedgerErrorCode::Internal,
                                                message: format!(
                                                    "Failed to encode team profile: {e}"
                                                ),
                                            },
                                            None,
                                        );
                                    },
                                };
                                let ops = vec![Operation::SetEntity {
                                    key: profile_key,
                                    value: profile_bytes,
                                    condition: None,
                                    expires_at: None,
                                }];
                                if let Err(e) =
                                    state_layer.apply_operations(SYSTEM_VAULT_ID, &ops, 0)
                                {
                                    return (
                                        LedgerResponse::Error {
                                            code: LedgerErrorCode::Internal,
                                            message: format!("Failed to update team profile: {e}"),
                                        },
                                        None,
                                    );
                                }
                                // Update name index after successful write
                                if let Some((old_name, new_name)) = renamed {
                                    state.team_name_index.remove(&(*organization, old_name));
                                    state.team_name_index.insert((*organization, new_name), *team);
                                }
                                LedgerResponse::OrganizationTeamUpdated {
                                    organization_id: *organization,
                                }
                            },
                            Err(err_response) => err_response,
                        }
                    },
                    Err(err_response) => err_response,
                };

                if matches!(response, LedgerResponse::OrganizationTeamUpdated { .. }) {
                    events.push(
                        ApplyPhaseEmitter::for_system(EventAction::TeamUpdated)
                            .detail("organization_id", &organization.to_string())
                            .detail("team_id", &team.to_string())
                            .outcome(EventOutcome::Success)
                            .build(block_height, *op_index, block_timestamp, ttl_days),
                    );
                    *op_index += 1;
                }

                (response, None)
            },

            // ================================================================
            // App operations
            // ================================================================
            LedgerRequest::CreateApp { organization, slug, name, description } => {
                let response = match require_active_org_with_state(
                    organization,
                    state,
                    &self.state_layer,
                    "modify",
                ) {
                    Ok(state_layer) => {
                        if has_app_name_conflict(state, *organization, name, None) {
                            LedgerResponse::Error {
                                code: LedgerErrorCode::AlreadyExists,
                                message: format!(
                                    "App name '{}' already exists in organization {}",
                                    name, organization
                                ),
                            }
                        } else if state.app_slug_index.contains_key(slug) {
                            LedgerResponse::Error {
                                code: LedgerErrorCode::AlreadyExists,
                                message: format!("App slug '{}' already exists", slug),
                            }
                        } else {
                            let app_id = state.sequences.next_app();
                            let app = App {
                                id: app_id,
                                slug: *slug,
                                organization: *organization,
                                name: name.clone(),
                                description: description.clone(),
                                enabled: false,
                                credentials: AppCredentials::default(),
                                created_at: block_timestamp,
                                updated_at: block_timestamp,
                            };
                            let app_bytes = match encode(&app) {
                                Ok(bytes) => bytes,
                                Err(e) => {
                                    return (
                                        LedgerResponse::Error {
                                            code: LedgerErrorCode::Internal,
                                            message: format!("Failed to encode app: {e}"),
                                        },
                                        None,
                                    );
                                },
                            };
                            let app_key = SystemKeys::app_key(*organization, app_id);
                            let slug_key = SystemKeys::app_slug_key(*slug);
                            let slug_value = format!("{}:{}", organization.value(), app_id.value());
                            let name_key = SystemKeys::app_name_index_key(*organization, name);
                            let name_value = app_id.value().to_string();
                            let ops = vec![
                                Operation::SetEntity {
                                    key: app_key,
                                    value: app_bytes,
                                    condition: None,
                                    expires_at: None,
                                },
                                Operation::SetEntity {
                                    key: slug_key,
                                    value: slug_value.into_bytes(),
                                    condition: None,
                                    expires_at: None,
                                },
                                Operation::SetEntity {
                                    key: name_key,
                                    value: name_value.into_bytes(),
                                    condition: None,
                                    expires_at: None,
                                },
                            ];
                            if let Err(e) = state_layer.apply_operations(SYSTEM_VAULT_ID, &ops, 0) {
                                return (
                                    LedgerResponse::Error {
                                        code: LedgerErrorCode::Internal,
                                        message: format!("Failed to write app: {e}"),
                                    },
                                    None,
                                );
                            }
                            state.app_slug_index.insert(*slug, (*organization, app_id));
                            state.app_id_to_slug.insert(app_id, *slug);
                            state.app_name_index.insert((*organization, name.clone()), app_id);
                            pending.app_slug_index.push((*slug, (*organization, app_id)));

                            LedgerResponse::AppCreated { app_id, app_slug: *slug }
                        }
                    },
                    Err(err_response) => err_response,
                };
                (response, None)
            },

            LedgerRequest::UpdateApp { organization, app, name, description } => {
                let response = match require_active_org_with_state(
                    organization,
                    state,
                    &self.state_layer,
                    "modify",
                ) {
                    Ok(state_layer) => {
                        match load_app(state_layer, *organization, *app) {
                            Ok(mut app_record) => {
                                let renamed = if let Some(new_name) = name {
                                    if has_app_name_conflict(
                                        state,
                                        *organization,
                                        new_name,
                                        Some(*app),
                                    ) {
                                        return (
                                            LedgerResponse::Error {
                                                code: LedgerErrorCode::AlreadyExists,
                                                message: format!(
                                                    "App name '{}' already exists in organization {}",
                                                    new_name, organization
                                                ),
                                            },
                                            None,
                                        );
                                    }
                                    let old_name = app_record.name.clone();
                                    app_record.name.clone_from(new_name);
                                    Some((old_name, new_name.clone()))
                                } else {
                                    None
                                };
                                if let Some(new_desc) = description {
                                    app_record.description.clone_from(new_desc);
                                }
                                app_record.updated_at = block_timestamp;
                                match save_app(state_layer, *organization, &app_record) {
                                    Ok(()) => {
                                        // Update name index after successful write
                                        if let Some((old_name, new_name)) = renamed {
                                            // Update state layer name index keys
                                            let old_key = SystemKeys::app_name_index_key(
                                                *organization,
                                                &old_name,
                                            );
                                            let new_key = SystemKeys::app_name_index_key(
                                                *organization,
                                                &new_name,
                                            );
                                            let value = app.value().to_string();
                                            let ops = vec![
                                                Operation::DeleteEntity { key: old_key },
                                                Operation::SetEntity {
                                                    key: new_key,
                                                    value: value.into_bytes(),
                                                    condition: None,
                                                    expires_at: None,
                                                },
                                            ];
                                            if let Err(e) = state_layer.apply_operations(
                                                SYSTEM_VAULT_ID,
                                                &ops,
                                                0,
                                            ) {
                                                return (
                                                    LedgerResponse::Error {
                                                        code: LedgerErrorCode::Internal,
                                                        message: format!(
                                                            "Failed to update app name index: {e}"
                                                        ),
                                                    },
                                                    None,
                                                );
                                            }
                                            state.app_name_index.remove(&(*organization, old_name));
                                            state
                                                .app_name_index
                                                .insert((*organization, new_name), *app);
                                        }
                                        LedgerResponse::AppUpdated {
                                            organization_id: *organization,
                                        }
                                    },
                                    Err(e) => e,
                                }
                            },
                            Err(e) => e,
                        }
                    },
                    Err(err_response) => err_response,
                };
                (response, None)
            },

            LedgerRequest::DeleteApp { organization, app } => {
                let response = match require_active_org_with_state(
                    organization,
                    state,
                    &self.state_layer,
                    "modify",
                ) {
                    Ok(state_layer) => {
                        match load_app(state_layer, *organization, *app) {
                            Ok(app_record) => {
                                // Delete app record, slug index, and name index
                                let app_key = SystemKeys::app_key(*organization, *app);
                                let slug_key = SystemKeys::app_slug_key(app_record.slug);
                                let name_key =
                                    SystemKeys::app_name_index_key(*organization, &app_record.name);
                                let mut ops = vec![
                                    Operation::DeleteEntity { key: app_key },
                                    Operation::DeleteEntity { key: slug_key },
                                    Operation::DeleteEntity { key: name_key },
                                ];
                                // Delete all vault connections (paginated)
                                let vault_prefix =
                                    SystemKeys::app_vault_prefix(*organization, *app);
                                collect_all_entities_for_deletion(
                                    state_layer,
                                    &vault_prefix,
                                    &mut ops,
                                );
                                // Delete all assertion entries (paginated)
                                let assertion_prefix =
                                    SystemKeys::app_assertion_prefix(*organization, *app);
                                collect_all_entities_for_deletion(
                                    state_layer,
                                    &assertion_prefix,
                                    &mut ops,
                                );
                                if let Err(e) =
                                    state_layer.apply_operations(SYSTEM_VAULT_ID, &ops, 0)
                                {
                                    return (
                                        LedgerResponse::Error {
                                            code: LedgerErrorCode::Internal,
                                            message: format!("Failed to delete app: {e}"),
                                        },
                                        None,
                                    );
                                }
                                state.app_slug_index.remove(&app_record.slug);
                                state.app_id_to_slug.remove(app);
                                state.app_name_index.remove(&(*organization, app_record.name));
                                pending.app_slug_index_deleted.push(app_record.slug);

                                LedgerResponse::AppDeleted { organization_id: *organization }
                            },
                            Err(e) => e,
                        }
                    },
                    Err(err_response) => err_response,
                };
                (response, None)
            },

            LedgerRequest::SetAppEnabled { organization, app, enabled } => {
                let response = match require_active_org_with_state(
                    organization,
                    state,
                    &self.state_layer,
                    "modify",
                ) {
                    Ok(state_layer) => match load_app(state_layer, *organization, *app) {
                        Ok(mut app_record) => {
                            let was_enabled = app_record.enabled;
                            app_record.enabled = *enabled;
                            app_record.updated_at = block_timestamp;
                            match save_app(state_layer, *organization, &app_record) {
                                Ok(()) => {
                                    // Cascade: revoke all refresh tokens when disabling an app
                                    if was_enabled && !*enabled {
                                        if let Some(app_slug) = state.app_id_to_slug.get(app) {
                                            let sys =
                                                SystemOrganizationService::new(state_layer.clone());
                                            let subject = TokenSubject::App(*app_slug);
                                            match sys.revoke_all_subject_tokens(
                                                &subject,
                                                block_timestamp,
                                            ) {
                                                Ok(result) => {
                                                    tracing::info!(
                                                        app_id = app.value(),
                                                        app_slug = app_slug.value(),
                                                        revoked_count = result.revoked_count,
                                                        "Cascade-revoked refresh tokens on app disable"
                                                    );
                                                },
                                                Err(e) => {
                                                    tracing::error!(
                                                        app_id = app.value(),
                                                        error = %e,
                                                        "Failed to cascade-revoke tokens on app disable"
                                                    );
                                                },
                                            }
                                        } else {
                                            tracing::warn!(
                                                app_id = app.value(),
                                                "App slug not found for cascade token revocation on disable"
                                            );
                                        }
                                    }
                                    LedgerResponse::AppToggled { organization_id: *organization }
                                },
                                Err(e) => e,
                            }
                        },
                        Err(e) => e,
                    },
                    Err(err_response) => err_response,
                };
                (response, None)
            },

            LedgerRequest::SetAppCredentialEnabled {
                organization,
                app,
                credential_type,
                enabled,
            } => {
                let response = match require_active_org_with_state(
                    organization,
                    state,
                    &self.state_layer,
                    "modify",
                ) {
                    Ok(state_layer) => match load_app(state_layer, *organization, *app) {
                        Ok(mut app_record) => {
                            match credential_type {
                                AppCredentialType::ClientSecret => {
                                    app_record.credentials.client_secret.enabled = *enabled;
                                },
                                AppCredentialType::MtlsCa => {
                                    app_record.credentials.mtls_ca.enabled = *enabled;
                                },
                                AppCredentialType::MtlsSelfSigned => {
                                    app_record.credentials.mtls_self_signed.enabled = *enabled;
                                },
                                AppCredentialType::ClientAssertion => {
                                    app_record.credentials.client_assertion.enabled = *enabled;
                                },
                            }
                            app_record.updated_at = block_timestamp;
                            match save_app(state_layer, *organization, &app_record) {
                                Ok(()) => LedgerResponse::AppCredentialToggled {
                                    organization_id: *organization,
                                },
                                Err(e) => e,
                            }
                        },
                        Err(e) => e,
                    },
                    Err(err_response) => err_response,
                };
                (response, None)
            },

            LedgerRequest::RotateAppClientSecret { organization, app, new_secret_hash } => {
                let response = match require_active_org_with_state(
                    organization,
                    state,
                    &self.state_layer,
                    "modify",
                ) {
                    Ok(state_layer) => match load_app(state_layer, *organization, *app) {
                        Ok(mut app_record) => {
                            app_record.credentials.client_secret.secret_hash =
                                Some(new_secret_hash.clone());
                            app_record.credentials.client_secret.rotated_at = Some(block_timestamp);
                            app_record.updated_at = block_timestamp;
                            match save_app(state_layer, *organization, &app_record) {
                                Ok(()) => LedgerResponse::AppClientSecretRotated {
                                    organization_id: *organization,
                                },
                                Err(e) => e,
                            }
                        },
                        Err(e) => e,
                    },
                    Err(err_response) => err_response,
                };
                (response, None)
            },

            LedgerRequest::CreateAppClientAssertion {
                organization,
                app,
                name,
                expires_at,
                public_key_bytes,
            } => {
                let response = match require_active_org_with_state(
                    organization,
                    state,
                    &self.state_layer,
                    "modify",
                ) {
                    Ok(state_layer) => {
                        // Verify app exists
                        match load_app(state_layer, *organization, *app) {
                            Ok(_) => {
                                let assertion_id = state.sequences.next_client_assertion();
                                let entry = ClientAssertionEntry {
                                    id: assertion_id,
                                    name: name.clone(),
                                    enabled: true,
                                    expires_at: *expires_at,
                                    public_key_bytes: public_key_bytes.clone(),
                                    created_at: block_timestamp,
                                };
                                let entry_bytes = match encode(&entry) {
                                    Ok(bytes) => bytes,
                                    Err(e) => {
                                        return (
                                            LedgerResponse::Error {
                                                code: LedgerErrorCode::Internal,
                                                message: format!(
                                                    "Failed to encode assertion entry: {e}"
                                                ),
                                            },
                                            None,
                                        );
                                    },
                                };
                                let key = SystemKeys::app_assertion_key(
                                    *organization,
                                    *app,
                                    assertion_id,
                                );
                                let ops = vec![Operation::SetEntity {
                                    key,
                                    value: entry_bytes,
                                    condition: None,
                                    expires_at: None,
                                }];
                                if let Err(e) =
                                    state_layer.apply_operations(SYSTEM_VAULT_ID, &ops, 0)
                                {
                                    return (
                                        LedgerResponse::Error {
                                            code: LedgerErrorCode::Internal,
                                            message: format!(
                                                "Failed to write assertion entry: {e}"
                                            ),
                                        },
                                        None,
                                    );
                                }
                                LedgerResponse::AppClientAssertionCreated { assertion_id }
                            },
                            Err(e) => e,
                        }
                    },
                    Err(err_response) => err_response,
                };
                (response, None)
            },

            LedgerRequest::DeleteAppClientAssertion { organization, app, assertion } => {
                let response = match require_active_org_with_state(
                    organization,
                    state,
                    &self.state_layer,
                    "modify",
                ) {
                    Ok(state_layer) => {
                        let key = SystemKeys::app_assertion_key(*organization, *app, *assertion);
                        // Verify entry exists
                        match state_layer.get_entity(SYSTEM_VAULT_ID, key.as_bytes()) {
                            Ok(Some(_)) => {
                                let ops = vec![Operation::DeleteEntity { key }];
                                if let Err(e) =
                                    state_layer.apply_operations(SYSTEM_VAULT_ID, &ops, 0)
                                {
                                    return (
                                        LedgerResponse::Error {
                                            code: LedgerErrorCode::Internal,
                                            message: format!(
                                                "Failed to delete assertion entry: {e}"
                                            ),
                                        },
                                        None,
                                    );
                                }
                                LedgerResponse::AppClientAssertionDeleted {
                                    organization_id: *organization,
                                }
                            },
                            Ok(None) => LedgerResponse::Error {
                                code: LedgerErrorCode::NotFound,
                                message: format!(
                                    "Assertion {} not found for app {} in organization {}",
                                    assertion, app, organization
                                ),
                            },
                            Err(e) => LedgerResponse::Error {
                                code: LedgerErrorCode::Internal,
                                message: format!("Failed to read assertion entry: {e}"),
                            },
                        }
                    },
                    Err(err_response) => err_response,
                };
                (response, None)
            },

            LedgerRequest::SetAppClientAssertionEnabled {
                organization,
                app,
                assertion,
                enabled,
            } => {
                let response = match require_active_org_with_state(
                    organization,
                    state,
                    &self.state_layer,
                    "modify",
                ) {
                    Ok(state_layer) => {
                        let key = SystemKeys::app_assertion_key(*organization, *app, *assertion);
                        match state_layer.get_entity(SYSTEM_VAULT_ID, key.as_bytes()) {
                            Ok(Some(entity)) => {
                                match decode::<ClientAssertionEntry>(&entity.value) {
                                    Ok(mut entry) => {
                                        entry.enabled = *enabled;
                                        match encode(&entry) {
                                            Ok(bytes) => {
                                                let ops = vec![Operation::SetEntity {
                                                    key,
                                                    value: bytes,
                                                    condition: None,
                                                    expires_at: None,
                                                }];
                                                if let Err(e) = state_layer.apply_operations(
                                                    SYSTEM_VAULT_ID,
                                                    &ops,
                                                    0,
                                                ) {
                                                    return (
                                                        LedgerResponse::Error {
                                                            code: LedgerErrorCode::Internal,
                                                            message: format!(
                                                                "Failed to update assertion: {e}"
                                                            ),
                                                        },
                                                        None,
                                                    );
                                                }
                                                LedgerResponse::AppClientAssertionToggled {
                                                    organization_id: *organization,
                                                }
                                            },
                                            Err(e) => LedgerResponse::Error {
                                                code: LedgerErrorCode::Internal,
                                                message: format!("Failed to encode assertion: {e}"),
                                            },
                                        }
                                    },
                                    Err(e) => LedgerResponse::Error {
                                        code: LedgerErrorCode::Internal,
                                        message: format!("Failed to decode assertion: {e}"),
                                    },
                                }
                            },
                            Ok(None) => LedgerResponse::Error {
                                code: LedgerErrorCode::NotFound,
                                message: format!(
                                    "Assertion {} not found for app {} in organization {}",
                                    assertion, app, organization
                                ),
                            },
                            Err(e) => LedgerResponse::Error {
                                code: LedgerErrorCode::Internal,
                                message: format!("Failed to read assertion: {e}"),
                            },
                        }
                    },
                    Err(err_response) => err_response,
                };
                (response, None)
            },

            LedgerRequest::AddAppVault { organization, app, vault, vault_slug, allowed_scopes } => {
                let response = match require_active_org_with_state(
                    organization,
                    state,
                    &self.state_layer,
                    "modify",
                ) {
                    Ok(state_layer) => {
                        // Verify app exists
                        match load_app(state_layer, *organization, *app) {
                            Ok(_) => {
                                let key = SystemKeys::app_vault_key(*organization, *app, *vault);
                                // Check if connection already exists
                                match state_layer.get_entity(SYSTEM_VAULT_ID, key.as_bytes()) {
                                    Ok(Some(_)) => LedgerResponse::Error {
                                        code: LedgerErrorCode::AlreadyExists,
                                        message: format!(
                                            "Vault connection already exists for vault {} on app {}",
                                            vault, app
                                        ),
                                    },
                                    Ok(None) => {
                                        let connection = AppVaultConnection {
                                            vault_id: *vault,
                                            vault_slug: *vault_slug,
                                            allowed_scopes: allowed_scopes.clone(),
                                            created_at: block_timestamp,
                                            updated_at: block_timestamp,
                                        };
                                        let bytes = match encode(&connection) {
                                            Ok(b) => b,
                                            Err(e) => {
                                                return (
                                                    LedgerResponse::Error {
                                                        code: LedgerErrorCode::Internal,
                                                        message: format!(
                                                            "Failed to encode vault connection: {e}"
                                                        ),
                                                    },
                                                    None,
                                                );
                                            },
                                        };
                                        let ops = vec![Operation::SetEntity {
                                            key,
                                            value: bytes,
                                            condition: None,
                                            expires_at: None,
                                        }];
                                        if let Err(e) =
                                            state_layer.apply_operations(SYSTEM_VAULT_ID, &ops, 0)
                                        {
                                            return (
                                                LedgerResponse::Error {
                                                    code: LedgerErrorCode::Internal,
                                                    message: format!(
                                                        "Failed to write vault connection: {e}"
                                                    ),
                                                },
                                                None,
                                            );
                                        }
                                        LedgerResponse::AppVaultAdded {
                                            organization_id: *organization,
                                        }
                                    },
                                    Err(e) => LedgerResponse::Error {
                                        code: LedgerErrorCode::Internal,
                                        message: format!("Failed to check vault connection: {e}"),
                                    },
                                }
                            },
                            Err(e) => e,
                        }
                    },
                    Err(err_response) => err_response,
                };
                (response, None)
            },

            LedgerRequest::UpdateAppVault { organization, app, vault, allowed_scopes } => {
                let response = match require_active_org_with_state(
                    organization,
                    state,
                    &self.state_layer,
                    "modify",
                ) {
                    Ok(state_layer) => {
                        let key = SystemKeys::app_vault_key(*organization, *app, *vault);
                        match state_layer.get_entity(SYSTEM_VAULT_ID, key.as_bytes()) {
                            Ok(Some(entity)) => match decode::<AppVaultConnection>(&entity.value) {
                                Ok(mut connection) => {
                                    connection.allowed_scopes = allowed_scopes.clone();
                                    connection.updated_at = block_timestamp;
                                    match encode(&connection) {
                                        Ok(bytes) => {
                                            let ops = vec![Operation::SetEntity {
                                                key,
                                                value: bytes,
                                                condition: None,
                                                expires_at: None,
                                            }];
                                            if let Err(e) = state_layer.apply_operations(
                                                SYSTEM_VAULT_ID,
                                                &ops,
                                                0,
                                            ) {
                                                return (
                                                    LedgerResponse::Error {
                                                        code: LedgerErrorCode::Internal,
                                                        message: format!(
                                                            "Failed to update vault connection: {e}"
                                                        ),
                                                    },
                                                    None,
                                                );
                                            }
                                            LedgerResponse::AppVaultUpdated {
                                                organization_id: *organization,
                                            }
                                        },
                                        Err(e) => LedgerResponse::Error {
                                            code: LedgerErrorCode::Internal,
                                            message: format!(
                                                "Failed to encode vault connection: {e}"
                                            ),
                                        },
                                    }
                                },
                                Err(e) => LedgerResponse::Error {
                                    code: LedgerErrorCode::Internal,
                                    message: format!("Failed to decode vault connection: {e}"),
                                },
                            },
                            Ok(None) => LedgerResponse::Error {
                                code: LedgerErrorCode::NotFound,
                                message: format!(
                                    "Vault connection not found for vault {} on app {}",
                                    vault, app
                                ),
                            },
                            Err(e) => LedgerResponse::Error {
                                code: LedgerErrorCode::Internal,
                                message: format!("Failed to read vault connection: {e}"),
                            },
                        }
                    },
                    Err(err_response) => err_response,
                };
                (response, None)
            },

            LedgerRequest::RemoveAppVault { organization, app, vault } => {
                let response = match require_active_org_with_state(
                    organization,
                    state,
                    &self.state_layer,
                    "modify",
                ) {
                    Ok(state_layer) => {
                        let key = SystemKeys::app_vault_key(*organization, *app, *vault);
                        match state_layer.get_entity(SYSTEM_VAULT_ID, key.as_bytes()) {
                            Ok(Some(_)) => {
                                let ops = vec![Operation::DeleteEntity { key }];
                                if let Err(e) =
                                    state_layer.apply_operations(SYSTEM_VAULT_ID, &ops, 0)
                                {
                                    return (
                                        LedgerResponse::Error {
                                            code: LedgerErrorCode::Internal,
                                            message: format!(
                                                "Failed to remove vault connection: {e}"
                                            ),
                                        },
                                        None,
                                    );
                                }
                                // Cascade: revoke all refresh tokens for this app+vault
                                if let Some(app_slug) = state.app_id_to_slug.get(app) {
                                    let sys = SystemOrganizationService::new(state_layer.clone());
                                    match sys.revoke_app_vault_tokens(
                                        *app_slug,
                                        *vault,
                                        block_timestamp,
                                    ) {
                                        Ok(result) => {
                                            tracing::info!(
                                                app_id = app.value(),
                                                vault_id = vault.value(),
                                                revoked_count = result.revoked_count,
                                                "Cascade-revoked refresh tokens on vault disconnect"
                                            );
                                        },
                                        Err(e) => {
                                            tracing::error!(
                                                app_id = app.value(),
                                                vault_id = vault.value(),
                                                error = %e,
                                                "Failed to cascade-revoke tokens on vault disconnect"
                                            );
                                        },
                                    }
                                } else {
                                    tracing::warn!(
                                        app_id = app.value(),
                                        vault_id = vault.value(),
                                        "App slug not found for cascade token revocation on vault disconnect"
                                    );
                                }
                                LedgerResponse::AppVaultRemoved { organization_id: *organization }
                            },
                            Ok(None) => LedgerResponse::Error {
                                code: LedgerErrorCode::NotFound,
                                message: format!(
                                    "Vault connection not found for vault {} on app {}",
                                    vault, app
                                ),
                            },
                            Err(e) => LedgerResponse::Error {
                                code: LedgerErrorCode::Internal,
                                message: format!("Failed to read vault connection: {e}"),
                            },
                        }
                    },
                    Err(err_response) => err_response,
                };
                (response, None)
            },

            LedgerRequest::PurgeOrganization { organization } => {
                let org_slug = state.id_to_slug.get(organization).copied();

                let response = if let Some(org) = state.organizations.get(organization) {
                    if org.status != OrganizationStatus::Deleted {
                        LedgerResponse::Error {
                            code: LedgerErrorCode::FailedPrecondition,
                            message: format!(
                                "Cannot purge organization {} — status must be Deleted, got {:?}",
                                organization, org.status
                            ),
                        }
                    } else {
                        // Force-delete all vaults belonging to this organization
                        let vault_keys: Vec<(OrganizationId, VaultId)> = state
                            .vaults
                            .keys()
                            .filter(|(org, _)| *org == *organization)
                            .copied()
                            .collect();

                        for key in &vault_keys {
                            if let Some(vault_meta) = state.vaults.get_mut(key)
                                && !vault_meta.deleted
                            {
                                vault_meta.deleted = true;
                                if let Some(blob) = try_encode(vault_meta, "vault_meta") {
                                    pending.vaults.push((vault_meta.vault, blob));
                                }
                            }
                            // Clean up vault slug index
                            if let Some(slug) = state.vault_id_to_slug.remove(&key.1) {
                                state.vault_slug_index.remove(&slug);
                                pending.vault_slug_index_deleted.push(slug);
                            }
                        }

                        // Clean up team slug indices for all teams in this organization
                        let team_slugs_to_remove: Vec<(TeamSlug, TeamId)> = state
                            .team_slug_index
                            .iter()
                            .filter(|(_, (org_id, _))| *org_id == *organization)
                            .map(|(slug, (_, team_id))| (*slug, *team_id))
                            .collect();
                        for (team_slug, team_id) in &team_slugs_to_remove {
                            state.team_slug_index.remove(team_slug);
                            state.team_id_to_slug.remove(team_id);
                            pending.team_slug_index_deleted.push(*team_slug);
                        }

                        // Clean up team name index entries for this organization
                        state.team_name_index.retain(|(org_id, _), _| *org_id != *organization);

                        // Clean up app slug indices for all apps in this organization
                        let app_slugs_to_remove: Vec<(AppSlug, AppId)> = state
                            .app_slug_index
                            .iter()
                            .filter(|(_, (org_id, _))| *org_id == *organization)
                            .map(|(slug, (_, app_id))| (*slug, *app_id))
                            .collect();
                        for (app_slug, app_id) in &app_slugs_to_remove {
                            state.app_slug_index.remove(app_slug);
                            state.app_id_to_slug.remove(app_id);
                            pending.app_slug_index_deleted.push(*app_slug);
                        }

                        // Clean up app name index entries for this organization
                        state.app_name_index.retain(|(org_id, _), _| *org_id != *organization);

                        // Clean up user→org index entries for this organization
                        state.user_org_index.retain(|_, orgs| {
                            orgs.remove(organization);
                            !orgs.is_empty()
                        });

                        // Remove organization slug index entries
                        if let Some(slug) = state.id_to_slug.remove(organization) {
                            state.slug_index.remove(&slug);
                            pending.slug_index_deleted.push(slug);
                        }

                        // Remove the organization from state
                        state.organizations.remove(organization);
                        pending.organizations_deleted.push(*organization);

                        // Clean up state layer entities (registry, profile, slug index key)
                        if let Some(state_layer) = &self.state_layer {
                            let mut cleanup_ops = vec![
                                Operation::DeleteEntity {
                                    key: SystemKeys::organization_key(*organization),
                                },
                                Operation::DeleteEntity {
                                    key: SystemKeys::organization_profile_key(*organization),
                                },
                            ];
                            if let Some(slug) = org_slug {
                                cleanup_ops.push(Operation::DeleteEntity {
                                    key: SystemKeys::organization_slug_key(slug),
                                });
                            }
                            // Clean up team profile + slug index keys
                            for (team_slug, team_id) in &team_slugs_to_remove {
                                cleanup_ops.push(Operation::DeleteEntity {
                                    key: SystemKeys::team_profile_key(*organization, *team_id),
                                });
                                cleanup_ops.push(Operation::DeleteEntity {
                                    key: SystemKeys::team_slug_key(*team_slug),
                                });
                            }
                            // Clean up app records, slug index keys, and name index keys
                            for (app_slug, app_id) in &app_slugs_to_remove {
                                cleanup_ops.push(Operation::DeleteEntity {
                                    key: SystemKeys::app_key(*organization, *app_id),
                                });
                                cleanup_ops.push(Operation::DeleteEntity {
                                    key: SystemKeys::app_slug_key(*app_slug),
                                });
                                // Clean up sub-resources (assertions, vault connections)
                                let assertion_prefix =
                                    SystemKeys::app_assertion_prefix(*organization, *app_id);
                                collect_all_entities_for_deletion(
                                    state_layer,
                                    &assertion_prefix,
                                    &mut cleanup_ops,
                                );
                                let vault_prefix =
                                    SystemKeys::app_vault_prefix(*organization, *app_id);
                                collect_all_entities_for_deletion(
                                    state_layer,
                                    &vault_prefix,
                                    &mut cleanup_ops,
                                );
                            }
                            // Clean up app name index keys for this organization
                            // (scan by prefix since we don't have individual name→key mappings)
                            let app_name_prefix = SystemKeys::app_name_index_prefix(*organization);
                            if let Ok(entities) = state_layer.list_entities(
                                SYSTEM_VAULT_ID,
                                Some(&app_name_prefix),
                                None,
                                10000,
                            ) {
                                for entity in &entities {
                                    cleanup_ops.push(Operation::DeleteEntity {
                                        key: String::from_utf8_lossy(&entity.key).to_string(),
                                    });
                                }
                            }
                            if let Err(e) =
                                state_layer.apply_operations(SYSTEM_VAULT_ID, &cleanup_ops, 0)
                            {
                                tracing::error!(
                                    organization_id = organization.value(),
                                    error = %e,
                                    "Failed to clean up state layer entities during purge"
                                );
                            }

                            // Cascade: delete org signing keys and revoke all refresh tokens
                            let sys = SystemOrganizationService::new(state_layer.clone());
                            match sys.delete_org_signing_keys(*organization) {
                                Ok(count) => {
                                    tracing::info!(
                                        organization_id = organization.value(),
                                        deleted_keys = count,
                                        "Deleted org signing keys during purge"
                                    );
                                },
                                Err(e) => {
                                    tracing::error!(
                                        organization_id = organization.value(),
                                        error = %e,
                                        "Failed to delete org signing keys during purge"
                                    );
                                },
                            }
                            match sys.revoke_all_org_refresh_tokens(*organization, block_timestamp)
                            {
                                Ok(result) => {
                                    tracing::info!(
                                        organization_id = organization.value(),
                                        revoked_count = result.revoked_count,
                                        "Revoked org refresh tokens during purge"
                                    );
                                },
                                Err(e) => {
                                    tracing::error!(
                                        organization_id = organization.value(),
                                        error = %e,
                                        "Failed to revoke org refresh tokens during purge"
                                    );
                                },
                            }
                        }

                        LedgerResponse::OrganizationPurged { organization_id: *organization }
                    }
                } else {
                    LedgerResponse::Error {
                        code: LedgerErrorCode::NotFound,
                        message: format!("Organization {} not found", organization),
                    }
                };

                // Emit OrganizationPurged event on success
                if matches!(response, LedgerResponse::OrganizationPurged { .. }) {
                    let mut emitter =
                        ApplyPhaseEmitter::for_system(EventAction::OrganizationPurged)
                            .detail("organization_id", &organization.to_string())
                            .outcome(EventOutcome::Success);
                    if let Some(slug) = org_slug {
                        emitter = emitter.detail("organization_slug", &slug.to_string());
                    }
                    events.push(emitter.build(block_height, *op_index, block_timestamp, ttl_days));
                    *op_index += 1;
                }

                (response, None)
            },

            LedgerRequest::SuspendOrganization { organization, reason } => {
                let org_slug = state.id_to_slug.get(organization).copied();
                let response = if let Some(org) = state.organizations.get_mut(organization) {
                    match org.status {
                        OrganizationStatus::Deleted => LedgerResponse::Error {
                            code: LedgerErrorCode::FailedPrecondition,
                            message: format!(
                                "Cannot suspend deleted organization {}",
                                organization
                            ),
                        },
                        OrganizationStatus::Suspended => LedgerResponse::Error {
                            code: LedgerErrorCode::FailedPrecondition,
                            message: format!("Organization {} is already suspended", organization),
                        },
                        _ => {
                            org.status = OrganizationStatus::Suspended;
                            if let Some(blob) = try_encode(org, "organization") {
                                pending.organizations.push((*organization, blob));
                            }
                            LedgerResponse::OrganizationSuspended { organization: *organization }
                        },
                    }
                } else {
                    LedgerResponse::Error {
                        code: LedgerErrorCode::NotFound,
                        message: format!("Organization {} not found", organization),
                    }
                };

                // Emit OrganizationSuspended event on success
                if matches!(response, LedgerResponse::OrganizationSuspended { .. }) {
                    let mut emitter =
                        ApplyPhaseEmitter::for_system(EventAction::OrganizationSuspended)
                            .detail("organization_id", &organization.to_string())
                            .outcome(EventOutcome::Success);
                    if let Some(slug) = org_slug {
                        emitter = emitter.detail("organization_slug", &slug.to_string());
                    }
                    if let Some(r) = reason {
                        emitter = emitter.detail("reason", r);
                    }
                    events.push(emitter.build(block_height, *op_index, block_timestamp, ttl_days));
                    *op_index += 1;
                }

                (response, None)
            },

            LedgerRequest::ResumeOrganization { organization } => {
                let org_slug = state.id_to_slug.get(organization).copied();
                let response = if let Some(org) = state.organizations.get_mut(organization) {
                    match org.status {
                        OrganizationStatus::Suspended => {
                            org.status = OrganizationStatus::Active;
                            if let Some(blob) = try_encode(org, "organization") {
                                pending.organizations.push((*organization, blob));
                            }
                            LedgerResponse::OrganizationResumed { organization: *organization }
                        },
                        OrganizationStatus::Active => LedgerResponse::Error {
                            code: LedgerErrorCode::FailedPrecondition,
                            message: format!("Organization {} is not suspended", organization),
                        },
                        OrganizationStatus::Deleted => LedgerResponse::Error {
                            code: LedgerErrorCode::FailedPrecondition,
                            message: format!("Cannot resume deleted organization {}", organization),
                        },
                        other => LedgerResponse::Error {
                            code: LedgerErrorCode::FailedPrecondition,
                            message: format!(
                                "Cannot resume organization {} in state {:?}",
                                organization, other
                            ),
                        },
                    }
                } else {
                    LedgerResponse::Error {
                        code: LedgerErrorCode::NotFound,
                        message: format!("Organization {} not found", organization),
                    }
                };

                // Emit OrganizationResumed event on success
                if matches!(response, LedgerResponse::OrganizationResumed { .. }) {
                    let mut emitter =
                        ApplyPhaseEmitter::for_system(EventAction::OrganizationResumed)
                            .detail("organization_id", &organization.to_string())
                            .outcome(EventOutcome::Success);
                    if let Some(slug) = org_slug {
                        emitter = emitter.detail("organization_slug", &slug.to_string());
                    }
                    events.push(emitter.build(block_height, *op_index, block_timestamp, ttl_days));
                    *op_index += 1;
                }

                (response, None)
            },

            LedgerRequest::StartMigration { organization, target_region_group } => {
                let response = if let Some(org) = state.organizations.get_mut(organization) {
                    match org.status {
                        OrganizationStatus::Active => {
                            // Validate target region is different
                            if *target_region_group == org.region {
                                LedgerResponse::Error {
                                    code: LedgerErrorCode::FailedPrecondition,
                                    message: format!(
                                        "Organization {} is already on region {}",
                                        organization, target_region_group
                                    ),
                                }
                            } else {
                                org.status = OrganizationStatus::Migrating;
                                org.pending_region = Some(*target_region_group);
                                if let Some(blob) = try_encode(org, "organization") {
                                    pending.organizations.push((*organization, blob));
                                }
                                LedgerResponse::MigrationStarted {
                                    organization: *organization,
                                    target_region_group: *target_region_group,
                                }
                            }
                        },
                        OrganizationStatus::Migrating => LedgerResponse::Error {
                            code: LedgerErrorCode::FailedPrecondition,
                            message: format!("Organization {} is already migrating", organization),
                        },
                        OrganizationStatus::Suspended => LedgerResponse::Error {
                            code: LedgerErrorCode::FailedPrecondition,
                            message: format!(
                                "Cannot start migration on suspended organization {}",
                                organization
                            ),
                        },
                        OrganizationStatus::Provisioning => LedgerResponse::Error {
                            code: LedgerErrorCode::FailedPrecondition,
                            message: format!(
                                "Cannot start migration on provisioning organization {}",
                                organization
                            ),
                        },
                        OrganizationStatus::Deleted => LedgerResponse::Error {
                            code: LedgerErrorCode::FailedPrecondition,
                            message: format!(
                                "Cannot start migration on deleted organization {}",
                                organization
                            ),
                        },
                    }
                } else {
                    LedgerResponse::Error {
                        code: LedgerErrorCode::NotFound,
                        message: format!("Organization {} not found", organization),
                    }
                };

                // Emit MigrationStarted event on success
                if matches!(response, LedgerResponse::MigrationStarted { .. }) {
                    events.push(
                        ApplyPhaseEmitter::for_system(EventAction::MigrationStarted)
                            .detail("organization_id", &organization.to_string())
                            .detail("target_region_group", &target_region_group.to_string())
                            .outcome(EventOutcome::Success)
                            .build(block_height, *op_index, block_timestamp, ttl_days),
                    );
                    *op_index += 1;
                }

                (response, None)
            },

            LedgerRequest::CompleteMigration { organization } => {
                let response = if let Some(org) = state.organizations.get_mut(organization) {
                    match org.status {
                        OrganizationStatus::Migrating => {
                            if let Some(target_region_group) = org.pending_region {
                                let old_region = org.region;
                                org.region = target_region_group;
                                org.status = OrganizationStatus::Active;
                                org.pending_region = None;
                                if let Some(blob) = try_encode(org, "organization") {
                                    pending.organizations.push((*organization, blob));
                                }
                                LedgerResponse::MigrationCompleted {
                                    organization: *organization,
                                    old_region,
                                    new_region: target_region_group,
                                }
                            } else {
                                // Should not happen, but handle gracefully
                                LedgerResponse::Error {
                                    code: LedgerErrorCode::FailedPrecondition,
                                    message: format!(
                                        "Organization {} is migrating but has no target region",
                                        organization
                                    ),
                                }
                            }
                        },
                        OrganizationStatus::Active => LedgerResponse::Error {
                            code: LedgerErrorCode::FailedPrecondition,
                            message: format!("Organization {} is not migrating", organization),
                        },
                        other => LedgerResponse::Error {
                            code: LedgerErrorCode::FailedPrecondition,
                            message: format!(
                                "Cannot complete migration for organization {} in state {:?}",
                                organization, other
                            ),
                        },
                    }
                } else {
                    LedgerResponse::Error {
                        code: LedgerErrorCode::NotFound,
                        message: format!("Organization {} not found", organization),
                    }
                };

                // Emit MigrationCompleted event on success
                if let LedgerResponse::MigrationCompleted { new_region, .. } = &response {
                    events.push(
                        ApplyPhaseEmitter::for_system(EventAction::MigrationCompleted)
                            .detail("organization_id", &organization.to_string())
                            .detail("region", &new_region.to_string())
                            .outcome(EventOutcome::Success)
                            .build(block_height, *op_index, block_timestamp, ttl_days),
                    );
                    *op_index += 1;
                }

                (response, None)
            },

            LedgerRequest::UpdateVaultHealth {
                organization,
                vault,
                healthy,
                expected_root,
                computed_root,
                diverged_at_height,
                recovery_attempt,
                recovery_started_at,
            } => {
                let key = (*organization, *vault);
                if *healthy {
                    // Mark vault as healthy
                    let status = VaultHealthStatus::Healthy;
                    crate::metrics::set_vault_health(*organization, *vault, status.as_str());
                    state.vault_health.insert(key, status.clone());
                    pending.vault_health.push((key, status));
                    tracing::info!(
                        organization_id = organization.value(),
                        vault_id = vault.value(),
                        "Vault health updated to Healthy via Raft"
                    );
                } else if let (Some(attempt), Some(started_at)) =
                    (recovery_attempt, recovery_started_at)
                {
                    // Mark vault as recovering
                    let status = VaultHealthStatus::Recovering {
                        started_at: *started_at,
                        attempt: *attempt,
                    };
                    crate::metrics::set_vault_health(*organization, *vault, status.as_str());
                    state.vault_health.insert(key, status.clone());
                    pending.vault_health.push((key, status));
                    tracing::info!(
                        organization_id = organization.value(),
                        vault_id = vault.value(),
                        attempt,
                        "Vault health updated to Recovering via Raft"
                    );
                } else {
                    // Mark vault as diverged
                    let expected = expected_root.unwrap_or(inferadb_ledger_types::ZERO_HASH);
                    let computed = computed_root.unwrap_or(inferadb_ledger_types::ZERO_HASH);
                    let at_height = diverged_at_height.unwrap_or(0);
                    let status = VaultHealthStatus::Diverged { expected, computed, at_height };
                    crate::metrics::set_vault_health(*organization, *vault, status.as_str());
                    state.vault_health.insert(key, status.clone());
                    pending.vault_health.push((key, status));
                    tracing::warn!(
                        organization_id = organization.value(),
                        vault_id = vault.value(),
                        at_height,
                        "Vault health updated to Diverged via Raft"
                    );
                }
                // Emit VaultHealthUpdated event
                let org_slug = state.id_to_slug.get(organization).copied();
                let vault_slug = state.vault_id_to_slug.get(vault).copied();
                let health_label =
                    state.vault_health.get(&key).map_or("healthy", VaultHealthStatus::as_str);
                let mut emitter = ApplyPhaseEmitter::for_organization(
                    EventAction::VaultHealthUpdated,
                    *organization,
                    org_slug,
                )
                .detail("health_status", health_label)
                .outcome(EventOutcome::Success);
                if let Some(vs) = vault_slug {
                    emitter = emitter.vault(vs);
                }
                events.push(emitter.build(block_height, *op_index, block_timestamp, ttl_days));
                *op_index += 1;

                (LedgerResponse::VaultHealthUpdated { success: true }, None)
            },

            LedgerRequest::System(system_request) => {
                // Construct once, reused by all SystemRequest arms that need state layer access.
                let sys_service = self.state_layer.as_ref().map(|sl| {
                    inferadb_ledger_state::system::SystemOrganizationService::new(sl.clone())
                });
                let response = match system_request {
                    SystemRequest::CreateUser { user, slug, .. } => {
                        let user_id = *user;
                        let slug = *slug;
                        state.user_slug_index.insert(slug, user_id);
                        state.user_id_to_slug.insert(user_id, slug);
                        pending.user_slug_index.push((slug, user_id));
                        LedgerResponse::UserCreated { user_id, slug }
                    },
                    SystemRequest::UpdateUser { user_id, name, role, primary_email } => {
                        if let Some(sys) = &sys_service {
                            match sys.update_user(*user_id, name.as_deref(), *role, *primary_email)
                            {
                                Ok(_user) => LedgerResponse::UserUpdated { user_id: *user_id },
                                Err(e) => LedgerResponse::Error {
                                    code: LedgerErrorCode::Internal,
                                    message: format!("Failed to update user: {e}"),
                                },
                            }
                        } else {
                            LedgerResponse::Empty
                        }
                    },
                    SystemRequest::DeleteUser { user_id } => {
                        if let Some(sys) = &sys_service {
                            match sys.soft_delete_user(*user_id) {
                                Ok(user) => {
                                    let retention_days = user.region.retention_days();
                                    LedgerResponse::UserSoftDeleted {
                                        user_id: *user_id,
                                        retention_days,
                                    }
                                },
                                Err(e) => LedgerResponse::Error {
                                    code: LedgerErrorCode::Internal,
                                    message: format!("Failed to soft-delete user: {e}"),
                                },
                            }
                        } else {
                            LedgerResponse::Empty
                        }
                    },
                    SystemRequest::CreateUserEmail { user_id, email } => {
                        if let Some(sys) = &sys_service {
                            match sys.create_user_email_record(*user_id, email) {
                                Ok(email_id) => LedgerResponse::UserEmailCreated { email_id },
                                Err(e) => LedgerResponse::Error {
                                    code: LedgerErrorCode::Internal,
                                    message: format!("Failed to create user email: {e}"),
                                },
                            }
                        } else {
                            LedgerResponse::Empty
                        }
                    },
                    SystemRequest::DeleteUserEmail { user_id, email_id } => {
                        if let Some(sys) = &sys_service {
                            match sys.delete_user_email_record(*user_id, *email_id) {
                                Ok(()) => LedgerResponse::UserEmailDeleted { email_id: *email_id },
                                Err(e) => LedgerResponse::Error {
                                    code: LedgerErrorCode::Internal,
                                    message: format!("Failed to delete user email: {e}"),
                                },
                            }
                        } else {
                            LedgerResponse::Empty
                        }
                    },
                    SystemRequest::VerifyUserEmail { email_id } => {
                        if let Some(sys) = &sys_service {
                            match sys.verify_user_email_record(*email_id) {
                                Ok(_email) => {
                                    LedgerResponse::UserEmailVerified { email_id: *email_id }
                                },
                                Err(e) => LedgerResponse::Error {
                                    code: LedgerErrorCode::Internal,
                                    message: format!("Failed to verify user email: {e}"),
                                },
                            }
                        } else {
                            LedgerResponse::Empty
                        }
                    },
                    SystemRequest::AddNode { .. } | SystemRequest::RemoveNode { .. } => {
                        LedgerResponse::Empty
                    },
                    SystemRequest::UpdateOrganizationRouting { organization, region } => {
                        if let Some(org) = state.organizations.get_mut(organization) {
                            if org.status == OrganizationStatus::Deleted {
                                LedgerResponse::Error {
                                    code: LedgerErrorCode::FailedPrecondition,
                                    message: format!(
                                        "Cannot migrate deleted organization {}",
                                        organization
                                    ),
                                }
                            } else {
                                let old_region = org.region;
                                org.region = *region;
                                if let Some(blob) = try_encode(org, "organization") {
                                    pending.organizations.push((*organization, blob));
                                }
                                LedgerResponse::OrganizationMigrated {
                                    organization: *organization,
                                    old_region,
                                    new_region: *region,
                                }
                            }
                        } else {
                            LedgerResponse::Error {
                                code: LedgerErrorCode::NotFound,
                                message: format!("Organization {} not found", organization),
                            }
                        }
                    },

                    // Email hash index and blinding key operations apply to the
                    // system entity store (vault 0). They flow through Raft for
                    // consistency — every node applies the same writes.
                    SystemRequest::RegisterEmailHash { hmac_hex, user_id } => {
                        if let Some(sys) = &sys_service {
                            match sys.register_email_hash(hmac_hex, *user_id) {
                                Ok(()) => LedgerResponse::Empty,
                                Err(
                                    inferadb_ledger_state::system::SystemError::AlreadyExists {
                                        ..
                                    },
                                ) => LedgerResponse::Error {
                                    code: LedgerErrorCode::AlreadyExists,
                                    message: format!("Email hash already registered: {hmac_hex}"),
                                },
                                Err(e) => LedgerResponse::Error {
                                    code: LedgerErrorCode::Internal,
                                    message: format!("Failed to register email hash: {e}"),
                                },
                            }
                        } else {
                            LedgerResponse::Empty
                        }
                    },
                    SystemRequest::RemoveEmailHash { hmac_hex } => {
                        if let Some(sys) = &sys_service {
                            if let Err(e) = sys.remove_email_hash(hmac_hex) {
                                LedgerResponse::Error {
                                    code: LedgerErrorCode::Internal,
                                    message: format!("Failed to remove email hash: {e}"),
                                }
                            } else {
                                LedgerResponse::Empty
                            }
                        } else {
                            LedgerResponse::Empty
                        }
                    },
                    SystemRequest::SetBlindingKeyVersion { version } => {
                        if let Some(sys) = &sys_service {
                            if let Err(e) = sys.set_blinding_key_version(*version) {
                                LedgerResponse::Error {
                                    code: LedgerErrorCode::Internal,
                                    message: format!("Failed to set blinding key version: {e}"),
                                }
                            } else {
                                LedgerResponse::Empty
                            }
                        } else {
                            LedgerResponse::Empty
                        }
                    },
                    SystemRequest::UpdateRehashProgress { region, entries_rehashed } => {
                        if let Some(sys) = &sys_service {
                            if let Err(e) = sys.set_rehash_progress(*region, *entries_rehashed) {
                                LedgerResponse::Error {
                                    code: LedgerErrorCode::Internal,
                                    message: format!("Failed to update rehash progress: {e}"),
                                }
                            } else {
                                LedgerResponse::Empty
                            }
                        } else {
                            LedgerResponse::Empty
                        }
                    },
                    SystemRequest::ClearRehashProgress { region } => {
                        if let Some(sys) = &sys_service {
                            if let Err(e) = sys.clear_rehash_progress(*region) {
                                LedgerResponse::Error {
                                    code: LedgerErrorCode::Internal,
                                    message: format!("Failed to clear rehash progress: {e}"),
                                }
                            } else {
                                LedgerResponse::Empty
                            }
                        } else {
                            LedgerResponse::Empty
                        }
                    },
                    SystemRequest::UpdateUserDirectoryStatus { user_id, status, region } => {
                        if let Some(sys) = &sys_service {
                            // Update status first
                            if let Err(e) = sys.update_user_directory_status(*user_id, *status) {
                                LedgerResponse::Error {
                                    code: LedgerErrorCode::Internal,
                                    message: format!("Failed to update user directory status: {e}"),
                                }
                            } else if let Some(new_region) = region {
                                // Then update region if provided
                                if let Err(e) =
                                    sys.update_user_directory_region(*user_id, *new_region)
                                {
                                    LedgerResponse::Error {
                                        code: LedgerErrorCode::Internal,
                                        message: format!(
                                            "Failed to update user directory region: {e}"
                                        ),
                                    }
                                } else {
                                    LedgerResponse::Empty
                                }
                            } else {
                                LedgerResponse::Empty
                            }
                        } else {
                            LedgerResponse::Empty
                        }
                    },
                    SystemRequest::EraseUser { user_id, erased_by, region } => {
                        if let Some(sys) = &sys_service {
                            // Forward-only finalization: each step idempotent.
                            // The service method handles directory update, email hash
                            // removal, subject key deletion, and audit record creation.
                            if let Err(e) = sys.erase_user(*user_id, erased_by, *region) {
                                LedgerResponse::Error {
                                    code: LedgerErrorCode::Internal,
                                    message: format!("Failed to erase user: {e}"),
                                }
                            } else {
                                LedgerResponse::UserErased { user_id: *user_id }
                            }
                        } else {
                            LedgerResponse::Empty
                        }
                    },
                    SystemRequest::MigrateExistingUsers { entries } => {
                        if let Some(sys) = &sys_service {
                            match sys.migrate_existing_users(entries) {
                                Ok(summary) => LedgerResponse::UsersMigrated {
                                    users: summary.users,
                                    migrated: summary.migrated,
                                    skipped: summary.skipped,
                                    errors: summary.errors,
                                },
                                Err(e) => LedgerResponse::Error {
                                    code: LedgerErrorCode::Internal,
                                    message: format!("User migration failed: {e}"),
                                },
                            }
                        } else {
                            LedgerResponse::Empty
                        }
                    },
                    SystemRequest::CreateOrganizationDirectory { slug, region, tier } => {
                        let organization_id = state.sequences.next_organization();
                        let org_meta = OrganizationMeta {
                            organization: organization_id,
                            slug: *slug,
                            region: *region,
                            status: OrganizationStatus::Provisioning,
                            tier: *tier,
                            pending_region: None,
                            storage_bytes: 0,
                        };
                        if let Some(blob) = try_encode(&org_meta, "org_meta") {
                            pending.organizations.push((organization_id, blob));
                        }
                        state.organizations.insert(organization_id, org_meta);
                        state.slug_index.insert(*slug, organization_id);
                        state.id_to_slug.insert(organization_id, *slug);
                        pending.slug_index.push((*slug, organization_id));

                        if let Some(state_layer) = &self.state_layer {
                            let registry = OrganizationRegistry {
                                organization_id,
                                region: *region,
                                member_nodes: state
                                    .membership
                                    .membership()
                                    .nodes()
                                    .map(|(id, _)| id.to_string())
                                    .collect(),
                                status: OrganizationStatus::Provisioning,
                                config_version: 1,
                                created_at: block_timestamp,
                                deleted_at: None,
                            };
                            if let Some(value) = try_encode(&registry, "org_registry") {
                                let key = SystemKeys::organization_key(organization_id);
                                let slug_index_key = SystemKeys::organization_slug_key(*slug);
                                let ops = vec![
                                    Operation::SetEntity {
                                        key,
                                        value,
                                        condition: None,
                                        expires_at: None,
                                    },
                                    Operation::SetEntity {
                                        key: slug_index_key,
                                        value: organization_id.to_string().into_bytes(),
                                        condition: None,
                                        expires_at: None,
                                    },
                                ];
                                if let Err(e) =
                                    state_layer.apply_operations(SYSTEM_VAULT_ID, &ops, 0)
                                {
                                    tracing::error!(
                                        organization_id = organization_id.value(),
                                        error = %e,
                                        "Failed to persist organization to StateLayer"
                                    );
                                }
                            }
                        }

                        LedgerResponse::OrganizationDirectoryCreated {
                            organization_id,
                            organization_slug: *slug,
                        }
                    },
                    SystemRequest::WriteOrganizationProfile {
                        organization,
                        profile_key,
                        admin,
                    } => {
                        if let Some(state_layer) = &self.state_layer {
                            // Read the pending profile written by the gRPC handler
                            let pending_result =
                                state_layer.get_entity(SYSTEM_VAULT_ID, profile_key.as_bytes());

                            let org_name = match pending_result {
                                Ok(Some(entity)) => {
                                    match decode::<PendingOrganizationProfile>(&entity.value) {
                                        Ok(pending) => pending.name,
                                        Err(e) => {
                                            tracing::error!(
                                                organization_id = organization.value(),
                                                profile_key = %profile_key,
                                                error = %e,
                                                "Failed to decode pending org profile"
                                            );
                                            return (
                                                LedgerResponse::Error {
                                                    code: LedgerErrorCode::FailedPrecondition,
                                                    message: format!(
                                                        "Failed to decode pending profile for org {}: {e}",
                                                        organization
                                                    ),
                                                },
                                                None,
                                            );
                                        },
                                    }
                                },
                                Ok(None) => {
                                    tracing::error!(
                                        organization_id = organization.value(),
                                        profile_key = %profile_key,
                                        "Pending org profile not found"
                                    );
                                    return (
                                        LedgerResponse::Error {
                                            code: LedgerErrorCode::NotFound,
                                            message: format!(
                                                "Pending profile not found at key {} for org {}",
                                                profile_key, organization
                                            ),
                                        },
                                        None,
                                    );
                                },
                                Err(e) => {
                                    tracing::error!(
                                        organization_id = organization.value(),
                                        profile_key = %profile_key,
                                        error = %e,
                                        "Failed to read pending org profile"
                                    );
                                    return (
                                        LedgerResponse::Error {
                                            code: LedgerErrorCode::FailedPrecondition,
                                            message: format!(
                                                "Failed to read pending profile for org {}: {e}",
                                                organization
                                            ),
                                        },
                                        None,
                                    );
                                },
                            };

                            // Look up org metadata for region/tier/slug
                            let (region, tier, slug) = if let Some(org_meta) =
                                state.organizations.get(organization)
                            {
                                (org_meta.region, org_meta.tier, org_meta.slug)
                            } else {
                                tracing::error!(
                                    organization_id = organization.value(),
                                    "Organization not found in state during profile write"
                                );
                                return (
                                    LedgerResponse::Error {
                                        code: LedgerErrorCode::NotFound,
                                        message: format!("Organization {} not found", organization),
                                    },
                                    None,
                                );
                            };

                            let profile = OrganizationProfile {
                                organization: *organization,
                                slug,
                                region,
                                name: org_name,
                                tier,
                                status: OrganizationStatus::Active,
                                members: vec![OrganizationMember {
                                    user_id: *admin,
                                    role: OrganizationMemberRole::Admin,
                                    joined_at: block_timestamp,
                                }],
                                created_at: block_timestamp,
                                updated_at: block_timestamp,
                                deleted_at: None,
                            };

                            if let Some(profile_bytes) = try_encode(&profile, "org_profile") {
                                let final_key = SystemKeys::organization_profile_key(*organization);
                                let mut ops = vec![Operation::SetEntity {
                                    key: final_key,
                                    value: profile_bytes,
                                    condition: None,
                                    expires_at: None,
                                }];
                                // Delete the pending key
                                ops.push(Operation::DeleteEntity { key: profile_key.clone() });
                                if let Err(e) =
                                    state_layer.apply_operations(SYSTEM_VAULT_ID, &ops, 0)
                                {
                                    tracing::error!(
                                        organization_id = organization.value(),
                                        error = %e,
                                        "Failed to write organization profile"
                                    );
                                } else {
                                    // Update user→org index for initial admin member
                                    state
                                        .user_org_index
                                        .entry(*admin)
                                        .or_default()
                                        .insert(*organization);
                                }
                            }
                        }

                        LedgerResponse::OrganizationProfileWritten {
                            organization_id: *organization,
                        }
                    },
                    SystemRequest::UpdateOrganizationDirectoryStatus { organization, status } => {
                        if let Some(org_meta) = state.organizations.get_mut(organization) {
                            let new_status = match status {
                                inferadb_ledger_state::system::OrganizationDirectoryStatus::Active => {
                                    OrganizationStatus::Active
                                },
                                inferadb_ledger_state::system::OrganizationDirectoryStatus::Provisioning => {
                                    OrganizationStatus::Provisioning
                                },
                                inferadb_ledger_state::system::OrganizationDirectoryStatus::Migrating => {
                                    OrganizationStatus::Migrating
                                },
                                inferadb_ledger_state::system::OrganizationDirectoryStatus::Deleted => {
                                    OrganizationStatus::Deleted
                                },
                            };
                            org_meta.status = new_status;
                            if let Some(blob) = try_encode(org_meta, "org_meta") {
                                pending.organizations.push((*organization, blob));
                            }
                            LedgerResponse::OrganizationDirectoryStatusUpdated {
                                organization_id: *organization,
                            }
                        } else {
                            tracing::error!(
                                organization_id = organization.value(),
                                "Organization not found for status update"
                            );
                            LedgerResponse::Error {
                                code: LedgerErrorCode::NotFound,
                                message: format!("Organization {} not found", organization),
                            }
                        }
                    },
                    SystemRequest::CreateOrganizationWithProfile {
                        slug,
                        region,
                        tier,
                        name,
                        admin,
                    } => {
                        let organization_id = state.sequences.next_organization();
                        let org_meta = OrganizationMeta {
                            organization: organization_id,
                            slug: *slug,
                            region: *region,
                            status: OrganizationStatus::Active,
                            tier: *tier,
                            pending_region: None,
                            storage_bytes: 0,
                        };
                        if let Some(blob) = try_encode(&org_meta, "org_meta") {
                            pending.organizations.push((organization_id, blob));
                        }
                        state.organizations.insert(organization_id, org_meta);
                        state.slug_index.insert(*slug, organization_id);
                        state.id_to_slug.insert(organization_id, *slug);
                        pending.slug_index.push((*slug, organization_id));

                        if let Some(state_layer) = &self.state_layer {
                            let registry = OrganizationRegistry {
                                organization_id,
                                region: *region,
                                member_nodes: state
                                    .membership
                                    .membership()
                                    .nodes()
                                    .map(|(id, _)| id.to_string())
                                    .collect(),
                                status: OrganizationStatus::Active,
                                config_version: 1,
                                created_at: block_timestamp,
                                deleted_at: None,
                            };
                            if let Some(value) = try_encode(&registry, "org_registry") {
                                let key = SystemKeys::organization_key(organization_id);
                                let slug_index_key = SystemKeys::organization_slug_key(*slug);
                                let mut ops = vec![
                                    Operation::SetEntity {
                                        key,
                                        value,
                                        condition: None,
                                        expires_at: None,
                                    },
                                    Operation::SetEntity {
                                        key: slug_index_key,
                                        value: organization_id.to_string().into_bytes(),
                                        condition: None,
                                        expires_at: None,
                                    },
                                ];

                                // Write organization profile atomically
                                let profile = OrganizationProfile {
                                    organization: organization_id,
                                    slug: *slug,
                                    region: *region,
                                    name: name.clone(),
                                    tier: *tier,
                                    status: OrganizationStatus::Active,
                                    members: vec![OrganizationMember {
                                        user_id: *admin,
                                        role: OrganizationMemberRole::Admin,
                                        joined_at: block_timestamp,
                                    }],
                                    created_at: block_timestamp,
                                    updated_at: block_timestamp,
                                    deleted_at: None,
                                };
                                if let Some(profile_bytes) = try_encode(&profile, "org_profile") {
                                    ops.push(Operation::SetEntity {
                                        key: SystemKeys::organization_profile_key(organization_id),
                                        value: profile_bytes,
                                        condition: None,
                                        expires_at: None,
                                    });
                                }

                                if let Err(e) =
                                    state_layer.apply_operations(SYSTEM_VAULT_ID, &ops, 0)
                                {
                                    tracing::error!(
                                        organization_id = organization_id.value(),
                                        error = %e,
                                        "Failed to persist organization with profile to StateLayer"
                                    );
                                } else {
                                    // Update user→org index for initial admin member
                                    state
                                        .user_org_index
                                        .entry(*admin)
                                        .or_default()
                                        .insert(organization_id);

                                    // Write CreateSigningKeySaga for this org's signing key.
                                    // The saga orchestrator picks it up on the next poll cycle.
                                    write_signing_key_saga(
                                        state_layer,
                                        SigningKeyScope::Organization(organization_id),
                                    );
                                }
                            }
                        }

                        LedgerResponse::OrganizationDirectoryCreated {
                            organization_id,
                            organization_slug: *slug,
                        }
                    },
                };

                // Emit events for system request variants
                match (&response, system_request) {
                    (
                        LedgerResponse::UserCreated { user_id, slug },
                        SystemRequest::CreateUser { admin, region, .. },
                    ) => {
                        events.push(
                            ApplyPhaseEmitter::for_system(EventAction::UserCreated)
                                .principal("system")
                                .detail("user_id", &user_id.to_string())
                                .detail("slug", &slug.to_string())
                                .detail("admin", &admin.to_string())
                                .detail("region", region.as_str())
                                .outcome(EventOutcome::Success)
                                .build(block_height, *op_index, block_timestamp, ttl_days),
                        );
                        *op_index += 1;
                    },
                    (LedgerResponse::Empty, SystemRequest::AddNode { node_id, address }) => {
                        events.push(
                            ApplyPhaseEmitter::for_system(EventAction::NodeJoinedCluster)
                                .principal("system")
                                .detail("node_id", &node_id.to_string())
                                .detail("address", address)
                                .outcome(EventOutcome::Success)
                                .build(block_height, *op_index, block_timestamp, ttl_days),
                        );
                        *op_index += 1;
                    },
                    (LedgerResponse::Empty, SystemRequest::RemoveNode { node_id }) => {
                        events.push(
                            ApplyPhaseEmitter::for_system(EventAction::NodeLeftCluster)
                                .principal("system")
                                .detail("node_id", &node_id.to_string())
                                .outcome(EventOutcome::Success)
                                .build(block_height, *op_index, block_timestamp, ttl_days),
                        );
                        *op_index += 1;
                    },
                    (
                        LedgerResponse::OrganizationMigrated { new_region, .. },
                        SystemRequest::UpdateOrganizationRouting { organization, .. },
                    ) => {
                        events.push(
                            ApplyPhaseEmitter::for_system(EventAction::RoutingUpdated)
                                .detail("organization_id", &organization.to_string())
                                .detail("region", &new_region.to_string())
                                .outcome(EventOutcome::Success)
                                .build(block_height, *op_index, block_timestamp, ttl_days),
                        );
                        *op_index += 1;
                    },
                    (LedgerResponse::UserUpdated { user_id }, SystemRequest::UpdateUser { .. }) => {
                        events.push(
                            ApplyPhaseEmitter::for_system(EventAction::UserUpdated)
                                .principal("system")
                                .detail("user_id", &user_id.to_string())
                                .outcome(EventOutcome::Success)
                                .build(block_height, *op_index, block_timestamp, ttl_days),
                        );
                        *op_index += 1;
                    },
                    (
                        LedgerResponse::UserSoftDeleted { user_id, retention_days },
                        SystemRequest::DeleteUser { .. },
                    ) => {
                        events.push(
                            ApplyPhaseEmitter::for_system(EventAction::UserSoftDeleted)
                                .principal("system")
                                .detail("user_id", &user_id.to_string())
                                .detail("retention_days", &retention_days.to_string())
                                .outcome(EventOutcome::Success)
                                .build(block_height, *op_index, block_timestamp, ttl_days),
                        );
                        *op_index += 1;
                    },
                    (
                        LedgerResponse::UserEmailCreated { email_id },
                        SystemRequest::CreateUserEmail { user_id, .. },
                    ) => {
                        events.push(
                            ApplyPhaseEmitter::for_system(EventAction::UserEmailCreated)
                                .principal("system")
                                .detail("user_id", &user_id.to_string())
                                .detail("email_id", &email_id.to_string())
                                .outcome(EventOutcome::Success)
                                .build(block_height, *op_index, block_timestamp, ttl_days),
                        );
                        *op_index += 1;
                    },
                    (
                        LedgerResponse::UserEmailDeleted { email_id },
                        SystemRequest::DeleteUserEmail { user_id, .. },
                    ) => {
                        events.push(
                            ApplyPhaseEmitter::for_system(EventAction::UserEmailDeleted)
                                .principal("system")
                                .detail("user_id", &user_id.to_string())
                                .detail("email_id", &email_id.to_string())
                                .outcome(EventOutcome::Success)
                                .build(block_height, *op_index, block_timestamp, ttl_days),
                        );
                        *op_index += 1;
                    },
                    (
                        LedgerResponse::UserEmailVerified { email_id },
                        SystemRequest::VerifyUserEmail { .. },
                    ) => {
                        events.push(
                            ApplyPhaseEmitter::for_system(EventAction::UserEmailVerified)
                                .principal("system")
                                .detail("email_id", &email_id.to_string())
                                .outcome(EventOutcome::Success)
                                .build(block_height, *op_index, block_timestamp, ttl_days),
                        );
                        *op_index += 1;
                    },
                    (
                        LedgerResponse::UserErased { user_id },
                        SystemRequest::EraseUser { erased_by, region, .. },
                    ) => {
                        events.push(
                            ApplyPhaseEmitter::for_system(EventAction::UserErased)
                                .principal(erased_by)
                                .detail("user_id", &user_id.to_string())
                                .detail("region", region.as_str())
                                .outcome(EventOutcome::Success)
                                .build(block_height, *op_index, block_timestamp, ttl_days),
                        );
                        *op_index += 1;
                    },
                    (
                        LedgerResponse::UsersMigrated { migrated, skipped, errors, .. },
                        SystemRequest::MigrateExistingUsers { entries },
                    ) => {
                        events.push(
                            ApplyPhaseEmitter::for_system(EventAction::UsersMigrated)
                                .principal("system")
                                .detail("users", &entries.len().to_string())
                                .detail("migrated", &migrated.to_string())
                                .detail("skipped", &skipped.to_string())
                                .detail("errors", &errors.to_string())
                                .outcome(EventOutcome::Success)
                                .build(block_height, *op_index, block_timestamp, ttl_days),
                        );
                        *op_index += 1;
                    },
                    (
                        LedgerResponse::OrganizationDirectoryCreated {
                            organization_id,
                            organization_slug,
                        },
                        SystemRequest::CreateOrganizationDirectory { region, .. }
                        | SystemRequest::CreateOrganizationWithProfile { region, .. },
                    ) => {
                        events.push(
                            ApplyPhaseEmitter::for_system(EventAction::OrganizationCreated)
                                .detail("organization_id", &organization_id.to_string())
                                .detail("organization_slug", &organization_slug.to_string())
                                .detail("region", region.as_str())
                                .outcome(EventOutcome::Success)
                                .build(block_height, *op_index, block_timestamp, ttl_days),
                        );
                        *op_index += 1;
                    },
                    _ => {},
                }

                (response, None)
            },

            // ── Signing Key Operations ──
            LedgerRequest::CreateSigningKey {
                scope,
                kid,
                public_key_bytes,
                encrypted_private_key,
                rmk_version,
            } => {
                let Some(state_layer) = &self.state_layer else {
                    return error_result(LedgerErrorCode::Internal, "State layer not available");
                };
                let sys = SystemOrganizationService::new(state_layer.clone());

                // Idempotent: if a key with this kid already exists and is Active, return success.
                match sys.get_signing_key_by_kid(kid) {
                    Ok(Some(existing)) if existing.status == SigningKeyStatus::Active => {
                        return (
                            LedgerResponse::SigningKeyCreated { id: existing.id, kid: kid.clone() },
                            None,
                        );
                    },
                    Ok(_) => {},
                    Err(e) => {
                        return error_result(
                            LedgerErrorCode::Internal,
                            format!("Failed to check existing signing key: {e}"),
                        );
                    },
                }

                // Verify no other active key exists for this scope (use RotateSigningKey instead).
                match sys.get_active_signing_key(scope) {
                    Ok(Some(active)) => {
                        return error_result(
                            LedgerErrorCode::FailedPrecondition,
                            format!(
                                "Active signing key already exists for scope: kid={}. Use RotateSigningKey.",
                                active.kid
                            ),
                        );
                    },
                    Ok(None) => {},
                    Err(e) => {
                        return error_result(
                            LedgerErrorCode::Internal,
                            format!("Failed to check active signing key: {e}"),
                        );
                    },
                }

                let id = state.sequences.next_signing_key();
                let key = SigningKey {
                    id,
                    kid: kid.clone(),
                    public_key_bytes: public_key_bytes.clone(),
                    encrypted_private_key: encrypted_private_key.clone(),
                    rmk_version: *rmk_version,
                    scope: *scope,
                    status: SigningKeyStatus::Active,
                    valid_from: block_timestamp,
                    valid_until: None,
                    created_at: block_timestamp,
                    rotated_at: None,
                    revoked_at: None,
                };

                if let Err(e) = sys.store_signing_key(&key) {
                    return error_result(
                        LedgerErrorCode::Internal,
                        format!("Failed to store signing key: {e}"),
                    );
                }

                (LedgerResponse::SigningKeyCreated { id, kid: kid.clone() }, None)
            },

            LedgerRequest::RotateSigningKey {
                old_kid,
                new_kid,
                new_public_key_bytes,
                new_encrypted_private_key,
                rmk_version,
                grace_period_secs,
            } => {
                let Some(state_layer) = &self.state_layer else {
                    return error_result(LedgerErrorCode::Internal, "State layer not available");
                };
                let sys = SystemOrganizationService::new(state_layer.clone());

                // Look up old key; verify it's Active.
                let old_key = match require_signing_key(&sys, old_kid) {
                    Ok(k) => k,
                    Err(resp) => return resp,
                };

                if old_key.status != SigningKeyStatus::Active {
                    return error_result(
                        LedgerErrorCode::FailedPrecondition,
                        format!(
                            "Signing key {old_kid} is not Active (status: {:?})",
                            old_key.status
                        ),
                    );
                }

                // Transition old key: Rotated (with grace period) or Revoked (immediate).
                if *grace_period_secs == 0 {
                    // Immediate revocation.
                    if let Err(e) = sys.update_signing_key_status(
                        old_kid,
                        SigningKeyStatus::Revoked,
                        None,
                        block_timestamp,
                    ) {
                        return error_result(
                            LedgerErrorCode::Internal,
                            format!("Failed to revoke old signing key: {e}"),
                        );
                    }
                } else {
                    // Grace period: old key stays valid for verification.
                    let valid_until =
                        block_timestamp + saturating_duration_secs(*grace_period_secs);
                    if let Err(e) = sys.update_signing_key_status(
                        old_kid,
                        SigningKeyStatus::Rotated,
                        Some(valid_until),
                        block_timestamp,
                    ) {
                        return error_result(
                            LedgerErrorCode::Internal,
                            format!("Failed to rotate old signing key: {e}"),
                        );
                    }
                }

                // Create new key as Active.
                let new_id = state.sequences.next_signing_key();
                let new_key = SigningKey {
                    id: new_id,
                    kid: new_kid.clone(),
                    public_key_bytes: new_public_key_bytes.clone(),
                    encrypted_private_key: new_encrypted_private_key.clone(),
                    rmk_version: *rmk_version,
                    scope: old_key.scope,
                    status: SigningKeyStatus::Active,
                    valid_from: block_timestamp,
                    valid_until: None,
                    created_at: block_timestamp,
                    rotated_at: None,
                    revoked_at: None,
                };

                if let Err(e) = sys.store_signing_key(&new_key) {
                    return error_result(
                        LedgerErrorCode::Internal,
                        format!("Failed to store new signing key: {e}"),
                    );
                }

                (
                    LedgerResponse::SigningKeyRotated {
                        old_kid: old_kid.clone(),
                        new_kid: new_kid.clone(),
                    },
                    None,
                )
            },

            LedgerRequest::RevokeSigningKey { kid } => {
                let Some(state_layer) = &self.state_layer else {
                    return error_result(LedgerErrorCode::Internal, "State layer not available");
                };
                let sys = SystemOrganizationService::new(state_layer.clone());

                let key = match require_signing_key(&sys, kid) {
                    Ok(k) => k,
                    Err(resp) => return resp,
                };

                if key.status == SigningKeyStatus::Revoked {
                    // Already revoked — idempotent success.
                    return (LedgerResponse::SigningKeyRevoked { kid: kid.clone() }, None);
                }

                if let Err(e) = sys.update_signing_key_status(
                    kid,
                    SigningKeyStatus::Revoked,
                    None,
                    block_timestamp,
                ) {
                    return error_result(
                        LedgerErrorCode::Internal,
                        format!("Failed to revoke signing key: {e}"),
                    );
                }

                (LedgerResponse::SigningKeyRevoked { kid: kid.clone() }, None)
            },

            LedgerRequest::TransitionSigningKeyRevoked { kid } => {
                let Some(state_layer) = &self.state_layer else {
                    return error_result(LedgerErrorCode::Internal, "State layer not available");
                };
                let sys = SystemOrganizationService::new(state_layer.clone());

                let key = match sys.get_signing_key_by_kid(kid) {
                    Ok(Some(k)) => k,
                    Ok(None) => {
                        // Key not found — idempotent no-op.
                        return (LedgerResponse::SigningKeyTransitioned { kid: kid.clone() }, None);
                    },
                    Err(e) => {
                        return error_result(
                            LedgerErrorCode::Internal,
                            format!("Failed to look up signing key '{kid}': {e}"),
                        );
                    },
                };

                // Only transition Rotated keys past their grace period.
                if key.status != SigningKeyStatus::Rotated {
                    return (LedgerResponse::SigningKeyTransitioned { kid: kid.clone() }, None);
                }

                // Verify grace period actually expired.
                if let Some(valid_until) = key.valid_until
                    && valid_until >= block_timestamp
                {
                    // Grace period not yet expired — no-op.
                    return (LedgerResponse::SigningKeyTransitioned { kid: kid.clone() }, None);
                }

                if let Err(e) = sys.update_signing_key_status(
                    kid,
                    SigningKeyStatus::Revoked,
                    key.valid_until,
                    block_timestamp,
                ) {
                    return error_result(
                        LedgerErrorCode::Internal,
                        format!("Failed to transition signing key: {e}"),
                    );
                }

                (LedgerResponse::SigningKeyTransitioned { kid: kid.clone() }, None)
            },

            // ── Refresh Token Operations ──
            LedgerRequest::CreateRefreshToken {
                token_hash,
                family,
                token_type,
                subject,
                organization,
                vault,
                kid,
                ttl_secs,
            } => {
                let Some(state_layer) = &self.state_layer else {
                    return error_result(LedgerErrorCode::Internal, "State layer not available");
                };
                let sys = SystemOrganizationService::new(state_layer.clone());

                let id = state.sequences.next_refresh_token();
                let expires_at = block_timestamp + saturating_duration_secs(*ttl_secs);

                let token = RefreshToken {
                    id,
                    token_hash: *token_hash,
                    family: *family,
                    token_type: *token_type,
                    subject: *subject,
                    organization: *organization,
                    vault: *vault,
                    kid: kid.clone(),
                    expires_at,
                    used: false,
                    created_at: block_timestamp,
                    used_at: None,
                    revoked_at: None,
                };

                if let Err(e) = sys.store_refresh_token(&token) {
                    return error_result(
                        LedgerErrorCode::Internal,
                        format!("Failed to store refresh token: {e}"),
                    );
                }

                (LedgerResponse::RefreshTokenCreated { id }, None)
            },

            LedgerRequest::UseRefreshToken {
                old_token_hash,
                new_token_hash,
                new_kid,
                ttl_secs,
                expected_version,
            } => {
                let Some(state_layer) = &self.state_layer else {
                    return error_result(LedgerErrorCode::Internal, "State layer not available");
                };
                let sys = SystemOrganizationService::new(state_layer.clone());

                // Look up old token by hash.
                let old_token = match sys.get_refresh_token_by_hash(old_token_hash) {
                    Ok(Some(t)) => t,
                    Ok(None) => {
                        return error_result(
                            LedgerErrorCode::Unauthenticated,
                            "Refresh token not found",
                        );
                    },
                    Err(e) => {
                        return error_result(
                            LedgerErrorCode::Internal,
                            format!("Failed to look up refresh token: {e}"),
                        );
                    },
                };

                // Check if family is poisoned (prior reuse detected).
                match sys.is_family_poisoned(&old_token.family) {
                    Ok(true) => {
                        return error_result(
                            LedgerErrorCode::Unauthenticated,
                            "Refresh token reuse detected: family revoked",
                        );
                    },
                    Ok(false) => {},
                    Err(e) => {
                        return error_result(
                            LedgerErrorCode::Internal,
                            format!("Failed to check family poison status: {e}"),
                        );
                    },
                }

                // If old token already used → poison the family (theft detection).
                if old_token.used {
                    if let Err(e) = sys.poison_token_family(&old_token.family) {
                        tracing::error!(error = %e, "Failed to poison token family on reuse detection");
                    }
                    return error_result(
                        LedgerErrorCode::Unauthenticated,
                        "Refresh token reuse detected: family revoked",
                    );
                }

                // Check expiry.
                if old_token.expires_at <= block_timestamp {
                    return error_result(LedgerErrorCode::Unauthenticated, "Refresh token expired");
                }

                // Check revocation.
                if old_token.revoked_at.is_some() {
                    return error_result(LedgerErrorCode::Unauthenticated, "Refresh token revoked");
                }

                // For user session refresh: validate TokenVersion.
                let mut token_version = None;
                if let Some(expected) = expected_version
                    && let TokenSubject::User(user_slug) = &old_token.subject
                {
                    let user_id = match state.user_slug_index.get(user_slug) {
                        Some(&id) => id,
                        None => {
                            return error_result(
                                LedgerErrorCode::NotFound,
                                "User slug not found in index",
                            );
                        },
                    };

                    // Read user entity to get current TokenVersion.
                    let user_key = SystemKeys::user_key(user_id);
                    let entity = match state_layer.get_entity(SYSTEM_VAULT_ID, user_key.as_bytes())
                    {
                        Ok(Some(entity)) => entity,
                        Ok(None) => {
                            return error_result(
                                LedgerErrorCode::NotFound,
                                "User not found for version check",
                            );
                        },
                        Err(e) => {
                            return error_result(
                                LedgerErrorCode::Internal,
                                format!("Failed to read user: {e}"),
                            );
                        },
                    };
                    let user = match decode::<User>(&entity.value) {
                        Ok(user) => user,
                        Err(e) => {
                            return error_result(
                                LedgerErrorCode::Internal,
                                format!("Failed to decode user: {e}"),
                            );
                        },
                    };
                    if user.version != *expected {
                        return error_result(
                            LedgerErrorCode::Unauthenticated,
                            "Token version mismatch: session invalidated",
                        );
                    }
                    token_version = Some(user.version);
                }

                // For vault token refresh: verify app enabled + read allowed_scopes.
                let mut allowed_scopes = None;
                if old_token.token_type == TokenType::VaultAccess
                    && let TokenSubject::App(app_slug) = &old_token.subject
                {
                    // Resolve app slug to (org_id, app_id).
                    let (org_id, app_id) = match state.app_slug_index.get(app_slug) {
                        Some(ids) => *ids,
                        None => {
                            return error_result(LedgerErrorCode::NotFound, "App not found");
                        },
                    };

                    // Verify app is enabled.
                    let app = match load_app(state_layer, org_id, app_id) {
                        Ok(app) => app,
                        Err(resp) => return (resp, None),
                    };
                    if !app.enabled {
                        return error_result(
                            LedgerErrorCode::FailedPrecondition,
                            "App is disabled",
                        );
                    }

                    // Read current AppVaultConnection for allowed_scopes.
                    if let Some(vault_id) = old_token.vault {
                        let connection_key = SystemKeys::app_vault_key(org_id, app_id, vault_id);
                        let entity = match state_layer
                            .get_entity(SYSTEM_VAULT_ID, connection_key.as_bytes())
                        {
                            Ok(Some(entity)) => entity,
                            Ok(None) => {
                                return error_result(
                                    LedgerErrorCode::FailedPrecondition,
                                    "Vault connection removed since token was issued",
                                );
                            },
                            Err(e) => {
                                return error_result(
                                    LedgerErrorCode::Internal,
                                    format!("Failed to read vault connection: {e}"),
                                );
                            },
                        };
                        let connection = match decode::<AppVaultConnection>(&entity.value) {
                            Ok(c) => c,
                            Err(e) => {
                                return error_result(
                                    LedgerErrorCode::Internal,
                                    format!("Failed to decode vault connection: {e}"),
                                );
                            },
                        };
                        allowed_scopes = Some(connection.allowed_scopes);
                    }
                }

                // Mark old token as used.
                if let Err(e) = sys.mark_refresh_token_used(old_token.id, block_timestamp) {
                    return error_result(
                        LedgerErrorCode::Internal,
                        format!("Failed to mark refresh token used: {e}"),
                    );
                }

                // Create new refresh token with same family.
                let new_id = state.sequences.next_refresh_token();
                let new_expires_at = block_timestamp + saturating_duration_secs(*ttl_secs);

                let new_token = RefreshToken {
                    id: new_id,
                    token_hash: *new_token_hash,
                    family: old_token.family,
                    token_type: old_token.token_type,
                    subject: old_token.subject,
                    organization: old_token.organization,
                    vault: old_token.vault,
                    kid: new_kid.clone(),
                    expires_at: new_expires_at,
                    used: false,
                    created_at: block_timestamp,
                    used_at: None,
                    revoked_at: None,
                };

                if let Err(e) = sys.store_refresh_token(&new_token) {
                    return error_result(
                        LedgerErrorCode::Internal,
                        format!("Failed to store new refresh token: {e}"),
                    );
                }

                (
                    LedgerResponse::RefreshTokenRotated { new_id, token_version, allowed_scopes },
                    None,
                )
            },

            LedgerRequest::RevokeTokenFamily { family } => {
                let Some(state_layer) = &self.state_layer else {
                    return error_result(LedgerErrorCode::Internal, "State layer not available");
                };
                let sys = SystemOrganizationService::new(state_layer.clone());

                match sys.revoke_token_family(family, block_timestamp) {
                    Ok(result) => {
                        (LedgerResponse::TokenFamilyRevoked { count: result.revoked_count }, None)
                    },
                    Err(e) => error_result(
                        LedgerErrorCode::Internal,
                        format!("Failed to revoke token family: {e}"),
                    ),
                }
            },

            LedgerRequest::RevokeAllUserSessions { user } => {
                let Some(state_layer) = &self.state_layer else {
                    return error_result(LedgerErrorCode::Internal, "State layer not available");
                };
                let sys = SystemOrganizationService::new(state_layer.clone());

                // Resolve UserId → UserSlug from in-memory state.
                let user_slug = match state.user_id_to_slug.get(user) {
                    Some(slug) => *slug,
                    None => {
                        return error_result(
                            LedgerErrorCode::NotFound,
                            format!("User slug not found for user {user}"),
                        );
                    },
                };

                match sys.revoke_all_user_sessions(*user, user_slug, block_timestamp) {
                    Ok(result) => (
                        LedgerResponse::AllUserSessionsRevoked {
                            count: result.revoked_count,
                            version: result.new_version,
                        },
                        None,
                    ),
                    Err(e) => error_result(
                        LedgerErrorCode::Internal,
                        format!("Failed to revoke all user sessions: {e}"),
                    ),
                }
            },

            LedgerRequest::DeleteExpiredRefreshTokens => {
                let Some(state_layer) = &self.state_layer else {
                    return error_result(LedgerErrorCode::Internal, "State layer not available");
                };
                let sys = SystemOrganizationService::new(state_layer.clone());

                match sys.delete_expired_refresh_tokens(block_timestamp) {
                    Ok(result) => (
                        LedgerResponse::ExpiredRefreshTokensDeleted {
                            count: result.expired_count + result.poisoned_families_cleaned,
                        },
                        None,
                    ),
                    Err(e) => error_result(
                        LedgerErrorCode::Internal,
                        format!("Failed to delete expired refresh tokens: {e}"),
                    ),
                }
            },

            LedgerRequest::BatchWrite { requests } => {
                // Process each request in the batch sequentially, collecting responses.
                // Vault entries are collected and the last one is returned (batches typically
                // target the same vault, so the final block includes all transactions).
                let mut responses = Vec::with_capacity(requests.len());
                let mut last_vault_entry = None;

                for inner_request in requests {
                    let (response, vault_entry) = self.apply_request_with_events(
                        inner_request,
                        state,
                        block_timestamp,
                        op_index,
                        events,
                        ttl_days,
                        pending,
                    );
                    responses.push(response);
                    if vault_entry.is_some() {
                        last_vault_entry = vault_entry;
                    }
                }

                // Emit BatchWriteCommitted event for the batch itself
                if let Some(ref ve) = last_vault_entry {
                    let org_slug = state.id_to_slug.get(&ve.organization).copied();
                    let vault_slug = state.vault_id_to_slug.get(&ve.vault).copied();
                    let mut emitter = ApplyPhaseEmitter::for_organization(
                        EventAction::BatchWriteCommitted,
                        ve.organization,
                        org_slug,
                    )
                    .outcome(EventOutcome::Success)
                    .operations_count(requests.len() as u32);
                    if let Some(vs) = vault_slug {
                        emitter = emitter.vault(vs);
                    }
                    events.push(emitter.build(block_height, *op_index, block_timestamp, ttl_days));
                    *op_index += 1;
                }

                (LedgerResponse::BatchWrite { responses }, last_vault_entry)
            },
        }
    }

    /// Computes a deterministic hash for a vault entry.
    ///
    /// Uses only the cryptographic commitments from the entry, not runtime
    /// metadata like timestamp or proposer. This ensures all Raft nodes
    /// compute the same hash for the same log entry.
    pub(super) fn compute_vault_block_hash(&self, entry: &VaultEntry) -> Hash {
        inferadb_ledger_types::vault_entry_hash(entry)
    }

    /// Computes a block hash (used in tests).
    #[allow(dead_code)] // reserved for block hash computation in state machine
    pub(super) fn compute_block_hash(
        &self,
        organization: OrganizationId,
        vault: VaultId,
        height: u64,
    ) -> Hash {
        use inferadb_ledger_types::sha256;
        let mut data = Vec::new();
        data.extend_from_slice(&organization.value().to_le_bytes());
        data.extend_from_slice(&vault.value().to_le_bytes());
        data.extend_from_slice(&height.to_le_bytes());
        sha256(&data)
    }
}
