//! State machine apply logic for Raft log entries.
//!
//! Transforms committed log entries into state mutations via the storage engine.

use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use inferadb_ledger_state::{
    StateError, StateLayer,
    system::{
        App, AppCredentialType, AppCredentials, AppProfile, AppVaultConnection,
        ClientAssertionEntry, EmailHashEntry, OnboardingAccount, OrganizationDirectoryEntry,
        OrganizationDirectoryStatus, OrganizationMember, OrganizationMemberRole,
        OrganizationProfile, OrganizationRegistry, OrganizationStatus, OrganizationTier,
        PendingEmailVerification, ProvisioningReservation, RefreshToken, RevocationResult,
        SYSTEM_VAULT_ID, SigningKey, SigningKeyStatus, SystemError, SystemKeys,
        SystemOrganizationService, TeamMember, TeamProfile, User, UserDirectoryEntry,
        UserDirectoryStatus, UserEmail,
    },
};
use inferadb_ledger_store::StorageBackend;
use inferadb_ledger_types::{
    AppId, AppSlug, ErrorCode, Hash, NodeId, Operation, OrganizationId, TeamId, TeamSlug,
    TokenSubject, TokenType, TokenVersion, UserRole, UserStatus, VaultEntry, VaultId,
    compute_tx_merkle_root, decode, encode,
    events::{EventAction, EventEntry, EventOutcome},
    hash_eq,
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
    types::{EmailCodeVerifiedResult, LedgerRequest, LedgerResponse, SystemRequest},
};

/// Executes a cascade revocation and logs the outcome.
///
/// Cascade revocations are best-effort — failures are logged but do not
/// abort the parent operation. Callers should enter a tracing span with
/// relevant context fields (org_id, app_id, etc.) before calling.
fn cascade_revoke<B: StorageBackend>(
    state_layer: &Arc<StateLayer<B>>,
    op: impl FnOnce(&SystemOrganizationService<B>) -> Result<RevocationResult, SystemError>,
    success_msg: &str,
    error_msg: &str,
) {
    let sys = SystemOrganizationService::new(state_layer.clone());
    match op(&sys) {
        Ok(result) => {
            tracing::info!(revoked_count = result.revoked_count, "{success_msg}");
        },
        Err(e) => {
            tracing::error!(error = %e, "{error_msg}");
        },
    }
}

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

/// Constructs a `LedgerResponse::Error` with the given code and message.
fn ledger_error(code: ErrorCode, message: impl Into<String>) -> LedgerResponse {
    LedgerResponse::Error { code, message: message.into() }
}

/// Constructs an early-return error tuple for the apply handler.
fn error_result(
    code: ErrorCode,
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
            ledger_error(ErrorCode::Internal, format!("Failed to read organization profile: {e}"))
        })?
        .ok_or_else(|| {
            ledger_error(
                ErrorCode::NotFound,
                format!("Organization profile not found for {organization}"),
            )
        })?;
    decode::<OrganizationProfile>(&entity.value).map_err(|e| {
        ledger_error(ErrorCode::Internal, format!("Failed to decode organization profile: {e}"))
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
        ledger_error(ErrorCode::Internal, format!("Failed to encode organization profile: {e}"))
    })?;
    let ops = vec![Operation::SetEntity { key, value: bytes, condition: None, expires_at: None }];
    state_layer.apply_operations(SYSTEM_VAULT_ID, &ops, 0).map(|_| ()).map_err(|e| {
        ledger_error(ErrorCode::Internal, format!("Failed to write organization profile: {e}"))
    })
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
        .map_err(|e| ledger_error(ErrorCode::Internal, format!("Failed to read app: {e}")))?
        .ok_or_else(|| {
            ledger_error(
                ErrorCode::NotFound,
                format!("App {} not found in organization {}", app, organization),
            )
        })?;
    decode::<App>(&entity.value)
        .map_err(|e| ledger_error(ErrorCode::Internal, format!("Failed to decode app: {e}")))
}

/// Saves an `App` to the state layer.
fn save_app<B: StorageBackend>(
    state_layer: &inferadb_ledger_state::StateLayer<B>,
    organization: OrganizationId,
    app: &App,
) -> Result<(), LedgerResponse> {
    let key = SystemKeys::app_key(organization, app.id);
    let bytes = encode(app)
        .map_err(|e| ledger_error(ErrorCode::Internal, format!("Failed to encode app: {e}")))?;
    let ops = vec![Operation::SetEntity { key, value: bytes, condition: None, expires_at: None }];
    state_layer
        .apply_operations(SYSTEM_VAULT_ID, &ops, 0)
        .map(|_| ())
        .map_err(|e| ledger_error(ErrorCode::Internal, format!("Failed to write app: {e}")))
}

/// Loads an `AppProfile` from the state layer by its storage key.
///
/// Returns `Ok(profile)` on success, or `Err(NotFound)` if no profile exists yet.
fn load_app_profile<B: StorageBackend>(
    state_layer: &inferadb_ledger_state::StateLayer<B>,
    organization: OrganizationId,
    app: AppId,
) -> Result<AppProfile, LedgerResponse> {
    let key = SystemKeys::app_profile_key(organization, app);
    let entity = state_layer
        .get_entity(SYSTEM_VAULT_ID, key.as_bytes())
        .map_err(|e| ledger_error(ErrorCode::Internal, format!("Failed to read app profile: {e}")))?
        .ok_or_else(|| {
            ledger_error(
                ErrorCode::NotFound,
                format!("App profile {} not found in organization {}", app, organization),
            )
        })?;
    decode::<AppProfile>(&entity.value).map_err(|e| {
        ledger_error(ErrorCode::Internal, format!("Failed to decode app profile: {e}"))
    })
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
        .map_err(|e| {
            ledger_error(ErrorCode::Internal, format!("Failed to read team profile: {e}"))
        })?
        .ok_or_else(|| ledger_error(ErrorCode::NotFound, format!("Team {} not found", team)))?;
    decode::<TeamProfile>(&entity.value).map_err(|e| {
        ledger_error(ErrorCode::Internal, format!("Failed to decode team profile: {e}"))
    })
}

/// Saves a team profile to the state layer.
fn save_team_profile<B: StorageBackend>(
    state_layer: &inferadb_ledger_state::StateLayer<B>,
    organization: OrganizationId,
    team: TeamId,
    profile: &TeamProfile,
) -> Result<(), LedgerResponse> {
    let key = SystemKeys::team_profile_key(organization, team);
    let bytes = encode(profile).map_err(|e| {
        ledger_error(ErrorCode::Internal, format!("Failed to encode team profile: {e}"))
    })?;
    let ops = vec![Operation::SetEntity { key, value: bytes, condition: None, expires_at: None }];
    state_layer.apply_operations(SYSTEM_VAULT_ID, &ops, 0).map(|_| ()).map_err(|e| {
        ledger_error(ErrorCode::Internal, format!("Failed to write team profile: {e}"))
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
        code: ErrorCode::NotFound,
        message: format!("Organization {} not found", organization),
    })?;
    if org.status == OrganizationStatus::Deleted {
        return Err(LedgerResponse::Error {
            code: ErrorCode::FailedPrecondition,
            message: format!("Cannot {} deleted organization {}", action, organization),
        });
    }
    state_layer.as_ref().ok_or_else(|| LedgerResponse::Error {
        code: ErrorCode::Internal,
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
                code: ErrorCode::FailedPrecondition,
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
            code: ErrorCode::NotFound,
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
        code: ErrorCode::Internal,
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
            code: ErrorCode::Internal,
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
        Ok(None) => Err(error_result(ErrorCode::NotFound, format!("Signing key not found: {kid}"))),
        Err(e) => {
            Err(error_result(ErrorCode::Internal, format!("Failed to look up signing key: {e}")))
        },
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
                            code: ErrorCode::FailedPrecondition,
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
                                    code: ErrorCode::Internal,
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
                                            code: ErrorCode::Internal,
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
                                code: ErrorCode::Internal,
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
                                    code: ErrorCode::Internal,
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

                // Compute read-only aggregates before mutating transactions.
                let storage_delta = estimate_write_storage_delta(transactions);
                let ops_count: u32 = transactions.iter().map(|tx| tx.operations.len() as u32).sum();

                // Update organization storage accounting.
                let storage_entry =
                    state.organization_storage_bytes.entry(*organization).or_insert(0);
                if storage_delta >= 0 {
                    *storage_entry = storage_entry.saturating_add(storage_delta as u64);
                } else {
                    *storage_entry = storage_entry.saturating_sub(storage_delta.unsigned_abs());
                }
                crate::metrics::set_organization_storage_bytes(*organization, *storage_entry);
                crate::metrics::record_organization_operation(*organization, "write");

                // Mirror updated OrganizationMeta (with new storage_bytes) to pending
                if let Some(org_meta) = state.organizations.get_mut(organization) {
                    org_meta.storage_bytes = *storage_entry;
                    if let Some(blob) = try_encode(org_meta, "org_meta") {
                        pending.organizations.push((*organization, blob));
                    }
                }

                // Server-assigned sequences: assign monotonic sequence to each transaction.
                // Clones each transaction because `request` is borrowed (&LedgerRequest).
                let mut assigned_sequence = 0u64;
                let sequenced_transactions: Vec<_> = transactions
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

                        if assigned_sequence == 0 {
                            assigned_sequence = new_sequence;
                        }

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
                    transactions: sequenced_transactions,
                    tx_merkle_root,
                    state_root,
                };

                // Compute block hash from vault entry (for response)
                let block_hash = self.compute_vault_block_hash(&vault_entry);

                // Emit WriteCommitted event
                let org_slug = state.id_to_slug.get(organization).copied();
                let vault_slug = state.vault_id_to_slug.get(vault).copied();
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
                for tx in &vault_entry.transactions {
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
                            code: ErrorCode::FailedPrecondition,
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
                            let _span = tracing::info_span!(
                                "cascade_revoke",
                                organization_id = organization.value()
                            )
                            .entered();
                            cascade_revoke(
                                state_layer,
                                |sys| {
                                    sys.revoke_all_org_refresh_tokens(
                                        *organization,
                                        block_timestamp,
                                    )
                                },
                                "Cascade-revoked refresh tokens on org soft-delete",
                                "Failed to cascade-revoke tokens on org soft-delete",
                            );
                        }

                        LedgerResponse::OrganizationDeleted {
                            organization_id: *organization,
                            deleted_at,
                            retention_days,
                        }
                    }
                } else {
                    LedgerResponse::Error {
                        code: ErrorCode::NotFound,
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
                        code: ErrorCode::NotFound,
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

            LedgerRequest::UpdateVault { organization, vault, retention_policy } => {
                let key = (*organization, *vault);
                let response = if let Some(vault_meta) = state.vaults.get_mut(&key) {
                    if vault_meta.deleted {
                        LedgerResponse::Error {
                            code: ErrorCode::NotFound,
                            message: format!("Vault {}:{} is deleted", organization, vault),
                        }
                    } else if let Some(policy) = retention_policy {
                        vault_meta.retention_policy = *policy;
                        // Re-serialize vault meta after mutation
                        if let Some(blob) = try_encode(vault_meta, "vault_meta") {
                            pending.vaults.push((vault_meta.vault, blob));
                        }
                        LedgerResponse::VaultUpdated { success: true }
                    } else {
                        // No fields to update — return success without re-serialization.
                        LedgerResponse::VaultUpdated { success: true }
                    }
                } else {
                    LedgerResponse::Error {
                        code: ErrorCode::NotFound,
                        message: format!("Vault {}:{} not found", organization, vault),
                    }
                };

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
                                    ErrorCode::FailedPrecondition,
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
                                    ErrorCode::FailedPrecondition,
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
                                    ErrorCode::FailedPrecondition,
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
                                    ErrorCode::FailedPrecondition,
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

            LedgerRequest::CreateOrganizationTeam { organization, slug } => {
                let response = match require_active_org_with_state(
                    organization,
                    state,
                    &self.state_layer,
                    "modify",
                ) {
                    Ok(state_layer) => {
                        // Check slug uniqueness
                        if state.team_slug_index.contains_key(slug) {
                            return (
                                LedgerResponse::Error {
                                    code: ErrorCode::AlreadyExists,
                                    message: format!("Team slug '{}' already exists", slug),
                                },
                                None,
                            );
                        }
                        let team_id = state.sequences.next_team();
                        // Only write the slug index to GLOBAL state. The TeamProfile
                        // (which contains PII like name) is written to REGIONAL state
                        // via a separate WriteTeamProfile proposal.
                        let slug_key = SystemKeys::team_slug_key(*slug);
                        let slug_value = format!("{}:{}", organization.value(), team_id.value());
                        let ops = vec![Operation::SetEntity {
                            key: slug_key,
                            value: slug_value.into_bytes(),
                            condition: None,
                            expires_at: None,
                        }];
                        if let Err(e) = state_layer.apply_operations(SYSTEM_VAULT_ID, &ops, 0) {
                            return (
                                LedgerResponse::Error {
                                    code: ErrorCode::Internal,
                                    message: format!("Failed to write team slug index: {e}"),
                                },
                                None,
                            );
                        }
                        // Update slug indices only after successful storage write
                        state.team_slug_index.insert(*slug, (*organization, team_id));
                        state.team_id_to_slug.insert(team_id, *slug);
                        pending.team_slug_index.push((*slug, (*organization, team_id)));

                        LedgerResponse::OrganizationTeamCreated { team_id, team_slug: *slug }
                    },
                    Err(err_response) => err_response,
                };

                if matches!(response, LedgerResponse::OrganizationTeamCreated { .. }) {
                    events.push(
                        ApplyPhaseEmitter::for_system(EventAction::TeamCreated)
                            .detail("organization_id", &organization.to_string())
                            .detail("team_slug", &slug.to_string())
                            .outcome(EventOutcome::Success)
                            .build(block_height, *op_index, block_timestamp, ttl_days),
                    );
                    *op_index += 1;
                }

                (response, None)
            },

            LedgerRequest::DeleteOrganizationTeam { organization, team } => {
                // GLOBAL cleanup only: slug index and in-memory maps.
                // Profile deletion and member migration are handled by REGIONAL
                // DeleteTeamProfile (proposed first by the service handler).
                let response = match require_active_org_with_state(
                    organization,
                    state,
                    &self.state_layer,
                    "modify",
                ) {
                    Ok(state_layer) => {
                        // Look up slug from in-memory index (no profile load needed)
                        let slug = match state.team_id_to_slug.get(team).copied() {
                            Some(s) => s,
                            None => {
                                return (
                                    LedgerResponse::Error {
                                        code: ErrorCode::NotFound,
                                        message: format!("Team {} not found", team),
                                    },
                                    None,
                                );
                            },
                        };

                        // Delete slug index from GLOBAL state
                        let slug_key = SystemKeys::team_slug_key(slug);
                        let ops = vec![Operation::DeleteEntity { key: slug_key }];
                        if let Err(e) = state_layer.apply_operations(SYSTEM_VAULT_ID, &ops, 0) {
                            return (
                                LedgerResponse::Error {
                                    code: ErrorCode::Internal,
                                    message: format!("Failed to delete team slug index: {e}"),
                                },
                                None,
                            );
                        }

                        // Clean up in-memory GLOBAL indices
                        state.team_slug_index.remove(&slug);
                        state.team_id_to_slug.remove(team);
                        pending.team_slug_index_deleted.push(slug);

                        LedgerResponse::OrganizationTeamDeleted { organization_id: *organization }
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

            // ================================================================
            // App operations
            // ================================================================
            LedgerRequest::CreateApp { organization, slug } => {
                let response = match require_active_org_with_state(
                    organization,
                    state,
                    &self.state_layer,
                    "modify",
                ) {
                    Ok(state_layer) => {
                        if state.app_slug_index.contains_key(slug) {
                            LedgerResponse::Error {
                                code: ErrorCode::AlreadyExists,
                                message: format!("App slug '{}' already exists", slug),
                            }
                        } else {
                            let app_id = state.sequences.next_app();
                            // Create app without name/description — these are PII and
                            // will be written separately via regional WriteAppProfile.
                            let app = App {
                                id: app_id,
                                slug: *slug,
                                organization: *organization,
                                name: String::new(),
                                description: None,
                                enabled: false,
                                credentials: AppCredentials::default(),
                                version: TokenVersion::default(),
                                created_at: block_timestamp,
                                updated_at: block_timestamp,
                            };
                            let app_bytes = match encode(&app) {
                                Ok(bytes) => bytes,
                                Err(e) => {
                                    return (
                                        LedgerResponse::Error {
                                            code: ErrorCode::Internal,
                                            message: format!("Failed to encode app: {e}"),
                                        },
                                        None,
                                    );
                                },
                            };
                            let app_key = SystemKeys::app_key(*organization, app_id);
                            let slug_key = SystemKeys::app_slug_key(*slug);
                            let slug_value = format!("{}:{}", organization.value(), app_id.value());
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
                            ];
                            if let Err(e) = state_layer.apply_operations(SYSTEM_VAULT_ID, &ops, 0) {
                                return (
                                    LedgerResponse::Error {
                                        code: ErrorCode::Internal,
                                        message: format!("Failed to write app: {e}"),
                                    },
                                    None,
                                );
                            }
                            state.app_slug_index.insert(*slug, (*organization, app_id));
                            state.app_id_to_slug.insert(app_id, *slug);
                            pending.app_slug_index.push((*slug, (*organization, app_id)));

                            LedgerResponse::AppCreated { app_id, app_slug: *slug }
                        }
                    },
                    Err(err_response) => err_response,
                };
                (response, None)
            },

            LedgerRequest::DeleteApp { organization, app } => {
                // GLOBAL cleanup: app record, slug index, vault connections,
                // assertions, and cascade token revocation.
                // Name index and AppProfile are REGIONAL — handled by
                // DeleteAppProfile (proposed first by the service handler).
                let response = match require_active_org_with_state(
                    organization,
                    state,
                    &self.state_layer,
                    "modify",
                ) {
                    Ok(state_layer) => {
                        // Look up slug from in-memory index (no profile load needed)
                        let slug = match state.app_id_to_slug.get(app).copied() {
                            Some(s) => s,
                            None => {
                                return (
                                    LedgerResponse::Error {
                                        code: ErrorCode::NotFound,
                                        message: format!("App {} not found", app),
                                    },
                                    None,
                                );
                            },
                        };

                        // Delete app record and slug index from GLOBAL state
                        let app_key = SystemKeys::app_key(*organization, *app);
                        let slug_key = SystemKeys::app_slug_key(slug);
                        let mut ops = vec![
                            Operation::DeleteEntity { key: app_key },
                            Operation::DeleteEntity { key: slug_key },
                        ];
                        // Delete all vault connections (paginated)
                        let vault_prefix = SystemKeys::app_vault_prefix(*organization, *app);
                        collect_all_entities_for_deletion(state_layer, &vault_prefix, &mut ops);
                        // Delete all assertion entries (paginated)
                        let assertion_prefix =
                            SystemKeys::app_assertion_prefix(*organization, *app);
                        collect_all_entities_for_deletion(state_layer, &assertion_prefix, &mut ops);
                        if let Err(e) = state_layer.apply_operations(SYSTEM_VAULT_ID, &ops, 0) {
                            return (
                                LedgerResponse::Error {
                                    code: ErrorCode::Internal,
                                    message: format!("Failed to delete app: {e}"),
                                },
                                None,
                            );
                        }
                        // Cascade: revoke all refresh tokens for this app
                        {
                            let _span = tracing::info_span!(
                                "cascade_revoke",
                                app_id = app.value(),
                                app_slug = slug.value()
                            )
                            .entered();
                            cascade_revoke(
                                state_layer,
                                |sys| {
                                    sys.revoke_all_subject_tokens(
                                        &TokenSubject::App(slug),
                                        block_timestamp,
                                    )
                                },
                                "Cascade-revoked refresh tokens on app delete",
                                "Failed to cascade-revoke tokens on app delete",
                            );
                        }

                        state.app_slug_index.remove(&slug);
                        state.app_id_to_slug.remove(app);
                        pending.app_slug_index_deleted.push(slug);

                        LedgerResponse::AppDeleted { organization_id: *organization }
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
                                            let slug = *app_slug;
                                            let _span = tracing::info_span!(
                                                "cascade_revoke",
                                                app_id = app.value(),
                                                app_slug = slug.value()
                                            )
                                            .entered();
                                            cascade_revoke(
                                                state_layer,
                                                |sys| {
                                                    sys.revoke_all_subject_tokens(
                                                        &TokenSubject::App(slug),
                                                        block_timestamp,
                                                    )
                                                },
                                                "Cascade-revoked refresh tokens on app disable",
                                                "Failed to cascade-revoke tokens on app disable",
                                            );
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
                                                code: ErrorCode::Internal,
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
                                            code: ErrorCode::Internal,
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
                                            code: ErrorCode::Internal,
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
                                code: ErrorCode::NotFound,
                                message: format!(
                                    "Assertion {} not found for app {} in organization {}",
                                    assertion, app, organization
                                ),
                            },
                            Err(e) => LedgerResponse::Error {
                                code: ErrorCode::Internal,
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
                                                            code: ErrorCode::Internal,
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
                                                code: ErrorCode::Internal,
                                                message: format!("Failed to encode assertion: {e}"),
                                            },
                                        }
                                    },
                                    Err(e) => LedgerResponse::Error {
                                        code: ErrorCode::Internal,
                                        message: format!("Failed to decode assertion: {e}"),
                                    },
                                }
                            },
                            Ok(None) => LedgerResponse::Error {
                                code: ErrorCode::NotFound,
                                message: format!(
                                    "Assertion {} not found for app {} in organization {}",
                                    assertion, app, organization
                                ),
                            },
                            Err(e) => LedgerResponse::Error {
                                code: ErrorCode::Internal,
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
                                        code: ErrorCode::AlreadyExists,
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
                                                        code: ErrorCode::Internal,
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
                                                    code: ErrorCode::Internal,
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
                                        code: ErrorCode::Internal,
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
                                                        code: ErrorCode::Internal,
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
                                            code: ErrorCode::Internal,
                                            message: format!(
                                                "Failed to encode vault connection: {e}"
                                            ),
                                        },
                                    }
                                },
                                Err(e) => LedgerResponse::Error {
                                    code: ErrorCode::Internal,
                                    message: format!("Failed to decode vault connection: {e}"),
                                },
                            },
                            Ok(None) => LedgerResponse::Error {
                                code: ErrorCode::NotFound,
                                message: format!(
                                    "Vault connection not found for vault {} on app {}",
                                    vault, app
                                ),
                            },
                            Err(e) => LedgerResponse::Error {
                                code: ErrorCode::Internal,
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
                                            code: ErrorCode::Internal,
                                            message: format!(
                                                "Failed to remove vault connection: {e}"
                                            ),
                                        },
                                        None,
                                    );
                                }
                                // Cascade: revoke all refresh tokens for this app+vault
                                if let Some(app_slug) = state.app_id_to_slug.get(app) {
                                    let slug = *app_slug;
                                    let _span = tracing::info_span!(
                                        "cascade_revoke",
                                        app_id = app.value(),
                                        vault_id = vault.value()
                                    )
                                    .entered();
                                    cascade_revoke(
                                        state_layer,
                                        |sys| {
                                            sys.revoke_app_vault_tokens(
                                                slug,
                                                *vault,
                                                block_timestamp,
                                            )
                                        },
                                        "Cascade-revoked refresh tokens on vault disconnect",
                                        "Failed to cascade-revoke tokens on vault disconnect",
                                    );
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
                                code: ErrorCode::NotFound,
                                message: format!(
                                    "Vault connection not found for vault {} on app {}",
                                    vault, app
                                ),
                            },
                            Err(e) => LedgerResponse::Error {
                                code: ErrorCode::Internal,
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
                            code: ErrorCode::FailedPrecondition,
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

                        // team_name_index and app_name_index are REGIONAL — cleaned up by
                        // PurgeOrganizationRegional (proposed first by the service handler).

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
                            // Clean up team slug index keys (profiles are REGIONAL)
                            for (team_slug, _team_id) in &team_slugs_to_remove {
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
                            // App name index and app profile keys are REGIONAL —
                            // cleaned up by PurgeOrganizationRegional.
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
                            {
                                let _span = tracing::info_span!(
                                    "cascade_revoke",
                                    organization_id = organization.value()
                                )
                                .entered();
                                match sys
                                    .revoke_all_org_refresh_tokens(*organization, block_timestamp)
                                {
                                    Ok(result) => {
                                        tracing::info!(
                                            revoked_count = result.revoked_count,
                                            "Revoked org refresh tokens during purge"
                                        );
                                    },
                                    Err(e) => {
                                        tracing::error!(error = %e, "Failed to revoke org refresh tokens during purge");
                                    },
                                }
                            }
                        }

                        LedgerResponse::OrganizationPurged { organization_id: *organization }
                    }
                } else {
                    LedgerResponse::Error {
                        code: ErrorCode::NotFound,
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
                            code: ErrorCode::FailedPrecondition,
                            message: format!(
                                "Cannot suspend deleted organization {}",
                                organization
                            ),
                        },
                        OrganizationStatus::Suspended => LedgerResponse::Error {
                            code: ErrorCode::FailedPrecondition,
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
                        code: ErrorCode::NotFound,
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
                            code: ErrorCode::FailedPrecondition,
                            message: format!("Organization {} is not suspended", organization),
                        },
                        OrganizationStatus::Deleted => LedgerResponse::Error {
                            code: ErrorCode::FailedPrecondition,
                            message: format!("Cannot resume deleted organization {}", organization),
                        },
                        other => LedgerResponse::Error {
                            code: ErrorCode::FailedPrecondition,
                            message: format!(
                                "Cannot resume organization {} in state {:?}",
                                organization, other
                            ),
                        },
                    }
                } else {
                    LedgerResponse::Error {
                        code: ErrorCode::NotFound,
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
                                    code: ErrorCode::FailedPrecondition,
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
                            code: ErrorCode::FailedPrecondition,
                            message: format!("Organization {} is already migrating", organization),
                        },
                        OrganizationStatus::Suspended => LedgerResponse::Error {
                            code: ErrorCode::FailedPrecondition,
                            message: format!(
                                "Cannot start migration on suspended organization {}",
                                organization
                            ),
                        },
                        OrganizationStatus::Provisioning => LedgerResponse::Error {
                            code: ErrorCode::FailedPrecondition,
                            message: format!(
                                "Cannot start migration on provisioning organization {}",
                                organization
                            ),
                        },
                        OrganizationStatus::Deleted => LedgerResponse::Error {
                            code: ErrorCode::FailedPrecondition,
                            message: format!(
                                "Cannot start migration on deleted organization {}",
                                organization
                            ),
                        },
                    }
                } else {
                    LedgerResponse::Error {
                        code: ErrorCode::NotFound,
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
                                    code: ErrorCode::FailedPrecondition,
                                    message: format!(
                                        "Organization {} is migrating but has no target region",
                                        organization
                                    ),
                                }
                            }
                        },
                        OrganizationStatus::Active => LedgerResponse::Error {
                            code: ErrorCode::FailedPrecondition,
                            message: format!("Organization {} is not migrating", organization),
                        },
                        other => LedgerResponse::Error {
                            code: ErrorCode::FailedPrecondition,
                            message: format!(
                                "Cannot complete migration for organization {} in state {:?}",
                                organization, other
                            ),
                        },
                    }
                } else {
                    LedgerResponse::Error {
                        code: ErrorCode::NotFound,
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
                    SystemRequest::UpdateUser { user_id, role, primary_email } => {
                        if let Some(sys) = &sys_service {
                            match sys.update_user(*user_id, *role, *primary_email) {
                                Ok(_user) => LedgerResponse::UserUpdated { user_id: *user_id },
                                Err(e) => LedgerResponse::Error {
                                    code: ErrorCode::Internal,
                                    message: format!("Failed to update user: {e}"),
                                },
                            }
                        } else {
                            LedgerResponse::Empty
                        }
                    },
                    SystemRequest::UpdateUserProfile { user_id, name } => {
                        if let Some(sys) = &sys_service {
                            match sys.update_user_profile(*user_id, name) {
                                Ok(_user) => {
                                    LedgerResponse::UserProfileUpdated { user_id: *user_id }
                                },
                                Err(e) => LedgerResponse::Error {
                                    code: ErrorCode::Internal,
                                    message: format!("Failed to update user profile: {e}"),
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
                                    code: ErrorCode::Internal,
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
                                    code: ErrorCode::Internal,
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
                                    code: ErrorCode::Internal,
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
                                    code: ErrorCode::Internal,
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
                                    code: ErrorCode::FailedPrecondition,
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
                                code: ErrorCode::NotFound,
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
                                    code: ErrorCode::AlreadyExists,
                                    message: format!("Email hash already registered: {hmac_hex}"),
                                },
                                Err(e) => LedgerResponse::Error {
                                    code: ErrorCode::Internal,
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
                                    code: ErrorCode::Internal,
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
                                    code: ErrorCode::Internal,
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
                                    code: ErrorCode::Internal,
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
                                    code: ErrorCode::Internal,
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
                            if let Err(e) =
                                sys.update_user_directory_status(*user_id, *status, block_timestamp)
                            {
                                LedgerResponse::Error {
                                    code: ErrorCode::Internal,
                                    message: format!("Failed to update user directory status: {e}"),
                                }
                            } else if let Some(new_region) = region {
                                // Then update region if provided
                                if let Err(e) =
                                    sys.update_user_directory_region(*user_id, *new_region)
                                {
                                    LedgerResponse::Error {
                                        code: ErrorCode::Internal,
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
                    SystemRequest::EraseUser { user_id, region } => {
                        if let Some(sys) = &sys_service {
                            // Forward-only finalization: each step idempotent.
                            // The service method handles directory update, email hash
                            // removal, subject key deletion, and audit record creation.
                            // Actor identity is captured in canonical log lines, not
                            // replicated via Raft (no PII in GLOBAL log).
                            if let Err(e) = sys.erase_user(*user_id, *region, block_timestamp) {
                                LedgerResponse::Error {
                                    code: ErrorCode::Internal,
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
                                    code: ErrorCode::Internal,
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
                                    .map(|(id, _)| NodeId::new(id.to_string()))
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
                        sealed_name,
                        name_nonce,
                        admin,
                        org_key_bytes,
                    } => {
                        if let Some(state_layer) = &self.state_layer {
                            // Step 1: Store the OrgKey for future crypto-shredding.
                            let sys = inferadb_ledger_state::system::SystemOrganizationService::new(
                                state_layer.clone(),
                            );
                            if let Err(e) = sys.store_org_key(*organization, org_key_bytes) {
                                tracing::error!(
                                    organization_id = organization.value(),
                                    error = %e,
                                    "Failed to store OrgKey"
                                );
                                return (
                                    LedgerResponse::Error {
                                        code: ErrorCode::Internal,
                                        message: format!("Failed to store OrgKey: {e}"),
                                    },
                                    None,
                                );
                            }

                            // Step 2: Unseal the organization name.
                            let aad = organization.value().to_le_bytes();
                            let name = match crate::entry_crypto::unseal(
                                sealed_name,
                                name_nonce,
                                org_key_bytes,
                                &aad,
                            ) {
                                Ok(bytes) => match String::from_utf8(bytes) {
                                    Ok(s) => s,
                                    Err(e) => {
                                        tracing::error!(
                                            organization_id = organization.value(),
                                            error = %e,
                                            "Unsealed organization name is not valid UTF-8"
                                        );
                                        return (
                                            LedgerResponse::Error {
                                                code: ErrorCode::Internal,
                                                message: format!(
                                                    "Unsealed organization name is not valid UTF-8: {e}"
                                                ),
                                            },
                                            None,
                                        );
                                    },
                                },
                                Err(e) => {
                                    tracing::error!(
                                        organization_id = organization.value(),
                                        error = %e,
                                        "Failed to unseal organization name"
                                    );
                                    return (
                                        LedgerResponse::Error {
                                            code: ErrorCode::Internal,
                                            message: format!(
                                                "Failed to unseal organization name: {e}"
                                            ),
                                        },
                                        None,
                                    );
                                },
                            };

                            // Step 3: Look up org metadata for region/tier/slug.
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
                                        code: ErrorCode::NotFound,
                                        message: format!("Organization {} not found", organization),
                                    },
                                    None,
                                );
                            };

                            let profile = OrganizationProfile {
                                organization: *organization,
                                slug,
                                region,
                                name,
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
                                let ops = vec![Operation::SetEntity {
                                    key: final_key,
                                    value: profile_bytes,
                                    condition: None,
                                    expires_at: None,
                                }];
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
                    SystemRequest::UpdateOrganizationProfile { organization, name } => {
                        if let Some(state_layer) = &self.state_layer {
                            match load_org_profile(state_layer, *organization) {
                                Ok(mut profile) => {
                                    profile.name = name.clone();
                                    profile.updated_at = block_timestamp;
                                    if let Err(e) =
                                        save_org_profile(state_layer, *organization, &profile)
                                    {
                                        return (e, None);
                                    }
                                    LedgerResponse::OrganizationUpdated {
                                        organization_id: *organization,
                                    }
                                },
                                Err(e) => e,
                            }
                        } else {
                            ledger_error(
                                ErrorCode::Internal,
                                "State layer unavailable for profile update".to_string(),
                            )
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
                                code: ErrorCode::NotFound,
                                message: format!("Organization {} not found", organization),
                            }
                        }
                    },

                    // ── Onboarding: CreateEmailVerification (REGIONAL) ──
                    SystemRequest::CreateEmailVerification {
                        email_hmac,
                        code_hash,
                        region,
                        expires_at,
                    } => {
                        if let Some(sys) = &sys_service {
                            // 1. Read existing record (if any)
                            let existing = match sys.get_email_verification(email_hmac) {
                                Ok(record) => record,
                                Err(e) => {
                                    return (
                                        LedgerResponse::Error {
                                            code: ErrorCode::Internal,
                                            message: format!(
                                                "Failed to read verification record: {e}"
                                            ),
                                        },
                                        None,
                                    );
                                },
                            };

                            // 2. Rate limit check
                            let window = chrono::Duration::from_std(
                                inferadb_ledger_types::onboarding::RATE_LIMIT_WINDOW,
                            )
                            .unwrap_or(chrono::Duration::hours(1));

                            let (new_count, window_start) = if let Some(ref rec) = existing {
                                let elapsed = block_timestamp - rec.rate_limit_window_start;
                                if elapsed < window {
                                    // Window still active
                                    if rec.rate_limit_count
                                        >= inferadb_ledger_types::onboarding::MAX_INITIATIONS_PER_HOUR
                                    {
                                        return (
                                            LedgerResponse::Error {
                                                code: ErrorCode::RateLimited,
                                                message: "Too many verification requests"
                                                    .to_string(),
                                            },
                                            None,
                                        );
                                    }
                                    (rec.rate_limit_count + 1, rec.rate_limit_window_start)
                                } else {
                                    // Window expired — reset
                                    (1, block_timestamp)
                                }
                            } else {
                                // No existing record — start fresh
                                (1, block_timestamp)
                            };

                            // 3. Write verification record
                            let record = PendingEmailVerification {
                                code_hash: *code_hash,
                                region: *region,
                                expires_at: *expires_at,
                                attempts: 0,
                                rate_limit_count: new_count,
                                rate_limit_window_start: window_start,
                            };

                            match sys.store_email_verification(email_hmac, &record) {
                                Ok(()) => LedgerResponse::EmailVerificationCreated,
                                Err(e) => LedgerResponse::Error {
                                    code: ErrorCode::Internal,
                                    message: format!("Failed to store verification record: {e}"),
                                },
                            }
                        } else {
                            LedgerResponse::EmailVerificationCreated
                        }
                    },

                    // ── Onboarding: VerifyEmailCode (REGIONAL) ──
                    SystemRequest::VerifyEmailCode {
                        email_hmac,
                        code_hash,
                        region,
                        existing_user_hmac_hit,
                        onboarding_token_hash,
                        onboarding_expires_at,
                    } => {
                        if let Some(sys) = &sys_service {
                            // 1. Read pending verification
                            let record = match sys.get_email_verification(email_hmac) {
                                Ok(Some(rec)) => rec,
                                Ok(None) => {
                                    return (
                                        LedgerResponse::Error {
                                            code: ErrorCode::NotFound,
                                            message: "No pending verification".to_string(),
                                        },
                                        None,
                                    );
                                },
                                Err(e) => {
                                    return (
                                        LedgerResponse::Error {
                                            code: ErrorCode::Internal,
                                            message: format!("Failed to read verification: {e}"),
                                        },
                                        None,
                                    );
                                },
                            };

                            // 2. Check expiry
                            if record.expires_at < block_timestamp {
                                let _ = sys.delete_email_verification(email_hmac);
                                return (
                                    LedgerResponse::Error {
                                        code: ErrorCode::Expired,
                                        message: "Verification code expired".to_string(),
                                    },
                                    None,
                                );
                            }

                            // 3. Check max attempts
                            if record.attempts
                                >= inferadb_ledger_types::onboarding::MAX_CODE_ATTEMPTS
                            {
                                return (
                                    LedgerResponse::Error {
                                        code: ErrorCode::TooManyAttempts,
                                        message: "Too many verification attempts".to_string(),
                                    },
                                    None,
                                );
                            }

                            // 4. Validate code (constant-time comparison)
                            if !hash_eq(code_hash, &record.code_hash) {
                                // Increment attempts and write back
                                let updated = PendingEmailVerification {
                                    attempts: record.attempts + 1,
                                    ..record
                                };
                                let _ = sys.store_email_verification(email_hmac, &updated);
                                return (
                                    LedgerResponse::Error {
                                        code: ErrorCode::InvalidArgument,
                                        message: "Invalid verification code".to_string(),
                                    },
                                    None,
                                );
                            }

                            // 5. Code valid — delete verification record
                            let _ = sys.delete_email_verification(email_hmac);

                            // 6. Branch on existing_user_hmac_hit
                            if *existing_user_hmac_hit {
                                // Existing user: signal only, no writes
                                LedgerResponse::EmailCodeVerified {
                                    result: EmailCodeVerifiedResult::ExistingUser,
                                }
                            } else {
                                // New user: create OnboardingAccount
                                let account = OnboardingAccount {
                                    token_hash: *onboarding_token_hash,
                                    region: *region,
                                    expires_at: *onboarding_expires_at,
                                    created_at: block_timestamp,
                                };
                                match sys.store_onboarding_account(email_hmac, &account) {
                                    Ok(()) => LedgerResponse::EmailCodeVerified {
                                        result: EmailCodeVerifiedResult::NewUser,
                                    },
                                    Err(e) => LedgerResponse::Error {
                                        code: ErrorCode::Internal,
                                        message: format!("Failed to store onboarding account: {e}"),
                                    },
                                }
                            }
                        } else {
                            LedgerResponse::EmailCodeVerified {
                                result: EmailCodeVerifiedResult::NewUser,
                            }
                        }
                    },

                    // ── Onboarding: CreateOnboardingUser — Saga Step 0 (GLOBAL) ──
                    SystemRequest::CreateOnboardingUser {
                        email_hmac,
                        user_slug,
                        organization_slug,
                        region,
                    } => {
                        if let Some(sys) = &sys_service {
                            // 1. Idempotency guard: read HMAC index
                            match sys.get_email_hash(email_hmac) {
                                Ok(Some(EmailHashEntry::Active(_))) => {
                                    return (
                                        LedgerResponse::Error {
                                            code: ErrorCode::AlreadyExists,
                                            message: "Email already registered".to_string(),
                                        },
                                        None,
                                    );
                                },
                                Ok(Some(EmailHashEntry::Provisioning(ref reservation))) => {
                                    // Check if this is our own saga retry
                                    if let Ok(Some(dir)) =
                                        sys.get_user_directory(reservation.user_id)
                                        && dir.status == UserDirectoryStatus::Provisioning
                                        && dir.slug == Some(*user_slug)
                                    {
                                        // Step 0 already ran — idempotent return
                                        return (
                                            LedgerResponse::OnboardingUserCreated {
                                                user_id: reservation.user_id,
                                                organization_id: reservation.organization_id,
                                            },
                                            None,
                                        );
                                    }
                                    // Different saga — email reserved by another
                                    return (
                                        LedgerResponse::Error {
                                            code: ErrorCode::AlreadyExists,
                                            message: "Email already registered".to_string(),
                                        },
                                        None,
                                    );
                                },
                                Ok(None) => {
                                    // No existing entry — proceed
                                },
                                Err(e) => {
                                    return (
                                        LedgerResponse::Error {
                                            code: ErrorCode::Internal,
                                            message: format!("Failed to read email hash: {e}"),
                                        },
                                        None,
                                    );
                                },
                            }

                            // 2. Allocate user ID
                            let user_id = state.sequences.next_user();

                            // 3. Allocate organization ID
                            let organization_id = state.sequences.next_organization();

                            // 4. Reserve HMAC as Provisioning (CAS MustNotExist)
                            let reservation = ProvisioningReservation { user_id, organization_id };
                            let entry = EmailHashEntry::Provisioning(reservation);
                            let hmac_key = SystemKeys::email_hash_index_key(email_hmac);
                            let hmac_value = match encode(&entry) {
                                Ok(v) => v,
                                Err(e) => {
                                    return (
                                        LedgerResponse::Error {
                                            code: ErrorCode::Internal,
                                            message: format!(
                                                "Failed to encode email hash entry: {e}"
                                            ),
                                        },
                                        None,
                                    );
                                },
                            };
                            let hmac_ops = vec![Operation::SetEntity {
                                key: hmac_key,
                                value: hmac_value,
                                condition: Some(inferadb_ledger_types::SetCondition::MustNotExist),
                                expires_at: None,
                            }];
                            if let Some(state_layer) = &self.state_layer
                                && let Err(e) =
                                    state_layer.apply_operations(SYSTEM_VAULT_ID, &hmac_ops, 0)
                            {
                                return (
                                    LedgerResponse::Error {
                                        code: ErrorCode::AlreadyExists,
                                        message: format!("Email hash CAS failed (race): {e}"),
                                    },
                                    None,
                                );
                            }

                            // 5. Write GLOBAL user directory (Provisioning status)
                            let user_dir = UserDirectoryEntry {
                                user: user_id,
                                slug: Some(*user_slug),
                                region: Some(*region),
                                status: UserDirectoryStatus::Provisioning,
                                updated_at: Some(block_timestamp),
                            };
                            if let Err(e) = sys.register_user_directory(&user_dir) {
                                tracing::error!(
                                    user_id = user_id.value(),
                                    error = %e,
                                    "Failed to write user directory"
                                );
                            }

                            // 6. Update in-memory user indices
                            state.user_slug_index.insert(*user_slug, user_id);
                            state.user_id_to_slug.insert(user_id, *user_slug);
                            pending.user_slug_index.push((*user_slug, user_id));

                            // 7. Write GLOBAL org directory (Provisioning status)
                            let org_meta = OrganizationMeta {
                                organization: organization_id,
                                slug: *organization_slug,
                                region: *region,
                                status: OrganizationStatus::Provisioning,
                                tier: OrganizationTier::default(),
                                pending_region: None,
                                storage_bytes: 0,
                            };
                            if let Some(blob) = try_encode(&org_meta, "org_meta") {
                                pending.organizations.push((organization_id, blob));
                            }
                            state.organizations.insert(organization_id, org_meta);
                            state.slug_index.insert(*organization_slug, organization_id);
                            state.id_to_slug.insert(organization_id, *organization_slug);
                            pending.slug_index.push((*organization_slug, organization_id));

                            // Write org registry to state layer
                            let registry = OrganizationRegistry {
                                organization_id,
                                region: *region,
                                member_nodes: state
                                    .membership
                                    .membership()
                                    .nodes()
                                    .map(|(id, _)| NodeId::new(id.to_string()))
                                    .collect(),
                                status: OrganizationStatus::Provisioning,
                                config_version: 1,
                                created_at: block_timestamp,
                                deleted_at: None,
                            };
                            if let Err(e) = sys.register_organization(&registry, *organization_slug)
                            {
                                tracing::error!(
                                    organization_id = organization_id.value(),
                                    error = %e,
                                    "Failed to persist org registry"
                                );
                            }

                            // Write org directory entry to state layer
                            let org_dir = OrganizationDirectoryEntry {
                                organization: organization_id,
                                slug: Some(*organization_slug),
                                region: Some(*region),
                                tier: OrganizationTier::default(),
                                status: OrganizationDirectoryStatus::Provisioning,
                                updated_at: Some(block_timestamp),
                            };
                            if let Err(e) = sys.register_organization_directory(&org_dir) {
                                tracing::error!(
                                    organization_id = organization_id.value(),
                                    error = %e,
                                    "Failed to persist org directory entry"
                                );
                            }

                            LedgerResponse::OnboardingUserCreated { user_id, organization_id }
                        } else {
                            LedgerResponse::Error {
                                code: ErrorCode::Internal,
                                message: "State layer not available".to_string(),
                            }
                        }
                    },

                    SystemRequest::WriteOnboardingUserProfile {
                        user_id,
                        user_slug,
                        organization_id,
                        organization_slug,
                        email_hmac,
                        sealed_pii,
                        pii_nonce,
                        subject_key_bytes,
                        refresh_token_hash,
                        refresh_family_id,
                        refresh_expires_at,
                        kid,
                        region,
                    } => {
                        if let Some(state_layer) = &self.state_layer {
                            let sys = SystemOrganizationService::new(state_layer.clone());

                            // Unseal PII using the subject key from this entry
                            let aad = user_id.value().to_le_bytes();
                            let pii_bytes = match crate::entry_crypto::unseal(
                                sealed_pii,
                                pii_nonce,
                                subject_key_bytes,
                                &aad,
                            ) {
                                Ok(bytes) => bytes,
                                Err(e) => {
                                    return (
                                        LedgerResponse::Error {
                                            code: ErrorCode::Internal,
                                            message: format!(
                                                "Failed to unseal onboarding PII: {e}"
                                            ),
                                        },
                                        None,
                                    );
                                },
                            };
                            let pii: crate::entry_crypto::OnboardingPii =
                                match postcard::from_bytes(&pii_bytes) {
                                    Ok(p) => p,
                                    Err(e) => {
                                        return (
                                            LedgerResponse::Error {
                                                code: ErrorCode::Internal,
                                                message: format!(
                                                    "Failed to deserialize onboarding PII: {e}"
                                                ),
                                            },
                                            None,
                                        );
                                    },
                                };

                            // 1. Write User record (Provisioning — activated in step 2)
                            let email_id = state.sequences.next_user_email();
                            let user = User {
                                id: *user_id,
                                slug: *user_slug,
                                region: *region,
                                name: pii.name.clone(),
                                email: email_id,
                                status: UserStatus::PendingOrg,
                                role: UserRole::default(),
                                created_at: block_timestamp,
                                updated_at: block_timestamp,
                                deleted_at: None,
                                version: TokenVersion::default(),
                            };
                            let user_key = SystemKeys::user_key(*user_id);
                            let user_value = match encode(&user) {
                                Ok(v) => v,
                                Err(e) => {
                                    return (
                                        LedgerResponse::Error {
                                            code: ErrorCode::Internal,
                                            message: format!("Failed to encode user: {e}"),
                                        },
                                        None,
                                    );
                                },
                            };

                            // 2. Write UserEmail record (verified_at set)
                            let user_email = UserEmail {
                                id: email_id,
                                user: *user_id,
                                email: pii.email.to_lowercase(),
                                created_at: block_timestamp,
                                verified_at: Some(block_timestamp),
                            };
                            let email_key = SystemKeys::user_email_key(email_id);
                            let email_value = match encode(&user_email) {
                                Ok(v) => v,
                                Err(e) => {
                                    return (
                                        LedgerResponse::Error {
                                            code: ErrorCode::Internal,
                                            message: format!("Failed to encode user email: {e}"),
                                        },
                                        None,
                                    );
                                },
                            };

                            // Write user + email atomically
                            let user_ops = vec![
                                Operation::SetEntity {
                                    key: user_key,
                                    value: user_value,
                                    condition: None,
                                    expires_at: None,
                                },
                                Operation::SetEntity {
                                    key: email_key,
                                    value: email_value,
                                    condition: None,
                                    expires_at: None,
                                },
                            ];
                            if let Err(e) =
                                state_layer.apply_operations(SYSTEM_VAULT_ID, &user_ops, 0)
                            {
                                return (
                                    LedgerResponse::Error {
                                        code: ErrorCode::Internal,
                                        message: format!("Failed to write user records: {e}"),
                                    },
                                    None,
                                );
                            }

                            // 3. Store subject encryption key
                            if let Err(e) = sys.store_subject_key(*user_id, subject_key_bytes) {
                                return (
                                    LedgerResponse::Error {
                                        code: ErrorCode::Internal,
                                        message: format!("Failed to store subject key: {e}"),
                                    },
                                    None,
                                );
                            }

                            // 4. Create refresh token
                            let refresh_token_id = state.sequences.next_refresh_token();
                            let token = RefreshToken {
                                id: refresh_token_id,
                                token_hash: *refresh_token_hash,
                                family: *refresh_family_id,
                                token_type: TokenType::UserSession,
                                subject: TokenSubject::User(*user_slug),
                                organization: Some(*organization_id),
                                vault: None,
                                kid: kid.clone(),
                                expires_at: *refresh_expires_at,
                                used: false,
                                created_at: block_timestamp,
                                used_at: None,
                                revoked_at: None,
                            };
                            if let Err(e) = sys.store_refresh_token(&token) {
                                return (
                                    LedgerResponse::Error {
                                        code: ErrorCode::Internal,
                                        message: format!("Failed to store refresh token: {e}"),
                                    },
                                    None,
                                );
                            }

                            // 5. Write organization profile (PII regional)
                            let profile = OrganizationProfile {
                                organization: *organization_id,
                                slug: *organization_slug,
                                region: *region,
                                name: pii.organization_name.clone(),
                                tier: OrganizationTier::default(),
                                status: OrganizationStatus::Provisioning,
                                members: vec![OrganizationMember {
                                    user_id: *user_id,
                                    role: OrganizationMemberRole::Admin,
                                    joined_at: block_timestamp,
                                }],
                                created_at: block_timestamp,
                                updated_at: block_timestamp,
                                deleted_at: None,
                            };
                            if let Err(e) =
                                save_org_profile(state_layer, *organization_id, &profile)
                            {
                                return (e, None);
                            }

                            // 6. Delete onboarding account record
                            let account_key = SystemKeys::onboard_account_key(email_hmac);
                            let cleanup_ops = vec![Operation::DeleteEntity { key: account_key }];
                            if let Err(e) =
                                state_layer.apply_operations(SYSTEM_VAULT_ID, &cleanup_ops, 0)
                            {
                                tracing::warn!(
                                    email_hmac = %email_hmac,
                                    error = %e,
                                    "Failed to delete onboarding account (non-fatal)"
                                );
                            }

                            LedgerResponse::OnboardingUserProfileWritten { refresh_token_id }
                        } else {
                            LedgerResponse::Error {
                                code: ErrorCode::Internal,
                                message: "State layer not available".to_string(),
                            }
                        }
                    },

                    SystemRequest::ActivateOnboardingUser {
                        user_id,
                        user_slug,
                        organization_id,
                        organization_slug,
                        email_hmac,
                    } => {
                        if let Some(sys) = &sys_service {
                            // 1. Activate user directory (Provisioning → Active)
                            if let Err(e) = sys.update_user_directory_status(
                                *user_id,
                                UserDirectoryStatus::Active,
                                block_timestamp,
                            ) {
                                return (
                                    LedgerResponse::Error {
                                        code: ErrorCode::Internal,
                                        message: format!("Failed to activate user directory: {e}"),
                                    },
                                    None,
                                );
                            }

                            // 2. Activate org directory (Provisioning → Active)
                            if let Err(e) = sys.update_organization_directory_status(
                                *organization_id,
                                OrganizationDirectoryStatus::Active,
                                block_timestamp,
                            ) {
                                return (
                                    LedgerResponse::Error {
                                        code: ErrorCode::Internal,
                                        message: format!("Failed to activate org directory: {e}"),
                                    },
                                    None,
                                );
                            }

                            // 3. Update HMAC index: Provisioning → Active(user_id)
                            let hmac_key = SystemKeys::email_hash_index_key(email_hmac);
                            let active_entry = EmailHashEntry::Active(*user_id);
                            let hmac_value = match encode(&active_entry) {
                                Ok(v) => v,
                                Err(e) => {
                                    return (
                                        LedgerResponse::Error {
                                            code: ErrorCode::Internal,
                                            message: format!(
                                                "Failed to encode active HMAC entry: {e}"
                                            ),
                                        },
                                        None,
                                    );
                                },
                            };
                            let hmac_ops = vec![Operation::SetEntity {
                                key: hmac_key,
                                value: hmac_value,
                                condition: None,
                                expires_at: None,
                            }];
                            if let Some(state_layer) = &self.state_layer
                                && let Err(e) =
                                    state_layer.apply_operations(SYSTEM_VAULT_ID, &hmac_ops, 0)
                            {
                                return (
                                    LedgerResponse::Error {
                                        code: ErrorCode::Internal,
                                        message: format!("Failed to activate HMAC index: {e}"),
                                    },
                                    None,
                                );
                            }

                            // 4. Update in-memory org meta status
                            if let Some(meta) = state.organizations.get_mut(organization_id) {
                                meta.status = OrganizationStatus::Active;
                            }

                            // 5. Update org registry status
                            if let Ok(Some(mut registry)) = sys.get_organization(*organization_id) {
                                registry.status = OrganizationStatus::Active;
                                if let Err(e) =
                                    sys.register_organization(&registry, *organization_slug)
                                {
                                    return (
                                        LedgerResponse::Error {
                                            code: ErrorCode::Internal,
                                            message: format!(
                                                "Failed to activate org registry: {e}"
                                            ),
                                        },
                                        None,
                                    );
                                }
                            }

                            // Ensure slug indices are up to date
                            state.slug_index.entry(*organization_slug).or_insert(*organization_id);
                            state.user_slug_index.entry(*user_slug).or_insert(*user_id);

                            LedgerResponse::OnboardingUserActivated
                        } else {
                            LedgerResponse::Error {
                                code: ErrorCode::Internal,
                                message: "System service not available".to_string(),
                            }
                        }
                    },

                    SystemRequest::CleanupExpiredOnboarding => {
                        if let Some(state_layer) = &self.state_layer {
                            let mut codes_deleted: u32 = 0;
                            let mut accounts_deleted: u32 = 0;

                            // 1. Scan expired verification codes
                            if let Ok(entities) = state_layer.list_entities(
                                SYSTEM_VAULT_ID,
                                Some(SystemKeys::ONBOARD_VERIFY_PREFIX),
                                None,
                                inferadb_ledger_types::onboarding::MAX_ONBOARDING_SCAN,
                            ) {
                                let mut delete_ops = Vec::new();
                                for entity in &entities {
                                    let key_str = String::from_utf8_lossy(&entity.key).to_string();
                                    if let Ok(record) =
                                        decode::<PendingEmailVerification>(&entity.value)
                                        && record.expires_at <= block_timestamp
                                    {
                                        delete_ops.push(Operation::DeleteEntity { key: key_str });
                                    }
                                }
                                codes_deleted = delete_ops.len() as u32;
                                if !delete_ops.is_empty()
                                    && let Err(e) = state_layer.apply_operations(
                                        SYSTEM_VAULT_ID,
                                        &delete_ops,
                                        0,
                                    )
                                {
                                    tracing::warn!(
                                        error = %e,
                                        "Failed to delete expired verification codes"
                                    );
                                }
                            }

                            // 2. Scan expired onboarding accounts
                            if let Ok(entities) = state_layer.list_entities(
                                SYSTEM_VAULT_ID,
                                Some(SystemKeys::ONBOARD_ACCOUNT_PREFIX),
                                None,
                                inferadb_ledger_types::onboarding::MAX_ONBOARDING_SCAN,
                            ) {
                                let mut delete_ops = Vec::new();
                                for entity in &entities {
                                    let key_str = String::from_utf8_lossy(&entity.key).to_string();
                                    if let Ok(record) = decode::<OnboardingAccount>(&entity.value)
                                        && record.expires_at <= block_timestamp
                                    {
                                        delete_ops.push(Operation::DeleteEntity { key: key_str });
                                    }
                                }
                                accounts_deleted = delete_ops.len() as u32;
                                if !delete_ops.is_empty()
                                    && let Err(e) = state_layer.apply_operations(
                                        SYSTEM_VAULT_ID,
                                        &delete_ops,
                                        0,
                                    )
                                {
                                    tracing::warn!(
                                        error = %e,
                                        "Failed to delete expired onboarding accounts"
                                    );
                                }
                            }

                            LedgerResponse::OnboardingCleanedUp {
                                verification_codes_deleted: codes_deleted,
                                onboarding_accounts_deleted: accounts_deleted,
                            }
                        } else {
                            LedgerResponse::Error {
                                code: ErrorCode::Internal,
                                message: "State layer not available".to_string(),
                            }
                        }
                    },

                    // ── WriteTeamProfile (REGIONAL) ──
                    // Upsert: creates profile from scratch on first call (REGIONAL
                    // state has no data from GLOBAL CreateOrganizationTeam), or
                    // updates name on subsequent calls (rename).
                    SystemRequest::WriteTeamProfile { organization, team, slug, name } => {
                        if let Some(state_layer) = &self.state_layer {
                            if has_team_name_conflict(state, *organization, name, Some(*team)) {
                                return error_result(
                                    ErrorCode::AlreadyExists,
                                    format!(
                                        "Team name '{}' already exists in organization {}",
                                        name, organization
                                    ),
                                );
                            }

                            let (profile, old_name) =
                                match load_team_profile(state_layer, *organization, *team) {
                                    Ok(mut existing) => {
                                        let old = existing.name.clone();
                                        existing.name.clone_from(name);
                                        existing.updated_at = block_timestamp;
                                        (existing, Some(old))
                                    },
                                    Err(_) => {
                                        // First write — create from scratch
                                        let fresh = TeamProfile {
                                            team: *team,
                                            organization: *organization,
                                            slug: *slug,
                                            name: name.clone(),
                                            members: Vec::new(),
                                            created_at: block_timestamp,
                                            updated_at: block_timestamp,
                                        };
                                        (fresh, None)
                                    },
                                };

                            match save_team_profile(state_layer, *organization, *team, &profile) {
                                Ok(()) => {
                                    if let Some(old) = old_name
                                        && !old.is_empty()
                                    {
                                        state.team_name_index.remove(&(*organization, old));
                                    }
                                    state
                                        .team_name_index
                                        .insert((*organization, name.clone()), *team);

                                    LedgerResponse::OrganizationUpdated {
                                        organization_id: *organization,
                                    }
                                },
                                Err(e) => e,
                            }
                        } else {
                            ledger_error(
                                ErrorCode::Internal,
                                "State layer unavailable for team profile write",
                            )
                        }
                    },

                    // ── WriteAppProfile (REGIONAL) ──
                    // Upsert: creates AppProfile from scratch on first call (REGIONAL
                    // state has no data from GLOBAL CreateApp), or updates
                    // name/description on subsequent calls.
                    SystemRequest::WriteAppProfile { organization, app, name, description } => {
                        if let Some(state_layer) = &self.state_layer {
                            // Name conflict check (uses REGIONAL app_name_index)
                            if has_app_name_conflict(state, *organization, name, Some(*app)) {
                                return error_result(
                                    ErrorCode::AlreadyExists,
                                    format!(
                                        "App name '{}' already exists in organization {}",
                                        name, organization
                                    ),
                                );
                            }

                            // Upsert: try load existing, create from scratch if not found
                            let (profile, old_name) =
                                match load_app_profile(state_layer, *organization, *app) {
                                    Ok(mut existing) => {
                                        let old = existing.name.clone();
                                        existing.name.clone_from(name);
                                        existing.description.clone_from(description);
                                        existing.updated_at = block_timestamp;
                                        (existing, Some(old))
                                    },
                                    Err(_) => {
                                        let fresh = AppProfile {
                                            app: *app,
                                            organization: *organization,
                                            name: name.clone(),
                                            description: description.clone(),
                                            updated_at: block_timestamp,
                                        };
                                        (fresh, None)
                                    },
                                };

                            // Batch profile write + name index ops atomically
                            let profile_key = SystemKeys::app_profile_key(*organization, *app);
                            let encoded = match encode(&profile) {
                                Ok(bytes) => bytes,
                                Err(e) => {
                                    return error_result(
                                        ErrorCode::Internal,
                                        format!("Failed to encode app profile: {e}"),
                                    );
                                },
                            };
                            let ops = vec![Operation::SetEntity {
                                key: profile_key,
                                value: encoded,
                                condition: None,
                                expires_at: None,
                            }];

                            if let Err(e) = state_layer.apply_operations(SYSTEM_VAULT_ID, &ops, 0) {
                                return error_result(
                                    ErrorCode::Internal,
                                    format!("Failed to write app profile: {e}"),
                                );
                            }

                            // Update in-memory indices after storage succeeds
                            if let Some(old) = old_name
                                && !old.is_empty()
                            {
                                state.app_name_index.remove(&(*organization, old));
                            }
                            state.app_name_index.insert((*organization, name.clone()), *app);

                            LedgerResponse::OrganizationUpdated { organization_id: *organization }
                        } else {
                            ledger_error(
                                ErrorCode::Internal,
                                "State layer unavailable for app profile write",
                            )
                        }
                    },

                    // ── DeleteTeamProfile (REGIONAL) ──
                    // Deletes the team profile and name index from REGIONAL state.
                    // Handles member migration if move_members_to is specified.
                    SystemRequest::DeleteTeamProfile { organization, team, move_members_to } => {
                        if let Some(state_layer) = &self.state_layer {
                            match load_team_profile(state_layer, *organization, *team) {
                                Ok(team_profile) => {
                                    // Move members if requested
                                    if let Some(target_team) = move_members_to {
                                        if *target_team == *team {
                                            return error_result(
                                                ErrorCode::FailedPrecondition,
                                                "Cannot move members to the same team being deleted",
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

                                    // Delete profile from REGIONAL state
                                    let profile_key =
                                        SystemKeys::team_profile_key(*organization, *team);
                                    let ops = vec![Operation::DeleteEntity { key: profile_key }];
                                    if let Err(e) =
                                        state_layer.apply_operations(SYSTEM_VAULT_ID, &ops, 0)
                                    {
                                        return error_result(
                                            ErrorCode::Internal,
                                            format!("Failed to delete team profile: {e}"),
                                        );
                                    }

                                    // Clean up REGIONAL name index
                                    state
                                        .team_name_index
                                        .remove(&(*organization, team_profile.name));

                                    LedgerResponse::OrganizationTeamDeleted {
                                        organization_id: *organization,
                                    }
                                },
                                Err(_) => {
                                    // Profile doesn't exist in REGIONAL state — not an error,
                                    // proceed with GLOBAL cleanup
                                    LedgerResponse::OrganizationTeamDeleted {
                                        organization_id: *organization,
                                    }
                                },
                            }
                        } else {
                            ledger_error(
                                ErrorCode::Internal,
                                "State layer unavailable for team profile delete",
                            )
                        }
                    },

                    // ── AddTeamMember (REGIONAL) ──
                    // Adds a member to a team's profile.
                    SystemRequest::AddTeamMember { organization, team, user_id, role } => {
                        if let Some(state_layer) = &self.state_layer {
                            match load_team_profile(state_layer, *organization, *team) {
                                Ok(mut profile) => {
                                    if profile.members.iter().any(|m| m.user_id == *user_id) {
                                        ledger_error(
                                            ErrorCode::AlreadyExists,
                                            format!(
                                                "User {} is already a member of team {}",
                                                user_id, team
                                            ),
                                        )
                                    } else {
                                        profile.members.push(TeamMember {
                                            user_id: *user_id,
                                            role: *role,
                                            joined_at: block_timestamp,
                                        });
                                        profile.updated_at = block_timestamp;
                                        match save_team_profile(
                                            state_layer,
                                            *organization,
                                            *team,
                                            &profile,
                                        ) {
                                            Ok(()) => LedgerResponse::OrganizationUpdated {
                                                organization_id: *organization,
                                            },
                                            Err(e) => e,
                                        }
                                    }
                                },
                                Err(e) => e,
                            }
                        } else {
                            ledger_error(
                                ErrorCode::Internal,
                                "State layer unavailable for add team member",
                            )
                        }
                    },

                    // ── RemoveTeamMember (REGIONAL) ──
                    // Removes a member from a team's profile.
                    SystemRequest::RemoveTeamMember { organization, team, user_id } => {
                        if let Some(state_layer) = &self.state_layer {
                            match load_team_profile(state_layer, *organization, *team) {
                                Ok(mut profile) => {
                                    use inferadb_ledger_state::system::TeamMemberRole;

                                    let original_len = profile.members.len();
                                    let is_last_manager = profile.members.iter().any(|m| {
                                        m.user_id == *user_id && m.role == TeamMemberRole::Manager
                                    }) && profile
                                        .members
                                        .iter()
                                        .filter(|m| m.role == TeamMemberRole::Manager)
                                        .count()
                                        == 1;
                                    if is_last_manager {
                                        ledger_error(
                                            ErrorCode::FailedPrecondition,
                                            format!(
                                                "Cannot remove the last manager from team {}",
                                                team
                                            ),
                                        )
                                    } else {
                                        profile.members.retain(|m| m.user_id != *user_id);
                                        if profile.members.len() == original_len {
                                            ledger_error(
                                                ErrorCode::NotFound,
                                                format!(
                                                    "User {} is not a member of team {}",
                                                    user_id, team
                                                ),
                                            )
                                        } else {
                                            profile.updated_at = block_timestamp;
                                            match save_team_profile(
                                                state_layer,
                                                *organization,
                                                *team,
                                                &profile,
                                            ) {
                                                Ok(()) => LedgerResponse::OrganizationUpdated {
                                                    organization_id: *organization,
                                                },
                                                Err(e) => e,
                                            }
                                        }
                                    }
                                },
                                Err(e) => e,
                            }
                        } else {
                            ledger_error(
                                ErrorCode::Internal,
                                "State layer unavailable for remove team member",
                            )
                        }
                    },

                    // ── WriteClientAssertionName (REGIONAL) ──
                    // Writes the user-provided assertion name to REGIONAL state.
                    SystemRequest::WriteClientAssertionName {
                        organization,
                        app,
                        assertion,
                        name,
                    } => {
                        if let Some(state_layer) = &self.state_layer {
                            let key =
                                SystemKeys::assertion_name_key(*organization, *app, *assertion);
                            let ops = vec![Operation::SetEntity {
                                key,
                                value: name.as_bytes().to_vec(),
                                condition: None,
                                expires_at: None,
                            }];
                            if let Err(e) = state_layer.apply_operations(SYSTEM_VAULT_ID, &ops, 0) {
                                return error_result(
                                    ErrorCode::Internal,
                                    format!("Failed to write assertion name: {e}"),
                                );
                            }
                            LedgerResponse::Empty
                        } else {
                            ledger_error(
                                ErrorCode::Internal,
                                "State layer unavailable for assertion name write",
                            )
                        }
                    },

                    // ── DeleteClientAssertionName (REGIONAL) ──
                    // Deletes an assertion's name from REGIONAL state.
                    SystemRequest::DeleteClientAssertionName { organization, app, assertion } => {
                        if let Some(state_layer) = &self.state_layer {
                            let key =
                                SystemKeys::assertion_name_key(*organization, *app, *assertion);
                            let ops = vec![Operation::DeleteEntity { key }];
                            if let Err(e) = state_layer.apply_operations(SYSTEM_VAULT_ID, &ops, 0) {
                                return error_result(
                                    ErrorCode::Internal,
                                    format!("Failed to delete assertion name: {e}"),
                                );
                            }
                            LedgerResponse::Empty
                        } else {
                            ledger_error(
                                ErrorCode::Internal,
                                "State layer unavailable for assertion name delete",
                            )
                        }
                    },

                    // ── DeleteAppProfile (REGIONAL) ──
                    // Deletes the app profile, name index, and assertion names
                    // from REGIONAL state.
                    SystemRequest::DeleteAppProfile { organization, app } => {
                        if let Some(state_layer) = &self.state_layer {
                            // Read profile name for index cleanup before deleting
                            let profile_name = load_app_profile(state_layer, *organization, *app)
                                .ok()
                                .map(|p| p.name)
                                .filter(|n| !n.is_empty());

                            let mut ops = Vec::new();

                            // Delete app profile
                            let profile_key = SystemKeys::app_profile_key(*organization, *app);
                            ops.push(Operation::DeleteEntity { key: profile_key });

                            // Delete all assertion names for this app
                            let assertion_name_prefix =
                                SystemKeys::assertion_name_prefix(*organization, *app);
                            collect_all_entities_for_deletion(
                                state_layer,
                                &assertion_name_prefix,
                                &mut ops,
                            );

                            if let Err(e) = state_layer.apply_operations(SYSTEM_VAULT_ID, &ops, 0) {
                                return error_result(
                                    ErrorCode::Internal,
                                    format!("Failed to delete app profile: {e}"),
                                );
                            }

                            // Update in-memory index only after storage succeeds
                            if let Some(name) = profile_name {
                                state.app_name_index.remove(&(*organization, name));
                            }

                            LedgerResponse::AppDeleted { organization_id: *organization }
                        } else {
                            ledger_error(
                                ErrorCode::Internal,
                                "State layer unavailable for app profile delete",
                            )
                        }
                    },

                    // ── PurgeOrganizationRegional (REGIONAL) ──
                    // Deletes all team profiles, app profiles, assertion names,
                    // and name indices belonging to this organization from the
                    // REGIONAL state layer.
                    SystemRequest::PurgeOrganizationRegional { organization } => {
                        if let Some(state_layer) = &self.state_layer {
                            let mut ops = Vec::new();

                            // Delete all team profiles
                            let team_prefix = format!(
                                "{}{}:",
                                SystemKeys::TEAM_PROFILE_PREFIX,
                                organization.value()
                            );
                            collect_all_entities_for_deletion(state_layer, &team_prefix, &mut ops);

                            // Delete all app profiles
                            let app_profile_prefix = format!(
                                "{}{}:",
                                SystemKeys::APP_PROFILE_PREFIX,
                                organization.value()
                            );
                            collect_all_entities_for_deletion(
                                state_layer,
                                &app_profile_prefix,
                                &mut ops,
                            );

                            // Delete all assertion names
                            let assertion_name_prefix =
                                SystemKeys::assertion_name_org_prefix(*organization);
                            collect_all_entities_for_deletion(
                                state_layer,
                                &assertion_name_prefix,
                                &mut ops,
                            );

                            // Delete the organization profile (contains plaintext name)
                            let profile_key = SystemKeys::organization_profile_key(*organization);
                            ops.push(Operation::DeleteEntity { key: profile_key });

                            // Destroy the OrgKey — crypto-shredding: all historical
                            // EncryptedOrgSystem entries for this organization become
                            // permanently unrecoverable.
                            let org_key_key = SystemKeys::org_key(*organization);
                            ops.push(Operation::DeleteEntity { key: org_key_key });

                            if !ops.is_empty()
                                && let Err(e) =
                                    state_layer.apply_operations(SYSTEM_VAULT_ID, &ops, 0)
                            {
                                tracing::error!(
                                    organization_id = organization.value(),
                                    error = %e,
                                    "Failed to purge REGIONAL organization data"
                                );
                            }

                            // Clean up REGIONAL in-memory indices
                            state.team_name_index.retain(|(org_id, _), _| *org_id != *organization);
                            state.app_name_index.retain(|(org_id, _), _| *org_id != *organization);

                            LedgerResponse::OrganizationPurged { organization_id: *organization }
                        } else {
                            ledger_error(
                                ErrorCode::Internal,
                                "State layer unavailable for regional purge",
                            )
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
                        LedgerResponse::UserProfileUpdated { user_id },
                        SystemRequest::UpdateUserProfile { .. },
                    ) => {
                        events.push(
                            ApplyPhaseEmitter::for_system(EventAction::UserUpdated)
                                .principal("system")
                                .detail("user_id", &user_id.to_string())
                                .detail("scope", "profile")
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
                        SystemRequest::EraseUser { region, .. },
                    ) => {
                        events.push(
                            ApplyPhaseEmitter::for_system(EventAction::UserErased)
                                .principal("system")
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
                        SystemRequest::CreateOrganizationDirectory { region, .. },
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
                    return error_result(ErrorCode::Internal, "State layer not available");
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
                            ErrorCode::Internal,
                            format!("Failed to check existing signing key: {e}"),
                        );
                    },
                }

                // Verify no other active key exists for this scope (use RotateSigningKey instead).
                match sys.get_active_signing_key(scope) {
                    Ok(Some(active)) => {
                        return error_result(
                            ErrorCode::FailedPrecondition,
                            format!(
                                "Active signing key already exists for scope: kid={}. Use RotateSigningKey.",
                                active.kid
                            ),
                        );
                    },
                    Ok(None) => {},
                    Err(e) => {
                        return error_result(
                            ErrorCode::Internal,
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
                        ErrorCode::Internal,
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
                    return error_result(ErrorCode::Internal, "State layer not available");
                };
                let sys = SystemOrganizationService::new(state_layer.clone());

                // Look up old key; verify it's Active.
                let old_key = match require_signing_key(&sys, old_kid) {
                    Ok(k) => k,
                    Err(resp) => return resp,
                };

                if old_key.status != SigningKeyStatus::Active {
                    return error_result(
                        ErrorCode::FailedPrecondition,
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
                            ErrorCode::Internal,
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
                            ErrorCode::Internal,
                            format!("Failed to rotate old signing key: {e}"),
                        );
                    }
                }

                // Create new key as Active.
                // ORDERING: old key status MUST be updated before new key is stored.
                // `store_signing_key` overwrites the scope index to point at the new kid.
                // If reversed, `update_signing_key_status` (Revoked + previously Active)
                // would delete the scope index, leaving no active key for this scope.
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
                        ErrorCode::Internal,
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
                    return error_result(ErrorCode::Internal, "State layer not available");
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
                        ErrorCode::Internal,
                        format!("Failed to revoke signing key: {e}"),
                    );
                }

                (LedgerResponse::SigningKeyRevoked { kid: kid.clone() }, None)
            },

            LedgerRequest::TransitionSigningKeyRevoked { kid } => {
                let Some(state_layer) = &self.state_layer else {
                    return error_result(ErrorCode::Internal, "State layer not available");
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
                            ErrorCode::Internal,
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
                        ErrorCode::Internal,
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
                    return error_result(ErrorCode::Internal, "State layer not available");
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
                        ErrorCode::Internal,
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
                    return error_result(ErrorCode::Internal, "State layer not available");
                };
                let sys = SystemOrganizationService::new(state_layer.clone());

                // Look up old token by hash.
                let old_token = match sys.get_refresh_token_by_hash(old_token_hash) {
                    Ok(Some(t)) => t,
                    Ok(None) => {
                        return error_result(ErrorCode::Unauthenticated, "Refresh token not found");
                    },
                    Err(e) => {
                        return error_result(
                            ErrorCode::Internal,
                            format!("Failed to look up refresh token: {e}"),
                        );
                    },
                };

                // Check if family is poisoned (prior reuse detected).
                match sys.is_family_poisoned(&old_token.family) {
                    Ok(true) => {
                        return error_result(
                            ErrorCode::Unauthenticated,
                            "Refresh token reuse detected: family revoked",
                        );
                    },
                    Ok(false) => {},
                    Err(e) => {
                        return error_result(
                            ErrorCode::Internal,
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
                        ErrorCode::Unauthenticated,
                        "Refresh token reuse detected: family revoked",
                    );
                }

                // Check expiry.
                if old_token.expires_at <= block_timestamp {
                    return error_result(ErrorCode::Unauthenticated, "Refresh token expired");
                }

                // Check revocation.
                if old_token.revoked_at.is_some() {
                    return error_result(ErrorCode::Unauthenticated, "Refresh token revoked");
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
                                ErrorCode::NotFound,
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
                                ErrorCode::NotFound,
                                "User not found for version check",
                            );
                        },
                        Err(e) => {
                            return error_result(
                                ErrorCode::Internal,
                                format!("Failed to read user: {e}"),
                            );
                        },
                    };
                    let user = match decode::<User>(&entity.value) {
                        Ok(user) => user,
                        Err(e) => {
                            return error_result(
                                ErrorCode::Internal,
                                format!("Failed to decode user: {e}"),
                            );
                        },
                    };
                    if user.version != *expected {
                        return error_result(
                            ErrorCode::Unauthenticated,
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
                            return error_result(ErrorCode::NotFound, "App not found");
                        },
                    };

                    // Verify app is enabled.
                    let app = match load_app(state_layer, org_id, app_id) {
                        Ok(app) => app,
                        Err(resp) => return (resp, None),
                    };
                    if !app.enabled {
                        return error_result(ErrorCode::FailedPrecondition, "App is disabled");
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
                                    ErrorCode::FailedPrecondition,
                                    "Vault connection removed since token was issued",
                                );
                            },
                            Err(e) => {
                                return error_result(
                                    ErrorCode::Internal,
                                    format!("Failed to read vault connection: {e}"),
                                );
                            },
                        };
                        let connection = match decode::<AppVaultConnection>(&entity.value) {
                            Ok(c) => c,
                            Err(e) => {
                                return error_result(
                                    ErrorCode::Internal,
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
                        ErrorCode::Internal,
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
                        ErrorCode::Internal,
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
                    return error_result(ErrorCode::Internal, "State layer not available");
                };
                let sys = SystemOrganizationService::new(state_layer.clone());

                match sys.revoke_token_family(family, block_timestamp) {
                    Ok(result) => {
                        (LedgerResponse::TokenFamilyRevoked { count: result.revoked_count }, None)
                    },
                    Err(e) => error_result(
                        ErrorCode::Internal,
                        format!("Failed to revoke token family: {e}"),
                    ),
                }
            },

            LedgerRequest::RevokeAllUserSessions { user } => {
                let Some(state_layer) = &self.state_layer else {
                    return error_result(ErrorCode::Internal, "State layer not available");
                };
                let sys = SystemOrganizationService::new(state_layer.clone());

                // Resolve UserId → UserSlug from in-memory state.
                let user_slug = match state.user_id_to_slug.get(user) {
                    Some(slug) => *slug,
                    None => {
                        return error_result(
                            ErrorCode::NotFound,
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
                        ErrorCode::Internal,
                        format!("Failed to revoke all user sessions: {e}"),
                    ),
                }
            },

            LedgerRequest::RevokeAllAppSessions { organization, app } => {
                let Some(state_layer) = &self.state_layer else {
                    return error_result(ErrorCode::Internal, "State layer not available");
                };
                let sys = SystemOrganizationService::new(state_layer.clone());

                let app_slug = match state.app_id_to_slug.get(app) {
                    Some(slug) => *slug,
                    None => {
                        return error_result(
                            ErrorCode::NotFound,
                            format!("App slug not found for app {app}"),
                        );
                    },
                };

                match sys.revoke_all_app_sessions(*organization, *app, app_slug, block_timestamp) {
                    Ok(result) => (
                        LedgerResponse::AllAppSessionsRevoked {
                            count: result.revoked_count,
                            version: result.new_version,
                        },
                        None,
                    ),
                    Err(e) => error_result(
                        ErrorCode::Internal,
                        format!("Failed to revoke all app sessions: {e}"),
                    ),
                }
            },

            LedgerRequest::DeleteExpiredRefreshTokens => {
                let Some(state_layer) = &self.state_layer else {
                    return error_result(ErrorCode::Internal, "State layer not available");
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
                        ErrorCode::Internal,
                        format!("Failed to delete expired refresh tokens: {e}"),
                    ),
                }
            },

            LedgerRequest::EncryptedUserSystem(encrypted) => {
                // Decrypt the SystemRequest using the user's SubjectKey.
                // If the key has been destroyed (user erased), skip the entry —
                // the state machine already reflects the erasure.
                let state_layer = match &self.state_layer {
                    Some(sl) => sl,
                    None => {
                        tracing::warn!(
                            user_id = encrypted.user_id.value(),
                            "EncryptedUserSystem: no state layer available, skipping"
                        );
                        return (LedgerResponse::Empty, None);
                    },
                };

                let key_str = SystemKeys::subject_key(encrypted.user_id);
                let subject_key_entity = match state_layer
                    .get_entity(SYSTEM_VAULT_ID, key_str.as_bytes())
                {
                    Ok(Some(entity)) => entity,
                    Ok(None) => {
                        // SubjectKey destroyed — user has been erased.
                        // Crypto-shredding: this entry is permanently unrecoverable.
                        tracing::info!(
                            user_id = encrypted.user_id.value(),
                            "EncryptedUserSystem: SubjectKey destroyed (user erased), skipping entry"
                        );
                        return (LedgerResponse::Empty, None);
                    },
                    Err(e) => {
                        tracing::error!(
                            user_id = encrypted.user_id.value(),
                            error = %e,
                            "EncryptedUserSystem: failed to read SubjectKey"
                        );
                        return (
                            LedgerResponse::Error {
                                code: ErrorCode::Internal,
                                message: format!("Failed to read SubjectKey: {e}"),
                            },
                            None,
                        );
                    },
                };

                let subject_key: inferadb_ledger_state::system::SubjectKey =
                    match decode(&subject_key_entity.value) {
                        Ok(sk) => sk,
                        Err(e) => {
                            tracing::error!(
                                user_id = encrypted.user_id.value(),
                                error = %e,
                                "EncryptedUserSystem: failed to decode SubjectKey"
                            );
                            return (
                                LedgerResponse::Error {
                                    code: ErrorCode::Internal,
                                    message: format!("Failed to decode SubjectKey: {e}"),
                                },
                                None,
                            );
                        },
                    };

                let sys_request = match crate::entry_crypto::decrypt_user_system_request(
                    encrypted,
                    &subject_key.key,
                ) {
                    Ok(req) => req,
                    Err(e) => {
                        tracing::error!(
                            user_id = encrypted.user_id.value(),
                            error = %e,
                            "EncryptedUserSystem: decryption failed"
                        );
                        return (
                            LedgerResponse::Error {
                                code: ErrorCode::Internal,
                                message: format!("Raft entry decryption failed: {e}"),
                            },
                            None,
                        );
                    },
                };

                // Apply the decrypted SystemRequest through the normal path
                let decrypted_request = LedgerRequest::System(sys_request);
                self.apply_request_with_events(
                    &decrypted_request,
                    state,
                    block_timestamp,
                    op_index,
                    events,
                    ttl_days,
                    pending,
                )
            },

            LedgerRequest::EncryptedOrgSystem(encrypted) => {
                // Decrypt the SystemRequest using the organization's OrgKey.
                // If the key has been destroyed (org purged), skip the entry —
                // the state machine already reflects the purge.
                let state_layer = match &self.state_layer {
                    Some(sl) => sl,
                    None => {
                        tracing::warn!(
                            organization_id = encrypted.organization.value(),
                            "EncryptedOrgSystem: no state layer available, skipping"
                        );
                        return (LedgerResponse::Empty, None);
                    },
                };

                let key_str = SystemKeys::org_key(encrypted.organization);
                let org_key_entity =
                    match state_layer.get_entity(SYSTEM_VAULT_ID, key_str.as_bytes()) {
                        Ok(Some(entity)) => entity,
                        Ok(None) => {
                            // OrgKey destroyed — organization has been purged.
                            // Crypto-shredding: this entry is permanently unrecoverable.
                            tracing::info!(
                                organization_id = encrypted.organization.value(),
                                "EncryptedOrgSystem: OrgKey destroyed (org purged), skipping entry"
                            );
                            return (LedgerResponse::Empty, None);
                        },
                        Err(e) => {
                            tracing::error!(
                                organization_id = encrypted.organization.value(),
                                error = %e,
                                "EncryptedOrgSystem: failed to read OrgKey"
                            );
                            return (
                                LedgerResponse::Error {
                                    code: ErrorCode::Internal,
                                    message: format!("Failed to read OrgKey: {e}"),
                                },
                                None,
                            );
                        },
                    };

                let org_key: inferadb_ledger_state::system::OrgKey =
                    match decode(&org_key_entity.value) {
                        Ok(ok) => ok,
                        Err(e) => {
                            tracing::error!(
                                organization_id = encrypted.organization.value(),
                                error = %e,
                                "EncryptedOrgSystem: failed to decode OrgKey"
                            );
                            return (
                                LedgerResponse::Error {
                                    code: ErrorCode::Internal,
                                    message: format!("Failed to decode OrgKey: {e}"),
                                },
                                None,
                            );
                        },
                    };

                let sys_request = match crate::entry_crypto::decrypt_org_system_request(
                    encrypted,
                    &org_key.key,
                ) {
                    Ok(req) => req,
                    Err(e) => {
                        tracing::error!(
                            organization_id = encrypted.organization.value(),
                            error = %e,
                            "EncryptedOrgSystem: decryption failed"
                        );
                        return (
                            LedgerResponse::Error {
                                code: ErrorCode::Internal,
                                message: format!("Raft entry decryption failed: {e}"),
                            },
                            None,
                        );
                    },
                };

                // Apply the decrypted SystemRequest through the normal path
                let decrypted_request = LedgerRequest::System(sys_request);
                self.apply_request_with_events(
                    &decrypted_request,
                    state,
                    block_timestamp,
                    op_index,
                    events,
                    ttl_days,
                    pending,
                )
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
}
