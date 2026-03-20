//! State machine apply logic for Raft log entries.
//!
//! Transforms committed log entries into state mutations via the storage engine.

mod app_helpers;
mod helpers;
mod org_helpers;
mod team_helpers;
mod token_helpers;

use chrono::{DateTime, Duration, Utc};
use inferadb_ledger_state::{
    StateError,
    system::{
        App, AppCredentialType, AppCredentials, AppProfile, AppVaultConnection,
        ClientAssertionEntry, EmailHashEntry, KeyTier, OnboardingAccount, OrganizationMember,
        OrganizationMemberRole, OrganizationProfile, OrganizationRegistry, OrganizationStatus,
        OrganizationTier, PendingEmailVerification, ProvisioningReservation, RefreshToken,
        SYSTEM_VAULT_ID, SigningKey, SigningKeyStatus, SystemError, SystemKeys,
        SystemOrganizationService, Team, TeamMember, User, UserDirectoryEntry, UserDirectoryStatus,
        UserEmail,
    },
};
use inferadb_ledger_store::StorageBackend;
use inferadb_ledger_types::{
    AppId, AppSlug, ErrorCode, Hash, InvitationStatus, InviteEmailEntry, InviteIndexEntry, NodeId,
    Operation, OrganizationId, PendingTotpChallenge, PrimaryAuthMethod, TeamId, TeamSlug,
    TokenSubject, TokenType, TokenVersion, UserRole, UserStatus, VaultEntry, VaultId,
    compute_tx_merkle_root, decode, encode,
    events::{EventAction, EventEntry, EventOutcome},
    hash_eq,
};

use self::{
    app_helpers::{has_app_name_conflict, load_app, load_app_profile, save_app},
    helpers::{
        cascade_revoke, collect_all_entities_for_deletion, error_result, ledger_error,
        saturating_duration_secs, try_encode,
    },
    org_helpers::{
        load_organization, require_active_org_with_state, require_fully_active_org,
        save_org_profile, save_organization,
    },
    team_helpers::{has_team_name_conflict, load_team, migrate_team_members, save_team},
    token_helpers::require_signing_key,
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
            None,
            false,
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
    ///
    /// `log_id_bytes`, when `Some`, causes the serialized Raft log ID to be
    /// persisted as an atomicity sentinel in the state layer's write
    /// transaction (crash-recovery protocol).
    ///
    /// `skip_state_writes`, when `true`, skips state layer entity writes
    /// for crash-recovery of already-applied entries. In-memory state
    /// updates still execute so subsequent entries see correct state.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn apply_request_with_events(
        &self,
        request: &LedgerRequest,
        state: &mut AppliedState,
        block_timestamp: DateTime<Utc>,
        op_index: &mut u32,
        events: &mut Vec<EventEntry>,
        ttl_days: u32,
        pending: &mut PendingExternalWrites,
        log_id_bytes: Option<&[u8]>,
        skip_state_writes: bool,
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
                    if skip_state_writes {
                        // Crash recovery: entity data already committed to state
                        // layer DB. Force full state root recomputation from
                        // existing data instead of re-applying operations (which
                        // would fail CAS conditions).
                        state_layer.mark_all_dirty(*vault);
                    } else {
                        // Normal path: apply all transactions' operations in a
                        // single storage transaction for atomicity.
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
                                    // On CAS failure, return current state for
                                    // conflict resolution. Write txn is dropped
                                    // (rolled back) automatically.
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
                                                message: format!(
                                                    "Failed to apply operations: {other}"
                                                ),
                                            },
                                            None,
                                        ),
                                    };
                                },
                            }
                        }
                        // Persist atomicity sentinel in the same transaction as
                        // entity data so crash recovery can detect
                        // already-applied entries.
                        if let Some(lid_bytes) = log_id_bytes
                            && let Err(e) = inferadb_ledger_state::StateLayer::persist_last_applied(
                                &mut write_txn,
                                lid_bytes,
                            )
                        {
                            return (
                                LedgerResponse::Error {
                                    code: ErrorCode::Internal,
                                    message: format!(
                                        "Failed to persist last_applied sentinel: {e}"
                                    ),
                                },
                                None,
                            );
                        }
                        // Mark dirty before commit: dirty marks are conservative
                        // (trigger re-hash from storage). Safe on commit failure.
                        state_layer.mark_dirty_keys(*vault, &all_dirty_keys);
                        if let Err(e) = write_txn.commit() {
                            return (
                                LedgerResponse::Error {
                                    code: ErrorCode::Internal,
                                    message: format!("Failed to commit write txn: {e}"),
                                },
                                None,
                            );
                        }
                    }

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
                            let reg_key = SystemKeys::organization_registry_key(*organization);
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
                    Ok(state_layer) => match load_organization(state_layer, *organization) {
                        Ok(mut org) => {
                            let Some(pos) = org.members.iter().position(|m| m.user_id == *target)
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
                            let is_last_admin = org.members[pos].role
                                == OrganizationMemberRole::Admin
                                && org
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
                            org.members.remove(pos);
                            org.updated_at = block_timestamp;
                            if let Err(e) = save_organization(state_layer, *organization, &org) {
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
                    Ok(state_layer) => match load_organization(state_layer, *organization) {
                        Ok(mut org) => {
                            let Some(pos) = org.members.iter().position(|m| m.user_id == *target)
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
                            if org.members[pos].role == OrganizationMemberRole::Admin
                                && *role == OrganizationMemberRole::Member
                                && org
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
                            org.members[pos].role = *role;
                            org.updated_at = block_timestamp;
                            if let Err(e) = save_organization(state_layer, *organization, &org) {
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

            LedgerRequest::AddOrganizationMember { organization, user, user_slug: _, role } => {
                let response = match require_active_org_with_state(
                    organization,
                    state,
                    &self.state_layer,
                    "modify",
                ) {
                    Ok(state_layer) => match load_organization(state_layer, *organization) {
                        Ok(mut org) => {
                            // Idempotent: if already a member, return success with
                            // already_member=true
                            if org.members.iter().any(|m| m.user_id == *user) {
                                LedgerResponse::OrganizationMemberAdded {
                                    organization_id: *organization,
                                    already_member: true,
                                }
                            } else {
                                org.members.push(
                                    inferadb_ledger_state::system::OrganizationMember {
                                        user_id: *user,
                                        role: *role,
                                        joined_at: block_timestamp,
                                    },
                                );
                                org.updated_at = block_timestamp;
                                if let Err(e) = save_organization(state_layer, *organization, &org)
                                {
                                    return (e, None);
                                }
                                // Update user→org index
                                state
                                    .user_org_index
                                    .entry(*user)
                                    .or_default()
                                    .insert(*organization);
                                // Re-serialize org meta
                                if let Some(org_mut) = state.organizations.get_mut(organization)
                                    && let Ok(blob) = encode(org_mut)
                                {
                                    pending.organizations.push((*organization, blob));
                                }
                                LedgerResponse::OrganizationMemberAdded {
                                    organization_id: *organization,
                                    already_member: false,
                                }
                            }
                        },
                        Err(e) => e,
                    },
                    Err(err_response) => err_response,
                };

                if matches!(
                    response,
                    LedgerResponse::OrganizationMemberAdded { already_member: false, .. }
                ) {
                    events.push(
                        ApplyPhaseEmitter::for_system(EventAction::OrganizationMemberAdded)
                            .detail("organization_id", &organization.to_string())
                            .detail("user_id", &user.to_string())
                            .detail("role", &format!("{role:?}"))
                            .outcome(EventOutcome::Success)
                            .build(block_height, *op_index, block_timestamp, ttl_days),
                    );
                    *op_index += 1;
                }

                (response, None)
            },

            // ── CreateOrganizationInvite (GLOBAL) ──
            // Allocates InviteId, computes expires_at, writes 3 GLOBAL indexes.
            LedgerRequest::CreateOrganizationInvite {
                organization,
                slug,
                token_hash,
                invitee_email_hmac,
                ttl_hours,
            } => {
                let response = match require_active_org_with_state(
                    organization,
                    state,
                    &self.state_layer,
                    "modify",
                ) {
                    Ok(state_layer) => {
                        let invite_id = state.sequences.next_invite();
                        let expires_at = block_timestamp + Duration::hours(i64::from(*ttl_hours));

                        // Write slug index: _idx:invite:slug:{slug} → InviteIndexEntry
                        let slug_key = SystemKeys::invite_slug_index_key(*slug);
                        let slug_entry =
                            InviteIndexEntry { organization: *organization, invite: invite_id };
                        let slug_value = match encode(&slug_entry) {
                            Ok(v) => v,
                            Err(e) => {
                                return error_result(
                                    ErrorCode::Internal,
                                    format!("Failed to encode invite slug index: {e}"),
                                );
                            },
                        };

                        // Write token hash index: _idx:invite:token_hash:{hex} →
                        // InviteIndexEntry
                        let token_hex: String = inferadb_ledger_types::bytes_to_hex(token_hash);
                        let token_key = SystemKeys::invite_token_hash_index_key(&token_hex);
                        let token_value = match encode(&slug_entry) {
                            Ok(v) => v,
                            Err(e) => {
                                return error_result(
                                    ErrorCode::Internal,
                                    format!("Failed to encode invite token hash index: {e}"),
                                );
                            },
                        };

                        // Write email hash index:
                        // _idx:invite:email_hash:{hmac}:{invite_id} → InviteEmailEntry
                        let email_key =
                            SystemKeys::invite_email_hash_index_key(invitee_email_hmac, invite_id);
                        let email_entry = InviteEmailEntry {
                            organization: *organization,
                            status: InvitationStatus::Pending,
                        };
                        let email_value = match encode(&email_entry) {
                            Ok(v) => v,
                            Err(e) => {
                                return error_result(
                                    ErrorCode::Internal,
                                    format!("Failed to encode invite email hash index: {e}"),
                                );
                            },
                        };

                        let ops = vec![
                            Operation::SetEntity {
                                key: slug_key,
                                value: slug_value,
                                condition: None,
                                expires_at: None,
                            },
                            Operation::SetEntity {
                                key: token_key,
                                value: token_value,
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
                        if let Err(e) = state_layer.apply_operations(SYSTEM_VAULT_ID, &ops, 0) {
                            return error_result(
                                ErrorCode::Internal,
                                format!("Failed to write invite indexes: {e}"),
                            );
                        }

                        LedgerResponse::OrganizationInviteCreated {
                            invite_id,
                            invite_slug: *slug,
                            expires_at,
                        }
                    },
                    Err(err_response) => err_response,
                };

                if matches!(response, LedgerResponse::OrganizationInviteCreated { .. }) {
                    events.push(
                        ApplyPhaseEmitter::for_system(EventAction::InvitationCreated)
                            .detail("organization_id", &organization.to_string())
                            .detail("invite_slug", &slug.to_string())
                            .outcome(EventOutcome::Success)
                            .build(block_height, *op_index, block_timestamp, ttl_days),
                    );
                    *op_index += 1;
                }

                (response, None)
            },

            // ── ResolveOrganizationInvite (GLOBAL) ──
            // CAS: Pending-only. Updates email hash index status, removes token hash index.
            LedgerRequest::ResolveOrganizationInvite {
                invite,
                organization,
                status,
                invitee_email_hmac,
                token_hash,
            } => {
                // Validate target status is terminal
                if !status.is_terminal() {
                    return error_result(
                        ErrorCode::InvalidArgument,
                        format!("Target status must be terminal, got {status}"),
                    );
                }

                let response = match require_active_org_with_state(
                    organization,
                    state,
                    &self.state_layer,
                    "modify",
                ) {
                    Ok(state_layer) => {
                        // Construct exact email hash index key using provided HMAC
                        let email_key =
                            SystemKeys::invite_email_hash_index_key(invitee_email_hmac, *invite);

                        // Read current email hash entry for CAS check
                        let current_entry = match state_layer
                            .get_entity(SYSTEM_VAULT_ID, email_key.as_bytes())
                        {
                            Ok(Some(entity)) => match decode::<InviteEmailEntry>(&entity.value) {
                                Ok(entry) => entry,
                                Err(e) => {
                                    return error_result(
                                        ErrorCode::Internal,
                                        format!("Failed to decode email hash entry: {e}"),
                                    );
                                },
                            },
                            Ok(None) => {
                                return error_result(
                                    ErrorCode::NotFound,
                                    format!("No email hash index found for invite {}", invite),
                                );
                            },
                            Err(e) => {
                                return error_result(
                                    ErrorCode::Internal,
                                    format!("Failed to read email hash index: {e}"),
                                );
                            },
                        };

                        // CAS: must be Pending
                        if current_entry.status != InvitationStatus::Pending {
                            return (
                                LedgerResponse::Error {
                                    code: ErrorCode::InvitationAlreadyResolved,
                                    message: format!(
                                        "Invitation {} is already {} (not Pending)",
                                        invite, current_entry.status
                                    ),
                                },
                                None,
                            );
                        }

                        // Update email hash entry with new terminal status
                        let updated_entry =
                            InviteEmailEntry { organization: *organization, status: *status };
                        let email_value = match encode(&updated_entry) {
                            Ok(v) => v,
                            Err(e) => {
                                return error_result(
                                    ErrorCode::Internal,
                                    format!("Failed to encode updated email hash entry: {e}"),
                                );
                            },
                        };

                        // Construct token hash index key for removal
                        let token_hex: String = inferadb_ledger_types::bytes_to_hex(token_hash);
                        let token_key = SystemKeys::invite_token_hash_index_key(&token_hex);

                        let ops = vec![
                            // Update email hash status
                            Operation::SetEntity {
                                key: email_key,
                                value: email_value,
                                condition: None,
                                expires_at: None,
                            },
                            // Remove token hash index (single-use)
                            Operation::DeleteEntity { key: token_key },
                        ];

                        if let Err(e) = state_layer.apply_operations(SYSTEM_VAULT_ID, &ops, 0) {
                            return error_result(
                                ErrorCode::Internal,
                                format!("Failed to update invite indexes: {e}"),
                            );
                        }

                        LedgerResponse::OrganizationInviteResolved { invite_id: *invite }
                    },
                    Err(err_response) => err_response,
                };

                if matches!(response, LedgerResponse::OrganizationInviteResolved { .. }) {
                    events.push(
                        ApplyPhaseEmitter::for_system(EventAction::InvitationResolved)
                            .detail("organization_id", &organization.to_string())
                            .detail("invite_id", &invite.to_string())
                            .detail("status", &format!("{status}"))
                            .outcome(EventOutcome::Success)
                            .build(block_height, *op_index, block_timestamp, ttl_days),
                    );
                    *op_index += 1;
                }

                (response, None)
            },

            // ── PurgeOrganizationInviteIndexes (GLOBAL) ──────────
            // Removes GLOBAL invitation indexes during retention reaping.
            // Deletes: slug index, email hash index entry, token hash index entry.
            LedgerRequest::PurgeOrganizationInviteIndexes {
                invite,
                slug,
                invitee_email_hmac,
                token_hash,
            } => {
                let response = if let Some(ref state_layer) = self.state_layer {
                    let slug_key = SystemKeys::invite_slug_index_key(*slug);
                    let email_key =
                        SystemKeys::invite_email_hash_index_key(invitee_email_hmac, *invite);
                    let token_hex: String = inferadb_ledger_types::bytes_to_hex(token_hash);
                    let token_key = SystemKeys::invite_token_hash_index_key(&token_hex);

                    let ops = vec![
                        Operation::DeleteEntity { key: slug_key },
                        Operation::DeleteEntity { key: email_key },
                        Operation::DeleteEntity { key: token_key },
                    ];

                    if let Err(e) = state_layer.apply_operations(SYSTEM_VAULT_ID, &ops, 0) {
                        return error_result(
                            ErrorCode::Internal,
                            format!("Failed to purge invite indexes: {e}"),
                        );
                    }

                    LedgerResponse::OrganizationInviteIndexesPurged { invite_id: *invite }
                } else {
                    return error_result(
                        ErrorCode::Internal,
                        "State layer not available".to_string(),
                    );
                };

                if matches!(response, LedgerResponse::OrganizationInviteIndexesPurged { .. }) {
                    events.push(
                        ApplyPhaseEmitter::for_system(EventAction::InvitationPurged)
                            .detail("invite_id", &invite.to_string())
                            .outcome(EventOutcome::Success)
                            .build(block_height, *op_index, block_timestamp, ttl_days),
                    );
                    *op_index += 1;
                }

                (response, None)
            },

            // ── RehashInviteEmailIndex (GLOBAL) ──────────────────
            // Re-keys the GLOBAL email hash index entry during blinding
            // key rotation. Deletes old HMAC entry, creates new one.
            LedgerRequest::RehashInviteEmailIndex {
                invite,
                old_hmac,
                new_hmac,
                organization,
                status,
            } => {
                let response = if let Some(ref state_layer) = self.state_layer {
                    let old_key = SystemKeys::invite_email_hash_index_key(old_hmac, *invite);
                    let new_key = SystemKeys::invite_email_hash_index_key(new_hmac, *invite);

                    let entry = InviteEmailEntry { organization: *organization, status: *status };
                    let value = match encode(&entry) {
                        Ok(v) => v,
                        Err(e) => {
                            return error_result(
                                ErrorCode::Internal,
                                format!("Failed to encode rehashed email entry: {e}"),
                            );
                        },
                    };

                    let ops = vec![
                        Operation::DeleteEntity { key: old_key },
                        Operation::SetEntity {
                            key: new_key,
                            value,
                            condition: None,
                            expires_at: None,
                        },
                    ];

                    if let Err(e) = state_layer.apply_operations(SYSTEM_VAULT_ID, &ops, 0) {
                        return error_result(
                            ErrorCode::Internal,
                            format!("Failed to rehash invite email index: {e}"),
                        );
                    }

                    LedgerResponse::InviteEmailIndexRehashed { invite_id: *invite }
                } else {
                    return error_result(
                        ErrorCode::Internal,
                        "State layer not available".to_string(),
                    );
                };

                if matches!(response, LedgerResponse::InviteEmailIndexRehashed { .. }) {
                    events.push(
                        ApplyPhaseEmitter::for_system(EventAction::InvitationEmailRehashed)
                            .detail("invite_id", &invite.to_string())
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
                        // Only write the slug index to GLOBAL state. The Team
                        // (which contains PII like name) is written to REGIONAL state
                        // via a separate WriteTeam proposal.
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
                // DeleteTeam (proposed first by the service handler).
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

                        // Clean up GLOBAL state layer entities (registry, skeleton, slug
                        // index key). Organization profile cleanup is handled by
                        // PurgeOrganizationRegional (profile lives in REGIONAL state).
                        if let Some(state_layer) = &self.state_layer {
                            let mut cleanup_ops = vec![
                                Operation::DeleteEntity {
                                    key: SystemKeys::organization_registry_key(*organization),
                                },
                                Operation::DeleteEntity {
                                    key: SystemKeys::organization_key(*organization),
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
                            // removal, user shred key deletion, and audit record creation.
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
                    SystemRequest::CreateOrganization { slug, region, tier, admin } => {
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
                                let key = SystemKeys::organization_registry_key(organization_id);
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

                            // Write Organization skeleton to GLOBAL (name = "").
                            // The skeleton is the authoritative record for structural
                            // fields — GLOBAL handlers (RemoveOrganizationMember,
                            // UpdateOrganizationMemberRole) operate on it directly.
                            let org_skeleton = inferadb_ledger_state::system::Organization {
                                organization: organization_id,
                                slug: *slug,
                                region: *region,
                                name: String::new(),
                                tier: *tier,
                                status: OrganizationStatus::Provisioning,
                                members: vec![OrganizationMember {
                                    user_id: *admin,
                                    role: OrganizationMemberRole::Admin,
                                    joined_at: block_timestamp,
                                }],
                                created_at: block_timestamp,
                                updated_at: block_timestamp,
                                deleted_at: None,
                            };
                            if let Err(e) =
                                save_organization(state_layer, organization_id, &org_skeleton)
                            {
                                return (e, None);
                            }
                        }

                        // Update user→org index for initial admin member
                        state.user_org_index.entry(*admin).or_default().insert(organization_id);

                        LedgerResponse::OrganizationCreated {
                            organization_id,
                            organization_slug: *slug,
                        }
                    },
                    SystemRequest::WriteOrganizationProfile {
                        organization,
                        sealed_name,
                        name_nonce,
                        shred_key_bytes,
                    } => {
                        if let Some(state_layer) = &self.state_layer {
                            // Step 1: Store the OrgShredKey for future crypto-shredding.
                            let sys = inferadb_ledger_state::system::SystemOrganizationService::new(
                                state_layer.clone(),
                            );
                            if let Err(e) = sys.store_org_shred_key(*organization, shred_key_bytes)
                            {
                                tracing::error!(
                                    organization_id = organization.value(),
                                    error = %e,
                                    "Failed to store OrgShredKey"
                                );
                                return (
                                    LedgerResponse::Error {
                                        code: ErrorCode::Internal,
                                        message: format!("Failed to store OrgShredKey: {e}"),
                                    },
                                    None,
                                );
                            }

                            // Step 2: Unseal the organization name.
                            let aad = organization.value().to_le_bytes();
                            let name = match crate::entry_crypto::unseal(
                                sealed_name,
                                name_nonce,
                                shred_key_bytes,
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

                            // Step 3: Write slimmed OrganizationProfile (PII) to REGIONAL.
                            // The Organization skeleton (with admin member) was already
                            // created by CreateOrganization (GLOBAL). The
                            // user_org_index was also updated there. We only write PII here.
                            let profile = OrganizationProfile { name, updated_at: block_timestamp };
                            if let Err(e) = save_org_profile(state_layer, *organization, &profile) {
                                return (e, None);
                            }
                        }

                        LedgerResponse::OrganizationProfileWritten {
                            organization_id: *organization,
                        }
                    },
                    SystemRequest::UpdateOrganizationProfile { organization, name } => {
                        if let Some(state_layer) = &self.state_layer {
                            let profile = OrganizationProfile {
                                name: name.clone(),
                                updated_at: block_timestamp,
                            };
                            if let Err(e) = save_org_profile(state_layer, *organization, &profile) {
                                return (e, None);
                            }
                            LedgerResponse::OrganizationUpdated { organization_id: *organization }
                        } else {
                            ledger_error(
                                ErrorCode::Internal,
                                "State layer unavailable for profile update".to_string(),
                            )
                        }
                    },
                    SystemRequest::UpdateOrganizationStatus { organization, status } => {
                        if let Some(org_meta) = state.organizations.get_mut(organization) {
                            org_meta.status = *status;
                            let slug = org_meta.slug;
                            if let Some(blob) = try_encode(org_meta, "org_meta") {
                                pending.organizations.push((*organization, blob));
                            }

                            // Sync OrganizationRegistry so RegionRouter and PurgeJob
                            // see the updated status.
                            if let Some(sys) = &sys_service
                                && let Ok(Some(mut registry)) = sys.get_organization(*organization)
                            {
                                registry.status = *status;
                                registry.config_version += 1;
                                if *status == OrganizationStatus::Deleted {
                                    registry.deleted_at = Some(block_timestamp);
                                }
                                if let Err(e) = sys.register_organization(&registry, slug) {
                                    tracing::error!(
                                        organization_id = organization.value(),
                                        error = %e,
                                        "Failed to sync org registry status"
                                    );
                                }
                            }

                            LedgerResponse::OrganizationStatusUpdated {
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
                        totp,
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

                            // 6. Branch on existing_user_hmac_hit and TOTP status
                            if *existing_user_hmac_hit {
                                if let Some(totp) = totp {
                                    // Existing user with TOTP: atomically create challenge
                                    let challenge = PendingTotpChallenge {
                                        nonce: totp.nonce,
                                        user: totp.user_id,
                                        user_slug: totp.user_slug,
                                        expires_at: totp.expires_at,
                                        attempts: 0,
                                        primary_method: PrimaryAuthMethod::EmailCode,
                                    };
                                    match sys.create_totp_challenge(&challenge) {
                                        Ok(()) => LedgerResponse::EmailCodeVerified {
                                            result: EmailCodeVerifiedResult::TotpRequired {
                                                nonce: totp.nonce,
                                            },
                                        },
                                        Err(SystemError::ResourceExhausted { message }) => {
                                            ledger_error(ErrorCode::RateLimited, message)
                                        },
                                        Err(e) => ledger_error(
                                            ErrorCode::Internal,
                                            format!("Failed to create TOTP challenge: {e}"),
                                        ),
                                    }
                                } else {
                                    // Existing user without TOTP: signal only, no writes
                                    LedgerResponse::EmailCodeVerified {
                                        result: EmailCodeVerifiedResult::ExistingUser,
                                    }
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
                                // Log but continue — the HMAC CAS has already claimed
                                // this email. Returning an error here would leave the
                                // HMAC in Provisioning state with no directory entry,
                                // and saga retries would permanently fail with
                                // AlreadyExists (the idempotency check requires the
                                // directory to exist). The saga's later steps will
                                // fill in missing state.
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
                                // Log but continue — same rationale as user directory
                                // write above. The HMAC CAS claim is irrevocable within
                                // this step; returning an error would strand the saga.
                                tracing::error!(
                                    organization_id = organization_id.value(),
                                    error = %e,
                                    "Failed to persist org registry"
                                );
                            }

                            // Write Organization skeleton to GLOBAL (name = "").
                            // This skeleton is the authoritative record for structural
                            // fields — GLOBAL handlers (RemoveOrganizationMember,
                            // UpdateOrganizationMemberRole) operate on it directly.
                            let org_skeleton = inferadb_ledger_state::system::Organization {
                                organization: organization_id,
                                slug: *organization_slug,
                                region: *region,
                                name: String::new(),
                                tier: OrganizationTier::default(),
                                status: OrganizationStatus::Provisioning,
                                members: vec![OrganizationMember {
                                    user_id,
                                    role: OrganizationMemberRole::Admin,
                                    joined_at: block_timestamp,
                                }],
                                created_at: block_timestamp,
                                updated_at: block_timestamp,
                                deleted_at: None,
                            };
                            if let Some(sl) = &self.state_layer
                                && let Err(e) =
                                    save_organization(sl, organization_id, &org_skeleton)
                            {
                                return (e, None);
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
                        organization_slug: _,
                        email_hmac,
                        sealed_pii,
                        pii_nonce,
                        shred_key_bytes,
                        refresh_token_hash,
                        refresh_family_id,
                        refresh_expires_at,
                        kid,
                        region,
                    } => {
                        if let Some(state_layer) = &self.state_layer {
                            let sys = SystemOrganizationService::new(state_layer.clone());

                            // Unseal PII using the shred key from this entry
                            let aad = user_id.value().to_le_bytes();
                            let pii_bytes = match crate::entry_crypto::unseal(
                                sealed_pii,
                                pii_nonce,
                                shred_key_bytes,
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

                            // 3. Store user shred key
                            if let Err(e) = sys.store_user_shred_key(*user_id, shred_key_bytes) {
                                return (
                                    LedgerResponse::Error {
                                        code: ErrorCode::Internal,
                                        message: format!("Failed to store user shred key: {e}"),
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
                                family_created_at: Some(block_timestamp),
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

                            // 5. Write slimmed OrganizationProfile (PII only) to REGIONAL.
                            let profile = OrganizationProfile {
                                name: pii.organization_name.clone(),
                                updated_at: block_timestamp,
                            };
                            if let Err(e) =
                                save_org_profile(state_layer, *organization_id, &profile)
                            {
                                return (e, None);
                            }

                            // 6. Delete onboarding account record
                            // Note: Organization skeleton (with admin member) was already
                            // created by CreateOnboardingUser (GLOBAL). We don't modify
                            // it here — REGIONAL handlers must not write GLOBAL data.
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

                            // 2. Update HMAC index: Provisioning → Active(user_id)
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

                            // 3. Update in-memory org meta status + persist to B+ tree
                            if let Some(meta) = state.organizations.get_mut(organization_id) {
                                meta.status = OrganizationStatus::Active;
                                if let Some(blob) = try_encode(meta, "org_meta") {
                                    pending.organizations.push((*organization_id, blob));
                                }
                            }

                            // 4. Update org registry status
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
                            let mut totp_deleted: u32 = 0;

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

                            // 3. Scan expired TOTP challenges
                            if let Ok(entities) = state_layer.list_entities(
                                SYSTEM_VAULT_ID,
                                Some(SystemKeys::TOTP_CHALLENGE_PREFIX),
                                None,
                                inferadb_ledger_types::onboarding::MAX_ONBOARDING_SCAN,
                            ) {
                                let mut delete_ops = Vec::new();
                                for entity in &entities {
                                    let key_str = String::from_utf8_lossy(&entity.key).to_string();
                                    if let Ok(record) =
                                        decode::<PendingTotpChallenge>(&entity.value)
                                        && record.expires_at <= block_timestamp
                                    {
                                        delete_ops.push(Operation::DeleteEntity { key: key_str });
                                    }
                                }
                                totp_deleted = delete_ops.len() as u32;
                                if !delete_ops.is_empty()
                                    && let Err(e) = state_layer.apply_operations(
                                        SYSTEM_VAULT_ID,
                                        &delete_ops,
                                        0,
                                    )
                                {
                                    tracing::warn!(
                                        error = %e,
                                        "Failed to delete expired TOTP challenges"
                                    );
                                }
                            }

                            LedgerResponse::OnboardingCleanedUp {
                                verification_codes_deleted: codes_deleted,
                                onboarding_accounts_deleted: accounts_deleted,
                                totp_challenges_deleted: totp_deleted,
                            }
                        } else {
                            LedgerResponse::Error {
                                code: ErrorCode::Internal,
                                message: "State layer not available".to_string(),
                            }
                        }
                    },

                    // ── WriteTeam (REGIONAL) ──
                    // Upsert: creates profile from scratch on first call (REGIONAL
                    // state has no data from GLOBAL CreateOrganizationTeam), or
                    // updates name on subsequent calls (rename).
                    SystemRequest::WriteTeam { organization, team, slug, name } => {
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
                                match load_team(state_layer, *organization, *team) {
                                    Ok(mut existing) => {
                                        let old = existing.name.clone();
                                        existing.name.clone_from(name);
                                        existing.updated_at = block_timestamp;
                                        (existing, Some(old))
                                    },
                                    Err(LedgerResponse::Error {
                                        code: ErrorCode::NotFound,
                                        ..
                                    }) => {
                                        // First write — create from scratch
                                        let fresh = Team {
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
                                    Err(e) => return (e, None),
                                };

                            match save_team(state_layer, *organization, *team, &profile) {
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
                                "State layer unavailable for team write",
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
                                    Err(LedgerResponse::Error {
                                        code: ErrorCode::NotFound,
                                        ..
                                    }) => {
                                        let fresh = AppProfile {
                                            app: *app,
                                            organization: *organization,
                                            name: name.clone(),
                                            description: description.clone(),
                                            updated_at: block_timestamp,
                                        };
                                        (fresh, None)
                                    },
                                    Err(e) => return (e, None),
                                };

                            // Batch profile write + name index ops atomically
                            let profile_key = SystemKeys::app_profile_key(*organization, *app);
                            if let Err(msg) =
                                SystemKeys::validate_key_tier(&profile_key, KeyTier::Regional)
                            {
                                return (ledger_error(ErrorCode::Internal, msg), None);
                            }
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

                    // ── DeleteTeam (REGIONAL) ──
                    // Deletes the team record and name index from REGIONAL state.
                    // Handles member migration if move_members_to is specified.
                    SystemRequest::DeleteTeam { organization, team, move_members_to } => {
                        if let Some(state_layer) = &self.state_layer {
                            match load_team(state_layer, *organization, *team) {
                                Ok(team_record) => {
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
                                            &team_record,
                                            *target_team,
                                            block_timestamp,
                                        ) {
                                            return (resp, None);
                                        }
                                    }

                                    // Delete team from REGIONAL state
                                    let team_storage_key =
                                        SystemKeys::team_key(*organization, *team);
                                    let ops =
                                        vec![Operation::DeleteEntity { key: team_storage_key }];
                                    if let Err(e) =
                                        state_layer.apply_operations(SYSTEM_VAULT_ID, &ops, 0)
                                    {
                                        return error_result(
                                            ErrorCode::Internal,
                                            format!("Failed to delete team: {e}"),
                                        );
                                    }

                                    // Clean up REGIONAL name index
                                    state
                                        .team_name_index
                                        .remove(&(*organization, team_record.name));

                                    LedgerResponse::OrganizationTeamDeleted {
                                        organization_id: *organization,
                                    }
                                },
                                Err(LedgerResponse::Error {
                                    code: ErrorCode::NotFound, ..
                                }) => {
                                    // Team doesn't exist in REGIONAL state — succeed idempotently
                                    LedgerResponse::OrganizationTeamDeleted {
                                        organization_id: *organization,
                                    }
                                },
                                Err(e) => return (e, None),
                            }
                        } else {
                            ledger_error(
                                ErrorCode::Internal,
                                "State layer unavailable for team delete",
                            )
                        }
                    },

                    // ── AddTeamMember (REGIONAL) ──
                    // Adds a member to a team.
                    SystemRequest::AddTeamMember { organization, team, user_id, role } => {
                        if let Some(state_layer) = &self.state_layer {
                            match load_team(state_layer, *organization, *team) {
                                Ok(mut t) => {
                                    if t.members.iter().any(|m| m.user_id == *user_id) {
                                        ledger_error(
                                            ErrorCode::AlreadyExists,
                                            format!(
                                                "User {} is already a member of team {}",
                                                user_id, team
                                            ),
                                        )
                                    } else {
                                        t.members.push(TeamMember {
                                            user_id: *user_id,
                                            role: *role,
                                            joined_at: block_timestamp,
                                        });
                                        t.updated_at = block_timestamp;
                                        match save_team(state_layer, *organization, *team, &t) {
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
                    // Removes a member from a team.
                    SystemRequest::RemoveTeamMember { organization, team, user_id } => {
                        if let Some(state_layer) = &self.state_layer {
                            match load_team(state_layer, *organization, *team) {
                                Ok(mut t) => {
                                    use inferadb_ledger_state::system::TeamMemberRole;

                                    let original_len = t.members.len();
                                    let is_last_manager = t.members.iter().any(|m| {
                                        m.user_id == *user_id && m.role == TeamMemberRole::Manager
                                    }) && t
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
                                        t.members.retain(|m| m.user_id != *user_id);
                                        if t.members.len() == original_len {
                                            ledger_error(
                                                ErrorCode::NotFound,
                                                format!(
                                                    "User {} is not a member of team {}",
                                                    user_id, team
                                                ),
                                            )
                                        } else {
                                            t.updated_at = block_timestamp;
                                            match save_team(state_layer, *organization, *team, &t) {
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

                    // ── UpdateTeamMemberRole (REGIONAL) ──
                    // Updates a member's role within a team.
                    SystemRequest::UpdateTeamMemberRole { organization, team, user_id, role } => {
                        if let Some(state_layer) = &self.state_layer {
                            match load_team(state_layer, *organization, *team) {
                                Ok(mut t) => {
                                    if let Some(member) =
                                        t.members.iter_mut().find(|m| m.user_id == *user_id)
                                    {
                                        member.role = *role;
                                        t.updated_at = block_timestamp;
                                        match save_team(state_layer, *organization, *team, &t) {
                                            Ok(()) => LedgerResponse::OrganizationUpdated {
                                                organization_id: *organization,
                                            },
                                            Err(e) => e,
                                        }
                                    } else {
                                        ledger_error(
                                            ErrorCode::NotFound,
                                            format!(
                                                "User {} is not a member of team {}",
                                                user_id, team
                                            ),
                                        )
                                    }
                                },
                                Err(e) => e,
                            }
                        } else {
                            ledger_error(
                                ErrorCode::Internal,
                                "State layer unavailable for update team member role",
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
                    // Deletes all team records, app profiles, assertion names,
                    // and name indices belonging to this organization from the
                    // REGIONAL state layer.
                    SystemRequest::PurgeOrganizationRegional { organization } => {
                        if let Some(state_layer) = &self.state_layer {
                            let mut ops = Vec::new();

                            // Delete all team records
                            let team_prefix =
                                format!("{}{}:", SystemKeys::TEAM_PREFIX, organization.value());
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

                            // Destroy the OrgShredKey — crypto-shredding: all historical
                            // EncryptedOrgSystem entries for this organization become
                            // permanently unrecoverable.
                            let shred_key = SystemKeys::org_shred_key(*organization);
                            ops.push(Operation::DeleteEntity { key: shred_key });

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

                    // ── User Credential CRUD (REGIONAL, encrypted) ──
                    SystemRequest::CreateUserCredential {
                        user_id,
                        credential_type,
                        credential_data,
                        name,
                    } => {
                        if let Some(sys) = &sys_service {
                            match sys.create_user_credential(
                                *user_id,
                                *credential_type,
                                credential_data.clone(),
                                name,
                                block_timestamp,
                            ) {
                                Ok(cred) => {
                                    LedgerResponse::UserCredentialCreated { credential_id: cred.id }
                                },
                                Err(SystemError::AlreadyExists { entity }) => ledger_error(
                                    ErrorCode::AlreadyExists,
                                    format!("Credential already exists: {entity}"),
                                ),
                                Err(SystemError::FailedPrecondition { message }) => {
                                    ledger_error(ErrorCode::InvalidArgument, message)
                                },
                                Err(e) => ledger_error(
                                    ErrorCode::Internal,
                                    format!("Failed to create credential: {e}"),
                                ),
                            }
                        } else {
                            ledger_error(
                                ErrorCode::Internal,
                                "State layer unavailable for credential create",
                            )
                        }
                    },

                    SystemRequest::UpdateUserCredential {
                        user_id,
                        credential_id,
                        name,
                        enabled,
                        passkey_update,
                    } => {
                        if let Some(sys) = &sys_service {
                            match sys.update_user_credential(
                                *user_id,
                                *credential_id,
                                name.as_deref(),
                                *enabled,
                                None, // last_used_at: management ops don't count as "use"
                                passkey_update.as_ref(),
                            ) {
                                Ok(cred) => {
                                    LedgerResponse::UserCredentialUpdated { credential_id: cred.id }
                                },
                                Err(SystemError::NotFound { entity }) => ledger_error(
                                    ErrorCode::NotFound,
                                    format!("Credential not found: {entity}"),
                                ),
                                Err(e) => ledger_error(
                                    ErrorCode::Internal,
                                    format!("Failed to update credential: {e}"),
                                ),
                            }
                        } else {
                            ledger_error(
                                ErrorCode::Internal,
                                "State layer unavailable for credential update",
                            )
                        }
                    },

                    SystemRequest::DeleteUserCredential { user_id, credential_id } => {
                        if let Some(sys) = &sys_service {
                            match sys.delete_user_credential(*user_id, *credential_id) {
                                Ok(()) => LedgerResponse::UserCredentialDeleted {
                                    credential_id: *credential_id,
                                },
                                Err(SystemError::NotFound { entity }) => ledger_error(
                                    ErrorCode::NotFound,
                                    format!("Credential not found: {entity}"),
                                ),
                                Err(SystemError::FailedPrecondition { message }) => {
                                    ledger_error(ErrorCode::FailedPrecondition, message)
                                },
                                Err(e) => ledger_error(
                                    ErrorCode::Internal,
                                    format!("Failed to delete credential: {e}"),
                                ),
                            }
                        } else {
                            ledger_error(
                                ErrorCode::Internal,
                                "State layer unavailable for credential delete",
                            )
                        }
                    },

                    // ── TOTP Challenge Lifecycle (REGIONAL, plain) ──
                    SystemRequest::CreateTotpChallenge {
                        user_id,
                        user_slug,
                        nonce,
                        expires_at,
                        primary_method,
                    } => {
                        if let Some(sys) = &sys_service {
                            let challenge = PendingTotpChallenge {
                                nonce: *nonce,
                                user: *user_id,
                                user_slug: *user_slug,
                                expires_at: *expires_at,
                                attempts: 0,
                                primary_method: *primary_method,
                            };
                            match sys.create_totp_challenge(&challenge) {
                                Ok(()) => LedgerResponse::TotpChallengeCreated { nonce: *nonce },
                                Err(SystemError::ResourceExhausted { message }) => {
                                    ledger_error(ErrorCode::RateLimited, message)
                                },
                                Err(e) => ledger_error(
                                    ErrorCode::Internal,
                                    format!("Failed to create TOTP challenge: {e}"),
                                ),
                            }
                        } else {
                            ledger_error(
                                ErrorCode::Internal,
                                "State layer unavailable for TOTP challenge create",
                            )
                        }
                    },

                    SystemRequest::ConsumeTotpAndCreateSession {
                        user_id,
                        nonce,
                        token_hash,
                        family,
                        kid,
                        ttl_secs,
                    } => {
                        if let Some(sys) = &sys_service {
                            // 1. Read and validate the challenge
                            let challenge = match sys.get_totp_challenge(*user_id, nonce) {
                                Ok(Some(c)) => c,
                                Ok(None) => {
                                    return (
                                        ledger_error(ErrorCode::NotFound, "Challenge not found"),
                                        None,
                                    );
                                },
                                Err(e) => {
                                    return (
                                        ledger_error(
                                            ErrorCode::Internal,
                                            format!("Failed to read challenge: {e}"),
                                        ),
                                        None,
                                    );
                                },
                            };

                            // 2. Deterministic expiry check (proposed_at, not SystemTime::now)
                            if challenge.expires_at <= block_timestamp {
                                return (
                                    ledger_error(
                                        ErrorCode::FailedPrecondition,
                                        "Challenge expired",
                                    ),
                                    None,
                                );
                            }

                            // 3. Defense-in-depth: reject if max attempts exceeded.
                            // The service layer should never propose consumption for a
                            // locked-out challenge, but the state machine enforces this
                            // independently to prevent bypasses.
                            // Mirrors `SystemOrganizationService::MAX_TOTP_ATTEMPTS`.
                            const MAX_TOTP_ATTEMPTS: u8 = 3;
                            if challenge.attempts >= MAX_TOTP_ATTEMPTS {
                                return (
                                    ledger_error(ErrorCode::TooManyAttempts, "Too many attempts"),
                                    None,
                                );
                            }

                            // 4. Delete the challenge (consumed)
                            if let Err(e) = sys.delete_totp_challenge(*user_id, nonce) {
                                return (
                                    ledger_error(
                                        ErrorCode::Internal,
                                        format!("Failed to delete challenge: {e}"),
                                    ),
                                    None,
                                );
                            }

                            // 5. Create refresh token (same pattern as CreateRefreshToken)
                            let user_slug = challenge.user_slug;
                            let token_id = state.sequences.next_refresh_token();
                            let expires_at = block_timestamp + saturating_duration_secs(*ttl_secs);

                            let token = RefreshToken {
                                id: token_id,
                                token_hash: *token_hash,
                                family: *family,
                                token_type: TokenType::UserSession,
                                subject: TokenSubject::User(user_slug),
                                organization: None,
                                vault: None,
                                kid: kid.clone(),
                                expires_at,
                                used: false,
                                created_at: block_timestamp,
                                used_at: None,
                                revoked_at: None,
                                family_created_at: Some(block_timestamp),
                            };

                            if let Err(e) = sys.store_refresh_token(&token) {
                                return (
                                    ledger_error(
                                        ErrorCode::Internal,
                                        format!("Failed to store refresh token: {e}"),
                                    ),
                                    None,
                                );
                            }

                            LedgerResponse::TotpVerified { refresh_token_id: token_id }
                        } else {
                            ledger_error(
                                ErrorCode::Internal,
                                "State layer unavailable for TOTP consumption",
                            )
                        }
                    },

                    SystemRequest::ConsumeRecoveryAndCreateSession {
                        user_id,
                        nonce,
                        code_hash,
                        credential_id,
                        token_hash,
                        family,
                        kid,
                        ttl_secs,
                    } => {
                        if let Some(sys) = &sys_service {
                            // 1. Read and validate the challenge
                            let challenge = match sys.get_totp_challenge(*user_id, nonce) {
                                Ok(Some(c)) => c,
                                Ok(None) => {
                                    return (
                                        ledger_error(ErrorCode::NotFound, "Challenge not found"),
                                        None,
                                    );
                                },
                                Err(e) => {
                                    return (
                                        ledger_error(
                                            ErrorCode::Internal,
                                            format!("Failed to read challenge: {e}"),
                                        ),
                                        None,
                                    );
                                },
                            };

                            // 2. Deterministic expiry check
                            if challenge.expires_at <= block_timestamp {
                                return (
                                    ledger_error(
                                        ErrorCode::FailedPrecondition,
                                        "Challenge expired",
                                    ),
                                    None,
                                );
                            }

                            // 3. Defense-in-depth: reject if max attempts exceeded.
                            // Mirrors `SystemOrganizationService::MAX_TOTP_ATTEMPTS`.
                            const MAX_TOTP_ATTEMPTS: u8 = 3;
                            if challenge.attempts >= MAX_TOTP_ATTEMPTS {
                                return (
                                    ledger_error(ErrorCode::TooManyAttempts, "Too many attempts"),
                                    None,
                                );
                            }

                            // 4. Delete the challenge first (invalidate before
                            // credential mutation — matches ConsumeTotpAndCreateSession
                            // ordering to prevent partial-replay windows).
                            let user_slug = challenge.user_slug;
                            if let Err(e) = sys.delete_totp_challenge(*user_id, nonce) {
                                return (
                                    ledger_error(
                                        ErrorCode::Internal,
                                        format!("Failed to delete challenge: {e}"),
                                    ),
                                    None,
                                );
                            }

                            // 5. Consume the recovery code (atomic hash removal)
                            let remaining = match sys.consume_recovery_code(
                                *user_id,
                                *credential_id,
                                code_hash,
                                block_timestamp,
                            ) {
                                Ok(count) => count,
                                Err(SystemError::NotFound { .. }) => {
                                    return (
                                        ledger_error(
                                            ErrorCode::Unauthenticated,
                                            "Verification failed",
                                        ),
                                        None,
                                    );
                                },
                                Err(e) => {
                                    return (
                                        ledger_error(
                                            ErrorCode::Internal,
                                            format!("Failed to consume recovery code: {e}"),
                                        ),
                                        None,
                                    );
                                },
                            };

                            // 6. Create refresh token
                            let token_id = state.sequences.next_refresh_token();
                            let expires_at = block_timestamp + saturating_duration_secs(*ttl_secs);

                            let token = RefreshToken {
                                id: token_id,
                                token_hash: *token_hash,
                                family: *family,
                                token_type: TokenType::UserSession,
                                subject: TokenSubject::User(user_slug),
                                organization: None,
                                vault: None,
                                kid: kid.clone(),
                                expires_at,
                                used: false,
                                created_at: block_timestamp,
                                used_at: None,
                                revoked_at: None,
                                family_created_at: Some(block_timestamp),
                            };

                            if let Err(e) = sys.store_refresh_token(&token) {
                                return (
                                    ledger_error(
                                        ErrorCode::Internal,
                                        format!("Failed to store refresh token: {e}"),
                                    ),
                                    None,
                                );
                            }

                            LedgerResponse::RecoveryCodeConsumed {
                                refresh_token_id: token_id,
                                remaining_codes: remaining,
                            }
                        } else {
                            ledger_error(
                                ErrorCode::Internal,
                                "State layer unavailable for recovery code consumption",
                            )
                        }
                    },

                    SystemRequest::IncrementTotpAttempt { user_id, nonce } => {
                        if let Some(sys) = &sys_service {
                            match sys.increment_totp_attempts(*user_id, nonce) {
                                Ok(challenge) => LedgerResponse::TotpAttemptIncremented {
                                    attempts: challenge.attempts,
                                },
                                Err(SystemError::NotFound { entity }) => {
                                    ledger_error(ErrorCode::NotFound, entity)
                                },
                                Err(SystemError::FailedPrecondition { message }) => {
                                    ledger_error(ErrorCode::TooManyAttempts, message)
                                },
                                Err(e) => ledger_error(
                                    ErrorCode::Internal,
                                    format!("Failed to increment TOTP attempts: {e}"),
                                ),
                            }
                        } else {
                            ledger_error(
                                ErrorCode::Internal,
                                "State layer unavailable for TOTP attempt increment",
                            )
                        }
                    },

                    // ── WriteOrganizationInvite (REGIONAL) ──
                    // Writes the full invitation record to REGIONAL state.
                    SystemRequest::WriteOrganizationInvite {
                        organization,
                        invite,
                        slug,
                        token_hash,
                        inviter,
                        invitee_email_hmac,
                        invitee_email,
                        role,
                        team,
                        expires_at,
                    } => {
                        if let Some(sys) = &sys_service {
                            let invitation = inferadb_ledger_types::OrganizationInvitation {
                                id: *invite,
                                slug: *slug,
                                organization: *organization,
                                token_hash: *token_hash,
                                inviter: *inviter,
                                invitee_email_hmac: invitee_email_hmac.clone(),
                                invitee_email: invitee_email.clone(),
                                role: *role,
                                team: *team,
                                status: InvitationStatus::Pending,
                                created_at: block_timestamp,
                                expires_at: *expires_at,
                                resolved_at: None,
                            };
                            match sys.create_invitation(&invitation) {
                                Ok(()) => LedgerResponse::Empty,
                                Err(e) => ledger_error(
                                    ErrorCode::Internal,
                                    format!("Failed to write invitation record: {e}"),
                                ),
                            }
                        } else {
                            ledger_error(
                                ErrorCode::Internal,
                                "State layer unavailable for invitation write",
                            )
                        }
                    },

                    // ── UpdateOrganizationInviteStatus (REGIONAL) ──
                    // CAS: Pending-only. Sets resolved_at = proposed_at.
                    SystemRequest::UpdateOrganizationInviteStatus {
                        organization,
                        invite,
                        status,
                    } => {
                        if let Some(sys) = &sys_service {
                            match sys.update_invitation_status(
                                *organization,
                                *invite,
                                *status,
                                block_timestamp,
                            ) {
                                Ok(_) => LedgerResponse::Empty,
                                Err(SystemError::NotFound { entity }) => {
                                    ledger_error(ErrorCode::NotFound, entity)
                                },
                                Err(SystemError::FailedPrecondition { message }) => {
                                    ledger_error(ErrorCode::InvitationAlreadyResolved, message)
                                },
                                Err(e) => ledger_error(
                                    ErrorCode::Internal,
                                    format!("Failed to update invitation status: {e}"),
                                ),
                            }
                        } else {
                            ledger_error(
                                ErrorCode::Internal,
                                "State layer unavailable for invitation status update",
                            )
                        }
                    },

                    // ── DeleteOrganizationInvite (REGIONAL) ──
                    // Deletes the invitation record. Used by the retention reaper.
                    SystemRequest::DeleteOrganizationInvite { organization, invite } => {
                        if let Some(sys) = &sys_service {
                            match sys.delete_invitation(*organization, *invite) {
                                Ok(()) => LedgerResponse::Empty,
                                Err(e) => ledger_error(
                                    ErrorCode::Internal,
                                    format!("Failed to delete invitation record: {e}"),
                                ),
                            }
                        } else {
                            ledger_error(
                                ErrorCode::Internal,
                                "State layer unavailable for invitation delete",
                            )
                        }
                    },

                    // ── RehashInvitationEmailHmac (REGIONAL) ──
                    // Updates the invitee_email_hmac field during blinding key rotation.
                    SystemRequest::RehashInvitationEmailHmac { organization, invite, new_hmac } => {
                        if let Some(sys) = &sys_service {
                            match sys.update_invitation_email_hmac(
                                *organization,
                                *invite,
                                new_hmac.clone(),
                            ) {
                                Ok(()) => LedgerResponse::Empty,
                                Err(e) => ledger_error(
                                    ErrorCode::Internal,
                                    format!("Failed to rehash invitation email HMAC: {e}"),
                                ),
                            }
                        } else {
                            ledger_error(
                                ErrorCode::Internal,
                                "State layer unavailable for invitation rehash",
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
                        LedgerResponse::OrganizationCreated { organization_id, organization_slug },
                        SystemRequest::CreateOrganization { region, .. },
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
                    family_created_at: Some(block_timestamp),
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
                max_family_lifetime_secs,
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

                // Check family lifetime. For tokens created before this field
                // existed, fall back to `created_at` (conservative: treats the
                // individual token's creation as the family origin).
                let family_origin = old_token.family_created_at.unwrap_or(old_token.created_at);
                let family_deadline =
                    family_origin + saturating_duration_secs(*max_family_lifetime_secs);
                if family_deadline <= block_timestamp {
                    return error_result(
                        ErrorCode::Expired,
                        "Refresh token family lifetime exceeded \u{2014} re-authentication required",
                    );
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
                    family_created_at: Some(family_origin),
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
                // Decrypt the SystemRequest using the user's UserShredKey.
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

                let key_str = SystemKeys::user_shred_key(encrypted.user_id);
                let shred_key_entity = match state_layer
                    .get_entity(SYSTEM_VAULT_ID, key_str.as_bytes())
                {
                    Ok(Some(entity)) => entity,
                    Ok(None) => {
                        // UserShredKey destroyed — user has been erased.
                        // Crypto-shredding: this entry is permanently unrecoverable.
                        tracing::info!(
                            user_id = encrypted.user_id.value(),
                            "EncryptedUserSystem: UserShredKey destroyed (user erased), skipping entry"
                        );
                        return (LedgerResponse::Empty, None);
                    },
                    Err(e) => {
                        tracing::error!(
                            user_id = encrypted.user_id.value(),
                            error = %e,
                            "EncryptedUserSystem: failed to read UserShredKey"
                        );
                        return (
                            LedgerResponse::Error {
                                code: ErrorCode::Internal,
                                message: format!("Failed to read UserShredKey: {e}"),
                            },
                            None,
                        );
                    },
                };

                let shred_key: inferadb_ledger_state::system::UserShredKey =
                    match decode(&shred_key_entity.value) {
                        Ok(sk) => sk,
                        Err(e) => {
                            tracing::error!(
                                user_id = encrypted.user_id.value(),
                                error = %e,
                                "EncryptedUserSystem: failed to decode UserShredKey"
                            );
                            return (
                                LedgerResponse::Error {
                                    code: ErrorCode::Internal,
                                    message: format!("Failed to decode UserShredKey: {e}"),
                                },
                                None,
                            );
                        },
                    };

                let sys_request = match crate::entry_crypto::decrypt_user_system_request(
                    encrypted,
                    &shred_key.key,
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
                    log_id_bytes,
                    skip_state_writes,
                )
            },

            LedgerRequest::EncryptedOrgSystem(encrypted) => {
                // Decrypt the SystemRequest using the organization's OrgShredKey.
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

                let key_str = SystemKeys::org_shred_key(encrypted.organization);
                let shred_key_entity = match state_layer
                    .get_entity(SYSTEM_VAULT_ID, key_str.as_bytes())
                {
                    Ok(Some(entity)) => entity,
                    Ok(None) => {
                        // OrgShredKey destroyed — organization has been purged.
                        // Crypto-shredding: this entry is permanently unrecoverable.
                        tracing::info!(
                            organization_id = encrypted.organization.value(),
                            "EncryptedOrgSystem: OrgShredKey destroyed (org purged), skipping entry"
                        );
                        return (LedgerResponse::Empty, None);
                    },
                    Err(e) => {
                        tracing::error!(
                            organization_id = encrypted.organization.value(),
                            error = %e,
                            "EncryptedOrgSystem: failed to read OrgShredKey"
                        );
                        return (
                            LedgerResponse::Error {
                                code: ErrorCode::Internal,
                                message: format!("Failed to read OrgShredKey: {e}"),
                            },
                            None,
                        );
                    },
                };

                let shred_key: inferadb_ledger_state::system::OrgShredKey =
                    match decode(&shred_key_entity.value) {
                        Ok(ok) => ok,
                        Err(e) => {
                            tracing::error!(
                                organization_id = encrypted.organization.value(),
                                error = %e,
                                "EncryptedOrgSystem: failed to decode OrgShredKey"
                            );
                            return (
                                LedgerResponse::Error {
                                    code: ErrorCode::Internal,
                                    message: format!("Failed to decode OrgShredKey: {e}"),
                                },
                                None,
                            );
                        },
                    };

                let sys_request = match crate::entry_crypto::decrypt_org_system_request(
                    encrypted,
                    &shred_key.key,
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
                    log_id_bytes,
                    skip_state_writes,
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
                        log_id_bytes,
                        skip_state_writes,
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
