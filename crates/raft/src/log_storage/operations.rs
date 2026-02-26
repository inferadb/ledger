//! State machine apply logic for Raft log entries.
//!
//! Transforms committed log entries into state mutations via the storage engine.

use chrono::{DateTime, Utc};
use inferadb_ledger_state::{
    StateError,
    system::{OrganizationRegistry, OrganizationStatus, SYSTEM_VAULT_ID, SystemKeys},
};
use inferadb_ledger_store::StorageBackend;
use inferadb_ledger_types::{
    Hash, Operation, OrganizationId, VaultEntry, VaultId, compute_tx_merkle_root, encode,
    events::{EventAction, EventEntry, EventOutcome},
};

use super::{
    store::RaftLogStore,
    types::{
        AppliedState, ClientSequenceEntry, OrganizationMeta, PendingExternalWrites,
        VaultHealthStatus, VaultMeta, estimate_write_storage_delta, select_least_loaded_shard,
    },
};
use crate::{
    event_writer::ApplyPhaseEmitter,
    types::{LedgerRequest, LedgerResponse, SystemRequest},
};

#[allow(clippy::result_large_err)]
impl<B: StorageBackend> RaftLogStore<B> {
    /// Applies a single request and returns the response plus optional vault entry.
    ///
    /// For Write requests, this also returns a VaultEntry that should be included
    /// in the ShardBlock. The caller is responsible for collecting these entries
    /// and creating the ShardBlock.
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
        // Block height for event emission (from shard chain state)
        let block_height = self.shard_chain.read().height + 1;

        match request {
            LedgerRequest::Write {
                organization,
                vault,
                transactions,
                idempotency_key,
                request_hash,
            } => {
                // Check organization status before processing write
                if let Some(org_meta) = state.organizations.get(organization) {
                    match org_meta.status {
                        OrganizationStatus::Suspended => {
                            return (
                                LedgerResponse::Error {
                                    message: format!(
                                        "Organization {} is suspended and not accepting writes",
                                        organization
                                    ),
                                },
                                None,
                            );
                        },
                        OrganizationStatus::Migrating => {
                            return (
                                LedgerResponse::Error {
                                    message: format!(
                                        "Organization {} is migrating and not accepting writes",
                                        organization
                                    ),
                                },
                                None,
                            );
                        },
                        OrganizationStatus::Deleting | OrganizationStatus::Deleted => {
                            return (
                                LedgerResponse::Error {
                                    message: format!(
                                        "Organization {} is deleted and not accepting writes",
                                        organization
                                    ),
                                },
                                None,
                            );
                        },
                        OrganizationStatus::Active => {}, // OK to proceed
                    }
                }

                let key = (*organization, *vault);
                if let Some(VaultHealthStatus::Diverged { .. }) = state.vault_health.get(&key) {
                    return (
                        LedgerResponse::Error {
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
                    // Collect all operations from all transactions
                    let all_ops: Vec<_> =
                        transactions.iter().flat_map(|tx| tx.operations.clone()).collect();

                    // Apply operations (StateLayer is internally thread-safe via
                    // inferadb-ledger-store MVCC)
                    if let Err(e) = state_layer.apply_operations(*vault, &all_ops, new_height) {
                        // On CAS failure, return current state for conflict resolution
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
                                    message: format!("Failed to apply operations: {}", other),
                                },
                                None,
                            ),
                        };
                    }

                    // Compute state root
                    match state_layer.compute_state_root(*vault) {
                        Ok(root) => root,
                        Err(e) => {
                            return (
                                LedgerResponse::Error {
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
                    if let Ok(blob) = encode(vault_meta) {
                        pending.vaults.push((vault_meta.vault_id, blob));
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
                        if let Ok(value) = encode(&ClientSequenceEntry {
                            sequence: new_sequence,
                            last_seen: block_timestamp.timestamp(),
                            last_idempotency_key: *idempotency_key,
                            last_request_hash: *request_hash,
                        }) {
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

                // Build VaultEntry for ShardBlock with server-assigned sequences
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
                crate::metrics::set_organization_storage_bytes(organization.value(), *entry);
                crate::metrics::record_organization_operation(organization.value(), "write");

                // Mirror updated OrganizationMeta (with new storage_bytes) to pending
                if let Some(org_meta) = state.organizations.get_mut(organization) {
                    org_meta.storage_bytes = *entry;
                    if let Ok(blob) = encode(org_meta) {
                        pending.organizations.push((*organization, blob));
                    }
                }

                // Compute block hash from vault entry (for response)
                // We temporarily build a BlockHeader to compute the hash
                let block_hash = self.compute_vault_block_hash(&vault_entry);

                // Emit WriteCommitted event
                let org_slug = state.id_to_slug.get(organization).map(|s| s.value());
                let vault_slug = state.vault_id_to_slug.get(vault).map(|s| s.value());
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

            LedgerRequest::CreateOrganization { name, slug, shard_id, quota } => {
                let organization_id = state.sequences.next_organization();
                // Use provided shard_id or select least-loaded shard
                let assigned_shard =
                    shard_id.unwrap_or_else(|| select_least_loaded_shard(&state.organizations));
                let org_meta = OrganizationMeta {
                    organization_id,
                    slug: *slug,
                    name: name.clone(),
                    shard_id: assigned_shard,
                    status: OrganizationStatus::Active,
                    pending_shard_id: None,
                    quota: quota.clone(),
                    storage_bytes: 0,
                };
                // Mirror to pending external writes
                if let Ok(blob) = encode(&org_meta) {
                    pending.organizations.push((organization_id, blob));
                }
                state.organizations.insert(organization_id, org_meta);

                // Insert into bidirectional slug index
                state.slug_index.insert(*slug, organization_id);
                state.id_to_slug.insert(organization_id, *slug);
                pending.slug_index.push((*slug, organization_id));

                // Persist organization to StateLayer for ShardRouter discovery.
                // This enables the ShardRouter to find the organization->shard mapping.
                if let Some(state_layer) = &self.state_layer {
                    let registry = OrganizationRegistry {
                        organization_id,
                        name: name.clone(),
                        shard_id: assigned_shard,
                        member_nodes: state
                            .membership
                            .membership()
                            .nodes()
                            .map(|(id, _)| id.to_string())
                            .collect(),
                        status: OrganizationStatus::Active,
                        config_version: 1,
                        created_at: chrono::Utc::now(),
                    };

                    // Serialize and write to StateLayer
                    if let Ok(value) = encode(&registry) {
                        let key = SystemKeys::organization_key(organization_id);
                        let slug_index_key = SystemKeys::organization_slug_key(*slug);
                        let ops = vec![
                            Operation::SetEntity { key, value, condition: None, expires_at: None },
                            Operation::SetEntity {
                                key: slug_index_key,
                                value: organization_id.to_string().into_bytes(),
                                condition: None,
                                expires_at: None,
                            },
                        ];

                        if let Err(e) = state_layer.apply_operations(SYSTEM_VAULT_ID, &ops, 0) {
                            tracing::error!(
                                organization_id = organization_id.value(),
                                error = %e,
                                "Failed to persist organization to StateLayer"
                            );
                        }
                    }
                }

                // Emit OrganizationCreated event
                events.push(
                    ApplyPhaseEmitter::for_system(EventAction::OrganizationCreated)
                        .detail("organization_name", name)
                        .detail("organization_id", &organization_id.to_string())
                        .detail("organization_slug", &slug.value().to_string())
                        .detail("shard_id", &assigned_shard.to_string())
                        .outcome(EventOutcome::Success)
                        .build(block_height, *op_index, block_timestamp, ttl_days),
                );
                *op_index += 1;

                (
                    LedgerResponse::OrganizationCreated {
                        organization: organization_id,
                        shard_id: assigned_shard,
                    },
                    None,
                )
            },

            LedgerRequest::CreateVault { organization, slug, name, retention_policy } => {
                // Check organization status before creating vault
                if let Some(org_meta) = state.organizations.get(organization) {
                    match org_meta.status {
                        OrganizationStatus::Suspended => {
                            return (
                                LedgerResponse::Error {
                                    message: format!(
                                        "Organization {} is suspended and not accepting new vaults",
                                        organization
                                    ),
                                },
                                None,
                            );
                        },
                        OrganizationStatus::Migrating => {
                            return (
                                LedgerResponse::Error {
                                    message: format!(
                                        "Organization {} is migrating and not accepting new vaults",
                                        organization
                                    ),
                                },
                                None,
                            );
                        },
                        OrganizationStatus::Deleting | OrganizationStatus::Deleted => {
                            return (
                                LedgerResponse::Error {
                                    message: format!(
                                        "Organization {} is deleted and not accepting new vaults",
                                        organization
                                    ),
                                },
                                None,
                            );
                        },
                        OrganizationStatus::Active => {}, // OK to proceed
                    }
                }

                let vault_id = state.sequences.next_vault();
                let key = (*organization, vault_id);
                state.vault_heights.insert(key, 0);
                pending.vault_heights.push((key, 0));
                state.vault_health.insert(key, VaultHealthStatus::Healthy);
                pending.vault_health.push((key, VaultHealthStatus::Healthy));
                let vault_meta = VaultMeta {
                    organization_id: *organization,
                    vault_id,
                    slug: *slug,
                    name: name.clone(),
                    deleted: false,
                    last_write_timestamp: 0, // No writes yet
                    retention_policy: retention_policy.unwrap_or_default(),
                };
                if let Ok(blob) = encode(&vault_meta) {
                    pending.vaults.push((vault_id, blob));
                }
                state.vaults.insert(key, vault_meta);

                // Insert into bidirectional vault slug index
                state.vault_slug_index.insert(*slug, vault_id);
                state.vault_id_to_slug.insert(vault_id, *slug);
                pending.vault_slug_index.push((*slug, vault_id));

                // Emit VaultCreated event
                let org_slug = state.id_to_slug.get(organization).map(|s| s.value());
                events.push(
                    ApplyPhaseEmitter::for_organization(
                        EventAction::VaultCreated,
                        *organization,
                        org_slug,
                    )
                    .vault(slug.value())
                    .detail("vault_name", name.as_deref().unwrap_or(""))
                    .outcome(EventOutcome::Success)
                    .build(block_height, *op_index, block_timestamp, ttl_days),
                );
                *op_index += 1;

                (LedgerResponse::VaultCreated { vault: vault_id, slug: *slug }, None)
            },

            LedgerRequest::DeleteOrganization { organization } => {
                // Capture slug before deletion (may be removed from index)
                let org_slug = state.id_to_slug.get(organization).map(|s| s.value());
                // Count active vaults at deletion time (for audit context)
                let active_vault_count = state
                    .vaults
                    .iter()
                    .filter(|((org, _), v)| *org == *organization && !v.deleted)
                    .count();
                // Collect active (non-deleted) vault IDs for this organization
                let blocking_vault_ids: Vec<VaultId> = state
                    .vaults
                    .iter()
                    .filter(|((org, _), v)| *org == *organization && !v.deleted)
                    .map(|((_, vault_id), _)| *vault_id)
                    .collect();

                let response = if let Some(org) = state.organizations.get_mut(organization) {
                    // Check current status
                    match org.status {
                        OrganizationStatus::Deleted => LedgerResponse::Error {
                            message: format!("Organization {} is already deleted", organization),
                        },
                        OrganizationStatus::Deleting => {
                            // Already deleting - check if vaults are gone now
                            if blocking_vault_ids.is_empty() {
                                org.status = OrganizationStatus::Deleted;
                                // Re-serialize after in-place mutation
                                if let Ok(blob) = encode(org) {
                                    pending.organizations.push((*organization, blob));
                                }
                                // Clean up slug index on final deletion
                                if let Some(slug) = state.id_to_slug.remove(organization) {
                                    state.slug_index.remove(&slug);
                                    pending.slug_index_deleted.push(slug);
                                }
                                LedgerResponse::OrganizationDeleted {
                                    success: true,
                                    blocking_vault_ids: vec![],
                                }
                            } else {
                                // Still has vaults
                                LedgerResponse::OrganizationDeleting {
                                    organization: *organization,
                                    blocking_vault_ids,
                                }
                            }
                        },
                        _ => {
                            // Active, Suspended, or Migrating
                            if blocking_vault_ids.is_empty() {
                                // No blocking vaults - delete immediately
                                org.status = OrganizationStatus::Deleted;
                                // Re-serialize after in-place mutation
                                if let Ok(blob) = encode(org) {
                                    pending.organizations.push((*organization, blob));
                                }
                                // Clean up slug index on final deletion
                                if let Some(slug) = state.id_to_slug.remove(organization) {
                                    state.slug_index.remove(&slug);
                                    pending.slug_index_deleted.push(slug);
                                }
                                LedgerResponse::OrganizationDeleted {
                                    success: true,
                                    blocking_vault_ids: vec![],
                                }
                            } else {
                                // Has vaults - transition to Deleting state
                                org.status = OrganizationStatus::Deleting;
                                // Re-serialize after in-place mutation
                                if let Ok(blob) = encode(org) {
                                    pending.organizations.push((*organization, blob));
                                }
                                LedgerResponse::OrganizationDeleting {
                                    organization: *organization,
                                    blocking_vault_ids,
                                }
                            }
                        },
                    }
                } else {
                    LedgerResponse::Error {
                        message: format!("Organization {} not found", organization),
                    }
                };

                // Emit OrganizationDeleted event on successful deletion
                if matches!(response, LedgerResponse::OrganizationDeleted { success: true, .. }) {
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
                    if let Ok(blob) = encode(vault_meta) {
                        pending.vaults.push((vault_meta.vault_id, blob));
                    }

                    // Clean up vault slug index
                    if let Some(slug) = state.vault_id_to_slug.remove(vault) {
                        state.vault_slug_index.remove(&slug);
                        pending.vault_slug_index_deleted.push(slug);
                    }

                    // Check if organization is in Deleting state and this was the last vault
                    if let Some(org) = state.organizations.get_mut(organization)
                        && org.status == OrganizationStatus::Deleting
                    {
                        // Check if any active vaults remain
                        let remaining_vaults = state
                            .vaults
                            .iter()
                            .any(|((org_id, _), v)| *org_id == *organization && !v.deleted);

                        if !remaining_vaults {
                            // Auto-transition organization to Deleted
                            org.status = OrganizationStatus::Deleted;
                            // Re-serialize org meta after in-place mutation
                            if let Ok(blob) = encode(org) {
                                pending.organizations.push((*organization, blob));
                            }
                            // Clean up slug index
                            if let Some(slug) = state.id_to_slug.remove(organization) {
                                state.slug_index.remove(&slug);
                                pending.slug_index_deleted.push(slug);
                            }
                            tracing::info!(
                                organization_id = organization.value(),
                                "Organization auto-transitioned to Deleted after last vault deleted"
                            );
                        }
                    }

                    LedgerResponse::VaultDeleted { success: true }
                } else {
                    LedgerResponse::Error {
                        message: format!("Vault {}:{} not found", organization, vault),
                    }
                };

                // Emit VaultDeleted event on successful deletion
                if matches!(response, LedgerResponse::VaultDeleted { success: true }) {
                    let org_slug = state.id_to_slug.get(organization).map(|s| s.value());
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

            LedgerRequest::SuspendOrganization { organization, reason } => {
                let org_slug = state.id_to_slug.get(organization).map(|s| s.value());
                let response = if let Some(org) = state.organizations.get_mut(organization) {
                    match org.status {
                        OrganizationStatus::Deleted => LedgerResponse::Error {
                            message: format!(
                                "Cannot suspend deleted organization {}",
                                organization
                            ),
                        },
                        OrganizationStatus::Suspended => LedgerResponse::Error {
                            message: format!("Organization {} is already suspended", organization),
                        },
                        _ => {
                            org.status = OrganizationStatus::Suspended;
                            if let Ok(blob) = encode(org) {
                                pending.organizations.push((*organization, blob));
                            }
                            LedgerResponse::OrganizationSuspended { organization: *organization }
                        },
                    }
                } else {
                    LedgerResponse::Error {
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
                let org_slug = state.id_to_slug.get(organization).map(|s| s.value());
                let response = if let Some(org) = state.organizations.get_mut(organization) {
                    match org.status {
                        OrganizationStatus::Suspended => {
                            org.status = OrganizationStatus::Active;
                            if let Ok(blob) = encode(org) {
                                pending.organizations.push((*organization, blob));
                            }
                            LedgerResponse::OrganizationResumed { organization: *organization }
                        },
                        OrganizationStatus::Active => LedgerResponse::Error {
                            message: format!("Organization {} is not suspended", organization),
                        },
                        OrganizationStatus::Deleted => LedgerResponse::Error {
                            message: format!("Cannot resume deleted organization {}", organization),
                        },
                        other => LedgerResponse::Error {
                            message: format!(
                                "Cannot resume organization {} in state {:?}",
                                organization, other
                            ),
                        },
                    }
                } else {
                    LedgerResponse::Error {
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

            LedgerRequest::StartMigration { organization, target_shard_id } => {
                let response = if let Some(org) = state.organizations.get_mut(organization) {
                    match org.status {
                        OrganizationStatus::Active => {
                            // Validate target shard is different
                            if *target_shard_id == org.shard_id {
                                LedgerResponse::Error {
                                    message: format!(
                                        "Organization {} is already on shard {}",
                                        organization, target_shard_id
                                    ),
                                }
                            } else {
                                org.status = OrganizationStatus::Migrating;
                                org.pending_shard_id = Some(*target_shard_id);
                                if let Ok(blob) = encode(org) {
                                    pending.organizations.push((*organization, blob));
                                }
                                LedgerResponse::MigrationStarted {
                                    organization: *organization,
                                    target_shard_id: *target_shard_id,
                                }
                            }
                        },
                        OrganizationStatus::Migrating => LedgerResponse::Error {
                            message: format!("Organization {} is already migrating", organization),
                        },
                        OrganizationStatus::Suspended => LedgerResponse::Error {
                            message: format!(
                                "Cannot start migration on suspended organization {}",
                                organization
                            ),
                        },
                        OrganizationStatus::Deleting | OrganizationStatus::Deleted => {
                            LedgerResponse::Error {
                                message: format!(
                                    "Cannot start migration on deleted organization {}",
                                    organization
                                ),
                            }
                        },
                    }
                } else {
                    LedgerResponse::Error {
                        message: format!("Organization {} not found", organization),
                    }
                };

                // Emit MigrationStarted event on success
                if matches!(response, LedgerResponse::MigrationStarted { .. }) {
                    events.push(
                        ApplyPhaseEmitter::for_system(EventAction::MigrationStarted)
                            .detail("organization_id", &organization.to_string())
                            .detail("target_shard_id", &target_shard_id.to_string())
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
                            if let Some(target_shard) = org.pending_shard_id {
                                let old_shard = org.shard_id;
                                org.shard_id = target_shard;
                                org.status = OrganizationStatus::Active;
                                org.pending_shard_id = None;
                                if let Ok(blob) = encode(org) {
                                    pending.organizations.push((*organization, blob));
                                }
                                LedgerResponse::MigrationCompleted {
                                    organization: *organization,
                                    old_shard_id: old_shard,
                                    new_shard_id: target_shard,
                                }
                            } else {
                                // Should not happen, but handle gracefully
                                LedgerResponse::Error {
                                    message: format!(
                                        "Organization {} is migrating but has no target shard",
                                        organization
                                    ),
                                }
                            }
                        },
                        OrganizationStatus::Active => LedgerResponse::Error {
                            message: format!("Organization {} is not migrating", organization),
                        },
                        other => LedgerResponse::Error {
                            message: format!(
                                "Cannot complete migration for organization {} in state {:?}",
                                organization, other
                            ),
                        },
                    }
                } else {
                    LedgerResponse::Error {
                        message: format!("Organization {} not found", organization),
                    }
                };

                // Emit MigrationCompleted event on success
                if let LedgerResponse::MigrationCompleted { new_shard_id, .. } = &response {
                    events.push(
                        ApplyPhaseEmitter::for_system(EventAction::MigrationCompleted)
                            .detail("organization_id", &organization.to_string())
                            .detail("shard_id", &new_shard_id.to_string())
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
                    state.vault_health.insert(key, status.clone());
                    pending.vault_health.push((key, status));
                    crate::metrics::set_vault_health(
                        organization.value(),
                        vault.value(),
                        "healthy",
                    );
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
                    state.vault_health.insert(key, status.clone());
                    pending.vault_health.push((key, status));
                    crate::metrics::set_vault_health(
                        organization.value(),
                        vault.value(),
                        "recovering",
                    );
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
                    state.vault_health.insert(key, status.clone());
                    pending.vault_health.push((key, status));
                    crate::metrics::set_vault_health(
                        organization.value(),
                        vault.value(),
                        "diverged",
                    );
                    tracing::warn!(
                        organization_id = organization.value(),
                        vault_id = vault.value(),
                        at_height,
                        "Vault health updated to Diverged via Raft"
                    );
                }
                // Emit VaultHealthUpdated event
                let org_slug = state.id_to_slug.get(organization).map(|s| s.value());
                let vault_slug = state.vault_id_to_slug.get(vault).map(|s| s.value());
                let health_status = if *healthy {
                    "healthy"
                } else if recovery_attempt.is_some() && recovery_started_at.is_some() {
                    "recovering"
                } else {
                    "diverged"
                };
                let mut emitter = ApplyPhaseEmitter::for_organization(
                    EventAction::VaultHealthUpdated,
                    *organization,
                    org_slug,
                )
                .detail("health_status", health_status)
                .outcome(EventOutcome::Success);
                if let Some(vs) = vault_slug {
                    emitter = emitter.vault(vs);
                }
                events.push(emitter.build(block_height, *op_index, block_timestamp, ttl_days));
                *op_index += 1;

                (LedgerResponse::VaultHealthUpdated { success: true }, None)
            },

            LedgerRequest::System(system_request) => {
                let response = match system_request {
                    SystemRequest::CreateUser { .. } => {
                        let user_id = state.sequences.next_user();
                        LedgerResponse::UserCreated { user_id }
                    },
                    SystemRequest::AddNode { .. } | SystemRequest::RemoveNode { .. } => {
                        LedgerResponse::Empty
                    },
                    SystemRequest::UpdateOrganizationRouting { organization, shard_id } => {
                        // Validate shard_id is non-negative
                        if *shard_id < 0 {
                            LedgerResponse::Error {
                                message: format!(
                                    "Invalid shard_id: {} (must be non-negative)",
                                    shard_id
                                ),
                            }
                        } else if let Some(org) = state.organizations.get_mut(organization) {
                            if org.status == OrganizationStatus::Deleted {
                                LedgerResponse::Error {
                                    message: format!(
                                        "Cannot migrate deleted organization {}",
                                        organization
                                    ),
                                }
                            } else {
                                let old_shard_id = org.shard_id;
                                // Safe cast: we already validated shard_id >= 0 above
                                #[allow(clippy::cast_sign_loss)]
                                let new_shard_id =
                                    inferadb_ledger_types::ShardId::new(*shard_id as u32);
                                org.shard_id = new_shard_id;
                                if let Ok(blob) = encode(org) {
                                    pending.organizations.push((*organization, blob));
                                }
                                LedgerResponse::OrganizationMigrated {
                                    organization: *organization,
                                    old_shard_id,
                                    new_shard_id,
                                }
                            }
                        } else {
                            LedgerResponse::Error {
                                message: format!("Organization {} not found", organization),
                            }
                        }
                    },
                };

                // Emit events for system request variants
                match (&response, system_request) {
                    (
                        LedgerResponse::UserCreated { user_id },
                        SystemRequest::CreateUser { name, email, admin },
                    ) => {
                        events.push(
                            ApplyPhaseEmitter::for_system(EventAction::UserCreated)
                                .principal("system")
                                .detail("user_id", &user_id.to_string())
                                .detail("name", name)
                                .detail("email", email)
                                .detail("admin", &admin.to_string())
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
                        LedgerResponse::OrganizationMigrated { new_shard_id, .. },
                        SystemRequest::UpdateOrganizationRouting { organization, .. },
                    ) => {
                        events.push(
                            ApplyPhaseEmitter::for_system(EventAction::RoutingUpdated)
                                .detail("organization_id", &organization.to_string())
                                .detail("shard_id", &new_shard_id.to_string())
                                .outcome(EventOutcome::Success)
                                .build(block_height, *op_index, block_timestamp, ttl_days),
                        );
                        *op_index += 1;
                    },
                    _ => {},
                }

                (response, None)
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
                    let org_slug = state.id_to_slug.get(&ve.organization).map(|s| s.value());
                    let vault_slug = state.vault_id_to_slug.get(&ve.vault).map(|s| s.value());
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
        organization_id: OrganizationId,
        vault_id: VaultId,
        height: u64,
    ) -> Hash {
        use inferadb_ledger_types::sha256;
        let mut data = Vec::new();
        data.extend_from_slice(&organization_id.value().to_le_bytes());
        data.extend_from_slice(&vault_id.value().to_le_bytes());
        data.extend_from_slice(&height.to_le_bytes());
        sha256(&data)
    }
}
