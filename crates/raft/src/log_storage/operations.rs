//! State machine apply logic for Raft log entries.
//!
//! Transforms committed log entries into state mutations via the storage engine.

use inferadb_ledger_state::{
    StateError,
    system::{OrganizationRegistry, OrganizationStatus, SYSTEM_VAULT_ID, SystemKeys},
};
use inferadb_ledger_store::StorageBackend;
use inferadb_ledger_types::{
    Hash, Operation, OrganizationId, VaultEntry, VaultId, compute_tx_merkle_root, encode,
};

use super::{
    store::RaftLogStore,
    types::{
        AppliedState, OrganizationMeta, VaultHealthStatus, VaultMeta, estimate_write_storage_delta,
        select_least_loaded_shard,
    },
};
use crate::types::{LedgerRequest, LedgerResponse, SystemRequest};

#[allow(clippy::result_large_err)]
impl<B: StorageBackend> RaftLogStore<B> {
    /// Applies a single request and returns the response plus optional vault entry.
    ///
    /// For Write requests, this also returns a VaultEntry that should be included
    /// in the ShardBlock. The caller is responsible for collecting these entries
    /// and creating the ShardBlock.
    pub(super) fn apply_request(
        &self,
        request: &LedgerRequest,
        state: &mut AppliedState,
    ) -> (LedgerResponse, Option<VaultEntry>) {
        match request {
            LedgerRequest::Write { organization_id, vault_id, transactions } => {
                // Check organization status before processing write
                if let Some(org_meta) = state.organizations.get(organization_id) {
                    match org_meta.status {
                        OrganizationStatus::Suspended => {
                            return (
                                LedgerResponse::Error {
                                    message: format!(
                                        "Organization {} is suspended and not accepting writes",
                                        organization_id
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
                                        organization_id
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
                                        organization_id
                                    ),
                                },
                                None,
                            );
                        },
                        OrganizationStatus::Active => {}, // OK to proceed
                    }
                }

                let key = (*organization_id, *vault_id);
                if let Some(VaultHealthStatus::Diverged { .. }) = state.vault_health.get(&key) {
                    return (
                        LedgerResponse::Error {
                            message: format!(
                                "Vault {}:{} is diverged and not accepting writes",
                                organization_id, vault_id
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
                    if let Err(e) = state_layer.apply_operations(*vault_id, &all_ops, new_height) {
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
                    match state_layer.compute_state_root(*vault_id) {
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

                // Update last write timestamp from latest transaction (deterministic)
                if let Some(last_tx) = transactions.last()
                    && let Some(vault_meta) = state.vaults.get_mut(&key)
                {
                    vault_meta.last_write_timestamp = last_tx.timestamp.timestamp() as u64;
                }

                // Server-assigned sequences: assign monotonic sequence to each transaction.
                // Each write request typically contains a single transaction from a single client.
                // The assigned_sequence returned is the sequence assigned to the first transaction.
                let mut assigned_sequence = 0u64;
                let transactions_with_sequences: Vec<_> = transactions
                    .iter()
                    .map(|tx| {
                        let client_key = (*organization_id, *vault_id, tx.client_id.clone());
                        let current = state.client_sequences.get(&client_key).copied().unwrap_or(0);
                        let new_sequence = current + 1;
                        state.client_sequences.insert(client_key, new_sequence);

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
                    organization_id: *organization_id,
                    vault_id: *vault_id,
                    vault_height: new_height,
                    previous_vault_hash,
                    transactions: transactions_with_sequences,
                    tx_merkle_root,
                    state_root,
                };

                // Update organization storage accounting.
                // Increment for sets/creates, decrement for deletes.
                let storage_delta = estimate_write_storage_delta(transactions);
                let entry = state.organization_storage_bytes.entry(*organization_id).or_insert(0);
                if storage_delta >= 0 {
                    *entry = entry.saturating_add(storage_delta as u64);
                } else {
                    *entry = entry.saturating_sub(storage_delta.unsigned_abs());
                }
                crate::metrics::set_organization_storage_bytes(organization_id.value(), *entry);
                crate::metrics::record_organization_operation(organization_id.value(), "write");

                // Compute block hash from vault entry (for response)
                // We temporarily build a BlockHeader to compute the hash
                let block_hash = self.compute_vault_block_hash(&vault_entry);

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
                state.organizations.insert(
                    organization_id,
                    OrganizationMeta {
                        organization_id,
                        slug: *slug,
                        name: name.clone(),
                        shard_id: assigned_shard,
                        status: OrganizationStatus::Active,
                        pending_shard_id: None,
                        quota: quota.clone(),
                    },
                );

                // Insert into bidirectional slug index
                state.slug_index.insert(*slug, organization_id);
                state.id_to_slug.insert(organization_id, *slug);

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

                (
                    LedgerResponse::OrganizationCreated {
                        organization_id,
                        shard_id: assigned_shard,
                    },
                    None,
                )
            },

            LedgerRequest::CreateVault { organization_id, name, retention_policy } => {
                // Check organization status before creating vault
                if let Some(org_meta) = state.organizations.get(organization_id) {
                    match org_meta.status {
                        OrganizationStatus::Suspended => {
                            return (
                                LedgerResponse::Error {
                                    message: format!(
                                        "Organization {} is suspended and not accepting new vaults",
                                        organization_id
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
                                        organization_id
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
                                        organization_id
                                    ),
                                },
                                None,
                            );
                        },
                        OrganizationStatus::Active => {}, // OK to proceed
                    }
                }

                let vault_id = state.sequences.next_vault();
                let key = (*organization_id, vault_id);
                state.vault_heights.insert(key, 0);
                state.vault_health.insert(key, VaultHealthStatus::Healthy);
                state.vaults.insert(
                    key,
                    VaultMeta {
                        organization_id: *organization_id,
                        vault_id,
                        name: name.clone(),
                        deleted: false,
                        last_write_timestamp: 0, // No writes yet
                        retention_policy: retention_policy.unwrap_or_default(),
                    },
                );
                (LedgerResponse::VaultCreated { vault_id }, None)
            },

            LedgerRequest::DeleteOrganization { organization_id } => {
                // Collect active (non-deleted) vault IDs for this organization
                let blocking_vault_ids: Vec<VaultId> = state
                    .vaults
                    .iter()
                    .filter(|((org, _), v)| *org == *organization_id && !v.deleted)
                    .map(|((_, vault_id), _)| *vault_id)
                    .collect();

                let response = if let Some(org) = state.organizations.get_mut(organization_id) {
                    // Check current status
                    match org.status {
                        OrganizationStatus::Deleted => LedgerResponse::Error {
                            message: format!("Organization {} is already deleted", organization_id),
                        },
                        OrganizationStatus::Deleting => {
                            // Already deleting - check if vaults are gone now
                            if blocking_vault_ids.is_empty() {
                                org.status = OrganizationStatus::Deleted;
                                // Clean up slug index on final deletion
                                if let Some(slug) = state.id_to_slug.remove(organization_id) {
                                    state.slug_index.remove(&slug);
                                }
                                LedgerResponse::OrganizationDeleted {
                                    success: true,
                                    blocking_vault_ids: vec![],
                                }
                            } else {
                                // Still has vaults
                                LedgerResponse::OrganizationDeleting {
                                    organization_id: *organization_id,
                                    blocking_vault_ids,
                                }
                            }
                        },
                        _ => {
                            // Active, Suspended, or Migrating
                            if blocking_vault_ids.is_empty() {
                                // No blocking vaults - delete immediately
                                org.status = OrganizationStatus::Deleted;
                                // Clean up slug index on final deletion
                                if let Some(slug) = state.id_to_slug.remove(organization_id) {
                                    state.slug_index.remove(&slug);
                                }
                                LedgerResponse::OrganizationDeleted {
                                    success: true,
                                    blocking_vault_ids: vec![],
                                }
                            } else {
                                // Has vaults - transition to Deleting state
                                org.status = OrganizationStatus::Deleting;
                                LedgerResponse::OrganizationDeleting {
                                    organization_id: *organization_id,
                                    blocking_vault_ids,
                                }
                            }
                        },
                    }
                } else {
                    LedgerResponse::Error {
                        message: format!("Organization {} not found", organization_id),
                    }
                };
                (response, None)
            },

            LedgerRequest::DeleteVault { organization_id, vault_id } => {
                let key = (*organization_id, *vault_id);
                // Mark vault as deleted (keep heights for historical queries)
                let response = if let Some(vault) = state.vaults.get_mut(&key) {
                    vault.deleted = true;

                    // Check if organization is in Deleting state and this was the last vault
                    if let Some(org) = state.organizations.get_mut(organization_id)
                        && org.status == OrganizationStatus::Deleting
                    {
                        // Check if any active vaults remain
                        let remaining_vaults = state
                            .vaults
                            .iter()
                            .any(|((org_id, _), v)| *org_id == *organization_id && !v.deleted);

                        if !remaining_vaults {
                            // Auto-transition organization to Deleted
                            org.status = OrganizationStatus::Deleted;
                            // Clean up slug index
                            if let Some(slug) = state.id_to_slug.remove(organization_id) {
                                state.slug_index.remove(&slug);
                            }
                            tracing::info!(
                                organization_id = organization_id.value(),
                                "Organization auto-transitioned to Deleted after last vault deleted"
                            );
                        }
                    }

                    LedgerResponse::VaultDeleted { success: true }
                } else {
                    LedgerResponse::Error {
                        message: format!("Vault {}:{} not found", organization_id, vault_id),
                    }
                };
                (response, None)
            },

            LedgerRequest::SuspendOrganization { organization_id, reason: _ } => {
                let response = if let Some(org) = state.organizations.get_mut(organization_id) {
                    match org.status {
                        OrganizationStatus::Deleted => LedgerResponse::Error {
                            message: format!(
                                "Cannot suspend deleted organization {}",
                                organization_id
                            ),
                        },
                        OrganizationStatus::Suspended => LedgerResponse::Error {
                            message: format!(
                                "Organization {} is already suspended",
                                organization_id
                            ),
                        },
                        _ => {
                            org.status = OrganizationStatus::Suspended;
                            LedgerResponse::OrganizationSuspended {
                                organization_id: *organization_id,
                            }
                        },
                    }
                } else {
                    LedgerResponse::Error {
                        message: format!("Organization {} not found", organization_id),
                    }
                };
                (response, None)
            },

            LedgerRequest::ResumeOrganization { organization_id } => {
                let response = if let Some(org) = state.organizations.get_mut(organization_id) {
                    match org.status {
                        OrganizationStatus::Suspended => {
                            org.status = OrganizationStatus::Active;
                            LedgerResponse::OrganizationResumed {
                                organization_id: *organization_id,
                            }
                        },
                        OrganizationStatus::Active => LedgerResponse::Error {
                            message: format!("Organization {} is not suspended", organization_id),
                        },
                        OrganizationStatus::Deleted => LedgerResponse::Error {
                            message: format!(
                                "Cannot resume deleted organization {}",
                                organization_id
                            ),
                        },
                        other => LedgerResponse::Error {
                            message: format!(
                                "Cannot resume organization {} in state {:?}",
                                organization_id, other
                            ),
                        },
                    }
                } else {
                    LedgerResponse::Error {
                        message: format!("Organization {} not found", organization_id),
                    }
                };
                (response, None)
            },

            LedgerRequest::StartMigration { organization_id, target_shard_id } => {
                let response = if let Some(org) = state.organizations.get_mut(organization_id) {
                    match org.status {
                        OrganizationStatus::Active => {
                            // Validate target shard is different
                            if *target_shard_id == org.shard_id {
                                LedgerResponse::Error {
                                    message: format!(
                                        "Organization {} is already on shard {}",
                                        organization_id, target_shard_id
                                    ),
                                }
                            } else {
                                org.status = OrganizationStatus::Migrating;
                                org.pending_shard_id = Some(*target_shard_id);
                                LedgerResponse::MigrationStarted {
                                    organization_id: *organization_id,
                                    target_shard_id: *target_shard_id,
                                }
                            }
                        },
                        OrganizationStatus::Migrating => LedgerResponse::Error {
                            message: format!(
                                "Organization {} is already migrating",
                                organization_id
                            ),
                        },
                        OrganizationStatus::Suspended => LedgerResponse::Error {
                            message: format!(
                                "Cannot start migration on suspended organization {}",
                                organization_id
                            ),
                        },
                        OrganizationStatus::Deleting | OrganizationStatus::Deleted => {
                            LedgerResponse::Error {
                                message: format!(
                                    "Cannot start migration on deleted organization {}",
                                    organization_id
                                ),
                            }
                        },
                    }
                } else {
                    LedgerResponse::Error {
                        message: format!("Organization {} not found", organization_id),
                    }
                };
                (response, None)
            },

            LedgerRequest::CompleteMigration { organization_id } => {
                let response = if let Some(org) = state.organizations.get_mut(organization_id) {
                    match org.status {
                        OrganizationStatus::Migrating => {
                            if let Some(target_shard) = org.pending_shard_id {
                                let old_shard = org.shard_id;
                                org.shard_id = target_shard;
                                org.status = OrganizationStatus::Active;
                                org.pending_shard_id = None;
                                LedgerResponse::MigrationCompleted {
                                    organization_id: *organization_id,
                                    old_shard_id: old_shard,
                                    new_shard_id: target_shard,
                                }
                            } else {
                                // Should not happen, but handle gracefully
                                LedgerResponse::Error {
                                    message: format!(
                                        "Organization {} is migrating but has no target shard",
                                        organization_id
                                    ),
                                }
                            }
                        },
                        OrganizationStatus::Active => LedgerResponse::Error {
                            message: format!("Organization {} is not migrating", organization_id),
                        },
                        other => LedgerResponse::Error {
                            message: format!(
                                "Cannot complete migration for organization {} in state {:?}",
                                organization_id, other
                            ),
                        },
                    }
                } else {
                    LedgerResponse::Error {
                        message: format!("Organization {} not found", organization_id),
                    }
                };
                (response, None)
            },

            LedgerRequest::UpdateVaultHealth {
                organization_id,
                vault_id,
                healthy,
                expected_root,
                computed_root,
                diverged_at_height,
                recovery_attempt,
                recovery_started_at,
            } => {
                let key = (*organization_id, *vault_id);
                if *healthy {
                    // Mark vault as healthy
                    state.vault_health.insert(key, VaultHealthStatus::Healthy);
                    crate::metrics::set_vault_health(
                        organization_id.value(),
                        vault_id.value(),
                        "healthy",
                    );
                    tracing::info!(
                        organization_id = organization_id.value(),
                        vault_id = vault_id.value(),
                        "Vault health updated to Healthy via Raft"
                    );
                } else if let (Some(attempt), Some(started_at)) =
                    (recovery_attempt, recovery_started_at)
                {
                    // Mark vault as recovering
                    state.vault_health.insert(
                        key,
                        VaultHealthStatus::Recovering {
                            started_at: *started_at,
                            attempt: *attempt,
                        },
                    );
                    crate::metrics::set_vault_health(
                        organization_id.value(),
                        vault_id.value(),
                        "recovering",
                    );
                    tracing::info!(
                        organization_id = organization_id.value(),
                        vault_id = vault_id.value(),
                        attempt,
                        "Vault health updated to Recovering via Raft"
                    );
                } else {
                    // Mark vault as diverged
                    let expected = expected_root.unwrap_or(inferadb_ledger_types::ZERO_HASH);
                    let computed = computed_root.unwrap_or(inferadb_ledger_types::ZERO_HASH);
                    let at_height = diverged_at_height.unwrap_or(0);
                    state
                        .vault_health
                        .insert(key, VaultHealthStatus::Diverged { expected, computed, at_height });
                    crate::metrics::set_vault_health(
                        organization_id.value(),
                        vault_id.value(),
                        "diverged",
                    );
                    tracing::warn!(
                        organization_id = organization_id.value(),
                        vault_id = vault_id.value(),
                        at_height,
                        "Vault health updated to Diverged via Raft"
                    );
                }
                (LedgerResponse::VaultHealthUpdated { success: true }, None)
            },

            LedgerRequest::System(system_request) => {
                let response = match system_request {
                    SystemRequest::CreateUser { name: _, email: _ } => {
                        let user_id = state.sequences.next_user();
                        LedgerResponse::UserCreated { user_id }
                    },
                    SystemRequest::AddNode { .. } | SystemRequest::RemoveNode { .. } => {
                        LedgerResponse::Empty
                    },
                    SystemRequest::UpdateOrganizationRouting { organization_id, shard_id } => {
                        // Validate shard_id is non-negative
                        if *shard_id < 0 {
                            LedgerResponse::Error {
                                message: format!(
                                    "Invalid shard_id: {} (must be non-negative)",
                                    shard_id
                                ),
                            }
                        } else if let Some(org) = state.organizations.get_mut(organization_id) {
                            if org.status == OrganizationStatus::Deleted {
                                LedgerResponse::Error {
                                    message: format!(
                                        "Cannot migrate deleted organization {}",
                                        organization_id
                                    ),
                                }
                            } else {
                                let old_shard_id = org.shard_id;
                                // Safe cast: we already validated shard_id >= 0 above
                                #[allow(clippy::cast_sign_loss)]
                                let new_shard_id =
                                    inferadb_ledger_types::ShardId::new(*shard_id as u32);
                                org.shard_id = new_shard_id;
                                LedgerResponse::OrganizationMigrated {
                                    organization_id: *organization_id,
                                    old_shard_id,
                                    new_shard_id,
                                }
                            }
                        } else {
                            LedgerResponse::Error {
                                message: format!("Organization {} not found", organization_id),
                            }
                        }
                    },
                };
                (response, None)
            },

            LedgerRequest::BatchWrite { requests } => {
                // Process each request in the batch sequentially, collecting responses.
                // Vault entries are collected and the last one is returned (batches typically
                // target the same vault, so the final block includes all transactions).
                let mut responses = Vec::with_capacity(requests.len());
                let mut last_vault_entry = None;

                for inner_request in requests {
                    let (response, vault_entry) = self.apply_request(inner_request, state);
                    responses.push(response);
                    if vault_entry.is_some() {
                        last_vault_entry = vault_entry;
                    }
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
