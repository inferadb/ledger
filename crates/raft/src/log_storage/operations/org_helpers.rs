//! Organization domain helpers for state machine apply logic.

use std::sync::Arc;

use inferadb_ledger_state::system::{
    KeyTier, OrganizationProfile, OrganizationStatus, SYSTEM_VAULT_ID, SystemKeys,
};
use inferadb_ledger_store::StorageBackend;
use inferadb_ledger_types::{ErrorCode, Operation, OrganizationId, decode, encode};

use super::{
    super::types::AppliedState,
    helpers::{check_tier, ledger_error},
};
use crate::types::LedgerResponse;

/// Loads and decodes an `Organization` skeleton from the GLOBAL state layer.
pub(crate) fn load_organization<B: StorageBackend>(
    state_layer: &inferadb_ledger_state::StateLayer<B>,
    organization: OrganizationId,
) -> Result<inferadb_ledger_state::system::Organization, LedgerResponse> {
    let key = SystemKeys::organization_key(organization);
    let entity = state_layer
        .get_entity(SYSTEM_VAULT_ID, key.as_bytes())
        .map_err(|e| {
            ledger_error(ErrorCode::Internal, format!("Failed to read organization: {e}"))
        })?
        .ok_or_else(|| {
            ledger_error(ErrorCode::NotFound, format!("Organization not found for {organization}"))
        })?;
    decode::<inferadb_ledger_state::system::Organization>(&entity.value).map_err(|e| {
        ledger_error(ErrorCode::Internal, format!("Failed to decode organization: {e}"))
    })
}

/// Encodes and writes an `Organization` skeleton back to the GLOBAL state layer.
///
/// The skeleton must have an empty `name` — PII lives in the REGIONAL
/// `OrganizationProfile` overlay. A non-empty name here would leak PII to GLOBAL.
pub(crate) fn save_organization<B: StorageBackend>(
    state_layer: &inferadb_ledger_state::StateLayer<B>,
    organization: OrganizationId,
    org: &inferadb_ledger_state::system::Organization,
) -> Result<(), LedgerResponse> {
    if !org.name.is_empty() {
        return Err(ledger_error(
            ErrorCode::Internal,
            format!(
                "BUG: Organization skeleton must not carry a name to GLOBAL (org={})",
                organization
            ),
        ));
    }
    let key = SystemKeys::organization_key(organization);
    check_tier(&key, KeyTier::Global)?;
    let bytes = encode(org).map_err(|e| {
        ledger_error(ErrorCode::Internal, format!("Failed to encode organization: {e}"))
    })?;
    let ops = vec![Operation::SetEntity { key, value: bytes, condition: None, expires_at: None }];
    state_layer.apply_operations(SYSTEM_VAULT_ID, &ops, 0).map(|_| ()).map_err(|e| {
        ledger_error(ErrorCode::Internal, format!("Failed to write organization: {e}"))
    })
}

/// Encodes and writes a slimmed `OrganizationProfile` (PII only) to the REGIONAL state layer.
pub(crate) fn save_org_profile<B: StorageBackend>(
    state_layer: &inferadb_ledger_state::StateLayer<B>,
    organization: OrganizationId,
    profile: &OrganizationProfile,
) -> Result<(), LedgerResponse> {
    let key = SystemKeys::organization_profile_key(organization);
    check_tier(&key, KeyTier::Regional)?;
    let bytes = encode(profile).map_err(|e| {
        ledger_error(ErrorCode::Internal, format!("Failed to encode organization profile: {e}"))
    })?;
    let ops = vec![Operation::SetEntity { key, value: bytes, condition: None, expires_at: None }];
    state_layer.apply_operations(SYSTEM_VAULT_ID, &ops, 0).map(|_| ()).map_err(|e| {
        ledger_error(ErrorCode::Internal, format!("Failed to write organization profile: {e}"))
    })
}

/// Validates that an organization exists, is not deleted, and returns the state layer.
///
/// Consolidates the three-step guard (org exists -> not deleted -> state layer available)
/// that repeats across most organization-scoped operations. Returns a reference to the
/// state layer on success.
///
/// The `action` parameter is used in error messages (e.g., "modify", "create vaults in").
pub(crate) fn require_active_org_with_state<'a, B: StorageBackend>(
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
///
/// In multi-Raft mode, data regions don't have organization metadata — only
/// the GLOBAL region does. When the org isn't found in the local state, we
/// allow the write to proceed (the service layer already validated the org
/// exists and is Active using GLOBAL state). When the org IS found locally
/// (GLOBAL region), we enforce the status check.
pub(crate) fn require_fully_active_org(
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
        // Organization not in local state — this is expected in data regions
        // where only the GLOBAL region has org metadata. The service layer
        // validated the org before routing the proposal here.
        Ok(())
    }
}
