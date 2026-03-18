//! App domain helpers for state machine apply logic.

use inferadb_ledger_state::system::{App, AppProfile, KeyTier, SYSTEM_VAULT_ID, SystemKeys};
use inferadb_ledger_store::StorageBackend;
use inferadb_ledger_types::{AppId, ErrorCode, Operation, OrganizationId, decode, encode};

use super::{
    super::types::AppliedState,
    helpers::{check_tier, ledger_error},
};
use crate::types::LedgerResponse;

/// Checks whether an app name already exists within an organization.
///
/// Uses the in-memory `app_name_index` for O(1) lookups.
/// The `exclude_app` parameter allows renaming an app to the same name.
pub(crate) fn has_app_name_conflict(
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
pub(crate) fn load_app<B: StorageBackend>(
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

/// Saves an `App` skeleton to the GLOBAL state layer.
pub(crate) fn save_app<B: StorageBackend>(
    state_layer: &inferadb_ledger_state::StateLayer<B>,
    organization: OrganizationId,
    app: &App,
) -> Result<(), LedgerResponse> {
    let key = SystemKeys::app_key(organization, app.id);
    check_tier(&key, KeyTier::Global)?;
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
pub(crate) fn load_app_profile<B: StorageBackend>(
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
