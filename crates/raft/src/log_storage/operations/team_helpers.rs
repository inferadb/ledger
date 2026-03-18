//! Team domain helpers for state machine apply logic.

use chrono::{DateTime, Utc};
use inferadb_ledger_state::system::{KeyTier, SYSTEM_VAULT_ID, SystemKeys, Team, TeamMember};
use inferadb_ledger_store::StorageBackend;
use inferadb_ledger_types::{ErrorCode, Operation, OrganizationId, TeamId, decode, encode};

use super::{
    super::types::AppliedState,
    helpers::{check_tier, ledger_error},
};
use crate::types::LedgerResponse;

/// Checks whether a team name already exists within an organization.
///
/// Uses the in-memory `team_name_index` for O(1) lookups instead of
/// scanning all team records. The `exclude_team` parameter allows
/// renaming a team to the same name (self-conflict).
pub(crate) fn has_team_name_conflict(
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

/// Loads and decodes a team record from the state layer.
///
/// Returns the decoded `Team` or a structured `LedgerResponse::Error`
/// suitable for direct return from the apply handler.
pub(crate) fn load_team<B: StorageBackend>(
    state_layer: &inferadb_ledger_state::StateLayer<B>,
    organization: OrganizationId,
    team: TeamId,
) -> Result<Team, LedgerResponse> {
    let key = SystemKeys::team_key(organization, team);
    let entity = state_layer
        .get_entity(SYSTEM_VAULT_ID, key.as_bytes())
        .map_err(|e| ledger_error(ErrorCode::Internal, format!("Failed to read team: {e}")))?
        .ok_or_else(|| ledger_error(ErrorCode::NotFound, format!("Team {} not found", team)))?;
    decode::<Team>(&entity.value)
        .map_err(|e| ledger_error(ErrorCode::Internal, format!("Failed to decode team: {e}")))
}

/// Saves a team record to the REGIONAL state layer.
pub(crate) fn save_team<B: StorageBackend>(
    state_layer: &inferadb_ledger_state::StateLayer<B>,
    organization: OrganizationId,
    team: TeamId,
    record: &Team,
) -> Result<(), LedgerResponse> {
    let key = SystemKeys::team_key(organization, team);
    check_tier(&key, KeyTier::Regional)?;
    let bytes = encode(record)
        .map_err(|e| ledger_error(ErrorCode::Internal, format!("Failed to encode team: {e}")))?;
    let ops = vec![Operation::SetEntity { key, value: bytes, condition: None, expires_at: None }];
    state_layer
        .apply_operations(SYSTEM_VAULT_ID, &ops, 0)
        .map(|_| ())
        .map_err(|e| ledger_error(ErrorCode::Internal, format!("Failed to write team: {e}")))
}

/// Migrates members from one team to another, preserving roles and join dates.
///
/// Skips members already present in the target team (deduplication by user_id).
pub(crate) fn migrate_team_members<B: StorageBackend>(
    state_layer: &inferadb_ledger_state::StateLayer<B>,
    organization: OrganizationId,
    source: &Team,
    target_team: TeamId,
    block_timestamp: DateTime<Utc>,
) -> Result<(), LedgerResponse> {
    let mut target = load_team(state_layer, organization, target_team)?;
    for member in &source.members {
        if !target.members.iter().any(|m| m.user_id == member.user_id) {
            target.members.push(TeamMember {
                user_id: member.user_id,
                role: member.role,
                joined_at: member.joined_at,
            });
        }
    }
    target.updated_at = block_timestamp;
    save_team(state_layer, organization, target_team, &target)
}
