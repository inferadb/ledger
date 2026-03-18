//! Generic utility functions shared across operation handlers.

use std::sync::Arc;

use chrono::Duration;
use inferadb_ledger_state::{
    StateLayer,
    system::{
        KeyTier, RevocationResult, SYSTEM_VAULT_ID, SystemError, SystemKeys,
        SystemOrganizationService,
    },
};
use inferadb_ledger_store::StorageBackend;
use inferadb_ledger_types::{ErrorCode, Operation, VaultEntry, encode};

use crate::types::LedgerResponse;

/// Executes a cascade revocation and logs the outcome.
///
/// Cascade revocations are best-effort — failures are logged but do not
/// abort the parent operation. Callers should enter a tracing span with
/// relevant context fields (org_id, app_id, etc.) before calling.
pub(crate) fn cascade_revoke<B: StorageBackend>(
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
pub(crate) fn try_encode<T: serde::Serialize>(value: &T, context: &str) -> Option<Vec<u8>> {
    match encode(value) {
        Ok(blob) => Some(blob),
        Err(e) => {
            tracing::error!(context, error = %e, "Failed to encode state — persistence skipped, potential state divergence");
            None
        },
    }
}

/// Constructs a `LedgerResponse::Error` with the given code and message.
pub(crate) fn ledger_error(code: ErrorCode, message: impl Into<String>) -> LedgerResponse {
    LedgerResponse::Error { code, message: message.into() }
}

/// Constructs an early-return error tuple for the apply handler.
pub(crate) fn error_result(
    code: ErrorCode,
    message: impl Into<String>,
) -> (LedgerResponse, Option<VaultEntry>) {
    (ledger_error(code, message), None)
}

/// Validates a key's tier, returning a `LedgerResponse` error on mismatch.
///
/// Used in helper functions that return `Result<_, LedgerResponse>`.
pub(crate) fn check_tier(key: &str, expected: KeyTier) -> Result<(), LedgerResponse> {
    SystemKeys::validate_key_tier(key, expected)
        .map_err(|msg| ledger_error(ErrorCode::Internal, msg))
}

/// Collects all entities matching a prefix and appends `DeleteEntity` operations for each.
///
/// Uses paginated `list_entities` calls to handle any number of sub-resources
/// without a hard cap.
pub(crate) fn collect_all_entities_for_deletion<B: StorageBackend>(
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

/// Converts a `u64` seconds value into a `chrono::Duration`, saturating at `i64::MAX`.
pub(crate) fn saturating_duration_secs(secs: u64) -> Duration {
    Duration::seconds(i64::try_from(secs).unwrap_or(i64::MAX))
}
