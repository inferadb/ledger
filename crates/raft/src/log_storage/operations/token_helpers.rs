//! Token and signing key helpers for state machine apply logic.

use inferadb_ledger_state::system::{SigningKey, SystemOrganizationService};
use inferadb_ledger_store::StorageBackend;
use inferadb_ledger_types::{ErrorCode, VaultEntry};

use super::helpers::error_result;
use crate::types::LedgerResponse;

/// Looks up a signing key by kid, returning the key or an early-return error tuple.
pub(crate) fn require_signing_key<B: StorageBackend>(
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
