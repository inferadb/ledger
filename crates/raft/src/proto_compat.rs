//! Proto conversions that depend on crate-local types.
//!
//! Most proto <-> domain conversions live in `inferadb_ledger_proto::convert`.
//! This module holds conversions that require types from `inferadb_ledger_state`,
//! which the proto crate intentionally does not depend on.
//!
//! These are free functions rather than `From` impls because the orphan rule
//! prevents implementing foreign traits between two external types.

use inferadb_ledger_proto::proto;
use inferadb_ledger_state::system::OrganizationStatus;

/// Converts a domain `OrganizationStatus` to its proto representation.
pub(crate) fn organization_status_to_proto(
    status: OrganizationStatus,
) -> proto::OrganizationStatus {
    match status {
        OrganizationStatus::Active => proto::OrganizationStatus::Active,
        OrganizationStatus::Migrating => proto::OrganizationStatus::Migrating,
        OrganizationStatus::Suspended => proto::OrganizationStatus::Suspended,
        OrganizationStatus::Deleting => proto::OrganizationStatus::Deleting,
        OrganizationStatus::Deleted => proto::OrganizationStatus::Deleted,
    }
}
