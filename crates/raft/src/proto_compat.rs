//! Proto conversions that depend on crate-local types.
//!
//! Most proto ↔ domain conversions live in `inferadb_ledger_proto::convert`.
//! This module holds conversions that require types from `inferadb_ledger_state`,
//! which the proto crate intentionally does not depend on.
//!
//! These are free functions rather than `From` impls because the orphan rule
//! prevents implementing foreign traits between two external types.

use inferadb_ledger_proto::proto;
use inferadb_ledger_state::system::NamespaceStatus;

/// Converts a domain `NamespaceStatus` to its proto representation.
pub(crate) fn namespace_status_to_proto(status: NamespaceStatus) -> proto::NamespaceStatus {
    match status {
        NamespaceStatus::Active => proto::NamespaceStatus::Active,
        NamespaceStatus::Migrating => proto::NamespaceStatus::Migrating,
        NamespaceStatus::Suspended => proto::NamespaceStatus::Suspended,
        NamespaceStatus::Deleting => proto::NamespaceStatus::Deleting,
        NamespaceStatus::Deleted => proto::NamespaceStatus::Deleted,
    }
}
