//! Proto conversions that depend on crate-local types.
//!
//! Most proto <-> domain conversions live in `inferadb_ledger_proto::convert`.
//! This module holds conversions that require types from `inferadb_ledger_state`,
//! which the proto crate intentionally does not depend on.
//!
//! These are free functions rather than `From` impls because the orphan rule
//! prevents implementing foreign traits between two external types.

use chrono::{DateTime, Utc};
use inferadb_ledger_proto::proto;
use inferadb_ledger_state::system::{
    OrganizationMemberRole, OrganizationStatus, OrganizationTier, TeamMemberRole,
};
use inferadb_ledger_types::LedgerErrorCode;
use tonic::Status;

/// Converts a domain `OrganizationStatus` to its proto representation.
pub(crate) fn organization_status_to_proto(
    status: OrganizationStatus,
) -> proto::OrganizationStatus {
    match status {
        OrganizationStatus::Active => proto::OrganizationStatus::Active,
        OrganizationStatus::Provisioning => proto::OrganizationStatus::Provisioning,
        OrganizationStatus::Migrating => proto::OrganizationStatus::Migrating,
        OrganizationStatus::Suspended => proto::OrganizationStatus::Suspended,
        OrganizationStatus::Deleted => proto::OrganizationStatus::Deleted,
    }
}

/// Converts a domain `OrganizationTier` to its proto representation.
pub(crate) fn organization_tier_to_proto(tier: OrganizationTier) -> proto::OrganizationTier {
    match tier {
        OrganizationTier::Free => proto::OrganizationTier::Free,
        OrganizationTier::Pro => proto::OrganizationTier::Pro,
        OrganizationTier::Enterprise => proto::OrganizationTier::Enterprise,
    }
}

/// Converts a proto `OrganizationTier` to its domain representation.
///
/// `Unspecified` maps to `Free` (the default tier for new organizations).
pub(crate) fn organization_tier_from_proto(tier: proto::OrganizationTier) -> OrganizationTier {
    match tier {
        proto::OrganizationTier::Unspecified | proto::OrganizationTier::Free => {
            OrganizationTier::Free
        },
        proto::OrganizationTier::Pro => OrganizationTier::Pro,
        proto::OrganizationTier::Enterprise => OrganizationTier::Enterprise,
    }
}

/// Converts a domain `OrganizationMemberRole` to its proto representation.
pub(crate) fn member_role_to_proto(role: OrganizationMemberRole) -> proto::OrganizationMemberRole {
    match role {
        OrganizationMemberRole::Admin => proto::OrganizationMemberRole::Admin,
        OrganizationMemberRole::Member => proto::OrganizationMemberRole::Member,
    }
}

/// Converts a proto `OrganizationMemberRole` to its domain representation.
///
/// `Unspecified` maps to `Member` (the default role for new members).
pub(crate) fn member_role_from_proto(
    role: proto::OrganizationMemberRole,
) -> OrganizationMemberRole {
    match role {
        proto::OrganizationMemberRole::Admin => OrganizationMemberRole::Admin,
        proto::OrganizationMemberRole::Member | proto::OrganizationMemberRole::Unspecified => {
            OrganizationMemberRole::Member
        },
    }
}

/// Converts a domain `TeamMemberRole` to its proto representation.
pub(crate) fn team_member_role_to_proto(role: TeamMemberRole) -> proto::OrganizationTeamMemberRole {
    match role {
        TeamMemberRole::Manager => proto::OrganizationTeamMemberRole::Manager,
        TeamMemberRole::Member => proto::OrganizationTeamMemberRole::Member,
    }
}

/// Maps a [`LedgerErrorCode`] to the corresponding gRPC [`Status`].
///
/// Used by service-layer error handlers to convert structured state-machine
/// error codes into the correct gRPC status without string matching.
pub(crate) fn error_code_to_status(code: LedgerErrorCode, message: String) -> Status {
    match code {
        LedgerErrorCode::NotFound => Status::not_found(message),
        LedgerErrorCode::AlreadyExists => Status::already_exists(message),
        LedgerErrorCode::FailedPrecondition => Status::failed_precondition(message),
        LedgerErrorCode::PermissionDenied => Status::permission_denied(message),
        LedgerErrorCode::InvalidArgument => Status::invalid_argument(message),
        LedgerErrorCode::Internal => Status::internal(message),
        LedgerErrorCode::Unauthenticated => Status::unauthenticated(message),
    }
}

/// Decodes an opaque page token as a big-endian u64 slug cursor.
///
/// Returns `None` for absent or malformed tokens (non-8-byte), which callers
/// treat as "start from the beginning."
pub(crate) fn decode_page_token(token: &Option<Vec<u8>>) -> Option<u64> {
    token
        .as_ref()
        .and_then(|bytes| <[u8; 8]>::try_from(bytes.as_slice()).ok().map(u64::from_be_bytes))
}

/// Encodes a slug value as an opaque page token (big-endian u64).
pub(crate) fn encode_page_token(slug: u64) -> Vec<u8> {
    slug.to_be_bytes().to_vec()
}

/// Clamps a requested page size to the valid range [1, 1000], defaulting 0 to 100.
pub(crate) fn normalize_page_size(requested: u32) -> usize {
    if requested == 0 { 100 } else { requested.min(1000) as usize }
}

/// Paginates a list of `(slug, T)` pairs by slug cursor.
///
/// Sorts by slug, applies the cursor filter (items after `start_after`),
/// takes `page_size` items, and returns the page items plus the next page token
/// (if more items exist).
pub(crate) fn paginate_by_slug<T>(
    mut items: Vec<(u64, T)>,
    start_after: Option<u64>,
    page_size: usize,
) -> (Vec<T>, Option<Vec<u8>>) {
    items.sort_by_key(|(slug, _)| *slug);
    if let Some(after) = start_after {
        let start = items.partition_point(|(slug, _)| *slug <= after);
        items.drain(..start);
    }
    let has_more = items.len() > page_size;
    let page: Vec<_> = items.into_iter().take(page_size).collect();
    let next_page_token =
        if has_more { page.last().map(|(slug, _)| encode_page_token(*slug)) } else { None };
    let result = page.into_iter().map(|(_, item)| item).collect();
    (result, next_page_token)
}

/// Converts a chrono `DateTime<Utc>` to a proto `Timestamp`.
pub(crate) fn datetime_to_proto(dt: &DateTime<Utc>) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: dt.timestamp(),
        nanos: i32::try_from(dt.timestamp_subsec_nanos()).unwrap_or(0),
    }
}

/// Converts a proto `Timestamp` to a chrono `DateTime<Utc>`.
///
/// Returns `DateTime::UNIX_EPOCH` if the timestamp cannot be represented.
pub(crate) fn proto_to_datetime(ts: &prost_types::Timestamp) -> DateTime<Utc> {
    DateTime::from_timestamp(ts.seconds, ts.nanos.max(0) as u32).unwrap_or(DateTime::UNIX_EPOCH)
}
