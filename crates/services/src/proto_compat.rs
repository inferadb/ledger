//! Proto conversions that depend on crate-local types.
//!
//! Most proto <-> domain conversions live in `inferadb_ledger_proto::convert`.
//! Conversions that require types from `inferadb_ledger_state`,
//! which the proto crate intentionally does not depend on.
//!
//! These are free functions rather than `From` impls because the orphan rule
//! prevents implementing foreign traits between two external types.

use chrono::{DateTime, Utc};
use inferadb_ledger_proto::{
    convert::{datetime_to_proto_timestamp, proto_timestamp_to_datetime},
    proto,
};
use inferadb_ledger_state::system::{
    OrganizationMemberRole, OrganizationStatus, OrganizationTier, TeamMemberRole,
};

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

/// Converts a proto `OrganizationTeamMemberRole` to its domain representation.
pub(crate) fn proto_to_team_member_role(role: proto::OrganizationTeamMemberRole) -> TeamMemberRole {
    match role {
        proto::OrganizationTeamMemberRole::Manager => TeamMemberRole::Manager,
        proto::OrganizationTeamMemberRole::Member
        | proto::OrganizationTeamMemberRole::Unspecified => TeamMemberRole::Member,
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
///
/// Delegates to [`inferadb_ledger_proto::convert::datetime_to_proto_timestamp`].
pub(crate) fn datetime_to_proto(dt: &DateTime<Utc>) -> prost_types::Timestamp {
    datetime_to_proto_timestamp(dt)
}

/// Converts a proto `Timestamp` to a chrono `DateTime<Utc>`.
///
/// Returns `DateTime::UNIX_EPOCH` if the timestamp cannot be represented.
/// Delegates to [`inferadb_ledger_proto::convert::proto_timestamp_to_datetime`].
pub(crate) fn proto_to_datetime(ts: &prost_types::Timestamp) -> DateTime<Utc> {
    proto_timestamp_to_datetime(ts)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    // =========================================================================
    // organization_status_to_proto
    // =========================================================================

    #[test]
    fn org_status_active() {
        assert_eq!(
            organization_status_to_proto(OrganizationStatus::Active),
            proto::OrganizationStatus::Active
        );
    }

    #[test]
    fn org_status_provisioning() {
        assert_eq!(
            organization_status_to_proto(OrganizationStatus::Provisioning),
            proto::OrganizationStatus::Provisioning
        );
    }

    #[test]
    fn org_status_migrating() {
        assert_eq!(
            organization_status_to_proto(OrganizationStatus::Migrating),
            proto::OrganizationStatus::Migrating
        );
    }

    #[test]
    fn org_status_suspended() {
        assert_eq!(
            organization_status_to_proto(OrganizationStatus::Suspended),
            proto::OrganizationStatus::Suspended
        );
    }

    #[test]
    fn org_status_deleted() {
        assert_eq!(
            organization_status_to_proto(OrganizationStatus::Deleted),
            proto::OrganizationStatus::Deleted
        );
    }

    // =========================================================================
    // organization_tier_to_proto / organization_tier_from_proto
    // =========================================================================

    #[test]
    fn org_tier_roundtrip_free() {
        let proto_tier = organization_tier_to_proto(OrganizationTier::Free);
        assert_eq!(proto_tier, proto::OrganizationTier::Free);
        assert_eq!(organization_tier_from_proto(proto_tier), OrganizationTier::Free);
    }

    #[test]
    fn org_tier_roundtrip_pro() {
        let proto_tier = organization_tier_to_proto(OrganizationTier::Pro);
        assert_eq!(proto_tier, proto::OrganizationTier::Pro);
        assert_eq!(organization_tier_from_proto(proto_tier), OrganizationTier::Pro);
    }

    #[test]
    fn org_tier_roundtrip_enterprise() {
        let proto_tier = organization_tier_to_proto(OrganizationTier::Enterprise);
        assert_eq!(proto_tier, proto::OrganizationTier::Enterprise);
        assert_eq!(organization_tier_from_proto(proto_tier), OrganizationTier::Enterprise);
    }

    #[test]
    fn org_tier_unspecified_defaults_to_free() {
        assert_eq!(
            organization_tier_from_proto(proto::OrganizationTier::Unspecified),
            OrganizationTier::Free
        );
    }

    // =========================================================================
    // member_role_to_proto / member_role_from_proto
    // =========================================================================

    #[test]
    fn member_role_roundtrip_admin() {
        let proto_role = member_role_to_proto(OrganizationMemberRole::Admin);
        assert_eq!(proto_role, proto::OrganizationMemberRole::Admin);
        assert_eq!(member_role_from_proto(proto_role), OrganizationMemberRole::Admin);
    }

    #[test]
    fn member_role_roundtrip_member() {
        let proto_role = member_role_to_proto(OrganizationMemberRole::Member);
        assert_eq!(proto_role, proto::OrganizationMemberRole::Member);
        assert_eq!(member_role_from_proto(proto_role), OrganizationMemberRole::Member);
    }

    #[test]
    fn member_role_unspecified_defaults_to_member() {
        assert_eq!(
            member_role_from_proto(proto::OrganizationMemberRole::Unspecified),
            OrganizationMemberRole::Member
        );
    }

    // =========================================================================
    // team_member_role_to_proto / proto_to_team_member_role
    // =========================================================================

    #[test]
    fn team_role_roundtrip_manager() {
        let proto_role = team_member_role_to_proto(TeamMemberRole::Manager);
        assert_eq!(proto_role, proto::OrganizationTeamMemberRole::Manager);
        assert_eq!(proto_to_team_member_role(proto_role), TeamMemberRole::Manager);
    }

    #[test]
    fn team_role_roundtrip_member() {
        let proto_role = team_member_role_to_proto(TeamMemberRole::Member);
        assert_eq!(proto_role, proto::OrganizationTeamMemberRole::Member);
        assert_eq!(proto_to_team_member_role(proto_role), TeamMemberRole::Member);
    }

    #[test]
    fn team_role_unspecified_defaults_to_member() {
        assert_eq!(
            proto_to_team_member_role(proto::OrganizationTeamMemberRole::Unspecified),
            TeamMemberRole::Member
        );
    }

    // =========================================================================
    // decode_page_token / encode_page_token
    // =========================================================================

    #[test]
    fn page_token_roundtrip() {
        let encoded = encode_page_token(42);
        let decoded = decode_page_token(&Some(encoded));
        assert_eq!(decoded, Some(42));
    }

    #[test]
    fn page_token_roundtrip_zero() {
        let encoded = encode_page_token(0);
        let decoded = decode_page_token(&Some(encoded));
        assert_eq!(decoded, Some(0));
    }

    #[test]
    fn page_token_roundtrip_max() {
        let encoded = encode_page_token(u64::MAX);
        let decoded = decode_page_token(&Some(encoded));
        assert_eq!(decoded, Some(u64::MAX));
    }

    #[test]
    fn decode_page_token_none() {
        assert_eq!(decode_page_token(&None), None);
    }

    #[test]
    fn decode_page_token_wrong_length() {
        assert_eq!(decode_page_token(&Some(vec![1, 2, 3])), None);
    }

    #[test]
    fn decode_page_token_empty() {
        assert_eq!(decode_page_token(&Some(vec![])), None);
    }

    #[test]
    fn encode_page_token_is_big_endian() {
        let bytes = encode_page_token(1);
        assert_eq!(bytes, vec![0, 0, 0, 0, 0, 0, 0, 1]);
    }

    // =========================================================================
    // normalize_page_size
    // =========================================================================

    #[test]
    fn normalize_page_size_zero_defaults_to_100() {
        assert_eq!(normalize_page_size(0), 100);
    }

    #[test]
    fn normalize_page_size_normal_value() {
        assert_eq!(normalize_page_size(50), 50);
    }

    #[test]
    fn normalize_page_size_max_clamped() {
        assert_eq!(normalize_page_size(5000), 1000);
    }

    #[test]
    fn normalize_page_size_exactly_1000() {
        assert_eq!(normalize_page_size(1000), 1000);
    }

    #[test]
    fn normalize_page_size_one() {
        assert_eq!(normalize_page_size(1), 1);
    }

    // =========================================================================
    // paginate_by_slug
    // =========================================================================

    #[test]
    fn paginate_empty() {
        let items: Vec<(u64, String)> = vec![];
        let (result, token) = paginate_by_slug(items, None, 10);
        assert!(result.is_empty());
        assert!(token.is_none());
    }

    #[test]
    fn paginate_single_page() {
        let items = vec![(3, "c"), (1, "a"), (2, "b")];
        let (result, token) = paginate_by_slug(items, None, 10);
        // Should be sorted by slug
        assert_eq!(result, vec!["a", "b", "c"]);
        assert!(token.is_none());
    }

    #[test]
    fn paginate_with_more_items() {
        let items = vec![(1, "a"), (2, "b"), (3, "c")];
        let (result, token) = paginate_by_slug(items, None, 2);
        assert_eq!(result, vec!["a", "b"]);
        assert!(token.is_some());
        // Token should encode slug 2 (last item in page)
        let cursor = decode_page_token(&token);
        assert_eq!(cursor, Some(2));
    }

    #[test]
    fn paginate_with_cursor() {
        let items = vec![(1, "a"), (2, "b"), (3, "c"), (4, "d")];
        let (result, token) = paginate_by_slug(items, Some(2), 10);
        // Items after slug 2
        assert_eq!(result, vec!["c", "d"]);
        assert!(token.is_none());
    }

    #[test]
    fn paginate_cursor_at_end() {
        let items = vec![(1, "a"), (2, "b")];
        let (result, token) = paginate_by_slug(items, Some(2), 10);
        assert!(result.is_empty());
        assert!(token.is_none());
    }

    #[test]
    fn paginate_cursor_beyond_end() {
        let items = vec![(1, "a"), (2, "b")];
        let (result, token) = paginate_by_slug(items, Some(100), 10);
        assert!(result.is_empty());
        assert!(token.is_none());
    }

    #[test]
    fn paginate_exact_page_size() {
        let items = vec![(1, "a"), (2, "b")];
        let (result, token) = paginate_by_slug(items, None, 2);
        assert_eq!(result, vec!["a", "b"]);
        // Exactly page_size items means no more
        assert!(token.is_none());
    }

    // =========================================================================
    // datetime_to_proto / proto_to_datetime
    // =========================================================================

    #[test]
    fn datetime_proto_roundtrip() {
        let now = Utc::now();
        let proto_ts = datetime_to_proto(&now);
        let back = proto_to_datetime(&proto_ts);
        // Subsecond precision may differ (nanos truncation), so compare at millis
        assert_eq!(now.timestamp(), back.timestamp());
        assert_eq!(now.timestamp_subsec_nanos(), back.timestamp_subsec_nanos());
    }

    #[test]
    fn datetime_proto_epoch() {
        use chrono::TimeZone;
        let epoch = Utc.timestamp_opt(0, 0).unwrap();
        let proto_ts = datetime_to_proto(&epoch);
        assert_eq!(proto_ts.seconds, 0);
        assert_eq!(proto_ts.nanos, 0);
        let back = proto_to_datetime(&proto_ts);
        assert_eq!(back, epoch);
    }
}
