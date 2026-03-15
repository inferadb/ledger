//! Shared protobuf conversion helpers used across SDK modules.

use inferadb_ledger_proto::proto;
use inferadb_ledger_types::{Region, UserEmailId, UserRole, UserSlug, UserStatus};

use crate::types::admin::{UserEmailInfo, UserInfo};

/// Converts a proto `Timestamp` to `SystemTime`, returning `None` for invalid values.
pub(crate) fn proto_timestamp_to_system_time(
    ts: &prost_types::Timestamp,
) -> Option<std::time::SystemTime> {
    let secs = u64::try_from(ts.seconds).ok()?;
    let nanos = u32::try_from(ts.nanos).ok().filter(|&n| n < 1_000_000_000)?;
    std::time::UNIX_EPOCH.checked_add(std::time::Duration::new(secs, nanos))
}

/// Converts a `SystemTime` to a proto `Timestamp`.
pub(crate) fn system_time_to_proto_timestamp(t: &std::time::SystemTime) -> prost_types::Timestamp {
    match t.duration_since(std::time::UNIX_EPOCH) {
        Ok(d) => {
            prost_types::Timestamp { seconds: d.as_secs() as i64, nanos: d.subsec_nanos() as i32 }
        },
        Err(_) => prost_types::Timestamp { seconds: 0, nanos: 0 },
    }
}

/// Constructs an `SdkError::Rpc` for a missing field in a server response.
pub(crate) fn missing_response_field(field: &str, response_type: &str) -> crate::error::SdkError {
    crate::error::SdkError::Rpc {
        code: tonic::Code::Internal,
        message: format!("Missing {field} in {response_type}"),
        request_id: None,
        trace_id: None,
        error_details: None,
    }
}

/// Converts a protobuf region `i32` to a domain [`Region`].
///
/// Returns `None` for unspecified (0) or unknown values, allowing callers
/// to decide the fallback behavior.
pub(crate) fn region_from_proto_i32(value: i32) -> Option<Region> {
    let proto_region = proto::Region::try_from(value).ok()?;
    Region::try_from(proto_region).ok()
}

/// Converts a domain [`Region`] to a protobuf `i32` value.
pub(crate) fn region_to_proto_i32(region: Region) -> i32 {
    let proto_region: proto::Region = region.into();
    proto_region.into()
}

/// Converts a protobuf `UserStatus` i32 to a domain [`UserStatus`].
pub(crate) fn user_status_from_proto_i32(value: i32) -> UserStatus {
    match proto::UserStatus::try_from(value) {
        Ok(proto::UserStatus::Active) => UserStatus::Active,
        Ok(proto::UserStatus::PendingOrg) => UserStatus::PendingOrg,
        Ok(proto::UserStatus::Suspended) => UserStatus::Suspended,
        Ok(proto::UserStatus::Deleting) => UserStatus::Deleting,
        Ok(proto::UserStatus::Deleted) => UserStatus::Deleted,
        _ => UserStatus::Active,
    }
}

/// Converts a protobuf `UserRole` i32 to a domain [`UserRole`].
pub(crate) fn user_role_from_proto_i32(value: i32) -> UserRole {
    match proto::UserRole::try_from(value) {
        Ok(proto::UserRole::Admin) => UserRole::Admin,
        _ => UserRole::User,
    }
}

/// Converts a proto `User` message to a [`UserInfo`].
pub(crate) fn user_info_from_proto(user: &proto::User) -> UserInfo {
    UserInfo {
        slug: UserSlug::new(user.slug.as_ref().map_or(0, |s| s.slug)),
        name: user.name.clone(),
        email: UserEmailId::new(user.email.as_ref().map_or(0, |e| e.id)),
        status: user_status_from_proto_i32(user.status),
        role: user_role_from_proto_i32(user.role),
        created_at: user.created_at.as_ref().and_then(proto_timestamp_to_system_time),
        updated_at: user.updated_at.as_ref().and_then(proto_timestamp_to_system_time),
        deleted_at: user.deleted_at.as_ref().and_then(proto_timestamp_to_system_time),
        retention_days: None,
    }
}

/// Converts a proto `UserEmail` message to a [`UserEmailInfo`].
pub(crate) fn user_email_info_from_proto(email: &proto::UserEmail) -> UserEmailInfo {
    UserEmailInfo {
        id: UserEmailId::new(email.id.as_ref().map_or(0, |e| e.id)),
        email: email.email.clone(),
        verified: email.verified_at.is_some(),
        created_at: email.created_at.as_ref().and_then(proto_timestamp_to_system_time),
        verified_at: email.verified_at.as_ref().and_then(proto_timestamp_to_system_time),
    }
}

/// Converts a non-empty string to `Some`, or returns `None` for empty strings.
///
/// Used for normalizing `next_page_token` from proto responses.
pub(crate) fn non_empty(s: String) -> Option<String> {
    if s.is_empty() { None } else { Some(s) }
}
