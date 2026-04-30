//! Wire-protocol error conversion.
//!
//! Bridges [`inferadb_ledger_wire::WireError`] (the wire-protocol error
//! payload) into the SDK's [`SdkError`] shape. SDK ops at the wire layer
//! call this from their per-RPC handlers; the wire-side error code is
//! preserved verbatim — there is no longer a tonic indirection.

use std::collections::HashMap;

use inferadb_ledger_types::Region;
use inferadb_ledger_wire::{ErrorCode as WireErrorCode, WireError};

use crate::error::{SdkError, ServerErrorDetails};

/// Converts a [`WireError`] into the SDK's [`SdkError`] shape.
///
/// Rate-limit and migration responses get dedicated variants; everything
/// else lands in [`SdkError::Rpc`].
///
/// `request_id` and `trace_id` are not on [`WireError`] (they live in
/// [`FrameHeader`](inferadb_ledger_wire::FrameHeader) on the carrying
/// frame). Callers that have access to the frame should set them via
/// `from_wire_error_with_correlation`.
impl From<WireError> for SdkError {
    fn from(err: WireError) -> Self {
        from_wire_error_with_correlation(err, None, None)
    }
}

/// Like [`From<WireError>`] but lets the caller propagate frame-level
/// correlation IDs (`request_id`, `trace_id`) lifted from
/// [`FrameHeader`](inferadb_ledger_wire::FrameHeader).
#[must_use]
pub(crate) fn from_wire_error_with_correlation(
    err: WireError,
    request_id: Option<String>,
    trace_id: Option<String>,
) -> SdkError {
    // Project context BTreeMap → HashMap for parity with the existing
    // ServerErrorDetails.context shape.
    let context: HashMap<String, String> =
        err.context.iter().map(|(k, v)| (k.clone(), v.clone())).collect();

    // Server-side error_code is conveyed via context["error_code"] when the
    // server needs to disambiguate sub-classes (e.g. "3106" =
    // OrganizationMigrating, "3107" = UserMigrating).
    let error_code = context.get("error_code").cloned().unwrap_or_default();

    // Rate-limited path. The rate-limit family is signaled by the wire
    // ErrorCode itself (no need to inspect status metadata).
    if matches!(
        err.code,
        WireErrorCode::RateLimited
            | WireErrorCode::TooManyAttempts
            | WireErrorCode::InvitationRateLimited
    ) {
        let retry_after = std::time::Duration::from_millis(err.retry_after_ms);
        let suggested_action =
            if err.suggested_action.is_empty() { None } else { Some(err.suggested_action) };
        return SdkError::RateLimited {
            message: err.message,
            retry_after,
            request_id,
            trace_id,
            error_details: Some(Box::new(ServerErrorDetails {
                error_code,
                is_retryable: err.retryable,
                retry_after_ms: i32::try_from(err.retry_after_ms).ok(),
                context,
                suggested_action,
            })),
        };
    }

    // Migration variants are signaled by FailedPrecondition + a sub-code
    // string in the context map.
    if err.code == WireErrorCode::FailedPrecondition {
        let source_region = context.get("source_region").and_then(|s| s.parse::<Region>().ok());
        let target_region = context.get("target_region").and_then(|s| s.parse::<Region>().ok());
        let retry_after_ms = if err.retry_after_ms > 0 { err.retry_after_ms } else { 30_000 };

        if error_code == "3106"
            && let (Some(src), Some(tgt)) = (source_region, target_region)
        {
            return SdkError::OrganizationMigrating {
                source_region: src,
                target_region: tgt,
                retry_after: std::time::Duration::from_millis(retry_after_ms),
            };
        }
        if error_code == "3107"
            && let (Some(src), Some(tgt)) = (source_region, target_region)
        {
            return SdkError::UserMigrating {
                source_region: src,
                target_region: tgt,
                retry_after: std::time::Duration::from_millis(retry_after_ms),
            };
        }
    }

    let suggested_action =
        if err.suggested_action.is_empty() { None } else { Some(err.suggested_action) };

    SdkError::Rpc {
        code: err.code,
        message: err.message,
        request_id,
        trace_id,
        error_details: Some(Box::new(ServerErrorDetails {
            error_code,
            is_retryable: err.retryable,
            retry_after_ms: i32::try_from(err.retry_after_ms).ok(),
            context,
            suggested_action,
        })),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn make_wire_error(code: WireErrorCode) -> WireError {
        WireError {
            code,
            message: "test".to_string(),
            retryable: false,
            retry_after_ms: 0,
            context: BTreeMap::new(),
            suggested_action: String::new(),
        }
    }

    #[test]
    fn rate_limited_codes_become_retryable_sdk_errors() {
        for code in [
            WireErrorCode::RateLimited,
            WireErrorCode::TooManyAttempts,
            WireErrorCode::InvitationRateLimited,
        ] {
            let err: SdkError = make_wire_error(code).into();
            assert!(
                matches!(err, SdkError::RateLimited { .. }),
                "{code:?} did not map to RateLimited"
            );
            assert!(err.is_retryable(), "{code:?} not retryable");
        }
    }

    #[test]
    fn stale_routing_becomes_retryable_rpc() {
        // StaleRouting → SdkError::Rpc { code: StaleRouting }, retryable.
        let err: SdkError = make_wire_error(WireErrorCode::StaleRouting).into();
        assert!(matches!(err, SdkError::Rpc { code: WireErrorCode::StaleRouting, .. }));
        assert!(err.is_retryable());
    }

    #[test]
    fn rate_limited_wire_error_becomes_rate_limited_sdk_error() {
        let err = WireError {
            code: WireErrorCode::RateLimited,
            message: "throttled".to_string(),
            retryable: true,
            retry_after_ms: 2500,
            context: BTreeMap::new(),
            suggested_action: "wait and retry".to_string(),
        };
        let sdk_err: SdkError = err.into();
        match sdk_err {
            SdkError::RateLimited { message, retry_after, error_details, .. } => {
                assert_eq!(message, "throttled");
                assert_eq!(retry_after, std::time::Duration::from_millis(2500));
                let details = error_details.expect("details present");
                assert!(details.is_retryable);
                assert_eq!(details.retry_after_ms, Some(2500));
                assert_eq!(details.suggested_action.as_deref(), Some("wait and retry"));
            },
            other => panic!("expected RateLimited, got {other:?}"),
        }
    }

    #[test]
    fn organization_migrating_wire_error_becomes_organization_migrating() {
        let mut context = BTreeMap::new();
        context.insert("error_code".to_string(), "3106".to_string());
        context.insert("source_region".to_string(), "us-east-va".to_string());
        context.insert("target_region".to_string(), "ie-east-dublin".to_string());

        let err = WireError {
            code: WireErrorCode::FailedPrecondition,
            message: "organization migrating".to_string(),
            retryable: true,
            retry_after_ms: 15_000,
            context,
            suggested_action: String::new(),
        };
        let sdk_err: SdkError = err.into();
        match sdk_err {
            SdkError::OrganizationMigrating { source_region, target_region, retry_after } => {
                assert_eq!(source_region, Region::US_EAST_VA);
                assert_eq!(target_region, Region::IE_EAST_DUBLIN);
                assert_eq!(retry_after, std::time::Duration::from_millis(15_000));
            },
            other => panic!("expected OrganizationMigrating, got {other:?}"),
        }
    }

    #[test]
    fn user_migrating_wire_error_becomes_user_migrating() {
        let mut context = BTreeMap::new();
        context.insert("error_code".to_string(), "3107".to_string());
        context.insert("source_region".to_string(), "us-east-va".to_string());
        context.insert("target_region".to_string(), "ie-east-dublin".to_string());

        let err = WireError {
            code: WireErrorCode::FailedPrecondition,
            message: "user migrating".to_string(),
            retryable: true,
            retry_after_ms: 20_000,
            context,
            suggested_action: String::new(),
        };
        let sdk_err: SdkError = err.into();
        assert!(matches!(sdk_err, SdkError::UserMigrating { .. }));
    }

    #[test]
    fn correlation_ids_propagate() {
        let err = make_wire_error(WireErrorCode::Internal);
        let sdk_err = from_wire_error_with_correlation(
            err,
            Some("req-123".to_string()),
            Some("trace-abc".to_string()),
        );
        match sdk_err {
            SdkError::Rpc { request_id, trace_id, .. } => {
                assert_eq!(request_id.as_deref(), Some("req-123"));
                assert_eq!(trace_id.as_deref(), Some("trace-abc"));
            },
            other => panic!("expected Rpc, got {other:?}"),
        }
    }

    #[test]
    fn empty_suggested_action_becomes_none() {
        let err = make_wire_error(WireErrorCode::Internal);
        let sdk_err: SdkError = err.into();
        match sdk_err {
            SdkError::Rpc { error_details, .. } => {
                let details = error_details.expect("details present");
                assert!(details.suggested_action.is_none());
            },
            other => panic!("expected Rpc, got {other:?}"),
        }
    }

    #[test]
    fn server_retryable_hint_propagates_to_error_details() {
        let mut context = BTreeMap::new();
        context.insert("error_code".to_string(), "9999".to_string());

        let err = WireError {
            code: WireErrorCode::Internal,
            message: "something".to_string(),
            retryable: true,
            retry_after_ms: 100,
            context,
            suggested_action: String::new(),
        };
        let sdk_err: SdkError = err.into();
        let details = sdk_err.server_error_details().expect("details");
        assert!(details.is_retryable);
        assert_eq!(details.retry_after_ms, Some(100));
        assert_eq!(details.error_code, "9999");
    }

    #[test]
    fn context_btreemap_projects_to_hashmap() {
        let mut context = BTreeMap::new();
        context.insert("a".to_string(), "1".to_string());
        context.insert("b".to_string(), "2".to_string());

        let err = WireError {
            code: WireErrorCode::Internal,
            message: "x".to_string(),
            retryable: false,
            retry_after_ms: 0,
            context,
            suggested_action: String::new(),
        };
        let sdk_err: SdkError = err.into();
        let details = sdk_err.server_error_details().expect("details");
        assert_eq!(details.context.len(), 2);
        assert_eq!(details.context.get("a").unwrap(), "1");
        assert_eq!(details.context.get("b").unwrap(), "2");
    }

    #[test]
    fn failed_precondition_without_migration_codes_stays_rpc() {
        // FailedPrecondition without an error_code subtype is a regular Rpc error.
        let err = WireError {
            code: WireErrorCode::FailedPrecondition,
            message: "precondition".to_string(),
            retryable: false,
            retry_after_ms: 0,
            context: BTreeMap::new(),
            suggested_action: String::new(),
        };
        let sdk_err: SdkError = err.into();
        assert!(matches!(sdk_err, SdkError::Rpc { code: WireErrorCode::FailedPrecondition, .. }));
    }

    #[test]
    fn wire_code_preserved_verbatim() {
        // Unlike the legacy tonic indirection, the SDK now carries the wire
        // ErrorCode directly. Verify the code on the resulting Rpc variant
        // matches the input wire code one-to-one for non-special variants.
        for code in [
            WireErrorCode::NotFound,
            WireErrorCode::AlreadyExists,
            WireErrorCode::PermissionDenied,
            WireErrorCode::InvalidArgument,
            WireErrorCode::Internal,
            WireErrorCode::Unauthenticated,
            WireErrorCode::StaleRouting,
            WireErrorCode::Expired,
            WireErrorCode::Deprecated,
            WireErrorCode::InvitationAlreadyResolved,
            WireErrorCode::InvitationEmailMismatch,
            WireErrorCode::InvitationAlreadyMember,
            WireErrorCode::InvitationDuplicatePending,
        ] {
            let err: SdkError = make_wire_error(code).into();
            match err {
                SdkError::Rpc { code: actual, .. } => {
                    assert_eq!(actual, code, "code mismatch for {code:?}");
                },
                other => panic!("{code:?} did not map to Rpc: {other:?}"),
            }
        }
    }
}
