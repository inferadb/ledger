//! Structured error-context builders for the bridged tonic surface.
//!
//! [`build_error_details`] returns a [`WireError`] so the rest of the
//! services crate stays wire-shaped on the error path. Surviving tonic-
//! side callers compose with [`wire_helpers::wire_error_to_tonic_status`]
//! when they still need a `tonic::Status`.

use std::collections::HashMap;

use inferadb_ledger_wire::{ErrorCode, WireError};

/// Builds a [`WireError`] with the structured fields the SDK reads on the
/// error path.
///
/// The `error_code` numeric field (e.g. `3203` for `AppInvalidArgument`)
/// is encoded as `context["error_code"]` — same key the wire-native
/// helpers in [`super::helpers`] use. Surviving tonic-shaped callers
/// compose this with [`super::wire_helpers::wire_error_to_tonic_status`]
/// to land back on a `tonic::Status` carrying a JSON-encoded `WireError`
/// in `Status::details()`.
pub(crate) fn build_error_details(
    error_code: u16,
    is_retryable: bool,
    retry_after_ms: Option<i32>,
    context: HashMap<String, String>,
    suggested_action: Option<&str>,
) -> WireError {
    let code = if is_retryable { ErrorCode::RateLimited } else { ErrorCode::FailedPrecondition };
    let mut ctx = std::collections::BTreeMap::new();
    for (k, v) in context {
        ctx.insert(k, v);
    }
    super::wire_helpers::build_wire_error(
        code,
        String::new(),
        error_code.to_string(),
        is_retryable,
        retry_after_ms.and_then(|v| u64::try_from(v.max(0)).ok()).unwrap_or(0),
        ctx,
        suggested_action.unwrap_or(""),
    )
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    #[test]
    fn build_error_details_basic() {
        let details =
            build_error_details(3203, false, None, HashMap::new(), Some("Fix parameters"));
        assert_eq!(details.context.get("error_code").map(String::as_str), Some("3203"));
        assert!(!details.retryable);
        assert_eq!(details.retry_after_ms, 0);
        assert_eq!(details.suggested_action, "Fix parameters");
    }

    #[test]
    fn build_error_details_with_retry_and_context() {
        let mut context = HashMap::new();
        context.insert("organization".to_owned(), "42".to_owned());

        let details = build_error_details(3204, true, Some(1000), context, None);
        assert_eq!(details.context.get("error_code").map(String::as_str), Some("3204"));
        assert!(details.retryable);
        assert_eq!(details.retry_after_ms, 1000);
        assert_eq!(details.context.get("organization").unwrap(), "42");
        assert_eq!(details.suggested_action, "");
    }

    #[test]
    fn build_error_details_round_trip_through_status() {
        // Bridge round-trip — the WireError survives Status::with_details
        // → tonic_status_to_wire_error byte-symmetrically because the
        // bridge JSON-encodes the entire WireError.
        let mut context = HashMap::new();
        context.insert("key".to_owned(), "value".to_owned());

        let original = build_error_details(1101, false, None, context, Some("Run integrity check"));

        let status = super::super::wire_helpers::wire_error_to_tonic_status(original);
        let decoded = super::super::wire_helpers::tonic_status_to_wire_error(status);

        assert_eq!(decoded.context.get("error_code").map(String::as_str), Some("1101"));
        assert!(!decoded.retryable);
        assert_eq!(decoded.retry_after_ms, 0);
        assert_eq!(decoded.context.get("key").unwrap(), "value");
        assert_eq!(decoded.suggested_action, "Run integrity check");
    }
}
