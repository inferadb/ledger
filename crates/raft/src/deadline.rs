//! Request deadline propagation and timeout management.
//!
//! Provides utilities for extracting gRPC deadlines from incoming requests,
//! computing effective timeouts (the minimum of server-configured proposal
//! timeout and client-specified gRPC deadline), and rejecting near-deadline
//! requests early to avoid wasted Raft proposals.
//!
//! ## How It Works
//!
//! 1. SDK sets `grpc-timeout` header on every request (derived from `ClientConfig::timeout`)
//! 2. Server extracts the deadline via the `grpc-timeout` metadata header
//! 3. The effective timeout is `min(proposal_timeout, grpc_deadline)`
//! 4. Requests with < 100ms remaining are rejected early with `DEADLINE_EXCEEDED`
//! 5. `ForwardClient` propagates remaining deadline to downstream shards

use std::time::Duration;

use tonic::{Request, Status};

/// Header name for gRPC timeout propagation.
const GRPC_TIMEOUT_HEADER: &str = "grpc-timeout";

/// Minimum remaining deadline to accept a request.
///
/// Requests arriving with less than this remaining before their deadline
/// are rejected early with `DEADLINE_EXCEEDED` to avoid starting Raft
/// proposals that will time out before they can commit.
const NEAR_DEADLINE_THRESHOLD: Duration = Duration::from_millis(100);

/// Extract the remaining gRPC deadline from an incoming request's metadata.
///
/// Parses the `grpc-timeout` header per the
/// [gRPC over HTTP/2 spec](https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md).
/// Returns `None` if the header is missing or unparseable.
pub fn extract_deadline<T>(request: &Request<T>) -> Option<Duration> {
    extract_deadline_from_metadata(request.metadata())
}

/// Extract the remaining gRPC deadline from a metadata map.
///
/// Useful when the request has already been consumed (e.g., after
/// `request.metadata().clone()` followed by `request.into_inner()`).
pub fn extract_deadline_from_metadata(metadata: &tonic::metadata::MetadataMap) -> Option<Duration> {
    let val = metadata.get(GRPC_TIMEOUT_HEADER)?;
    let s = val.to_str().ok()?;
    parse_grpc_timeout(s)
}

/// Compute the effective timeout for a Raft proposal.
///
/// Returns the minimum of the server-configured `proposal_timeout` and
/// the client's gRPC deadline (if present). This ensures that the server
/// never waits longer than the client is willing to wait.
pub fn effective_timeout(proposal_timeout: Duration, grpc_deadline: Option<Duration>) -> Duration {
    match grpc_deadline {
        Some(deadline) => proposal_timeout.min(deadline),
        None => proposal_timeout,
    }
}

/// Reject requests whose remaining deadline is below the near-deadline threshold.
///
/// Returns `Err(Status::deadline_exceeded(...))` if the request has < 100ms
/// remaining, preventing wasted work on Raft proposals that will expire
/// before they can commit.
///
/// Returns `Ok(())` if the request has sufficient remaining time or no deadline.
pub fn check_near_deadline<T>(request: &Request<T>) -> Result<(), Status> {
    if let Some(remaining) = extract_deadline(request)
        && remaining < NEAR_DEADLINE_THRESHOLD
    {
        return Err(Status::deadline_exceeded(format!(
            "Request deadline too short: {}ms remaining (minimum {}ms)",
            remaining.as_millis(),
            NEAR_DEADLINE_THRESHOLD.as_millis(),
        )));
    }
    Ok(())
}

/// Compute the remaining deadline to propagate to a forwarded request.
///
/// If the incoming request has a gRPC deadline, returns the remaining time
/// (clamped to a minimum of 1ms to avoid zero-duration timeouts).
/// If no deadline is present, returns the `default_timeout`.
pub fn forwarding_timeout(grpc_deadline: Option<Duration>, default_timeout: Duration) -> Duration {
    match grpc_deadline {
        Some(remaining) => remaining.max(Duration::from_millis(1)),
        None => default_timeout,
    }
}

/// Parse the `grpc-timeout` header value per the gRPC spec.
///
/// Format: `{value}{unit}` where unit is one of: H (hours), M (minutes),
/// S (seconds), m (milliseconds), u (microseconds), n (nanoseconds).
/// Value is at most 8 decimal digits.
fn parse_grpc_timeout(s: &str) -> Option<Duration> {
    if s.is_empty() {
        return None;
    }

    let (value_str, unit) = s.split_at(s.len() - 1);

    if value_str.len() > 8 {
        return None;
    }

    let value: u64 = value_str.parse().ok()?;

    match unit {
        "H" => Some(Duration::from_secs(value * 3600)),
        "M" => Some(Duration::from_secs(value * 60)),
        "S" => Some(Duration::from_secs(value)),
        "m" => Some(Duration::from_millis(value)),
        "u" => Some(Duration::from_micros(value)),
        "n" => Some(Duration::from_nanos(value)),
        _ => None,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]
mod tests {
    use super::*;

    // ── parse_grpc_timeout ──────────────────────────────────────────────

    #[test]
    fn test_parse_hours() {
        assert_eq!(parse_grpc_timeout("3H"), Some(Duration::from_secs(3 * 3600)));
    }

    #[test]
    fn test_parse_minutes() {
        assert_eq!(parse_grpc_timeout("2M"), Some(Duration::from_secs(120)));
    }

    #[test]
    fn test_parse_seconds() {
        assert_eq!(parse_grpc_timeout("30S"), Some(Duration::from_secs(30)));
    }

    #[test]
    fn test_parse_milliseconds() {
        assert_eq!(parse_grpc_timeout("500m"), Some(Duration::from_millis(500)));
    }

    #[test]
    fn test_parse_microseconds() {
        assert_eq!(parse_grpc_timeout("30000000u"), Some(Duration::from_secs(30)));
    }

    #[test]
    fn test_parse_nanoseconds() {
        assert_eq!(parse_grpc_timeout("1000n"), Some(Duration::from_nanos(1000)));
    }

    #[test]
    fn test_parse_empty() {
        assert_eq!(parse_grpc_timeout(""), None);
    }

    #[test]
    fn test_parse_invalid_unit() {
        assert_eq!(parse_grpc_timeout("30f"), None);
    }

    #[test]
    fn test_parse_too_many_digits() {
        assert_eq!(parse_grpc_timeout("123456789H"), None);
    }

    #[test]
    fn test_parse_non_numeric() {
        assert_eq!(parse_grpc_timeout("abcS"), None);
    }

    // ── effective_timeout ───────────────────────────────────────────────

    #[test]
    fn test_effective_timeout_uses_proposal_timeout_when_no_deadline() {
        let proposal = Duration::from_secs(30);
        assert_eq!(effective_timeout(proposal, None), proposal);
    }

    #[test]
    fn test_effective_timeout_uses_shorter_deadline() {
        let proposal = Duration::from_secs(30);
        let deadline = Duration::from_secs(5);
        assert_eq!(effective_timeout(proposal, Some(deadline)), deadline);
    }

    #[test]
    fn test_effective_timeout_uses_proposal_when_shorter() {
        let proposal = Duration::from_secs(5);
        let deadline = Duration::from_secs(30);
        assert_eq!(effective_timeout(proposal, Some(deadline)), proposal);
    }

    #[test]
    fn test_effective_timeout_equal_values() {
        let duration = Duration::from_secs(10);
        assert_eq!(effective_timeout(duration, Some(duration)), duration);
    }

    // ── check_near_deadline ─────────────────────────────────────────────

    #[test]
    fn test_check_near_deadline_no_deadline_passes() {
        let request = Request::new(());
        assert!(check_near_deadline(&request).is_ok());
    }

    #[test]
    fn test_check_near_deadline_sufficient_time_passes() {
        let mut request = Request::new(());
        request.set_timeout(Duration::from_secs(5));
        assert!(check_near_deadline(&request).is_ok());
    }

    #[test]
    fn test_check_near_deadline_at_threshold_passes() {
        let mut request = Request::new(());
        request.set_timeout(Duration::from_millis(100));
        assert!(check_near_deadline(&request).is_ok());
    }

    #[test]
    fn test_check_near_deadline_below_threshold_rejects() {
        let mut request = Request::new(());
        request.set_timeout(Duration::from_millis(50));
        let err = check_near_deadline(&request).unwrap_err();
        assert_eq!(err.code(), tonic::Code::DeadlineExceeded);
        assert!(err.message().contains("50ms remaining"));
        assert!(err.message().contains("minimum 100ms"));
    }

    #[test]
    fn test_check_near_deadline_zero_rejects() {
        let mut request = Request::new(());
        request.set_timeout(Duration::ZERO);
        let err = check_near_deadline(&request).unwrap_err();
        assert_eq!(err.code(), tonic::Code::DeadlineExceeded);
    }

    // ── forwarding_timeout ──────────────────────────────────────────────

    #[test]
    fn test_forwarding_timeout_with_deadline() {
        let deadline = Duration::from_secs(5);
        let default = Duration::from_secs(30);
        assert_eq!(forwarding_timeout(Some(deadline), default), deadline);
    }

    #[test]
    fn test_forwarding_timeout_without_deadline() {
        let default = Duration::from_secs(30);
        assert_eq!(forwarding_timeout(None, default), default);
    }

    #[test]
    fn test_forwarding_timeout_clamps_to_minimum() {
        let deadline = Duration::ZERO;
        let default = Duration::from_secs(30);
        assert_eq!(forwarding_timeout(Some(deadline), default), Duration::from_millis(1));
    }

    #[test]
    fn test_forwarding_timeout_very_short_deadline() {
        let deadline = Duration::from_micros(500);
        let default = Duration::from_secs(30);
        assert_eq!(forwarding_timeout(Some(deadline), default), Duration::from_millis(1));
    }

    // ── extract_deadline ────────────────────────────────────────────────

    #[test]
    fn test_extract_deadline_no_timeout() {
        let request = Request::new(());
        assert!(extract_deadline(&request).is_none());
    }

    #[test]
    fn test_extract_deadline_with_timeout() {
        let mut request = Request::new(());
        // set_timeout uses the grpc-timeout header format
        request.set_timeout(Duration::from_secs(10));
        let deadline = extract_deadline(&request).expect("should have deadline");
        // set_timeout converts to microseconds (e.g., "10000000u")
        assert_eq!(deadline, Duration::from_secs(10));
    }

    #[test]
    fn test_extract_deadline_from_metadata_map() {
        let mut request = Request::new(());
        request.set_timeout(Duration::from_millis(500));
        let metadata = request.metadata().clone();
        let deadline =
            extract_deadline_from_metadata(&metadata).expect("should have deadline from metadata");
        assert_eq!(deadline, Duration::from_millis(500));
    }
}
