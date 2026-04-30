//! Request deadline propagation and timeout management.
//!
//! Provides utilities for computing effective timeouts (the minimum of the
//! server-configured proposal timeout and the client-specified gRPC
//! deadline) and propagating remaining deadlines to forwarded calls.
//!
//! ## How It Works
//!
//! 1. SDK sets `grpc-timeout` header on every request (derived from `ClientConfig::timeout`)
//! 2. The wire dispatcher parses the header into [`inferadb_ledger_wire::RequestContext::deadline`]
//! 3. Handlers compute `min(proposal_timeout, grpc_deadline)` via [`effective_timeout`]

use std::time::Duration;

/// Computes the effective timeout for a Raft proposal.
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

/// Computes the remaining deadline to propagate to a forwarded request.
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]
mod tests {
    use super::*;

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
}
