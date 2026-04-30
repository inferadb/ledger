//! Per-RPC context object passed to every wire-protocol handler.
//!
//! [`RequestContext`] is the dispatcher's hand-off to a handler: it surfaces
//! every value a handler currently reads from `tonic::Request<T>` so the
//! gRPC → wire migration is a mechanical rewrite rather than a redesign.
//!
//! The context is built once per RPC by the dispatcher (see Phase 0.0.B
//! `build_request_context(...)` in
//! `docs/superpowers/plans/2026-04-30-region-aware-sdk-multiplexer.md`) and
//! lives for the duration of the handler invocation. It is `Send + Sync`; it
//! crosses task boundaries on every dispatch.

use std::{net::SocketAddr, sync::Arc, time::Instant};

use inferadb_ledger_types::{AppId, OrganizationId, UserId};
use tokio_util::sync::CancellationToken;

/// How the caller authenticated to the wire connection.
///
/// Determined once at connection establishment (the auth handshake produces
/// the cached per-connection JWT in decision D1) and reused on every RPC the
/// connection issues. Process-internal — never serialized.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AuthMethod {
    /// User JWT from interactive login.
    UserSession,
    /// App authenticated via signed `client_assertion` JWT.
    AppClientAssertion,
    /// App authenticated via `client_id` + `client_secret`.
    AppClientSecret,
    /// App authenticated via mTLS client certificate.
    AppMtls,
    /// Inter-node Raft / system traffic.
    NodePeer,
    /// Pre-authentication or health-check probes.
    Anonymous,
}

/// Identity of the authenticated caller as cached on the wire connection.
///
/// Resolved at connection establishment from the JWT presented during the
/// auth handshake (decision D1). `user_id`, `app_id`, and `organization_id`
/// are populated as appropriate for the [`AuthMethod`]; for `Anonymous` and
/// `NodePeer` connections they may all be `None`.
///
/// Internal `{Entity}Id(i64)` newtypes are used here, not slugs:
/// `RequestContext` is server-internal, and authorization checks operate on
/// internal IDs. Slug → ID resolution happens at the handler boundary before
/// the context is built.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallerIdentity {
    /// Authenticated user, when the connection authenticated as a user.
    pub user_id: Option<UserId>,
    /// Authenticated app, when the connection authenticated as an app.
    pub app_id: Option<AppId>,
    /// Organization the caller is acting within, when known.
    pub organization_id: Option<OrganizationId>,
    /// Mechanism that produced this identity.
    pub auth_method: AuthMethod,
    /// Wall-clock instant at which the cached JWT expires.
    ///
    /// The dispatcher checks `Instant::now() >= token_expiry` on every RPC
    /// (decision D1, per-RPC token-expiry check) and returns `Unauthenticated`
    /// when the token has expired, even though the auth handshake itself only
    /// runs once per connection.
    pub token_expiry: Instant,
    /// Global signing-key epoch the JWT was issued under.
    ///
    /// If the cluster's signing-key epoch advances past this value, the
    /// revocation watchdog closes the connection: every subsequent RPC on
    /// this connection would carry a token signed by a revoked key.
    pub revocation_epoch: u64,
}

/// Connection-level health and observability hook.
///
/// Stub for now — Phase 14 of the rollout populates the fields used by
/// `WireClient::status()` (see Step 9 of Phase 0.0.B in
/// `docs/superpowers/plans/2026-04-30-region-aware-sdk-multiplexer.md`).
/// Defined here so the [`RequestContext`] surface compiles today and Phase
/// 14 is a mechanical field add rather than a context-shape change.
#[derive(Debug, Default)]
pub struct ConnectionMetrics;

/// Per-RPC context the dispatcher passes to every handler.
///
/// Contains the request-scoped identifiers the wire frame carries
/// ([`FrameHeader`](crate::FrameHeader)), the cached connection-level
/// identity, and the cancellation handle the handler races against the
/// deadline. Built once per RPC by `build_request_context(...)` (Phase 0.0.B
/// Step 2) and consumed by the handler.
///
/// `Clone` is value-cheap: every field is either `Copy`, an `Arc`, or a
/// `CancellationToken` (already `Arc`-internally). Cloning lets handlers
/// hand the context to spawned subtasks without lifetime gymnastics.
///
/// Intentionally not `PartialEq`/`Eq`: [`CancellationToken`] does not
/// implement them and a request context's identity is its `request_id`, not
/// structural equality.
#[derive(Debug, Clone)]
pub struct RequestContext {
    /// 128-bit request correlation ID — mirrors `FrameHeader::request_id`.
    pub request_id: u128,
    /// Phase 12 idempotency contract from `FrameHeader::idempotency_token`;
    /// all-zero bytes mean no idempotency was requested.
    pub idempotency_token: [u8; 16],
    /// OpenTelemetry W3C trace ID from `FrameHeader::trace_id`.
    pub trace_id: [u8; 16],
    /// OpenTelemetry W3C span ID from `FrameHeader::span_id`.
    pub span_id: [u8; 8],
    /// OpenTelemetry W3C trace flags from `FrameHeader::trace_flags`.
    pub trace_flags: u8,
    /// Phase 5 causality token from `FrameHeader::causality_commit_index`;
    /// zero means no constraint.
    pub causality_commit_index: u64,
    /// Monotonic deadline for this RPC, when one was requested.
    ///
    /// The dispatcher derives this from `FrameHeader::deadline_unix_nanos`
    /// (wall-clock UNIX nanos) by computing
    /// `Instant::now() + (deadline_unix_nanos - SystemTime::now())`. `None`
    /// when the frame's `deadline_unix_nanos` is zero (no deadline).
    pub deadline: Option<Instant>,
    /// Caller identity from the cached per-connection JWT (decision D1).
    pub caller_identity: CallerIdentity,
    /// Remote socket address of the connecting peer.
    pub peer_addr: SocketAddr,
    /// SDK version string from connection-level metadata, when present.
    ///
    /// Mirrors the existing `x-sdk-version` gRPC header. Populated once at
    /// connection establishment and reused on every RPC.
    pub sdk_version: Option<String>,
    /// Forwarded client IP if the connection arrived through a proxy.
    ///
    /// Mirrors the existing `x-forwarded-for` gRPC header (first hop only).
    /// `None` when the peer connected directly.
    pub forwarded_for: Option<String>,
    /// Alias for [`Self::request_id`] preserved for the canonical-log-line
    /// path in `crates/raft/src/logging/request_context.rs::CanonicalLogLine`,
    /// which spells the same value `correlation_id`.
    pub correlation_id: u128,
    /// Token the handler races against the deadline; cancelled by the
    /// dispatcher on connection close, deadline expiry, or explicit abort.
    pub cancellation: CancellationToken,
    /// Connection-level health and observability hook.
    ///
    /// Stub until Phase 14 (`WireClient::status()`); the `Arc<ConnectionMetrics>`
    /// shape is final so Phase 14 only adds fields to [`ConnectionMetrics`]
    /// rather than reshaping every dispatcher call site.
    pub conn_metrics: Arc<ConnectionMetrics>,
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]

    use super::*;

    fn sample_identity() -> CallerIdentity {
        CallerIdentity {
            user_id: Some(UserId::new(1)),
            app_id: None,
            organization_id: Some(OrganizationId::new(2)),
            auth_method: AuthMethod::UserSession,
            token_expiry: Instant::now(),
            revocation_epoch: 0,
        }
    }

    #[test]
    fn request_context_constructs() {
        let ctx = RequestContext {
            request_id: 0xDEAD_BEEF_CAFE_F00D_DEAD_BEEF_CAFE_F00D,
            idempotency_token: [1u8; 16],
            trace_id: [2u8; 16],
            span_id: [3u8; 8],
            trace_flags: 0x01,
            causality_commit_index: 42,
            deadline: Some(Instant::now()),
            caller_identity: sample_identity(),
            peer_addr: "127.0.0.1:0".parse().unwrap(),
            sdk_version: Some("inferadb-ledger-sdk/0.1.0".to_string()),
            forwarded_for: Some("203.0.113.7".to_string()),
            correlation_id: 0xDEAD_BEEF_CAFE_F00D_DEAD_BEEF_CAFE_F00D,
            cancellation: CancellationToken::new(),
            conn_metrics: Arc::new(ConnectionMetrics),
        };

        assert_eq!(ctx.request_id, ctx.correlation_id);
        assert_eq!(ctx.idempotency_token, [1u8; 16]);
        assert_eq!(ctx.trace_id, [2u8; 16]);
        assert_eq!(ctx.span_id, [3u8; 8]);
        assert_eq!(ctx.trace_flags, 0x01);
        assert_eq!(ctx.causality_commit_index, 42);
        assert!(ctx.deadline.is_some());
        assert_eq!(ctx.caller_identity.auth_method, AuthMethod::UserSession);
        assert_eq!(ctx.peer_addr.port(), 0);
        assert_eq!(ctx.sdk_version.as_deref(), Some("inferadb-ledger-sdk/0.1.0"));
        assert_eq!(ctx.forwarded_for.as_deref(), Some("203.0.113.7"));
        assert!(!ctx.cancellation.is_cancelled());
    }

    #[test]
    fn auth_method_variants_distinct() {
        let all = [
            AuthMethod::UserSession,
            AuthMethod::AppClientAssertion,
            AuthMethod::AppClientSecret,
            AuthMethod::AppMtls,
            AuthMethod::NodePeer,
            AuthMethod::Anonymous,
        ];
        for (i, a) in all.iter().enumerate() {
            for (j, b) in all.iter().enumerate() {
                if i == j {
                    assert_eq!(a, b);
                } else {
                    assert_ne!(a, b, "variants {i} and {j} collide");
                }
            }
        }
    }

    #[test]
    fn caller_identity_clone_is_independent() {
        let original = sample_identity();
        let mut cloned = original.clone();
        cloned.revocation_epoch = 99;

        assert_eq!(original.revocation_epoch, 0);
        assert_eq!(cloned.revocation_epoch, 99);
    }

    #[test]
    fn request_context_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<RequestContext>();
    }
}
