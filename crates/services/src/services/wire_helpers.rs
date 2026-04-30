//! Wire-protocol error helpers (Phase 0.0.E.4).
//!
//! Mirrors the tonic-side helpers in `metadata` (super::metadata) and
//! `error_details` (super::error_details), producing
//! [`inferadb_ledger_wire::WireError`] for the new wire transport. Coexists
//! with the tonic helpers during the migration — every per-service shim
//! uses these to convert post-handler `tonic::Status` values into
//! `WireError` for the wire impl's return.
//!
//! After F.1 (drop tonic deps) inlines service bodies onto the wire trait
//! directly, this module's `tonic_status_to_wire_error` shim becomes
//! obsolete and the remaining `wire_error_with_correlation` /
//! `build_wire_error` helpers become the canonical primitives.

use std::collections::BTreeMap;

use inferadb_ledger_wire::{
    AuthMethod, CallerIdentity, ConnectionMetrics, ErrorCode, RequestContext as WireRequestContext,
    WireError,
};
use tonic::Code;

/// Build a `WireError` with structured context.
///
/// Mirrors `error_details::build_error_details` (super::error_details::build_error_details)
/// but produces `WireError` directly rather than the proto `ErrorDetails`
/// shape. Use this from wire-trait handler bodies that build their own
/// errors (rate-limit, validation, custom precondition).
///
/// `error_code` is the numeric internal error code (e.g. "3204" for
/// rate-limit). It's stored under `context["error_code"]` so the SDK's
/// `from_wire_error_with_correlation` can detect migration sub-codes
/// (3106 OrganizationMigrating, 3107 UserMigrating).
#[must_use]
pub fn build_wire_error(
    code: ErrorCode,
    message: impl Into<String>,
    error_code: impl Into<String>,
    retryable: bool,
    retry_after_ms: u64,
    context: BTreeMap<String, String>,
    suggested_action: impl Into<String>,
) -> WireError {
    let mut ctx = context;
    let err_code_str = error_code.into();
    if !err_code_str.is_empty() {
        ctx.insert("error_code".to_string(), err_code_str);
    }
    WireError {
        code,
        message: message.into(),
        retryable,
        retry_after_ms,
        context: ctx,
        suggested_action: suggested_action.into(),
    }
}

/// Stamp wire-frame correlation metadata onto a `WireError`.
///
/// The wire-protocol equivalent of
/// `metadata::status_with_correlation` (super::metadata::status_with_correlation):
/// surfaces `request_id` / `trace_id` into the error's context map so the
/// dispatcher can write them into response-frame headers without the
/// handler having to know about frame layout. Tests should round-trip
/// through this for the same reasons the existing tonic helpers are
/// load-bearing — see the metadata.rs golden rule in the services CLAUDE.md.
#[must_use]
pub fn wire_error_with_correlation(
    mut err: WireError,
    request_id: u128,
    trace_id: [u8; 16],
) -> WireError {
    // request_id lands in the frame header; surface as a context key as
    // well so log lines that look at the WireError directly (without the
    // frame) carry it. Format as 32-char lowercase hex matching how the
    // tonic side surfaces `x-request-id`.
    err.context.insert("request_id".to_string(), format!("{request_id:032x}"));
    // Trace ID: same treatment. All-zero trace ID is the "no trace" signal
    // — skip it to avoid polluting context with a useless field.
    if trace_id != [0u8; 16] {
        let hex: String = trace_id.iter().map(|b| format!("{b:02x}")).collect();
        err.context.insert("trace_id".to_string(), hex);
    }
    err
}

/// Build a synthetic wire [`RequestContext`](inferadb_ledger_wire::RequestContext)
/// from a tonic `Request<T>`.
///
/// Surviving tonic handlers call this once at the top of each handler to
/// obtain a wire-shaped context, then pass `&wire_ctx` down through
/// `ServiceContext::propose_*` and `make_request_context_from`. F.1.f.2
/// deletes this bridge along with the tonic service impls.
///
/// The synthesized context populates everything wire-side callees may
/// read: `request_id` (randomized — wire frame correlation lives in the
/// future wire dispatcher); `trace_id` / `span_id` from W3C `traceparent`
/// (zeros when absent or malformed); `sdk_version` from `x-sdk-version`;
/// `forwarded_for` from `x-forwarded-for`; `caller_identity` defaults to
/// [`AuthMethod::Anonymous`] (the proto-slug-based caller extraction
/// continues via `super::helpers::extract_caller_from_proto_slug` for
/// surviving tonic paths); `peer_addr` from `Request::remote_addr()` (or
/// `0.0.0.0:0` fallback); `deadline` from `grpc-timeout` header.
#[must_use]
pub fn tonic_request_to_wire_context<T>(req: &tonic::Request<T>) -> WireRequestContext {
    let metadata = req.metadata();

    // Trace context: parse W3C traceparent if present; fall back to all-zeros.
    // Format: `00-{trace_id_hex32}-{span_id_hex16}-{flags_hex2}`.
    let mut trace_id = [0u8; 16];
    let mut span_id = [0u8; 8];
    let mut trace_flags: u8 = 0;
    if let Some(tp) = metadata.get("traceparent")
        && let Ok(tp_str) = tp.to_str()
    {
        let parts: Vec<&str> = tp_str.split('-').collect();
        if parts.len() >= 4 {
            if let Ok(decoded) = decode_hex_into::<16>(parts[1]) {
                trace_id = decoded;
            }
            if let Ok(decoded) = decode_hex_into::<8>(parts[2]) {
                span_id = decoded;
            }
            if let Ok(flags) = u8::from_str_radix(parts[3], 16) {
                trace_flags = flags;
            }
        }
    }

    // SDK version + forwarded-for: optional headers — capture as `String`.
    let sdk_version =
        metadata.get("x-sdk-version").and_then(|v| v.to_str().ok()).map(str::to_string);
    let forwarded_for =
        metadata.get("x-forwarded-for").and_then(|v| v.to_str().ok()).and_then(|ips| {
            // x-forwarded-for may carry a comma-delimited proxy chain; the
            // first hop is the original client IP.
            ips.split(',').next().map(str::trim).filter(|s| !s.is_empty()).map(str::to_string)
        });

    // Deadline: extract via the existing tonic helper so the wire context
    // observes the same deadline semantics the propose path used previously.
    let deadline = super::tonic_compat::extract_deadline_from_metadata(metadata)
        .map(|remaining| std::time::Instant::now() + remaining);

    // Synthesize a per-RPC request id. Wire-mode dispatchers populate this
    // from the frame header; the tonic adapter has no equivalent, so each
    // tonic-bridged RPC gets a fresh random id.
    let request_id = uuid_to_u128(uuid::Uuid::new_v4());

    // Caller identity: tonic handlers don't carry connection-cached JWT
    // identity. The proto-slug-based `extract_caller_from_proto_slug` path
    // continues to populate the log context's `caller` field. Anonymous +
    // empty user/app/org reflects that absence honestly.
    let caller_identity = CallerIdentity {
        user_id: None,
        app_id: None,
        organization_id: None,
        auth_method: AuthMethod::Anonymous,
        // `Instant::now()` for `token_expiry` is the safest default — every
        // expiry check `Instant::now() >= token_expiry` will fail closed for
        // a downstream wire-aware caller, which matches "no token". The
        // tonic adapter should never reach a token-expiry check today.
        token_expiry: std::time::Instant::now(),
        revocation_epoch: 0,
    };

    let peer_addr = req.remote_addr().unwrap_or_else(|| {
        "0.0.0.0:0".parse().unwrap_or_else(|_| {
            // Unreachable: "0.0.0.0:0" parses on every platform.
            std::net::SocketAddr::from(([0, 0, 0, 0], 0))
        })
    });

    WireRequestContext {
        request_id,
        idempotency_token: [0u8; 16],
        trace_id,
        span_id,
        trace_flags,
        causality_commit_index: 0,
        deadline,
        caller_identity,
        peer_addr,
        sdk_version,
        forwarded_for,
        correlation_id: request_id,
        cancellation: tokio_util::sync::CancellationToken::new(),
        conn_metrics: std::sync::Arc::new(ConnectionMetrics),
    }
}

/// Reconstructs the W3C `traceparent` header value from a wire
/// [`RequestContext`](inferadb_ledger_wire::RequestContext).
///
/// Returns `None` when the context carries no trace ID (all-zero
/// `trace_id`); otherwise returns
/// `"00-{trace_id_hex32}-{span_id_hex16}-{flags_hex2}"`. Used by saga
/// dispatch sites that need to propagate the originating trace into a
/// regional proposal — the wire context decomposes traceparent into
/// `trace_id` / `span_id` / `trace_flags` fields, but downstream sagas
/// expect the assembled header.
#[must_use]
pub fn wire_context_traceparent(ctx: &WireRequestContext) -> Option<String> {
    if ctx.trace_id == [0u8; 16] {
        return None;
    }
    let trace_id_hex: String = ctx.trace_id.iter().map(|b| format!("{b:02x}")).collect();
    let span_id_hex: String = ctx.span_id.iter().map(|b| format!("{b:02x}")).collect();
    Some(format!("00-{trace_id_hex}-{span_id_hex}-{:02x}", ctx.trace_flags))
}

/// Decodes a fixed-length lowercase-hex string into an `N`-byte array.
///
/// Returns `Err(())` for any string whose length is not exactly `2 * N`
/// or whose contents include a non-hex character.
fn decode_hex_into<const N: usize>(s: &str) -> Result<[u8; N], ()> {
    if s.len() != N * 2 {
        return Err(());
    }
    let bytes = s.as_bytes();
    let mut out = [0u8; N];
    for (i, chunk) in bytes.chunks_exact(2).enumerate() {
        let hi = hex_nibble(chunk[0])?;
        let lo = hex_nibble(chunk[1])?;
        out[i] = (hi << 4) | lo;
    }
    Ok(out)
}

/// Decodes a single ASCII hex nibble (`0-9`, `a-f`, `A-F`).
fn hex_nibble(b: u8) -> Result<u8, ()> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(b - b'a' + 10),
        b'A'..=b'F' => Ok(b - b'A' + 10),
        _ => Err(()),
    }
}

/// Reinterprets a UUID as a `u128` (preserving byte order).
fn uuid_to_u128(u: uuid::Uuid) -> u128 {
    u.as_u128()
}

/// Convert a `tonic::Status` into a `WireError`.
///
/// The shim delegation primitive — surviving tonic-shaped helpers call this
/// to translate a `tonic::Status` (built by [`wire_error_to_tonic_status`]
/// or returned from a tonic-bridged helper) into a wire-trait return.
/// `Status.details` carries the `WireError`'s context + suggested-action
/// fields as a JSON blob; the bridge is byte-symmetric with
/// [`wire_error_to_tonic_status`] when the status came from the same hop.
///
/// Statuses without a JSON `details` payload (e.g. `Status::not_found("…")`
/// from a non-bridge call site) yield a `WireError` with empty context.
/// Any `retry-after-ms` metadata header always wins over the JSON payload —
/// matches the precedent the previous proto-encoded form set.
#[must_use]
pub fn tonic_status_to_wire_error(status: tonic::Status) -> WireError {
    let fallback_code = tonic_code_to_wire(status.code());
    let fallback_message = status.message().to_string();

    // Decode the JSON-encoded `WireError` payload (if any). This is the
    // inverse of `wire_error_to_tonic_status`, which serializes the entire
    // `WireError` into `status.details()` as JSON.
    let details_bytes = status.details();
    let mut wire_err = if details_bytes.is_empty() {
        WireError {
            code: fallback_code,
            message: fallback_message,
            retryable: false,
            retry_after_ms: 0,
            context: BTreeMap::new(),
            suggested_action: String::new(),
        }
    } else {
        match serde_json::from_slice::<WireError>(details_bytes) {
            Ok(decoded) => decoded,
            // Non-bridge tonic Status (no JSON details) — fall back to the
            // status's bare code + message, leaving context empty.
            Err(_) => WireError {
                code: fallback_code,
                message: fallback_message,
                retryable: false,
                retry_after_ms: 0,
                context: BTreeMap::new(),
                suggested_action: String::new(),
            },
        }
    };

    // Pull retry-after-ms from response metadata (the rate-limited path the
    // existing tonic SDK reads). Wins over the JSON payload if both are
    // present, matching the precedent the previous proto-encoded form set.
    if let Some(value) = status.metadata().get("retry-after-ms")
        && let Ok(s) = value.to_str()
        && let Ok(ms) = s.parse::<u64>()
    {
        wire_err.retry_after_ms = ms;
    }

    wire_err
}

/// Convert a `WireError` into a `tonic::Status`.
///
/// The reverse of [`tonic_status_to_wire_error`]. Surviving tonic-shaped
/// helpers call this to bridge a `WireError` (already populated with
/// context + retryability + suggested action) back to a `tonic::Status`
/// suitable for the few internal call sites still typed against
/// `tonic::Status`.
///
/// The entire `WireError` is JSON-encoded into `status.details()` so the
/// inverse [`tonic_status_to_wire_error`] can recover it byte-for-byte on
/// the same hop. The status's gRPC code mirrors the wire variant via
/// `wire_code_to_tonic`.
#[must_use]
pub fn wire_error_to_tonic_status(err: WireError) -> tonic::Status {
    let code = wire_code_to_tonic(err.code);
    let message = err.message.clone();
    match serde_json::to_vec(&err) {
        Ok(buf) => tonic::Status::with_details(code, message, buf.into()),
        // Serialization of a `WireError` (BTreeMap<String, String> +
        // primitives) cannot fail in practice; fall back to a bare status
        // rather than panic. The lossy fallback drops context/suggested
        // action, but the gRPC code + message still convey the failure.
        Err(_) => tonic::Status::new(code, message),
    }
}

/// Map a wire-protocol [`ErrorCode`] back to a gRPC [`tonic::Code`].
///
/// Inverse of [`tonic_code_to_wire`]. The wire enum is narrower than gRPC's
/// `Code` (no `Ok`/`Cancelled`/`Unknown`/`DataLoss`/etc.), so the mapping is
/// surjective in one direction only. Wire-only variants (`Expired`,
/// `TooManyAttempts`, the four `Invitation*` cases, `Deprecated`) collapse to
/// the closest gRPC code that preserves retry semantics on the SDK side.
fn wire_code_to_tonic(code: ErrorCode) -> Code {
    match code {
        ErrorCode::NotFound => Code::NotFound,
        ErrorCode::AlreadyExists => Code::AlreadyExists,
        ErrorCode::FailedPrecondition => Code::FailedPrecondition,
        ErrorCode::PermissionDenied => Code::PermissionDenied,
        ErrorCode::InvalidArgument => Code::InvalidArgument,
        ErrorCode::Internal => Code::Internal,
        ErrorCode::Unauthenticated => Code::Unauthenticated,
        ErrorCode::RateLimited => Code::ResourceExhausted,
        // Wire-only variants — collapse to the gRPC code that preserves
        // SDK retry classification. The numeric `error_code` in the
        // `ErrorDetails` context is the disambiguator on round-trip.
        ErrorCode::Expired => Code::FailedPrecondition,
        ErrorCode::TooManyAttempts => Code::ResourceExhausted,
        ErrorCode::InvitationRateLimited => Code::ResourceExhausted,
        ErrorCode::InvitationAlreadyResolved => Code::FailedPrecondition,
        ErrorCode::InvitationEmailMismatch => Code::FailedPrecondition,
        ErrorCode::InvitationAlreadyMember => Code::FailedPrecondition,
        ErrorCode::InvitationDuplicatePending => Code::AlreadyExists,
        ErrorCode::StaleRouting => Code::Unavailable,
        ErrorCode::Deprecated => Code::Unimplemented,
    }
}

/// Map a gRPC [`tonic::Code`] to a wire-protocol [`ErrorCode`].
///
/// One-to-one for codes the wire protocol shares with gRPC; ambiguous gRPC
/// codes (`Cancelled`, `Unknown`, `DataLoss`) collapse to `Internal`.
/// `ResourceExhausted` maps to `RateLimited` (the dominant cause; the
/// invitation-rate-limit and too-many-attempts cases need explicit
/// disambiguation via `error_code` context, since the SDK's inverse mapping
/// has the same overlap).
fn tonic_code_to_wire(code: Code) -> ErrorCode {
    match code {
        Code::Ok => ErrorCode::Internal, // unreachable: status was OK
        Code::NotFound => ErrorCode::NotFound,
        Code::AlreadyExists => ErrorCode::AlreadyExists,
        Code::FailedPrecondition => ErrorCode::FailedPrecondition,
        Code::PermissionDenied => ErrorCode::PermissionDenied,
        Code::InvalidArgument => ErrorCode::InvalidArgument,
        Code::Internal => ErrorCode::Internal,
        Code::Unauthenticated => ErrorCode::Unauthenticated,
        Code::ResourceExhausted => ErrorCode::RateLimited,
        Code::DeadlineExceeded => ErrorCode::FailedPrecondition,
        Code::Unavailable => ErrorCode::StaleRouting,
        Code::Aborted => ErrorCode::FailedPrecondition,
        Code::Cancelled
        | Code::Unknown
        | Code::DataLoss
        | Code::OutOfRange
        | Code::Unimplemented => ErrorCode::Internal,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    #[test]
    fn build_wire_error_populates_error_code_in_context() {
        let mut ctx = BTreeMap::new();
        ctx.insert("foo".to_string(), "bar".to_string());
        let err = build_wire_error(
            ErrorCode::RateLimited,
            "throttled",
            "3204",
            true,
            500,
            ctx,
            "back off",
        );
        assert_eq!(err.code, ErrorCode::RateLimited);
        assert_eq!(err.message, "throttled");
        assert!(err.retryable);
        assert_eq!(err.retry_after_ms, 500);
        assert_eq!(err.context.get("foo").map(String::as_str), Some("bar"));
        assert_eq!(err.context.get("error_code").map(String::as_str), Some("3204"));
        assert_eq!(err.suggested_action, "back off");
    }

    #[test]
    fn build_wire_error_skips_empty_error_code() {
        let err = build_wire_error(ErrorCode::Internal, "x", "", false, 0, BTreeMap::new(), "");
        assert!(!err.context.contains_key("error_code"));
    }

    #[test]
    fn wire_error_with_correlation_writes_request_id_hex() {
        let err = WireError {
            code: ErrorCode::Internal,
            message: "x".to_string(),
            retryable: false,
            retry_after_ms: 0,
            context: BTreeMap::new(),
            suggested_action: String::new(),
        };
        let stamped =
            wire_error_with_correlation(err, 0xDEAD_BEEF_CAFE_F00D_DEAD_BEEF_CAFE_F00D, [0u8; 16]);
        assert_eq!(
            stamped.context.get("request_id").map(String::as_str),
            Some("deadbeefcafef00ddeadbeefcafef00d")
        );
        // Empty trace_id should NOT be written
        assert!(!stamped.context.contains_key("trace_id"));
    }

    #[test]
    fn wire_error_with_correlation_writes_trace_id_hex_when_set() {
        let err = WireError {
            code: ErrorCode::Internal,
            message: "x".to_string(),
            retryable: false,
            retry_after_ms: 0,
            context: BTreeMap::new(),
            suggested_action: String::new(),
        };
        let trace_id = [
            0xAB, 0xCD, 0xEF, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B,
            0x0C, 0x0D,
        ];
        let stamped = wire_error_with_correlation(err, 0, trace_id);
        assert_eq!(
            stamped.context.get("trace_id").map(String::as_str),
            Some("abcdef0102030405060708090a0b0c0d")
        );
    }

    #[test]
    fn tonic_code_to_wire_one_to_one() {
        assert_eq!(tonic_code_to_wire(Code::NotFound), ErrorCode::NotFound);
        assert_eq!(tonic_code_to_wire(Code::AlreadyExists), ErrorCode::AlreadyExists);
        assert_eq!(tonic_code_to_wire(Code::FailedPrecondition), ErrorCode::FailedPrecondition);
        assert_eq!(tonic_code_to_wire(Code::PermissionDenied), ErrorCode::PermissionDenied);
        assert_eq!(tonic_code_to_wire(Code::InvalidArgument), ErrorCode::InvalidArgument);
        assert_eq!(tonic_code_to_wire(Code::Internal), ErrorCode::Internal);
        assert_eq!(tonic_code_to_wire(Code::Unauthenticated), ErrorCode::Unauthenticated);
        assert_eq!(tonic_code_to_wire(Code::ResourceExhausted), ErrorCode::RateLimited);
        assert_eq!(tonic_code_to_wire(Code::Unavailable), ErrorCode::StaleRouting);
        assert_eq!(tonic_code_to_wire(Code::DeadlineExceeded), ErrorCode::FailedPrecondition);
        assert_eq!(tonic_code_to_wire(Code::Aborted), ErrorCode::FailedPrecondition);
        assert_eq!(tonic_code_to_wire(Code::Cancelled), ErrorCode::Internal);
        assert_eq!(tonic_code_to_wire(Code::Unknown), ErrorCode::Internal);
        assert_eq!(tonic_code_to_wire(Code::DataLoss), ErrorCode::Internal);
    }

    #[test]
    fn tonic_status_to_wire_error_preserves_message_and_code() {
        let status = tonic::Status::not_found("missing");
        let err = tonic_status_to_wire_error(status);
        assert_eq!(err.code, ErrorCode::NotFound);
        assert_eq!(err.message, "missing");
        assert!(err.context.is_empty());
        assert!(!err.retryable);
        assert_eq!(err.retry_after_ms, 0);
    }

    #[test]
    fn tonic_status_to_wire_error_decodes_json_payload_round_trip() {
        // Bridge round-trip: build a `WireError`, encode via
        // `wire_error_to_tonic_status`, decode via the inverse and verify
        // every field survives.
        let mut ctx = BTreeMap::new();
        ctx.insert("organization_id".to_string(), "42".to_string());
        ctx.insert("error_code".to_string(), "3204".to_string());
        let original = WireError {
            code: ErrorCode::RateLimited,
            message: "throttled".to_string(),
            retryable: true,
            retry_after_ms: 500,
            context: ctx,
            suggested_action: "wait and retry".to_string(),
        };

        let status = wire_error_to_tonic_status(original);
        assert_eq!(status.code(), tonic::Code::ResourceExhausted);

        let err = tonic_status_to_wire_error(status);
        assert_eq!(err.code, ErrorCode::RateLimited);
        assert!(err.retryable);
        assert_eq!(err.retry_after_ms, 500);
        assert_eq!(err.context.get("organization_id").map(String::as_str), Some("42"));
        assert_eq!(err.context.get("error_code").map(String::as_str), Some("3204"));
        assert_eq!(err.suggested_action, "wait and retry");
    }

    #[test]
    fn wire_error_to_tonic_status_round_trip_codes() {
        // Every wire ErrorCode round-trips through the tonic→wire mapping
        // via wire_error_to_tonic_status → tonic_status_to_wire_error.
        //
        // Wire-only variants collapse to a closest-gRPC code in the
        // forward direction; the inverse maps that gRPC code back to a
        // canonical wire variant — so the canonical-form round-trip is
        // (canonical wire variant) → (wider gRPC code) → (canonical wire variant).
        let canonical_pairs = [
            ErrorCode::NotFound,
            ErrorCode::AlreadyExists,
            ErrorCode::FailedPrecondition,
            ErrorCode::PermissionDenied,
            ErrorCode::InvalidArgument,
            ErrorCode::Internal,
            ErrorCode::Unauthenticated,
            ErrorCode::RateLimited,
            ErrorCode::StaleRouting,
        ];
        for original in canonical_pairs {
            let err = WireError::new(original, "msg");
            let status = wire_error_to_tonic_status(err);
            let back = tonic_status_to_wire_error(status);
            assert_eq!(back.code, original, "round-trip failed for {original:?}");
            assert_eq!(back.message, "msg");
        }
    }

    #[test]
    fn wire_error_to_tonic_status_preserves_context_and_action() {
        let mut ctx = BTreeMap::new();
        ctx.insert("organization_id".to_string(), "42".to_string());
        ctx.insert("error_code".to_string(), "3204".to_string());
        let err = WireError {
            code: ErrorCode::RateLimited,
            message: "throttled".to_string(),
            retryable: true,
            retry_after_ms: 750,
            context: ctx,
            suggested_action: "wait and retry".to_string(),
        };
        let status = wire_error_to_tonic_status(err);
        assert_eq!(status.code(), Code::ResourceExhausted);
        assert_eq!(status.message(), "throttled");

        let back = tonic_status_to_wire_error(status);
        assert_eq!(back.code, ErrorCode::RateLimited);
        assert!(back.retryable);
        assert_eq!(back.retry_after_ms, 750);
        assert_eq!(back.context.get("organization_id").map(String::as_str), Some("42"));
        assert_eq!(back.context.get("error_code").map(String::as_str), Some("3204"));
        assert_eq!(back.suggested_action, "wait and retry");
    }

    #[test]
    fn wire_code_to_tonic_invitation_variants_carry_error_code_for_disambiguation() {
        // The four Invitation* variants all collapse to FailedPrecondition
        // or AlreadyExists or ResourceExhausted on the gRPC side; the
        // inverse mapping reads the canonical wire variant from gRPC code,
        // so without the numeric error_code the round-trip can't recover
        // the exact variant. This test pins that behaviour so the wire
        // ↔ tonic precedent stays explicit.
        assert_eq!(wire_code_to_tonic(ErrorCode::InvitationRateLimited), Code::ResourceExhausted);
        assert_eq!(
            wire_code_to_tonic(ErrorCode::InvitationAlreadyResolved),
            Code::FailedPrecondition
        );
        assert_eq!(
            wire_code_to_tonic(ErrorCode::InvitationEmailMismatch),
            Code::FailedPrecondition
        );
        assert_eq!(
            wire_code_to_tonic(ErrorCode::InvitationAlreadyMember),
            Code::FailedPrecondition
        );
        assert_eq!(wire_code_to_tonic(ErrorCode::InvitationDuplicatePending), Code::AlreadyExists);
    }

    #[test]
    fn tonic_request_to_wire_context_empty_request_uses_defaults() {
        let req: tonic::Request<()> = tonic::Request::new(());
        let ctx = tonic_request_to_wire_context(&req);

        assert_eq!(ctx.trace_id, [0u8; 16]);
        assert_eq!(ctx.span_id, [0u8; 8]);
        assert_eq!(ctx.trace_flags, 0);
        assert!(ctx.deadline.is_none());
        assert_eq!(ctx.sdk_version, None);
        assert_eq!(ctx.forwarded_for, None);
        assert_eq!(ctx.caller_identity.auth_method, AuthMethod::Anonymous);
        assert!(ctx.caller_identity.user_id.is_none());
        assert!(ctx.caller_identity.app_id.is_none());
        assert!(ctx.caller_identity.organization_id.is_none());
        // Synthesized request_id is non-zero; correlation_id mirrors it.
        assert_ne!(ctx.request_id, 0);
        assert_eq!(ctx.request_id, ctx.correlation_id);
        // Cancellation is fresh (not pre-cancelled).
        assert!(!ctx.cancellation.is_cancelled());
    }

    #[test]
    fn tonic_request_to_wire_context_parses_traceparent() {
        let mut req: tonic::Request<()> = tonic::Request::new(());
        req.metadata_mut().insert(
            "traceparent",
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".parse().unwrap(),
        );

        let ctx = tonic_request_to_wire_context(&req);
        assert_eq!(
            ctx.trace_id,
            [
                0x0a, 0xf7, 0x65, 0x19, 0x16, 0xcd, 0x43, 0xdd, 0x84, 0x48, 0xeb, 0x21, 0x1c, 0x80,
                0x31, 0x9c
            ]
        );
        assert_eq!(ctx.span_id, [0xb7, 0xad, 0x6b, 0x71, 0x69, 0x20, 0x33, 0x31]);
        assert_eq!(ctx.trace_flags, 0x01);
    }

    #[test]
    fn tonic_request_to_wire_context_extracts_grpc_timeout() {
        let mut req: tonic::Request<()> = tonic::Request::new(());
        req.set_timeout(std::time::Duration::from_secs(5));

        let ctx = tonic_request_to_wire_context(&req);
        let deadline = ctx.deadline.expect("deadline should be populated from grpc-timeout header");

        // Deadline ≈ now + 5s; allow a generous skew window for slow CI.
        let now = std::time::Instant::now();
        let remaining = deadline.saturating_duration_since(now);
        assert!(
            remaining <= std::time::Duration::from_secs(5)
                && remaining > std::time::Duration::from_secs(4),
            "deadline remaining = {remaining:?}, expected ~5s"
        );
    }

    #[test]
    fn tonic_request_to_wire_context_reads_sdk_version_and_forwarded_for() {
        let mut req: tonic::Request<()> = tonic::Request::new(());
        req.metadata_mut().insert("x-sdk-version", "inferadb-ledger-sdk/0.1.0".parse().unwrap());
        req.metadata_mut().insert("x-forwarded-for", "203.0.113.7, 10.0.0.1".parse().unwrap());

        let ctx = tonic_request_to_wire_context(&req);
        assert_eq!(ctx.sdk_version.as_deref(), Some("inferadb-ledger-sdk/0.1.0"));
        // Only the first hop survives.
        assert_eq!(ctx.forwarded_for.as_deref(), Some("203.0.113.7"));
    }

    #[test]
    fn tonic_request_to_wire_context_malformed_traceparent_falls_back_to_zeros() {
        let mut req: tonic::Request<()> = tonic::Request::new(());
        req.metadata_mut().insert("traceparent", "not-a-valid-traceparent".parse().unwrap());

        let ctx = tonic_request_to_wire_context(&req);
        assert_eq!(ctx.trace_id, [0u8; 16]);
        assert_eq!(ctx.span_id, [0u8; 8]);
        assert_eq!(ctx.trace_flags, 0);
    }

    #[test]
    fn tonic_status_to_wire_error_metadata_retry_after_overrides_details() {
        // Build a status carrying a JSON-encoded WireError with retry_after_ms = 100,
        // then add a `retry-after-ms` metadata header. The header must win.
        let original = WireError {
            code: ErrorCode::RateLimited,
            message: "throttled".to_string(),
            retryable: true,
            retry_after_ms: 100,
            context: BTreeMap::new(),
            suggested_action: String::new(),
        };
        let mut status = wire_error_to_tonic_status(original);
        status.metadata_mut().insert("retry-after-ms", "5000".parse().unwrap());

        let err = tonic_status_to_wire_error(status);
        assert_eq!(err.retry_after_ms, 5000);
    }
}
