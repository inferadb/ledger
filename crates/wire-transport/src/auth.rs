//! Pluggable authentication for wire-transport connections.
//!
//! The transport layer never embeds JWT logic directly — that lives in
//! `crates/services/src/jwt.rs`. Instead, the transport defines
//! [`AuthVerifier`] as a dependency-injected trait. The server crate wires
//! its `JwtEngine` into a transport-facing impl at construction time, so the
//! dependency direction stays `services → wire-transport`, never the
//! reverse.
//!
//! D1 (per-connection auth cache + per-RPC token-expiry check + revocation
//! watchdog) is implemented in `crates/wire-transport/src/server/auth_cache.rs`
//! (Phase 0.0.B.5). [`AuthVerifier`] surfaces only the primitives that
//! implementation needs: verify a presented credential, and observe
//! signing-key epoch changes.
//!
//! # Object safety
//!
//! [`AuthVerifier`] is held by the server as a generic parameter
//! (`WireServer<V: AuthVerifier>`) rather than `Arc<dyn AuthVerifier>`. The
//! `verify` method returns `impl Future<…> + Send` — Rust 1.92's bare
//! async-fn-in-trait support makes this kind of return-position-impl-trait
//! method **not** dyn-compatible without `#[trait_variant::make]` or
//! `#[async_trait]`, both of which we explicitly avoid (Tier-1 fix #4).
//! Generics on the server give us static dispatch, monomorphic per-impl
//! optimization, and no extra crate dependency. The `auth_verifier_arc_dyn`
//! test below documents the dyn-incompatibility check.

use std::time::Instant;

use bytes::Bytes;
use inferadb_ledger_wire::CallerIdentity;
use tokio::sync::watch;

/// Outcome of a successful authentication handshake.
///
/// Cached on the connection for the connection's lifetime; the caller's
/// [`AuthMethod`](inferadb_ledger_wire::AuthMethod) determines which fields
/// of the embedded [`CallerIdentity`] are populated.
#[derive(Debug, Clone)]
pub struct VerifiedAuth {
    /// Caller identity resolved from the presented credential.
    pub identity: CallerIdentity,
    /// Wall-clock instant at which the credential expires; the dispatcher
    /// rejects subsequent RPCs once `Instant::now() >= token_expiry`.
    pub token_expiry: Instant,
    /// Global signing-key epoch the credential was issued under. If the
    /// cluster epoch advances past this value, the connection is closed.
    pub revocation_epoch: u64,
}

/// Pluggable authentication backend for the wire transport.
///
/// Implemented by the services crate to bridge `JwtEngine` (and any future
/// auth backends) into the transport layer. Each connection invokes
/// [`Self::verify`] once on its `OpAuthEstablish` frame; the resulting
/// [`VerifiedAuth`] is cached for the connection's lifetime, with periodic
/// re-checks driven by [`Self::subscribe_epoch`] and the cached
/// `token_expiry`.
///
/// Implementations must be `Send + Sync` because the transport spawns one
/// task per connection and shares a single [`AuthVerifier`] across all of
/// them.
///
/// Held as a generic parameter on the server type (not `dyn`) — see the
/// module-level note on object safety.
pub trait AuthVerifier: Send + Sync + 'static {
    /// Verify the credential carried by an `OpAuthEstablish` frame's
    /// payload. Returns the resolved [`VerifiedAuth`] on success.
    ///
    /// The transport never inspects the credential bytes itself — `payload`
    /// is whatever the client serialized into the frame, opaque to this
    /// crate.
    fn verify(
        &self,
        payload: Bytes,
    ) -> impl std::future::Future<Output = Result<VerifiedAuth, AuthError>> + Send;

    /// Subscribe to the cluster's signing-key revocation epoch.
    ///
    /// The revocation watchdog (B.5) holds the receiver and closes any
    /// connection whose cached `revocation_epoch` falls behind the latest
    /// value. The receiver MUST yield the current epoch on subscribe (i.e.,
    /// implementations should not return a fresh `watch::channel` whose
    /// initial value is zero unless zero is genuinely the current epoch).
    fn subscribe_epoch(&self) -> watch::Receiver<u64>;
}

/// Errors surfaced from [`AuthVerifier::verify`].
///
/// Mapped to a `WireError` with `ErrorCode::Unauthenticated` by the dispatch
/// layer; the variant carries enough detail for diagnostics without leaking
/// credential material.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum AuthError {
    /// Credential format was invalid (parse error, malformed JWT, missing
    /// claims, etc).
    #[error("malformed credential: {0}")]
    MalformedCredential(String),

    /// Credential signature did not verify, or the signing key is unknown.
    #[error("invalid signature")]
    InvalidSignature,

    /// Credential is expired at the time of verification.
    #[error("credential expired")]
    Expired,

    /// Credential is well-formed and signed, but the principal it identifies
    /// is not authorized to open a connection (revoked, disabled, banned).
    #[error("principal not authorized: {0}")]
    PrincipalNotAuthorized(String),

    /// An internal error in the verifier (database unreachable, key store
    /// miss, etc). Never exposed to the client; mapped to `Internal`.
    #[error("internal verifier error: {0}")]
    Internal(String),
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use std::sync::Arc;

    use inferadb_ledger_wire::AuthMethod;

    use super::*;

    fn sample_identity() -> CallerIdentity {
        CallerIdentity {
            user_id: None,
            app_id: None,
            organization_id: None,
            auth_method: AuthMethod::Anonymous,
            token_expiry: Instant::now(),
            revocation_epoch: 0,
        }
    }

    struct StubVerifier {
        epoch_tx: watch::Sender<u64>,
    }

    impl StubVerifier {
        fn new() -> Self {
            let (tx, _rx) = watch::channel(0);
            Self { epoch_tx: tx }
        }
    }

    impl AuthVerifier for StubVerifier {
        async fn verify(&self, _payload: Bytes) -> Result<VerifiedAuth, AuthError> {
            Err(AuthError::Internal("stub".into()))
        }

        fn subscribe_epoch(&self) -> watch::Receiver<u64> {
            self.epoch_tx.subscribe()
        }
    }

    /// Compile-time check that `AuthVerifier` impls work behind `Arc<T>` —
    /// the server holds one verifier and shares it across connection tasks.
    /// Generic, not `dyn`: see the module-level note on object safety.
    #[test]
    fn auth_verifier_object_safe_via_arc() {
        fn assert_arc<T: AuthVerifier>(_: Arc<T>) {}
        let v = Arc::new(StubVerifier::new());
        assert_arc(v);
    }

    #[test]
    fn verified_auth_constructs() {
        let auth = VerifiedAuth {
            identity: sample_identity(),
            token_expiry: Instant::now(),
            revocation_epoch: 7,
        };
        assert_eq!(auth.revocation_epoch, 7);
        assert_eq!(auth.identity.auth_method, AuthMethod::Anonymous);
    }

    #[test]
    fn auth_error_variants_distinct() {
        // `AuthError` doesn't implement `PartialEq` (the inner `String`s
        // would force structural equality on diagnostic text). Use
        // `matches!` to confirm each variant is reachable and distinct
        // from every other variant.
        let all: [AuthError; 5] = [
            AuthError::MalformedCredential("a".into()),
            AuthError::InvalidSignature,
            AuthError::Expired,
            AuthError::PrincipalNotAuthorized("b".into()),
            AuthError::Internal("c".into()),
        ];

        for (i, a) in all.iter().enumerate() {
            for (j, b) in all.iter().enumerate() {
                let same_kind = matches!(
                    (a, b),
                    (AuthError::MalformedCredential(_), AuthError::MalformedCredential(_))
                        | (AuthError::InvalidSignature, AuthError::InvalidSignature)
                        | (AuthError::Expired, AuthError::Expired)
                        | (
                            AuthError::PrincipalNotAuthorized(_),
                            AuthError::PrincipalNotAuthorized(_)
                        )
                        | (AuthError::Internal(_), AuthError::Internal(_))
                );
                if i == j {
                    assert!(same_kind, "variants {i} and {j} should be the same kind");
                } else {
                    assert!(!same_kind, "variants {i} and {j} collide");
                }
            }
        }
    }
}
