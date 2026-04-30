//! Stub [`AuthVerifier`] for the F.1.b wire-transport bring-up.
//!
//! F.1.b adds a parallel `WireServer` bind path on `LedgerServer` so the
//! tonic and wire transports can be selected at startup via
//! [`crate::server::TransportKind`]. The wire transport requires an
//! [`AuthVerifier`] impl to drive the per-connection auth handshake, but
//! the production [`crate::jwt::JwtEngine`]-backed verifier (`JwtAuthVerifier`)
//! is the F.1.c deliverable. To keep F.1.b shippable in isolation without
//! letting a wide-open verifier leak into production builds, this module
//! exposes [`PermissiveVerifier`] under the `permissive-wire-auth` cargo
//! feature.
//!
//! # Production builds
//!
//! Without the `permissive-wire-auth` feature, this module is empty —
//! attempting to construct a [`crate::server::LedgerServer`] in
//! [`crate::server::TransportKind::Wire`] mode without supplying a real
//! verifier will fail at runtime via the `expect("auth verifier required …")`
//! call in [`crate::server::LedgerServer::serve`].
//!
//! # Tests and developer builds
//!
//! With the feature on, [`PermissiveVerifier`] accepts every credential —
//! mirroring the test-only verifier in
//! `crates/wire-transport/src/server/accept.rs::tests::PermissiveVerifier`.
//! It is intended for: F.1.b smoke tests, F.1.d default-flip dry-runs, and
//! local-dev cluster bring-up while F.1.c is in flight. **Never enable in
//! production.**

#![cfg(any(test, feature = "permissive-wire-auth"))]

use std::time::{Duration, Instant};

use bytes::Bytes;
use inferadb_ledger_wire::{AuthMethod, CallerIdentity};
use inferadb_ledger_wire_transport::auth::{AuthError, AuthVerifier, VerifiedAuth};
use tokio::sync::watch;

/// F.1.b placeholder verifier. Accepts every credential with a fixed
/// 5-minute token expiry.
///
/// Replaced by the production `JwtAuthVerifier` in F.1.c. The dispatcher
/// surface, the transport's auth-cache shape, and the `serve()` branch
/// landing in F.1.b are independent of which verifier is plugged in;
/// swapping in the real one is a one-line change in
/// [`crate::server::LedgerServer::serve`].
pub struct PermissiveVerifier {
    /// Revocation-epoch broadcaster. Held for the lifetime of the
    /// verifier so `subscribe_epoch` returns a live receiver. The epoch
    /// never advances under this stub — the wire transport's revocation
    /// watchdog will simply never fire, which is the intended behaviour
    /// for a feature-gated test verifier.
    epoch_tx: watch::Sender<u64>,
}

impl PermissiveVerifier {
    /// Construct a fresh verifier with `revocation_epoch = 0`.
    #[must_use]
    pub fn new() -> Self {
        let (epoch_tx, _rx) = watch::channel(0);
        Self { epoch_tx }
    }
}

impl Default for PermissiveVerifier {
    fn default() -> Self {
        Self::new()
    }
}

impl AuthVerifier for PermissiveVerifier {
    async fn verify(&self, _payload: Bytes) -> Result<VerifiedAuth, AuthError> {
        let token_expiry = Instant::now() + Duration::from_secs(300);
        let identity = CallerIdentity {
            user_id: None,
            app_id: None,
            organization_id: None,
            auth_method: AuthMethod::Anonymous,
            token_expiry,
            revocation_epoch: 0,
        };
        Ok(VerifiedAuth { identity, token_expiry, revocation_epoch: 0 })
    }

    fn subscribe_epoch(&self) -> watch::Receiver<u64> {
        self.epoch_tx.subscribe()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    #[test]
    fn permissive_verifier_constructs() {
        let v = PermissiveVerifier::new();
        let mut rx = v.subscribe_epoch();
        // Initial value is the channel's 0 — `subscribe_epoch` MUST yield
        // the current epoch on subscribe (per the trait doc).
        assert_eq!(*rx.borrow_and_update(), 0);
    }

    #[tokio::test]
    async fn permissive_verifier_accepts_any_credential() {
        let v = PermissiveVerifier::new();
        let auth = v.verify(Bytes::from_static(b"anything")).await.expect("verify accepts");
        assert_eq!(auth.identity.auth_method, AuthMethod::Anonymous);
        assert!(auth.identity.user_id.is_none());
        assert!(auth.identity.app_id.is_none());
        assert!(auth.identity.organization_id.is_none());
        // Token expiry should be in the future.
        assert!(auth.token_expiry > Instant::now());
        assert_eq!(auth.revocation_epoch, 0);
    }

    #[test]
    fn default_matches_new() {
        let _ = PermissiveVerifier::default();
    }
}
