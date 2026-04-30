//! Production [`AuthVerifier`] impl backed by [`JwtEngine`] (Phase 0.0.F.1.c).
//!
//! Bridges the existing JWT engine onto the wire-transport's auth contract:
//!
//! - [`AuthVerifier::verify`] — payload is the JWT bytes; decode UTF-8, run [`JwtEngine::validate`]
//!   (trying both audiences), project the resulting [`ValidatedToken`] onto a [`VerifiedAuth`] with
//!   the right [`CallerIdentity`] / [`AuthMethod`] / `token_expiry` / `revocation_epoch`.
//!
//! - [`AuthVerifier::subscribe_epoch`] — returns a [`watch::Receiver<u64>`] that the
//!   wire-transport's revocation watchdog uses to close connections whose cached signing-key epoch
//!   falls behind. [`JwtEngine`] does not yet track a global revocation epoch, so the verifier owns
//!   its own [`watch::Sender<u64>`] seeded at zero. Future work — once cluster-side key revocation
//!   lands — will tie [`JwtEngine::evict_key`] to bumping this sender.
//!
//! # Caller-identity fidelity
//!
//! [`UserSessionClaims`] / [`VaultTokenClaims`] carry external [`UserSlug`] /
//! [`AppSlug`] / [`OrganizationSlug`] / [`VaultSlug`] values, but
//! [`CallerIdentity`] expects internal `{Entity}Id(i64)` newtypes
//! ([`UserId`] / [`AppId`] / [`OrganizationId`]). Slug → ID resolution
//! requires the [`StateLayer`](inferadb_ledger_state::StateLayer), which is
//! *not* injected into the verifier — wiring that in would couple every
//! auth-handshake call to a state-backed lookup.
//!
//! Per [`CallerIdentity`]'s own doc comment ("Slug → ID resolution happens at
//! the handler boundary before the context is built"), the verifier therefore
//! leaves `user_id` / `app_id` / `organization_id` as `None` and projects
//! only the [`AuthMethod`] discriminant + token expiry + revocation epoch.
//! The handler-side dispatcher is the right place to resolve slugs into IDs
//! when authorization needs them.
//!
//! [`UserId`]: inferadb_ledger_types::UserId
//! [`AppId`]: inferadb_ledger_types::AppId
//! [`OrganizationId`]: inferadb_ledger_types::OrganizationId
//! [`UserSlug`]: inferadb_ledger_types::types::UserSlug
//! [`AppSlug`]: inferadb_ledger_types::types::AppSlug
//! [`OrganizationSlug`]: inferadb_ledger_types::types::OrganizationSlug
//! [`VaultSlug`]: inferadb_ledger_types::types::VaultSlug
//! [`UserSessionClaims`]: inferadb_ledger_types::token::UserSessionClaims
//! [`VaultTokenClaims`]: inferadb_ledger_types::token::VaultTokenClaims

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::Bytes;
use chrono::Utc;
use inferadb_ledger_types::token::{SESSION_AUDIENCE, VAULT_AUDIENCE, ValidatedToken};
use inferadb_ledger_wire::{AuthMethod, CallerIdentity};
use inferadb_ledger_wire_transport::auth::{AuthError, AuthVerifier, VerifiedAuth};
use tokio::sync::watch;

use crate::jwt::{JwtEngine, JwtError};

/// Production [`AuthVerifier`] backed by a [`JwtEngine`].
///
/// Holds an [`Arc<JwtEngine>`] for token verification and an internal
/// [`watch::Sender<u64>`] for the revocation epoch the wire transport's
/// watchdog observes through [`AuthVerifier::subscribe_epoch`]. The sender
/// is held for the verifier's lifetime so subscribers always observe a
/// live channel.
pub struct JwtAuthVerifier {
    /// JWT signing/validation engine. Cloned on construction; the verifier
    /// reads through it on every [`AuthVerifier::verify`] call.
    engine: Arc<JwtEngine>,
    /// Revocation-epoch broadcaster. The current value never advances under
    /// the F.1.c verifier — the engine does not yet expose an epoch hook.
    /// Once cluster-side signing-key revocation lands, future work will tie
    /// [`JwtEngine::evict_key`] (or the equivalent rotation primitive) to
    /// `epoch_tx.send(epoch + 1)` so existing connections close on the next
    /// watchdog tick.
    epoch_tx: watch::Sender<u64>,
}

impl JwtAuthVerifier {
    /// Construct a verifier wrapping the supplied [`JwtEngine`].
    ///
    /// The internal revocation-epoch [`watch::channel`] starts at `0`. The
    /// wire-transport contract requires that [`AuthVerifier::subscribe_epoch`]
    /// yield the current epoch on subscribe (not a fresh zero unless zero is
    /// genuinely current); since [`JwtEngine`] has no notion of an epoch
    /// today, zero is the only valid current value.
    #[must_use]
    pub fn new(engine: Arc<JwtEngine>) -> Self {
        let (epoch_tx, _rx) = watch::channel(0);
        Self { engine, epoch_tx }
    }

    /// Try [`JwtEngine::validate`] with the session audience first, then the
    /// vault audience on `InvalidAudience` failure. Either succeeds with a
    /// [`ValidatedToken`] discriminant the caller projects onto an
    /// [`AuthMethod`].
    fn validate_either_audience(&self, token: &str) -> Result<ValidatedToken, JwtError> {
        match self.engine.validate(token, SESSION_AUDIENCE) {
            Ok(validated) => Ok(validated),
            Err(JwtError::Token {
                source: inferadb_ledger_types::token::TokenError::InvalidAudience { .. },
                ..
            }) => self.engine.validate(token, VAULT_AUDIENCE),
            // jsonwebtoken surfaces audience mismatches via its decoding
            // error path too; retry once on any decoding failure so a
            // vault-audience token doesn't reach the caller as
            // InvalidSignature when the only issue was audience.
            Err(JwtError::Decoding { .. }) => self.engine.validate(token, VAULT_AUDIENCE),
            Err(other) => Err(other),
        }
    }
}

/// Map [`JwtError`] onto the wire transport's [`AuthError`] surface.
///
/// The wire layer collapses every auth failure into a single
/// `Unauthenticated` response on the network (per
/// [`auth_errors::unified_auth_error`](super::auth_errors::unified_auth_error)
/// — no enumeration leakage), but the variant carried back to the dispatch
/// layer is still useful for tracing + metrics, so we preserve the shape.
fn map_jwt_error(err: JwtError) -> AuthError {
    use inferadb_ledger_types::token::TokenError;
    match err {
        JwtError::Token { source, .. } => match source {
            TokenError::Expired => AuthError::Expired,
            TokenError::InvalidSignature
            | TokenError::SigningKeyNotFound { .. }
            | TokenError::SigningKeyExpired { .. } => AuthError::InvalidSignature,
            TokenError::InvalidAudience { .. } | TokenError::InvalidTokenType { .. } => {
                AuthError::PrincipalNotAuthorized(source.to_string())
            },
            TokenError::MissingClaim { .. } => AuthError::MalformedCredential(source.to_string()),
        },
        JwtError::Decoding { source, .. } => {
            // jsonwebtoken's ErrorKind covers both "expired" and "malformed"
            // among others. Project the well-known kinds onto the right
            // AuthError variant; everything else is malformed by default.
            use jsonwebtoken::errors::ErrorKind;
            match source.kind() {
                ErrorKind::ExpiredSignature => AuthError::Expired,
                ErrorKind::InvalidSignature
                | ErrorKind::InvalidToken
                | ErrorKind::InvalidAlgorithm
                | ErrorKind::InvalidAlgorithmName
                | ErrorKind::InvalidKeyFormat
                | ErrorKind::InvalidEcdsaKey
                | ErrorKind::InvalidRsaKey(_) => AuthError::InvalidSignature,
                ErrorKind::InvalidIssuer
                | ErrorKind::InvalidAudience
                | ErrorKind::InvalidSubject => {
                    AuthError::PrincipalNotAuthorized(source.to_string())
                },
                _ => AuthError::MalformedCredential(source.to_string()),
            }
        },
        JwtError::Signing { .. } | JwtError::KeyDecryption | JwtError::KeyEncryption => {
            AuthError::Internal(err.to_string())
        },
        JwtError::StateLookup { .. } => AuthError::Internal(err.to_string()),
    }
}

/// Convert a JWT `exp` claim (seconds since UNIX epoch) into a monotonic
/// [`Instant`].
///
/// Computes the wall-clock delta `exp - now_unix` then offsets the current
/// [`Instant`] by that delta so callers can compare against
/// [`Instant::now()`] without dragging in [`std::time::SystemTime`]. Past-due
/// expirations clamp to "now" — the dispatcher's per-RPC token-expiry check
/// will reject the connection on the next dispatch.
fn instant_from_unix_exp(exp: i64) -> Instant {
    let now_unix = Utc::now().timestamp();
    let delta_secs = exp.saturating_sub(now_unix);
    if delta_secs <= 0 {
        Instant::now()
    } else {
        // i64 → u64 is safe because we just checked > 0.
        #[allow(clippy::cast_sign_loss)]
        let secs = delta_secs as u64;
        Instant::now() + Duration::from_secs(secs)
    }
}

impl AuthVerifier for JwtAuthVerifier {
    async fn verify(&self, payload: Bytes) -> Result<VerifiedAuth, AuthError> {
        let token_str = std::str::from_utf8(&payload).map_err(|err| {
            AuthError::MalformedCredential(format!("non-utf8 token bytes: {err}"))
        })?;

        let validated = self.validate_either_audience(token_str).map_err(map_jwt_error)?;

        // Project the validated token onto AuthMethod + token_expiry. Slug
        // → ID resolution is deferred to the handler boundary (see module
        // doc); user_id/app_id/organization_id are left as None.
        let (auth_method, token_expiry_unix) = match &validated {
            ValidatedToken::UserSession(claims) => (AuthMethod::UserSession, claims.exp),
            // Vault tokens are issued to apps. The token itself does not
            // record *how* the app authenticated to obtain it
            // (client_assertion vs client_secret vs mTLS); the most common
            // production path is the assertion-based flow, so map there
            // and let future work refine the discriminant once the claim
            // shape exposes it. The dispatcher does not currently key
            // authorization decisions off the AuthMethod sub-variant —
            // it's a tracing / observability hint.
            ValidatedToken::VaultAccess(claims) => (AuthMethod::AppClientAssertion, claims.exp),
        };

        let token_expiry = instant_from_unix_exp(token_expiry_unix);
        let revocation_epoch = *self.epoch_tx.subscribe().borrow();

        let identity = CallerIdentity {
            user_id: None,
            app_id: None,
            organization_id: None,
            auth_method,
            token_expiry,
            revocation_epoch,
        };

        Ok(VerifiedAuth { identity, token_expiry, revocation_epoch })
    }

    fn subscribe_epoch(&self) -> watch::Receiver<u64> {
        self.epoch_tx.subscribe()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods, clippy::panic)]
mod tests {
    use base64::Engine as _;
    use chrono::Duration as ChronoDuration;
    use ed25519_dalek::SigningKey as Ed25519SigningKey;
    use inferadb_ledger_state::system::{SigningKey, SigningKeyScope, SigningKeyStatus};
    use inferadb_ledger_store::crypto::RegionMasterKey;
    use inferadb_ledger_types::{
        SigningKeyId,
        config::JwtConfig,
        types::{AppSlug, OrganizationSlug, TokenVersion, UserSlug, VaultSlug},
    };

    use super::*;

    fn test_config() -> JwtConfig {
        JwtConfig::builder()
            .session_access_ttl_secs(1800_u64)
            .vault_access_ttl_secs(900_u64)
            .build()
            .unwrap()
    }

    fn test_rmk() -> RegionMasterKey {
        RegionMasterKey::new(1, [0x42; 32])
    }

    /// Generate an in-memory test signing key and return `(SigningKey, RMK)`.
    /// Mirrors the helper in `jwt.rs::tests::generate_test_signing_key` —
    /// duplicated here because that helper is `#[cfg(test)]` private to
    /// the `jwt` module.
    fn generate_test_signing_key(kid: &str) -> (SigningKey, RegionMasterKey) {
        let rmk = test_rmk();

        let mut secret_bytes = [0u8; 32];
        rand::RngExt::fill(&mut rand::rng(), &mut secret_bytes);
        let signing_key = Ed25519SigningKey::from_bytes(&secret_bytes);
        let public_key_bytes = signing_key.verifying_key().to_bytes().to_vec();
        let private_key_bytes = signing_key.to_bytes();

        let (envelope, rmk_version) =
            crate::jwt::encrypt_private_key(&private_key_bytes, kid, &rmk).unwrap();

        let key = SigningKey {
            id: SigningKeyId::new(1),
            kid: kid.to_string(),
            public_key_bytes,
            encrypted_private_key: envelope.to_bytes().to_vec(),
            rmk_version,
            scope: SigningKeyScope::Global,
            status: SigningKeyStatus::Active,
            valid_from: Utc::now(),
            valid_until: None,
            created_at: Utc::now(),
            rotated_at: None,
            revoked_at: None,
        };

        (key, rmk)
    }

    fn build_engine() -> (Arc<JwtEngine>, String) {
        let engine = Arc::new(JwtEngine::new(test_config()));
        let kid = uuid::Uuid::new_v4().to_string();
        let (key, rmk) = generate_test_signing_key(&kid);
        engine.load_key(&key, &rmk).unwrap();
        (engine, kid)
    }

    #[tokio::test]
    async fn jwt_auth_verifier_accepts_valid_session_token() {
        let (engine, kid) = build_engine();
        let (token, expires_at) = engine
            .sign_user_session(UserSlug::new(7), "user", TokenVersion::default(), &kid)
            .unwrap();

        let verifier = JwtAuthVerifier::new(engine);
        let verified =
            verifier.verify(Bytes::from(token.into_bytes())).await.expect("valid session token");

        assert_eq!(verified.identity.auth_method, AuthMethod::UserSession);
        assert!(verified.identity.user_id.is_none());
        assert!(verified.identity.app_id.is_none());
        assert!(verified.identity.organization_id.is_none());
        assert_eq!(verified.revocation_epoch, 0);
        // Token expiry should round-trip approximately to the engine's exp.
        // Allow 5s slack — the verifier computes the Instant from a
        // wall-clock delta, and Utc::now() may have advanced microseconds
        // since signing.
        let now_plus_remaining = Instant::now()
            + Duration::from_secs((expires_at.timestamp() - Utc::now().timestamp()).max(0) as u64);
        let drift = if verified.token_expiry > now_plus_remaining {
            verified.token_expiry - now_plus_remaining
        } else {
            now_plus_remaining - verified.token_expiry
        };
        assert!(drift < Duration::from_secs(5), "token_expiry drift too large: {drift:?}");
    }

    #[tokio::test]
    async fn jwt_auth_verifier_accepts_valid_vault_token() {
        let (engine, kid) = build_engine();
        let scopes = vec!["vault:read".to_string()];
        let (token, _expires_at) = engine
            .sign_vault_token(
                OrganizationSlug::new(10),
                AppSlug::new(20),
                VaultSlug::new(30),
                &scopes,
                &kid,
            )
            .unwrap();

        let verifier = JwtAuthVerifier::new(engine);
        let verified =
            verifier.verify(Bytes::from(token.into_bytes())).await.expect("valid vault token");

        assert_eq!(verified.identity.auth_method, AuthMethod::AppClientAssertion);
    }

    #[tokio::test]
    async fn jwt_auth_verifier_rejects_invalid_signature() {
        let (engine, kid) = build_engine();
        let (token, _) = engine
            .sign_user_session(UserSlug::new(1), "user", TokenVersion::default(), &kid)
            .unwrap();

        // Flip the last signature byte.
        let mut parts: Vec<&str> = token.splitn(3, '.').collect();
        let mut sig_bytes =
            base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(parts[2]).unwrap();
        if let Some(b) = sig_bytes.first_mut() {
            *b ^= 0xFF;
        }
        let tampered_sig = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&sig_bytes);
        parts[2] = &tampered_sig;
        let tampered = format!("{}.{}.{}", parts[0], parts[1], parts[2]);

        let verifier = JwtAuthVerifier::new(engine);
        let err = verifier
            .verify(Bytes::from(tampered.into_bytes()))
            .await
            .expect_err("tampered token must fail");
        assert!(matches!(err, AuthError::InvalidSignature), "got: {err:?}");
    }

    #[tokio::test]
    async fn jwt_auth_verifier_rejects_non_utf8_payload() {
        let (engine, _kid) = build_engine();
        let verifier = JwtAuthVerifier::new(engine);
        // Invalid UTF-8: a lone continuation byte.
        let payload = Bytes::from_static(&[0xff, 0xfe, 0xfd]);
        let err = verifier.verify(payload).await.expect_err("non-utf8 must fail");
        assert!(matches!(err, AuthError::MalformedCredential(_)), "got: {err:?}");
    }

    #[tokio::test]
    async fn jwt_auth_verifier_rejects_expired_token() {
        // Build a token whose `exp` is in the past, signed with the same
        // key the engine already has loaded so the signature is valid —
        // only the timestamp is bad. We sign manually with raw key bytes
        // (mirroring jwt::tests::prop_expired_token_always_rejected) and
        // load those bytes into the engine under the matching kid.
        let engine = Arc::new(JwtEngine::new(test_config()));
        let kid = uuid::Uuid::new_v4().to_string();
        let rmk = test_rmk();

        // Generate the keypair locally so we keep the raw private bytes
        // for manual signing; the engine consumes the same bytes via the
        // SigningKey wrapper.
        let mut secret_bytes = [0u8; 32];
        rand::RngExt::fill(&mut rand::rng(), &mut secret_bytes);
        let signing_key = Ed25519SigningKey::from_bytes(&secret_bytes);
        let public_key_bytes = signing_key.verifying_key().to_bytes().to_vec();
        let private_key_bytes = signing_key.to_bytes();

        let (envelope, rmk_version) =
            crate::jwt::encrypt_private_key(&private_key_bytes, &kid, &rmk).unwrap();
        let key = SigningKey {
            id: SigningKeyId::new(1),
            kid: kid.clone(),
            public_key_bytes,
            encrypted_private_key: envelope.to_bytes().to_vec(),
            rmk_version,
            scope: SigningKeyScope::Global,
            status: SigningKeyStatus::Active,
            valid_from: Utc::now(),
            valid_until: None,
            created_at: Utc::now(),
            rotated_at: None,
            revoked_at: None,
        };
        engine.load_key(&key, &rmk).unwrap();

        // Manually encode an expired session token with the same raw
        // private key the engine just loaded.
        let pkcs8_prefix: [u8; 16] = [
            0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22,
            0x04, 0x20,
        ];
        let mut der = Vec::with_capacity(48);
        der.extend_from_slice(&pkcs8_prefix);
        der.extend_from_slice(&private_key_bytes);
        let encoding = jsonwebtoken::EncodingKey::from_ed_der(&der);

        let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::EdDSA);
        header.kid = Some(kid.clone());

        let now = Utc::now();
        let exp = now - ChronoDuration::seconds(3600);
        let claims = inferadb_ledger_types::token::UserSessionClaims {
            iss: "inferadb".to_string(),
            sub: "user:1".to_string(),
            aud: vec![SESSION_AUDIENCE.to_string()],
            exp: exp.timestamp(),
            iat: (exp - ChronoDuration::seconds(60)).timestamp(),
            nbf: (exp - ChronoDuration::seconds(60)).timestamp(),
            jti: uuid::Uuid::new_v4().to_string(),
            token_type: inferadb_ledger_types::token::TokenType::UserSession,
            user: UserSlug::new(1),
            role: "user".to_string(),
            version: TokenVersion::default(),
        };

        let token = jsonwebtoken::encode(&header, &claims, &encoding).unwrap();

        let verifier = JwtAuthVerifier::new(engine);
        let err = verifier
            .verify(Bytes::from(token.into_bytes()))
            .await
            .expect_err("expired token must fail");
        assert!(matches!(err, AuthError::Expired), "got: {err:?}");
    }

    #[tokio::test]
    async fn jwt_auth_verifier_subscribe_epoch_is_live() {
        let (engine, _kid) = build_engine();
        let verifier = JwtAuthVerifier::new(engine);
        let mut rx = verifier.subscribe_epoch();
        // Per the trait contract, subscribe_epoch must yield the current
        // epoch on subscribe.
        assert_eq!(*rx.borrow_and_update(), 0);

        // The verifier's internal sender drives the channel, so a manual
        // bump propagates to subscribers — proves the receiver is live and
        // not a one-shot zero. Future work ties this to actual key
        // revocation events.
        verifier.epoch_tx.send(1).unwrap();
        rx.changed().await.unwrap();
        assert_eq!(*rx.borrow_and_update(), 1);
    }

    #[test]
    fn map_jwt_error_token_expired_maps_to_expired() {
        let err = JwtError::Token {
            source: inferadb_ledger_types::token::TokenError::Expired,
            location: snafu::location!(),
        };
        assert!(matches!(map_jwt_error(err), AuthError::Expired));
    }

    #[test]
    fn map_jwt_error_token_invalid_sig_maps_to_invalid_signature() {
        let err = JwtError::Token {
            source: inferadb_ledger_types::token::TokenError::InvalidSignature,
            location: snafu::location!(),
        };
        assert!(matches!(map_jwt_error(err), AuthError::InvalidSignature));
    }

    #[test]
    fn map_jwt_error_token_missing_claim_maps_to_malformed() {
        let err = JwtError::Token {
            source: inferadb_ledger_types::token::TokenError::MissingClaim {
                claim: "exp".to_string(),
            },
            location: snafu::location!(),
        };
        assert!(matches!(map_jwt_error(err), AuthError::MalformedCredential(_)));
    }

    #[test]
    fn map_jwt_error_key_decryption_maps_to_internal() {
        let err = JwtError::KeyDecryption;
        assert!(matches!(map_jwt_error(err), AuthError::Internal(_)));
    }

    #[test]
    fn instant_from_unix_exp_past_clamps_to_now() {
        let past = Utc::now().timestamp() - 100;
        let computed = instant_from_unix_exp(past);
        // Past expirations should not be far in the future.
        assert!(computed <= Instant::now() + Duration::from_millis(100));
    }

    #[test]
    fn instant_from_unix_exp_future_offsets_correctly() {
        let future = Utc::now().timestamp() + 60;
        let computed = instant_from_unix_exp(future);
        let expected_lower = Instant::now() + Duration::from_secs(55);
        let expected_upper = Instant::now() + Duration::from_secs(65);
        assert!(computed >= expected_lower);
        assert!(computed <= expected_upper);
    }
}
