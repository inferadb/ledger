//! User service implementation.
//!
//! Handles user lifecycle (CRUD), email management, region migration,
//! and GDPR erasure. Write operations flow through Raft for consistency;
//! read operations hit the local state layer directly.
//!
//! User creation and region migration use sagas (multi-step distributed
//! transactions) driven by the saga orchestrator. Other write operations
//! are single-step Raft proposals.

use chrono::Utc;
use inferadb_ledger_raft::types::SystemRequest;
use inferadb_ledger_state::system::SystemOrganizationService;
use inferadb_ledger_wire::{ErrorCode, WireError, services::shared as ws};
use tonic::Status;

use super::{
    error_classify,
    service_infra::ServiceContext,
    slug_resolver::SlugResolver,
    wire_helpers::{tonic_status_to_wire_error, wire_error_to_tonic_status},
};

/// gRPC handler for user lifecycle operations.
pub struct UserService {
    pub(super) ctx: ServiceContext,
}

/// Session tokens returned by [`UserService::create_user_session`].
pub(super) struct SessionTokens {
    pub(super) access_token: String,
    pub(super) access_expires_at: chrono::DateTime<Utc>,
    pub(super) refresh_token: String,
    pub(super) refresh_expires_at: chrono::DateTime<Utc>,
}

impl UserService {
    /// Creates a new `UserService` from shared service infrastructure.
    pub(crate) fn new(ctx: ServiceContext) -> Self {
        Self { ctx }
    }

    /// Ensures the active global signing key is loaded and cached in the JWT engine.
    ///
    /// Returns the signing key metadata. Subsequent calls are fast (cache hit).
    pub(super) fn ensure_signing_key_cached(
        &self,
        sys_svc: &SystemOrganizationService<inferadb_ledger_store::FileBackend>,
    ) -> Result<inferadb_ledger_state::system::SigningKey, Status> {
        let jwt_engine = self.ctx.jwt_engine.as_ref().ok_or_else(|| {
            Status::failed_precondition("JWT engine not configured for session signing")
        })?;
        let key_manager = self
            .ctx
            .key_manager
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("Key manager not configured"))?;

        let signing_key = sys_svc
            .get_active_signing_key(&inferadb_ledger_state::system::SigningKeyScope::Global)
            .map_err(|e| wire_error_to_tonic_status(error_classify::crypto_error(&e)))?
            .ok_or_else(|| Status::failed_precondition("No active signing key"))?;

        if !jwt_engine.has_cached_key(&signing_key.kid) {
            let scope_region = crate::jwt::scope_to_region(&signing_key.scope, sys_svc)
                .map_err(|e| wire_error_to_tonic_status(error_classify::crypto_error(&e)))?;
            let rmk = key_manager
                .rmk_by_version(scope_region, signing_key.rmk_version)
                .map_err(|e| wire_error_to_tonic_status(error_classify::crypto_error(&e)))?;
            jwt_engine
                .load_key(&signing_key, &rmk)
                .map_err(|e| wire_error_to_tonic_status(error_classify::crypto_error(&e)))?;
        }

        Ok(signing_key)
    }

    /// Creates a full user session: signs a JWT access token, generates a
    /// refresh token, and proposes `CreateRefreshToken` to the user's region.
    pub(super) async fn create_user_session(
        &self,
        sys_svc: &SystemOrganizationService<inferadb_ledger_store::FileBackend>,
        user_slug: inferadb_ledger_types::UserSlug,
        role: inferadb_ledger_types::UserRole,
        token_version: inferadb_ledger_types::TokenVersion,
        region: inferadb_ledger_types::Region,
        wire_ctx: &inferadb_ledger_wire::RequestContext,
        ctx: &mut inferadb_ledger_raft::logging::CanonicalLogLine,
    ) -> Result<SessionTokens, Status> {
        use inferadb_ledger_types::{TokenSubject, TokenType};

        let signing_key = self.ensure_signing_key_cached(sys_svc)?;
        let jwt_engine = self
            .ctx
            .jwt_engine
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("JWT engine not configured"))?;
        let jwt_config = self
            .ctx
            .jwt_config
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("JWT config not configured"))?;

        let role_str = match role {
            inferadb_ledger_types::UserRole::Admin => "admin",
            inferadb_ledger_types::UserRole::User => "user",
        };

        let (access_token, access_expires_at) = jwt_engine
            .sign_user_session(user_slug, role_str, token_version, &signing_key.kid)
            .map_err(|e| wire_error_to_tonic_status(error_classify::crypto_error(&e)))?;

        let (refresh_token_str, refresh_token_hash) = crate::jwt::generate_refresh_token();
        let family = crate::jwt::generate_family_id();

        self.ctx
            .propose_regional(
                region,
                SystemRequest::CreateRefreshToken {
                    token_hash: refresh_token_hash,
                    family,
                    token_type: TokenType::UserSession,
                    subject: TokenSubject::User(user_slug),
                    organization: None,
                    vault: None,
                    kid: signing_key.kid.clone(),
                    ttl_secs: jwt_config.session_refresh_ttl_secs,
                },
                wire_ctx,
                ctx,
            )
            .await
            .map_err(wire_error_to_tonic_status)?;

        let refresh_expires_at =
            Utc::now() + chrono::Duration::seconds(jwt_config.session_refresh_ttl_secs as i64);

        Ok(SessionTokens {
            access_token,
            access_expires_at,
            refresh_token: refresh_token_str,
            refresh_expires_at,
        })
    }

    /// Resolves a user slug to internal ID, external slug, and home region.
    ///
    /// This is the common preamble for credential and TOTP handlers that need
    /// to know which region to propose to or read from.
    pub(super) fn resolve_user_region(
        &self,
        slug: Option<inferadb_ledger_types::UserSlug>,
        ctx: &mut inferadb_ledger_raft::logging::CanonicalLogLine,
    ) -> Result<
        (
            inferadb_ledger_types::UserId,
            inferadb_ledger_types::UserSlug,
            inferadb_ledger_types::Region,
        ),
        Status,
    > {
        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let user_id = slug_resolver
            .extract_and_resolve_user(slug)
            .inspect_err(|status| {
                ctx.set_error("InvalidArgument", &status.message);
            })
            .map_err(wire_error_to_tonic_status)?;
        let user_slug =
            SlugResolver::extract_user_slug(slug).map_err(wire_error_to_tonic_status)?;

        let sys_svc = self.ctx.system_service();
        let dir_entry = sys_svc
            .get_user_directory(user_id)
            .map_err(|e| wire_error_to_tonic_status(error_classify::storage_error(&e)))?
            .ok_or_else(|| {
                ctx.set_error("NotFound", "User directory entry not found");
                Status::not_found("User not found")
            })?;
        let region = dir_entry.region.ok_or_else(|| {
            ctx.set_error("Internal", "User has no assigned region");
            Status::internal("User has no assigned region")
        })?;

        Ok((user_id, user_slug, region))
    }

    pub(super) fn verify_totp_code(
        secret: &[u8],
        code: &str,
        algorithm: inferadb_ledger_types::TotpAlgorithm,
        digits: u8,
        period: u32,
    ) -> bool {
        use std::time::{SystemTime, UNIX_EPOCH};

        use hmac::{Hmac, KeyInit, Mac};

        // Reject if code length doesn't match configured digits (prevents ct_eq length leak)
        if code.len() != digits as usize {
            return false;
        }

        let Ok(now) = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()) else {
            // System clock before UNIX epoch — refuse to verify rather than using counter 0
            return false;
        };
        let counter = now / u64::from(period);
        let modulus = 10u32.pow(u32::from(digits));

        // Check current step, next step (+1), and previous step (-1 via wrapping)
        for offset in [0u64, 1, u64::MAX] {
            let step = counter.wrapping_add(offset);
            let step_bytes = step.to_be_bytes();

            let computed = match algorithm {
                inferadb_ledger_types::TotpAlgorithm::Sha1 => {
                    let Ok(mut mac) = Hmac::<sha1::Sha1>::new_from_slice(secret) else {
                        return false;
                    };
                    mac.update(&step_bytes);
                    Self::dynamic_truncate(&mac.finalize().into_bytes(), modulus)
                },
                inferadb_ledger_types::TotpAlgorithm::Sha256 => {
                    let Ok(mut mac) = Hmac::<sha2::Sha256>::new_from_slice(secret) else {
                        return false;
                    };
                    mac.update(&step_bytes);
                    Self::dynamic_truncate(&mac.finalize().into_bytes(), modulus)
                },
                inferadb_ledger_types::TotpAlgorithm::Sha512 => {
                    let Ok(mut mac) = Hmac::<sha2::Sha512>::new_from_slice(secret) else {
                        return false;
                    };
                    mac.update(&step_bytes);
                    Self::dynamic_truncate(&mac.finalize().into_bytes(), modulus)
                },
            };

            let expected = format!("{computed:0>width$}", width = digits as usize);
            if subtle::ConstantTimeEq::ct_eq(expected.as_bytes(), code.as_bytes()).into() {
                return true;
            }
        }
        false
    }

    /// RFC 4226 §5.4 dynamic truncation: extracts a `digits`-length code from an HMAC result.
    fn dynamic_truncate(hmac_result: &[u8], modulus: u32) -> u32 {
        let offset = (hmac_result[hmac_result.len() - 1] & 0x0F) as usize;
        let bin_code = u32::from_be_bytes([
            hmac_result[offset] & 0x7F,
            hmac_result[offset + 1],
            hmac_result[offset + 2],
            hmac_result[offset + 3],
        ]);
        bin_code % modulus
    }

    /// Signs a JWT and builds a `TokenPair` after successful TOTP or recovery code verification.
    ///
    /// Reads the user from state (for role/version), signs the access token, and assembles
    /// the full token pair with the pre-generated refresh token.
    pub(super) fn sign_session_after_challenge(
        &self,
        user_id: inferadb_ledger_types::UserId,
        user_slug: inferadb_ledger_types::UserSlug,
        refresh_token_str: String,
        signing_key: &inferadb_ledger_state::system::SigningKey,
        refresh_ttl_secs: u64,
    ) -> Result<ws::TokenPair, WireError> {
        let jwt_engine = self.ctx.jwt_engine.as_ref().ok_or_else(|| {
            WireError::new(ErrorCode::FailedPrecondition, "JWT engine not configured")
        })?;

        let sys_svc = self.ctx.system_service();
        let dir_entry = sys_svc
            .get_user_directory(user_id)
            .map_err(|e| error_classify::storage_error(&e))?
            .ok_or_else(|| WireError::new(ErrorCode::Internal, "User directory not found"))?;
        let user_region = dir_entry
            .region
            .ok_or_else(|| WireError::new(ErrorCode::Internal, "User has no assigned region"))?;

        let regional_state =
            self.ctx.regional_state(user_region).map_err(tonic_status_to_wire_error)?;
        let regional_sys = self.ctx.regional_system_service(regional_state);
        let user = regional_sys
            .get_user(user_id)
            .map_err(|e| error_classify::storage_error(&e))?
            .ok_or_else(|| {
            WireError::new(ErrorCode::Internal, "User not found after verification")
        })?;

        let role_str = match user.role {
            inferadb_ledger_types::UserRole::Admin => "admin",
            inferadb_ledger_types::UserRole::User => "user",
        };

        let (access_token, access_expires_at) = jwt_engine
            .sign_user_session(user_slug, role_str, user.version, &signing_key.kid)
            .map_err(|e| error_classify::crypto_error(&e))?;

        let refresh_expires_at = Utc::now() + chrono::Duration::seconds(refresh_ttl_secs as i64);

        // Wire `TokenPair.{access,refresh}_expires_at` are UNIX nanoseconds (`u64`).
        Ok(ws::TokenPair {
            access_token,
            refresh_token: refresh_token_str,
            access_expires_at: access_expires_at.timestamp_nanos_opt().unwrap_or(0).max(0) as u64,
            refresh_expires_at: refresh_expires_at.timestamp_nanos_opt().unwrap_or(0).max(0) as u64,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods, clippy::panic)]
mod tests {
    use super::*;

    // =========================================================================
    // dynamic_truncate tests (RFC 4226 §5.4)
    // =========================================================================

    #[test]
    fn dynamic_truncate_extracts_code_from_hmac() {
        // RFC 4226 Appendix D test vector for HOTP counter=0 with
        // secret "12345678901234567890" (ASCII):
        // HMAC-SHA1 = cc93cf18508d94934c64b65d8ba7667fb7cde4b0
        // Offset = last nibble = 0x0 → offset 0
        // Binary code = 0x4c93cf18 & 0x7FFFFFFF = 0x4c93cf18
        // OTP = 0x4c93cf18 % 10^6 = 755224
        let hmac_result: [u8; 20] = [
            0xcc, 0x93, 0xcf, 0x18, 0x50, 0x8d, 0x94, 0x93, 0x4c, 0x64, 0xb6, 0x5d, 0x8b, 0xa7,
            0x66, 0x7f, 0xb7, 0xcd, 0xe4, 0xb0,
        ];
        let result = UserService::dynamic_truncate(&hmac_result, 1_000_000);
        assert_eq!(result, 755_224);
    }

    #[test]
    fn dynamic_truncate_different_offset() {
        // Craft an HMAC result where the last byte's low nibble points to offset 4
        let mut hmac = [0u8; 20];
        hmac[19] = 0x04; // offset = 4
        // Place known bytes at offset 4..8
        hmac[4] = 0x7F; // high bit clear
        hmac[5] = 0x12;
        hmac[6] = 0x34;
        hmac[7] = 0x56;
        let result = UserService::dynamic_truncate(&hmac, 1_000_000);
        // 0x7F123456 = 2131899478, mod 10^6 = 899478
        assert_eq!(result, 899_478);
    }

    #[test]
    fn dynamic_truncate_clears_high_bit() {
        // When the byte at offset has the high bit set, it should be cleared
        let mut hmac = [0u8; 20];
        hmac[19] = 0x00; // offset = 0
        hmac[0] = 0xFF; // high bit set → after & 0x7F → 0x7F
        hmac[1] = 0xFF;
        hmac[2] = 0xFF;
        hmac[3] = 0xFF;
        let result = UserService::dynamic_truncate(&hmac, 1_000_000);
        // 0x7FFFFFFF = 2147483647, mod 10^6 = 483647
        assert_eq!(result, 483_647);
    }

    // =========================================================================
    // verify_totp_code tests
    // =========================================================================

    /// Generates a TOTP code for the current time step using the specified algorithm.
    fn generate_totp_code(
        secret: &[u8],
        algorithm: inferadb_ledger_types::TotpAlgorithm,
        digits: u8,
        period: u32,
    ) -> String {
        use std::time::{SystemTime, UNIX_EPOCH};

        use hmac::{Hmac, KeyInit, Mac};

        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let counter = now / u64::from(period);
        let step_bytes = counter.to_be_bytes();

        let modulus = 10u32.pow(u32::from(digits));
        let computed = match algorithm {
            inferadb_ledger_types::TotpAlgorithm::Sha1 => {
                let mut mac = Hmac::<sha1::Sha1>::new_from_slice(secret).unwrap();
                mac.update(&step_bytes);
                UserService::dynamic_truncate(&mac.finalize().into_bytes(), modulus)
            },
            inferadb_ledger_types::TotpAlgorithm::Sha256 => {
                let mut mac = Hmac::<sha2::Sha256>::new_from_slice(secret).unwrap();
                mac.update(&step_bytes);
                UserService::dynamic_truncate(&mac.finalize().into_bytes(), modulus)
            },
            inferadb_ledger_types::TotpAlgorithm::Sha512 => {
                let mut mac = Hmac::<sha2::Sha512>::new_from_slice(secret).unwrap();
                mac.update(&step_bytes);
                UserService::dynamic_truncate(&mac.finalize().into_bytes(), modulus)
            },
        };
        format!("{computed:0>width$}", width = digits as usize)
    }

    #[test]
    fn verify_totp_code_accepts_valid_code() {
        let secret = b"12345678901234567890";
        let alg = inferadb_ledger_types::TotpAlgorithm::Sha1;
        let code = generate_totp_code(secret, alg, 6, 30);
        assert!(
            UserService::verify_totp_code(secret, &code, alg, 6, 30),
            "should accept a code generated for the current time step"
        );
    }

    #[test]
    fn verify_totp_code_rejects_wrong_code() {
        let secret = b"12345678901234567890";
        let alg = inferadb_ledger_types::TotpAlgorithm::Sha1;
        // Generate the valid code, then offset by 1 to guarantee rejection
        let valid = generate_totp_code(secret, alg, 6, 30);
        let valid_n: u32 = valid.parse().unwrap();
        let wrong = format!("{:06}", (valid_n + 1) % 1_000_000);
        assert!(
            !UserService::verify_totp_code(secret, &wrong, alg, 6, 30),
            "should reject code that is off by 1 from the valid code"
        );
    }

    #[test]
    fn verify_totp_code_rejects_wrong_length() {
        let secret = b"12345678901234567890";
        // Code length mismatch: configured for 6 digits but passing 8
        assert!(
            !UserService::verify_totp_code(
                secret,
                "12345678",
                inferadb_ledger_types::TotpAlgorithm::Sha1,
                6,
                30,
            ),
            "should reject code with wrong length"
        );
    }

    #[test]
    fn verify_totp_code_rejects_empty_secret() {
        // Empty secret causes HMAC to fail
        assert!(
            !UserService::verify_totp_code(
                &[],
                "123456",
                inferadb_ledger_types::TotpAlgorithm::Sha1,
                6,
                30,
            ),
            "should handle empty secret gracefully"
        );
    }

    #[test]
    fn verify_totp_code_sha256_accepts_valid() {
        let secret = b"12345678901234567890123456789012"; // 32-byte secret for SHA-256
        let alg = inferadb_ledger_types::TotpAlgorithm::Sha256;
        let code = generate_totp_code(secret, alg, 6, 30);
        assert!(
            UserService::verify_totp_code(secret, &code, alg, 6, 30),
            "should accept SHA-256 TOTP code for current time step"
        );
    }

    #[test]
    fn verify_totp_code_sha512_accepts_valid() {
        let secret = b"1234567890123456789012345678901234567890123456789012345678901234"; // 64-byte
        let alg = inferadb_ledger_types::TotpAlgorithm::Sha512;
        let code = generate_totp_code(secret, alg, 6, 30);
        assert!(
            UserService::verify_totp_code(secret, &code, alg, 6, 30),
            "should accept SHA-512 TOTP code for current time step"
        );
    }

    #[test]
    fn verify_totp_code_8_digits() {
        let secret = b"12345678901234567890";
        let alg = inferadb_ledger_types::TotpAlgorithm::Sha1;
        let code = generate_totp_code(secret, alg, 8, 30);
        assert!(
            UserService::verify_totp_code(secret, &code, alg, 8, 30),
            "should accept 8-digit TOTP code"
        );
    }
}
