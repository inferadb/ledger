//! SDK types for the Token service.
//!
//! Provides ergonomic wrappers around the proto-generated token types,
//! converting timestamps and enums to idiomatic Rust types.

use std::time::SystemTime;

use inferadb_ledger_proto::proto;
use inferadb_ledger_types::{AppSlug, UserSlug, VaultSlug};

use crate::proto_util::proto_timestamp_to_system_time;

/// An access + refresh token pair returned by session/vault token creation.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TokenPair {
    /// JWT access token (short-lived).
    pub access_token: String,
    /// Opaque refresh token (long-lived, rotate-on-use).
    pub refresh_token: String,
    /// When the access token expires.
    pub access_expires_at: Option<SystemTime>,
    /// When the refresh token expires.
    pub refresh_expires_at: Option<SystemTime>,
}

impl TokenPair {
    /// Converts from protobuf `TokenPair`.
    pub(crate) fn from_proto(p: proto::TokenPair) -> Self {
        Self {
            access_token: p.access_token,
            refresh_token: p.refresh_token,
            access_expires_at: p
                .access_expires_at
                .as_ref()
                .and_then(proto_timestamp_to_system_time),
            refresh_expires_at: p
                .refresh_expires_at
                .as_ref()
                .and_then(proto_timestamp_to_system_time),
        }
    }
}

/// Parsed claims from a validated access token.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum ValidatedToken {
    /// User session token claims.
    UserSession {
        /// The user's slug identifier.
        user: UserSlug,
        /// User role ("user" or "admin").
        role: String,
    },
    /// Vault access token claims.
    VaultAccess {
        /// Organization slug.
        organization: u64,
        /// Application slug.
        app: AppSlug,
        /// Vault slug.
        vault: VaultSlug,
        /// Granted scopes.
        scopes: Vec<String>,
    },
}

impl ValidatedToken {
    /// Converts from protobuf `ValidateTokenResponse`.
    pub(crate) fn from_proto(r: proto::ValidateTokenResponse) -> Option<Self> {
        match r.claims {
            Some(proto::validate_token_response::Claims::UserSession(c)) => {
                Some(Self::UserSession { user: UserSlug::new(c.user_slug), role: c.role })
            },
            Some(proto::validate_token_response::Claims::VaultAccess(c)) => {
                Some(Self::VaultAccess {
                    organization: c.org_slug,
                    app: AppSlug::new(c.app_slug),
                    vault: VaultSlug::new(c.vault_slug),
                    scopes: c.scopes,
                })
            },
            None => None,
        }
    }
}

/// Public key metadata for token verification (JWKS-style).
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct PublicKeyInfo {
    /// Key identifier (kid).
    pub kid: String,
    /// 32-byte Ed25519 public key.
    pub public_key: Vec<u8>,
    /// Key status ("active", "rotated", "revoked").
    pub status: String,
    /// When this key became valid.
    pub valid_from: Option<SystemTime>,
    /// When this key expires (None if active).
    pub valid_until: Option<SystemTime>,
    /// When this key was created.
    pub created_at: Option<SystemTime>,
}

impl PublicKeyInfo {
    /// Converts from protobuf `PublicKeyInfo`.
    pub(crate) fn from_proto(p: proto::PublicKeyInfo) -> Self {
        Self {
            kid: p.kid,
            public_key: p.public_key,
            status: p.status,
            valid_from: p.valid_from.as_ref().and_then(proto_timestamp_to_system_time),
            valid_until: p.valid_until.as_ref().and_then(proto_timestamp_to_system_time),
            created_at: p.created_at.as_ref().and_then(proto_timestamp_to_system_time),
        }
    }
}
