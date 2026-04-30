//! SDK types for the Token service.
//!
//! Provides ergonomic wrappers around the wire-protocol token types,
//! converting timestamps and enums to idiomatic Rust types. Wire-side
//! conversions live in `crate::ops_wire::token`; the structs here are
//! the consumer-facing surface returned by those dispatch functions.

use std::time::SystemTime;

use inferadb_ledger_types::{AppSlug, UserSlug, VaultSlug};

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
