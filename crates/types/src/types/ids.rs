//! Identifier types generated via `define_id!`/`define_slug!` macros.
//!
//! Includes both internal sequential IDs (i64) for storage and
//! Snowflake-based external slugs (u64) for APIs.

use std::fmt;

use serde::{Deserialize, Serialize};

// ============================================================================
// Identifier Macros
// ============================================================================

/// Generates a newtype wrapper around a numeric type for type-safe identifiers.
///
/// Each generated type provides:
/// - Standard derives: Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord
/// - Serde with `#[serde(transparent)]` for wire format compatibility
/// - `From<inner>` and `Into<inner>` conversions
/// - `Display` with a semantic prefix (e.g., `org:123`)
/// - `new()` constructor and `value()` accessor
macro_rules! define_id {
    (
        $(#[$meta:meta])*
        $name:ident, $inner:ty, $prefix:expr
    ) => {
        $(#[$meta])*
        #[derive(
            Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord,
            Serialize, Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name($inner);

        impl $name {
            /// Creates a new identifier from a raw value.
            #[inline]
            pub const fn new(value: $inner) -> Self {
                Self(value)
            }

            /// Returns the raw numeric value.
            #[inline]
            pub const fn value(self) -> $inner {
                self.0
            }
        }

        impl From<$inner> for $name {
            #[inline]
            fn from(value: $inner) -> Self {
                Self(value)
            }
        }

        impl From<$name> for $inner {
            #[inline]
            fn from(id: $name) -> Self {
                id.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}:{}", $prefix, self.0)
            }
        }

        impl std::str::FromStr for $name {
            type Err = <$inner as std::str::FromStr>::Err;

            fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
                s.parse::<$inner>().map(Self)
            }
        }
    };
}

/// Generates a newtype wrapper around `u64` for Snowflake-based external identifiers.
///
/// Each generated type provides:
/// - Standard derives: Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord
/// - Serde with `#[serde(transparent)]` for wire format compatibility
/// - `From<u64>` and `Into<u64>` conversions
/// - `Display` as raw number (no prefix) for API clarity
/// - `new()` constructor and `value()` accessor
macro_rules! define_slug {
    (
        $(#[$meta:meta])*
        $name:ident
    ) => {
        $(#[$meta])*
        #[derive(
            Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord,
            Serialize, Deserialize, Default,
        )]
        #[serde(transparent)]
        pub struct $name(u64);

        impl $name {
            /// Creates a new slug from a raw Snowflake ID value.
            ///
            /// A value of `0` is a sentinel (e.g., system-vault writes / background
            /// jobs that do not carry an external slug) and is also the value
            /// returned by [`Default::default`].
            #[inline]
            pub const fn new(value: u64) -> Self {
                Self(value)
            }

            /// Returns the raw Snowflake ID value.
            #[inline]
            pub const fn value(self) -> u64 {
                self.0
            }
        }

        impl From<u64> for $name {
            #[inline]
            fn from(value: u64) -> Self {
                Self(value)
            }
        }

        impl From<$name> for u64 {
            #[inline]
            fn from(slug: $name) -> Self {
                slug.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        impl std::str::FromStr for $name {
            type Err = <u64 as std::str::FromStr>::Err;

            fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
                s.parse::<u64>().map(Self)
            }
        }
    };
}

/// Generates a newtype wrapper around `String` for domain identifiers.
///
/// Each generated type provides:
/// - Standard derives: Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord
/// - Serde with `#[serde(transparent)]` for wire format compatibility
/// - `new()`, `value()` (&str), `into_inner()` (String), `as_bytes()` (&[u8])
/// - `From<String>`, `From<&str>`, `Into<String>`, `AsRef<str>`, `Borrow<str>`
/// - `Display` showing the raw string (no prefix)
macro_rules! define_string_id {
    (
        $(#[$meta:meta])*
        $name:ident
    ) => {
        $(#[$meta])*
        #[derive(
            Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord,
            Serialize, Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            /// Creates a new identifier from a string value.
            #[inline]
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            /// Returns the string value as a slice.
            #[inline]
            pub fn value(&self) -> &str {
                &self.0
            }

            /// Consumes the wrapper, returning the inner `String`.
            #[inline]
            pub fn into_inner(self) -> String {
                self.0
            }

            /// Returns the string value as a byte slice.
            #[inline]
            pub fn as_bytes(&self) -> &[u8] {
                self.0.as_bytes()
            }
        }

        impl From<String> for $name {
            #[inline]
            fn from(value: String) -> Self {
                Self(value)
            }
        }

        impl From<&str> for $name {
            #[inline]
            fn from(value: &str) -> Self {
                Self(value.to_owned())
            }
        }

        impl From<$name> for String {
            #[inline]
            fn from(id: $name) -> Self {
                id.0
            }
        }

        impl AsRef<str> for $name {
            #[inline]
            fn as_ref(&self) -> &str {
                &self.0
            }
        }

        impl std::borrow::Borrow<str> for $name {
            #[inline]
            fn borrow(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }
    };
}

// ============================================================================
// Organization Identifiers
// ============================================================================

define_id!(
    /// Internal sequential identifier for an organization (storage-layer only).
    ///
    /// Wraps an `i64` with compile-time type safety to prevent mixing
    /// with [`VaultId`], [`UserId`], or [`UserEmailId`]. Never exposed in APIs —
    /// use [`OrganizationSlug`] for external identification.
    ///
    /// # Display
    ///
    /// Formats with `org:` prefix: `org:42`.
    OrganizationId, i64, "org"
);

define_slug!(
    /// Snowflake-generated external identifier for an organization.
    ///
    /// Wraps a `u64` Snowflake ID that is the sole external identifier for
    /// organizations in gRPC APIs and the SDK.
    ///
    /// # Display
    ///
    /// Displays as the raw number: `1234567890`.
    OrganizationSlug
);

// ============================================================================
// Vault Identifiers
// ============================================================================

define_id!(
    /// Internal sequential identifier for a vault within an organization (storage-layer only).
    ///
    /// Never exposed in APIs — use [`VaultSlug`] for external identification.
    ///
    /// # Display
    ///
    /// Formats with `vault:` prefix: `vault:7`.
    VaultId, i64, "vault"
);

define_slug!(
    /// Snowflake-generated external identifier for a vault.
    ///
    /// Wraps a `u64` Snowflake ID that is the sole external identifier for
    /// vaults in gRPC APIs and the SDK.
    ///
    /// # Display
    ///
    /// Displays as the raw number: `1234567890`.
    VaultSlug
);

// ============================================================================
// User Identifiers
// ============================================================================

define_id!(
    /// Internal sequential identifier for a user (storage-layer only).
    ///
    /// Never exposed in APIs — use [`UserSlug`] for external identification.
    ///
    /// # Display
    ///
    /// Formats with `user:` prefix: `user:1`.
    UserId, i64, "user"
);

define_slug!(
    /// Snowflake-generated external identifier for a user.
    ///
    /// Wraps a `u64` Snowflake ID that is the sole external identifier for
    /// users in gRPC APIs and the SDK.
    ///
    /// # Display
    ///
    /// Displays as the raw number: `1234567890`.
    UserSlug
);

define_id!(
    /// Unique identifier for a user email record.
    /// Never exposed in APIs.
    ///
    /// Wraps an `i64` with compile-time type safety to prevent mixing
    /// with [`UserId`], [`OrganizationId`], or [`VaultId`].
    ///
    /// # Display
    ///
    /// Formats with `email:` prefix: `email:42`.
    UserEmailId, i64, "email"
);

define_id!(
    /// Unique identifier for an email verification token.
    /// Never exposed in APIs.
    ///
    /// Sequential `i64` assigned by the Raft leader from the
    /// `_meta:seq:email_verify` sequence counter.
    ///
    /// Wraps an `i64` with compile-time type safety to prevent mixing
    /// with [`UserEmailId`], [`UserId`], or other identifier types.
    ///
    /// # Display
    ///
    /// Formats with `verify:` prefix: `verify:42`.
    EmailVerifyTokenId, i64, "verify"
);

// ============================================================================
// Team Identifiers
// ============================================================================

define_id!(
    /// Internal sequential identifier for a team within an organization.
    ///
    /// Wraps an `i64` with compile-time type safety to prevent mixing
    /// with [`OrganizationId`], [`VaultId`], [`UserId`], or other identifiers.
    /// Never exposed in APIs — use [`TeamSlug`] for external identification.
    ///
    /// # Display
    ///
    /// Formats with `team:` prefix: `team:3`.
    TeamId, i64, "team"
);

impl Default for TeamId {
    fn default() -> Self {
        Self::new(0)
    }
}

define_slug!(
    /// Snowflake-generated external identifier for a team.
    ///
    /// Wraps a `u64` Snowflake ID that is the sole external identifier for
    /// teams in gRPC APIs and the SDK.
    ///
    /// # Display
    ///
    /// Displays as the raw number: `1234567890`.
    TeamSlug
);

// ============================================================================
// App Identifiers
// ============================================================================

define_id!(
    /// Internal sequential identifier for an application within an organization.
    ///
    /// Wraps an `i64` with compile-time type safety. Never exposed in APIs —
    /// use [`AppSlug`] for external identification.
    ///
    /// # Display
    ///
    /// Formats with `app:` prefix: `app:5`.
    AppId, i64, "app"
);

impl Default for AppId {
    fn default() -> Self {
        Self::new(0)
    }
}

define_slug!(
    /// Snowflake-generated external identifier for an application.
    ///
    /// Wraps a `u64` Snowflake ID that is the sole external identifier for
    /// applications in gRPC APIs and the SDK.
    ///
    /// # Display
    ///
    /// Displays as the raw number: `1234567890`.
    AppSlug
);

// ============================================================================
// Other Identifiers
// ============================================================================

define_id!(
    /// Internal sequential identifier for a client assertion entry.
    ///
    /// Wraps an `i64` with compile-time type safety. Used within the
    /// application's client assertion credential type to identify
    /// individual certificate entries.
    ///
    /// # Display
    ///
    /// Formats with `assertion:` prefix: `assertion:2`.
    ClientAssertionId, i64, "assertion"
);

impl Default for ClientAssertionId {
    fn default() -> Self {
        Self::new(0)
    }
}

define_id!(
    /// Internal sequential identifier for a signing key.
    ///
    /// Wraps an `i64` assigned by the Raft leader from the
    /// `_meta:seq:signing_key` sequence counter.
    ///
    /// # Display
    ///
    /// Formats with `sigkey:` prefix: `sigkey:1`.
    SigningKeyId, i64, "sigkey"
);

impl Default for SigningKeyId {
    fn default() -> Self {
        Self::new(0)
    }
}

define_id!(
    /// Internal sequential identifier for a refresh token.
    ///
    /// Wraps an `i64` assigned by the Raft leader from the
    /// `_meta:seq:refresh_token` sequence counter.
    ///
    /// # Display
    ///
    /// Formats with `rtoken:` prefix: `rtoken:42`.
    RefreshTokenId, i64, "rtoken"
);

impl Default for RefreshTokenId {
    fn default() -> Self {
        Self::new(0)
    }
}

define_id!(
    /// Internal sequential identifier for a user credential.
    ///
    /// Wraps an `i64` assigned by the Raft leader from the
    /// `_meta:seq:user_credential` REGIONAL sequence counter.
    ///
    /// # Display
    ///
    /// Formats with `ucred:` prefix: `ucred:1`.
    UserCredentialId, i64, "ucred"
);

impl Default for UserCredentialId {
    fn default() -> Self {
        Self::new(0)
    }
}

define_id!(
    /// Internal sequential identifier for an organization invitation.
    ///
    /// Wraps an `i64` assigned by the Raft leader from the
    /// `_meta:seq:invite` REGIONAL sequence counter.
    ///
    /// # Display
    ///
    /// Formats with `invite:` prefix: `invite:1`.
    InviteId, i64, "invite"
);

impl Default for InviteId {
    fn default() -> Self {
        Self::new(0)
    }
}

define_slug!(
    /// External Snowflake identifier for an organization invitation.
    ///
    /// Used in gRPC APIs and SDK methods. Resolved to [`InviteId`] at
    /// the service boundary via slug index lookup.
    ///
    /// # Display
    ///
    /// Displays as the raw `u64` value: `1234567890`.
    InviteSlug
);

// ============================================================================
// TokenVersion
// ============================================================================

/// Monotonic counter for forced session invalidation.
///
/// Incremented on password change, account compromise, or admin force-revoke.
/// Stored on the `User` entity and embedded in JWT claims. During token
/// validation, the claim's version is compared against the current stored
/// version — a mismatch means the session was force-invalidated.
///
/// # Display
///
/// Formats with `v` prefix: `v0`, `v3`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TokenVersion(u64);

impl TokenVersion {
    /// Creates a new version from a raw counter value.
    #[inline]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the raw counter value.
    #[inline]
    pub const fn value(self) -> u64 {
        self.0
    }

    /// Returns the next version (current + 1), saturating at `u64::MAX`.
    #[inline]
    pub const fn increment(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

impl From<u64> for TokenVersion {
    #[inline]
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<TokenVersion> for u64 {
    #[inline]
    fn from(v: TokenVersion) -> Self {
        v.0
    }
}

impl fmt::Display for TokenVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "v{}", self.0)
    }
}

// ============================================================================
// Transaction ID and String Identifiers
// ============================================================================

/// Transaction identifier (16 bytes, typically UUIDv4).
pub type TxId = [u8; 16];

define_string_id!(
    /// Node identifier in the Raft cluster.
    ///
    /// Wraps a `String` with compile-time type safety to prevent mixing
    /// with [`ClientId`] or other string identifiers.
    NodeId
);

define_string_id!(
    /// Client identifier for idempotency tracking.
    ///
    /// Wraps a `String` with compile-time type safety to prevent mixing
    /// with [`NodeId`] or other string identifiers.
    ClientId
);

/// Numeric node identifier for the consensus engine.
///
/// Uses `u64` for efficient storage and comparison. Mapped from
/// human-readable [`NodeId`] strings (e.g., `"node-1"`) via the
/// `_system` organization's node registry.
pub type LedgerNodeId = u64;

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::borrow::Borrow;

    use super::*;

    // ── define_id! (OrganizationId as representative) ───────────────

    #[test]
    fn id_new_and_value() {
        let id = OrganizationId::new(42);
        assert_eq!(id.value(), 42);
    }

    #[test]
    fn id_from_and_into() {
        let id: OrganizationId = 7_i64.into();
        assert_eq!(id.value(), 7);
        let raw: i64 = id.into();
        assert_eq!(raw, 7);
    }

    #[test]
    fn id_display_has_prefix() {
        assert_eq!(OrganizationId::new(42).to_string(), "org:42");
        assert_eq!(VaultId::new(7).to_string(), "vault:7");
        assert_eq!(UserId::new(1).to_string(), "user:1");
        assert_eq!(SigningKeyId::new(3).to_string(), "sigkey:3");
    }

    #[test]
    fn id_from_str() {
        let id: OrganizationId = "42".parse().expect("parse");
        assert_eq!(id.value(), 42);
    }

    #[test]
    fn id_from_str_invalid() {
        let result = "not_a_number".parse::<OrganizationId>();
        assert!(result.is_err());
    }

    #[test]
    fn id_serde_roundtrip() {
        let id = OrganizationId::new(99);
        let json = serde_json::to_string(&id).expect("serialize");
        assert_eq!(json, "99");
        let back: OrganizationId = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back, id);
    }

    #[test]
    fn id_ord_and_eq() {
        let a = VaultId::new(1);
        let b = VaultId::new(2);
        assert!(a < b);
        assert_eq!(a, VaultId::new(1));
    }

    // ── define_slug! (OrganizationSlug as representative) ───────────

    #[test]
    fn slug_new_and_value() {
        let slug = OrganizationSlug::new(123456);
        assert_eq!(slug.value(), 123456);
    }

    #[test]
    fn slug_from_and_into() {
        let slug: OrganizationSlug = 99_u64.into();
        assert_eq!(slug.value(), 99);
        let raw: u64 = slug.into();
        assert_eq!(raw, 99);
    }

    #[test]
    fn slug_display_no_prefix() {
        assert_eq!(OrganizationSlug::new(42).to_string(), "42");
        assert_eq!(VaultSlug::new(777).to_string(), "777");
    }

    #[test]
    fn slug_from_str() {
        let slug: OrganizationSlug = "12345".parse().expect("parse");
        assert_eq!(slug.value(), 12345);
    }

    #[test]
    fn slug_from_str_invalid() {
        let result = "abc".parse::<VaultSlug>();
        assert!(result.is_err());
    }

    #[test]
    fn slug_serde_roundtrip() {
        let slug = VaultSlug::new(55);
        let json = serde_json::to_string(&slug).expect("serialize");
        assert_eq!(json, "55");
        let back: VaultSlug = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back, slug);
    }

    // ── define_string_id! (NodeId as representative) ────────────────

    #[test]
    fn string_id_new_and_value() {
        let id = NodeId::new("node-1");
        assert_eq!(id.value(), "node-1");
    }

    #[test]
    fn string_id_into_inner() {
        let id = NodeId::new("node-1");
        let s: String = id.into_inner();
        assert_eq!(s, "node-1");
    }

    #[test]
    fn string_id_as_bytes() {
        let id = NodeId::new("abc");
        assert_eq!(id.as_bytes(), b"abc");
    }

    #[test]
    fn string_id_from_string_and_str() {
        let from_string: NodeId = String::from("node-2").into();
        let from_str: NodeId = "node-2".into();
        assert_eq!(from_string, from_str);
    }

    #[test]
    fn string_id_as_ref_and_borrow() {
        let id = ClientId::new("client-x");
        let r: &str = id.as_ref();
        assert_eq!(r, "client-x");
        let b: &str = id.borrow();
        assert_eq!(b, "client-x");
    }

    #[test]
    fn string_id_display() {
        let id = NodeId::new("leader-node");
        assert_eq!(id.to_string(), "leader-node");
    }

    #[test]
    fn string_id_into_string() {
        let id = ClientId::new("c1");
        let s: String = id.into();
        assert_eq!(s, "c1");
    }

    #[test]
    fn string_id_serde_roundtrip() {
        let id = NodeId::new("node-3");
        let json = serde_json::to_string(&id).expect("serialize");
        assert_eq!(json, "\"node-3\"");
        let back: NodeId = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back.value(), "node-3");
    }

    // ── TokenVersion ────────────────────────────────────────────────

    #[test]
    fn token_version_new_and_value() {
        let v = TokenVersion::new(5);
        assert_eq!(v.value(), 5);
    }

    #[test]
    fn token_version_default_is_zero() {
        assert_eq!(TokenVersion::default().value(), 0);
    }

    #[test]
    fn token_version_increment() {
        let v = TokenVersion::new(3);
        assert_eq!(v.increment().value(), 4);
    }

    #[test]
    fn token_version_increment_saturates() {
        let v = TokenVersion::new(u64::MAX);
        assert_eq!(v.increment().value(), u64::MAX);
    }

    #[test]
    fn token_version_from_and_into() {
        let v: TokenVersion = 10_u64.into();
        assert_eq!(v.value(), 10);
        let raw: u64 = v.into();
        assert_eq!(raw, 10);
    }

    #[test]
    fn token_version_display() {
        assert_eq!(TokenVersion::new(0).to_string(), "v0");
        assert_eq!(TokenVersion::new(42).to_string(), "v42");
    }

    #[test]
    fn token_version_serde_roundtrip() {
        let v = TokenVersion::new(7);
        let json = serde_json::to_string(&v).expect("serialize");
        assert_eq!(json, "7");
        let back: TokenVersion = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back, v);
    }
}
