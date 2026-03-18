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
/// - `Display` with a semantic prefix (e.g., `ns:123`)
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
            Serialize, Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(u64);

        impl $name {
            /// Creates a new slug from a raw Snowflake ID value.
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
    /// Unique identifier for a vault within an organization.
    ///
    /// Wraps an `i64` with compile-time type safety to prevent mixing
    /// with [`OrganizationId`], [`UserId`], or [`UserEmailId`].
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
    /// Unique identifier for a user in the `_system` organization.
    ///
    /// Wraps an `i64` with compile-time type safety to prevent mixing
    /// with [`OrganizationId`], [`VaultId`], or [`UserEmailId`].
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
    /// `_meta:seq:invite` GLOBAL sequence counter.
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

/// Node identifier in the Raft cluster.
///
/// We use u64 for efficient storage and comparison. The mapping from
/// human-readable node names (e.g., "node-1") to numeric IDs is maintained
/// in the `_system` organization.
pub type LedgerNodeId = u64;
