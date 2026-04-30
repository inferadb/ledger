//! Error model for the InferaDB Ledger wire protocol.
//!
//! Distinct from `inferadb_ledger_types::ErrorCode` (the server-side
//! in-memory enum). The wire `ErrorCode` carries explicit `u32` discriminants
//! that become part of the wire contract — renumbering is a protocol-breaking
//! change.
//!
//! Discriminant `0` is reserved for "no error" (the frame header's
//! `error_code: u32` field carries `0` for success frames, a non-zero value
//! for error frames).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Wire-protocol error code with explicit `u32` discriminants for stable
/// wire encoding.
///
/// Variants match `inferadb_ledger_types::ErrorCode` in name; the numeric
/// values are wire-stable and MUST NOT be renumbered without bumping the
/// protocol version.
///
/// Intentionally has no `Default` impl — discriminant `0` is reserved for
/// "no error" in the frame header and does not map to any variant.
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorCode {
    /// Entity not found (organization, vault, user, team, etc.).
    NotFound = 1,
    /// Entity already exists (duplicate name, slug collision, etc.).
    AlreadyExists = 2,
    /// Operation violates a precondition (wrong state, dependency exists, etc.).
    FailedPrecondition = 3,
    /// Caller lacks permission for this operation.
    PermissionDenied = 4,
    /// Invalid input (bad name, missing field, etc.).
    InvalidArgument = 5,
    /// Internal error (storage failure, serialization error, etc.).
    Internal = 6,
    /// Authentication failure (invalid token, expired, revoked, bad signature).
    Unauthenticated = 7,
    /// Rate limit exceeded (too many requests in the time window).
    RateLimited = 8,
    /// Resource has expired (verification code, onboarding token, etc.).
    Expired = 9,
    /// Too many failed attempts (code verification, authentication, etc.).
    TooManyAttempts = 10,
    /// Invitation rate limit exceeded (per-user, per-org, per-email, or cooldown).
    InvitationRateLimited = 11,
    /// Invitation is no longer pending (already accepted, declined, expired, or revoked).
    InvitationAlreadyResolved = 12,
    /// User's email does not match the invitee address.
    InvitationEmailMismatch = 13,
    /// Invitee email belongs to an existing member of the inviting organization.
    InvitationAlreadyMember = 14,
    /// A pending invitation already exists for this org+email combination.
    InvitationDuplicatePending = 15,
    /// Vault routing changed between slug resolution and proposal submission.
    StaleRouting = 16,
    /// RPC, message, or feature is deprecated and the server refuses to handle it.
    Deprecated = 17,
}

/// Returned when a wire `error_code` value cannot be mapped to a known variant
/// (forward-compatibility: an older client may receive an error code
/// introduced in a newer server version).
#[derive(Debug, Clone, Copy, Error, PartialEq, Eq)]
#[error("unknown wire error code {0}")]
pub struct UnknownErrorCode(pub u32);

impl ErrorCode {
    /// Returns the wire `u32` discriminant value.
    #[must_use]
    pub const fn as_u32(self) -> u32 {
        self as u32
    }

    /// Attempts to construct an `ErrorCode` from a wire `u32` value.
    ///
    /// # Errors
    ///
    /// Returns `UnknownErrorCode(value)` if the value doesn't match any
    /// known variant. Forward-compatibility: a newer server may use codes
    /// an older client doesn't recognize.
    pub fn try_from_u32(value: u32) -> Result<Self, UnknownErrorCode> {
        match value {
            1 => Ok(Self::NotFound),
            2 => Ok(Self::AlreadyExists),
            3 => Ok(Self::FailedPrecondition),
            4 => Ok(Self::PermissionDenied),
            5 => Ok(Self::InvalidArgument),
            6 => Ok(Self::Internal),
            7 => Ok(Self::Unauthenticated),
            8 => Ok(Self::RateLimited),
            9 => Ok(Self::Expired),
            10 => Ok(Self::TooManyAttempts),
            11 => Ok(Self::InvitationRateLimited),
            12 => Ok(Self::InvitationAlreadyResolved),
            13 => Ok(Self::InvitationEmailMismatch),
            14 => Ok(Self::InvitationAlreadyMember),
            15 => Ok(Self::InvitationDuplicatePending),
            16 => Ok(Self::StaleRouting),
            17 => Ok(Self::Deprecated),
            other => Err(UnknownErrorCode(other)),
        }
    }
}

impl serde::Serialize for ErrorCode {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_u32(self.as_u32())
    }
}

impl<'de> serde::Deserialize<'de> for ErrorCode {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let v = u32::deserialize(d)?;
        Self::try_from_u32(v)
            .map_err(|_| serde::de::Error::custom(format!("unknown ErrorCode discriminant: {v}")))
    }
}

impl From<inferadb_ledger_types::ErrorCode> for ErrorCode {
    fn from(value: inferadb_ledger_types::ErrorCode) -> Self {
        // Exhaustive match — adding a variant to types::ErrorCode without
        // adding it here is a compile error. Lockstep enforced.
        use inferadb_ledger_types::ErrorCode as TypesCode;
        match value {
            TypesCode::NotFound => Self::NotFound,
            TypesCode::AlreadyExists => Self::AlreadyExists,
            TypesCode::FailedPrecondition => Self::FailedPrecondition,
            TypesCode::PermissionDenied => Self::PermissionDenied,
            TypesCode::InvalidArgument => Self::InvalidArgument,
            TypesCode::Internal => Self::Internal,
            TypesCode::Unauthenticated => Self::Unauthenticated,
            TypesCode::RateLimited => Self::RateLimited,
            TypesCode::Expired => Self::Expired,
            TypesCode::TooManyAttempts => Self::TooManyAttempts,
            TypesCode::InvitationRateLimited => Self::InvitationRateLimited,
            TypesCode::InvitationAlreadyResolved => Self::InvitationAlreadyResolved,
            TypesCode::InvitationEmailMismatch => Self::InvitationEmailMismatch,
            TypesCode::InvitationAlreadyMember => Self::InvitationAlreadyMember,
            TypesCode::InvitationDuplicatePending => Self::InvitationDuplicatePending,
            TypesCode::StaleRouting => Self::StaleRouting,
            TypesCode::Deprecated => Self::Deprecated,
        }
    }
}

/// Variable-length payload of an error frame.
///
/// Carried in the frame's payload bytes (postcard-encoded) when the frame
/// header's `error_code != 0`. The header's `error_code` is the fast-path
/// discriminant (`u32` value matching `ErrorCode::as_u32`); this payload
/// provides the human-readable detail.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WireError {
    /// Same numeric value as the frame header's `error_code` field; redundant
    /// for fast-path callers that already inspected the header but useful for
    /// log-line completeness when only the payload is captured.
    pub code: ErrorCode,
    /// Human-readable error message. Should be operator-friendly and avoid
    /// leaking internal state (e.g., do not include raw stack traces or
    /// internal IDs).
    pub message: String,
    /// True if the SDK should retry the request after `retry_after_ms`.
    /// False means the error is terminal (e.g., `NotFound`, `PermissionDenied`).
    pub retryable: bool,
    /// Suggested backoff in milliseconds before retrying. Zero when
    /// `retryable == false` or no specific delay is recommended.
    pub retry_after_ms: u64,
    /// Structured context key/value pairs (e.g., entity name, region slug).
    /// Uses `BTreeMap` for deterministic serialization order, which is required
    /// for byte-level equality and content-addressable caching.
    pub context: BTreeMap<String, String>,
    /// Operator-facing remediation suggestion (e.g., "increase rate limit
    /// quota", "wait for migration to complete"). Empty string if none.
    pub suggested_action: String,
}

impl WireError {
    /// Constructs a `WireError` with `retryable = false`, no retry delay,
    /// and no context/action — minimal error for cases where only the code
    /// and message are known.
    #[must_use]
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            retryable: false,
            retry_after_ms: 0,
            context: BTreeMap::new(),
            suggested_action: String::new(),
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn error_code_as_u32_round_trip() {
        let variants = [
            ErrorCode::NotFound,
            ErrorCode::AlreadyExists,
            ErrorCode::FailedPrecondition,
            ErrorCode::PermissionDenied,
            ErrorCode::InvalidArgument,
            ErrorCode::Internal,
            ErrorCode::Unauthenticated,
            ErrorCode::RateLimited,
            ErrorCode::Expired,
            ErrorCode::TooManyAttempts,
            ErrorCode::InvitationRateLimited,
            ErrorCode::InvitationAlreadyResolved,
            ErrorCode::InvitationEmailMismatch,
            ErrorCode::InvitationAlreadyMember,
            ErrorCode::InvitationDuplicatePending,
            ErrorCode::StaleRouting,
            ErrorCode::Deprecated,
        ];
        for variant in variants {
            assert_eq!(
                ErrorCode::try_from_u32(variant.as_u32()),
                Ok(variant),
                "round-trip failed for {variant:?}"
            );
        }
    }

    #[test]
    fn error_code_zero_is_not_a_valid_variant() {
        assert_eq!(ErrorCode::try_from_u32(0), Err(UnknownErrorCode(0)));
    }

    #[test]
    fn error_code_try_from_u32_unknown_returns_error() {
        assert_eq!(ErrorCode::try_from_u32(99999), Err(UnknownErrorCode(99999)));
    }

    #[test]
    fn error_code_postcard_encodes_discriminant_value() {
        use postcard::to_allocvec;

        // Postcard encodes the u32 discriminant directly (varint by default).
        let code = ErrorCode::NotFound;
        let encoded = to_allocvec(&code).expect("serialize");
        let expected_u32 = to_allocvec(&code.as_u32()).expect("serialize u32");
        assert_eq!(
            encoded, expected_u32,
            "ErrorCode must serialize as its u32 discriminant, not variant_index"
        );

        // Round-trip still works.
        let decoded: ErrorCode = postcard::from_bytes(&encoded).expect("deserialize");
        assert_eq!(decoded, code);
    }

    #[test]
    fn error_code_postcard_rejects_unknown_discriminant() {
        use postcard::{from_bytes, to_allocvec};
        let unknown = to_allocvec(&999u32).expect("serialize");
        let result: Result<ErrorCode, _> = from_bytes(&unknown);
        assert!(result.is_err(), "decoding unknown discriminant must fail");
    }

    #[test]
    fn wire_error_new_minimal_construction() {
        let err = WireError::new(ErrorCode::NotFound, "not found");
        assert_eq!(err.code, ErrorCode::NotFound);
        assert_eq!(err.message, "not found");
        assert!(!err.retryable);
        assert_eq!(err.retry_after_ms, 0);
        assert!(err.context.is_empty());
        assert_eq!(err.suggested_action, "");
    }

    #[test]
    fn wire_error_serde_round_trip_postcard() {
        let mut context = BTreeMap::new();
        context.insert("entity".to_string(), "vault-42".to_string());
        context.insert("region".to_string(), "eu-west-1".to_string());

        let original = WireError {
            code: ErrorCode::RateLimited,
            message: "rate limit exceeded".to_string(),
            retryable: true,
            retry_after_ms: 500,
            context,
            suggested_action: "reduce request frequency".to_string(),
        };

        let encoded = postcard::to_allocvec(&original).expect("serialize");
        let decoded: WireError = postcard::from_bytes(&encoded).expect("deserialize");
        assert_eq!(decoded, original);
    }

    #[test]
    fn error_code_discriminants_are_stable() {
        assert_eq!(ErrorCode::NotFound as u32, 1);
        assert_eq!(ErrorCode::AlreadyExists as u32, 2);
        assert_eq!(ErrorCode::FailedPrecondition as u32, 3);
        assert_eq!(ErrorCode::PermissionDenied as u32, 4);
        assert_eq!(ErrorCode::InvalidArgument as u32, 5);
        assert_eq!(ErrorCode::Internal as u32, 6);
        assert_eq!(ErrorCode::Unauthenticated as u32, 7);
        assert_eq!(ErrorCode::RateLimited as u32, 8);
        assert_eq!(ErrorCode::Expired as u32, 9);
        assert_eq!(ErrorCode::TooManyAttempts as u32, 10);
        assert_eq!(ErrorCode::InvitationRateLimited as u32, 11);
        assert_eq!(ErrorCode::InvitationAlreadyResolved as u32, 12);
        assert_eq!(ErrorCode::InvitationEmailMismatch as u32, 13);
        assert_eq!(ErrorCode::InvitationAlreadyMember as u32, 14);
        assert_eq!(ErrorCode::InvitationDuplicatePending as u32, 15);
        assert_eq!(ErrorCode::StaleRouting as u32, 16);
        assert_eq!(ErrorCode::Deprecated as u32, 17);
    }

    #[test]
    fn types_error_code_maps_to_wire_error_code_for_all_variants() {
        use inferadb_ledger_types::ErrorCode as TypesCode;
        let cases = [
            (TypesCode::NotFound, ErrorCode::NotFound),
            (TypesCode::AlreadyExists, ErrorCode::AlreadyExists),
            (TypesCode::FailedPrecondition, ErrorCode::FailedPrecondition),
            (TypesCode::PermissionDenied, ErrorCode::PermissionDenied),
            (TypesCode::InvalidArgument, ErrorCode::InvalidArgument),
            (TypesCode::Internal, ErrorCode::Internal),
            (TypesCode::Unauthenticated, ErrorCode::Unauthenticated),
            (TypesCode::RateLimited, ErrorCode::RateLimited),
            (TypesCode::Expired, ErrorCode::Expired),
            (TypesCode::TooManyAttempts, ErrorCode::TooManyAttempts),
            (TypesCode::InvitationRateLimited, ErrorCode::InvitationRateLimited),
            (TypesCode::InvitationAlreadyResolved, ErrorCode::InvitationAlreadyResolved),
            (TypesCode::InvitationEmailMismatch, ErrorCode::InvitationEmailMismatch),
            (TypesCode::InvitationAlreadyMember, ErrorCode::InvitationAlreadyMember),
            (TypesCode::InvitationDuplicatePending, ErrorCode::InvitationDuplicatePending),
            (TypesCode::StaleRouting, ErrorCode::StaleRouting),
            (TypesCode::Deprecated, ErrorCode::Deprecated),
        ];
        for (input, expected) in cases {
            assert_eq!(ErrorCode::from(input), expected, "mismatch for {input:?}");
        }
    }

    #[test]
    fn unknown_error_code_display() {
        assert_eq!(UnknownErrorCode(42).to_string(), "unknown wire error code 42");
    }
}
