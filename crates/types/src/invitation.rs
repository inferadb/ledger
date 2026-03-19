//! Organization invitation types.
//!
//! Defines the data model for the invitation lifecycle: status enum,
//! full REGIONAL record, and lightweight GLOBAL index entries.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::types::{InviteId, InviteSlug, OrganizationId, OrganizationMemberRole, TeamId, UserId};

/// Returns the effective invitation status, accounting for lazy expiration.
///
/// If the stored status is [`Pending`](InvitationStatus::Pending) and the
/// invitation has passed its `expires_at` timestamp, returns
/// [`Expired`](InvitationStatus::Expired). Otherwise returns the stored status
/// unchanged.
///
/// This is a free function (not a method) because it's used on both
/// [`OrganizationInvitation`] and [`InviteEmailEntry`] fields.
pub fn effective_invitation_status(
    status: InvitationStatus,
    expires_at: DateTime<Utc>,
    now: DateTime<Utc>,
) -> InvitationStatus {
    if status == InvitationStatus::Pending && expires_at < now {
        InvitationStatus::Expired
    } else {
        status
    }
}

/// State of an organization invitation.
///
/// All transitions originate from [`Pending`](InvitationStatus::Pending).
/// Terminal states (Accepted, Declined, Expired, Revoked) are immutable —
/// enforced atomically by the Raft apply handler's CAS check.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InvitationStatus {
    /// Initial state. Token is active, awaiting invitee action.
    #[default]
    Pending,
    /// Terminal. Invitee accepted; membership was granted.
    Accepted,
    /// Terminal. Invitee explicitly declined.
    Declined,
    /// Terminal. TTL elapsed without action.
    Expired,
    /// Terminal. Org admin cancelled the invitation.
    Revoked,
}

impl InvitationStatus {
    /// Returns `true` if this is a terminal (non-Pending) state.
    pub const fn is_terminal(self) -> bool {
        !matches!(self, Self::Pending)
    }
}

impl std::fmt::Display for InvitationStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending => f.write_str("pending"),
            Self::Accepted => f.write_str("accepted"),
            Self::Declined => f.write_str("declined"),
            Self::Expired => f.write_str("expired"),
            Self::Revoked => f.write_str("revoked"),
        }
    }
}

/// Full invitation record — stored in REGIONAL only (Pattern 1).
///
/// Encrypted with the organization's `OrgShredKey` before entering the
/// Raft log. Contains PII (`invitee_email`) that never appears in GLOBAL state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrganizationInvitation {
    /// Internal sequential identifier.
    pub id: InviteId,
    /// External Snowflake identifier for API use.
    pub slug: InviteSlug,
    /// Organization that created the invitation.
    pub organization: OrganizationId,
    /// SHA-256 hash of the raw invitation token.
    pub token_hash: [u8; 32],
    /// User who created the invitation (must be org admin).
    pub inviter: UserId,
    /// HMAC hex of the normalized invitee email — also stored in GLOBAL indexes.
    pub invitee_email_hmac: String,
    /// Plaintext invitee email — REGIONAL only, never in GLOBAL state.
    pub invitee_email: String,
    /// Role granted upon acceptance.
    pub role: OrganizationMemberRole,
    /// Auto-join team on acceptance.
    pub team: Option<TeamId>,
    /// Current lifecycle state.
    pub status: InvitationStatus,
    /// When the invitation was created.
    pub created_at: DateTime<Utc>,
    /// When the invitation expires (computed as `proposed_at + ttl_hours`).
    pub expires_at: DateTime<Utc>,
    /// Timestamp when the invitation transitioned to a terminal state.
    pub resolved_at: Option<DateTime<Utc>>,
}

/// Lightweight index entry stored in GLOBAL `_idx:invite:slug:` and
/// `_idx:invite:token_hash:` keys.
///
/// Region is **not** cached — looked up live from `OrganizationRegistry`
/// at request time to avoid stale routing after org migration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct InviteIndexEntry {
    /// Organization that owns the invitation.
    pub organization: OrganizationId,
    /// Internal invitation identifier for REGIONAL record lookup.
    pub invite: InviteId,
}

/// Lightweight index entry stored in GLOBAL
/// `_idx:invite:email_hash:{hmac}:{invite_id}` keys.
///
/// Deliberately minimal — only `org_id` and `status`. No timestamps cached,
/// eliminating dual-write overhead on every state transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct InviteEmailEntry {
    /// Organization that created the invitation.
    pub organization: OrganizationId,
    /// Current lifecycle state of the invitation.
    pub status: InvitationStatus,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    #[test]
    fn invitation_status_display() {
        assert_eq!(InvitationStatus::Pending.to_string(), "pending");
        assert_eq!(InvitationStatus::Accepted.to_string(), "accepted");
        assert_eq!(InvitationStatus::Declined.to_string(), "declined");
        assert_eq!(InvitationStatus::Expired.to_string(), "expired");
        assert_eq!(InvitationStatus::Revoked.to_string(), "revoked");
    }

    #[test]
    fn invitation_status_terminal() {
        assert!(!InvitationStatus::Pending.is_terminal());
        assert!(InvitationStatus::Accepted.is_terminal());
        assert!(InvitationStatus::Declined.is_terminal());
        assert!(InvitationStatus::Expired.is_terminal());
        assert!(InvitationStatus::Revoked.is_terminal());
    }

    #[test]
    fn invitation_status_default_is_pending() {
        assert_eq!(InvitationStatus::default(), InvitationStatus::Pending);
    }

    #[test]
    fn invitation_status_serde_roundtrip() {
        for status in [
            InvitationStatus::Pending,
            InvitationStatus::Accepted,
            InvitationStatus::Declined,
            InvitationStatus::Expired,
            InvitationStatus::Revoked,
        ] {
            let json = serde_json::to_string(&status).unwrap();
            let deserialized: InvitationStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(deserialized, status);
        }
    }

    #[test]
    fn invitation_status_serde_snake_case() {
        assert_eq!(serde_json::to_string(&InvitationStatus::Pending).unwrap(), "\"pending\"");
        assert_eq!(serde_json::to_string(&InvitationStatus::Accepted).unwrap(), "\"accepted\"");
    }

    #[test]
    fn organization_invitation_serde_roundtrip() {
        let invitation = OrganizationInvitation {
            id: InviteId::new(1),
            slug: InviteSlug::new(1234567890),
            organization: OrganizationId::new(42),
            token_hash: [0xab; 32],
            inviter: UserId::new(7),
            invitee_email_hmac: "deadbeef".into(),
            invitee_email: "alice@example.com".into(),
            role: OrganizationMemberRole::Member,
            team: Some(TeamId::new(3)),
            status: InvitationStatus::Pending,
            created_at: Utc::now(),
            expires_at: Utc::now(),
            resolved_at: None,
        };
        let bytes = postcard::to_allocvec(&invitation).unwrap();
        let deserialized: OrganizationInvitation = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(deserialized, invitation);
    }

    #[test]
    fn invite_index_entry_serde_roundtrip() {
        let entry =
            InviteIndexEntry { organization: OrganizationId::new(42), invite: InviteId::new(7) };
        let bytes = postcard::to_allocvec(&entry).unwrap();
        let deserialized: InviteIndexEntry = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(deserialized, entry);
    }

    #[test]
    fn effective_status_pending_not_expired() {
        let now = Utc::now();
        let expires_at = now + chrono::Duration::hours(1);
        assert_eq!(
            effective_invitation_status(InvitationStatus::Pending, expires_at, now),
            InvitationStatus::Pending,
        );
    }

    #[test]
    fn effective_status_pending_expired() {
        let now = Utc::now();
        let expires_at = now - chrono::Duration::seconds(1);
        assert_eq!(
            effective_invitation_status(InvitationStatus::Pending, expires_at, now),
            InvitationStatus::Expired,
        );
    }

    #[test]
    fn effective_status_terminal_unchanged() {
        let now = Utc::now();
        let expires_at = now - chrono::Duration::hours(1);
        // Terminal statuses remain unchanged even if past expires_at
        for status in [
            InvitationStatus::Accepted,
            InvitationStatus::Declined,
            InvitationStatus::Expired,
            InvitationStatus::Revoked,
        ] {
            assert_eq!(effective_invitation_status(status, expires_at, now), status);
        }
    }

    #[test]
    fn invite_email_entry_serde_roundtrip() {
        let entry = InviteEmailEntry {
            organization: OrganizationId::new(42),
            status: InvitationStatus::Pending,
        };
        let bytes = postcard::to_allocvec(&entry).unwrap();
        let deserialized: InviteEmailEntry = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(deserialized, entry);
    }
}
