//! Invitation domain types for the SDK public API.

use inferadb_ledger_types::{InviteSlug, OrganizationSlug, TeamSlug, UserSlug};

use crate::types::admin::OrganizationMemberRole;

/// Status of an organization invitation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum InvitationStatus {
    /// Invitation is awaiting a response.
    Pending,
    /// Invitation was accepted.
    Accepted,
    /// Invitation was declined by the invitee.
    Declined,
    /// Invitation expired without action.
    Expired,
    /// Invitation was revoked by an org admin.
    Revoked,
}

impl InvitationStatus {
    /// Returns the status as a static string slice.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Accepted => "accepted",
            Self::Declined => "declined",
            Self::Expired => "expired",
            Self::Revoked => "revoked",
        }
    }
}

impl std::fmt::Display for InvitationStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Response from creating an organization invitation.
///
/// The `token` field is the raw invitation token returned only at creation time.
/// It is redacted from [`Debug`] output to prevent accidental logging.
#[derive(Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct InvitationCreated {
    /// External invitation slug.
    pub slug: InviteSlug,
    /// Current status (always `Pending` on creation).
    pub status: InvitationStatus,
    /// When the invitation was created.
    pub created_at: Option<std::time::SystemTime>,
    /// When the invitation expires.
    pub expires_at: Option<std::time::SystemTime>,
    /// Raw invitation token for embedding in email URLs.
    pub token: String,
}

impl std::fmt::Debug for InvitationCreated {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InvitationCreated")
            .field("slug", &self.slug)
            .field("status", &self.status)
            .field("created_at", &self.created_at)
            .field("expires_at", &self.expires_at)
            .field("token", &"<redacted>")
            .finish()
    }
}

/// Admin-facing invitation information.
///
/// Contains the invitee email (visible to org admins) but no organization name.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct InvitationInfo {
    /// External invitation slug.
    pub slug: InviteSlug,
    /// Organization that sent the invitation.
    pub organization: OrganizationSlug,
    /// User who created the invitation.
    pub inviter: UserSlug,
    /// Invitee email address (admin-visible only).
    pub invitee_email: String,
    /// Role assigned upon acceptance.
    pub role: OrganizationMemberRole,
    /// Team to auto-join on acceptance.
    pub team: Option<TeamSlug>,
    /// Current invitation status.
    pub status: InvitationStatus,
    /// When the invitation was created.
    pub created_at: Option<std::time::SystemTime>,
    /// When the invitation expires.
    pub expires_at: Option<std::time::SystemTime>,
    /// When the invitation was resolved (accepted/declined/revoked/expired).
    pub resolved_at: Option<std::time::SystemTime>,
}

/// User-facing invitation information.
///
/// Contains the organization name (visible to invitees) but no invitee email.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ReceivedInvitationInfo {
    /// External invitation slug.
    pub slug: InviteSlug,
    /// Organization that sent the invitation.
    pub organization: OrganizationSlug,
    /// Organization display name.
    pub organization_name: String,
    /// Role assigned upon acceptance.
    pub role: OrganizationMemberRole,
    /// Team to auto-join on acceptance.
    pub team: Option<TeamSlug>,
    /// Current invitation status.
    pub status: InvitationStatus,
    /// When the invitation was created.
    pub created_at: Option<std::time::SystemTime>,
    /// When the invitation expires.
    pub expires_at: Option<std::time::SystemTime>,
}

/// Paginated list of admin-facing invitations.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct InvitationPage {
    /// Invitation records.
    pub invitations: Vec<InvitationInfo>,
    /// Token for fetching the next page, if more results exist.
    pub next_page_token: Option<Vec<u8>>,
}

/// Paginated list of user-facing received invitations.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ReceivedInvitationPage {
    /// Invitation records.
    pub invitations: Vec<ReceivedInvitationInfo>,
    /// Token for fetching the next page, if more results exist.
    pub next_page_token: Option<Vec<u8>>,
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn invitation_status_as_str() {
        assert_eq!(InvitationStatus::Pending.as_str(), "pending");
        assert_eq!(InvitationStatus::Accepted.as_str(), "accepted");
        assert_eq!(InvitationStatus::Declined.as_str(), "declined");
        assert_eq!(InvitationStatus::Expired.as_str(), "expired");
        assert_eq!(InvitationStatus::Revoked.as_str(), "revoked");
    }

    #[test]
    fn invitation_created_debug_redacts_token() {
        let created = InvitationCreated {
            slug: InviteSlug::new(1),
            status: InvitationStatus::Pending,
            created_at: None,
            expires_at: None,
            token: "super_secret_token_value".to_string(),
        };
        let debug_output = format!("{created:?}");
        assert!(debug_output.contains("<redacted>"));
        assert!(!debug_output.contains("super_secret_token_value"));
    }

    #[test]
    fn invitation_page_derives() {
        let page = InvitationPage { invitations: vec![], next_page_token: None };
        let page2 = page.clone();
        assert_eq!(page, page2);
        let _ = format!("{page:?}");
    }

    #[test]
    fn received_invitation_page_derives() {
        let page = ReceivedInvitationPage { invitations: vec![], next_page_token: None };
        let page2 = page.clone();
        assert_eq!(page, page2);
        let _ = format!("{page:?}");
    }
}
