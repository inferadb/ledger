//! Invitation domain types for the SDK public API.

use inferadb_ledger_proto::proto;
use inferadb_ledger_types::{InviteSlug, OrganizationSlug, TeamSlug, UserSlug};

use crate::{proto_util::proto_timestamp_to_system_time, types::admin::OrganizationMemberRole};

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

    /// Converts from protobuf enum value.
    pub(crate) fn from_proto(value: i32) -> Self {
        match proto::InvitationStatus::try_from(value) {
            Ok(proto::InvitationStatus::Accepted) => InvitationStatus::Accepted,
            Ok(proto::InvitationStatus::Declined) => InvitationStatus::Declined,
            Ok(proto::InvitationStatus::Expired) => InvitationStatus::Expired,
            Ok(proto::InvitationStatus::Revoked) => InvitationStatus::Revoked,
            _ => InvitationStatus::Pending,
        }
    }

    /// Converts to protobuf `i32` value.
    pub(crate) fn to_proto(self) -> i32 {
        match self {
            InvitationStatus::Pending => proto::InvitationStatus::Pending.into(),
            InvitationStatus::Accepted => proto::InvitationStatus::Accepted.into(),
            InvitationStatus::Declined => proto::InvitationStatus::Declined.into(),
            InvitationStatus::Expired => proto::InvitationStatus::Expired.into(),
            InvitationStatus::Revoked => proto::InvitationStatus::Revoked.into(),
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

impl InvitationCreated {
    /// Creates from protobuf response.
    pub(crate) fn from_proto(r: &proto::CreateOrganizationInviteResponse) -> Self {
        Self {
            slug: InviteSlug::new(r.slug.as_ref().map_or(0, |s| s.slug)),
            status: InvitationStatus::from_proto(r.status),
            created_at: r.created_at.as_ref().and_then(proto_timestamp_to_system_time),
            expires_at: r.expires_at.as_ref().and_then(proto_timestamp_to_system_time),
            token: r.token.clone(),
        }
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

impl InvitationInfo {
    /// Creates from a protobuf `Invitation` message (admin view).
    pub(crate) fn from_proto(inv: &proto::Invitation) -> Self {
        Self {
            slug: InviteSlug::new(inv.slug.as_ref().map_or(0, |s| s.slug)),
            organization: OrganizationSlug::new(inv.organization.as_ref().map_or(0, |o| o.slug)),
            inviter: UserSlug::new(inv.inviter.as_ref().map_or(0, |u| u.slug)),
            invitee_email: inv.invitee_email.clone(),
            role: OrganizationMemberRole::from_proto(inv.role),
            team: inv.team.as_ref().map(|t| TeamSlug::new(t.slug)),
            status: InvitationStatus::from_proto(inv.status),
            created_at: inv.created_at.as_ref().and_then(proto_timestamp_to_system_time),
            expires_at: inv.expires_at.as_ref().and_then(proto_timestamp_to_system_time),
            resolved_at: inv.resolved_at.as_ref().and_then(proto_timestamp_to_system_time),
        }
    }
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

impl ReceivedInvitationInfo {
    /// Creates from a protobuf `Invitation` message (user view).
    pub(crate) fn from_proto(inv: &proto::Invitation) -> Self {
        Self {
            slug: InviteSlug::new(inv.slug.as_ref().map_or(0, |s| s.slug)),
            organization: OrganizationSlug::new(inv.organization.as_ref().map_or(0, |o| o.slug)),
            organization_name: inv.organization_name.clone(),
            role: OrganizationMemberRole::from_proto(inv.role),
            team: inv.team.as_ref().map(|t| TeamSlug::new(t.slug)),
            status: InvitationStatus::from_proto(inv.status),
            created_at: inv.created_at.as_ref().and_then(proto_timestamp_to_system_time),
            expires_at: inv.expires_at.as_ref().and_then(proto_timestamp_to_system_time),
        }
    }
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
    fn invitation_status_roundtrip_all_variants() {
        for (status, expected_i32) in [
            (InvitationStatus::Pending, 1),
            (InvitationStatus::Accepted, 2),
            (InvitationStatus::Declined, 3),
            (InvitationStatus::Expired, 4),
            (InvitationStatus::Revoked, 5),
        ] {
            let proto_val = status.to_proto();
            assert_eq!(proto_val, expected_i32);
            let roundtripped = InvitationStatus::from_proto(proto_val);
            assert_eq!(roundtripped, status);
        }
    }

    #[test]
    fn invitation_status_from_proto_unknown_defaults_to_pending() {
        assert_eq!(InvitationStatus::from_proto(0), InvitationStatus::Pending);
        assert_eq!(InvitationStatus::from_proto(99), InvitationStatus::Pending);
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
    fn invitation_created_from_proto() {
        let proto_resp = proto::CreateOrganizationInviteResponse {
            slug: Some(proto::InviteSlug { slug: 42 }),
            status: proto::InvitationStatus::Pending.into(),
            created_at: None,
            expires_at: None,
            token: "abc123".to_string(),
        };
        let created = InvitationCreated::from_proto(&proto_resp);
        assert_eq!(created.slug, InviteSlug::new(42));
        assert_eq!(created.status, InvitationStatus::Pending);
        assert_eq!(created.token, "abc123");
    }

    #[test]
    fn invitation_info_from_proto() {
        let proto_inv = proto::Invitation {
            slug: Some(proto::InviteSlug { slug: 10 }),
            organization: Some(proto::OrganizationSlug { slug: 20 }),
            inviter: Some(proto::UserSlug { slug: 30 }),
            invitee_email: "test@example.com".to_string(),
            organization_name: String::new(),
            role: proto::OrganizationMemberRole::Member.into(),
            team: Some(proto::TeamSlug { slug: 40 }),
            status: proto::InvitationStatus::Accepted.into(),
            created_at: None,
            expires_at: None,
            resolved_at: None,
        };
        let info = InvitationInfo::from_proto(&proto_inv);
        assert_eq!(info.slug, InviteSlug::new(10));
        assert_eq!(info.organization, OrganizationSlug::new(20));
        assert_eq!(info.inviter, UserSlug::new(30));
        assert_eq!(info.invitee_email, "test@example.com");
        assert_eq!(info.role, OrganizationMemberRole::Member);
        assert_eq!(info.team, Some(TeamSlug::new(40)));
        assert_eq!(info.status, InvitationStatus::Accepted);
    }

    #[test]
    fn received_invitation_info_from_proto() {
        let proto_inv = proto::Invitation {
            slug: Some(proto::InviteSlug { slug: 50 }),
            organization: Some(proto::OrganizationSlug { slug: 60 }),
            inviter: Some(proto::UserSlug { slug: 70 }),
            invitee_email: String::new(),
            organization_name: "Acme Corp".to_string(),
            role: proto::OrganizationMemberRole::Admin.into(),
            team: None,
            status: proto::InvitationStatus::Pending.into(),
            created_at: None,
            expires_at: None,
            resolved_at: None,
        };
        let info = ReceivedInvitationInfo::from_proto(&proto_inv);
        assert_eq!(info.slug, InviteSlug::new(50));
        assert_eq!(info.organization, OrganizationSlug::new(60));
        assert_eq!(info.organization_name, "Acme Corp");
        assert_eq!(info.role, OrganizationMemberRole::Admin);
        assert!(info.team.is_none());
        assert_eq!(info.status, InvitationStatus::Pending);
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
