//! Organization invitation operations.

use inferadb_ledger_types::{InviteSlug, OrganizationSlug, TeamSlug, UserSlug};

use crate::{
    LedgerClient,
    error::Result,
    types::invitation::{
        InvitationCreated, InvitationInfo, InvitationPage, InvitationStatus,
        ReceivedInvitationInfo, ReceivedInvitationPage,
    },
};

impl LedgerClient {
    // ========================================================================
    // Admin operations
    // ========================================================================

    /// Creates an organization invitation.
    ///
    /// Sends an invitation to the specified email address with a designated role
    /// and optional team assignment. Returns a one-time raw token for embedding
    /// in the invitation email URL.
    pub async fn create_organization_invite(
        &self,
        organization: OrganizationSlug,
        caller: UserSlug,
        email: impl Into<String>,
        role: crate::types::admin::OrganizationMemberRole,
        ttl_hours: u32,
        team: Option<TeamSlug>,
    ) -> Result<InvitationCreated> {
        let email = email.into();
        // Generate the invite slug once, outside the retry loop, so every
        // retry hits the server's apply-side idempotency path instead of
        // creating duplicate invitations on response-lost-in-flight.
        let invite_slug = inferadb_ledger_types::snowflake::generate().map_err(|e| {
            crate::error::SdkError::Config { message: format!("generate invite slug: {e}") }
        })?;
        let pool = self.pool.clone();
        self.call_with_retry("create_organization_invite", || {
            let pool = pool.clone();
            let email = email.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::invitation::create_organization_invite(
                    wire_client,
                    request_id,
                    organization,
                    caller,
                    email,
                    role,
                    ttl_hours,
                    team,
                    invite_slug,
                )
                .await
            }
        })
        .await
    }

    /// Lists invitations for an organization (admin view).
    ///
    /// Returns paginated invitations with invitee emails visible.
    pub async fn list_organization_invites(
        &self,
        organization: OrganizationSlug,
        caller: UserSlug,
        status_filter: Option<InvitationStatus>,
        page_token: Option<Vec<u8>>,
        page_size: u32,
    ) -> Result<InvitationPage> {
        let pool = self.pool.clone();
        self.call_with_retry("list_organization_invites", || {
            let pool = pool.clone();
            let page_token = page_token.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::invitation::list_organization_invites(
                    wire_client,
                    request_id,
                    organization,
                    caller,
                    status_filter,
                    page_token,
                    page_size,
                )
                .await
            }
        })
        .await
    }

    /// Gets a single invitation by slug (admin view).
    pub async fn get_organization_invite(
        &self,
        slug: InviteSlug,
        caller: UserSlug,
    ) -> Result<InvitationInfo> {
        let pool = self.pool.clone();
        self.call_with_retry("get_organization_invite", || {
            let pool = pool.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::invitation::get_organization_invite(
                    wire_client,
                    request_id,
                    slug,
                    caller,
                )
                .await
            }
        })
        .await
    }

    /// Revokes a pending invitation (admin operation).
    pub async fn revoke_organization_invite(
        &self,
        slug: InviteSlug,
        caller: UserSlug,
    ) -> Result<InvitationInfo> {
        let pool = self.pool.clone();
        self.call_with_retry("revoke_organization_invite", || {
            let pool = pool.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::invitation::revoke_organization_invite(
                    wire_client,
                    request_id,
                    slug,
                    caller,
                )
                .await
            }
        })
        .await
    }

    // ========================================================================
    // User operations
    // ========================================================================

    /// Lists invitations received by the authenticated user.
    ///
    /// Returns paginated invitations with organization names visible.
    pub async fn list_received_invitations(
        &self,
        user: UserSlug,
        status_filter: Option<InvitationStatus>,
        page_token: Option<Vec<u8>>,
        page_size: u32,
    ) -> Result<ReceivedInvitationPage> {
        let pool = self.pool.clone();
        self.call_with_retry("list_received_invitations", || {
            let pool = pool.clone();
            let page_token = page_token.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::invitation::list_received_invitations(
                    wire_client,
                    request_id,
                    user,
                    status_filter,
                    page_token,
                    page_size,
                )
                .await
            }
        })
        .await
    }

    /// Gets details of a specific invitation for the authenticated user.
    pub async fn get_invitation_details(
        &self,
        slug: InviteSlug,
        user: UserSlug,
    ) -> Result<ReceivedInvitationInfo> {
        let pool = self.pool.clone();
        self.call_with_retry("get_invitation_details", || {
            let pool = pool.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::invitation::get_invitation_details(
                    wire_client,
                    request_id,
                    slug,
                    user,
                )
                .await
            }
        })
        .await
    }

    /// Accepts a pending invitation.
    ///
    /// On success, the user is added as an organization member with the
    /// designated role and optional team membership. Returns user-view data
    /// (organization name, no invitee email).
    pub async fn accept_invitation(
        &self,
        slug: InviteSlug,
        caller: UserSlug,
    ) -> Result<ReceivedInvitationInfo> {
        let pool = self.pool.clone();
        self.call_with_retry("accept_invitation", || {
            let pool = pool.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::invitation::accept_invitation(
                    wire_client,
                    request_id,
                    slug,
                    caller,
                )
                .await
            }
        })
        .await
    }

    /// Declines a pending invitation.
    pub async fn decline_invitation(
        &self,
        slug: InviteSlug,
        caller: UserSlug,
    ) -> Result<ReceivedInvitationInfo> {
        let pool = self.pool.clone();
        self.call_with_retry("decline_invitation", || {
            let pool = pool.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::invitation::decline_invitation(
                    wire_client,
                    request_id,
                    slug,
                    caller,
                )
                .await
            }
        })
        .await
    }
}
