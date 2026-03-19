//! Organization invitation operations.

use inferadb_ledger_proto::proto;
use inferadb_ledger_types::{InviteSlug, OrganizationSlug, TeamSlug, UserSlug};

use crate::{
    LedgerClient,
    error::Result,
    proto_util::missing_response_field,
    retry::with_retry_cancellable,
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
        self.check_shutdown(None)?;

        let email = email.into();
        let role_i32 = role.to_proto();
        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "create_organization_invite",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "create_organization_invite",
                || async {
                    let mut client = crate::connected_client!(pool, create_invitation_client);

                    let request = proto::CreateOrganizationInviteRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        caller: Some(proto::UserSlug { slug: caller.value() }),
                        email: email.clone(),
                        role: role_i32,
                        ttl_hours,
                        team: team.map(|t| proto::TeamSlug { slug: t.value() }),
                    };

                    let response = client
                        .create_organization_invite(tonic::Request::new(request))
                        .await?
                        .into_inner();

                    Ok(InvitationCreated::from_proto(&response))
                },
            ),
        )
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
        self.check_shutdown(None)?;

        let status_filter_i32 = status_filter.map(|s| s.to_proto());
        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "list_organization_invites",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "list_organization_invites",
                || async {
                    let mut client = crate::connected_client!(pool, create_invitation_client);

                    let request = proto::ListOrganizationInvitesRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        caller: Some(proto::UserSlug { slug: caller.value() }),
                        status_filter: status_filter_i32,
                        page_token: page_token.clone(),
                        page_size,
                    };

                    let response = client
                        .list_organization_invites(tonic::Request::new(request))
                        .await?
                        .into_inner();

                    Ok(InvitationPage {
                        invitations: response
                            .invitations
                            .iter()
                            .map(InvitationInfo::from_proto)
                            .collect(),
                        next_page_token: response.next_page_token,
                    })
                },
            ),
        )
        .await
    }

    /// Gets a single invitation by slug (admin view).
    pub async fn get_organization_invite(
        &self,
        slug: InviteSlug,
        caller: UserSlug,
    ) -> Result<InvitationInfo> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "get_organization_invite",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "get_organization_invite",
                || async {
                    let mut client = crate::connected_client!(pool, create_invitation_client);

                    let request = proto::GetOrganizationInviteRequest {
                        slug: Some(proto::InviteSlug { slug: slug.value() }),
                        caller: Some(proto::UserSlug { slug: caller.value() }),
                    };

                    let response = client
                        .get_organization_invite(tonic::Request::new(request))
                        .await?
                        .into_inner();

                    response.invitation.as_ref().map(InvitationInfo::from_proto).ok_or_else(|| {
                        missing_response_field("invitation", "GetOrganizationInviteResponse")
                    })
                },
            ),
        )
        .await
    }

    /// Revokes a pending invitation (admin operation).
    pub async fn revoke_organization_invite(
        &self,
        slug: InviteSlug,
        caller: UserSlug,
    ) -> Result<InvitationInfo> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "revoke_organization_invite",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "revoke_organization_invite",
                || async {
                    let mut client = crate::connected_client!(pool, create_invitation_client);

                    let request = proto::RevokeOrganizationInviteRequest {
                        slug: Some(proto::InviteSlug { slug: slug.value() }),
                        caller: Some(proto::UserSlug { slug: caller.value() }),
                    };

                    let response = client
                        .revoke_organization_invite(tonic::Request::new(request))
                        .await?
                        .into_inner();

                    response.invitation.as_ref().map(InvitationInfo::from_proto).ok_or_else(|| {
                        missing_response_field("invitation", "RevokeOrganizationInviteResponse")
                    })
                },
            ),
        )
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
        self.check_shutdown(None)?;

        let status_filter_i32 = status_filter.map(|s| s.to_proto());
        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "list_received_invitations",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "list_received_invitations",
                || async {
                    let mut client = crate::connected_client!(pool, create_invitation_client);

                    let request = proto::ListReceivedInvitationsRequest {
                        user: Some(proto::UserSlug { slug: user.value() }),
                        status_filter: status_filter_i32,
                        page_token: page_token.clone(),
                        page_size,
                    };

                    let response = client
                        .list_received_invitations(tonic::Request::new(request))
                        .await?
                        .into_inner();

                    Ok(ReceivedInvitationPage {
                        invitations: response
                            .invitations
                            .iter()
                            .map(ReceivedInvitationInfo::from_proto)
                            .collect(),
                        next_page_token: response.next_page_token,
                    })
                },
            ),
        )
        .await
    }

    /// Gets details of a specific invitation for the authenticated user.
    pub async fn get_invitation_details(
        &self,
        slug: InviteSlug,
        user: UserSlug,
    ) -> Result<ReceivedInvitationInfo> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "get_invitation_details",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "get_invitation_details",
                || async {
                    let mut client = crate::connected_client!(pool, create_invitation_client);

                    let request = proto::GetInvitationDetailsRequest {
                        slug: Some(proto::InviteSlug { slug: slug.value() }),
                        user: Some(proto::UserSlug { slug: user.value() }),
                    };

                    let response = client
                        .get_invitation_details(tonic::Request::new(request))
                        .await?
                        .into_inner();

                    response.invitation.as_ref().map(ReceivedInvitationInfo::from_proto).ok_or_else(
                        || missing_response_field("invitation", "GetInvitationDetailsResponse"),
                    )
                },
            ),
        )
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
        acceptor: UserSlug,
    ) -> Result<ReceivedInvitationInfo> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "accept_invitation",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "accept_invitation",
                || async {
                    let mut client = crate::connected_client!(pool, create_invitation_client);

                    let request = proto::AcceptInvitationRequest {
                        slug: Some(proto::InviteSlug { slug: slug.value() }),
                        acceptor: Some(proto::UserSlug { slug: acceptor.value() }),
                    };

                    let response =
                        client.accept_invitation(tonic::Request::new(request)).await?.into_inner();

                    response.invitation.as_ref().map(ReceivedInvitationInfo::from_proto).ok_or_else(
                        || missing_response_field("invitation", "AcceptInvitationResponse"),
                    )
                },
            ),
        )
        .await
    }

    /// Declines a pending invitation.
    pub async fn decline_invitation(
        &self,
        slug: InviteSlug,
        user: UserSlug,
    ) -> Result<ReceivedInvitationInfo> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "decline_invitation",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "decline_invitation",
                || async {
                    let mut client = crate::connected_client!(pool, create_invitation_client);

                    let request = proto::DeclineInvitationRequest {
                        slug: Some(proto::InviteSlug { slug: slug.value() }),
                        user: Some(proto::UserSlug { slug: user.value() }),
                    };

                    let response =
                        client.decline_invitation(tonic::Request::new(request)).await?.into_inner();

                    response.invitation.as_ref().map(ReceivedInvitationInfo::from_proto).ok_or_else(
                        || missing_response_field("invitation", "DeclineInvitationResponse"),
                    )
                },
            ),
        )
        .await
    }
}
