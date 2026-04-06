//! Mock implementation of the invitation gRPC service.

use std::sync::Arc;

use inferadb_ledger_proto::proto;
use tonic::{Request, Response, Status};

use super::MockState;

pub(super) struct MockInvitationService {
    pub(super) state: Arc<MockState>,
}

impl MockInvitationService {
    pub(super) fn new(state: Arc<MockState>) -> Self {
        Self { state }
    }

    fn now_timestamp() -> prost_types::Timestamp {
        let now =
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
        prost_types::Timestamp { seconds: now.as_secs() as i64, nanos: now.subsec_nanos() as i32 }
    }

    fn expires_timestamp() -> prost_types::Timestamp {
        let expires =
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default()
                + std::time::Duration::from_secs(72 * 3600);
        prost_types::Timestamp {
            seconds: expires.as_secs() as i64,
            nanos: expires.subsec_nanos() as i32,
        }
    }
}

#[tonic::async_trait]
impl inferadb_ledger_proto::proto::invitation_service_server::InvitationService
    for MockInvitationService
{
    async fn create_organization_invite(
        &self,
        request: Request<proto::CreateOrganizationInviteRequest>,
    ) -> Result<Response<proto::CreateOrganizationInviteResponse>, Status> {
        self.state.check_injection().await?;

        let req = request.into_inner();
        let org_slug = req.organization.map_or(0, |o| o.slug);

        // Validate org exists
        {
            let organizations = self.state.organizations.read();
            if !organizations.contains_key(&inferadb_ledger_types::OrganizationSlug::new(org_slug))
            {
                return Err(Status::not_found("Organization not found"));
            }
        }

        let invite_slug = self.state.next_invite.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        Ok(Response::new(proto::CreateOrganizationInviteResponse {
            slug: Some(proto::InviteSlug { slug: invite_slug }),
            status: proto::InvitationStatus::Pending as i32,
            created_at: Some(Self::now_timestamp()),
            expires_at: Some(Self::expires_timestamp()),
            token: format!("mock_invite_token_{invite_slug}"),
        }))
    }

    async fn list_organization_invites(
        &self,
        request: Request<proto::ListOrganizationInvitesRequest>,
    ) -> Result<Response<proto::ListOrganizationInvitesResponse>, Status> {
        self.state.check_injection().await?;

        let req = request.into_inner();
        let org_slug = req.organization.map_or(0, |o| o.slug);

        let invitations = self.state.invitations.read();
        let filtered: Vec<proto::Invitation> = invitations
            .iter()
            .filter(|inv| inv.organization.as_ref().is_some_and(|o| o.slug == org_slug))
            .filter(|inv| req.status_filter.is_none_or(|filter_status| inv.status == filter_status))
            .cloned()
            .collect();

        Ok(Response::new(proto::ListOrganizationInvitesResponse {
            invitations: filtered,
            next_page_token: None,
        }))
    }

    async fn get_organization_invite(
        &self,
        request: Request<proto::GetOrganizationInviteRequest>,
    ) -> Result<Response<proto::GetOrganizationInviteResponse>, Status> {
        self.state.check_injection().await?;

        let req = request.into_inner();
        let invite_slug = req.slug.map_or(0, |s| s.slug);

        let invitations = self.state.invitations.read();
        let invitation = invitations
            .iter()
            .find(|inv| inv.slug.as_ref().is_some_and(|s| s.slug == invite_slug))
            .cloned()
            .ok_or_else(|| Status::not_found("Invitation not found"))?;

        Ok(Response::new(proto::GetOrganizationInviteResponse { invitation: Some(invitation) }))
    }

    async fn revoke_organization_invite(
        &self,
        request: Request<proto::RevokeOrganizationInviteRequest>,
    ) -> Result<Response<proto::RevokeOrganizationInviteResponse>, Status> {
        self.state.check_injection().await?;

        let req = request.into_inner();
        let invite_slug = req.slug.map_or(0, |s| s.slug);

        let mut invitations = self.state.invitations.write();
        let invitation = invitations
            .iter_mut()
            .find(|inv| inv.slug.as_ref().is_some_and(|s| s.slug == invite_slug))
            .ok_or_else(|| Status::not_found("Invitation not found"))?;

        invitation.status = proto::InvitationStatus::Revoked as i32;
        invitation.resolved_at = Some(Self::now_timestamp());

        Ok(Response::new(proto::RevokeOrganizationInviteResponse {
            invitation: Some(invitation.clone()),
        }))
    }

    async fn list_received_invitations(
        &self,
        request: Request<proto::ListReceivedInvitationsRequest>,
    ) -> Result<Response<proto::ListReceivedInvitationsResponse>, Status> {
        self.state.check_injection().await?;

        let req = request.into_inner();
        let invitations = self.state.invitations.read();
        let filtered: Vec<proto::Invitation> = invitations
            .iter()
            .filter(|inv| req.status_filter.is_none_or(|filter_status| inv.status == filter_status))
            .cloned()
            .collect();

        Ok(Response::new(proto::ListReceivedInvitationsResponse {
            invitations: filtered,
            next_page_token: None,
        }))
    }

    async fn get_invitation_details(
        &self,
        request: Request<proto::GetInvitationDetailsRequest>,
    ) -> Result<Response<proto::GetInvitationDetailsResponse>, Status> {
        self.state.check_injection().await?;

        let req = request.into_inner();
        let invite_slug = req.slug.map_or(0, |s| s.slug);

        let invitations = self.state.invitations.read();
        let invitation = invitations
            .iter()
            .find(|inv| inv.slug.as_ref().is_some_and(|s| s.slug == invite_slug))
            .cloned()
            .ok_or_else(|| Status::not_found("Invitation not found"))?;

        Ok(Response::new(proto::GetInvitationDetailsResponse { invitation: Some(invitation) }))
    }

    async fn accept_invitation(
        &self,
        request: Request<proto::AcceptInvitationRequest>,
    ) -> Result<Response<proto::AcceptInvitationResponse>, Status> {
        self.state.check_injection().await?;

        let req = request.into_inner();
        let invite_slug = req.slug.map_or(0, |s| s.slug);

        let mut invitations = self.state.invitations.write();
        let invitation = invitations
            .iter_mut()
            .find(|inv| inv.slug.as_ref().is_some_and(|s| s.slug == invite_slug))
            .ok_or_else(|| Status::not_found("Invitation not found"))?;

        if invitation.status != proto::InvitationStatus::Pending as i32 {
            return Err(Status::failed_precondition("Invitation is not pending"));
        }

        invitation.status = proto::InvitationStatus::Accepted as i32;
        invitation.resolved_at = Some(Self::now_timestamp());

        Ok(Response::new(proto::AcceptInvitationResponse { invitation: Some(invitation.clone()) }))
    }

    async fn decline_invitation(
        &self,
        request: Request<proto::DeclineInvitationRequest>,
    ) -> Result<Response<proto::DeclineInvitationResponse>, Status> {
        self.state.check_injection().await?;

        let req = request.into_inner();
        let invite_slug = req.slug.map_or(0, |s| s.slug);

        let mut invitations = self.state.invitations.write();
        let invitation = invitations
            .iter_mut()
            .find(|inv| inv.slug.as_ref().is_some_and(|s| s.slug == invite_slug))
            .ok_or_else(|| Status::not_found("Invitation not found"))?;

        if invitation.status != proto::InvitationStatus::Pending as i32 {
            return Err(Status::failed_precondition("Invitation is not pending"));
        }

        invitation.status = proto::InvitationStatus::Declined as i32;
        invitation.resolved_at = Some(Self::now_timestamp());

        Ok(Response::new(proto::DeclineInvitationResponse { invitation: Some(invitation.clone()) }))
    }
}
