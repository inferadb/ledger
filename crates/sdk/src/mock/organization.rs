use std::sync::{Arc, atomic::Ordering};

use inferadb_ledger_proto::proto;
use inferadb_ledger_types::OrganizationSlug;
use tonic::{Request, Response, Status};

use super::{MockMember, MockState, OrganizationData, TeamData};

pub(super) struct MockOrganizationService {
    pub(super) state: Arc<MockState>,
}

impl MockOrganizationService {
    pub(super) fn new(state: Arc<MockState>) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl inferadb_ledger_proto::proto::organization_service_server::OrganizationService
    for MockOrganizationService
{
    async fn create_organization(
        &self,
        request: Request<proto::CreateOrganizationRequest>,
    ) -> Result<Response<proto::CreateOrganizationResponse>, Status> {
        self.state.check_injection().await?;

        let req = request.into_inner();
        let organization =
            OrganizationSlug::new(self.state.next_organization.fetch_add(1, Ordering::SeqCst));

        let region = crate::proto_util::region_from_proto_i32(req.region)
            .unwrap_or(inferadb_ledger_types::Region::GLOBAL);
        let admin_slug = req.caller.map_or(0, |u| u.slug);
        let members = vec![MockMember {
            slug: admin_slug,
            role: proto::OrganizationMemberRole::Admin as i32,
        }];
        let proto_members: Vec<_> = members.iter().map(MockMember::to_proto).collect();

        {
            let mut organizations = self.state.organizations.write();
            organizations.insert(
                organization,
                OrganizationData {
                    name: req.name.clone(),
                    region,
                    status: proto::OrganizationStatus::Active as i32,
                    members,
                    deleted_at: None,
                },
            );
        }

        Ok(Response::new(proto::CreateOrganizationResponse {
            slug: Some(proto::OrganizationSlug { slug: organization.value() }),
            name: req.name,
            region: crate::proto_util::region_to_proto_i32(region),
            member_nodes: vec![],
            status: proto::OrganizationStatus::Active as i32,
            config_version: 1,
            created_at: None,
            tier: req.tier.unwrap_or(0),
            members: proto_members,
            updated_at: None,
        }))
    }

    async fn delete_organization(
        &self,
        request: Request<proto::DeleteOrganizationRequest>,
    ) -> Result<Response<proto::DeleteOrganizationResponse>, Status> {
        self.state.check_injection().await?;

        let req = request.into_inner();
        let slug = req
            .slug
            .as_ref()
            .map(|s| OrganizationSlug::new(s.slug))
            .ok_or_else(|| Status::invalid_argument("Missing organization slug"))?;

        let initiator_slug = req
            .caller
            .as_ref()
            .map(|u| u.slug)
            .ok_or_else(|| Status::invalid_argument("Missing initiator"))?;

        let mut organizations = self.state.organizations.write();
        let data = organizations
            .get_mut(&slug)
            .ok_or_else(|| Status::not_found("Organization not found"))?;

        if !data.members.is_empty() && !data.is_admin(initiator_slug) {
            return Err(Status::permission_denied("Initiator is not an admin"));
        }

        data.status = proto::OrganizationStatus::Deleted as i32;
        data.deleted_at = Some(std::time::SystemTime::now());

        let now = chrono::Utc::now();
        Ok(Response::new(proto::DeleteOrganizationResponse {
            deleted_at: Some(prost_types::Timestamp {
                seconds: now.timestamp(),
                nanos: now.timestamp_subsec_nanos() as i32,
            }),
            retention_days: 90,
        }))
    }

    async fn get_organization(
        &self,
        request: Request<proto::GetOrganizationRequest>,
    ) -> Result<Response<proto::GetOrganizationResponse>, Status> {
        self.state.check_injection().await?;

        let req = request.into_inner();
        let organization = req
            .slug
            .as_ref()
            .map(|s| OrganizationSlug::new(s.slug))
            .ok_or_else(|| Status::invalid_argument("Missing organization slug"))?;

        let organizations = self.state.organizations.read();
        let data = organizations
            .get(&organization)
            .ok_or_else(|| Status::not_found("Organization not found"))?;

        if data.deleted_at.is_some() {
            return Err(Status::not_found("Organization not found"));
        }

        Ok(Response::new(proto::GetOrganizationResponse {
            slug: Some(proto::OrganizationSlug { slug: organization.value() }),
            name: data.name.clone(),
            region: crate::proto_util::region_to_proto_i32(data.region),
            member_nodes: vec![],
            status: data.status,
            config_version: 1,
            created_at: None,
            tier: 0, // Free (default)
            members: data.members.iter().map(MockMember::to_proto).collect(),
            updated_at: None,
        }))
    }

    async fn list_organizations(
        &self,
        _request: Request<proto::ListOrganizationsRequest>,
    ) -> Result<Response<proto::ListOrganizationsResponse>, Status> {
        self.state.check_injection().await?;

        let organizations = self.state.organizations.read();
        let responses: Vec<proto::GetOrganizationResponse> = organizations
            .iter()
            .filter(|(_, data)| data.deleted_at.is_none())
            .map(|(slug, data)| proto::GetOrganizationResponse {
                slug: Some(proto::OrganizationSlug { slug: slug.value() }),
                name: data.name.clone(),
                region: crate::proto_util::region_to_proto_i32(data.region),
                member_nodes: vec![],
                status: data.status,
                config_version: 1,
                created_at: None,
                tier: 0, // Free (default)
                members: data
                    .members
                    .iter()
                    .map(|m| proto::OrganizationMember {
                        user: Some(proto::UserSlug { slug: m.slug }),
                        role: m.role,
                        joined_at: None,
                    })
                    .collect(),
                updated_at: None,
            })
            .collect();

        Ok(Response::new(proto::ListOrganizationsResponse {
            organizations: responses,
            next_page_token: None,
        }))
    }

    async fn migrate_organization(
        &self,
        request: Request<proto::MigrateOrganizationRequest>,
    ) -> Result<Response<proto::MigrateOrganizationResponse>, Status> {
        self.state.check_injection().await?;

        let req = request.into_inner();
        let slug_val = req.slug.map_or(0, |s| s.slug);
        let slug = OrganizationSlug::new(slug_val);

        let initiator_slug = req
            .caller
            .as_ref()
            .map(|u| u.slug)
            .ok_or_else(|| Status::invalid_argument("Missing initiator"))?;

        // Look up existing org to get source region and validate initiator
        let source_region_i32 = {
            let organizations = self.state.organizations.read();
            let data = organizations
                .get(&slug)
                .ok_or_else(|| Status::not_found("Organization not found"))?;

            if !data.members.is_empty() && !data.is_admin(initiator_slug) {
                return Err(Status::permission_denied("Initiator is not an admin"));
            }

            crate::proto_util::region_to_proto_i32(data.region)
        };

        // Update status to Migrating
        {
            let mut organizations = self.state.organizations.write();
            if let Some(org) = organizations.get_mut(&slug) {
                org.status = proto::OrganizationStatus::Migrating as i32;
            }
        }

        Ok(Response::new(proto::MigrateOrganizationResponse {
            slug: Some(proto::OrganizationSlug { slug: slug_val }),
            source_region: source_region_i32,
            target_region: req.target_region,
            status: proto::OrganizationStatus::Migrating as i32,
        }))
    }

    async fn update_organization(
        &self,
        request: Request<proto::UpdateOrganizationRequest>,
    ) -> Result<Response<proto::UpdateOrganizationResponse>, Status> {
        self.state.check_injection().await?;

        let req = request.into_inner();
        let slug_val = req.slug.map_or(0, |s| s.slug);
        let slug = OrganizationSlug::new(slug_val);

        let initiator_slug = req
            .caller
            .as_ref()
            .map(|u| u.slug)
            .ok_or_else(|| Status::invalid_argument("Missing initiator"))?;

        let mut organizations = self.state.organizations.write();
        let data = organizations
            .get_mut(&slug)
            .ok_or_else(|| Status::not_found("Organization not found"))?;

        if !data.members.is_empty() && !data.is_admin(initiator_slug) {
            return Err(Status::permission_denied("Initiator is not an admin"));
        }

        if let Some(name) = req.name {
            data.name = name;
        }

        let now = chrono::Utc::now();
        Ok(Response::new(proto::UpdateOrganizationResponse {
            slug: Some(proto::OrganizationSlug { slug: slug_val }),
            name: data.name.clone(),
            region: crate::proto_util::region_to_proto_i32(data.region),
            member_nodes: vec![],
            status: data.status,
            config_version: 1,
            created_at: None,
            tier: 0,
            members: data.members.iter().map(MockMember::to_proto).collect(),
            updated_at: Some(prost_types::Timestamp {
                seconds: now.timestamp(),
                nanos: now.timestamp_subsec_nanos() as i32,
            }),
        }))
    }

    async fn list_organization_members(
        &self,
        request: Request<proto::ListOrganizationMembersRequest>,
    ) -> Result<Response<proto::ListOrganizationMembersResponse>, Status> {
        self.state.check_injection().await?;
        let req = request.into_inner();
        let slug = OrganizationSlug::new(req.slug.map_or(0, |n| n.slug));

        let organizations = self.state.organizations.read();
        let data =
            organizations.get(&slug).ok_or_else(|| Status::not_found("Organization not found"))?;

        let members = data
            .members
            .iter()
            .map(|m| proto::OrganizationMember {
                user: Some(proto::UserSlug { slug: m.slug }),
                role: m.role,
                joined_at: None,
            })
            .collect();

        Ok(Response::new(proto::ListOrganizationMembersResponse { members, next_page_token: None }))
    }

    async fn remove_organization_member(
        &self,
        request: Request<proto::RemoveOrganizationMemberRequest>,
    ) -> Result<Response<proto::RemoveOrganizationMemberResponse>, Status> {
        self.state.check_injection().await?;
        let req = request.into_inner();
        let slug = OrganizationSlug::new(req.slug.map_or(0, |n| n.slug));
        let initiator_slug = req.caller.map_or(0, |u| u.slug);
        let target_slug = req.target.map_or(0, |u| u.slug);

        let mut organizations = self.state.organizations.write();
        let data = organizations
            .get_mut(&slug)
            .ok_or_else(|| Status::not_found("Organization not found"))?;

        if initiator_slug != target_slug && !data.is_admin(initiator_slug) {
            return Err(Status::permission_denied("Initiator is not an admin"));
        }

        if let Some(pos) = data.members.iter().position(|m| m.slug == target_slug) {
            data.members.remove(pos);
        } else {
            return Err(Status::not_found("Target is not a member"));
        }

        Ok(Response::new(proto::RemoveOrganizationMemberResponse {}))
    }

    async fn update_organization_member_role(
        &self,
        request: Request<proto::UpdateOrganizationMemberRoleRequest>,
    ) -> Result<Response<proto::UpdateOrganizationMemberRoleResponse>, Status> {
        self.state.check_injection().await?;
        let req = request.into_inner();
        let slug = OrganizationSlug::new(req.slug.map_or(0, |n| n.slug));
        let initiator_slug = req.caller.map_or(0, |u| u.slug);
        let target_slug = req.target.map_or(0, |u| u.slug);

        let mut organizations = self.state.organizations.write();
        let data = organizations
            .get_mut(&slug)
            .ok_or_else(|| Status::not_found("Organization not found"))?;

        if !data.is_admin(initiator_slug) {
            return Err(Status::permission_denied("Initiator is not an admin"));
        }

        if let Some(member) = data.members.iter_mut().find(|m| m.slug == target_slug) {
            member.role = req.role;
            Ok(Response::new(proto::UpdateOrganizationMemberRoleResponse {
                member: Some(member.to_proto()),
            }))
        } else {
            Err(Status::not_found("Target is not a member"))
        }
    }

    async fn list_organization_teams(
        &self,
        request: Request<proto::ListOrganizationTeamsRequest>,
    ) -> Result<Response<proto::ListOrganizationTeamsResponse>, Status> {
        self.state.check_injection().await?;
        let req = request.into_inner();
        let org_slug = req.organization.map_or(0, |n| n.slug);

        let teams = self.state.teams.read();
        let org_teams: Vec<proto::OrganizationTeam> = teams
            .iter()
            .filter(|(_, data)| data.org_slug == org_slug)
            .map(|(&slug, data)| proto::OrganizationTeam {
                slug: Some(proto::TeamSlug { slug }),
                organization: Some(proto::OrganizationSlug { slug: data.org_slug }),
                name: data.name.clone(),
                members: vec![],
                created_at: None,
                updated_at: None,
            })
            .collect();

        Ok(Response::new(proto::ListOrganizationTeamsResponse {
            teams: org_teams,
            next_page_token: None,
        }))
    }

    async fn create_organization_team(
        &self,
        request: Request<proto::CreateOrganizationTeamRequest>,
    ) -> Result<Response<proto::CreateOrganizationTeamResponse>, Status> {
        self.state.check_injection().await?;
        let req = request.into_inner();
        let org_slug = req.organization.map_or(0, |n| n.slug);

        // Validate organization exists
        {
            let organizations = self.state.organizations.read();
            if !organizations.contains_key(&OrganizationSlug::new(org_slug)) {
                return Err(Status::not_found("Organization not found"));
            }
        }

        // Check name uniqueness within org
        {
            let teams = self.state.teams.read();
            let duplicate = teams.values().any(|t| t.org_slug == org_slug && t.name == req.name);
            if duplicate {
                return Err(Status::already_exists(format!(
                    "Team name '{}' already exists",
                    req.name
                )));
            }
        }

        let team_slug = self.state.next_team.fetch_add(1, Ordering::SeqCst);
        {
            let mut teams = self.state.teams.write();
            teams.insert(team_slug, TeamData { name: req.name.clone(), org_slug });
        }

        Ok(Response::new(proto::CreateOrganizationTeamResponse {
            team: Some(proto::OrganizationTeam {
                slug: Some(proto::TeamSlug { slug: team_slug }),
                organization: Some(proto::OrganizationSlug { slug: org_slug }),
                name: req.name,
                members: vec![],
                created_at: None,
                updated_at: None,
            }),
        }))
    }

    async fn delete_organization_team(
        &self,
        request: Request<proto::DeleteOrganizationTeamRequest>,
    ) -> Result<Response<proto::DeleteOrganizationTeamResponse>, Status> {
        self.state.check_injection().await?;
        let req = request.into_inner();
        let team_slug = req.slug.map_or(0, |n| n.slug);

        // Validate team exists and its org exists
        let org_slug = {
            let teams = self.state.teams.read();
            let team_data =
                teams.get(&team_slug).ok_or_else(|| Status::not_found("Team not found"))?;
            team_data.org_slug
        };

        {
            let organizations = self.state.organizations.read();
            if !organizations.contains_key(&OrganizationSlug::new(org_slug)) {
                return Err(Status::not_found("Organization not found"));
            }
        }

        let mut teams = self.state.teams.write();
        teams.remove(&team_slug);

        Ok(Response::new(proto::DeleteOrganizationTeamResponse {}))
    }

    async fn update_organization_team(
        &self,
        request: Request<proto::UpdateOrganizationTeamRequest>,
    ) -> Result<Response<proto::UpdateOrganizationTeamResponse>, Status> {
        self.state.check_injection().await?;
        let req = request.into_inner();
        let team_slug = req.slug.map_or(0, |n| n.slug);

        // Validate team exists and its org exists
        {
            let teams = self.state.teams.read();
            let team_data =
                teams.get(&team_slug).ok_or_else(|| Status::not_found("Team not found"))?;
            let org_slug = team_data.org_slug;
            drop(teams);

            let organizations = self.state.organizations.read();
            if !organizations.contains_key(&OrganizationSlug::new(org_slug)) {
                return Err(Status::not_found("Organization not found"));
            }
        }

        let mut teams = self.state.teams.write();

        if let Some(ref name) = req.name {
            let org_slug = teams[&team_slug].org_slug;
            let duplicate = teams
                .iter()
                .any(|(&s, t)| s != team_slug && t.org_slug == org_slug && t.name == *name);
            if duplicate {
                return Err(Status::already_exists(format!("Team name '{}' already exists", name)));
            }
            if let Some(data) = teams.get_mut(&team_slug) {
                data.name = name.clone();
            }
        }

        let team_data = &teams[&team_slug];
        let response_team = proto::OrganizationTeam {
            slug: Some(proto::TeamSlug { slug: team_slug }),
            organization: Some(proto::OrganizationSlug { slug: team_data.org_slug }),
            name: team_data.name.clone(),
            members: vec![],
            created_at: None,
            updated_at: None,
        };

        Ok(Response::new(proto::UpdateOrganizationTeamResponse { team: Some(response_team) }))
    }

    async fn get_organization_team(
        &self,
        request: Request<proto::GetOrganizationTeamRequest>,
    ) -> Result<Response<proto::GetOrganizationTeamResponse>, Status> {
        self.state.check_injection().await?;
        let req = request.into_inner();
        let team_slug = req.slug.map_or(0, |n| n.slug);

        let teams = self.state.teams.read();
        let team_data = teams.get(&team_slug).ok_or_else(|| Status::not_found("Team not found"))?;

        let response_team = proto::OrganizationTeam {
            slug: Some(proto::TeamSlug { slug: team_slug }),
            organization: Some(proto::OrganizationSlug { slug: team_data.org_slug }),
            name: team_data.name.clone(),
            members: vec![],
            created_at: None,
            updated_at: None,
        };

        Ok(Response::new(proto::GetOrganizationTeamResponse { team: Some(response_team) }))
    }

    async fn add_team_member(
        &self,
        request: Request<proto::AddTeamMemberRequest>,
    ) -> Result<Response<proto::AddTeamMemberResponse>, Status> {
        self.state.check_injection().await?;
        let req = request.into_inner();

        let team = proto::OrganizationTeam {
            slug: req.team,
            organization: None,
            name: String::new(),
            members: vec![],
            created_at: None,
            updated_at: None,
        };
        Ok(Response::new(proto::AddTeamMemberResponse { team: Some(team) }))
    }

    async fn remove_team_member(
        &self,
        _request: Request<proto::RemoveTeamMemberRequest>,
    ) -> Result<Response<proto::RemoveTeamMemberResponse>, Status> {
        self.state.check_injection().await?;
        Ok(Response::new(proto::RemoveTeamMemberResponse {}))
    }

    async fn update_team_member_role(
        &self,
        request: Request<proto::UpdateTeamMemberRoleRequest>,
    ) -> Result<Response<proto::UpdateTeamMemberRoleResponse>, Status> {
        self.state.check_injection().await?;
        let req = request.into_inner();

        let team = proto::OrganizationTeam {
            slug: req.team,
            organization: None,
            name: String::new(),
            members: vec![],
            created_at: None,
            updated_at: None,
        };
        Ok(Response::new(proto::UpdateTeamMemberRoleResponse { team: Some(team) }))
    }
}
