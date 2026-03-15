//! Organization CRUD, team management, migration, and erasure operations.

use inferadb_ledger_proto::proto;
use inferadb_ledger_types::{OrganizationSlug, Region, TeamSlug, UserSlug};

use crate::{
    LedgerClient,
    error::Result,
    proto_util::{missing_response_field, proto_timestamp_to_system_time, region_from_proto_i32},
    retry::with_retry_cancellable,
    types::admin::{
        MigrationInfo, OrganizationDeleteInfo, OrganizationInfo, OrganizationMemberInfo,
        OrganizationMemberRole, OrganizationStatus, OrganizationTier, TeamInfo, TeamMemberRole,
        UserMigrationInfo,
    },
};

impl LedgerClient {
    // =========================================================================
    // Admin Operations
    // =========================================================================

    /// Creates a new organization with the specified data residency region.
    ///
    /// Every organization must declare a region at creation time. The region
    /// determines where the organization's data is stored and which data
    /// protection regulations apply.
    ///
    /// # Arguments
    ///
    /// * `name` - Human-readable name for the organization (e.g., "acme_corp").
    /// * `region` - Data residency region. Must not be `Region::Global` (the control plane) or
    ///   `Region::Unspecified`.
    ///
    /// # Returns
    ///
    /// Returns [`OrganizationInfo`] containing the generated slug and metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Connection fails after retry attempts
    /// - The organization name is invalid
    /// - The region is `Global` or `Unspecified`
    /// - A protected region has insufficient in-region nodes (< 3)
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationTier, Region, UserSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// let org = client.create_organization("my-org", Region::US_EAST_VA, UserSlug::new(1), OrganizationTier::Free).await?;
    /// println!("Created organization with slug: {}", org.slug);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn create_organization(
        &self,
        name: impl Into<String>,
        region: Region,
        admin: UserSlug,
        tier: OrganizationTier,
    ) -> Result<OrganizationInfo> {
        self.check_shutdown(None)?;

        let name = name.into();
        let proto_region: proto::Region = region.into();
        let region_i32: i32 = proto_region.into();
        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "create_organization",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "create_organization",
                || async {
                    let mut client = crate::connected_client!(pool, create_organization_client);

                    let request = proto::CreateOrganizationRequest {
                        name: name.clone(),
                        region: region_i32,
                        tier: Some(tier.to_proto()),
                        admin: Some(proto::UserSlug { slug: admin.value() }),
                    };

                    let response = client
                        .create_organization(tonic::Request::new(request))
                        .await?
                        .into_inner();

                    Ok(OrganizationInfo::from_fields(
                        response.slug,
                        response.name,
                        response.region,
                        response.member_nodes,
                        response.config_version,
                        response.status,
                        response.tier,
                        &response.members,
                    ))
                },
            ),
        )
        .await
    }

    /// Returns information about an organization by slug.
    ///
    /// # Arguments
    ///
    /// * `slug` - Organization slug (external identifier).
    ///
    /// # Returns
    ///
    /// Returns [`OrganizationInfo`] containing organization metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Connection fails after retry attempts
    /// - The organization does not exist
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, UserSlug, VaultSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let slug = OrganizationSlug::new(1);
    /// let info = client.get_organization(slug, UserSlug::new(42)).await?;
    /// println!("Organization: {} (status: {:?})", info.name, info.status);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_organization(
        &self,
        slug: OrganizationSlug,
        user: UserSlug,
    ) -> Result<OrganizationInfo> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "get_organization",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "get_organization",
                || async {
                    let mut client = crate::connected_client!(pool, create_organization_client);

                    let request = proto::GetOrganizationRequest {
                        slug: Some(proto::OrganizationSlug { slug: slug.value() }),
                        caller: Some(proto::UserSlug { slug: user.value() }),
                    };

                    let response =
                        client.get_organization(tonic::Request::new(request)).await?.into_inner();

                    Ok(OrganizationInfo::from_proto(response))
                },
            ),
        )
        .await
    }

    /// Updates an organization's mutable fields.
    ///
    /// Currently supports renaming an organization. The initiator must be
    /// an organization administrator.
    ///
    /// # Arguments
    ///
    /// * `slug` - Organization slug (external identifier).
    /// * `user` - User slug of the admin performing the update.
    /// * `name` - New name for the organization, or `None` to keep the current name.
    ///
    /// # Returns
    ///
    /// Returns the updated [`OrganizationInfo`].
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Connection fails after retry attempts
    /// - The organization does not exist
    /// - The initiator is not an organization admin
    /// - The new name is invalid
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, UserSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let slug = OrganizationSlug::new(1);
    /// let updated = client.update_organization(slug, UserSlug::new(42), Some("new-name".into())).await?;
    /// println!("Updated organization: {}", updated.name);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn update_organization(
        &self,
        slug: OrganizationSlug,
        user: UserSlug,
        name: Option<String>,
    ) -> Result<OrganizationInfo> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "update_organization",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "update_organization",
                || async {
                    let mut client = crate::connected_client!(pool, create_organization_client);

                    let request = proto::UpdateOrganizationRequest {
                        slug: Some(proto::OrganizationSlug { slug: slug.value() }),
                        initiator: Some(proto::UserSlug { slug: user.value() }),
                        name: name.clone(),
                    };

                    let response = client
                        .update_organization(tonic::Request::new(request))
                        .await?
                        .into_inner();

                    Ok(OrganizationInfo::from_update_proto(response))
                },
            ),
        )
        .await
    }

    /// Soft-deletes an organization by slug.
    ///
    /// Marks the organization for deletion. Fails if the organization
    /// still contains active vaults. The organization enters `Deleted` status
    /// and data is retained for `retention_days` before being purged.
    ///
    /// # Arguments
    ///
    /// * `slug` - Organization slug (external identifier).
    /// * `user` - User slug of the admin initiating the delete.
    ///
    /// # Returns
    ///
    /// Returns [`OrganizationDeleteInfo`] with the deletion timestamp and retention period.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Connection fails after retry attempts
    /// - The organization does not exist
    /// - The organization still has active vaults
    /// - The initiator is not an organization admin
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, UserSlug, VaultSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let slug = OrganizationSlug::new(1);
    /// let delete_info = client.delete_organization(slug, UserSlug::new(42)).await?;
    /// println!("Deleted, retention: {} days", delete_info.retention_days);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn delete_organization(
        &self,
        slug: OrganizationSlug,
        user: UserSlug,
    ) -> Result<OrganizationDeleteInfo> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "delete_organization",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "delete_organization",
                || async {
                    let mut client = crate::connected_client!(pool, create_organization_client);

                    let request = proto::DeleteOrganizationRequest {
                        slug: Some(proto::OrganizationSlug { slug: slug.value() }),
                        initiator: Some(proto::UserSlug { slug: user.value() }),
                    };

                    let response = client
                        .delete_organization(tonic::Request::new(request))
                        .await?
                        .into_inner();

                    let deleted_at =
                        response.deleted_at.as_ref().and_then(proto_timestamp_to_system_time);

                    Ok(OrganizationDeleteInfo {
                        deleted_at,
                        retention_days: response.retention_days,
                    })
                },
            ),
        )
        .await
    }

    /// Lists organizations visible to the caller.
    ///
    /// Returns a paginated list of organizations. Pass the returned
    /// `next_page_token` into subsequent calls to retrieve further pages.
    ///
    /// # Arguments
    ///
    /// * `caller` - User slug of the caller (for authorization filtering).
    /// * `page_size` - Maximum items per page (0 = server default).
    /// * `page_token` - Opaque cursor from a previous response, or `None` for the first page.
    ///
    /// # Returns
    ///
    /// A tuple of `(organizations, next_page_token)`. When `next_page_token`
    /// is `None`, there are no more pages.
    ///
    /// # Errors
    ///
    /// Returns an error if connection fails after retry attempts.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::LedgerClient;
    /// # use inferadb_ledger_types::UserSlug;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// let (organizations, _next) = client.list_organizations(UserSlug::new(1), 100, None).await?;
    /// for org in organizations {
    ///     println!("Organization: {} (slug: {})", org.name, org.slug);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn list_organizations(
        &self,
        caller: UserSlug,
        page_size: u32,
        page_token: Option<Vec<u8>>,
    ) -> Result<(Vec<OrganizationInfo>, Option<Vec<u8>>)> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "list_organizations",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "list_organizations",
                || async {
                    let mut client = crate::connected_client!(pool, create_organization_client);

                    let request = proto::ListOrganizationsRequest {
                        page_token: page_token.clone(),
                        page_size,
                        caller: Some(proto::UserSlug { slug: caller.value() }),
                    };

                    let response =
                        client.list_organizations(tonic::Request::new(request)).await?.into_inner();

                    let organizations = response
                        .organizations
                        .into_iter()
                        .map(OrganizationInfo::from_proto)
                        .collect();

                    Ok((organizations, response.next_page_token))
                },
            ),
        )
        .await
    }

    /// Lists members of an organization.
    ///
    /// # Arguments
    ///
    /// * `slug` - Organization slug (external identifier).
    /// * `caller` - User slug of the caller (must be a member).
    /// * `page_size` - Maximum items per page (0 = server default).
    /// * `page_token` - Opaque cursor from a previous response, or `None` for the first page.
    ///
    /// # Returns
    ///
    /// A tuple of `(members, next_page_token)`. When `next_page_token`
    /// is `None`, there are no more pages.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Connection fails after retry attempts
    /// - The organization does not exist
    /// - The caller is not a member
    pub async fn list_organization_members(
        &self,
        slug: OrganizationSlug,
        caller: UserSlug,
        page_size: u32,
        page_token: Option<Vec<u8>>,
    ) -> Result<(Vec<OrganizationMemberInfo>, Option<Vec<u8>>)> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "list_organization_members",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "list_organization_members",
                || async {
                    let mut client = crate::connected_client!(pool, create_organization_client);

                    let request = proto::ListOrganizationMembersRequest {
                        slug: Some(proto::OrganizationSlug { slug: slug.value() }),
                        caller: Some(proto::UserSlug { slug: caller.value() }),
                        page_token: page_token.clone(),
                        page_size,
                    };

                    let response = client
                        .list_organization_members(tonic::Request::new(request))
                        .await?
                        .into_inner();

                    let members =
                        response.members.iter().map(OrganizationMemberInfo::from_proto).collect();

                    Ok((members, response.next_page_token))
                },
            ),
        )
        .await
    }

    /// Removes a member from an organization.
    ///
    /// Self-removal: any member can leave unless they are the last admin.
    /// Removing others: initiator must be an admin.
    ///
    /// # Arguments
    ///
    /// * `slug` - Organization slug (external identifier).
    /// * `user` - User slug of the person performing the removal.
    /// * `target` - User slug of the member to remove.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The target is not a member
    /// - The initiator lacks permission
    /// - Removing the last admin
    pub async fn remove_organization_member(
        &self,
        slug: OrganizationSlug,
        user: UserSlug,
        target: UserSlug,
    ) -> Result<()> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "remove_organization_member",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "remove_organization_member",
                || async {
                    let mut client = crate::connected_client!(pool, create_organization_client);

                    let request = proto::RemoveOrganizationMemberRequest {
                        slug: Some(proto::OrganizationSlug { slug: slug.value() }),
                        initiator: Some(proto::UserSlug { slug: user.value() }),
                        target: Some(proto::UserSlug { slug: target.value() }),
                    };

                    client.remove_organization_member(tonic::Request::new(request)).await?;

                    Ok(())
                },
            ),
        )
        .await
    }

    /// Updates a member's role within an organization.
    ///
    /// Initiator must be an admin. Cannot demote the last admin.
    ///
    /// # Arguments
    ///
    /// * `slug` - Organization slug (external identifier).
    /// * `user` - User slug of the admin performing the change.
    /// * `target` - User slug of the member to update.
    /// * `role` - New role for the member.
    ///
    /// # Returns
    ///
    /// Returns the updated [`OrganizationMemberInfo`].
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The initiator is not an admin
    /// - The target is not a member
    /// - Demoting the last admin
    pub async fn update_organization_member_role(
        &self,
        slug: OrganizationSlug,
        user: UserSlug,
        target: UserSlug,
        role: OrganizationMemberRole,
    ) -> Result<OrganizationMemberInfo> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "update_organization_member_role",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "update_organization_member_role",
                || async {
                    let mut client = crate::connected_client!(pool, create_organization_client);

                    let request = proto::UpdateOrganizationMemberRoleRequest {
                        slug: Some(proto::OrganizationSlug { slug: slug.value() }),
                        initiator: Some(proto::UserSlug { slug: user.value() }),
                        target: Some(proto::UserSlug { slug: target.value() }),
                        role: role.to_proto(),
                    };

                    let response = client
                        .update_organization_member_role(tonic::Request::new(request))
                        .await?
                        .into_inner();

                    let member = response
                        .member
                        .as_ref()
                        .map(OrganizationMemberInfo::from_proto)
                        .ok_or_else(|| {
                        missing_response_field("member", "UpdateOrganizationMemberRoleResponse")
                    })?;

                    Ok(member)
                },
            ),
        )
        .await
    }

    // =========================================================================
    // Organization Teams
    // =========================================================================

    /// Lists teams in an organization.
    ///
    /// # Arguments
    ///
    /// * `organization` - Organization slug (external identifier).
    /// * `caller` - User slug of the caller (for authorization filtering).
    /// * `page_size` - Maximum items per page (0 = server default).
    /// * `page_token` - Opaque cursor from a previous response, or `None` for the first page.
    ///
    /// # Returns
    ///
    /// A tuple of `(teams, next_page_token)`. When `next_page_token`
    /// is `None`, there are no more pages.
    pub async fn list_organization_teams(
        &self,
        organization: OrganizationSlug,
        caller: UserSlug,
        page_size: u32,
        page_token: Option<Vec<u8>>,
    ) -> Result<(Vec<TeamInfo>, Option<Vec<u8>>)> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "list_organization_teams",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "list_organization_teams",
                || async {
                    let mut client = crate::connected_client!(pool, create_organization_client);

                    let request = proto::ListOrganizationTeamsRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        caller: Some(proto::UserSlug { slug: caller.value() }),
                        page_token: page_token.clone(),
                        page_size,
                    };

                    let response = client
                        .list_organization_teams(tonic::Request::new(request))
                        .await?
                        .into_inner();

                    let teams = response.teams.iter().map(TeamInfo::from_proto).collect();

                    Ok((teams, response.next_page_token))
                },
            ),
        )
        .await
    }

    /// Creates a new team within an organization.
    ///
    /// The team name must be unique within the organization.
    /// A Snowflake team slug is generated server-side.
    pub async fn create_organization_team(
        &self,
        organization: OrganizationSlug,
        name: &str,
        initiator: UserSlug,
    ) -> Result<TeamInfo> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();
        let name = name.to_string();

        self.with_metrics(
            "create_organization_team",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "create_organization_team",
                || async {
                    let mut client = crate::connected_client!(pool, create_organization_client);

                    let request = proto::CreateOrganizationTeamRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        name: name.clone(),
                        initiator: Some(proto::UserSlug { slug: initiator.value() }),
                    };

                    let response = client
                        .create_organization_team(tonic::Request::new(request))
                        .await?
                        .into_inner();

                    response
                        .team
                        .as_ref()
                        .map(TeamInfo::from_proto)
                        .ok_or_else(|| missing_response_field("team", "CreateTeamResponse"))
                },
            ),
        )
        .await
    }

    /// Deletes a team from an organization.
    ///
    /// Caller must be an organization administrator or team manager.
    /// Optionally moves all members to another team before deletion.
    pub async fn delete_organization_team(
        &self,
        team: TeamSlug,
        initiator: UserSlug,
        move_members_to: Option<TeamSlug>,
    ) -> Result<()> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "delete_organization_team",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "delete_organization_team",
                || async {
                    let mut client = crate::connected_client!(pool, create_organization_client);

                    let request = proto::DeleteOrganizationTeamRequest {
                        slug: Some(proto::TeamSlug { slug: team.value() }),
                        move_members_to: move_members_to
                            .map(|s| proto::TeamSlug { slug: s.value() }),
                        initiator: Some(proto::UserSlug { slug: initiator.value() }),
                    };

                    client.delete_organization_team(tonic::Request::new(request)).await?;

                    Ok(())
                },
            ),
        )
        .await
    }

    /// Updates a team's metadata (currently: name only).
    ///
    /// Caller must be an organization administrator or team manager.
    pub async fn update_organization_team(
        &self,
        team: TeamSlug,
        initiator: UserSlug,
        name: Option<&str>,
    ) -> Result<TeamInfo> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();
        let name = name.map(|s| s.to_string());

        self.with_metrics(
            "update_organization_team",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "update_organization_team",
                || async {
                    let mut client = crate::connected_client!(pool, create_organization_client);

                    let request = proto::UpdateOrganizationTeamRequest {
                        slug: Some(proto::TeamSlug { slug: team.value() }),
                        initiator: Some(proto::UserSlug { slug: initiator.value() }),
                        name: name.clone(),
                    };

                    let response = client
                        .update_organization_team(tonic::Request::new(request))
                        .await?
                        .into_inner();

                    response
                        .team
                        .as_ref()
                        .map(TeamInfo::from_proto)
                        .ok_or_else(|| missing_response_field("team", "UpdateTeamResponse"))
                },
            ),
        )
        .await
    }

    /// Retrieves a single team by slug.
    ///
    /// Admins can see any team; non-admin callers can only see teams they
    /// belong to.
    pub async fn get_organization_team(
        &self,
        team: TeamSlug,
        caller: UserSlug,
    ) -> Result<TeamInfo> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "get_organization_team",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "get_organization_team",
                || async {
                    let mut client = crate::connected_client!(pool, create_organization_client);

                    let request = proto::GetOrganizationTeamRequest {
                        slug: Some(proto::TeamSlug { slug: team.value() }),
                        caller: Some(proto::UserSlug { slug: caller.value() }),
                    };

                    let response = client
                        .get_organization_team(tonic::Request::new(request))
                        .await?
                        .into_inner();

                    response.team.as_ref().map(TeamInfo::from_proto).ok_or_else(|| {
                        missing_response_field("team", "GetOrganizationTeamResponse")
                    })
                },
            ),
        )
        .await
    }

    /// Adds a member to a team.
    ///
    /// Returns the updated team info.
    pub async fn add_team_member(
        &self,
        team: TeamSlug,
        user: UserSlug,
        role: TeamMemberRole,
        initiator: UserSlug,
    ) -> Result<TeamInfo> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "add_team_member",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "add_team_member",
                || async {
                    let mut client = crate::connected_client!(pool, create_organization_client);

                    let request = proto::AddTeamMemberRequest {
                        team: Some(proto::TeamSlug { slug: team.value() }),
                        user: Some(proto::UserSlug { slug: user.value() }),
                        role: role.to_proto().into(),
                        initiator: Some(proto::UserSlug { slug: initiator.value() }),
                    };

                    let response =
                        client.add_team_member(tonic::Request::new(request)).await?.into_inner();

                    response
                        .team
                        .as_ref()
                        .map(TeamInfo::from_proto)
                        .ok_or_else(|| missing_response_field("team", "AddTeamMemberResponse"))
                },
            ),
        )
        .await
    }

    /// Removes a member from a team.
    pub async fn remove_team_member(
        &self,
        team: TeamSlug,
        user: UserSlug,
        initiator: UserSlug,
    ) -> Result<()> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "remove_team_member",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "remove_team_member",
                || async {
                    let mut client = crate::connected_client!(pool, create_organization_client);

                    let request = proto::RemoveTeamMemberRequest {
                        team: Some(proto::TeamSlug { slug: team.value() }),
                        user: Some(proto::UserSlug { slug: user.value() }),
                        initiator: Some(proto::UserSlug { slug: initiator.value() }),
                    };

                    client.remove_team_member(tonic::Request::new(request)).await?;
                    Ok(())
                },
            ),
        )
        .await
    }

    /// Initiates migration of an organization to a different region.
    ///
    /// Transitions the organization to `Migrating` status and creates a background
    /// saga to drive the migration through. Writes are blocked during migration.
    ///
    /// # Arguments
    ///
    /// * `slug` - Organization slug (external identifier).
    /// * `target_region` - Target region for the migration.
    /// * `acknowledge_residency_downgrade` - Required for protected -> non-protected.
    ///
    /// # Returns
    ///
    /// Returns [`MigrationInfo`] with source/target regions and current status.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Organization does not exist
    /// - Organization is not in `Active` status
    /// - Target region is the same as current region
    /// - Protected -> non-protected without acknowledgment
    pub async fn migrate_organization(
        &self,
        slug: OrganizationSlug,
        target_region: Region,
        acknowledge_residency_downgrade: bool,
        user: UserSlug,
    ) -> Result<MigrationInfo> {
        self.check_shutdown(None)?;

        let proto_target: proto::Region = target_region.into();
        let target_i32: i32 = proto_target.into();
        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "migrate_organization",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "migrate_organization",
                || async {
                    let mut client = crate::connected_client!(pool, create_organization_client);

                    let request = proto::MigrateOrganizationRequest {
                        slug: Some(proto::OrganizationSlug { slug: slug.value() }),
                        target_region: target_i32,
                        acknowledge_residency_downgrade,
                        initiator: Some(proto::UserSlug { slug: user.value() }),
                    };

                    let response = client
                        .migrate_organization(tonic::Request::new(request))
                        .await?
                        .into_inner();

                    Ok(MigrationInfo {
                        slug: OrganizationSlug::new(response.slug.map_or(0, |s| s.slug)),
                        source_region: region_from_proto_i32(response.source_region)
                            .unwrap_or(Region::GLOBAL),
                        target_region: region_from_proto_i32(response.target_region)
                            .unwrap_or(target_region),
                        status: OrganizationStatus::from_proto(response.status),
                    })
                },
            ),
        )
        .await
    }

    /// Initiates a user region migration.
    ///
    /// Moves the user's data residency to the target region. Authenticated API
    /// calls for this user are temporarily blocked while the migration is in
    /// progress.
    ///
    /// # Arguments
    ///
    /// * `user_slug` - User slug (external identifier).
    /// * `target_region` - Target region for the migration.
    ///
    /// # Returns
    ///
    /// Returns [`UserMigrationInfo`] with source/target regions and current directory status.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - User does not exist
    /// - Target region is the same as current region
    /// - Connection fails after retry attempts
    pub async fn migrate_user_region(
        &self,
        user: UserSlug,
        target_region: Region,
    ) -> Result<UserMigrationInfo> {
        self.check_shutdown(None)?;

        let proto_target: proto::Region = target_region.into();
        let target_i32: i32 = proto_target.into();
        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "migrate_user_region",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "migrate_user_region",
                || async {
                    let mut client = crate::connected_client!(pool, create_user_client);

                    let request = proto::MigrateUserRegionRequest {
                        slug: Some(proto::UserSlug { slug: user.value() }),
                        target_region: target_i32,
                    };

                    let response = client
                        .migrate_user_region(tonic::Request::new(request))
                        .await?
                        .into_inner();

                    Ok(UserMigrationInfo {
                        slug: UserSlug::new(response.slug.map_or(0, |s| s.slug)),
                        source_region: region_from_proto_i32(response.source_region)
                            .unwrap_or(Region::GLOBAL),
                        target_region: region_from_proto_i32(response.target_region)
                            .unwrap_or(target_region),
                        directory_status: response.directory_status,
                    })
                },
            ),
        )
        .await
    }

    /// Erases a user's PII through crypto-shredding.
    ///
    /// Permanently destroys the user's blinding key material, rendering all
    /// associated email hashes unrecoverable. This operation is irreversible.
    ///
    /// # Arguments
    ///
    /// * `user` - User slug (Snowflake ID) to erase.
    /// * `erased_by` - Identity of the actor requesting erasure (audit trail).
    /// * `region` - Region where the user's PII resides.
    ///
    /// # Returns
    ///
    /// Returns the user slug that was erased.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - User does not exist
    /// - Confirmation token is invalid, expired, or missing
    /// - Connection fails after retry attempts
    pub async fn erase_user(
        &self,
        user: UserSlug,
        erased_by: impl Into<String>,
        region: Region,
    ) -> Result<UserSlug> {
        self.check_shutdown(None)?;

        let erased_by = erased_by.into();
        let proto_region: proto::Region = region.into();
        let region_i32: i32 = proto_region.into();
        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "erase_user",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                || async {
                    let mut client = crate::connected_client!(pool, create_user_client);

                    let request = proto::EraseUserRequest {
                        user: Some(proto::UserSlug { slug: user.value() }),
                        erased_by: erased_by.clone(),
                        region: region_i32,
                    };

                    let response =
                        client.erase_user(tonic::Request::new(request)).await?.into_inner();

                    Ok(UserSlug::new(response.user.map_or(0, |u| u.slug)))
                },
            ),
        )
        .await
    }
}
