//! Organization CRUD, team management, migration, and erasure operations.

use inferadb_ledger_types::{OrganizationSlug, Region, TeamSlug, UserSlug};

use crate::{
    LedgerClient,
    error::Result,
    types::admin::{
        MigrationInfo, OrganizationDeleteInfo, OrganizationInfo, OrganizationMemberInfo,
        OrganizationMemberRole, OrganizationTier, TeamInfo, TeamMemberRole, UserMigrationInfo,
    },
};

impl LedgerClient {
    // =========================================================================
    // Admin Operations
    // =========================================================================

    /// Creates a new organization with the specified data residency region.
    ///
    /// `name` is a human-readable identifier (e.g., `"acme_corp"`). `region`
    /// determines where the organization's data is stored and which data
    /// protection regulations apply; it must not be `Region::Global` or
    /// `Region::Unspecified`. Returns [`OrganizationInfo`] containing the
    /// generated slug and metadata.
    pub async fn create_organization(
        &self,
        name: impl Into<String>,
        region: Region,
        caller: UserSlug,
        tier: OrganizationTier,
    ) -> Result<OrganizationInfo> {
        let name = name.into();
        // Generate the organization slug once, outside the retry loop.
        // Every retry for this logical call reuses it so the saga's
        // idempotency-by-slug path returns the same OrganizationId
        // instead of creating a duplicate body on
        // response-lost-in-flight.
        let org_slug =
            inferadb_ledger_types::snowflake::generate_organization_slug().map_err(|e| {
                crate::error::SdkError::Config {
                    message: format!("generate organization slug: {e}"),
                }
            })?;
        let pool = self.pool.clone();
        self.call_with_retry("create_organization", || {
            let pool = pool.clone();
            let name = name.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::organization::create_organization(
                    wire_client,
                    request_id,
                    name,
                    region,
                    caller,
                    tier,
                    org_slug,
                )
                .await
            }
        })
        .await
    }

    /// Returns information about an organization by slug.
    pub async fn get_organization(
        &self,
        slug: OrganizationSlug,
        user: UserSlug,
    ) -> Result<OrganizationInfo> {
        let pool = self.pool.clone();
        self.call_with_retry("get_organization", || {
            let pool = pool.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::organization::get_organization(wire_client, request_id, slug, user)
                    .await
            }
        })
        .await
    }

    /// Updates an organization's mutable fields.
    pub async fn update_organization(
        &self,
        slug: OrganizationSlug,
        user: UserSlug,
        name: Option<String>,
    ) -> Result<OrganizationInfo> {
        let pool = self.pool.clone();
        self.call_with_retry("update_organization", || {
            let pool = pool.clone();
            let name = name.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::organization::update_organization(
                    wire_client,
                    request_id,
                    slug,
                    user,
                    name,
                )
                .await
            }
        })
        .await
    }

    /// Soft-deletes an organization by slug.
    pub async fn delete_organization(
        &self,
        slug: OrganizationSlug,
        user: UserSlug,
    ) -> Result<OrganizationDeleteInfo> {
        let pool = self.pool.clone();
        self.call_with_retry("delete_organization", || {
            let pool = pool.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::organization::delete_organization(
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

    /// Lists organizations visible to the caller.
    pub async fn list_organizations(
        &self,
        caller: UserSlug,
        page_size: u32,
        page_token: Option<Vec<u8>>,
    ) -> Result<(Vec<OrganizationInfo>, Option<Vec<u8>>)> {
        let pool = self.pool.clone();
        self.call_with_retry("list_organizations", || {
            let pool = pool.clone();
            let page_token = page_token.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::organization::list_organizations(
                    wire_client,
                    request_id,
                    caller,
                    page_size,
                    page_token,
                )
                .await
            }
        })
        .await
    }

    /// Lists members of an organization.
    pub async fn list_organization_members(
        &self,
        slug: OrganizationSlug,
        caller: UserSlug,
        page_size: u32,
        page_token: Option<Vec<u8>>,
    ) -> Result<(Vec<OrganizationMemberInfo>, Option<Vec<u8>>)> {
        let pool = self.pool.clone();
        self.call_with_retry("list_organization_members", || {
            let pool = pool.clone();
            let page_token = page_token.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::organization::list_organization_members(
                    wire_client,
                    request_id,
                    slug,
                    caller,
                    page_size,
                    page_token,
                )
                .await
            }
        })
        .await
    }

    /// Removes a member from an organization.
    pub async fn remove_organization_member(
        &self,
        slug: OrganizationSlug,
        user: UserSlug,
        target: UserSlug,
    ) -> Result<()> {
        let pool = self.pool.clone();
        self.call_with_retry("remove_organization_member", || {
            let pool = pool.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::organization::remove_organization_member(
                    wire_client,
                    request_id,
                    slug,
                    user,
                    target,
                )
                .await
            }
        })
        .await
    }

    /// Updates a member's role within an organization.
    pub async fn update_organization_member_role(
        &self,
        slug: OrganizationSlug,
        user: UserSlug,
        target: UserSlug,
        role: OrganizationMemberRole,
    ) -> Result<OrganizationMemberInfo> {
        let pool = self.pool.clone();
        self.call_with_retry("update_organization_member_role", || {
            let pool = pool.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::organization::update_organization_member_role(
                    wire_client,
                    request_id,
                    slug,
                    user,
                    target,
                    role,
                )
                .await
            }
        })
        .await
    }

    // =========================================================================
    // Organization Teams
    // =========================================================================

    /// Lists teams in an organization.
    pub async fn list_organization_teams(
        &self,
        organization: OrganizationSlug,
        caller: UserSlug,
        page_size: u32,
        page_token: Option<Vec<u8>>,
    ) -> Result<(Vec<TeamInfo>, Option<Vec<u8>>)> {
        let pool = self.pool.clone();
        self.call_with_retry("list_organization_teams", || {
            let pool = pool.clone();
            let page_token = page_token.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::organization::list_organization_teams(
                    wire_client,
                    request_id,
                    organization,
                    caller,
                    page_size,
                    page_token,
                )
                .await
            }
        })
        .await
    }

    /// Creates a new team within an organization.
    pub async fn create_organization_team(
        &self,
        organization: OrganizationSlug,
        name: &str,
        caller: UserSlug,
    ) -> Result<TeamInfo> {
        let pool = self.pool.clone();
        let name = name.to_string();
        // Generate the team slug once, outside the retry loop. Every retry
        // for this logical call reuses it so the per-org apply
        // idempotency-by-slug path returns the same TeamId instead of
        // creating a duplicate directory entry on
        // response-lost-in-flight.
        let team_slug = inferadb_ledger_types::snowflake::generate_team_slug().map_err(|e| {
            crate::error::SdkError::Config { message: format!("generate team slug: {e}") }
        })?;

        self.call_with_retry("create_organization_team", || {
            let pool = pool.clone();
            let name = name.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::organization::create_organization_team(
                    wire_client,
                    request_id,
                    organization,
                    name,
                    caller,
                    team_slug,
                )
                .await
            }
        })
        .await
    }

    /// Deletes a team from an organization.
    pub async fn delete_organization_team(
        &self,
        team: TeamSlug,
        caller: UserSlug,
        move_members_to: Option<TeamSlug>,
    ) -> Result<()> {
        let pool = self.pool.clone();
        self.call_with_retry("delete_organization_team", || {
            let pool = pool.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::organization::delete_organization_team(
                    wire_client,
                    request_id,
                    team,
                    caller,
                    move_members_to,
                )
                .await
            }
        })
        .await
    }

    /// Updates a team's metadata (currently: name only).
    pub async fn update_organization_team(
        &self,
        team: TeamSlug,
        caller: UserSlug,
        name: Option<&str>,
    ) -> Result<TeamInfo> {
        let pool = self.pool.clone();
        let name = name.map(|s| s.to_string());

        self.call_with_retry("update_organization_team", || {
            let pool = pool.clone();
            let name = name.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::organization::update_organization_team(
                    wire_client,
                    request_id,
                    team,
                    caller,
                    name,
                )
                .await
            }
        })
        .await
    }

    /// Retrieves a single team by slug.
    pub async fn get_organization_team(
        &self,
        team: TeamSlug,
        caller: UserSlug,
    ) -> Result<TeamInfo> {
        let pool = self.pool.clone();
        self.call_with_retry("get_organization_team", || {
            let pool = pool.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::organization::get_organization_team(
                    wire_client,
                    request_id,
                    team,
                    caller,
                )
                .await
            }
        })
        .await
    }

    /// Adds a member to a team.
    pub async fn add_team_member(
        &self,
        team: TeamSlug,
        user: UserSlug,
        role: TeamMemberRole,
        caller: UserSlug,
    ) -> Result<TeamInfo> {
        let pool = self.pool.clone();
        self.call_with_retry("add_team_member", || {
            let pool = pool.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::organization::add_team_member(
                    wire_client,
                    request_id,
                    team,
                    user,
                    role,
                    caller,
                )
                .await
            }
        })
        .await
    }

    /// Removes a member from a team.
    pub async fn remove_team_member(
        &self,
        team: TeamSlug,
        user: UserSlug,
        caller: UserSlug,
    ) -> Result<()> {
        let pool = self.pool.clone();
        self.call_with_retry("remove_team_member", || {
            let pool = pool.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::organization::remove_team_member(
                    wire_client,
                    request_id,
                    team,
                    user,
                    caller,
                )
                .await
            }
        })
        .await
    }

    /// Updates a team member's role.
    pub async fn update_team_member_role(
        &self,
        team: TeamSlug,
        user: UserSlug,
        role: TeamMemberRole,
        caller: UserSlug,
    ) -> Result<TeamInfo> {
        let pool = self.pool.clone();
        self.call_with_retry("update_team_member_role", || {
            let pool = pool.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::organization::update_team_member_role(
                    wire_client,
                    request_id,
                    team,
                    user,
                    role,
                    caller,
                )
                .await
            }
        })
        .await
    }

    /// Initiates migration of an organization to a different region.
    pub async fn migrate_organization(
        &self,
        slug: OrganizationSlug,
        target_region: Region,
        acknowledge_residency_downgrade: bool,
        user: UserSlug,
    ) -> Result<MigrationInfo> {
        let pool = self.pool.clone();
        self.call_with_retry("migrate_organization", || {
            let pool = pool.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::organization::migrate_organization(
                    wire_client,
                    request_id,
                    slug,
                    target_region,
                    acknowledge_residency_downgrade,
                    user,
                )
                .await
            }
        })
        .await
    }

    /// Initiates a user region migration.
    pub async fn migrate_user_region(
        &self,
        caller: UserSlug,
        user: UserSlug,
        target_region: Region,
    ) -> Result<UserMigrationInfo> {
        let pool = self.pool.clone();
        self.call_with_retry("migrate_user_region", || {
            let pool = pool.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::user::migrate_user_region(
                    wire_client,
                    request_id,
                    caller,
                    user,
                    target_region,
                )
                .await
            }
        })
        .await
    }

    /// Erases a user's PII through crypto-shredding.
    pub async fn erase_user(
        &self,
        user: UserSlug,
        caller: UserSlug,
        region: Region,
    ) -> Result<UserSlug> {
        let pool = self.pool.clone();
        self.call_with_retry("erase_user", || {
            let pool = pool.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::user::erase_user(wire_client, request_id, user, caller, region)
                    .await
            }
        })
        .await
    }
}
