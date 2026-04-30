//! Wire-protocol service surface for the InferaDB Ledger.
//!
//! This crate is the canonical home of the [`define_protocol!`] invocation
//! that enumerates every RPC across all 14 services. The macro emits:
//!
//! - One server trait per service (`pub trait ReadService { ... }`).
//! - One client struct per service (`pub struct ReadServiceClient { ... }`).
//! - Per-service unary + streaming dispatch fns (`dispatch_read_service_unary`,
//!   `dispatch_read_service_stream`).
//! - The populated `OpCode` enum + `is_registered_opcode` predicate.
//! - `OpReflectEntry` introspection support and `op_reflect_registry()`.
//! - The shared `TypedResponseStream<T>` decoder for streaming responses.
//!
//! Message types live in [`inferadb_ledger_wire::services`]; this crate
//! brings them into local scope via wildcard imports and instantiates the
//! macro. The resulting tokens are emitted at this crate's root, so client
//! code calls e.g. `inferadb_ledger_wire_services::ReadServiceClient::new`.
//!
//! # Crate layering
//!
//! `wire-services` sits above both `wire` (frame format, errors, request
//! context) and `wire-transport` (QUIC `WireServer` / `WireClient`,
//! [`UnaryRequest`], [`Dispatcher`]). It must be a separate crate because
//! the macro's emitted code references both, and `wire-transport` itself
//! depends on `wire` — placing the invocation in `wire` would create a
//! dep cycle.
//!
//! [`define_protocol!`]: inferadb_ledger_wire_macro::define_protocol
//! [`UnaryRequest`]: inferadb_ledger_wire_transport::UnaryRequest
//! [`Dispatcher`]: inferadb_ledger_wire_transport::Dispatcher
//!
//! # Naming convention
//!
//! Within each service block, opcodes follow the proto file's RPC
//! declaration order. Service bases match the constants in
//! [`inferadb_ledger_wire::opcode`]. The full per-RPC opcode allocation is
//! cross-referenced in `docs/migrations/from-grpc.md`.

// The macro emits items with auto-generated names whose individual rustdoc
// would be redundant — the proto file is the canonical schema source.
#![allow(missing_docs)]

// Bring every service-module message type into local scope so the
// `define_protocol!` invocation below can reference them by their bare
// type name rather than `read::ReadRequest`. The macro expansion runs in
// this scope, so these wildcard imports drive type-name resolution for
// the entire emitted dispatch surface.
//
// Wildcard imports across 14 service modules + `shared` carry a real
// collision risk; if a future addition introduces a same-named type in
// two modules, qualify the path in the DSL (`rpc 0x... method(read::Foo)
// -> read::Bar;`) rather than re-exporting.
//
// `unused_imports` is silenced because not every service exports types
// referenced inside this crate — `shared` is the obvious case (its
// definitions back fields of other modules' message types, not RPC
// signatures), but several service modules also export auxiliary types
// (`*Filter`, `*Info`, `*Member`, etc.) that don't appear directly in
// any RPC arrow.
#[allow(unused_imports)]
use inferadb_ledger_wire::services::{
    admin::*, app::*, discovery::*, events::*, health::*, invitation::*, organization::*, raft::*,
    read::*, schema::*, shared::*, token::*, user::*, vault::*, write::*,
};

inferadb_ledger_wire_macro::define_protocol! {
    // ReadService — base 0x0010 — 13 RPCs.
    // Matches `proto/ledger/v1/ledger.proto:340-389`. WatchBlocks is the
    // only server-streaming RPC.
    service ReadService {
        base: 0x0010,
        rpc 0x0010 read(ReadRequest) -> ReadResponse;
        rpc 0x0011 batch_read(BatchReadRequest) -> BatchReadResponse;
        rpc 0x0012 verified_read(VerifiedReadRequest) -> VerifiedReadResponse;
        rpc 0x0013 historical_read(HistoricalReadRequest) -> HistoricalReadResponse;
        rpc 0x0014 watch_blocks(WatchBlocksRequest) -> stream BlockAnnouncement;
        rpc 0x0015 get_block(GetBlockRequest) -> GetBlockResponse;
        rpc 0x0016 get_block_range(GetBlockRangeRequest) -> GetBlockRangeResponse;
        rpc 0x0017 get_tip(GetTipRequest) -> GetTipResponse;
        rpc 0x0018 get_client_state(GetClientStateRequest) -> GetClientStateResponse;
        rpc 0x0019 list_relationships(ListRelationshipsRequest) -> ListRelationshipsResponse;
        rpc 0x001A check_relationship(CheckRelationshipRequest) -> CheckRelationshipResponse;
        rpc 0x001B list_resources(ListResourcesRequest) -> ListResourcesResponse;
        rpc 0x001C list_entities(ListEntitiesRequest) -> ListEntitiesResponse;
    }

    // WriteService — base 0x0100 — 1 RPC.
    // Matches `proto/ledger/v1/ledger.proto:631-634`. WriteResponse uses a
    // postcard-encoded oneof (success/error) — the codec round-trips via
    // the Rust enum representation defined in `wire/src/services/write.rs`.
    service WriteService {
        base: 0x0100,
        rpc 0x0100 write(WriteRequest) -> WriteResponse;
    }

    // OrganizationService — base 0x0200 — 17 RPCs.
    // Matches `proto/ledger/v1/ledger.proto:732-794`.
    service OrganizationService {
        base: 0x0200,
        rpc 0x0200 create_organization(CreateOrganizationRequest) -> CreateOrganizationResponse;
        rpc 0x0201 delete_organization(DeleteOrganizationRequest) -> DeleteOrganizationResponse;
        rpc 0x0202 get_organization(GetOrganizationRequest) -> GetOrganizationResponse;
        rpc 0x0203 list_organizations(ListOrganizationsRequest) -> ListOrganizationsResponse;
        rpc 0x0204 migrate_organization(MigrateOrganizationRequest) -> MigrateOrganizationResponse;
        rpc 0x0205 update_organization(UpdateOrganizationRequest) -> UpdateOrganizationResponse;
        rpc 0x0206 list_organization_members(ListOrganizationMembersRequest) -> ListOrganizationMembersResponse;
        rpc 0x0207 remove_organization_member(RemoveOrganizationMemberRequest) -> RemoveOrganizationMemberResponse;
        rpc 0x0208 update_organization_member_role(UpdateOrganizationMemberRoleRequest) -> UpdateOrganizationMemberRoleResponse;
        rpc 0x0209 list_organization_teams(ListOrganizationTeamsRequest) -> ListOrganizationTeamsResponse;
        rpc 0x020A create_organization_team(CreateOrganizationTeamRequest) -> CreateOrganizationTeamResponse;
        rpc 0x020B delete_organization_team(DeleteOrganizationTeamRequest) -> DeleteOrganizationTeamResponse;
        rpc 0x020C update_organization_team(UpdateOrganizationTeamRequest) -> UpdateOrganizationTeamResponse;
        rpc 0x020D get_organization_team(GetOrganizationTeamRequest) -> GetOrganizationTeamResponse;
        rpc 0x020E add_team_member(AddTeamMemberRequest) -> AddTeamMemberResponse;
        rpc 0x020F remove_team_member(RemoveTeamMemberRequest) -> RemoveTeamMemberResponse;
        rpc 0x0210 update_team_member_role(UpdateTeamMemberRoleRequest) -> UpdateTeamMemberRoleResponse;
    }

    // VaultService — base 0x0300 — 5 RPCs.
    // Matches `proto/ledger/v1/ledger.proto:800-815`.
    service VaultService {
        base: 0x0300,
        rpc 0x0300 create_vault(CreateVaultRequest) -> CreateVaultResponse;
        rpc 0x0301 delete_vault(DeleteVaultRequest) -> DeleteVaultResponse;
        rpc 0x0302 get_vault(GetVaultRequest) -> GetVaultResponse;
        rpc 0x0303 list_vaults(ListVaultsRequest) -> ListVaultsResponse;
        rpc 0x0304 update_vault(UpdateVaultRequest) -> UpdateVaultResponse;
    }

    // SchemaService — base 0x0400 — 7 RPCs.
    // Matches `proto/ledger/v1/ledger.proto:823-844`. NOTE: no server-side
    // implementation today — the SDK implements schema management as a
    // pure client-side convention layered over Read/Write. The opcode
    // block is reserved either way; resolution before Task E.4.
    service SchemaService {
        base: 0x0400,
        rpc 0x0400 deploy_schema(DeploySchemaRequest) -> DeploySchemaResponse;
        rpc 0x0401 list_schema_versions(ListSchemaVersionsRequest) -> ListSchemaVersionsResponse;
        rpc 0x0402 get_schema(GetSchemaRequest) -> GetSchemaResponse;
        rpc 0x0403 activate_schema(ActivateSchemaRequest) -> ActivateSchemaResponse;
        rpc 0x0404 rollback_schema(RollbackSchemaRequest) -> RollbackSchemaResponse;
        rpc 0x0405 get_active_schema(GetActiveSchemaRequest) -> GetActiveSchemaResponse;
        rpc 0x0406 diff_schemas(DiffSchemasRequest) -> DiffSchemasResponse;
    }

    // UserService — base 0x0500 — 22 RPCs.
    // Matches `proto/ledger/v1/ledger.proto:2298-2398`.
    service UserService {
        base: 0x0500,
        rpc 0x0500 create_user(CreateUserRequest) -> CreateUserResponse;
        rpc 0x0501 get_user(GetUserRequest) -> GetUserResponse;
        rpc 0x0502 update_user(UpdateUserRequest) -> UpdateUserResponse;
        rpc 0x0503 delete_user(DeleteUserRequest) -> DeleteUserResponse;
        rpc 0x0504 list_users(ListUsersRequest) -> ListUsersResponse;
        rpc 0x0505 search_users(SearchUsersRequest) -> SearchUsersResponse;
        rpc 0x0506 create_user_email(CreateUserEmailRequest) -> CreateUserEmailResponse;
        rpc 0x0507 delete_user_email(DeleteUserEmailRequest) -> DeleteUserEmailResponse;
        rpc 0x0508 search_user_email(SearchUserEmailRequest) -> SearchUserEmailResponse;
        rpc 0x0509 verify_user_email(VerifyUserEmailRequest) -> VerifyUserEmailResponse;
        rpc 0x050A migrate_user_region(MigrateUserRegionRequest) -> MigrateUserRegionResponse;
        rpc 0x050B erase_user(EraseUserRequest) -> EraseUserResponse;
        rpc 0x050C initiate_email_verification(InitiateEmailVerificationRequest) -> InitiateEmailVerificationResponse;
        rpc 0x050D verify_email_code(VerifyEmailCodeRequest) -> VerifyEmailCodeResponse;
        rpc 0x050E complete_registration(CompleteRegistrationRequest) -> CompleteRegistrationResponse;
        rpc 0x050F create_user_credential(CreateUserCredentialRequest) -> CreateUserCredentialResponse;
        rpc 0x0510 list_user_credentials(ListUserCredentialsRequest) -> ListUserCredentialsResponse;
        rpc 0x0511 update_user_credential(UpdateUserCredentialRequest) -> UpdateUserCredentialResponse;
        rpc 0x0512 delete_user_credential(DeleteUserCredentialRequest) -> DeleteUserCredentialResponse;
        rpc 0x0513 create_totp_challenge(CreateTotpChallengeRequest) -> CreateTotpChallengeResponse;
        rpc 0x0514 verify_totp(VerifyTotpRequest) -> VerifyTotpResponse;
        rpc 0x0515 consume_recovery_code(ConsumeRecoveryCodeRequest) -> ConsumeRecoveryCodeResponse;
    }

    // AppService — base 0x0600 — 18 RPCs.
    // Matches `proto/ledger/v1/ledger.proto:2588-2642`.
    service AppService {
        base: 0x0600,
        rpc 0x0600 create_app(CreateAppRequest) -> CreateAppResponse;
        rpc 0x0601 get_app(GetAppRequest) -> GetAppResponse;
        rpc 0x0602 list_apps(ListAppsRequest) -> ListAppsResponse;
        rpc 0x0603 update_app(UpdateAppRequest) -> UpdateAppResponse;
        rpc 0x0604 delete_app(DeleteAppRequest) -> DeleteAppResponse;
        rpc 0x0605 set_app_enabled(SetAppEnabledRequest) -> SetAppEnabledResponse;
        rpc 0x0606 set_app_credential_enabled(SetAppCredentialEnabledRequest) -> SetAppCredentialEnabledResponse;
        rpc 0x0607 get_app_client_secret(GetAppClientSecretRequest) -> GetAppClientSecretResponse;
        rpc 0x0608 rotate_app_client_secret(RotateAppClientSecretRequest) -> RotateAppClientSecretResponse;
        rpc 0x0609 list_app_client_assertions(ListAppClientAssertionsRequest) -> ListAppClientAssertionsResponse;
        rpc 0x060A get_app_client_assertion(GetAppClientAssertionRequest) -> GetAppClientAssertionResponse;
        rpc 0x060B create_app_client_assertion(CreateAppClientAssertionRequest) -> CreateAppClientAssertionResponse;
        rpc 0x060C delete_app_client_assertion(DeleteAppClientAssertionRequest) -> DeleteAppClientAssertionResponse;
        rpc 0x060D set_app_client_assertion_enabled(SetAppClientAssertionEnabledRequest) -> SetAppClientAssertionEnabledResponse;
        rpc 0x060E list_app_vaults(ListAppVaultsRequest) -> ListAppVaultsResponse;
        rpc 0x060F add_app_vault(AddAppVaultRequest) -> AddAppVaultResponse;
        rpc 0x0610 update_app_vault(UpdateAppVaultRequest) -> UpdateAppVaultResponse;
        rpc 0x0611 remove_app_vault(RemoveAppVaultRequest) -> RemoveAppVaultResponse;
    }

    // InvitationService — base 0x0700 — 8 RPCs.
    // Matches `proto/ledger/v1/ledger.proto:2441-2477`.
    service InvitationService {
        base: 0x0700,
        rpc 0x0700 create_organization_invite(CreateOrganizationInviteRequest) -> CreateOrganizationInviteResponse;
        rpc 0x0701 list_organization_invites(ListOrganizationInvitesRequest) -> ListOrganizationInvitesResponse;
        rpc 0x0702 get_organization_invite(GetOrganizationInviteRequest) -> GetOrganizationInviteResponse;
        rpc 0x0703 revoke_organization_invite(RevokeOrganizationInviteRequest) -> RevokeOrganizationInviteResponse;
        rpc 0x0704 list_received_invitations(ListReceivedInvitationsRequest) -> ListReceivedInvitationsResponse;
        rpc 0x0705 get_invitation_details(GetInvitationDetailsRequest) -> GetInvitationDetailsResponse;
        rpc 0x0706 accept_invitation(AcceptInvitationRequest) -> AcceptInvitationResponse;
        rpc 0x0707 decline_invitation(DeclineInvitationRequest) -> DeclineInvitationResponse;
    }

    // TokenService — base 0x0800 — 12 RPCs.
    // Matches `proto/ledger/v1/ledger.proto:2905-2946`. ValidateTokenResponse
    // carries a oneof claims (UserSession / VaultAccess); the postcard codec
    // round-trips it through the Rust enum.
    service TokenService {
        base: 0x0800,
        rpc 0x0800 create_user_session(CreateUserSessionRequest) -> CreateUserSessionResponse;
        rpc 0x0801 validate_token(ValidateTokenRequest) -> ValidateTokenResponse;
        rpc 0x0802 create_vault_token(CreateVaultTokenRequest) -> CreateVaultTokenResponse;
        rpc 0x0803 refresh_token(RefreshTokenRequest) -> RefreshTokenResponse;
        rpc 0x0804 revoke_token(RevokeTokenRequest) -> RevokeTokenResponse;
        rpc 0x0805 revoke_all_user_sessions(RevokeAllUserSessionsRequest) -> RevokeAllUserSessionsResponse;
        rpc 0x0806 revoke_all_app_sessions(RevokeAllAppSessionsRequest) -> RevokeAllAppSessionsResponse;
        rpc 0x0807 create_signing_key(CreateSigningKeyRequest) -> CreateSigningKeyResponse;
        rpc 0x0808 rotate_signing_key(RotateSigningKeyRequest) -> RotateSigningKeyResponse;
        rpc 0x0809 revoke_signing_key(RevokeSigningKeyRequest) -> RevokeSigningKeyResponse;
        rpc 0x080A get_public_keys(GetPublicKeysRequest) -> GetPublicKeysResponse;
        rpc 0x080B authenticate_client_assertion(AuthenticateClientAssertionRequest) -> AuthenticateClientAssertionResponse;
    }

    // EventsService — base 0x0900 — 4 RPCs.
    // Matches `proto/ledger/v1/ledger.proto:3138-3151`.
    service EventsService {
        base: 0x0900,
        rpc 0x0900 list_events(ListEventsRequest) -> ListEventsResponse;
        rpc 0x0901 get_event(GetEventRequest) -> GetEventResponse;
        rpc 0x0902 count_events(CountEventsRequest) -> CountEventsResponse;
        rpc 0x0903 ingest_events(IngestEventsRequest) -> IngestEventsResponse;
    }

    // AdminService — base 0x0A00 — 28 RPCs.
    // Matches `proto/ledger/v1/ledger.proto:954-1116`.
    service AdminService {
        base: 0x0A00,
        rpc 0x0A00 join_cluster(JoinClusterRequest) -> JoinClusterResponse;
        rpc 0x0A01 leave_cluster(LeaveClusterRequest) -> LeaveClusterResponse;
        rpc 0x0A02 get_decommission_status(GetDecommissionStatusRequest) -> GetDecommissionStatusResponse;
        rpc 0x0A03 check_peer_liveness(CheckPeerLivenessRequest) -> CheckPeerLivenessResponse;
        rpc 0x0A04 get_cluster_info(GetClusterInfoRequest) -> GetClusterInfoResponse;
        rpc 0x0A05 get_node_info(GetNodeInfoRequest) -> GetNodeInfoResponse;
        rpc 0x0A06 init_cluster(InitClusterRequest) -> InitClusterResponse;
        rpc 0x0A07 transfer_leadership(TransferLeadershipRequest) -> TransferLeadershipResponse;
        rpc 0x0A08 create_snapshot(CreateSnapshotRequest) -> CreateSnapshotResponse;
        rpc 0x0A09 check_integrity(CheckIntegrityRequest) -> CheckIntegrityResponse;
        rpc 0x0A0A recover_vault(RecoverVaultRequest) -> RecoverVaultResponse;
        rpc 0x0A0B simulate_divergence(SimulateDivergenceRequest) -> SimulateDivergenceResponse;
        rpc 0x0A0C force_gc(ForceGcRequest) -> ForceGcResponse;
        rpc 0x0A0D update_config(UpdateConfigRequest) -> UpdateConfigResponse;
        rpc 0x0A0E get_config(GetConfigRequest) -> GetConfigResponse;
        rpc 0x0A0F create_backup(CreateBackupRequest) -> CreateBackupResponse;
        rpc 0x0A10 list_backups(ListBackupsRequest) -> ListBackupsResponse;
        rpc 0x0A11 restore_backup(RestoreBackupRequest) -> RestoreBackupResponse;
        rpc 0x0A12 rotate_blinding_key(RotateBlindingKeyRequest) -> RotateBlindingKeyResponse;
        rpc 0x0A13 get_blinding_key_rehash_status(GetBlindingKeyRehashStatusRequest) -> GetBlindingKeyRehashStatusResponse;
        rpc 0x0A14 rotate_region_key(RotateRegionKeyRequest) -> RotateRegionKeyResponse;
        rpc 0x0A15 get_rewrap_status(GetRewrapStatusRequest) -> GetRewrapStatusResponse;
        rpc 0x0A16 migrate_existing_users(MigrateExistingUsersRequest) -> MigrateExistingUsersResponse;
        rpc 0x0A17 provision_region(ProvisionRegionRequest) -> ProvisionRegionResponse;
        rpc 0x0A18 set_region_residency(SetRegionResidencyRequest) -> SetRegionResidencyResponse;
        rpc 0x0A19 admin_list_vaults(AdminListVaultsRequest) -> AdminListVaultsResponse;
        rpc 0x0A1A show_vault(ShowVaultRequest) -> ShowVaultResponse;
        rpc 0x0A1B repair_vault(RepairVaultRequest) -> RepairVaultResponse;
    }

    // HealthService — base 0x0B00 — 1 RPC.
    // Matches `proto/ledger/v1/ledger.proto:3372-3374`.
    service HealthService {
        base: 0x0B00,
        rpc 0x0B00 check(HealthCheckRequest) -> HealthCheckResponse;
    }

    // SystemDiscoveryService — base 0x0C00 — 5 RPCs.
    // Matches `proto/ledger/v1/ledger.proto:3398-3417`. WatchLeader is
    // server-streaming.
    service SystemDiscoveryService {
        base: 0x0C00,
        rpc 0x0C00 get_peers(GetPeersRequest) -> GetPeersResponse;
        rpc 0x0C01 announce_peer(AnnouncePeerRequest) -> AnnouncePeerResponse;
        rpc 0x0C02 get_system_state(GetSystemStateRequest) -> GetSystemStateResponse;
        rpc 0x0C03 resolve_region_leader(ResolveRegionLeaderRequest) -> ResolveRegionLeaderResponse;
        rpc 0x0C04 watch_leader(WatchLeaderRequest) -> stream LeaderUpdate;
    }

    // RaftService — base 0x0D00 — 4 RPCs.
    // Matches `proto/ledger/v1/ledger.proto:3515-3556`. Replicate is
    // declared as a unary placeholder; the real bidirectional handler
    // ships in Task E.6b (consensus transport migration). Same for
    // InstallSnapshotStream (client-stream) and Task E.6a (snapshot
    // streaming sub-protocol).
    service RaftService {
        base: 0x0D00,
        rpc 0x0D00 committed_index(CommittedIndexRequest) -> CommittedIndexResponse;
        // TODO(E.6b): real handler is bidirectional — declared unary here
        // because the wire-macro DSL doesn't model bidi today. The custom
        // consensus transport extension provides the real RPC shape.
        rpc 0x0D01 replicate(ConsensusEnvelope) -> ConsensusAck;
        rpc 0x0D02 regional_proposal(RegionalProposalRequest) -> RegionalProposalResult;
        // TODO(E.6a): real handler is client-streaming over the chunked
        // snapshot sub-protocol — declared unary here because the
        // wire-macro DSL doesn't model client-stream today. The header is
        // the placeholder request type for opcode registration purposes.
        rpc 0x0D03 install_snapshot_stream(InstallSnapshotChunk) -> InstallSnapshotStreamResponse;
    }
}
