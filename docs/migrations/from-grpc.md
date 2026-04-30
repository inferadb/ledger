# Migration: gRPC to Wire Protocol

> Tracking document for Phase 0.0.E of the SDK rewrite plan
> (see `docs/superpowers/plans/2026-04-30-region-aware-sdk-multiplexer.md`).

## Overview

The InferaDB Ledger is migrating from tonic / gRPC / HTTP-2 to a custom QUIC-based wire protocol. The foundation crates — `crates/wire/`, `crates/wire-transport/`, `crates/wire-macro/` — landed in Phase 0.0.A through 0.0.D. This document tracks the per-handler migration of every server RPC and every SDK client op as Phase 0.0.E proceeds.

**Status legend:** `planned`, `in-progress`, `done`.

**Current totals**

| Quantity | Count |
|---|---|
| Services | 14 |
| RPCs | 145 |
| Proto messages | 376 |
| Streaming RPCs | 4 |

The counts match the plan estimate exactly.

**Streaming RPC inventory**

| Service | RPC | Pattern |
|---|---|---|
| `ReadService` | `WatchBlocks` | server-stream |
| `SystemDiscoveryService` | `WatchLeader` | server-stream |
| `RaftService` | `Replicate` | bidirectional |
| `RaftService` | `InstallSnapshotStream` | client-stream |

**Quirks worth flagging**

- `SchemaService` (7 RPCs, lines 823 to 844 of `proto/ledger/v1/ledger.proto`) is declared in the proto but has no server-side implementation. The SDK's `crates/sdk/src/ops/schema.rs` implements schema management as a pure client-side convention layered over `ReadService` and `WriteService`, storing JSON blobs at well-known key prefixes. The wire migration must either implement these RPCs server-side or drop them from the protocol. Flagged for resolution before Task E.4.
- `WriteResponse` uses a `oneof` with `success` and `error` variants. The wire-macro codec must round-trip oneof variants correctly; this is the single oneof in a top-level response.
- `ValidateTokenResponse` carries a `oneof claims { UserSessionClaims | VaultAccessClaims }`.
- `InstallSnapshotChunk` carries a `oneof payload { header | data | footer }` with strict ordering enforced by the receiver.
- `RaftService.Replicate` is bidirectional and long-lived (one stream per peer). It is the highest-traffic RPC in the system.
- `RaftService.RegionalProposal` is the only server-side request-forwarding RPC; everything else is redirect-only via `NotLeader` plus `LeaderHint` in `ErrorDetails` (golden rule 11).

## Service Inventory

Each service section lists every RPC declared in `proto/ledger/v1/ledger.proto`, the wire opcode assigned by `crates/wire/src/opcode.rs`, the request and response types verbatim, the streaming pattern, and per-RPC migration status.

### ReadService

**Source:** `proto/ledger/v1/ledger.proto:340-389`
**Implementation:** `crates/services/src/services/read.rs`
**Wire opcode base:** `0x0010` (`READ_SERVICE_BASE`)
**Migration status:** planned

| RPC | Opcode | Request | Response | Streaming | Status |
|---|---|---|---|---|---|
| `Read` | `0x0010` | `ReadRequest` | `ReadResponse` | no | planned |
| `BatchRead` | `0x0011` | `BatchReadRequest` | `BatchReadResponse` | no | planned |
| `VerifiedRead` | `0x0012` | `VerifiedReadRequest` | `VerifiedReadResponse` | no | planned |
| `HistoricalRead` | `0x0013` | `HistoricalReadRequest` | `HistoricalReadResponse` | no | planned |
| `WatchBlocks` | `0x0014` | `WatchBlocksRequest` | `BlockAnnouncement` | server-stream | planned |
| `GetBlock` | `0x0015` | `GetBlockRequest` | `GetBlockResponse` | no | planned |
| `GetBlockRange` | `0x0016` | `GetBlockRangeRequest` | `GetBlockRangeResponse` | no | planned |
| `GetTip` | `0x0017` | `GetTipRequest` | `GetTipResponse` | no | planned |
| `GetClientState` | `0x0018` | `GetClientStateRequest` | `GetClientStateResponse` | no | planned |
| `ListRelationships` | `0x0019` | `ListRelationshipsRequest` | `ListRelationshipsResponse` | no | planned |
| `CheckRelationship` | `0x001A` | `CheckRelationshipRequest` | `CheckRelationshipResponse` | no | planned |
| `ListResources` | `0x001B` | `ListResourcesRequest` | `ListResourcesResponse` | no | planned |
| `ListEntities` | `0x001C` | `ListEntitiesRequest` | `ListEntitiesResponse` | no | planned |

### WriteService

**Source:** `proto/ledger/v1/ledger.proto:631-634`
**Implementation:** `crates/services/src/services/write.rs`
**Wire opcode base:** `0x0100` (`WRITE_SERVICE_BASE`)
**Migration status:** planned

| RPC | Opcode | Request | Response | Streaming | Status |
|---|---|---|---|---|---|
| `Write` | `0x0100` | `WriteRequest` | `WriteResponse` | no | planned |

### OrganizationService

**Source:** `proto/ledger/v1/ledger.proto:732-794`
**Implementation:** `crates/services/src/services/organization.rs`
**Wire opcode base:** `0x0200` (`ORGANIZATION_SERVICE_BASE`)
**Migration status:** planned

| RPC | Opcode | Request | Response | Streaming | Status |
|---|---|---|---|---|---|
| `CreateOrganization` | `0x0200` | `CreateOrganizationRequest` | `CreateOrganizationResponse` | no | planned |
| `DeleteOrganization` | `0x0201` | `DeleteOrganizationRequest` | `DeleteOrganizationResponse` | no | planned |
| `GetOrganization` | `0x0202` | `GetOrganizationRequest` | `GetOrganizationResponse` | no | planned |
| `ListOrganizations` | `0x0203` | `ListOrganizationsRequest` | `ListOrganizationsResponse` | no | planned |
| `MigrateOrganization` | `0x0204` | `MigrateOrganizationRequest` | `MigrateOrganizationResponse` | no | planned |
| `UpdateOrganization` | `0x0205` | `UpdateOrganizationRequest` | `UpdateOrganizationResponse` | no | planned |
| `ListOrganizationMembers` | `0x0206` | `ListOrganizationMembersRequest` | `ListOrganizationMembersResponse` | no | planned |
| `RemoveOrganizationMember` | `0x0207` | `RemoveOrganizationMemberRequest` | `RemoveOrganizationMemberResponse` | no | planned |
| `UpdateOrganizationMemberRole` | `0x0208` | `UpdateOrganizationMemberRoleRequest` | `UpdateOrganizationMemberRoleResponse` | no | planned |
| `ListOrganizationTeams` | `0x0209` | `ListOrganizationTeamsRequest` | `ListOrganizationTeamsResponse` | no | planned |
| `CreateOrganizationTeam` | `0x020A` | `CreateOrganizationTeamRequest` | `CreateOrganizationTeamResponse` | no | planned |
| `DeleteOrganizationTeam` | `0x020B` | `DeleteOrganizationTeamRequest` | `DeleteOrganizationTeamResponse` | no | planned |
| `UpdateOrganizationTeam` | `0x020C` | `UpdateOrganizationTeamRequest` | `UpdateOrganizationTeamResponse` | no | planned |
| `GetOrganizationTeam` | `0x020D` | `GetOrganizationTeamRequest` | `GetOrganizationTeamResponse` | no | planned |
| `AddTeamMember` | `0x020E` | `AddTeamMemberRequest` | `AddTeamMemberResponse` | no | planned |
| `RemoveTeamMember` | `0x020F` | `RemoveTeamMemberRequest` | `RemoveTeamMemberResponse` | no | planned |
| `UpdateTeamMemberRole` | `0x0210` | `UpdateTeamMemberRoleRequest` | `UpdateTeamMemberRoleResponse` | no | planned |

### VaultService

**Source:** `proto/ledger/v1/ledger.proto:800-815`
**Implementation:** `crates/services/src/services/vault.rs`
**Wire opcode base:** `0x0300` (`VAULT_SERVICE_BASE`)
**Migration status:** planned

| RPC | Opcode | Request | Response | Streaming | Status |
|---|---|---|---|---|---|
| `CreateVault` | `0x0300` | `CreateVaultRequest` | `CreateVaultResponse` | no | planned |
| `DeleteVault` | `0x0301` | `DeleteVaultRequest` | `DeleteVaultResponse` | no | planned |
| `GetVault` | `0x0302` | `GetVaultRequest` | `GetVaultResponse` | no | planned |
| `ListVaults` | `0x0303` | `ListVaultsRequest` | `ListVaultsResponse` | no | planned |
| `UpdateVault` | `0x0304` | `UpdateVaultRequest` | `UpdateVaultResponse` | no | planned |

### SchemaService

**Source:** `proto/ledger/v1/ledger.proto:823-844`
**Implementation:** none (declared in proto but unimplemented server-side; SDK implements schema as a layered convention over Read/Write — see `crates/sdk/src/ops/schema.rs`)
**Wire opcode base:** `0x0400` (`SCHEMA_SERVICE_BASE`)
**Migration status:** planned (see Concerns)

| RPC | Opcode | Request | Response | Streaming | Status |
|---|---|---|---|---|---|
| `DeploySchema` | `0x0400` | `DeploySchemaRequest` | `DeploySchemaResponse` | no | planned |
| `ListSchemaVersions` | `0x0401` | `ListSchemaVersionsRequest` | `ListSchemaVersionsResponse` | no | planned |
| `GetSchema` | `0x0402` | `GetSchemaRequest` | `GetSchemaResponse` | no | planned |
| `ActivateSchema` | `0x0403` | `ActivateSchemaRequest` | `ActivateSchemaResponse` | no | planned |
| `RollbackSchema` | `0x0404` | `RollbackSchemaRequest` | `RollbackSchemaResponse` | no | planned |
| `GetActiveSchema` | `0x0405` | `GetActiveSchemaRequest` | `GetActiveSchemaResponse` | no | planned |
| `DiffSchemas` | `0x0406` | `DiffSchemasRequest` | `DiffSchemasResponse` | no | planned |

> Open question: `SchemaService` has no server handler today. Either implement these as real RPCs in Phase 0.0.E, or drop them from the protocol and keep `crates/sdk/src/ops/schema.rs` as the sole entry point. The opcode block is reserved either way.

### UserService

**Source:** `proto/ledger/v1/ledger.proto:2298-2398`
**Implementation:** `crates/services/src/services/user.rs`
**Wire opcode base:** `0x0500` (`USER_SERVICE_BASE`)
**Migration status:** planned

| RPC | Opcode | Request | Response | Streaming | Status |
|---|---|---|---|---|---|
| `CreateUser` | `0x0500` | `CreateUserRequest` | `CreateUserResponse` | no | planned |
| `GetUser` | `0x0501` | `GetUserRequest` | `GetUserResponse` | no | planned |
| `UpdateUser` | `0x0502` | `UpdateUserRequest` | `UpdateUserResponse` | no | planned |
| `DeleteUser` | `0x0503` | `DeleteUserRequest` | `DeleteUserResponse` | no | planned |
| `ListUsers` | `0x0504` | `ListUsersRequest` | `ListUsersResponse` | no | planned |
| `SearchUsers` | `0x0505` | `SearchUsersRequest` | `SearchUsersResponse` | no | planned |
| `CreateUserEmail` | `0x0506` | `CreateUserEmailRequest` | `CreateUserEmailResponse` | no | planned |
| `DeleteUserEmail` | `0x0507` | `DeleteUserEmailRequest` | `DeleteUserEmailResponse` | no | planned |
| `SearchUserEmail` | `0x0508` | `SearchUserEmailRequest` | `SearchUserEmailResponse` | no | planned |
| `VerifyUserEmail` | `0x0509` | `VerifyUserEmailRequest` | `VerifyUserEmailResponse` | no | planned |
| `MigrateUserRegion` | `0x050A` | `MigrateUserRegionRequest` | `MigrateUserRegionResponse` | no | planned |
| `EraseUser` | `0x050B` | `EraseUserRequest` | `EraseUserResponse` | no | planned |
| `InitiateEmailVerification` | `0x050C` | `InitiateEmailVerificationRequest` | `InitiateEmailVerificationResponse` | no | planned |
| `VerifyEmailCode` | `0x050D` | `VerifyEmailCodeRequest` | `VerifyEmailCodeResponse` | no | planned |
| `CompleteRegistration` | `0x050E` | `CompleteRegistrationRequest` | `CompleteRegistrationResponse` | no | planned |
| `CreateUserCredential` | `0x050F` | `CreateUserCredentialRequest` | `CreateUserCredentialResponse` | no | planned |
| `ListUserCredentials` | `0x0510` | `ListUserCredentialsRequest` | `ListUserCredentialsResponse` | no | planned |
| `UpdateUserCredential` | `0x0511` | `UpdateUserCredentialRequest` | `UpdateUserCredentialResponse` | no | planned |
| `DeleteUserCredential` | `0x0512` | `DeleteUserCredentialRequest` | `DeleteUserCredentialResponse` | no | planned |
| `CreateTotpChallenge` | `0x0513` | `CreateTotpChallengeRequest` | `CreateTotpChallengeResponse` | no | planned |
| `VerifyTotp` | `0x0514` | `VerifyTotpRequest` | `VerifyTotpResponse` | no | planned |
| `ConsumeRecoveryCode` | `0x0515` | `ConsumeRecoveryCodeRequest` | `ConsumeRecoveryCodeResponse` | no | planned |

### AppService

**Source:** `proto/ledger/v1/ledger.proto:2588-2642`
**Implementation:** `crates/services/src/services/app.rs`
**Wire opcode base:** `0x0600` (`APP_SERVICE_BASE`)
**Migration status:** planned

| RPC | Opcode | Request | Response | Streaming | Status |
|---|---|---|---|---|---|
| `CreateApp` | `0x0600` | `CreateAppRequest` | `CreateAppResponse` | no | planned |
| `GetApp` | `0x0601` | `GetAppRequest` | `GetAppResponse` | no | planned |
| `ListApps` | `0x0602` | `ListAppsRequest` | `ListAppsResponse` | no | planned |
| `UpdateApp` | `0x0603` | `UpdateAppRequest` | `UpdateAppResponse` | no | planned |
| `DeleteApp` | `0x0604` | `DeleteAppRequest` | `DeleteAppResponse` | no | planned |
| `SetAppEnabled` | `0x0605` | `SetAppEnabledRequest` | `SetAppEnabledResponse` | no | planned |
| `SetAppCredentialEnabled` | `0x0606` | `SetAppCredentialEnabledRequest` | `SetAppCredentialEnabledResponse` | no | planned |
| `GetAppClientSecret` | `0x0607` | `GetAppClientSecretRequest` | `GetAppClientSecretResponse` | no | planned |
| `RotateAppClientSecret` | `0x0608` | `RotateAppClientSecretRequest` | `RotateAppClientSecretResponse` | no | planned |
| `ListAppClientAssertions` | `0x0609` | `ListAppClientAssertionsRequest` | `ListAppClientAssertionsResponse` | no | planned |
| `GetAppClientAssertion` | `0x060A` | `GetAppClientAssertionRequest` | `GetAppClientAssertionResponse` | no | planned |
| `CreateAppClientAssertion` | `0x060B` | `CreateAppClientAssertionRequest` | `CreateAppClientAssertionResponse` | no | planned |
| `DeleteAppClientAssertion` | `0x060C` | `DeleteAppClientAssertionRequest` | `DeleteAppClientAssertionResponse` | no | planned |
| `SetAppClientAssertionEnabled` | `0x060D` | `SetAppClientAssertionEnabledRequest` | `SetAppClientAssertionEnabledResponse` | no | planned |
| `ListAppVaults` | `0x060E` | `ListAppVaultsRequest` | `ListAppVaultsResponse` | no | planned |
| `AddAppVault` | `0x060F` | `AddAppVaultRequest` | `AddAppVaultResponse` | no | planned |
| `UpdateAppVault` | `0x0610` | `UpdateAppVaultRequest` | `UpdateAppVaultResponse` | no | planned |
| `RemoveAppVault` | `0x0611` | `RemoveAppVaultRequest` | `RemoveAppVaultResponse` | no | planned |

### InvitationService

**Source:** `proto/ledger/v1/ledger.proto:2441-2477`
**Implementation:** `crates/services/src/services/invitation.rs`
**Wire opcode base:** `0x0700` (`INVITATION_SERVICE_BASE`)
**Migration status:** planned

| RPC | Opcode | Request | Response | Streaming | Status |
|---|---|---|---|---|---|
| `CreateOrganizationInvite` | `0x0700` | `CreateOrganizationInviteRequest` | `CreateOrganizationInviteResponse` | no | planned |
| `ListOrganizationInvites` | `0x0701` | `ListOrganizationInvitesRequest` | `ListOrganizationInvitesResponse` | no | planned |
| `GetOrganizationInvite` | `0x0702` | `GetOrganizationInviteRequest` | `GetOrganizationInviteResponse` | no | planned |
| `RevokeOrganizationInvite` | `0x0703` | `RevokeOrganizationInviteRequest` | `RevokeOrganizationInviteResponse` | no | planned |
| `ListReceivedInvitations` | `0x0704` | `ListReceivedInvitationsRequest` | `ListReceivedInvitationsResponse` | no | planned |
| `GetInvitationDetails` | `0x0705` | `GetInvitationDetailsRequest` | `GetInvitationDetailsResponse` | no | planned |
| `AcceptInvitation` | `0x0706` | `AcceptInvitationRequest` | `AcceptInvitationResponse` | no | planned |
| `DeclineInvitation` | `0x0707` | `DeclineInvitationRequest` | `DeclineInvitationResponse` | no | planned |

### TokenService

**Source:** `proto/ledger/v1/ledger.proto:2905-2946`
**Implementation:** `crates/services/src/services/token.rs`
**Wire opcode base:** `0x0800` (`TOKEN_SERVICE_BASE`)
**Migration status:** planned

| RPC | Opcode | Request | Response | Streaming | Status |
|---|---|---|---|---|---|
| `CreateUserSession` | `0x0800` | `CreateUserSessionRequest` | `CreateUserSessionResponse` | no | planned |
| `ValidateToken` | `0x0801` | `ValidateTokenRequest` | `ValidateTokenResponse` | no | planned |
| `CreateVaultToken` | `0x0802` | `CreateVaultTokenRequest` | `CreateVaultTokenResponse` | no | planned |
| `RefreshToken` | `0x0803` | `RefreshTokenRequest` | `RefreshTokenResponse` | no | planned |
| `RevokeToken` | `0x0804` | `RevokeTokenRequest` | `RevokeTokenResponse` | no | planned |
| `RevokeAllUserSessions` | `0x0805` | `RevokeAllUserSessionsRequest` | `RevokeAllUserSessionsResponse` | no | planned |
| `RevokeAllAppSessions` | `0x0806` | `RevokeAllAppSessionsRequest` | `RevokeAllAppSessionsResponse` | no | planned |
| `CreateSigningKey` | `0x0807` | `CreateSigningKeyRequest` | `CreateSigningKeyResponse` | no | planned |
| `RotateSigningKey` | `0x0808` | `RotateSigningKeyRequest` | `RotateSigningKeyResponse` | no | planned |
| `RevokeSigningKey` | `0x0809` | `RevokeSigningKeyRequest` | `RevokeSigningKeyResponse` | no | planned |
| `GetPublicKeys` | `0x080A` | `GetPublicKeysRequest` | `GetPublicKeysResponse` | no | planned |
| `AuthenticateClientAssertion` | `0x080B` | `AuthenticateClientAssertionRequest` | `AuthenticateClientAssertionResponse` | no | planned |

### EventsService

**Source:** `proto/ledger/v1/ledger.proto:3138-3151`
**Implementation:** `crates/services/src/services/events.rs`
**Wire opcode base:** `0x0900` (`EVENTS_SERVICE_BASE`)
**Migration status:** planned

| RPC | Opcode | Request | Response | Streaming | Status |
|---|---|---|---|---|---|
| `ListEvents` | `0x0900` | `ListEventsRequest` | `ListEventsResponse` | no | planned |
| `GetEvent` | `0x0901` | `GetEventRequest` | `GetEventResponse` | no | planned |
| `CountEvents` | `0x0902` | `CountEventsRequest` | `CountEventsResponse` | no | planned |
| `IngestEvents` | `0x0903` | `IngestEventsRequest` | `IngestEventsResponse` | no | planned |

### AdminService

**Source:** `proto/ledger/v1/ledger.proto:954-1116`
**Implementation:** `crates/services/src/services/admin.rs`
**Wire opcode base:** `0x0A00` (`ADMIN_SERVICE_BASE`)
**Migration status:** planned

| RPC | Opcode | Request | Response | Streaming | Status |
|---|---|---|---|---|---|
| `JoinCluster` | `0x0A00` | `JoinClusterRequest` | `JoinClusterResponse` | no | planned |
| `LeaveCluster` | `0x0A01` | `LeaveClusterRequest` | `LeaveClusterResponse` | no | planned |
| `GetDecommissionStatus` | `0x0A02` | `GetDecommissionStatusRequest` | `GetDecommissionStatusResponse` | no | planned |
| `CheckPeerLiveness` | `0x0A03` | `CheckPeerLivenessRequest` | `CheckPeerLivenessResponse` | no | planned |
| `GetClusterInfo` | `0x0A04` | `GetClusterInfoRequest` | `GetClusterInfoResponse` | no | planned |
| `GetNodeInfo` | `0x0A05` | `GetNodeInfoRequest` | `GetNodeInfoResponse` | no | planned |
| `InitCluster` | `0x0A06` | `InitClusterRequest` | `InitClusterResponse` | no | planned |
| `TransferLeadership` | `0x0A07` | `TransferLeadershipRequest` | `TransferLeadershipResponse` | no | planned |
| `CreateSnapshot` | `0x0A08` | `CreateSnapshotRequest` | `CreateSnapshotResponse` | no | planned |
| `CheckIntegrity` | `0x0A09` | `CheckIntegrityRequest` | `CheckIntegrityResponse` | no | planned |
| `RecoverVault` | `0x0A0A` | `RecoverVaultRequest` | `RecoverVaultResponse` | no | planned |
| `SimulateDivergence` | `0x0A0B` | `SimulateDivergenceRequest` | `SimulateDivergenceResponse` | no | planned |
| `ForceGc` | `0x0A0C` | `ForceGcRequest` | `ForceGcResponse` | no | planned |
| `UpdateConfig` | `0x0A0D` | `UpdateConfigRequest` | `UpdateConfigResponse` | no | planned |
| `GetConfig` | `0x0A0E` | `GetConfigRequest` | `GetConfigResponse` | no | planned |
| `CreateBackup` | `0x0A0F` | `CreateBackupRequest` | `CreateBackupResponse` | no | planned |
| `ListBackups` | `0x0A10` | `ListBackupsRequest` | `ListBackupsResponse` | no | planned |
| `RestoreBackup` | `0x0A11` | `RestoreBackupRequest` | `RestoreBackupResponse` | no | planned |
| `RotateBlindingKey` | `0x0A12` | `RotateBlindingKeyRequest` | `RotateBlindingKeyResponse` | no | planned |
| `GetBlindingKeyRehashStatus` | `0x0A13` | `GetBlindingKeyRehashStatusRequest` | `GetBlindingKeyRehashStatusResponse` | no | planned |
| `RotateRegionKey` | `0x0A14` | `RotateRegionKeyRequest` | `RotateRegionKeyResponse` | no | planned |
| `GetRewrapStatus` | `0x0A15` | `GetRewrapStatusRequest` | `GetRewrapStatusResponse` | no | planned |
| `MigrateExistingUsers` | `0x0A16` | `MigrateExistingUsersRequest` | `MigrateExistingUsersResponse` | no | planned |
| `ProvisionRegion` | `0x0A17` | `ProvisionRegionRequest` | `ProvisionRegionResponse` | no | planned |
| `SetRegionResidency` | `0x0A18` | `SetRegionResidencyRequest` | `SetRegionResidencyResponse` | no | planned |
| `AdminListVaults` | `0x0A19` | `AdminListVaultsRequest` | `AdminListVaultsResponse` | no | planned |
| `ShowVault` | `0x0A1A` | `ShowVaultRequest` | `ShowVaultResponse` | no | planned |
| `RepairVault` | `0x0A1B` | `RepairVaultRequest` | `RepairVaultResponse` | no | planned |

### HealthService

**Source:** `proto/ledger/v1/ledger.proto:3372-3374`
**Implementation:** `crates/services/src/services/health.rs`
**Wire opcode base:** `0x0B00` (`HEALTH_SERVICE_BASE`)
**Migration status:** planned

| RPC | Opcode | Request | Response | Streaming | Status |
|---|---|---|---|---|---|
| `Check` | `0x0B00` | `HealthCheckRequest` | `HealthCheckResponse` | no | planned |

### SystemDiscoveryService

**Source:** `proto/ledger/v1/ledger.proto:3398-3417`
**Implementation:** `crates/services/src/services/discovery.rs` (the in-tree service file is named `discovery.rs`; the proto service is `SystemDiscoveryService`)
**Wire opcode base:** `0x0C00` (`SYSTEM_DISCOVERY_SERVICE_BASE`)
**Migration status:** planned

| RPC | Opcode | Request | Response | Streaming | Status |
|---|---|---|---|---|---|
| `GetPeers` | `0x0C00` | `GetPeersRequest` | `GetPeersResponse` | no | planned |
| `AnnouncePeer` | `0x0C01` | `AnnouncePeerRequest` | `AnnouncePeerResponse` | no | planned |
| `GetSystemState` | `0x0C02` | `GetSystemStateRequest` | `GetSystemStateResponse` | no | planned |
| `ResolveRegionLeader` | `0x0C03` | `ResolveRegionLeaderRequest` | `ResolveRegionLeaderResponse` | no | planned |
| `WatchLeader` | `0x0C04` | `WatchLeaderRequest` | `LeaderUpdate` | server-stream | planned |

### RaftService

**Source:** `proto/ledger/v1/ledger.proto:3515-3556`
**Implementation:** `crates/services/src/services/raft.rs`
**Wire opcode base:** `0x0D00` (`RAFT_CONSENSUS_SERVICE_BASE`)
**Migration status:** planned (Tasks E.6a, E.6b, E.6c — data-durability critical)

| RPC | Opcode | Request | Response | Streaming | Status |
|---|---|---|---|---|---|
| `CommittedIndex` | `0x0D00` | `CommittedIndexRequest` | `CommittedIndexResponse` | no | planned |
| `Replicate` | `0x0D01` | `ConsensusEnvelope` | `ConsensusAck` | bidirectional | planned |
| `RegionalProposal` | `0x0D02` | `RegionalProposalRequest` | `RegionalProposalResult` | no | planned |
| `InstallSnapshotStream` | `0x0D03` | `InstallSnapshotChunk` | `InstallSnapshotStreamResponse` | client-stream | planned |

> Note: `crates/wire/src/opcode.rs` reserves the Raft block as `RAFT_CONSENSUS_SERVICE_BASE` (`0x0D00`). The proto service is named `RaftService`; the on-disk implementation file is `crates/services/src/services/raft.rs`. The `MAINTENANCE_SERVICE_BASE` (`0x0E00`) and `LEASE_SERVICE_BASE` (`0x0F00`) blocks are reserved in `opcode.rs` but no proto service currently maps to them. The reservations remain in place for forward compatibility.

## Reserved Opcode Blocks

The full opcode allocation table from `crates/wire/src/opcode.rs`:

```text
0x0000-0x000F  Reserved (protocol-level: handshake, ping, auth, reflect)
0x0010-0x00FF  ReadService
0x0100-0x01FF  WriteService
0x0200-0x02FF  OrganizationService
0x0300-0x03FF  VaultService
0x0400-0x04FF  SchemaService
0x0500-0x05FF  UserService
0x0600-0x06FF  AppService
0x0700-0x07FF  InvitationService
0x0800-0x08FF  TokenService
0x0900-0x09FF  EventsService
0x0A00-0x0AFF  AdminService
0x0B00-0x0BFF  HealthService
0x0C00-0x0CFF  SystemDiscoveryService
0x0D00-0x0DFF  RaftService (a.k.a. Raft Consensus)
0x0E00-0x0EFF  Reserved: Maintenance (no proto service yet)
0x0F00-0x0FFF  Reserved: Lease (no proto service yet)
```

`OPCODE_SPACE_END = 0x1000`. New service blocks must update that constant.

## Message Types

Brief inventory by service module. Each service section lists messages owned by that service's RPCs (request and response types plus the immediate building blocks that are not used cross-service). Cross-cutting types live in **Shared**.

### Shared (used by multiple services)

Foundational types declared early in the proto file (`proto/ledger/v1/ledger.proto:20-332`):

- `Hash`
- `OrganizationSlug`, `VaultSlug`, `UserSlug`, `TeamSlug`, `AppSlug`, `InviteSlug`, `ClientAssertionId`
- `NodeId`, `UserId`, `UserEmailId`, `EmailVerifyTokenId`
- `User`, `UserEmail`, `EmailVerificationToken`
- `ClientId`, `TxId`
- `BlockHeader`, `Block`, `BlockAnnouncement`, `Transaction`
- `Operation`, `CreateRelationship`, `DeleteRelationship`, `SetEntity`, `SetCondition`, `DeleteEntity`, `ExpireEntity`
- `Relationship`, `Entity`
- `MerkleProof`, `MerkleSibling`, `ChainProof`, `StateProof`
- `ErrorDetails` (the structured error payload attached to every error response — see golden rule 12)

### Read service messages

- `ReadRequest`, `ReadResponse`
- `BatchReadRequest`, `BatchReadResponse`, `BatchReadResult`
- `VerifiedReadRequest`, `VerifiedReadResponse`
- `HistoricalReadRequest`, `HistoricalReadResponse`
- `WatchBlocksRequest`
- `GetBlockRequest`, `GetBlockResponse`
- `GetBlockRangeRequest`, `GetBlockRangeResponse`
- `GetTipRequest`, `GetTipResponse`
- `GetClientStateRequest`, `GetClientStateResponse`
- `ListRelationshipsRequest`, `ListRelationshipsResponse`
- `CheckRelationshipRequest`, `CheckRelationshipResponse`
- `ListResourcesRequest`, `ListResourcesResponse`
- `ListEntitiesRequest`, `ListEntitiesResponse`

### Write service messages

- `WriteRequest`, `WriteResponse`
- `WriteSuccess`, `WriteError` (the `oneof` arms of `WriteResponse`)

### Organization service messages

- `CreateOrganizationRequest`, `CreateOrganizationResponse`
- `DeleteOrganizationRequest`, `DeleteOrganizationResponse`
- `GetOrganizationRequest`, `GetOrganizationResponse`
- `ListOrganizationsRequest`, `ListOrganizationsResponse`
- `MigrateOrganizationRequest`, `MigrateOrganizationResponse`
- `UpdateOrganizationRequest`, `UpdateOrganizationResponse`
- `OrganizationMember`
- `ListOrganizationMembersRequest`, `ListOrganizationMembersResponse`
- `RemoveOrganizationMemberRequest`, `RemoveOrganizationMemberResponse`
- `UpdateOrganizationMemberRoleRequest`, `UpdateOrganizationMemberRoleResponse`
- `OrganizationTeamMember`, `OrganizationTeam`
- `ListOrganizationTeamsRequest`, `ListOrganizationTeamsResponse`
- `CreateOrganizationTeamRequest`, `CreateOrganizationTeamResponse`
- `DeleteOrganizationTeamRequest`, `DeleteOrganizationTeamResponse`
- `UpdateOrganizationTeamRequest`, `UpdateOrganizationTeamResponse`
- `GetOrganizationTeamRequest`, `GetOrganizationTeamResponse`
- `AddTeamMemberRequest`, `AddTeamMemberResponse`
- `RemoveTeamMemberRequest`, `RemoveTeamMemberResponse`
- `UpdateTeamMemberRoleRequest`, `UpdateTeamMemberRoleResponse`

### Vault service messages

- `CreateVaultRequest`, `CreateVaultResponse`
- `DeleteVaultRequest`, `DeleteVaultResponse`
- `GetVaultRequest`, `GetVaultResponse`
- `ListVaultsRequest`, `ListVaultsResponse`
- `UpdateVaultRequest`, `UpdateVaultResponse`
- `BlockRetentionPolicy`

### Schema service messages

- `DeploySchemaRequest`, `DeploySchemaResponse`
- `ListSchemaVersionsRequest`, `ListSchemaVersionsResponse`, `SchemaVersionEntry`
- `GetSchemaRequest`, `GetSchemaResponse`
- `ActivateSchemaRequest`, `ActivateSchemaResponse`
- `RollbackSchemaRequest`, `RollbackSchemaResponse`
- `GetActiveSchemaRequest`, `GetActiveSchemaResponse`
- `DiffSchemasRequest`, `DiffSchemasResponse`, `DiffFieldChange`

### User service messages

- `CreateUserRequest`, `CreateUserResponse`
- `GetUserRequest`, `GetUserResponse`
- `UpdateUserRequest`, `UpdateUserResponse`
- `DeleteUserRequest`, `DeleteUserResponse`
- `ListUsersRequest`, `ListUsersResponse`
- `UserSearchFilter`, `SearchUsersRequest`, `SearchUsersResponse`
- `CreateUserEmailRequest`, `CreateUserEmailResponse`
- `DeleteUserEmailRequest`, `DeleteUserEmailResponse`
- `UserEmailSearchFilter`, `SearchUserEmailRequest`, `SearchUserEmailResponse`
- `VerifyUserEmailRequest`, `VerifyUserEmailResponse`
- `MigrateUserRegionRequest`, `MigrateUserRegionResponse`
- `EraseUserRequest`, `EraseUserResponse`
- `InitiateEmailVerificationRequest`, `InitiateEmailVerificationResponse`
- `VerifyEmailCodeRequest`, `VerifyEmailCodeResponse`, `ExistingUserSession`, `OnboardingSession`
- `CompleteRegistrationRequest`, `CompleteRegistrationResponse`
- `UserCredential`, `PasskeyCredentialData`, `TotpCredentialData`, `RecoveryCodeCredentialData`
- `CredentialInfo`, `TotpRequired`
- `CreateUserCredentialRequest`, `CreateUserCredentialResponse`
- `ListUserCredentialsRequest`, `ListUserCredentialsResponse`
- `UpdateUserCredentialRequest`, `UpdateUserCredentialResponse`
- `DeleteUserCredentialRequest`, `DeleteUserCredentialResponse`
- `CreateTotpChallengeRequest`, `CreateTotpChallengeResponse`
- `VerifyTotpRequest`, `VerifyTotpResponse`
- `ConsumeRecoveryCodeRequest`, `ConsumeRecoveryCodeResponse`

### App service messages

- `AppInfo`, `AppCredentialsInfo`, `AppClientAssertionInfo`, `AppVaultConnectionInfo`
- `CreateAppRequest`, `CreateAppResponse`
- `GetAppRequest`, `GetAppResponse`
- `ListAppsRequest`, `ListAppsResponse`
- `UpdateAppRequest`, `UpdateAppResponse`
- `DeleteAppRequest`, `DeleteAppResponse`
- `SetAppEnabledRequest`, `SetAppEnabledResponse`
- `SetAppCredentialEnabledRequest`, `SetAppCredentialEnabledResponse`
- `GetAppClientSecretRequest`, `GetAppClientSecretResponse`
- `RotateAppClientSecretRequest`, `RotateAppClientSecretResponse`
- `ListAppClientAssertionsRequest`, `ListAppClientAssertionsResponse`
- `GetAppClientAssertionRequest`, `GetAppClientAssertionResponse`
- `CreateAppClientAssertionRequest`, `CreateAppClientAssertionResponse`
- `DeleteAppClientAssertionRequest`, `DeleteAppClientAssertionResponse`
- `SetAppClientAssertionEnabledRequest`, `SetAppClientAssertionEnabledResponse`
- `ListAppVaultsRequest`, `ListAppVaultsResponse`
- `AddAppVaultRequest`, `AddAppVaultResponse`
- `UpdateAppVaultRequest`, `UpdateAppVaultResponse`
- `RemoveAppVaultRequest`, `RemoveAppVaultResponse`

### Invitation service messages

- `Invitation`
- `CreateOrganizationInviteRequest`, `CreateOrganizationInviteResponse`
- `ListOrganizationInvitesRequest`, `ListOrganizationInvitesResponse`
- `GetOrganizationInviteRequest`, `GetOrganizationInviteResponse`
- `RevokeOrganizationInviteRequest`, `RevokeOrganizationInviteResponse`
- `ListReceivedInvitationsRequest`, `ListReceivedInvitationsResponse`
- `GetInvitationDetailsRequest`, `GetInvitationDetailsResponse`
- `AcceptInvitationRequest`, `AcceptInvitationResponse`
- `DeclineInvitationRequest`, `DeclineInvitationResponse`

### Token service messages

- `TokenPair`, `PublicKeyInfo`
- `CreateUserSessionRequest`, `CreateUserSessionResponse`
- `ValidateTokenRequest`, `ValidateTokenResponse`, `UserSessionClaims`, `VaultAccessClaims`
- `CreateVaultTokenRequest`, `CreateVaultTokenResponse`
- `AuthenticateClientAssertionRequest`, `AuthenticateClientAssertionResponse`
- `RefreshTokenRequest`, `RefreshTokenResponse`
- `RevokeTokenRequest`, `RevokeTokenResponse`
- `RevokeAllUserSessionsRequest`, `RevokeAllUserSessionsResponse`
- `RevokeAllAppSessionsRequest`, `RevokeAllAppSessionsResponse`
- `CreateSigningKeyRequest`, `CreateSigningKeyResponse`
- `RotateSigningKeyRequest`, `RotateSigningKeyResponse`
- `RevokeSigningKeyRequest`, `RevokeSigningKeyResponse`
- `GetPublicKeysRequest`, `GetPublicKeysResponse`

### Events service messages

- `EventEntry`, `EventFilter`
- `ListEventsRequest`, `ListEventsResponse`
- `GetEventRequest`, `GetEventResponse`
- `CountEventsRequest`, `CountEventsResponse`
- `IngestEventEntry`, `IngestEventsRequest`, `IngestEventsResponse`, `RejectedEvent`

### Admin service messages

- `JoinClusterRequest`, `JoinClusterResponse`
- `LeaveClusterRequest`, `LeaveClusterResponse`
- `GetDecommissionStatusRequest`, `GetDecommissionStatusResponse`, `DataRegionReplica`
- `CheckPeerLivenessRequest`, `CheckPeerLivenessResponse`
- `GetClusterInfoRequest`, `GetClusterInfoResponse`, `ClusterMember`
- `GetNodeInfoRequest`, `GetNodeInfoResponse`
- `InitClusterRequest`, `InitClusterResponse`
- `TransferLeadershipRequest`, `TransferLeadershipResponse`
- `CreateSnapshotRequest`, `CreateSnapshotResponse`
- `CheckIntegrityRequest`, `CheckIntegrityResponse`, `IntegrityIssue`
- `RecoverVaultRequest`, `RecoverVaultResponse`
- `SimulateDivergenceRequest`, `SimulateDivergenceResponse`
- `ForceGcRequest`, `ForceGcResponse`
- `UpdateConfigRequest`, `UpdateConfigResponse`
- `GetConfigRequest`, `GetConfigResponse`
- `BackupDbEntry`, `BackupManifest`, `BackupInfo`
- `CreateBackupRequest`, `CreateBackupResponse`
- `ListBackupsRequest`, `ListBackupsResponse`
- `RestoreBackupRequest`, `RestoreBackupResponse`
- `RotateBlindingKeyRequest`, `RotateBlindingKeyResponse`
- `GetBlindingKeyRehashStatusRequest`, `GetBlindingKeyRehashStatusResponse`
- `RotateRegionKeyRequest`, `RotateRegionKeyResponse`
- `GetRewrapStatusRequest`, `GetRewrapStatusResponse`
- `MigrateExistingUsersRequest`, `MigrateExistingUsersResponse`
- `ProvisionRegionRequest`, `ProvisionRegionResponse`, `RegionDirectoryEntry`
- `SetRegionResidencyRequest`, `SetRegionResidencyResponse`
- `AdminVaultInfo`, `AdminListVaultsRequest`, `AdminListVaultsResponse`
- `ShowVaultRequest`, `ShowVaultResponse`
- `RepairVaultRequest`, `RepairVaultResponse`

### Health service messages

- `HealthCheckRequest`, `HealthCheckResponse`

### System Discovery service messages

- `GetPeersRequest`, `GetPeersResponse`, `PeerInfo`
- `AnnouncePeerRequest`, `AnnouncePeerResponse`
- `GetSystemStateRequest`, `GetSystemStateResponse`, `NodeInfo`, `OrganizationRegistry`
- `ResolveRegionLeaderRequest`, `ResolveRegionLeaderResponse`
- `WatchLeaderRequest`, `LeaderUpdate`

### Raft service messages

- `CommittedIndexRequest`, `CommittedIndexResponse`
- `ConsensusEnvelope`, `ConsensusAck`
- `RegionalProposalRequest`, `RegionalProposalResult`
- `InstallSnapshotChunk`, `InstallSnapshotHeader`, `InstallSnapshotOrgScope`, `InstallSnapshotVaultScope`, `InstallSnapshotFooter`, `InstallSnapshotStreamResponse`
- `RaftVote`, `RaftLogId`, `RaftSnapshotMeta`, `RaftMembership`, `RaftMembershipConfig`

## SDK Op Inventory

Files under `crates/sdk/src/ops/` map to the proto services they dispatch into.

| File | Service(s) used | Status |
|---|---|---|
| `crates/sdk/src/ops/app.rs` | AppService | planned |
| `crates/sdk/src/ops/credential.rs` | UserService (credential and TOTP RPCs) | planned |
| `crates/sdk/src/ops/data.rs` | ReadService, WriteService | planned |
| `crates/sdk/src/ops/events.rs` | EventsService | planned |
| `crates/sdk/src/ops/health.rs` | HealthService | planned |
| `crates/sdk/src/ops/invitation.rs` | InvitationService | planned |
| `crates/sdk/src/ops/list.rs` | ReadService (`ListEntities`, `ListRelationships`, `ListResources`) | planned |
| `crates/sdk/src/ops/onboarding.rs` | UserService (`InitiateEmailVerification`, `VerifyEmailCode`, `CompleteRegistration`) | planned |
| `crates/sdk/src/ops/organization.rs` | OrganizationService, AdminService (admin migration helpers) | planned |
| `crates/sdk/src/ops/relationship.rs` | ReadService (`CheckRelationship`) | planned |
| `crates/sdk/src/ops/schema.rs` | ReadService, WriteService (no `SchemaService` calls — pure layered convention) | planned |
| `crates/sdk/src/ops/token.rs` | TokenService | planned |
| `crates/sdk/src/ops/user.rs` | UserService, AdminService (`RotateBlindingKey`, `GetBlindingKeyRehashStatus`) | planned |
| `crates/sdk/src/ops/vault.rs` | VaultService | planned |
| `crates/sdk/src/ops/verified_read.rs` | ReadService (`VerifiedRead`, `HistoricalRead`) | planned |

## Migration Plan

1. **Foundation (additive, no breakage):** Tasks E.2 and E.3 — server-side `wire-transport` listener and SDK transport selector.
2. **Server-side switchover (breaking, parallel):** Task E.4 — convert every handler in `crates/services/src/services/*.rs` to a wire-protocol handler.
3. **Client-side switchover (breaking, parallel):** Task E.5 — convert every op in `crates/sdk/src/ops/*.rs` to dispatch through the wire transport.
4. **Raft transport switchover (breaking; data-durability critical):** Tasks E.6a, E.6b, E.6c — flip Raft inter-node transport from gRPC to wire, with explicit cluster-coordination protocol.
5. **Test cluster switchover:** Task E.7 — server integration binary moves over.
6. **K8s probes plus manifest:** Tasks E.8 and E.9 — readiness, liveness, and startup probes plus rollout manifest.
7. **Cleanup:** Tasks E.10, E.12, F.1, F.2, F.3 — remove tonic, drop generated proto code, retire `crates/proto/src/generated/` from runtime hot paths.

## Rolling Upgrade

> Placeholder — Task E.6b populates this with the cluster-coordination protocol and operator runbook for live Raft-transport rollover.

## K8s Rollout

> Placeholder — Task E.9 populates this with the manifest changes (probe paths, port surface, ALPN advertising) and the rollout sequence.

## Metrics and Alerting

> Placeholder — Task F.4 populates this with the dual-emit metric set during the migration window and the cutover alerts.

## Per-Handler Migration Status

Flat table of all 145 RPCs for atomic status tracking during migration.

| Service | RPC | Opcode | Status | Notes |
|---|---|---|---|---|
| ReadService | Read | 0x0010 | planned | |
| ReadService | BatchRead | 0x0011 | planned | |
| ReadService | VerifiedRead | 0x0012 | planned | |
| ReadService | HistoricalRead | 0x0013 | planned | |
| ReadService | WatchBlocks | 0x0014 | planned | server-stream |
| ReadService | GetBlock | 0x0015 | planned | |
| ReadService | GetBlockRange | 0x0016 | planned | |
| ReadService | GetTip | 0x0017 | planned | |
| ReadService | GetClientState | 0x0018 | planned | |
| ReadService | ListRelationships | 0x0019 | planned | |
| ReadService | CheckRelationship | 0x001A | planned | |
| ReadService | ListResources | 0x001B | planned | |
| ReadService | ListEntities | 0x001C | planned | |
| WriteService | Write | 0x0100 | planned | response is a `oneof` |
| OrganizationService | CreateOrganization | 0x0200 | planned | |
| OrganizationService | DeleteOrganization | 0x0201 | planned | |
| OrganizationService | GetOrganization | 0x0202 | planned | |
| OrganizationService | ListOrganizations | 0x0203 | planned | |
| OrganizationService | MigrateOrganization | 0x0204 | planned | |
| OrganizationService | UpdateOrganization | 0x0205 | planned | |
| OrganizationService | ListOrganizationMembers | 0x0206 | planned | |
| OrganizationService | RemoveOrganizationMember | 0x0207 | planned | |
| OrganizationService | UpdateOrganizationMemberRole | 0x0208 | planned | |
| OrganizationService | ListOrganizationTeams | 0x0209 | planned | |
| OrganizationService | CreateOrganizationTeam | 0x020A | planned | |
| OrganizationService | DeleteOrganizationTeam | 0x020B | planned | |
| OrganizationService | UpdateOrganizationTeam | 0x020C | planned | |
| OrganizationService | GetOrganizationTeam | 0x020D | planned | |
| OrganizationService | AddTeamMember | 0x020E | planned | |
| OrganizationService | RemoveTeamMember | 0x020F | planned | |
| OrganizationService | UpdateTeamMemberRole | 0x0210 | planned | |
| VaultService | CreateVault | 0x0300 | planned | |
| VaultService | DeleteVault | 0x0301 | planned | |
| VaultService | GetVault | 0x0302 | planned | |
| VaultService | ListVaults | 0x0303 | planned | |
| VaultService | UpdateVault | 0x0304 | planned | |
| SchemaService | DeploySchema | 0x0400 | planned | no server-side handler today |
| SchemaService | ListSchemaVersions | 0x0401 | planned | no server-side handler today |
| SchemaService | GetSchema | 0x0402 | planned | no server-side handler today |
| SchemaService | ActivateSchema | 0x0403 | planned | no server-side handler today |
| SchemaService | RollbackSchema | 0x0404 | planned | no server-side handler today |
| SchemaService | GetActiveSchema | 0x0405 | planned | no server-side handler today |
| SchemaService | DiffSchemas | 0x0406 | planned | no server-side handler today |
| UserService | CreateUser | 0x0500 | planned | |
| UserService | GetUser | 0x0501 | planned | |
| UserService | UpdateUser | 0x0502 | planned | |
| UserService | DeleteUser | 0x0503 | planned | |
| UserService | ListUsers | 0x0504 | planned | |
| UserService | SearchUsers | 0x0505 | planned | |
| UserService | CreateUserEmail | 0x0506 | planned | |
| UserService | DeleteUserEmail | 0x0507 | planned | |
| UserService | SearchUserEmail | 0x0508 | planned | |
| UserService | VerifyUserEmail | 0x0509 | planned | |
| UserService | MigrateUserRegion | 0x050A | planned | |
| UserService | EraseUser | 0x050B | planned | |
| UserService | InitiateEmailVerification | 0x050C | planned | |
| UserService | VerifyEmailCode | 0x050D | planned | |
| UserService | CompleteRegistration | 0x050E | planned | |
| UserService | CreateUserCredential | 0x050F | planned | |
| UserService | ListUserCredentials | 0x0510 | planned | |
| UserService | UpdateUserCredential | 0x0511 | planned | |
| UserService | DeleteUserCredential | 0x0512 | planned | |
| UserService | CreateTotpChallenge | 0x0513 | planned | |
| UserService | VerifyTotp | 0x0514 | planned | |
| UserService | ConsumeRecoveryCode | 0x0515 | planned | |
| AppService | CreateApp | 0x0600 | planned | |
| AppService | GetApp | 0x0601 | planned | |
| AppService | ListApps | 0x0602 | planned | |
| AppService | UpdateApp | 0x0603 | planned | |
| AppService | DeleteApp | 0x0604 | planned | |
| AppService | SetAppEnabled | 0x0605 | planned | |
| AppService | SetAppCredentialEnabled | 0x0606 | planned | |
| AppService | GetAppClientSecret | 0x0607 | planned | |
| AppService | RotateAppClientSecret | 0x0608 | planned | |
| AppService | ListAppClientAssertions | 0x0609 | planned | |
| AppService | GetAppClientAssertion | 0x060A | planned | |
| AppService | CreateAppClientAssertion | 0x060B | planned | |
| AppService | DeleteAppClientAssertion | 0x060C | planned | |
| AppService | SetAppClientAssertionEnabled | 0x060D | planned | |
| AppService | ListAppVaults | 0x060E | planned | |
| AppService | AddAppVault | 0x060F | planned | |
| AppService | UpdateAppVault | 0x0610 | planned | |
| AppService | RemoveAppVault | 0x0611 | planned | |
| InvitationService | CreateOrganizationInvite | 0x0700 | planned | |
| InvitationService | ListOrganizationInvites | 0x0701 | planned | |
| InvitationService | GetOrganizationInvite | 0x0702 | planned | |
| InvitationService | RevokeOrganizationInvite | 0x0703 | planned | |
| InvitationService | ListReceivedInvitations | 0x0704 | planned | |
| InvitationService | GetInvitationDetails | 0x0705 | planned | |
| InvitationService | AcceptInvitation | 0x0706 | planned | |
| InvitationService | DeclineInvitation | 0x0707 | planned | |
| TokenService | CreateUserSession | 0x0800 | planned | |
| TokenService | ValidateToken | 0x0801 | planned | response carries `oneof claims` |
| TokenService | CreateVaultToken | 0x0802 | planned | |
| TokenService | RefreshToken | 0x0803 | planned | |
| TokenService | RevokeToken | 0x0804 | planned | |
| TokenService | RevokeAllUserSessions | 0x0805 | planned | |
| TokenService | RevokeAllAppSessions | 0x0806 | planned | |
| TokenService | CreateSigningKey | 0x0807 | planned | |
| TokenService | RotateSigningKey | 0x0808 | planned | |
| TokenService | RevokeSigningKey | 0x0809 | planned | |
| TokenService | GetPublicKeys | 0x080A | planned | |
| TokenService | AuthenticateClientAssertion | 0x080B | planned | |
| EventsService | ListEvents | 0x0900 | planned | |
| EventsService | GetEvent | 0x0901 | planned | |
| EventsService | CountEvents | 0x0902 | planned | |
| EventsService | IngestEvents | 0x0903 | planned | |
| AdminService | JoinCluster | 0x0A00 | planned | |
| AdminService | LeaveCluster | 0x0A01 | planned | |
| AdminService | GetDecommissionStatus | 0x0A02 | planned | |
| AdminService | CheckPeerLiveness | 0x0A03 | planned | |
| AdminService | GetClusterInfo | 0x0A04 | planned | |
| AdminService | GetNodeInfo | 0x0A05 | planned | |
| AdminService | InitCluster | 0x0A06 | planned | |
| AdminService | TransferLeadership | 0x0A07 | planned | |
| AdminService | CreateSnapshot | 0x0A08 | planned | |
| AdminService | CheckIntegrity | 0x0A09 | planned | |
| AdminService | RecoverVault | 0x0A0A | planned | |
| AdminService | SimulateDivergence | 0x0A0B | planned | |
| AdminService | ForceGc | 0x0A0C | planned | |
| AdminService | UpdateConfig | 0x0A0D | planned | |
| AdminService | GetConfig | 0x0A0E | planned | |
| AdminService | CreateBackup | 0x0A0F | planned | |
| AdminService | ListBackups | 0x0A10 | planned | |
| AdminService | RestoreBackup | 0x0A11 | planned | |
| AdminService | RotateBlindingKey | 0x0A12 | planned | |
| AdminService | GetBlindingKeyRehashStatus | 0x0A13 | planned | |
| AdminService | RotateRegionKey | 0x0A14 | planned | |
| AdminService | GetRewrapStatus | 0x0A15 | planned | |
| AdminService | MigrateExistingUsers | 0x0A16 | planned | |
| AdminService | ProvisionRegion | 0x0A17 | planned | |
| AdminService | SetRegionResidency | 0x0A18 | planned | |
| AdminService | AdminListVaults | 0x0A19 | planned | |
| AdminService | ShowVault | 0x0A1A | planned | |
| AdminService | RepairVault | 0x0A1B | planned | stub server-side; no consensus repair entrypoint yet |
| HealthService | Check | 0x0B00 | planned | |
| SystemDiscoveryService | GetPeers | 0x0C00 | planned | |
| SystemDiscoveryService | AnnouncePeer | 0x0C01 | planned | |
| SystemDiscoveryService | GetSystemState | 0x0C02 | planned | |
| SystemDiscoveryService | ResolveRegionLeader | 0x0C03 | planned | |
| SystemDiscoveryService | WatchLeader | 0x0C04 | planned | server-stream |
| RaftService | CommittedIndex | 0x0D00 | planned | |
| RaftService | Replicate | 0x0D01 | planned | bidirectional, long-lived per peer |
| RaftService | RegionalProposal | 0x0D02 | planned | only server-side request-forwarding RPC |
| RaftService | InstallSnapshotStream | 0x0D03 | planned | client-stream; payload is encrypted snapshot envelope |
