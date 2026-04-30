//! gRPC services and server assembly for InferaDB Ledger.
//!
//! Implements all 14 gRPC services exposed by an InferaDB Ledger node and
//! wires them into a [`LedgerServer`] backed by multi-Raft consensus:
//!
//! | Service | Responsibility |
//! |---|---|
//! | [`services::ReadService`] | Entity/relationship reads, block queries, streaming |
//! | [`services::WriteService`] | Transaction submission through per-vault Raft |
//! | [`services::AdminService`] | Cluster membership, snapshots, backup/restore, key rotation |
//! | [`services::OrganizationService`] | Organization lifecycle: create, delete, migrate, teams |
//! | [`services::VaultService`] | Vault lifecycle: create, delete, update, list |
//! | [`services::UserService`] | User lifecycle: registration, credentials, TOTP, email |
//! | [`services::AppService`] | Application management: credentials, vault connections |
//! | [`services::InvitationService`] | Organization invitation lifecycle |
//! | [`services::TokenServiceImpl`] | JWT sessions, vault tokens, signing key management |
//! | [`services::EventsService`] | Organization-scoped event queries and ingestion |
//! | [`services::HealthService`] | Startup, liveness, and readiness probes |
//! | [`services::DiscoveryService`] | Peer discovery and system state |
//!
//! Inter-node consensus RPCs (`Replicate`, `RegionalProposal`,
//! `CommittedIndex`, `InstallSnapshotStream`) are owned by
//! `inferadb_ledger_raft::wire_consensus_transport::WireRaftServerHandler`,
//! not by a service in this crate.
//!
//! ## Key helpers
//!
//! - [`services::slug_resolver::SlugResolver`] — translates external [`OrganizationSlug`] /
//!   [`VaultSlug`] values to internal IDs at every gRPC handler boundary (golden rule 4).
//! - `services::metadata::not_leader_wire_error_*` — wire-native builders for `NotLeader`
//!   rejections; carry leader-hint context the SDK reads for direct redirection.
//! - `services::wire_helpers::wire_error_with_correlation` — stamps frame-level correlation
//!   metadata onto a [`WireError`] for the wire dispatcher.
//! - `services::service_infra::ServiceContext` — shared Raft, state, and event infrastructure
//!   embedded in `OrganizationService`, `VaultService`, `UserService`, `AppService`, and
//!   `InvitationService`.
//! - `proposal::ProposalService` — abstraction over vault-scoped Raft proposal submission, enabling
//!   unit testing with `MockProposalService`.
//! - `api_version::ApiVersionLayer` — Tower layer that validates and injects `x-ledger-api-version`
//!   on every request/response.
//! - [`jwt::JwtEngine`] — Ed25519 JWT signing and validation with encrypted key envelope storage.
//!
//! [`OrganizationSlug`]: inferadb_ledger_types::OrganizationSlug
//! [`VaultSlug`]: inferadb_ledger_types::VaultSlug
//! [`WireError`]: inferadb_ledger_wire::WireError

#![deny(unsafe_code)]
#![warn(missing_docs)]
#![allow(clippy::result_large_err)]

#[doc(hidden)]
pub mod api_version;
/// JWT signing, validation, and key management.
pub mod jwt;
#[doc(hidden)]
pub mod peer_maintenance;
pub(crate) mod proposal;
#[doc(hidden)]
pub mod server;
#[doc(hidden)]
pub mod services;
#[doc(hidden)]
pub mod wire_server;

#[doc(hidden)]
pub use server::LedgerServer;
#[doc(hidden)]
pub use wire_server::LedgerWireDispatcher;
