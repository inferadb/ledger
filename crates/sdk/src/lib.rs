//! Rust client SDK for InferaDB Ledger.
//!
//! InferaDB Ledger is a blockchain database for cryptographically verifiable
//! authorization. Every write produces a Merkle-chained block; every
//! authorization check is provable. This crate provides a high-level,
//! ergonomic API for Rust applications to read from and write to the Ledger
//! service over gRPC.
//!
//! # Capabilities
//!
//! - **Type-safe operations**: Strong types for all Ledger operations.
//! - **Automatic idempotency**: UUID-based deduplication across retries; each client has a stable
//!   `client_id` and the SDK generates per-operation keys.
//! - **Resilient connectivity**: Exponential-backoff retry, redirect-only leader routing via
//!   [`RegionLeaderCache`] and [`VaultLeaderCache`], and an optional per-endpoint
//!   [`CircuitBreaker`].
//! - **Cancellation**: Per-request and client-level cancellation via
//!   [`tokio_util::sync::CancellationToken`].
//! - **Metrics**: Pluggable metrics trait ([`SdkMetrics`]) with a zero-overhead default
//!   ([`NoopSdkMetrics`]) and a ready-made [`metrics`-crate][`MetricsSdkMetrics`] integration.
//!
//! # Quick Start
//!
//! ```no_run
//! use inferadb_ledger_sdk::{
//!     ClientConfig, LedgerClient, Operation, OrganizationSlug, ServerSource, UserSlug,
//! };
//!
//! #[tokio::main]
//! async fn main() -> inferadb_ledger_sdk::Result<()> {
//!     let config = ClientConfig::builder()
//!         .servers(ServerSource::from_static(["http://localhost:50051"]))
//!         .client_id("my-app-001")
//!         .build()?;
//!
//!     let client = LedgerClient::new(config).await?;
//!
//!     let organization = OrganizationSlug::new(1);
//!     let caller = UserSlug::new(1);
//!
//!     // Read a value.
//!     let _value = client.read(caller, organization, None, "user:123", None, None).await?;
//!
//!     // Write with automatic idempotency.
//!     let operations = vec![Operation::set_entity("user:123", b"data".to_vec(), None, None)];
//!     let result = client.write(caller, organization, None, operations, None).await?;
//!     println!(
//!         "Committed at block {} with sequence {}",
//!         result.block_height, result.assigned_sequence
//!     );
//!
//!     Ok(())
//! }
//! ```
//!
//! # Durability Guarantees
//!
//! A successful write response implies **WAL durability**: the Raft log has
//! fsynced the committed entry on every replica. State-DB materialization
//! (the B-tree checkpointer) is lazy and lands on the next checkpointer tick,
//! or immediately on graceful shutdown, snapshot, or backup. Crash recovery
//! replays the WAL tail to rebuild any unpersisted state; replay is
//! idempotent.
//!
//! [`LedgerClient::ingest_events`] carries the same WAL-durability guarantee:
//! a successful response means the events committed through Raft and were
//! applied to the events store in-memory; fsync to the events-state DB
//! happens on the next checkpointer tick.
//!
//! Handler-phase audit events (side-effects of non-ingest RPCs) are
//! checkpoint-covered, not per-emission-fsynced. A crash before the next
//! checkpoint loses flushed-but-uncheckpointed entries. Strict-durability
//! deployments can restore per-emission fsync semantics via the `UpdateConfig`
//! admin RPC (`event_writer_batch.enabled = false`). See
//! [`EventFilter::handler_phase_only`] for the consumer-visible consequence.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                 LedgerClient (Public API)                   │
//! │  .read()  .write()  .admin()                                │
//! ├─────────────────────────────────────────────────────────────┤
//! │                    Idempotency Layer                        │
//! │      UUID generation │ retry-key preservation │ dedup      │
//! ├─────────────────────────────────────────────────────────────┤
//! │              Resilience Layer (retry + cancellation)        │
//! │      Exponential backoff │ leader-redirect routing         │
//! ├─────────────────────────────────────────────────────────────┤
//! │                     Connection Pool                         │
//! │     N independent wire connections │ round-robin dispatch  │
//! └─────────────────────────────────────────────────────────────┘
//! ```

#![deny(unsafe_code, missing_docs)]

mod builders;
mod circuit_breaker;
mod client;
mod config;
mod connection;
mod discovery;
mod error;
mod metrics;
mod ops;
mod ops_wire;
pub mod region_resolver;
mod retry;
pub mod server;
mod tls_bridge;
pub mod token;
mod tracing;
mod types;
pub mod vault_resolver;
mod wire_conversion;

// Public API exports
pub use builders::{BatchReadBuilder, RelationshipQueryBuilder, WriteBuilder};
pub use circuit_breaker::{CircuitBreaker, CircuitBreakerConfig, CircuitState};
pub use client::LedgerClient;
pub use config::{
    CertificateData, ClientConfig, ClientConfigBuilder, DiscoveryConfig, RetryPolicy, TlsConfig,
    TlsConfigBuilder,
};
pub use connection::ConnectionPool;
pub use discovery::{DiscoveryResult, DiscoveryService, PeerInfo};
pub use error::{LeaderHint, Result, SdkError, ServerErrorDetails};
// Re-export commonly used types from inferadb-ledger-types
pub use inferadb_ledger_types::{
    AppId, AppSlug, BlockRetentionMode, BlockRetentionPolicy, ClientAssertionId, InviteSlug,
    OrganizationId, OrganizationSlug, Region, TeamId, TeamSlug, UserCredentialId, UserEmailId,
    UserRole, UserSlug, UserStatus, VaultId, VaultSlug,
};
pub use metrics::{ConnectionEvent, MetricsSdkMetrics, NoopSdkMetrics, SdkMetrics};
pub use region_resolver::RegionLeaderCache;
pub use retry::{with_retry, with_retry_cancellable};
pub use server::{
    DnsConfig, FileConfig, ResolvedServer, ServerResolver, ServerSelector, ServerSource,
};
pub use token::{PublicKeyInfo, TokenPair, ValidatedToken};
pub use tracing::TraceConfig;
pub use types::schema::{
    SchemaDeployResult, SchemaDiffChange, SchemaVersion, SchemaVersionSummary,
};
// Public API exports — domain types (from types/ modules)
pub use types::{
    admin::{
        EmailVerificationCode, EmailVerificationResult, HealthCheckResult, HealthStatus,
        MigrationInfo, OrganizationDeleteInfo, OrganizationInfo, OrganizationMemberInfo,
        OrganizationMemberRole, OrganizationStatus, OrganizationTier, RegistrationResult, TeamInfo,
        TeamMemberInfo, TeamMemberRole, UserEmailInfo, UserInfo, UserMigrationInfo, VaultInfo,
        VaultStatus,
    },
    app::{
        AppClientAssertionInfo, AppClientSecretStatus, AppCredentialType, AppCredentialsInfo,
        AppInfo, AppVaultConnectionInfo, CreateAppClientAssertionResult,
    },
    credential::{
        CredentialData, CredentialType, PasskeyCredentialInfo, RecoveryCodeCredentialInfo,
        RecoveryCodeResult, TotpAlgorithm, TotpCredentialInfo, UserCredentialInfo,
    },
    events::{
        EventEmissionPath, EventFilter, EventOutcome, EventPage, EventScope, EventSource,
        IngestRejection, IngestResult, SdkEventEntry, SdkIngestEventEntry,
    },
    invitation::{
        InvitationCreated, InvitationInfo, InvitationPage, InvitationStatus,
        ReceivedInvitationInfo, ReceivedInvitationPage,
    },
    query::{
        CheckRelationshipOutcome, Entity, ListEntitiesOpts, ListRelationshipsOpts,
        ListResourcesOpts, Operation, PagedResult, Relationship, SetCondition, VerifiedValue,
    },
    read::{ReadConsistency, WriteSuccess},
    verified_read::{
        Block, BlockHeader, ChainProof, ChainTip, Direction, HistoricalRead, MerkleProof,
        MerkleSibling, Transaction, VerifyOpts,
    },
};
pub use vault_resolver::{LeaderEntry as VaultLeaderEntry, VaultLeaderCache};
