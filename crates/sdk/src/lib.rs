//! Production-grade Rust SDK for InferaDB Ledger service.
//!
//! Provides a high-level, ergonomic API for Rust applications to interact
//! with Ledger's blockchain database. Wraps the gRPC services with automatic
//! idempotency, resilient connectivity, and streaming support.
//!
//! # Features
//!
//! - **Type-safe API**: Strong typing for all Ledger operations
//! - **Automatic idempotency**: Server-assigned sequences with UUID-based deduplication
//! - **Resilient connectivity**: Exponential backoff retry and failover
//! - **Streaming support**: WatchBlocks with automatic reconnection
//! - **Cancellation support**: Per-request and client-level cancellation via `CancellationToken`
//! - **Minimal-overhead abstractions**: Efficient serialization and connection pooling
//!
//! # Quick Start
//!
//! ```no_run
//! use inferadb_ledger_sdk::{LedgerClient, ClientConfig, Operation, OrganizationSlug, UserSlug, ServerSource};
//!
//! #[tokio::main]
//! async fn main() -> inferadb_ledger_sdk::Result<()> {
//!     let config = ClientConfig::builder()
//!         .servers(ServerSource::from_static(["http://localhost:50051"]))
//!         .client_id("my-app-001")
//!         .build()?;
//!
//!     let client = LedgerClient::new(config).await?;
//!     # let organization = OrganizationSlug::new(1);
//!     # let caller = UserSlug::new(1);
//!
//!     // Read operations
//!     let value = client.read(caller, organization, None, "user:123", None, None).await?;
//!
//!     // Write operations with automatic idempotency
//!     let operations = vec![Operation::set_entity("user:123", b"data".to_vec(), None, None)];
//!     let result = client.write(caller, organization, None, operations, None).await?;
//!     println!("Committed at block {} with sequence {}", result.block_height, result.assigned_sequence);
//!
//!     Ok(())
//! }
//! ```
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    LedgerClient (Public API)                │
//! │  .read() │ .write() │ .watch_blocks() │ .admin()           │
//! ├─────────────────────────────────────────────────────────────┤
//! │                   Idempotency Layer                         │
//! │   UUID generation │ Retry key preservation │ Dedup         │
//! ├─────────────────────────────────────────────────────────────┤
//! │                   Resilience Layer (retry + cancellation)    │
//! │   Retry middleware │ Exponential backoff │ Timeout         │
//! ├─────────────────────────────────────────────────────────────┤
//! │                   Connection Pool                           │
//! │   Channel management │ Load balancing │ Health checks      │
//! ├─────────────────────────────────────────────────────────────┤
//! │                   Tonic gRPC Clients                        │
//! │   ReadServiceClient │ WriteServiceClient │ AdminService    │
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
pub mod mock;
mod ops;
pub(crate) mod proto_util;
pub mod region_resolver;
mod retry;
pub mod server;
mod streaming;
pub mod token;
mod tracing;
mod types;

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
pub use error::{Result, SdkError};
// Re-export commonly used types from inferadb-ledger-types
pub use inferadb_ledger_types::{
    AppId, AppSlug, ClientAssertionId, InviteSlug, OrganizationId, OrganizationSlug, Region,
    TeamId, TeamSlug, UserCredentialId, UserEmailId, UserRole, UserSlug, UserStatus, VaultId,
    VaultSlug,
};
pub use metrics::{ConnectionEvent, MetricsSdkMetrics, NoopSdkMetrics, SdkMetrics};
pub use region_resolver::RegionLeaderCache;
pub use retry::{with_retry, with_retry_cancellable};
pub use server::{
    DnsConfig, FileConfig, ResolvedServer, ServerResolver, ServerSelector, ServerSource,
};
pub use streaming::{HeightTracker, ReconnectingStream};
pub use token::{PublicKeyInfo, TokenPair, ValidatedToken};
pub use tracing::TraceConfig;
pub use types::schema::{
    SchemaDeployResult, SchemaDiffChange, SchemaVersion, SchemaVersionSummary,
};
// Public API exports — domain types (from types/ modules)
pub use types::{
    admin::{
        BlindingKeyRehashStatus, BlindingKeyRotationStatus, EmailVerificationCode,
        EmailVerificationResult, HealthCheckResult, HealthStatus, MigrationInfo,
        OrganizationDeleteInfo, OrganizationInfo, OrganizationMemberInfo, OrganizationMemberRole,
        OrganizationStatus, OrganizationTier, RegistrationResult, TeamInfo, TeamMemberInfo,
        TeamMemberRole, UserEmailInfo, UserInfo, UserMigrationInfo, VaultInfo, VaultStatus,
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
        Entity, ListEntitiesOpts, ListRelationshipsOpts, ListResourcesOpts, Operation, PagedResult,
        Relationship, SetCondition, VerifiedValue,
    },
    read::{ReadConsistency, WriteSuccess},
    streaming::BlockAnnouncement,
    verified_read::{
        Block, BlockHeader, ChainProof, ChainTip, Direction, HistoricalRead, MerkleProof,
        MerkleSibling, Transaction, VerifyOpts,
    },
};
