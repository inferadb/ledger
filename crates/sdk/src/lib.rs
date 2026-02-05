//! Production-grade Rust SDK for InferaDB Ledger service.
//!
//! This SDK provides a high-level, ergonomic API for Rust applications to interact
//! with Ledger's blockchain database. It wraps the gRPC services with automatic
//! idempotency, resilient connectivity, and streaming support.
//!
//! # Features
//!
//! - **Type-safe API**: Strong typing for all Ledger operations
//! - **Automatic idempotency**: Server-assigned sequences with UUID-based deduplication
//! - **Resilient connectivity**: Exponential backoff retry and failover
//! - **Streaming support**: WatchBlocks with automatic reconnection
//! - **Zero-cost abstractions**: Efficient serialization and connection pooling
//!
//! # Quick Start
//!
//! ```no_run
//! use inferadb_ledger_sdk::{LedgerClient, ClientConfig, Operation, ServerSource};
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
//!     // Read operations
//!     let value = client.read(1, None, "user:123").await?;
//!
//!     // Write operations with automatic idempotency
//!     let operations = vec![Operation::set_entity("user:123", b"data".to_vec())];
//!     let result = client.write(1, None, operations).await?;
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
//! │                   Resilience Layer (backon)                 │
//! │   Retry middleware │ Exponential backoff │ Timeout         │
//! ├─────────────────────────────────────────────────────────────┤
//! │                   Connection Pool                           │
//! │   Channel management │ Load balancing │ Health checks      │
//! ├─────────────────────────────────────────────────────────────┤
//! │                   Tonic gRPC Clients                        │
//! │   ReadServiceClient │ WriteServiceClient │ AdminService    │
//! └─────────────────────────────────────────────────────────────┘
//! ```

#![deny(unsafe_code)]
#![warn(missing_docs)]

mod client;
mod config;
mod connection;
mod discovery;
mod error;
mod idempotency;
pub mod mock;
mod retry;
pub mod server;
mod streaming;
mod tracing;

// Public API exports
pub use client::{
    BlockAnnouncement, BlockHeader, ChainProof, Direction, Entity, HealthCheckResult, HealthStatus,
    LedgerClient, ListEntitiesOpts, ListRelationshipsOpts, ListResourcesOpts, MerkleProof,
    MerkleSibling, NamespaceInfo, NamespaceStatus, Operation, PagedResult, ReadConsistency,
    Relationship, SetCondition, VaultInfo, VaultStatus, VerifiedValue, VerifyOpts, WriteSuccess,
};
pub use config::{
    CertificateData, ClientConfig, ClientConfigBuilder, DiscoveryConfig, RetryPolicy, TlsConfig,
    TlsConfigBuilder,
};
pub use connection::ConnectionPool;
pub use discovery::{DiscoveryResult, DiscoveryService, PeerInfo};
pub use error::{Result, SdkError};
// Re-export commonly used types from inferadb-ledger-types
pub use inferadb_ledger_types::{NamespaceId, VaultId};
pub use retry::with_retry;
pub use server::{
    DnsConfig, FileConfig, ResolvedServer, SelectorStats, ServerResolver, ServerSelector,
    ServerSource,
};
pub use streaming::{HeightTracker, PositionTracker, ReconnectingStream};
pub use tracing::TraceConfig;
