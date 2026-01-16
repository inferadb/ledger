//! Production-grade Rust SDK for InferaDB Ledger service.
//!
//! This SDK provides a high-level, ergonomic API for Rust applications to interact
//! with Ledger's blockchain database. It wraps the gRPC services with automatic
//! idempotency tracking, resilient connectivity, and streaming support.
//!
//! # Features
//!
//! - **Type-safe API**: Strong typing for all Ledger operations
//! - **Automatic idempotency**: Client-side sequence tracking per vault
//! - **Resilient connectivity**: Exponential backoff retry and failover
//! - **Streaming support**: WatchBlocks with automatic reconnection
//! - **Zero-cost abstractions**: Efficient serialization and connection pooling
//!
//! # Quick Start
//!
//! ```rust,ignore
//! use ledger_sdk::{LedgerClient, ClientConfig};
//!
//! #[tokio::main]
//! async fn main() -> ledger_sdk::Result<()> {
//!     let config = ClientConfig::builder()
//!         .with_endpoint("http://localhost:50051")
//!         .with_client_id("my-app-001")
//!         .build()?;
//!
//!     let client = LedgerClient::new(config).await?;
//!
//!     // Read operations
//!     let value = client.read(1, 0, "user:123").await?;
//!
//!     // Write operations with automatic idempotency
//!     let result = client.write(1, 0, operations).await?;
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
//! │                   SequenceTracker Layer                     │
//! │   Per-vault sequence tracking │ Crash recovery │ Dedup     │
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
mod error;
mod idempotency;
mod retry;
mod streaming;

// Public API exports
pub use config::{ClientConfig, ClientConfigBuilder, RetryPolicy, RetryPolicyBuilder};
pub use error::{Result, SdkError};

// Re-export commonly used types from ledger-types
pub use ledger_types::{NamespaceId, VaultId};
