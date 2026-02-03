//! gRPC server for InferaDB Ledger.
//!
//! This module provides the main server that exposes all gRPC services:
//! - ReadService: Query operations
//! - WriteService: Transaction submission
//! - AdminService: Namespace and vault management
//! - HealthService: Health checks
//! - SystemDiscoveryService: Peer discovery

use std::{net::SocketAddr, sync::Arc, time::Duration};

use inferadb_ledger_state::{BlockArchive, StateLayer};
use inferadb_ledger_store::FileBackend;
use openraft::Raft;
use tokio::sync::broadcast;
use tonic::transport::Server;
use tower::ServiceBuilder;

use crate::{
    IdempotencyCache,
    batching::BatchConfig,
    log_storage::AppliedStateAccessor,
    proto::{
        BlockAnnouncement, admin_service_server::AdminServiceServer,
        health_service_server::HealthServiceServer, raft_service_server::RaftServiceServer,
        read_service_server::ReadServiceServer,
        system_discovery_service_server::SystemDiscoveryServiceServer,
        write_service_server::WriteServiceServer,
    },
    rate_limit::NamespaceRateLimiter,
    services::{
        AdminServiceImpl, DiscoveryServiceImpl, HealthServiceImpl, RaftServiceImpl,
        ReadServiceImpl, WriteServiceImpl,
    },
    types::LedgerTypeConfig,
};

/// The main Ledger gRPC server.
///
/// Combines all services with the Raft consensus layer and state storage.
#[derive(bon::Builder)]
#[builder(on(_, required))]
pub struct LedgerServer {
    /// The Raft consensus instance.
    raft: Arc<Raft<LedgerTypeConfig>>,
    /// The shared state layer.
    state: Arc<StateLayer<FileBackend>>,
    /// Accessor for applied state (vault heights, health).
    applied_state: AppliedStateAccessor,
    /// Idempotency cache for duplicate detection.
    #[builder(default = Arc::new(IdempotencyCache::new()))]
    idempotency: Arc<IdempotencyCache>,
    /// Block archive for historical block retrieval.
    #[builder(default)]
    block_archive: Option<Arc<BlockArchive<FileBackend>>>,
    /// Block announcement broadcast channel.
    #[builder(default = broadcast::channel(1000).0)]
    block_announcements: broadcast::Sender<BlockAnnouncement>,
    /// Server address.
    addr: SocketAddr,
    /// Rate limit: requests per second (reserved for future use).
    #[allow(dead_code)] // reserved for rate limiting configuration
    #[builder(default = 1000)]
    requests_per_second: u64,
    /// Max concurrent requests per connection.
    #[builder(default = 100)]
    max_concurrent: usize,
    /// Request timeout in seconds.
    #[builder(default = 30)]
    timeout_secs: u64,
    /// Per-namespace rate limiter (optional).
    #[builder(default)]
    namespace_rate_limiter: Option<Arc<NamespaceRateLimiter>>,
}

impl LedgerServer {
    /// Start the gRPC server.
    ///
    /// This method blocks until the server is shut down.
    pub async fn serve(self) -> Result<(), Box<dyn std::error::Error>> {
        tracing::info!(
            max_concurrent = self.max_concurrent,
            timeout_secs = self.timeout_secs,
            "Configuring request limits"
        );

        // Configure backpressure with tower layers
        // Note: RateLimitLayer is not used because it doesn't implement Clone,
        // which tonic's serve() requires. The concurrency_limit + load_shed
        // combination provides effective backpressure protection.
        let layer = ServiceBuilder::new()
            // Limit concurrent requests per connection
            .concurrency_limit(self.max_concurrent)
            // Reject new requests when overloaded (returns 503)
            .load_shed()
            // Set timeout for requests
            .timeout(Duration::from_secs(self.timeout_secs))
            .into_inner();

        // Create service implementations
        let read_service = ReadServiceImpl::builder()
            .state(self.state.clone())
            .applied_state(self.applied_state.clone())
            .block_archive(self.block_archive.clone())
            .block_announcements(self.block_announcements.clone())
            .idempotency(Some(self.idempotency.clone()))
            .raft(Some(self.raft.clone()))
            .build();
        // Create write service with batching enabled for high throughput.
        // Per DESIGN.md §6.3: Server-level batching coalesces individual Write RPCs
        // into single Raft proposals. This improves throughput when clients can't
        // or don't use BatchWrite RPC.
        let batch_config = BatchConfig::default();
        let write_service = if let Some(archive) = &self.block_archive {
            let (service, task) = WriteServiceImpl::with_block_archive_and_batching(
                self.raft.clone(),
                self.idempotency.clone(),
                archive.clone(),
                batch_config,
            );
            tokio::spawn(task);
            service
        } else {
            let (service, task) = WriteServiceImpl::new_with_batching(
                self.raft.clone(),
                self.idempotency.clone(),
                batch_config,
            );
            tokio::spawn(task);
            service
        };
        // Add applied state for sequence gap detection
        let write_service = write_service.with_applied_state(self.applied_state.clone());
        // Add per-namespace rate limiting if configured
        let write_service = match &self.namespace_rate_limiter {
            Some(limiter) => write_service.with_rate_limiter(limiter.clone()),
            None => write_service,
        };
        let admin_service = AdminServiceImpl::builder()
            .raft(self.raft.clone())
            .state(self.state.clone())
            .applied_state(self.applied_state.clone())
            .block_archive(self.block_archive.clone())
            .listen_addr(self.addr)
            .build();
        let health_service = HealthServiceImpl::new(
            self.raft.clone(),
            self.state.clone(),
            self.applied_state.clone(),
        );
        let discovery_service = DiscoveryServiceImpl::builder()
            .raft(self.raft.clone())
            .state(self.state.clone())
            .applied_state(self.applied_state.clone())
            .build();

        // RaftService handles inter-node Raft RPCs (Vote, AppendEntries, InstallSnapshot)
        let raft_service = RaftServiceImpl::new(self.raft.clone());

        tracing::info!("Starting Ledger gRPC server on {}", self.addr);

        Server::builder()
            .layer(layer)
            .add_service(ReadServiceServer::new(read_service))
            .add_service(WriteServiceServer::new(write_service))
            .add_service(AdminServiceServer::new(admin_service))
            .add_service(HealthServiceServer::new(health_service))
            .add_service(SystemDiscoveryServiceServer::new(discovery_service))
            .add_service(RaftServiceServer::new(raft_service))
            .serve(self.addr)
            .await?;

        Ok(())
    }

    /// Get the Raft instance.
    #[must_use]
    pub fn raft(&self) -> &Arc<Raft<LedgerTypeConfig>> {
        &self.raft
    }

    /// Get the state layer.
    #[must_use]
    pub fn state(&self) -> &Arc<StateLayer<FileBackend>> {
        &self.state
    }

    /// Get the idempotency cache.
    #[must_use]
    pub fn idempotency(&self) -> &Arc<IdempotencyCache> {
        &self.idempotency
    }

    /// Get the block announcements sender (for broadcasting new blocks).
    #[must_use]
    pub fn block_announcements(&self) -> &broadcast::Sender<BlockAnnouncement> {
        &self.block_announcements
    }
}
