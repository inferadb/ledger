//! gRPC server for InferaDB Ledger.
//!
//! This module provides the main server that exposes all gRPC services:
//! - ReadService: Query operations
//! - WriteService: Transaction submission
//! - AdminService: Namespace and vault management
//! - HealthService: Health checks
//! - SystemDiscoveryService: Peer discovery

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use openraft::Raft;
use tokio::sync::broadcast;
use tonic::transport::Server;
use tower::ServiceBuilder;

use crate::IdempotencyCache;
use crate::log_storage::AppliedStateAccessor;
use crate::proto::BlockAnnouncement;
use crate::proto::admin_service_server::AdminServiceServer;
use crate::proto::health_service_server::HealthServiceServer;
use crate::proto::raft_service_server::RaftServiceServer;
use crate::proto::read_service_server::ReadServiceServer;
use crate::proto::system_discovery_service_server::SystemDiscoveryServiceServer;
use crate::proto::write_service_server::WriteServiceServer;
use crate::rate_limit::NamespaceRateLimiter;
use crate::services::{
    AdminServiceImpl, DiscoveryServiceImpl, HealthServiceImpl, RaftServiceImpl, ReadServiceImpl,
    WriteServiceImpl,
};
use crate::types::LedgerTypeConfig;

use inkwell::FileBackend;
use ledger_storage::{BlockArchive, StateLayer};

/// The main Ledger gRPC server.
///
/// Combines all services with the Raft consensus layer and state storage.
pub struct LedgerServer {
    /// The Raft consensus instance.
    raft: Arc<Raft<LedgerTypeConfig>>,
    /// The shared state layer.
    state: Arc<StateLayer<FileBackend>>,
    /// Accessor for applied state (vault heights, health).
    applied_state: AppliedStateAccessor,
    /// Idempotency cache for duplicate detection.
    idempotency: Arc<IdempotencyCache>,
    /// Block archive for historical block retrieval.
    block_archive: Option<Arc<BlockArchive<FileBackend>>>,
    /// Block announcement broadcast channel.
    block_announcements: broadcast::Sender<BlockAnnouncement>,
    /// Server address.
    addr: SocketAddr,
    /// Rate limit: requests per second (reserved for future use).
    #[allow(dead_code)]
    requests_per_second: u64,
    /// Max concurrent requests per connection.
    max_concurrent: usize,
    /// Request timeout in seconds.
    timeout_secs: u64,
    /// Per-namespace rate limiter (optional).
    namespace_rate_limiter: Option<Arc<NamespaceRateLimiter>>,
}

impl LedgerServer {
    /// Create a new Ledger server.
    pub fn new(
        raft: Arc<Raft<LedgerTypeConfig>>,
        state: Arc<StateLayer<FileBackend>>,
        applied_state: AppliedStateAccessor,
        addr: SocketAddr,
    ) -> Self {
        Self::with_block_archive(raft, state, applied_state, None, addr)
    }

    /// Create a new Ledger server with block archive for GetBlock/GetBlockRange.
    pub fn with_block_archive(
        raft: Arc<Raft<LedgerTypeConfig>>,
        state: Arc<StateLayer<FileBackend>>,
        applied_state: AppliedStateAccessor,
        block_archive: Option<Arc<BlockArchive<FileBackend>>>,
        addr: SocketAddr,
    ) -> Self {
        // Create broadcast channel for block announcements (capacity 1000)
        let (block_announcements, _) = broadcast::channel(1000);

        Self {
            raft,
            state,
            applied_state,
            idempotency: Arc::new(IdempotencyCache::new()),
            block_archive,
            block_announcements,
            addr,
            // Default rate limiting values
            requests_per_second: 1000,
            max_concurrent: 100,
            timeout_secs: 30,
            namespace_rate_limiter: None,
        }
    }

    /// Configure request limits for this server.
    ///
    /// - `max_concurrent`: Max concurrent requests per connection
    /// - `timeout_secs`: Request timeout in seconds
    pub fn with_rate_limit(mut self, max_concurrent: usize, timeout_secs: u64) -> Self {
        self.max_concurrent = max_concurrent;
        self.timeout_secs = timeout_secs;
        self
    }

    /// Configure per-namespace rate limiting.
    ///
    /// Per DESIGN.md §3.7: Mitigates noisy neighbor problems by limiting
    /// requests per namespace. Each namespace gets an independent rate limit.
    pub fn with_namespace_rate_limit(mut self, requests_per_second: u64) -> Self {
        self.namespace_rate_limiter = Some(Arc::new(NamespaceRateLimiter::with_limit(
            requests_per_second,
        )));
        self
    }

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
        let read_service = ReadServiceImpl::with_idempotency(
            self.state.clone(),
            self.applied_state.clone(),
            self.block_archive.clone(),
            self.block_announcements.clone(),
            self.idempotency.clone(),
        );
        let write_service = {
            let base = match &self.block_archive {
                Some(archive) => WriteServiceImpl::with_block_archive(
                    self.raft.clone(),
                    self.idempotency.clone(),
                    archive.clone(),
                ),
                None => WriteServiceImpl::new(self.raft.clone(), self.idempotency.clone()),
            }
            // Add applied state for sequence gap detection
            .with_applied_state(self.applied_state.clone());
            // Add per-namespace rate limiting if configured
            match &self.namespace_rate_limiter {
                Some(limiter) => base.with_rate_limiter(limiter.clone()),
                None => base,
            }
        };
        let admin_service = match &self.block_archive {
            Some(archive) => AdminServiceImpl::with_block_archive(
                self.raft.clone(),
                self.state.clone(),
                self.applied_state.clone(),
                archive.clone(),
            ),
            None => AdminServiceImpl::new(
                self.raft.clone(),
                self.state.clone(),
                self.applied_state.clone(),
            ),
        };
        let health_service = HealthServiceImpl::new(
            self.raft.clone(),
            self.state.clone(),
            self.applied_state.clone(),
        );
        let discovery_service = DiscoveryServiceImpl::new(
            self.raft.clone(),
            self.state.clone(),
            self.applied_state.clone(),
        );

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
    pub fn raft(&self) -> &Arc<Raft<LedgerTypeConfig>> {
        &self.raft
    }

    /// Get the state layer.
    pub fn state(&self) -> &Arc<StateLayer<FileBackend>> {
        &self.state
    }

    /// Get the idempotency cache.
    pub fn idempotency(&self) -> &Arc<IdempotencyCache> {
        &self.idempotency
    }

    /// Get the block announcements sender (for broadcasting new blocks).
    pub fn block_announcements(&self) -> &broadcast::Sender<BlockAnnouncement> {
        &self.block_announcements
    }
}
