//! Multi-shard gRPC server for InferaDB Ledger.
//!
//! This module provides a server that supports multi-shard deployments,
//! routing requests to the appropriate shard based on namespace.
//!
//! ## Architecture
//!
//! ```text
//! Client Request
//!       |
//!       v
//! MultiShardLedgerServer
//!       |
//!       +-- MultiShardReadService  --+
//!       |                            |
//!       +-- MultiShardWriteService --+--> ShardResolver --> Shard N
//!       |                            |
//!       +-- AdminService (system)  --+
//! ```

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tonic::transport::Server;
use tower::ServiceBuilder;

use crate::IdempotencyCache;
use crate::multi_raft::MultiRaftManager;
use crate::proto::admin_service_server::AdminServiceServer;
use crate::proto::health_service_server::HealthServiceServer;
use crate::proto::raft_service_server::RaftServiceServer;
use crate::proto::read_service_server::ReadServiceServer;
use crate::proto::system_discovery_service_server::SystemDiscoveryServiceServer;
use crate::proto::write_service_server::WriteServiceServer;
use crate::rate_limit::NamespaceRateLimiter;
use crate::services::{
    AdminServiceImpl, DiscoveryServiceImpl, HealthServiceImpl, MultiShardReadService,
    MultiShardResolver, MultiShardWriteService, RaftServiceImpl,
};

/// Multi-shard Ledger gRPC server.
///
/// Combines multi-shard read/write services with the MultiRaftManager
/// for routing requests to the correct shard.
pub struct MultiShardLedgerServer {
    /// The multi-raft manager containing all shards.
    manager: Arc<MultiRaftManager>,
    /// Idempotency cache for duplicate detection.
    idempotency: Arc<IdempotencyCache>,
    /// Server address.
    addr: SocketAddr,
    /// Max concurrent requests per connection.
    max_concurrent: usize,
    /// Request timeout in seconds.
    timeout_secs: u64,
    /// Per-namespace rate limiter (optional).
    namespace_rate_limiter: Option<Arc<NamespaceRateLimiter>>,
}

impl MultiShardLedgerServer {
    /// Create a new multi-shard Ledger server.
    ///
    /// The MultiRaftManager must have at least the system shard (shard 0) started.
    pub fn new(manager: Arc<MultiRaftManager>, addr: SocketAddr) -> Self {
        Self {
            manager,
            idempotency: Arc::new(IdempotencyCache::new()),
            addr,
            max_concurrent: 100,
            timeout_secs: 30,
            namespace_rate_limiter: None,
        }
    }

    /// Configure request limits for this server.
    pub fn with_rate_limit(mut self, max_concurrent: usize, timeout_secs: u64) -> Self {
        self.max_concurrent = max_concurrent;
        self.timeout_secs = timeout_secs;
        self
    }

    /// Configure per-namespace rate limiting.
    pub fn with_namespace_rate_limit(mut self, requests_per_second: u64) -> Self {
        self.namespace_rate_limiter = Some(Arc::new(NamespaceRateLimiter::with_limit(
            requests_per_second,
        )));
        self
    }

    /// Use a custom idempotency cache.
    pub fn with_idempotency_cache(mut self, idempotency: Arc<IdempotencyCache>) -> Self {
        self.idempotency = idempotency;
        self
    }

    /// Start the gRPC server.
    ///
    /// This method blocks until the server is shut down.
    pub async fn serve(self) -> Result<(), Box<dyn std::error::Error>> {
        tracing::info!(
            max_concurrent = self.max_concurrent,
            timeout_secs = self.timeout_secs,
            "Configuring multi-shard request limits"
        );

        // Configure backpressure with tower layers
        let layer = ServiceBuilder::new()
            .concurrency_limit(self.max_concurrent)
            .load_shed()
            .timeout(Duration::from_secs(self.timeout_secs))
            .into_inner();

        // Create the shard resolver
        let resolver: Arc<dyn crate::services::ShardResolver> =
            Arc::new(MultiShardResolver::new(self.manager.clone()));

        // Create multi-shard services
        let read_service = MultiShardReadService::new(resolver.clone());

        let write_service = {
            let base = MultiShardWriteService::new(resolver.clone(), self.idempotency.clone());
            match &self.namespace_rate_limiter {
                Some(limiter) => base.with_rate_limiter(limiter.clone()),
                None => base,
            }
        };

        // Admin, Health, and Discovery services use the system shard
        // These handle global operations like namespace management
        let system_shard = self.manager.system_shard().map_err(|e| {
            Box::new(std::io::Error::other(format!(
                "System shard not available: {}",
                e
            ))) as Box<dyn std::error::Error>
        })?;

        let admin_service = AdminServiceImpl::with_block_archive(
            system_shard.raft().clone(),
            system_shard.state().clone(),
            system_shard.applied_state().clone(),
            system_shard.block_archive().clone(),
        );

        let health_service = HealthServiceImpl::new(
            system_shard.raft().clone(),
            system_shard.state().clone(),
            system_shard.applied_state().clone(),
        );

        let discovery_service = DiscoveryServiceImpl::new(
            system_shard.raft().clone(),
            system_shard.state().clone(),
            system_shard.applied_state().clone(),
        );

        // RaftService for the system shard handles inter-node Raft RPCs
        // TODO: Multi-shard Raft service would need to route to correct shard
        let raft_service = RaftServiceImpl::new(system_shard.raft().clone());

        tracing::info!(
            addr = %self.addr,
            shards = self.manager.list_shards().len(),
            "Starting multi-shard Ledger gRPC server"
        );

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

    /// Get the multi-raft manager.
    pub fn manager(&self) -> &Arc<MultiRaftManager> {
        &self.manager
    }

    /// Get the idempotency cache.
    pub fn idempotency(&self) -> &Arc<IdempotencyCache> {
        &self.idempotency
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_multi_shard_server_creation() {
        // Basic struct test - full testing requires MultiRaftManager setup
    }
}
