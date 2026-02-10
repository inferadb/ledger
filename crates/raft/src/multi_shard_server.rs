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

use std::{net::SocketAddr, sync::Arc, time::Duration};

use inferadb_ledger_proto::proto::{
    admin_service_server::AdminServiceServer, health_service_server::HealthServiceServer,
    raft_service_server::RaftServiceServer, read_service_server::ReadServiceServer,
    system_discovery_service_server::SystemDiscoveryServiceServer,
    write_service_server::WriteServiceServer,
};
use tonic::transport::Server;
use tower::ServiceBuilder;

use crate::{
    api_version::{ApiVersionLayer, api_version_interceptor},
    graceful_shutdown::ConnectionTrackingLayer,
    idempotency::IdempotencyCache,
    multi_raft::MultiRaftManager,
    rate_limit::RateLimiter,
    services::{
        AdminServiceImpl, DiscoveryServiceImpl, HealthServiceImpl, MultiShardRaftService,
        MultiShardReadService, MultiShardResolver, MultiShardWriteService,
    },
};

/// Multi-shard Ledger gRPC server.
///
/// Combines multi-shard read/write services with the MultiRaftManager
/// for routing requests to the correct shard. Supports graceful shutdown
/// via a `shutdown_rx` watch channel.
#[derive(bon::Builder)]
#[builder(on(_, required))]
pub struct MultiShardLedgerServer {
    /// The multi-raft manager containing all shards.
    manager: Arc<MultiRaftManager>,
    /// Idempotency cache for duplicate detection.
    #[builder(default = Arc::new(IdempotencyCache::new()))]
    idempotency: Arc<IdempotencyCache>,
    /// Server address.
    addr: SocketAddr,
    /// Max concurrent requests per connection.
    #[builder(default = 100)]
    max_concurrent: usize,
    /// Request timeout in seconds.
    #[builder(default = 30)]
    timeout_secs: u64,
    /// Per-namespace rate limiter (optional).
    #[builder(default)]
    namespace_rate_limiter: Option<Arc<RateLimiter>>,
    /// Hot key detector for identifying frequently accessed keys (optional).
    #[builder(default)]
    hot_key_detector: Option<Arc<crate::hot_key_detector::HotKeyDetector>>,
    /// Node health state for three-probe health checking.
    #[builder(default)]
    health_state: crate::graceful_shutdown::HealthState,
    /// Shutdown signal receiver. When `true` is sent, the server stops.
    #[builder(default)]
    shutdown_rx: Option<tokio::sync::watch::Receiver<bool>>,
    /// Maximum time to wait for Raft proposals to commit.
    ///
    /// Passed to write and admin services. If a client's gRPC deadline
    /// is shorter, the deadline takes precedence.
    #[builder(default = Duration::from_secs(30))]
    proposal_timeout: Duration,
}

impl MultiShardLedgerServer {
    /// Start the gRPC server.
    ///
    /// This method blocks until the server is shut down. If a `shutdown_rx`
    /// was provided via the builder, the server will stop when the signal
    /// is received. Otherwise, it blocks indefinitely.
    ///
    /// # Errors
    ///
    /// Returns an error if the server fails to bind to the configured address
    /// or encounters a transport-level error during operation.
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

        // Create multi-shard services with forwarding support
        let read_service =
            MultiShardReadService::with_manager(resolver.clone(), self.manager.clone());

        let write_service = MultiShardWriteService::builder()
            .resolver(resolver.clone())
            .idempotency(self.idempotency.clone())
            .rate_limiter(self.namespace_rate_limiter.clone())
            .hot_key_detector(self.hot_key_detector.clone())
            .proposal_timeout(self.proposal_timeout)
            .build();

        // Admin, Health, and Discovery services use the system shard
        // These handle global operations like namespace management
        let system_shard = self.manager.system_shard().map_err(|e| {
            Box::new(std::io::Error::other(format!("System shard not available: {}", e)))
                as Box<dyn std::error::Error>
        })?;

        let admin_service = AdminServiceImpl::builder()
            .raft(system_shard.raft().clone())
            .state(system_shard.state().clone())
            .applied_state(system_shard.applied_state().clone())
            .block_archive(Some(system_shard.block_archive().clone()))
            .listen_addr(self.addr)
            .proposal_timeout(self.proposal_timeout)
            .build();

        // Extract connection tracker before health_state is moved into HealthServiceImpl
        let connection_tracker = self.health_state.connection_tracker().clone();
        let health_service = HealthServiceImpl::new(
            system_shard.raft().clone(),
            system_shard.state().clone(),
            system_shard.applied_state().clone(),
            self.health_state,
        );

        let discovery_service = DiscoveryServiceImpl::builder()
            .raft(system_shard.raft().clone())
            .state(system_shard.state().clone())
            .applied_state(system_shard.applied_state().clone())
            .build();

        // Multi-shard Raft service routes inter-node RPCs to the correct shard
        let raft_service = MultiShardRaftService::new(self.manager.clone());

        tracing::info!(
            addr = %self.addr,
            shards = self.manager.list_shards().len(),
            "Starting multi-shard Ledger gRPC server"
        );

        let router = Server::builder()
            // Track in-flight requests for connection draining during shutdown
            .layer(ConnectionTrackingLayer::new(connection_tracker))
            .layer(layer)
            // API version response header on all responses
            .layer(ApiVersionLayer)
            // Client-facing services validate x-ledger-api-version request header.
            // Health, Discovery, and Raft services are exempted — they are
            // infrastructure endpoints used by probes and inter-node communication.
            .add_service(ReadServiceServer::with_interceptor(read_service, api_version_interceptor))
            .add_service(WriteServiceServer::with_interceptor(
                write_service,
                api_version_interceptor,
            ))
            .add_service(AdminServiceServer::with_interceptor(
                admin_service,
                api_version_interceptor,
            ))
            .add_service(HealthServiceServer::new(health_service))
            .add_service(SystemDiscoveryServiceServer::new(discovery_service))
            .add_service(RaftServiceServer::new(raft_service));

        if let Some(mut shutdown_rx) = self.shutdown_rx {
            router
                .serve_with_shutdown(self.addr, async move {
                    let _ = shutdown_rx.wait_for(|v| *v).await;
                    tracing::info!("Shutdown signal received, stopping multi-shard gRPC server");
                })
                .await?;
        } else {
            router.serve(self.addr).await?;
        }

        Ok(())
    }

    /// Get the multi-raft manager.
    #[must_use]
    pub fn manager(&self) -> &Arc<MultiRaftManager> {
        &self.manager
    }

    /// Get the idempotency cache.
    #[must_use]
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
