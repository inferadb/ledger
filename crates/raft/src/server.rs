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
use parking_lot::RwLock;
use tokio::sync::broadcast;
use tonic::transport::Server;
use tower::ServiceBuilder;

use crate::proto::admin_service_server::AdminServiceServer;
use crate::proto::health_service_server::HealthServiceServer;
use crate::proto::read_service_server::ReadServiceServer;
use crate::proto::system_discovery_service_server::SystemDiscoveryServiceServer;
use crate::proto::write_service_server::WriteServiceServer;
use crate::proto::BlockAnnouncement;
use crate::services::{
    AdminServiceImpl, DiscoveryServiceImpl, HealthServiceImpl, ReadServiceImpl, WriteServiceImpl,
};
use crate::types::LedgerTypeConfig;
use crate::IdempotencyCache;

use ledger_storage::StateLayer;

/// The main Ledger gRPC server.
///
/// Combines all services with the Raft consensus layer and state storage.
pub struct LedgerServer {
    /// The Raft consensus instance.
    raft: Arc<Raft<LedgerTypeConfig>>,
    /// The shared state layer.
    state: Arc<RwLock<StateLayer>>,
    /// Idempotency cache for duplicate detection.
    idempotency: Arc<IdempotencyCache>,
    /// Block announcement broadcast channel.
    block_announcements: broadcast::Sender<BlockAnnouncement>,
    /// Server address.
    addr: SocketAddr,
}

impl LedgerServer {
    /// Create a new Ledger server.
    pub fn new(
        raft: Arc<Raft<LedgerTypeConfig>>,
        state: Arc<RwLock<StateLayer>>,
        addr: SocketAddr,
    ) -> Self {
        // Create broadcast channel for block announcements (capacity 1000)
        let (block_announcements, _) = broadcast::channel(1000);

        Self {
            raft,
            state,
            idempotency: Arc::new(IdempotencyCache::new()),
            block_announcements,
            addr,
        }
    }

    /// Start the gRPC server.
    ///
    /// This method blocks until the server is shut down.
    pub async fn serve(self) -> Result<(), Box<dyn std::error::Error>> {
        // Configure backpressure with tower layers
        let layer = ServiceBuilder::new()
            // Limit concurrent requests per connection
            .concurrency_limit(100)
            // Reject new requests when overloaded (returns 503)
            .load_shed()
            // Set timeout for requests
            .timeout(Duration::from_secs(30))
            .into_inner();

        // Create service implementations
        let read_service = ReadServiceImpl::new(
            self.state.clone(),
            self.block_announcements.clone(),
        );
        let write_service = WriteServiceImpl::new(
            self.raft.clone(),
            self.idempotency.clone(),
        );
        let admin_service = AdminServiceImpl::new(
            self.raft.clone(),
            self.state.clone(),
        );
        let health_service = HealthServiceImpl::new(
            self.raft.clone(),
            self.state.clone(),
        );
        let discovery_service = DiscoveryServiceImpl::new(
            self.raft.clone(),
            self.state.clone(),
        );

        tracing::info!("Starting Ledger gRPC server on {}", self.addr);

        Server::builder()
            .layer(layer)
            .add_service(ReadServiceServer::new(read_service))
            .add_service(WriteServiceServer::new(write_service))
            .add_service(AdminServiceServer::new(admin_service))
            .add_service(HealthServiceServer::new(health_service))
            .add_service(SystemDiscoveryServiceServer::new(discovery_service))
            .serve(self.addr)
            .await?;

        Ok(())
    }

    /// Get the Raft instance.
    pub fn raft(&self) -> &Arc<Raft<LedgerTypeConfig>> {
        &self.raft
    }

    /// Get the state layer.
    pub fn state(&self) -> &Arc<RwLock<StateLayer>> {
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
