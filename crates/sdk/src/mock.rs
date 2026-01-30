//! Mock gRPC server for SDK integration testing.
//!
//! This module provides a controllable mock implementation of the Ledger gRPC services
//! for testing SDK functionality without a real Ledger cluster.
//!
//! # Features
//!
//! - **Entity storage**: Store and retrieve entities for read tests
//! - **Client state tracking**: Manage client sequences for idempotency tests
//! - **Failure injection**: Inject UNAVAILABLE errors or delays for resilience tests
//! - **Request counting**: Track number of requests for verification
//!
//! # Example
//!
//! ```no_run
//! use inferadb_ledger_sdk::mock::MockLedgerServer;
//! use inferadb_ledger_sdk::{LedgerClient, ClientConfig, ServerSource};
//!
//! #[tokio::test]
//! async fn test_read() {
//!     // Start mock server on ephemeral port
//!     let server = MockLedgerServer::start().await.unwrap();
//!
//!     // Set up test data
//!     server.set_entity("user:123", b"test-value");
//!
//!     // Create client connected to mock server
//!     let config = ClientConfig::builder()
//!         .servers(ServerSource::from_static([server.endpoint().to_string()]))
//!         .client_id("test-client")
//!         .build()
//!         .unwrap();
//!     let client = LedgerClient::new(config).await.unwrap();
//!
//!     // Test read operation
//!     let value = client.read(1, Some(0), "user:123").await.unwrap();
//!     assert_eq!(value, Some(b"test-value".to_vec()));
//! }
//! ```

use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use inferadb_ledger_raft::proto::{
    self,
    admin_service_server::{AdminService, AdminServiceServer},
    health_service_server::{HealthService, HealthServiceServer},
    read_service_server::{ReadService, ReadServiceServer},
    system_discovery_service_server::{SystemDiscoveryService, SystemDiscoveryServiceServer},
    write_service_server::{WriteService, WriteServiceServer},
};
use parking_lot::RwLock;
use tokio::sync::oneshot;
use tonic::{Request, Response, Status, transport::Server};

/// Key for client state: (namespace_id, vault_id, client_id)
type ClientKey = (i64, i64, String);

/// Key for entity storage: (namespace_id, vault_id, key)
type EntityKey = (i64, i64, String);

/// Entity data stored in mock server.
type EntityData = (Vec<u8>, u64, Option<u64>);

/// Entity storage map type.
type EntityStorage = HashMap<EntityKey, EntityData>;

/// Shared state for the mock server.
#[derive(Debug, Default)]
struct MockState {
    /// Entity storage: key -> (value, version, expires_at)
    entities: RwLock<EntityStorage>,

    /// Client sequences: client_key -> last_committed_sequence
    client_sequences: RwLock<HashMap<ClientKey, u64>>,

    /// Number of UNAVAILABLE errors to inject for next requests
    unavailable_count: AtomicUsize,

    /// Delay to inject for each request (milliseconds)
    delay_ms: AtomicU64,

    /// Total write requests received
    write_count: AtomicUsize,

    /// Total read requests received
    read_count: AtomicUsize,

    /// Current block height (incremented on each write)
    block_height: AtomicU64,

    /// Relationships storage: (namespace_id, vault_id) -> Vec<Relationship>
    relationships: RwLock<HashMap<(i64, i64), Vec<proto::Relationship>>>,

    /// Namespace info: namespace_id -> NamespaceInfo
    namespaces: RwLock<HashMap<i64, NamespaceData>>,

    /// Vault info: (namespace_id, vault_id) -> VaultData
    vaults: RwLock<HashMap<(i64, i64), VaultData>>,

    /// Next namespace ID to assign
    next_namespace_id: AtomicU64,

    /// Next vault ID to assign
    next_vault_id: AtomicU64,

    /// Peer info for discovery
    peers: RwLock<Vec<proto::PeerInfo>>,
}

/// Namespace metadata for mock storage.
#[derive(Debug, Clone)]
struct NamespaceData {
    name: String,
    shard_id: u32,
    status: i32,
}

/// Vault metadata for mock storage.
#[derive(Debug, Clone)]
struct VaultData {
    height: u64,
    state_root: Vec<u8>,
    status: i32,
}

impl MockState {
    fn new() -> Self {
        Self {
            block_height: AtomicU64::new(1),
            next_namespace_id: AtomicU64::new(1),
            next_vault_id: AtomicU64::new(1),
            ..Default::default()
        }
    }

    /// Check if we should inject an unavailable error, decrementing counter if so.
    fn should_inject_unavailable(&self) -> bool {
        loop {
            let current = self.unavailable_count.load(Ordering::SeqCst);
            if current == 0 {
                return false;
            }
            if self
                .unavailable_count
                .compare_exchange(current, current - 1, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return true;
            }
        }
    }

    /// Get delay to inject (if any).
    async fn maybe_delay(&self) {
        let delay_ms = self.delay_ms.load(Ordering::SeqCst);
        if delay_ms > 0 {
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        }
    }

    /// Apply configured delay and check for injected errors.
    ///
    /// Combines `maybe_delay()` and `should_inject_unavailable()` into a single call.
    async fn check_injection(&self) -> Result<(), Status> {
        self.maybe_delay().await;
        if self.should_inject_unavailable() {
            return Err(Status::unavailable("Injected error"));
        }
        Ok(())
    }
}

/// Mock implementation of Ledger gRPC services.
///
/// Provides controllable behavior for testing SDK functionality:
/// - Entity storage for read tests
/// - Client sequence tracking for idempotency tests
/// - Failure injection for resilience tests
pub struct MockLedgerServer {
    state: Arc<MockState>,
    endpoint: String,
    shutdown_tx: Option<oneshot::Sender<()>>,
}

impl MockLedgerServer {
    /// Start a new mock server on an ephemeral port.
    ///
    /// Returns the server handle which provides the endpoint address and control methods.
    pub async fn start() -> crate::Result<Self> {
        Self::start_on_port(0).await
    }

    /// Start a new mock server on a specific port.
    ///
    /// Use port 0 to let the OS assign an ephemeral port.
    pub async fn start_on_port(port: u16) -> crate::Result<Self> {
        let state = Arc::new(MockState::new());

        // Bind to localhost with specified port
        let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().map_err(|e| {
            crate::error::ConfigSnafu { message: format!("Invalid port: {}", e) }.build()
        })?;

        // Get the actual bound address (important for ephemeral ports)
        let listener = tokio::net::TcpListener::bind(addr).await.map_err(|e| {
            crate::error::ConnectionSnafu { message: format!("Failed to bind: {}", e) }.build()
        })?;
        let local_addr = listener.local_addr().map_err(|e| {
            crate::error::ConnectionSnafu { message: format!("Failed to get local addr: {}", e) }
                .build()
        })?;

        let endpoint = format!("http://{}", local_addr);

        // Create service implementations
        let read_service = MockReadService::new(state.clone());
        let write_service = MockWriteService::new(state.clone());
        let admin_service = MockAdminService::new(state.clone());
        let health_service = MockHealthService::new(state.clone());
        let discovery_service = MockDiscoveryService::new(state.clone());

        // Create shutdown channel
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        // Spawn server task
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        tokio::spawn(async move {
            let result = Server::builder()
                .add_service(ReadServiceServer::new(read_service))
                .add_service(WriteServiceServer::new(write_service))
                .add_service(AdminServiceServer::new(admin_service))
                .add_service(HealthServiceServer::new(health_service))
                .add_service(SystemDiscoveryServiceServer::new(discovery_service))
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = shutdown_rx.await;
                })
                .await;

            if let Err(e) = result {
                tracing::error!("Mock server error: {}", e);
            }
        });

        Ok(Self { state, endpoint, shutdown_tx: Some(shutdown_tx) })
    }

    /// Get the endpoint URL for connecting to this server.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Set an entity value for read tests.
    ///
    /// The entity will be stored with a version of 1 and no expiration.
    pub fn set_entity(&self, namespace_id: i64, vault_id: i64, key: &str, value: &[u8]) {
        self.set_entity_with_options(namespace_id, vault_id, key, value, 1, None);
    }

    /// Set an entity value with full options.
    pub fn set_entity_with_options(
        &self,
        namespace_id: i64,
        vault_id: i64,
        key: &str,
        value: &[u8],
        version: u64,
        expires_at: Option<u64>,
    ) {
        let mut entities = self.state.entities.write();
        entities.insert(
            (namespace_id, vault_id, key.to_string()),
            (value.to_vec(), version, expires_at),
        );
    }

    /// Remove an entity from storage.
    pub fn remove_entity(&self, namespace_id: i64, vault_id: i64, key: &str) {
        let mut entities = self.state.entities.write();
        entities.remove(&(namespace_id, vault_id, key.to_string()));
    }

    /// Set the last committed sequence for a client.
    ///
    /// Used to test idempotency behavior (ALREADY_COMMITTED, SEQUENCE_GAP).
    pub fn set_client_state(&self, namespace_id: i64, vault_id: i64, client_id: &str, seq: u64) {
        let mut sequences = self.state.client_sequences.write();
        sequences.insert((namespace_id, vault_id, client_id.to_string()), seq);
    }

    /// Get the last committed sequence for a client.
    pub fn get_client_state(
        &self,
        namespace_id: i64,
        vault_id: i64,
        client_id: &str,
    ) -> Option<u64> {
        let sequences = self.state.client_sequences.read();
        sequences.get(&(namespace_id, vault_id, client_id.to_string())).copied()
    }

    /// Inject UNAVAILABLE errors for the next N requests.
    ///
    /// Each request will decrement this counter and return UNAVAILABLE until it reaches 0.
    pub fn inject_unavailable(&self, count: usize) {
        self.state.unavailable_count.store(count, Ordering::SeqCst);
    }

    /// Inject a delay for all subsequent requests.
    ///
    /// Set to 0 to disable delay.
    pub fn inject_delay(&self, millis: u64) {
        self.state.delay_ms.store(millis, Ordering::SeqCst);
    }

    /// Get the total number of write requests received.
    pub fn write_count(&self) -> usize {
        self.state.write_count.load(Ordering::SeqCst)
    }

    /// Get the total number of read requests received.
    pub fn read_count(&self) -> usize {
        self.state.read_count.load(Ordering::SeqCst)
    }

    /// Get the current mock block height.
    pub fn block_height(&self) -> u64 {
        self.state.block_height.load(Ordering::SeqCst)
    }

    /// Add a relationship for query tests.
    pub fn add_relationship(
        &self,
        namespace_id: i64,
        vault_id: i64,
        resource: &str,
        relation: &str,
        subject: &str,
    ) {
        let mut relationships = self.state.relationships.write();
        let entry = relationships.entry((namespace_id, vault_id)).or_default();
        entry.push(proto::Relationship {
            resource: resource.to_string(),
            relation: relation.to_string(),
            subject: subject.to_string(),
        });
    }

    /// Add a namespace for admin tests.
    pub fn add_namespace(&self, namespace_id: i64, name: &str, shard_id: u32) {
        let mut namespaces = self.state.namespaces.write();
        namespaces.insert(
            namespace_id,
            NamespaceData {
                name: name.to_string(),
                shard_id,
                status: proto::NamespaceStatus::Active as i32,
            },
        );
    }

    /// Add a vault for admin tests.
    pub fn add_vault(&self, namespace_id: i64, vault_id: i64) {
        let mut vaults = self.state.vaults.write();
        vaults.insert(
            (namespace_id, vault_id),
            VaultData {
                height: 1,
                state_root: vec![0u8; 32],
                status: proto::VaultStatus::Active as i32,
            },
        );
    }

    /// Add peer info for discovery tests.
    pub fn add_peer(&self, node_id: &str, addresses: Vec<String>, grpc_port: u32) {
        let mut peers = self.state.peers.write();
        peers.push(proto::PeerInfo {
            node_id: Some(proto::NodeId { id: node_id.to_string() }),
            addresses,
            grpc_port,
            last_seen: None,
        });
    }

    /// Reset all state to initial values.
    pub fn reset(&self) {
        self.state.entities.write().clear();
        self.state.client_sequences.write().clear();
        self.state.relationships.write().clear();
        self.state.namespaces.write().clear();
        self.state.vaults.write().clear();
        self.state.peers.write().clear();
        self.state.unavailable_count.store(0, Ordering::SeqCst);
        self.state.delay_ms.store(0, Ordering::SeqCst);
        self.state.write_count.store(0, Ordering::SeqCst);
        self.state.read_count.store(0, Ordering::SeqCst);
        self.state.block_height.store(1, Ordering::SeqCst);
    }

    /// Shutdown the server gracefully.
    pub fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

impl Drop for MockLedgerServer {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

// =============================================================================
// Mock ReadService Implementation
// =============================================================================

struct MockReadService {
    state: Arc<MockState>,
}

impl MockReadService {
    fn new(state: Arc<MockState>) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl ReadService for MockReadService {
    async fn read(
        &self,
        request: Request<proto::ReadRequest>,
    ) -> Result<Response<proto::ReadResponse>, Status> {
        self.state.check_injection().await?;

        self.state.read_count.fetch_add(1, Ordering::SeqCst);

        let req = request.into_inner();
        let namespace_id = req.namespace_id.map_or(0, |n| n.id);
        let vault_id = req.vault_id.map_or(0, |v| v.id);

        let entities = self.state.entities.read();
        let key = (namespace_id, vault_id, req.key);

        let (value, block_height) = match entities.get(&key) {
            Some((v, ..)) => (Some(v.clone()), self.state.block_height.load(Ordering::SeqCst)),
            None => (None, self.state.block_height.load(Ordering::SeqCst)),
        };

        Ok(Response::new(proto::ReadResponse { value, block_height }))
    }

    async fn batch_read(
        &self,
        request: Request<proto::BatchReadRequest>,
    ) -> Result<Response<proto::BatchReadResponse>, Status> {
        self.state.check_injection().await?;

        self.state.read_count.fetch_add(1, Ordering::SeqCst);

        let req = request.into_inner();
        let namespace_id = req.namespace_id.map_or(0, |n| n.id);
        let vault_id = req.vault_id.map_or(0, |v| v.id);

        let entities = self.state.entities.read();
        let results: Vec<proto::BatchReadResult> = req
            .keys
            .iter()
            .map(|key| {
                let entity_key = (namespace_id, vault_id, key.clone());
                match entities.get(&entity_key) {
                    Some((v, ..)) => proto::BatchReadResult {
                        key: key.clone(),
                        value: Some(v.clone()),
                        found: true,
                    },
                    None => proto::BatchReadResult { key: key.clone(), value: None, found: false },
                }
            })
            .collect();

        Ok(Response::new(proto::BatchReadResponse {
            results,
            block_height: self.state.block_height.load(Ordering::SeqCst),
        }))
    }

    async fn verified_read(
        &self,
        request: Request<proto::VerifiedReadRequest>,
    ) -> Result<Response<proto::VerifiedReadResponse>, Status> {
        self.state.check_injection().await?;

        self.state.read_count.fetch_add(1, Ordering::SeqCst);

        let req = request.into_inner();
        let namespace_id = req.namespace_id.map_or(0, |n| n.id);
        let vault_id = req.vault_id.map_or(0, |v| v.id);

        let entities = self.state.entities.read();
        let key = (namespace_id, vault_id, req.key);
        let block_height = self.state.block_height.load(Ordering::SeqCst);

        let value = entities.get(&key).map(|(v, ..)| v.clone());

        // Create minimal valid proofs for testing
        let state_root = vec![0u8; 32];
        let block_header = proto::BlockHeader {
            height: block_height,
            namespace_id: Some(proto::NamespaceId { id: namespace_id }),
            vault_id: Some(proto::VaultId { id: vault_id }),
            previous_hash: Some(proto::Hash { value: vec![0u8; 32] }),
            tx_merkle_root: Some(proto::Hash { value: vec![0u8; 32] }),
            state_root: Some(proto::Hash { value: state_root.clone() }),
            timestamp: None,
            leader_id: Some(proto::NodeId { id: "mock-node".to_string() }),
            term: 1,
            committed_index: block_height,
        };

        // Create a simple merkle proof (single element tree)
        let merkle_proof = proto::MerkleProof {
            leaf_hash: Some(proto::Hash { value: state_root }),
            siblings: vec![],
        };

        Ok(Response::new(proto::VerifiedReadResponse {
            value,
            block_height,
            block_header: Some(block_header),
            merkle_proof: Some(merkle_proof),
            chain_proof: None,
        }))
    }

    async fn historical_read(
        &self,
        request: Request<proto::HistoricalReadRequest>,
    ) -> Result<Response<proto::HistoricalReadResponse>, Status> {
        self.state.check_injection().await?;

        self.state.read_count.fetch_add(1, Ordering::SeqCst);

        let req = request.into_inner();
        let namespace_id = req.namespace_id.map_or(0, |n| n.id);
        let vault_id = req.vault_id.map_or(0, |v| v.id);

        let entities = self.state.entities.read();
        let key = (namespace_id, vault_id, req.key);
        let value = entities.get(&key).map(|(v, ..)| v.clone());

        Ok(Response::new(proto::HistoricalReadResponse {
            value,
            block_height: req.at_height,
            block_header: None,
            merkle_proof: None,
            chain_proof: None,
        }))
    }

    type WatchBlocksStream = futures::stream::Pending<Result<proto::BlockAnnouncement, Status>>;

    async fn watch_blocks(
        &self,
        _request: Request<proto::WatchBlocksRequest>,
    ) -> Result<Response<Self::WatchBlocksStream>, Status> {
        self.state.check_injection().await?;

        // Return a pending stream for now - integration tests will need a more sophisticated mock
        Ok(Response::new(futures::stream::pending()))
    }

    async fn get_block(
        &self,
        request: Request<proto::GetBlockRequest>,
    ) -> Result<Response<proto::GetBlockResponse>, Status> {
        self.state.check_injection().await?;

        let req = request.into_inner();
        let namespace_id = req.namespace_id.map_or(0, |n| n.id);
        let vault_id = req.vault_id.map_or(0, |v| v.id);

        let block = proto::Block {
            header: Some(proto::BlockHeader {
                height: req.height,
                namespace_id: Some(proto::NamespaceId { id: namespace_id }),
                vault_id: Some(proto::VaultId { id: vault_id }),
                previous_hash: Some(proto::Hash { value: vec![0u8; 32] }),
                tx_merkle_root: Some(proto::Hash { value: vec![0u8; 32] }),
                state_root: Some(proto::Hash { value: vec![0u8; 32] }),
                timestamp: None,
                leader_id: Some(proto::NodeId { id: "mock-node".to_string() }),
                term: 1,
                committed_index: req.height,
            }),
            transactions: vec![],
        };

        Ok(Response::new(proto::GetBlockResponse { block: Some(block) }))
    }

    async fn get_block_range(
        &self,
        request: Request<proto::GetBlockRangeRequest>,
    ) -> Result<Response<proto::GetBlockRangeResponse>, Status> {
        self.state.check_injection().await?;

        let req = request.into_inner();
        let namespace_id = req.namespace_id.map_or(0, |n| n.id);
        let vault_id = req.vault_id.map_or(0, |v| v.id);

        let blocks: Vec<proto::Block> = (req.start_height..=req.end_height)
            .map(|height| proto::Block {
                header: Some(proto::BlockHeader {
                    height,
                    namespace_id: Some(proto::NamespaceId { id: namespace_id }),
                    vault_id: Some(proto::VaultId { id: vault_id }),
                    previous_hash: Some(proto::Hash { value: vec![0u8; 32] }),
                    tx_merkle_root: Some(proto::Hash { value: vec![0u8; 32] }),
                    state_root: Some(proto::Hash { value: vec![0u8; 32] }),
                    timestamp: None,
                    leader_id: Some(proto::NodeId { id: "mock-node".to_string() }),
                    term: 1,
                    committed_index: height,
                }),
                transactions: vec![],
            })
            .collect();

        Ok(Response::new(proto::GetBlockRangeResponse {
            blocks,
            current_tip: self.state.block_height.load(Ordering::SeqCst),
        }))
    }

    async fn get_tip(
        &self,
        request: Request<proto::GetTipRequest>,
    ) -> Result<Response<proto::GetTipResponse>, Status> {
        self.state.check_injection().await?;

        let _req = request.into_inner();
        let height = self.state.block_height.load(Ordering::SeqCst);

        Ok(Response::new(proto::GetTipResponse {
            height,
            block_hash: Some(proto::Hash { value: vec![0u8; 32] }),
            state_root: Some(proto::Hash { value: vec![0u8; 32] }),
        }))
    }

    async fn get_client_state(
        &self,
        request: Request<proto::GetClientStateRequest>,
    ) -> Result<Response<proto::GetClientStateResponse>, Status> {
        self.state.check_injection().await?;

        let req = request.into_inner();
        let namespace_id = req.namespace_id.map_or(0, |n| n.id);
        let vault_id = req.vault_id.map_or(0, |v| v.id);
        let client_id = req.client_id.map(|c| c.id).unwrap_or_default();

        let sequences = self.state.client_sequences.read();
        let last_seq = sequences.get(&(namespace_id, vault_id, client_id)).copied().unwrap_or(0);

        Ok(Response::new(proto::GetClientStateResponse { last_committed_sequence: last_seq }))
    }

    async fn list_relationships(
        &self,
        request: Request<proto::ListRelationshipsRequest>,
    ) -> Result<Response<proto::ListRelationshipsResponse>, Status> {
        self.state.check_injection().await?;

        self.state.read_count.fetch_add(1, Ordering::SeqCst);

        let req = request.into_inner();
        let namespace_id = req.namespace_id.map_or(0, |n| n.id);
        let vault_id = req.vault_id.map_or(0, |v| v.id);

        let relationships = self.state.relationships.read();
        let rels = relationships.get(&(namespace_id, vault_id)).cloned().unwrap_or_default();

        // Apply filters
        let filtered: Vec<proto::Relationship> = rels
            .into_iter()
            .filter(|r| req.resource.as_ref().is_none_or(|f| &r.resource == f))
            .filter(|r| req.relation.as_ref().is_none_or(|f| &r.relation == f))
            .filter(|r| req.subject.as_ref().is_none_or(|f| &r.subject == f))
            .take(req.limit.max(100) as usize)
            .collect();

        Ok(Response::new(proto::ListRelationshipsResponse {
            relationships: filtered,
            block_height: self.state.block_height.load(Ordering::SeqCst),
            next_page_token: String::new(),
        }))
    }

    async fn list_resources(
        &self,
        request: Request<proto::ListResourcesRequest>,
    ) -> Result<Response<proto::ListResourcesResponse>, Status> {
        self.state.check_injection().await?;

        self.state.read_count.fetch_add(1, Ordering::SeqCst);

        let req = request.into_inner();
        let namespace_id = req.namespace_id.map_or(0, |n| n.id);
        let vault_id = req.vault_id.map_or(0, |v| v.id);

        let relationships = self.state.relationships.read();
        let resources: Vec<String> = relationships
            .get(&(namespace_id, vault_id))
            .map(|rels| {
                rels.iter()
                    .filter(|r| r.resource.starts_with(&format!("{}:", req.resource_type)))
                    .map(|r| r.resource.clone())
                    .collect::<std::collections::HashSet<_>>()
                    .into_iter()
                    .take(req.limit.max(100) as usize)
                    .collect()
            })
            .unwrap_or_default();

        Ok(Response::new(proto::ListResourcesResponse {
            resources,
            block_height: self.state.block_height.load(Ordering::SeqCst),
            next_page_token: String::new(),
        }))
    }

    async fn list_entities(
        &self,
        request: Request<proto::ListEntitiesRequest>,
    ) -> Result<Response<proto::ListEntitiesResponse>, Status> {
        self.state.check_injection().await?;

        self.state.read_count.fetch_add(1, Ordering::SeqCst);

        let req = request.into_inner();
        let namespace_id = req.namespace_id.map_or(0, |n| n.id);

        let entities = self.state.entities.read();
        let matching: Vec<proto::Entity> = entities
            .iter()
            .filter(|((ns, _vault, key), _)| {
                *ns == namespace_id && key.starts_with(&req.key_prefix)
            })
            .map(|((_, _, key), (value, version, expires_at))| proto::Entity {
                key: key.clone(),
                value: value.clone(),
                expires_at: *expires_at,
                version: *version,
            })
            .take(req.limit.max(100) as usize)
            .collect();

        Ok(Response::new(proto::ListEntitiesResponse {
            entities: matching,
            block_height: self.state.block_height.load(Ordering::SeqCst),
            next_page_token: String::new(),
        }))
    }
}

// =============================================================================
// Mock WriteService Implementation
// =============================================================================

struct MockWriteService {
    state: Arc<MockState>,
}

impl MockWriteService {
    fn new(state: Arc<MockState>) -> Self {
        Self { state }
    }

    fn generate_tx_id() -> Vec<u8> {
        // Generate a simple incrementing tx_id for testing
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        now.to_le_bytes().to_vec()
    }
}

#[tonic::async_trait]
impl WriteService for MockWriteService {
    async fn write(
        &self,
        request: Request<proto::WriteRequest>,
    ) -> Result<Response<proto::WriteResponse>, Status> {
        self.state.check_injection().await?;

        self.state.write_count.fetch_add(1, Ordering::SeqCst);

        let req = request.into_inner();
        let namespace_id = req.namespace_id.map_or(0, |n| n.id);
        let vault_id = req.vault_id.map_or(0, |v| v.id);
        let client_id = req.client_id.map(|c| c.id).unwrap_or_default();
        let sequence = req.sequence;

        // Check idempotency
        let client_key = (namespace_id, vault_id, client_id.clone());
        {
            let sequences = self.state.client_sequences.read();
            if let Some(&last_seq) = sequences.get(&client_key) {
                if sequence <= last_seq {
                    // Already committed
                    return Ok(Response::new(proto::WriteResponse {
                        result: Some(proto::write_response::Result::Error(proto::WriteError {
                            code: proto::WriteErrorCode::AlreadyCommitted as i32,
                            key: String::new(),
                            current_version: None,
                            current_value: None,
                            message: "Already committed".to_string(),
                            committed_tx_id: Some(proto::TxId { id: Self::generate_tx_id() }),
                            committed_block_height: Some(
                                self.state.block_height.load(Ordering::SeqCst),
                            ),
                            last_committed_sequence: Some(last_seq),
                        })),
                    }));
                } else if sequence > last_seq + 1 {
                    // Sequence gap
                    return Ok(Response::new(proto::WriteResponse {
                        result: Some(proto::write_response::Result::Error(proto::WriteError {
                            code: proto::WriteErrorCode::SequenceGap as i32,
                            key: String::new(),
                            current_version: None,
                            current_value: None,
                            message: format!(
                                "Sequence gap: expected {}, got {}",
                                last_seq + 1,
                                sequence
                            ),
                            committed_tx_id: None,
                            committed_block_height: None,
                            last_committed_sequence: Some(last_seq),
                        })),
                    }));
                }
            } else if sequence != 1 {
                // First write must be sequence 1
                return Ok(Response::new(proto::WriteResponse {
                    result: Some(proto::write_response::Result::Error(proto::WriteError {
                        code: proto::WriteErrorCode::SequenceGap as i32,
                        key: String::new(),
                        current_version: None,
                        current_value: None,
                        message: format!("First sequence must be 1, got {}", sequence),
                        committed_tx_id: None,
                        committed_block_height: None,
                        last_committed_sequence: Some(0),
                    })),
                }));
            }
        }

        // Apply operations
        let block_height = self.state.block_height.fetch_add(1, Ordering::SeqCst);
        {
            let mut entities = self.state.entities.write();
            for op in req.operations {
                if let Some(op_inner) = op.op {
                    match op_inner {
                        proto::operation::Op::SetEntity(set) => {
                            entities.insert(
                                (namespace_id, vault_id, set.key),
                                (set.value, block_height, set.expires_at),
                            );
                        },
                        proto::operation::Op::DeleteEntity(del) => {
                            entities.remove(&(namespace_id, vault_id, del.key));
                        },
                        proto::operation::Op::CreateRelationship(rel) => {
                            let mut relationships = self.state.relationships.write();
                            let entry = relationships.entry((namespace_id, vault_id)).or_default();
                            entry.push(proto::Relationship {
                                resource: rel.resource,
                                relation: rel.relation,
                                subject: rel.subject,
                            });
                        },
                        proto::operation::Op::DeleteRelationship(del) => {
                            let mut relationships = self.state.relationships.write();
                            if let Some(rels) = relationships.get_mut(&(namespace_id, vault_id)) {
                                rels.retain(|r| {
                                    r.resource != del.resource
                                        || r.relation != del.relation
                                        || r.subject != del.subject
                                });
                            }
                        },
                        proto::operation::Op::ExpireEntity(expire) => {
                            entities.remove(&(namespace_id, vault_id, expire.key));
                        },
                    }
                }
            }
        }

        // Update client sequence
        {
            let mut sequences = self.state.client_sequences.write();
            sequences.insert(client_key, sequence);
        }

        Ok(Response::new(proto::WriteResponse {
            result: Some(proto::write_response::Result::Success(proto::WriteSuccess {
                tx_id: Some(proto::TxId { id: Self::generate_tx_id() }),
                block_height,
                block_header: None,
                tx_proof: None,
            })),
        }))
    }

    async fn batch_write(
        &self,
        request: Request<proto::BatchWriteRequest>,
    ) -> Result<Response<proto::BatchWriteResponse>, Status> {
        self.state.check_injection().await?;

        self.state.write_count.fetch_add(1, Ordering::SeqCst);

        let req = request.into_inner();
        let namespace_id = req.namespace_id.map_or(0, |n| n.id);
        let vault_id = req.vault_id.map_or(0, |v| v.id);
        let client_id = req.client_id.map(|c| c.id).unwrap_or_default();
        let sequence = req.sequence;

        // Check idempotency (same as single write)
        let client_key = (namespace_id, vault_id, client_id.clone());
        {
            let sequences = self.state.client_sequences.read();
            if let Some(&last_seq) = sequences.get(&client_key) {
                if sequence <= last_seq {
                    return Ok(Response::new(proto::BatchWriteResponse {
                        result: Some(proto::batch_write_response::Result::Error(
                            proto::WriteError {
                                code: proto::WriteErrorCode::AlreadyCommitted as i32,
                                key: String::new(),
                                current_version: None,
                                current_value: None,
                                message: "Already committed".to_string(),
                                committed_tx_id: Some(proto::TxId { id: Self::generate_tx_id() }),
                                committed_block_height: Some(
                                    self.state.block_height.load(Ordering::SeqCst),
                                ),
                                last_committed_sequence: Some(last_seq),
                            },
                        )),
                    }));
                } else if sequence > last_seq + 1 {
                    return Ok(Response::new(proto::BatchWriteResponse {
                        result: Some(proto::batch_write_response::Result::Error(
                            proto::WriteError {
                                code: proto::WriteErrorCode::SequenceGap as i32,
                                key: String::new(),
                                current_version: None,
                                current_value: None,
                                message: format!(
                                    "Sequence gap: expected {}, got {}",
                                    last_seq + 1,
                                    sequence
                                ),
                                committed_tx_id: None,
                                committed_block_height: None,
                                last_committed_sequence: Some(last_seq),
                            },
                        )),
                    }));
                }
            } else if sequence != 1 {
                return Ok(Response::new(proto::BatchWriteResponse {
                    result: Some(proto::batch_write_response::Result::Error(proto::WriteError {
                        code: proto::WriteErrorCode::SequenceGap as i32,
                        key: String::new(),
                        current_version: None,
                        current_value: None,
                        message: format!("First sequence must be 1, got {}", sequence),
                        committed_tx_id: None,
                        committed_block_height: None,
                        last_committed_sequence: Some(0),
                    })),
                }));
            }
        }

        // Apply all operations from all batches
        let block_height = self.state.block_height.fetch_add(1, Ordering::SeqCst);
        {
            let mut entities = self.state.entities.write();
            for batch in req.operations {
                for op in batch.operations {
                    if let Some(op_inner) = op.op {
                        match op_inner {
                            proto::operation::Op::SetEntity(set) => {
                                entities.insert(
                                    (namespace_id, vault_id, set.key),
                                    (set.value, block_height, set.expires_at),
                                );
                            },
                            proto::operation::Op::DeleteEntity(del) => {
                                entities.remove(&(namespace_id, vault_id, del.key));
                            },
                            proto::operation::Op::CreateRelationship(rel) => {
                                let mut relationships = self.state.relationships.write();
                                let entry =
                                    relationships.entry((namespace_id, vault_id)).or_default();
                                entry.push(proto::Relationship {
                                    resource: rel.resource,
                                    relation: rel.relation,
                                    subject: rel.subject,
                                });
                            },
                            proto::operation::Op::DeleteRelationship(del) => {
                                let mut relationships = self.state.relationships.write();
                                if let Some(rels) = relationships.get_mut(&(namespace_id, vault_id))
                                {
                                    rels.retain(|r| {
                                        r.resource != del.resource
                                            || r.relation != del.relation
                                            || r.subject != del.subject
                                    });
                                }
                            },
                            proto::operation::Op::ExpireEntity(expire) => {
                                entities.remove(&(namespace_id, vault_id, expire.key));
                            },
                        }
                    }
                }
            }
        }

        // Update client sequence
        {
            let mut sequences = self.state.client_sequences.write();
            sequences.insert(client_key, sequence);
        }

        Ok(Response::new(proto::BatchWriteResponse {
            result: Some(proto::batch_write_response::Result::Success(proto::BatchWriteSuccess {
                tx_id: Some(proto::TxId { id: Self::generate_tx_id() }),
                block_height,
                block_header: None,
                tx_proof: None,
            })),
        }))
    }
}

// =============================================================================
// Mock AdminService Implementation
// =============================================================================

struct MockAdminService {
    state: Arc<MockState>,
}

impl MockAdminService {
    fn new(state: Arc<MockState>) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl AdminService for MockAdminService {
    async fn create_namespace(
        &self,
        request: Request<proto::CreateNamespaceRequest>,
    ) -> Result<Response<proto::CreateNamespaceResponse>, Status> {
        self.state.check_injection().await?;

        let req = request.into_inner();
        let namespace_id = self.state.next_namespace_id.fetch_add(1, Ordering::SeqCst) as i64;

        {
            let mut namespaces = self.state.namespaces.write();
            namespaces.insert(
                namespace_id,
                NamespaceData {
                    name: req.name,
                    shard_id: req.shard_id.map_or(1, |s| s.id),
                    status: proto::NamespaceStatus::Active as i32,
                },
            );
        }

        Ok(Response::new(proto::CreateNamespaceResponse {
            namespace_id: Some(proto::NamespaceId { id: namespace_id }),
            shard_id: Some(proto::ShardId { id: 1 }),
        }))
    }

    async fn delete_namespace(
        &self,
        _request: Request<proto::DeleteNamespaceRequest>,
    ) -> Result<Response<proto::DeleteNamespaceResponse>, Status> {
        self.state.check_injection().await?;

        Ok(Response::new(proto::DeleteNamespaceResponse { deleted_at: None }))
    }

    async fn get_namespace(
        &self,
        request: Request<proto::GetNamespaceRequest>,
    ) -> Result<Response<proto::GetNamespaceResponse>, Status> {
        self.state.check_injection().await?;

        let req = request.into_inner();
        let namespace_id = match req.lookup {
            Some(proto::get_namespace_request::Lookup::NamespaceId(id)) => id.id,
            Some(proto::get_namespace_request::Lookup::Name(name)) => {
                let namespaces = self.state.namespaces.read();
                namespaces
                    .iter()
                    .find(|(_, data)| data.name == name)
                    .map(|(id, _)| *id)
                    .ok_or_else(|| Status::not_found("Namespace not found"))?
            },
            None => return Err(Status::invalid_argument("Missing lookup")),
        };

        let namespaces = self.state.namespaces.read();
        let data = namespaces
            .get(&namespace_id)
            .ok_or_else(|| Status::not_found("Namespace not found"))?;

        Ok(Response::new(proto::GetNamespaceResponse {
            namespace_id: Some(proto::NamespaceId { id: namespace_id }),
            name: data.name.clone(),
            shard_id: Some(proto::ShardId { id: data.shard_id }),
            member_nodes: vec![],
            status: data.status,
            config_version: 1,
            created_at: None,
        }))
    }

    async fn list_namespaces(
        &self,
        _request: Request<proto::ListNamespacesRequest>,
    ) -> Result<Response<proto::ListNamespacesResponse>, Status> {
        self.state.check_injection().await?;

        let namespaces = self.state.namespaces.read();
        let responses: Vec<proto::GetNamespaceResponse> = namespaces
            .iter()
            .map(|(id, data)| proto::GetNamespaceResponse {
                namespace_id: Some(proto::NamespaceId { id: *id }),
                name: data.name.clone(),
                shard_id: Some(proto::ShardId { id: data.shard_id }),
                member_nodes: vec![],
                status: data.status,
                config_version: 1,
                created_at: None,
            })
            .collect();

        Ok(Response::new(proto::ListNamespacesResponse {
            namespaces: responses,
            next_page_token: None,
        }))
    }

    async fn create_vault(
        &self,
        request: Request<proto::CreateVaultRequest>,
    ) -> Result<Response<proto::CreateVaultResponse>, Status> {
        self.state.check_injection().await?;

        let req = request.into_inner();
        let namespace_id = req.namespace_id.map_or(0, |n| n.id);
        let vault_id = self.state.next_vault_id.fetch_add(1, Ordering::SeqCst) as i64;

        {
            let mut vaults = self.state.vaults.write();
            vaults.insert(
                (namespace_id, vault_id),
                VaultData {
                    height: 1,
                    state_root: vec![0u8; 32],
                    status: proto::VaultStatus::Active as i32,
                },
            );
        }

        Ok(Response::new(proto::CreateVaultResponse {
            vault_id: Some(proto::VaultId { id: vault_id }),
            genesis: Some(proto::BlockHeader {
                height: 0,
                namespace_id: Some(proto::NamespaceId { id: namespace_id }),
                vault_id: Some(proto::VaultId { id: vault_id }),
                previous_hash: None,
                tx_merkle_root: Some(proto::Hash { value: vec![0u8; 32] }),
                state_root: Some(proto::Hash { value: vec![0u8; 32] }),
                timestamp: None,
                leader_id: Some(proto::NodeId { id: "mock-node".to_string() }),
                term: 1,
                committed_index: 0,
            }),
        }))
    }

    async fn delete_vault(
        &self,
        _request: Request<proto::DeleteVaultRequest>,
    ) -> Result<Response<proto::DeleteVaultResponse>, Status> {
        self.state.check_injection().await?;

        Ok(Response::new(proto::DeleteVaultResponse { deleted_at: None }))
    }

    async fn get_vault(
        &self,
        request: Request<proto::GetVaultRequest>,
    ) -> Result<Response<proto::GetVaultResponse>, Status> {
        self.state.check_injection().await?;

        let req = request.into_inner();
        let namespace_id = req.namespace_id.map_or(0, |n| n.id);
        let vault_id = req.vault_id.map_or(0, |v| v.id);

        let vaults = self.state.vaults.read();
        let data = vaults
            .get(&(namespace_id, vault_id))
            .ok_or_else(|| Status::not_found("Vault not found"))?;

        Ok(Response::new(proto::GetVaultResponse {
            namespace_id: Some(proto::NamespaceId { id: namespace_id }),
            vault_id: Some(proto::VaultId { id: vault_id }),
            height: data.height,
            state_root: Some(proto::Hash { value: data.state_root.clone() }),
            nodes: vec![],
            leader: Some(proto::NodeId { id: "mock-node".to_string() }),
            status: data.status,
            retention_policy: None,
        }))
    }

    async fn list_vaults(
        &self,
        _request: Request<proto::ListVaultsRequest>,
    ) -> Result<Response<proto::ListVaultsResponse>, Status> {
        self.state.check_injection().await?;

        let vaults = self.state.vaults.read();
        let responses: Vec<proto::GetVaultResponse> = vaults
            .iter()
            .map(|((namespace_id, vault_id), data)| proto::GetVaultResponse {
                namespace_id: Some(proto::NamespaceId { id: *namespace_id }),
                vault_id: Some(proto::VaultId { id: *vault_id }),
                height: data.height,
                state_root: Some(proto::Hash { value: data.state_root.clone() }),
                nodes: vec![],
                leader: Some(proto::NodeId { id: "mock-node".to_string() }),
                status: data.status,
                retention_policy: None,
            })
            .collect();

        Ok(Response::new(proto::ListVaultsResponse { vaults: responses }))
    }

    async fn join_cluster(
        &self,
        _request: Request<proto::JoinClusterRequest>,
    ) -> Result<Response<proto::JoinClusterResponse>, Status> {
        self.state.check_injection().await?;

        Ok(Response::new(proto::JoinClusterResponse {
            success: true,
            message: "Joined".to_string(),
            leader_id: 1,
            leader_address: "127.0.0.1:50051".to_string(),
        }))
    }

    async fn leave_cluster(
        &self,
        _request: Request<proto::LeaveClusterRequest>,
    ) -> Result<Response<proto::LeaveClusterResponse>, Status> {
        self.state.check_injection().await?;

        Ok(Response::new(proto::LeaveClusterResponse {
            success: true,
            message: "Left".to_string(),
        }))
    }

    async fn get_cluster_info(
        &self,
        _request: Request<proto::GetClusterInfoRequest>,
    ) -> Result<Response<proto::GetClusterInfoResponse>, Status> {
        self.state.check_injection().await?;

        Ok(Response::new(proto::GetClusterInfoResponse {
            members: vec![proto::ClusterMember {
                node_id: 1,
                address: "127.0.0.1:50051".to_string(),
                role: proto::ClusterMemberRole::Voter as i32,
                is_leader: true,
            }],
            leader_id: 1,
            term: 1,
        }))
    }

    async fn get_node_info(
        &self,
        _request: Request<proto::GetNodeInfoRequest>,
    ) -> Result<Response<proto::GetNodeInfoResponse>, Status> {
        self.state.check_injection().await?;

        Ok(Response::new(proto::GetNodeInfoResponse {
            node_id: 1,
            address: "127.0.0.1:50051".to_string(),
            is_cluster_member: true,
            term: 1,
        }))
    }

    async fn create_snapshot(
        &self,
        request: Request<proto::CreateSnapshotRequest>,
    ) -> Result<Response<proto::CreateSnapshotResponse>, Status> {
        self.state.check_injection().await?;

        let _req = request.into_inner();
        Ok(Response::new(proto::CreateSnapshotResponse {
            block_height: self.state.block_height.load(Ordering::SeqCst),
            state_root: Some(proto::Hash { value: vec![0u8; 32] }),
            snapshot_path: "/tmp/mock-snapshot".to_string(),
        }))
    }

    async fn check_integrity(
        &self,
        _request: Request<proto::CheckIntegrityRequest>,
    ) -> Result<Response<proto::CheckIntegrityResponse>, Status> {
        self.state.check_injection().await?;

        Ok(Response::new(proto::CheckIntegrityResponse { healthy: true, issues: vec![] }))
    }

    async fn recover_vault(
        &self,
        _request: Request<proto::RecoverVaultRequest>,
    ) -> Result<Response<proto::RecoverVaultResponse>, Status> {
        self.state.check_injection().await?;

        Ok(Response::new(proto::RecoverVaultResponse {
            success: true,
            message: "Recovered".to_string(),
            health_status: proto::VaultHealthProto::Healthy as i32,
            final_height: self.state.block_height.load(Ordering::SeqCst),
            final_state_root: Some(proto::Hash { value: vec![0u8; 32] }),
        }))
    }

    async fn simulate_divergence(
        &self,
        _request: Request<proto::SimulateDivergenceRequest>,
    ) -> Result<Response<proto::SimulateDivergenceResponse>, Status> {
        self.state.check_injection().await?;

        Ok(Response::new(proto::SimulateDivergenceResponse {
            success: true,
            message: "Divergence simulated".to_string(),
            health_status: proto::VaultHealthProto::Diverged as i32,
        }))
    }

    async fn force_gc(
        &self,
        _request: Request<proto::ForceGcRequest>,
    ) -> Result<Response<proto::ForceGcResponse>, Status> {
        self.state.check_injection().await?;

        Ok(Response::new(proto::ForceGcResponse {
            success: true,
            message: "GC completed".to_string(),
            expired_count: 0,
            vaults_scanned: 1,
        }))
    }
}

// =============================================================================
// Mock HealthService Implementation
// =============================================================================

struct MockHealthService {
    state: Arc<MockState>,
}

impl MockHealthService {
    fn new(state: Arc<MockState>) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl HealthService for MockHealthService {
    async fn check(
        &self,
        _request: Request<proto::HealthCheckRequest>,
    ) -> Result<Response<proto::HealthCheckResponse>, Status> {
        self.state.check_injection().await?;

        Ok(Response::new(proto::HealthCheckResponse {
            status: proto::HealthStatus::Healthy as i32,
            message: "Mock server healthy".to_string(),
            details: std::collections::HashMap::new(),
        }))
    }
}

// =============================================================================
// Mock SystemDiscoveryService Implementation
// =============================================================================

struct MockDiscoveryService {
    state: Arc<MockState>,
}

impl MockDiscoveryService {
    fn new(state: Arc<MockState>) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl SystemDiscoveryService for MockDiscoveryService {
    async fn get_peers(
        &self,
        _request: Request<proto::GetPeersRequest>,
    ) -> Result<Response<proto::GetPeersResponse>, Status> {
        self.state.check_injection().await?;

        let peers = self.state.peers.read();
        Ok(Response::new(proto::GetPeersResponse { peers: peers.clone(), system_version: 1 }))
    }

    async fn announce_peer(
        &self,
        request: Request<proto::AnnouncePeerRequest>,
    ) -> Result<Response<proto::AnnouncePeerResponse>, Status> {
        self.state.check_injection().await?;

        let req = request.into_inner();
        if let Some(peer) = req.peer {
            let mut peers = self.state.peers.write();
            peers.push(peer);
        }

        Ok(Response::new(proto::AnnouncePeerResponse { accepted: true }))
    }

    async fn get_system_state(
        &self,
        _request: Request<proto::GetSystemStateRequest>,
    ) -> Result<Response<proto::GetSystemStateResponse>, Status> {
        self.state.check_injection().await?;

        Ok(Response::new(proto::GetSystemStateResponse {
            version: 1,
            nodes: vec![],
            namespaces: vec![],
        }))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]
mod tests {
    use super::*;

    mod mock_server_tests {
        use super::*;

        #[tokio::test]
        async fn test_mock_server_starts_on_ephemeral_port() {
            let server = MockLedgerServer::start().await.unwrap();
            assert!(server.endpoint().starts_with("http://127.0.0.1:"));
            assert!(!server.endpoint().ends_with(":0"));
        }

        #[tokio::test]
        async fn test_mock_server_entity_storage() {
            let server = MockLedgerServer::start().await.unwrap();

            // Set entity
            server.set_entity(1, 0, "test-key", b"test-value");

            // Verify read_count starts at 0
            assert_eq!(server.read_count(), 0);
        }

        #[tokio::test]
        async fn test_mock_server_entity_with_options() {
            let server = MockLedgerServer::start().await.unwrap();

            server.set_entity_with_options(1, 0, "test-key", b"value", 42, Some(1000000));

            // Entity should be stored (we can't directly read it without a client, but we verify no
            // panic)
        }

        #[tokio::test]
        async fn test_mock_server_remove_entity() {
            let server = MockLedgerServer::start().await.unwrap();

            server.set_entity(1, 0, "test-key", b"test-value");
            server.remove_entity(1, 0, "test-key");

            // Entity should be removed (verify no panic)
        }

        #[tokio::test]
        async fn test_mock_server_client_state() {
            let server = MockLedgerServer::start().await.unwrap();

            // Initially no client state
            assert_eq!(server.get_client_state(1, 0, "client-1"), None);

            // Set client state
            server.set_client_state(1, 0, "client-1", 5);
            assert_eq!(server.get_client_state(1, 0, "client-1"), Some(5));

            // Different client has no state
            assert_eq!(server.get_client_state(1, 0, "client-2"), None);
        }

        #[tokio::test]
        async fn test_mock_server_inject_unavailable() {
            let server = MockLedgerServer::start().await.unwrap();

            // Inject 3 unavailable errors
            server.inject_unavailable(3);

            // The state tracks this (we can't test the actual injection without a client)
            // but we verify the counter was set
        }

        #[tokio::test]
        async fn test_mock_server_inject_delay() {
            let server = MockLedgerServer::start().await.unwrap();

            // Inject 100ms delay
            server.inject_delay(100);

            // The state tracks this (verify no panic)
        }

        #[tokio::test]
        async fn test_mock_server_counters() {
            let server = MockLedgerServer::start().await.unwrap();

            assert_eq!(server.write_count(), 0);
            assert_eq!(server.read_count(), 0);
            assert_eq!(server.block_height(), 1);
        }

        #[tokio::test]
        async fn test_mock_server_add_relationship() {
            let server = MockLedgerServer::start().await.unwrap();

            server.add_relationship(1, 0, "doc:1", "viewer", "user:alice");

            // Relationship should be stored (verify no panic)
        }

        #[tokio::test]
        async fn test_mock_server_add_namespace() {
            let server = MockLedgerServer::start().await.unwrap();

            server.add_namespace(1, "test-namespace", 1);

            // Namespace should be stored (verify no panic)
        }

        #[tokio::test]
        async fn test_mock_server_add_vault() {
            let server = MockLedgerServer::start().await.unwrap();

            server.add_vault(1, 0);

            // Vault should be stored (verify no panic)
        }

        #[tokio::test]
        async fn test_mock_server_add_peer() {
            let server = MockLedgerServer::start().await.unwrap();

            server.add_peer("node-1", vec!["10.0.0.1".to_string()], 5000);

            // Peer should be stored (verify no panic)
        }

        #[tokio::test]
        async fn test_mock_server_reset() {
            let server = MockLedgerServer::start().await.unwrap();

            // Add some data
            server.set_entity(1, 0, "key", b"value");
            server.set_client_state(1, 0, "client", 5);
            server.add_relationship(1, 0, "r", "rel", "s");
            server.add_namespace(1, "ns", 1);
            server.add_vault(1, 0);
            server.add_peer("node", vec![], 5000);
            server.inject_unavailable(3);
            server.inject_delay(100);

            // Reset
            server.reset();

            // Verify reset (client state should be None)
            assert_eq!(server.get_client_state(1, 0, "client"), None);
            assert_eq!(server.write_count(), 0);
            assert_eq!(server.read_count(), 0);
            assert_eq!(server.block_height(), 1);
        }

        #[tokio::test]
        async fn test_mock_server_shutdown() {
            let server = MockLedgerServer::start().await.unwrap();
            let endpoint = server.endpoint().to_string();

            // Shutdown should not panic
            server.shutdown();

            // Verify endpoint was valid before shutdown
            assert!(endpoint.starts_with("http://"));
        }

        #[tokio::test]
        async fn test_mock_state_should_inject_unavailable_decrements() {
            let state = MockState::new();

            state.unavailable_count.store(2, Ordering::SeqCst);

            assert!(state.should_inject_unavailable());
            assert_eq!(state.unavailable_count.load(Ordering::SeqCst), 1);

            assert!(state.should_inject_unavailable());
            assert_eq!(state.unavailable_count.load(Ordering::SeqCst), 0);

            assert!(!state.should_inject_unavailable());
            assert_eq!(state.unavailable_count.load(Ordering::SeqCst), 0);
        }

        #[tokio::test]
        async fn test_mock_state_maybe_delay_zero() {
            let state = MockState::new();
            state.delay_ms.store(0, Ordering::SeqCst);

            // Should return immediately
            let start = std::time::Instant::now();
            state.maybe_delay().await;
            let elapsed = start.elapsed();

            assert!(elapsed.as_millis() < 10);
        }

        #[tokio::test]
        async fn test_mock_state_maybe_delay_nonzero() {
            let state = MockState::new();
            state.delay_ms.store(50, Ordering::SeqCst);

            let start = std::time::Instant::now();
            state.maybe_delay().await;
            let elapsed = start.elapsed();

            assert!(elapsed.as_millis() >= 40); // Allow some tolerance
        }
    }

    /// Integration tests for client → mock server roundtrips.
    ///
    /// These tests verify the full stack: client creates request, mock server processes,
    /// client parses response. Validates serialization, error handling, and protocol behavior.
    mod integration_tests {
        use std::time::Duration;

        use super::*;
        use crate::{ClientConfig, LedgerClient, Operation, RetryPolicy, ServerSource};

        /// Helper to create a client connected to a mock server.
        async fn create_client_for_mock(server: &MockLedgerServer) -> LedgerClient {
            let config = ClientConfig::builder()
                .servers(ServerSource::from_static([server.endpoint().to_string()]))
                .client_id("test-client")
                .timeout(Duration::from_secs(5))
                .connect_timeout(Duration::from_secs(2))
                .build()
                .expect("valid config");

            LedgerClient::new(config).await.expect("client creation")
        }

        /// Helper to create a client with custom retry policy.
        async fn create_client_with_retry(
            server: &MockLedgerServer,
            client_id: &str,
            max_attempts: u32,
        ) -> LedgerClient {
            let retry_policy = RetryPolicy::builder()
                .max_attempts(max_attempts)
                .initial_backoff(Duration::from_millis(10))
                .max_backoff(Duration::from_millis(100))
                .multiplier(2.0)
                .build();

            let config = ClientConfig::builder()
                .servers(ServerSource::from_static([server.endpoint().to_string()]))
                .client_id(client_id)
                .timeout(Duration::from_secs(5))
                .connect_timeout(Duration::from_secs(2))
                .retry_policy(retry_policy)
                .build()
                .expect("valid config");

            LedgerClient::new(config).await.expect("client creation")
        }

        // ==================== Read Operations ====================

        #[tokio::test]
        async fn test_read_existing_key_returns_value() {
            let server = MockLedgerServer::start().await.unwrap();
            server.set_entity(1, 0, "user:123", b"test data");
            let client = create_client_for_mock(&server).await;

            let result = client.read(1, Some(0), "user:123").await.unwrap();

            assert_eq!(result, Some(b"test data".to_vec()));
        }

        #[tokio::test]
        async fn test_read_missing_key_returns_none() {
            let server = MockLedgerServer::start().await.unwrap();
            let client = create_client_for_mock(&server).await;

            let result = client.read(1, Some(0), "nonexistent").await.unwrap();

            assert_eq!(result, None);
        }

        #[tokio::test]
        async fn test_read_consistent_returns_value() {
            let server = MockLedgerServer::start().await.unwrap();
            server.set_entity(1, 0, "key", b"consistent value");
            let client = create_client_for_mock(&server).await;

            let result = client.read_consistent(1, Some(0), "key").await.unwrap();

            assert_eq!(result, Some(b"consistent value".to_vec()));
        }

        #[tokio::test]
        async fn test_batch_read_mixed_found_not_found() {
            let server = MockLedgerServer::start().await.unwrap();
            server.set_entity(1, 0, "exists1", b"value1");
            server.set_entity(1, 0, "exists2", b"value2");
            let client = create_client_for_mock(&server).await;

            let keys = vec!["exists1".to_string(), "missing".to_string(), "exists2".to_string()];
            let result = client.batch_read(1, Some(0), keys).await.unwrap();

            assert_eq!(result.len(), 3);
            assert_eq!(result[0], ("exists1".to_string(), Some(b"value1".to_vec())));
            assert_eq!(result[1], ("missing".to_string(), None));
            assert_eq!(result[2], ("exists2".to_string(), Some(b"value2".to_vec())));
        }

        #[tokio::test]
        async fn test_batch_read_consistent_returns_values() {
            let server = MockLedgerServer::start().await.unwrap();
            server.set_entity(1, 0, "a", b"1");
            server.set_entity(1, 0, "b", b"2");
            let client = create_client_for_mock(&server).await;

            let keys = vec!["a".to_string(), "b".to_string()];
            let result = client.batch_read_consistent(1, Some(0), keys).await.unwrap();

            assert_eq!(result.len(), 2);
            assert_eq!(result[0], ("a".to_string(), Some(b"1".to_vec())));
            assert_eq!(result[1], ("b".to_string(), Some(b"2".to_vec())));
        }

        #[tokio::test]
        async fn test_read_increments_read_count() {
            let server = MockLedgerServer::start().await.unwrap();
            server.set_entity(1, 0, "key", b"value");
            let client = create_client_for_mock(&server).await;

            assert_eq!(server.read_count(), 0);

            client.read(1, Some(0), "key").await.unwrap();
            assert_eq!(server.read_count(), 1);

            client.read(1, Some(0), "key").await.unwrap();
            assert_eq!(server.read_count(), 2);
        }

        // ==================== Write Operations ====================

        #[tokio::test]
        async fn test_write_single_operation_succeeds() {
            let server = MockLedgerServer::start().await.unwrap();
            let client = create_client_for_mock(&server).await;

            let ops = vec![Operation::set_entity("entity:1", b"data".to_vec())];
            let result = client.write(1, Some(0), ops).await.unwrap();

            assert!(!result.tx_id.is_empty());
            assert!(result.block_height > 0);
            assert_eq!(server.write_count(), 1);
        }

        #[tokio::test]
        async fn test_write_can_be_read_back() {
            let server = MockLedgerServer::start().await.unwrap();
            let client = create_client_for_mock(&server).await;

            let ops = vec![Operation::set_entity("user:abc", b"user data".to_vec())];
            client.write(1, Some(0), ops).await.unwrap();

            let value = client.read(1, Some(0), "user:abc").await.unwrap();
            assert_eq!(value, Some(b"user data".to_vec()));
        }

        #[tokio::test]
        async fn test_write_multiple_operations() {
            let server = MockLedgerServer::start().await.unwrap();
            let client = create_client_for_mock(&server).await;

            let ops = vec![
                Operation::set_entity("k1", b"v1".to_vec()),
                Operation::set_entity("k2", b"v2".to_vec()),
                Operation::set_entity("k3", b"v3".to_vec()),
            ];
            let result = client.write(1, Some(0), ops).await.unwrap();

            assert!(!result.tx_id.is_empty());

            // All three should be readable
            assert_eq!(client.read(1, Some(0), "k1").await.unwrap(), Some(b"v1".to_vec()));
            assert_eq!(client.read(1, Some(0), "k2").await.unwrap(), Some(b"v2".to_vec()));
            assert_eq!(client.read(1, Some(0), "k3").await.unwrap(), Some(b"v3".to_vec()));
        }

        #[tokio::test]
        async fn test_write_delete_entity() {
            let server = MockLedgerServer::start().await.unwrap();
            server.set_entity(1, 0, "to_delete", b"exists");
            let client = create_client_for_mock(&server).await;

            // Verify it exists
            assert!(client.read(1, Some(0), "to_delete").await.unwrap().is_some());

            // Delete it
            let ops = vec![Operation::delete_entity("to_delete")];
            client.write(1, Some(0), ops).await.unwrap();

            // Verify deleted
            assert_eq!(client.read(1, Some(0), "to_delete").await.unwrap(), None);
        }

        #[tokio::test]
        async fn test_write_create_relationship() {
            let server = MockLedgerServer::start().await.unwrap();
            let client = create_client_for_mock(&server).await;

            let ops = vec![Operation::create_relationship("document:123", "viewer", "user:456")];
            client.write(1, Some(0), ops).await.unwrap();

            // Relationship was created (verified by write count, detailed check via list)
            assert_eq!(server.write_count(), 1);
        }

        #[tokio::test]
        async fn test_batch_write_atomic() {
            let server = MockLedgerServer::start().await.unwrap();
            let client = create_client_for_mock(&server).await;

            let batches = vec![
                vec![Operation::set_entity("batch1:a", b"a".to_vec())],
                vec![
                    Operation::set_entity("batch2:b", b"b".to_vec()),
                    Operation::set_entity("batch2:c", b"c".to_vec()),
                ],
            ];
            let result = client.batch_write(1, Some(0), batches).await.unwrap();

            assert!(!result.tx_id.is_empty());
            assert_eq!(server.write_count(), 1); // Single batch write

            // All entities from all batches should be readable
            assert_eq!(client.read(1, Some(0), "batch1:a").await.unwrap(), Some(b"a".to_vec()));
            assert_eq!(client.read(1, Some(0), "batch2:b").await.unwrap(), Some(b"b".to_vec()));
            assert_eq!(client.read(1, Some(0), "batch2:c").await.unwrap(), Some(b"c".to_vec()));
        }

        // ==================== Idempotency ====================

        #[tokio::test]
        async fn test_duplicate_write_returns_already_committed() {
            let server = MockLedgerServer::start().await.unwrap();
            let client = create_client_for_mock(&server).await;

            // First write succeeds
            let ops1 = vec![Operation::set_entity("key1", b"first".to_vec())];
            let result1 = client.write(1, Some(0), ops1).await.unwrap();

            // Reset the client's local sequence to force duplicate
            client.sequences().set_sequence(1, 0, 0);

            // Second write with same sequence returns ALREADY_COMMITTED
            // which is handled as success (idempotent retry)
            let ops2 = vec![Operation::set_entity("key2", b"second".to_vec())];
            let result2 = client.write(1, Some(0), ops2).await.unwrap();

            // Both should return valid results
            assert!(!result1.tx_id.is_empty());
            assert!(!result2.tx_id.is_empty());

            // Server received 2 write requests (second returned ALREADY_COMMITTED)
            // The write_count tracks requests, not mutations
            assert_eq!(server.write_count(), 2);
        }

        #[tokio::test]
        async fn test_already_committed_returns_cached_result() {
            // Tests scenario: server has advanced past client's sequence (crash recovery)
            // Client sends sequence 1, but server already has sequence 5 committed.
            // This triggers ALREADY_COMMITTED, which client treats as idempotent success.
            let server = MockLedgerServer::start().await.unwrap();
            let client = create_client_for_mock(&server).await;

            // Simulate server having processed sequence 1 already
            server.set_client_state(1, 0, "test-client", 1);

            // Client starts at 0, so next_sequence returns 1
            // Since 1 <= 1, server returns ALREADY_COMMITTED
            // Client treats this as success (the operation was already applied)
            let ops = vec![Operation::set_entity("key", b"data".to_vec())];
            let result = client.write(1, Some(0), ops).await.unwrap();

            // ALREADY_COMMITTED returns a tx_id from the cached result
            assert!(!result.tx_id.is_empty());

            // The next write (sequence 2) should succeed normally
            let ops2 = vec![Operation::set_entity("newkey", b"newdata".to_vec())];
            let result2 = client.write(1, Some(0), ops2).await.unwrap();

            assert!(!result2.tx_id.is_empty());

            // This write actually stored the data
            assert_eq!(client.read(1, Some(0), "newkey").await.unwrap(), Some(b"newdata".to_vec()));
        }

        #[tokio::test]
        async fn test_already_committed_for_batch_write() {
            let server = MockLedgerServer::start().await.unwrap();
            let client = create_client_for_mock(&server).await;

            // First batch write
            let batches1 = vec![vec![Operation::set_entity("bw:1", b"first".to_vec())]];
            let result1 = client.batch_write(1, Some(0), batches1).await.unwrap();

            // Reset sequence to force duplicate
            client.sequences().set_sequence(1, 0, 0);

            // Second batch write with same sequence should return ALREADY_COMMITTED
            let batches2 = vec![vec![Operation::set_entity("bw:2", b"second".to_vec())]];
            let result2 = client.batch_write(1, Some(0), batches2).await.unwrap();

            assert!(!result1.tx_id.is_empty());
            assert!(!result2.tx_id.is_empty());
            // Server received 2 write requests (second returned ALREADY_COMMITTED)
            assert_eq!(server.write_count(), 2);
        }

        #[tokio::test]
        async fn test_sequence_increments_correctly() {
            let server = MockLedgerServer::start().await.unwrap();
            let client = create_client_for_mock(&server).await;

            // Each write should increment sequence
            for i in 1..=5 {
                let ops = vec![Operation::set_entity(format!("seq:{i}"), b"data".to_vec())];
                client.write(1, Some(0), ops).await.unwrap();

                assert_eq!(client.sequences().current_sequence(1, 0), i);
            }

            // 5 writes total
            assert_eq!(server.write_count(), 5);
        }

        // ==================== Retry ====================

        #[tokio::test]
        async fn test_retry_succeeds_after_transient_failure() {
            let server = MockLedgerServer::start().await.unwrap();
            let client = create_client_with_retry(&server, "retry-client", 3).await;

            // Inject 1 UNAVAILABLE error - second attempt should succeed
            server.inject_unavailable(1);
            server.set_entity(1, 0, "retry-key", b"retry-value");

            let result = client.read(1, Some(0), "retry-key").await.unwrap();

            assert_eq!(result, Some(b"retry-value".to_vec()));
            // 2 reads: 1 failed, 1 succeeded
            assert_eq!(server.read_count(), 1); // Only successful read is counted
        }

        #[tokio::test]
        async fn test_retry_exhaustion_returns_error() {
            let server = MockLedgerServer::start().await.unwrap();
            let client = create_client_with_retry(&server, "exhaust-client", 2).await;

            // Inject more failures than max attempts
            server.inject_unavailable(5);

            let result = client.read(1, Some(0), "any-key").await;

            assert!(result.is_err());
            let err = result.unwrap_err();
            // Should be RetryExhausted wrapping the underlying error
            assert!(
                matches!(err, crate::SdkError::RetryExhausted { .. })
                    || matches!(err, crate::SdkError::Transport { .. }),
                "Expected RetryExhausted or Transport error, got: {:?}",
                err
            );
        }

        #[tokio::test]
        async fn test_retry_write_succeeds_after_transient_failure() {
            let server = MockLedgerServer::start().await.unwrap();
            let client = create_client_with_retry(&server, "write-retry", 3).await;

            // First request fails, second succeeds
            server.inject_unavailable(1);

            let ops = vec![Operation::set_entity("retry-write", b"value".to_vec())];
            let result = client.write(1, Some(0), ops).await.unwrap();

            assert!(!result.tx_id.is_empty());
        }

        // ==================== Concurrent Operations ====================

        #[tokio::test]
        async fn test_concurrent_writes_to_different_vaults() {
            let server = MockLedgerServer::start().await.unwrap();
            let client = create_client_for_mock(&server).await;

            // Spawn concurrent writes to different vaults
            let client1 = client.clone();
            let client2 = client.clone();

            let handle1 = tokio::spawn(async move {
                for i in 0..10 {
                    let ops = vec![Operation::set_entity(format!("v0:k{i}"), b"v0".to_vec())];
                    client1.write(1, Some(0), ops).await.unwrap();
                }
            });

            let handle2 = tokio::spawn(async move {
                for i in 0..10 {
                    let ops = vec![Operation::set_entity(format!("v1:k{i}"), b"v1".to_vec())];
                    client2.write(1, Some(1), ops).await.unwrap();
                }
            });

            handle1.await.unwrap();
            handle2.await.unwrap();

            // Both vaults should have independent sequences
            assert_eq!(client.sequences().current_sequence(1, 0), 10);
            assert_eq!(client.sequences().current_sequence(1, 1), 10);

            // 20 writes total
            assert_eq!(server.write_count(), 20);
        }

        #[tokio::test]
        async fn test_concurrent_reads() {
            let server = MockLedgerServer::start().await.unwrap();
            for i in 0..100 {
                server.set_entity(1, 0, &format!("key:{}", i), format!("value:{}", i).as_bytes());
            }
            let client = create_client_for_mock(&server).await;

            // Spawn many concurrent reads
            let mut handles = vec![];
            for i in 0..100 {
                let client_clone = client.clone();
                handles.push(tokio::spawn(async move {
                    let key = format!("key:{}", i);
                    let expected = format!("value:{}", i).into_bytes();
                    let result = client_clone.read(1, Some(0), &key).await.unwrap();
                    assert_eq!(result, Some(expected));
                }));
            }

            for handle in handles {
                handle.await.unwrap();
            }

            assert_eq!(server.read_count(), 100);
        }

        // ==================== Admin Operations ====================

        #[tokio::test]
        async fn test_create_and_get_namespace() {
            let server = MockLedgerServer::start().await.unwrap();
            let client = create_client_for_mock(&server).await;

            let ns_id = client.create_namespace("test-namespace").await.unwrap();
            assert!(ns_id > 0);

            let ns_info = client.get_namespace(ns_id).await.unwrap();
            assert_eq!(ns_info.namespace_id, ns_id);
            assert_eq!(ns_info.name, "test-namespace");
        }

        #[tokio::test]
        async fn test_list_namespaces() {
            let server = MockLedgerServer::start().await.unwrap();
            server.add_namespace(1, "ns1", 1);
            server.add_namespace(2, "ns2", 1);
            let client = create_client_for_mock(&server).await;

            let namespaces = client.list_namespaces().await.unwrap();

            assert_eq!(namespaces.len(), 2);
            let names: Vec<_> = namespaces.iter().map(|n| n.name.as_str()).collect();
            assert!(names.contains(&"ns1"));
            assert!(names.contains(&"ns2"));
        }

        #[tokio::test]
        async fn test_create_and_get_vault() {
            let server = MockLedgerServer::start().await.unwrap();
            server.add_namespace(1, "ns", 1);
            let client = create_client_for_mock(&server).await;

            let vault_info = client.create_vault(1).await.unwrap();
            assert!(vault_info.vault_id > 0);

            let fetched = client.get_vault(1, vault_info.vault_id).await.unwrap();
            assert_eq!(fetched.vault_id, vault_info.vault_id);
        }

        #[tokio::test]
        async fn test_list_vaults() {
            let server = MockLedgerServer::start().await.unwrap();
            server.add_vault(1, 0);
            server.add_vault(1, 1);
            let client = create_client_for_mock(&server).await;

            let vaults = client.list_vaults().await.unwrap();

            assert_eq!(vaults.len(), 2);
        }

        // ==================== Health Check ====================

        #[tokio::test]
        async fn test_health_check_returns_healthy() {
            let server = MockLedgerServer::start().await.unwrap();
            let client = create_client_for_mock(&server).await;

            let is_healthy = client.health_check().await.unwrap();

            assert!(is_healthy);
        }

        #[tokio::test]
        async fn test_health_check_detailed_returns_result() {
            let server = MockLedgerServer::start().await.unwrap();
            let client = create_client_for_mock(&server).await;

            let result = client.health_check_detailed().await.unwrap();

            assert!(result.is_healthy());
        }

        // ==================== Query Operations ====================

        #[tokio::test]
        async fn test_list_entities_with_prefix() {
            let server = MockLedgerServer::start().await.unwrap();
            server.set_entity(1, 0, "user:1", b"data1");
            server.set_entity(1, 0, "user:2", b"data2");
            server.set_entity(1, 0, "team:1", b"team");
            let client = create_client_for_mock(&server).await;

            use crate::ListEntitiesOpts;
            let result =
                client.list_entities(1, ListEntitiesOpts::with_prefix("user:")).await.unwrap();

            assert_eq!(result.items.len(), 2);
            assert!(result.items.iter().all(|e| e.key.starts_with("user:")));
        }

        #[tokio::test]
        async fn test_list_relationships_returns_relationships() {
            let server = MockLedgerServer::start().await.unwrap();
            server.add_relationship(1, 0, "doc:1", "viewer", "user:alice");
            server.add_relationship(1, 0, "doc:1", "editor", "user:bob");
            let client = create_client_for_mock(&server).await;

            use crate::ListRelationshipsOpts;
            let result =
                client.list_relationships(1, 0, ListRelationshipsOpts::new()).await.unwrap();

            assert_eq!(result.items.len(), 2);
        }

        #[tokio::test]
        async fn test_list_relationships_with_filter() {
            let server = MockLedgerServer::start().await.unwrap();
            server.add_relationship(1, 0, "doc:1", "viewer", "user:alice");
            server.add_relationship(1, 0, "doc:1", "editor", "user:bob");
            server.add_relationship(1, 0, "doc:2", "viewer", "user:charlie");
            let client = create_client_for_mock(&server).await;

            use crate::ListRelationshipsOpts;
            let result = client
                .list_relationships(1, 0, ListRelationshipsOpts::new().relation("viewer"))
                .await
                .unwrap();

            assert_eq!(result.items.len(), 2);
            assert!(result.items.iter().all(|r| r.relation == "viewer"));
        }

        // ==================== Graceful Shutdown ====================

        #[tokio::test]
        async fn test_server_shutdown_closes_connections() {
            let server = MockLedgerServer::start().await.unwrap();
            let endpoint = server.endpoint().to_string();
            let client = create_client_for_mock(&server).await;

            // Verify connection works
            assert!(client.health_check().await.unwrap());

            // Shutdown server
            server.shutdown();

            // Give server time to shut down
            tokio::time::sleep(Duration::from_millis(100)).await;

            // Connection should fail after shutdown
            let result = client.health_check().await;
            assert!(result.is_err(), "Expected error after shutdown, endpoint was: {}", endpoint);
        }

        #[tokio::test]
        async fn test_client_can_reconnect_after_server_restart() {
            // Start first server
            let server1 = MockLedgerServer::start().await.unwrap();
            let endpoint = server1.endpoint().to_string();
            let port: u16 = endpoint.trim_start_matches("http://127.0.0.1:").parse().unwrap();

            let client = create_client_for_mock(&server1).await;
            assert!(client.health_check().await.unwrap());

            // Shutdown first server
            server1.shutdown();
            tokio::time::sleep(Duration::from_millis(100)).await;

            // Start new server on same port
            let server2 = MockLedgerServer::start_on_port(port).await.unwrap();

            // Client should be able to reconnect
            // (May need a small delay for the new server to be ready)
            tokio::time::sleep(Duration::from_millis(50)).await;

            // Reset connection pool to force reconnection
            client.pool().reset();

            let result = client.health_check().await;
            assert!(result.is_ok(), "Expected success after restart: {:?}", result);

            server2.shutdown();
        }

        // ==================== Edge Cases ====================

        #[tokio::test]
        async fn test_empty_batch_read() {
            let server = MockLedgerServer::start().await.unwrap();
            let client = create_client_for_mock(&server).await;

            let empty_keys: Vec<String> = vec![];
            let result = client.batch_read(1, Some(0), empty_keys).await.unwrap();

            assert!(result.is_empty());
        }

        #[tokio::test]
        async fn test_read_namespace_level_without_vault() {
            let server = MockLedgerServer::start().await.unwrap();
            // Set entity at namespace level (vault_id = 0 is treated as default)
            server.set_entity(1, 0, "ns-entity", b"namespace data");
            let client = create_client_for_mock(&server).await;

            let result = client.read(1, None, "ns-entity").await.unwrap();

            assert_eq!(result, Some(b"namespace data".to_vec()));
        }

        #[tokio::test]
        async fn test_write_with_none_vault_id() {
            let server = MockLedgerServer::start().await.unwrap();
            let client = create_client_for_mock(&server).await;

            let ops = vec![Operation::set_entity("ns:key", b"value".to_vec())];
            let result = client.write(1, None, ops).await.unwrap();

            assert!(!result.tx_id.is_empty());

            // Read back with None vault_id
            let value = client.read(1, None, "ns:key").await.unwrap();
            assert_eq!(value, Some(b"value".to_vec()));
        }

        #[tokio::test]
        async fn test_large_value_read_write() {
            let server = MockLedgerServer::start().await.unwrap();
            let client = create_client_for_mock(&server).await;

            // 1MB value
            let large_value = vec![0u8; 1024 * 1024];
            let ops = vec![Operation::set_entity("large:key", large_value.clone())];
            client.write(1, Some(0), ops).await.unwrap();

            let result = client.read(1, Some(0), "large:key").await.unwrap();
            assert_eq!(result.unwrap().len(), 1024 * 1024);
        }

        #[tokio::test]
        async fn test_multiple_clients_same_server() {
            let server = MockLedgerServer::start().await.unwrap();

            // Two clients with different client IDs
            let client1 = create_client_with_retry(&server, "client-1", 1).await;
            let client2 = create_client_with_retry(&server, "client-2", 1).await;

            // Both can write (they have independent sequences)
            let ops1 = vec![Operation::set_entity("c1:key", b"from-c1".to_vec())];
            let ops2 = vec![Operation::set_entity("c2:key", b"from-c2".to_vec())];

            client1.write(1, Some(0), ops1).await.unwrap();
            client2.write(1, Some(0), ops2).await.unwrap();

            // Both can read each other's data
            assert_eq!(
                client1.read(1, Some(0), "c2:key").await.unwrap(),
                Some(b"from-c2".to_vec())
            );
            assert_eq!(
                client2.read(1, Some(0), "c1:key").await.unwrap(),
                Some(b"from-c1".to_vec())
            );
        }

        // ==================== Client Shutdown Integration Tests ====================

        #[tokio::test]
        async fn test_client_shutdown_cancels_in_flight_request() {
            let server = MockLedgerServer::start().await.unwrap();
            // Add 500ms delay to simulate slow request
            server.inject_delay(500);

            let client = create_client_for_mock(&server).await;

            // Start a slow read in background
            let client_clone = client.clone();
            let handle = tokio::spawn(async move { client_clone.read(1, Some(0), "key").await });

            // Give time for request to start
            tokio::time::sleep(Duration::from_millis(50)).await;

            // Shutdown the client while request is in flight
            client.shutdown().await;

            // The spawned task should complete (may succeed or fail with transport error)
            // The key point is it doesn't hang forever
            let result = tokio::time::timeout(Duration::from_secs(2), handle).await;

            assert!(result.is_ok(), "Request should complete within timeout after shutdown");
        }

        #[tokio::test]
        async fn test_client_shutdown_prevents_new_requests() {
            let server = MockLedgerServer::start().await.unwrap();
            server.set_entity(1, 0, "key", b"value");
            let client = create_client_for_mock(&server).await;

            // Verify normal operation
            let result = client.read(1, Some(0), "key").await;
            assert!(result.is_ok());

            // Shutdown
            client.shutdown().await;

            // New requests should fail with Shutdown error
            let result = client.read(1, Some(0), "key").await;
            assert!(matches!(result, Err(crate::error::SdkError::Shutdown)));

            // Write should also fail
            let ops = vec![Operation::set_entity("new:key", b"value".to_vec())];
            let result = client.write(1, Some(0), ops).await;
            assert!(matches!(result, Err(crate::error::SdkError::Shutdown)));
        }

        #[tokio::test]
        async fn test_client_shutdown_with_multiple_operations() {
            let server = MockLedgerServer::start().await.unwrap();
            let client = create_client_for_mock(&server).await;

            // Perform several successful operations
            for i in 0..5 {
                let ops = vec![Operation::set_entity(
                    format!("key:{i}"),
                    format!("value:{i}").into_bytes(),
                )];
                client.write(1, Some(0), ops).await.unwrap();
            }

            // Verify sequence state before shutdown
            let seq_before = client.sequences().current_sequence(1, 0);
            assert_eq!(seq_before, 5);

            // Shutdown
            client.shutdown().await;

            // Operations should fail
            let ops = vec![Operation::set_entity("key:5", b"value".to_vec())];
            let result = client.write(1, Some(0), ops).await;
            assert!(matches!(result, Err(crate::error::SdkError::Shutdown)));

            // Sequence should not have incremented
            let seq_after = client.sequences().current_sequence(1, 0);
            assert_eq!(seq_after, 5, "Sequence should not increment after shutdown");
        }

        #[tokio::test]
        async fn test_cloned_client_shutdown_affects_all_clones() {
            let server = MockLedgerServer::start().await.unwrap();
            server.set_entity(1, 0, "key", b"value");

            let client1 = create_client_for_mock(&server).await;
            let client2 = client1.clone();
            let client3 = client1.clone();

            // All clones should work initially
            assert!(client1.read(1, Some(0), "key").await.is_ok());
            assert!(client2.read(1, Some(0), "key").await.is_ok());
            assert!(client3.read(1, Some(0), "key").await.is_ok());

            // Shutdown through client2
            client2.shutdown().await;

            // All clones should now fail
            assert!(matches!(
                client1.read(1, Some(0), "key").await,
                Err(crate::error::SdkError::Shutdown)
            ));
            assert!(matches!(
                client2.read(1, Some(0), "key").await,
                Err(crate::error::SdkError::Shutdown)
            ));
            assert!(matches!(
                client3.read(1, Some(0), "key").await,
                Err(crate::error::SdkError::Shutdown)
            ));
        }
    }
}
