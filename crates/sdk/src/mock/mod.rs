//! Mock gRPC server for SDK integration testing.
//!
//! This module provides a controllable mock implementation of the Ledger gRPC services
//! for testing SDK functionality without a real Ledger cluster.
//!
//! # Features
//!
//! - **Entity storage**: Store and retrieve entities for read tests
//! - **Client state tracking**: Manage server-assigned sequences for idempotency tests
//! - **Failure injection**: Inject UNAVAILABLE errors or delays for resilience tests
//! - **Request counting**: Track number of requests for verification
//!
//! # Example
//!
//! ```no_run
//! use inferadb_ledger_sdk::mock::MockLedgerServer;
//! use inferadb_ledger_sdk::{LedgerClient, ClientConfig, OrganizationSlug, UserSlug, VaultSlug, ServerSource};
//!
//! #[tokio::test]
//! async fn test_read() {
//!     let organization = OrganizationSlug::new(1);
//!     let vault = VaultSlug::new(0);
//!
//!     // Start mock server on ephemeral port
//!     let server = MockLedgerServer::start().await.unwrap();
//!
//!     // Set up test data
//!     server.set_entity(organization, vault, "user:123", b"test-value");
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
//!     let value = client.read(UserSlug::new(42), organization, Some(vault), "user:123", None, None).await.unwrap();
//!     assert_eq!(value, Some(b"test-value".to_vec()));
//! }
//! ```

mod admin;
mod app;
mod discovery;
mod events;
mod health;
mod invitation;
mod organization;
mod read;
mod token;
mod user;
mod vault;
mod write;

#[cfg(test)]
mod tests;

use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use admin::MockAdminService;
use app::MockAppService;
use discovery::MockDiscoveryService;
use events::MockEventsService;
use health::MockHealthService;
use inferadb_ledger_proto::proto::{
    self, admin_service_server::AdminServiceServer, app_service_server::AppServiceServer,
    events_service_server::EventsServiceServer, health_service_server::HealthServiceServer,
    invitation_service_server::InvitationServiceServer,
    organization_service_server::OrganizationServiceServer, read_service_server::ReadServiceServer,
    system_discovery_service_server::SystemDiscoveryServiceServer,
    token_service_server::TokenServiceServer, user_service_server::UserServiceServer,
    vault_service_server::VaultServiceServer, write_service_server::WriteServiceServer,
};
use inferadb_ledger_types::{AppSlug, ClientAssertionId, OrganizationSlug, Region, VaultSlug};
use invitation::MockInvitationService;
use organization::MockOrganizationService;
use parking_lot::RwLock;
use read::MockReadService;
use token::MockTokenService;
use tokio::sync::oneshot;
use tonic::transport::Server;
use user::MockUserService;
use vault::MockVaultService;
use write::MockWriteService;

/// Key for client state: (organization slug, vault slug, client_id)
type ClientKey = (OrganizationSlug, VaultSlug, String);

/// Key for entity storage: (organization slug, vault slug, key)
type EntityKey = (OrganizationSlug, VaultSlug, String);

/// Key for idempotency cache: (organization slug, vault slug, client_id, idempotency_key)
type IdempotencyKey = (OrganizationSlug, VaultSlug, String, Vec<u8>);

/// Value for idempotency cache: (tx_id, block_height, assigned_sequence)
type IdempotencyCacheEntry = (Vec<u8>, u64, u64);

/// Entity data: (value, version, expires_at).
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

    /// Idempotency cache for deduplicating writes.
    idempotency_cache: RwLock<HashMap<IdempotencyKey, IdempotencyCacheEntry>>,

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

    /// Relationships storage: (organization slug, vault slug) -> Vec<Relationship>
    relationships: RwLock<HashMap<(OrganizationSlug, VaultSlug), Vec<proto::Relationship>>>,

    /// Organization info: organization slug -> OrganizationData
    organizations: RwLock<HashMap<OrganizationSlug, OrganizationData>>,

    /// Vault info: (organization slug, vault slug) -> VaultData
    vaults: RwLock<HashMap<(OrganizationSlug, VaultSlug), VaultData>>,

    /// Next organization ID to assign
    next_organization: AtomicU64,

    /// Next vault ID to assign
    next_vault: AtomicU64,

    /// Peer info for discovery
    peers: RwLock<Vec<proto::PeerInfo>>,

    /// Leader-update broadcast sender for `WatchLeader`. Lazily initialized
    /// on first subscriber so tests that never touch the watch path incur
    /// no cost. Capacity is 16 — watchers lagging beyond that are dropped
    /// and expected to reconnect.
    leader_watch: RwLock<Option<tokio::sync::broadcast::Sender<proto::LeaderUpdate>>>,

    /// User storage: user slug -> User proto
    users: RwLock<HashMap<u64, proto::User>>,

    /// User email storage: email_id -> UserEmail proto
    user_emails: RwLock<HashMap<i64, proto::UserEmail>>,

    /// Next user slug to assign
    next_user_slug: AtomicU64,

    /// Next user email ID to assign
    next_user_email_id: AtomicU64,

    /// Team storage: team slug -> TeamData
    teams: RwLock<HashMap<u64, TeamData>>,

    /// Next team slug to assign
    next_team: AtomicU64,

    /// Invitation storage
    invitations: RwLock<Vec<proto::Invitation>>,

    /// Next invite slug to assign
    next_invite: AtomicU64,

    /// Event storage
    events: RwLock<Vec<proto::EventEntry>>,

    /// App client assertion storage: (org, app, assertion_id) -> info.
    ///
    /// Used by `get_app_client_assertion` to resolve a single assertion by ID.
    /// The existing `list_app_client_assertions` mock returns a static
    /// fixture and does not consult this map.
    app_client_assertions: RwLock<
        HashMap<(OrganizationSlug, AppSlug, ClientAssertionId), proto::AppClientAssertionInfo>,
    >,
}

/// Member entry in mock organization storage.
#[derive(Debug, Clone)]
struct MockMember {
    slug: u64,
    role: i32,
}

impl MockMember {
    fn to_proto(&self) -> proto::OrganizationMember {
        proto::OrganizationMember {
            user: Some(proto::UserSlug { slug: self.slug }),
            role: self.role,
            joined_at: None,
        }
    }
}

/// Organization metadata for mock storage.
#[derive(Debug, Clone)]
struct OrganizationData {
    name: String,
    region: Region,
    status: i32,
    members: Vec<MockMember>,
    deleted_at: Option<std::time::SystemTime>,
}

impl OrganizationData {
    fn is_admin(&self, slug: u64) -> bool {
        self.members
            .iter()
            .any(|m| m.slug == slug && m.role == proto::OrganizationMemberRole::Admin as i32)
    }
}

/// Team metadata for mock storage.
#[derive(Debug, Clone)]
struct TeamData {
    name: String,
    org_slug: u64,
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
            next_organization: AtomicU64::new(1),
            next_vault: AtomicU64::new(1),
            next_user_slug: AtomicU64::new(1000),
            next_user_email_id: AtomicU64::new(1),
            next_team: AtomicU64::new(1),
            next_invite: AtomicU64::new(1),
            ..Default::default()
        }
    }

    /// Checks if we should inject an unavailable error, decrementing counter if so.
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

    /// Returns delay to inject (if any).
    async fn maybe_delay(&self) {
        let delay_ms = self.delay_ms.load(Ordering::SeqCst);
        if delay_ms > 0 {
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        }
    }

    /// Applies configured delay and checks for injected errors.
    ///
    /// Combines `maybe_delay()` and `should_inject_unavailable()` into a single call.
    async fn check_injection(&self) -> Result<(), tonic::Status> {
        self.maybe_delay().await;
        if self.should_inject_unavailable() {
            return Err(tonic::Status::unavailable("Injected error"));
        }
        Ok(())
    }

    /// Returns a receiver for leader watch updates. Initializes the broadcast
    /// channel on first call.
    pub(crate) fn subscribe_leader_watch(
        &self,
    ) -> tokio::sync::broadcast::Receiver<proto::LeaderUpdate> {
        let mut slot = self.leader_watch.write();
        let sender = slot.get_or_insert_with(|| tokio::sync::broadcast::channel(16).0);
        sender.subscribe()
    }

    /// Returns the current broadcast sender, if a subscriber has been
    /// registered. Tests use this to push updates to active watchers.
    pub(crate) fn leader_watch_sender(
        &self,
    ) -> Option<tokio::sync::broadcast::Sender<proto::LeaderUpdate>> {
        self.leader_watch.read().clone()
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
    /// Starts a new mock server on an ephemeral port.
    ///
    /// Returns the server handle which provides the endpoint address and control methods.
    ///
    /// # Errors
    ///
    /// Returns `SdkError::Connection` if binding to an ephemeral port fails.
    pub async fn start() -> crate::Result<Self> {
        Self::start_on_port(0).await
    }

    /// Starts a new mock server on a specific port.
    ///
    /// Use port 0 to let the OS assign an ephemeral port.
    ///
    /// # Errors
    ///
    /// Returns `SdkError::Config` if the port is invalid.
    /// Returns `SdkError::Connection` if binding to the specified port fails.
    pub async fn start_on_port(port: u16) -> crate::Result<Self> {
        let state = Arc::new(MockState::new());

        // Bind to localhost with specified port
        let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().map_err(|e| {
            crate::error::SdkError::Config { message: format!("Invalid port: {e}") }
        })?;

        // Get the actual bound address (important for ephemeral ports)
        let listener = tokio::net::TcpListener::bind(addr).await.map_err(|e| {
            crate::error::SdkError::Connection { message: format!("Failed to bind: {e}") }
        })?;
        let local_addr = listener.local_addr().map_err(|e| crate::error::SdkError::Connection {
            message: format!("Failed to get local addr: {e}"),
        })?;

        let endpoint = format!("http://{}", local_addr);

        // Create service implementations
        let read_service = MockReadService::new(state.clone());
        let write_service = MockWriteService::new(state.clone());
        let admin_service = MockAdminService::new(state.clone());
        let app_service = MockAppService::new(state.clone());
        let organization_service = MockOrganizationService::new(state.clone());
        let vault_service = MockVaultService::new(state.clone());
        let health_service = MockHealthService::new(state.clone());
        let discovery_service = MockDiscoveryService::new(state.clone());
        let token_service = MockTokenService::new(state.clone());
        let user_service = MockUserService::new(state.clone());
        let invitation_service = MockInvitationService::new(state.clone());
        let events_service = MockEventsService::new(state.clone());

        // Create shutdown channel
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        // Spawn server task
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        tokio::spawn(async move {
            let result = Server::builder()
                .add_service(ReadServiceServer::new(read_service))
                .add_service(WriteServiceServer::new(write_service))
                .add_service(AdminServiceServer::new(admin_service))
                .add_service(AppServiceServer::new(app_service))
                .add_service(OrganizationServiceServer::new(organization_service))
                .add_service(VaultServiceServer::new(vault_service))
                .add_service(HealthServiceServer::new(health_service))
                .add_service(SystemDiscoveryServiceServer::new(discovery_service))
                .add_service(TokenServiceServer::new(token_service))
                .add_service(UserServiceServer::new(user_service))
                .add_service(InvitationServiceServer::new(invitation_service))
                .add_service(EventsServiceServer::new(events_service))
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

    /// Returns the endpoint URL for connecting to this server.
    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Sets an entity value for read tests.
    ///
    /// The entity will be stored with a version of 1 and no expiration.
    ///
    /// # Arguments
    ///
    /// * `organization` - Organization slug (external identifier).
    /// * `vault` - Vault slug (external identifier).
    /// * `key` - Entity key.
    /// * `value` - Entity value bytes.
    pub fn set_entity(
        &self,
        organization: OrganizationSlug,
        vault: VaultSlug,
        key: &str,
        value: &[u8],
    ) {
        self.set_entity_with_options(organization, vault, key, value, 1, None);
    }

    /// Sets an entity value with version and optional expiration.
    ///
    /// # Arguments
    ///
    /// * `organization` - Organization slug (external identifier).
    /// * `vault` - Vault slug (external identifier).
    /// * `key` - Entity key.
    /// * `value` - Entity value bytes.
    /// * `version` - Entity version number.
    /// * `expires_at` - Optional expiration timestamp (Unix seconds).
    pub fn set_entity_with_options(
        &self,
        organization: OrganizationSlug,
        vault: VaultSlug,
        key: &str,
        value: &[u8],
        version: u64,
        expires_at: Option<u64>,
    ) {
        let mut entities = self.state.entities.write();
        entities
            .insert((organization, vault, key.to_string()), (value.to_vec(), version, expires_at));
    }

    /// Removes an entity from storage.
    ///
    /// # Arguments
    ///
    /// * `organization` - Organization slug (external identifier).
    /// * `vault` - Vault slug (external identifier).
    /// * `key` - Entity key to remove.
    pub fn remove_entity(&self, organization: OrganizationSlug, vault: VaultSlug, key: &str) {
        let mut entities = self.state.entities.write();
        entities.remove(&(organization, vault, key.to_string()));
    }

    /// Sets the last committed sequence for a client.
    ///
    /// Used to test idempotency behavior (ALREADY_COMMITTED, SEQUENCE_GAP).
    ///
    /// # Arguments
    ///
    /// * `organization` - Organization slug (external identifier).
    /// * `vault` - Vault slug (external identifier).
    /// * `client_id` - Client identifier string.
    /// * `seq` - Last committed sequence number.
    pub fn set_client_state(
        &self,
        organization: OrganizationSlug,
        vault: VaultSlug,
        client_id: &str,
        seq: u64,
    ) {
        let mut sequences = self.state.client_sequences.write();
        sequences.insert((organization, vault, client_id.to_string()), seq);
    }

    /// Returns the last committed sequence for a client.
    ///
    /// # Arguments
    ///
    /// * `organization` - Organization slug (external identifier).
    /// * `vault` - Vault slug (external identifier).
    /// * `client_id` - Client identifier string.
    pub fn get_client_state(
        &self,
        organization: OrganizationSlug,
        vault: VaultSlug,
        client_id: &str,
    ) -> Option<u64> {
        let sequences = self.state.client_sequences.read();
        sequences.get(&(organization, vault, client_id.to_string())).copied()
    }

    /// Injects UNAVAILABLE errors for the next N requests.
    ///
    /// Each request will decrement this counter and return UNAVAILABLE until it reaches 0.
    pub fn inject_unavailable(&self, count: usize) {
        self.state.unavailable_count.store(count, Ordering::SeqCst);
    }

    /// Injects a delay for all subsequent requests.
    ///
    /// Sets to 0 to disable delay.
    pub fn inject_delay(&self, millis: u64) {
        self.state.delay_ms.store(millis, Ordering::SeqCst);
    }

    /// Pushes a leader update to all active `WatchLeader` subscribers.
    ///
    /// Returns `true` if at least one subscriber received the update.
    /// No-op (returns `false`) when no client has subscribed yet.
    pub fn push_leader_update(
        &self,
        endpoint: impl Into<String>,
        raft_term: u64,
        leader_node_id: u64,
    ) -> bool {
        let Some(sender) = self.state.leader_watch_sender() else {
            return false;
        };
        let update = proto::LeaderUpdate { endpoint: endpoint.into(), raft_term, leader_node_id };
        sender.send(update).is_ok()
    }

    /// Returns the total number of write requests received.
    pub fn write_count(&self) -> usize {
        self.state.write_count.load(Ordering::SeqCst)
    }

    /// Returns the total number of read requests received.
    pub fn read_count(&self) -> usize {
        self.state.read_count.load(Ordering::SeqCst)
    }

    /// Returns the current mock block height.
    pub fn block_height(&self) -> u64 {
        self.state.block_height.load(Ordering::SeqCst)
    }

    /// Adds a relationship for query tests.
    ///
    /// # Arguments
    ///
    /// * `organization` - Organization slug (external identifier).
    /// * `vault` - Vault slug (external identifier).
    /// * `resource` - Resource identifier (e.g., "document:123").
    /// * `relation` - Relation name (e.g., "viewer").
    /// * `subject` - Subject identifier (e.g., "user:alice").
    pub fn add_relationship(
        &self,
        organization: OrganizationSlug,
        vault: VaultSlug,
        resource: &str,
        relation: &str,
        subject: &str,
    ) {
        let mut relationships = self.state.relationships.write();
        let entry = relationships.entry((organization, vault)).or_default();
        entry.push(proto::Relationship {
            resource: resource.to_string(),
            relation: relation.to_string(),
            subject: subject.to_string(),
        });
    }

    /// Adds an organization for admin tests.
    ///
    /// # Arguments
    ///
    /// * `organization` - Organization slug (external identifier).
    /// * `name` - Human-readable organization name.
    /// * `region` - Data residency region for the organization.
    pub fn add_organization(&self, organization: OrganizationSlug, name: &str, region: Region) {
        let mut organizations = self.state.organizations.write();
        organizations.insert(
            organization,
            OrganizationData {
                name: name.to_string(),
                region,
                status: proto::OrganizationStatus::Active as i32,
                members: vec![],
                deleted_at: None,
            },
        );
    }

    /// Adds a vault for admin tests.
    ///
    /// # Arguments
    ///
    /// * `organization` - Organization slug (external identifier).
    /// * `vault` - Vault slug (external identifier).
    pub fn add_vault(&self, organization: OrganizationSlug, vault: VaultSlug) {
        let mut vaults = self.state.vaults.write();
        vaults.insert(
            (organization, vault),
            VaultData {
                height: 1,
                state_root: vec![0u8; 32],
                status: proto::VaultStatus::Active as i32,
            },
        );
    }

    /// Adds peer info for discovery tests.
    pub fn add_peer(&self, node_id: &str, addresses: Vec<String>, grpc_port: u32) {
        let mut peers = self.state.peers.write();
        peers.push(proto::PeerInfo {
            node_id: Some(proto::NodeId { id: node_id.to_string() }),
            addresses,
            grpc_port,
            last_seen: None,
        });
    }

    /// Adds a mock invitation for invitation tests.
    ///
    /// # Arguments
    ///
    /// * `invite_slug` - External invitation slug.
    /// * `org_slug` - Organization that sent the invitation.
    /// * `inviter_slug` - User who created the invitation.
    /// * `email` - Invitee email (admin view).
    /// * `org_name` - Organization name (user view).
    /// * `role` - Role assigned upon acceptance (proto i32).
    /// * `status` - Invitation status (proto i32).
    pub fn add_invitation(
        &self,
        invite_slug: u64,
        org_slug: OrganizationSlug,
        inviter_slug: u64,
        email: &str,
        org_name: &str,
        role: i32,
        status: i32,
    ) {
        let mut invitations = self.state.invitations.write();
        invitations.push(proto::Invitation {
            slug: Some(proto::InviteSlug { slug: invite_slug }),
            organization: Some(proto::OrganizationSlug { slug: org_slug.value() }),
            inviter: Some(proto::UserSlug { slug: inviter_slug }),
            invitee_email: email.to_string(),
            organization_name: org_name.to_string(),
            role,
            team: None,
            status,
            created_at: None,
            expires_at: None,
            resolved_at: None,
        });
    }

    /// Seeds an app client assertion for `get_app_client_assertion` tests.
    ///
    /// The mock handler returns this fixture when queried by matching
    /// `(organization, app, assertion_id)`, and `Status::not_found` otherwise.
    ///
    /// # Arguments
    ///
    /// * `organization` - Organization slug.
    /// * `app` - App slug.
    /// * `assertion_id` - Client assertion ID.
    /// * `name` - Display name for the assertion.
    /// * `enabled` - Whether the assertion is enabled.
    pub fn add_app_client_assertion(
        &self,
        organization: OrganizationSlug,
        app: AppSlug,
        assertion_id: ClientAssertionId,
        name: &str,
        enabled: bool,
    ) {
        let now =
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
        let now_proto = prost_types::Timestamp {
            seconds: now.as_secs() as i64,
            nanos: now.subsec_nanos() as i32,
        };
        let info = proto::AppClientAssertionInfo {
            id: Some(proto::ClientAssertionId { id: assertion_id.value() }),
            name: name.to_string(),
            enabled,
            expires_at: Some(now_proto),
            created_at: Some(now_proto),
        };
        self.state.app_client_assertions.write().insert((organization, app, assertion_id), info);
    }

    /// Adds a mock event for events tests.
    pub fn add_event(
        &self,
        event_id: Vec<u8>,
        organization: OrganizationSlug,
        event_type: &str,
        principal: &str,
    ) {
        let mut events = self.state.events.write();
        events.push(proto::EventEntry {
            event_id,
            source_service: "ledger".to_string(),
            event_type: event_type.to_string(),
            timestamp: None,
            scope: proto::EventScope::Organization as i32,
            action: event_type.to_string(),
            emission_path: proto::EventEmissionPath::EmissionPathApplyPhase as i32,
            principal: principal.to_string(),
            organization: Some(proto::OrganizationSlug { slug: organization.value() }),
            vault: None,
            outcome: proto::EventOutcome::Success as i32,
            error_code: None,
            error_detail: None,
            denial_reason: None,
            details: std::collections::HashMap::new(),
            block_height: None,
            node_id: None,
            trace_id: None,
            correlation_id: None,
            operations_count: None,
            expires_at: 0,
        });
    }

    /// Resets all state to initial values.
    pub fn reset(&self) {
        self.state.entities.write().clear();
        self.state.client_sequences.write().clear();
        self.state.idempotency_cache.write().clear();
        self.state.relationships.write().clear();
        self.state.organizations.write().clear();
        self.state.vaults.write().clear();
        self.state.peers.write().clear();
        self.state.users.write().clear();
        self.state.user_emails.write().clear();
        self.state.invitations.write().clear();
        self.state.events.write().clear();
        self.state.app_client_assertions.write().clear();
        self.state.unavailable_count.store(0, Ordering::SeqCst);
        self.state.delay_ms.store(0, Ordering::SeqCst);
        self.state.write_count.store(0, Ordering::SeqCst);
        self.state.read_count.store(0, Ordering::SeqCst);
        self.state.block_height.store(1, Ordering::SeqCst);
        self.state.next_user_slug.store(1000, Ordering::SeqCst);
        self.state.next_user_email_id.store(1, Ordering::SeqCst);
        self.state.next_invite.store(1, Ordering::SeqCst);
    }

    /// Shuts down the server gracefully.
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
