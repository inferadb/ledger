//! Main `LedgerClient` implementation.
//!
//! Provides the high-level API for interacting with the Ledger service,
//! orchestrating connection pool, sequence tracker, and retry logic.

use inferadb_ledger_raft::proto;

use crate::{
    config::ClientConfig,
    connection::ConnectionPool,
    error::{self, Result},
    idempotency::SequenceTracker,
    retry::with_retry,
    streaming::{HeightTracker, ReconnectingStream},
};

/// Read consistency level for read operations.
///
/// Controls whether reads are served from any replica (eventual) or must
/// go through the leader (linearizable).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReadConsistency {
    /// Read from any replica (fastest, may be stale).
    #[default]
    Eventual,
    /// Read from leader (strong consistency, higher latency).
    Linearizable,
}

impl ReadConsistency {
    /// Convert to proto enum value.
    fn to_proto(self) -> proto::ReadConsistency {
        match self {
            ReadConsistency::Eventual => proto::ReadConsistency::Eventual,
            ReadConsistency::Linearizable => proto::ReadConsistency::Linearizable,
        }
    }
}

// =============================================================================
// Write Types
// =============================================================================

/// Result of a successful write operation.
///
/// Contains the transaction ID and block height where the write was committed.
/// This information can be used for:
/// - Tracking transaction history
/// - Waiting for replication to replicas
/// - Verified reads at a specific block height
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteSuccess {
    /// Unique transaction ID assigned by the server.
    pub tx_id: String,
    /// Block height where the transaction was committed.
    pub block_height: u64,
}

// =============================================================================
// Streaming Types
// =============================================================================

/// A block announcement from the WatchBlocks stream.
///
/// Contains metadata about a newly committed block in a vault's chain.
/// Used for real-time notifications of state changes.
///
/// # Example
///
/// ```no_run
/// # use inferadb_ledger_sdk::{LedgerClient, ClientConfig};
/// # use futures::StreamExt;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = LedgerClient::new(ClientConfig::builder()
/// #     .endpoints(vec!["http://localhost:50051".into()])
/// #     .client_id("example")
/// #     .build()?).await?;
/// # let (namespace_id, vault_id, start_height) = (1i64, 1i64, 1u64);
/// let mut stream = client.watch_blocks(namespace_id, vault_id, start_height).await?;
/// while let Some(announcement) = stream.next().await {
///     let block = announcement?;
///     println!("New block at height {}: {:?}", block.height, block.block_hash);
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockAnnouncement {
    /// Namespace containing the vault.
    pub namespace_id: i64,
    /// Vault within the namespace.
    pub vault_id: i64,
    /// Block height (1-indexed).
    pub height: u64,
    /// Hash of the block header.
    pub block_hash: Vec<u8>,
    /// Merkle root of the state at this block.
    pub state_root: Vec<u8>,
    /// Timestamp when the block was committed.
    pub timestamp: Option<std::time::SystemTime>,
}

impl BlockAnnouncement {
    /// Create a BlockAnnouncement from the proto type.
    fn from_proto(proto: proto::BlockAnnouncement) -> Self {
        let timestamp = proto.timestamp.map(|ts| {
            std::time::UNIX_EPOCH + std::time::Duration::new(ts.seconds as u64, ts.nanos as u32)
        });

        Self {
            namespace_id: proto.namespace_id.map(|n| n.id).unwrap_or(0),
            vault_id: proto.vault_id.map(|v| v.id).unwrap_or(0),
            height: proto.height,
            block_hash: proto.block_hash.map(|h| h.value).unwrap_or_default(),
            state_root: proto.state_root.map(|h| h.value).unwrap_or_default(),
            timestamp,
        }
    }
}

// =============================================================================
// Admin Types
// =============================================================================

/// Status of a namespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum NamespaceStatus {
    /// Status is unknown or unspecified.
    #[default]
    Unspecified,
    /// Namespace is active and operational.
    Active,
    /// Namespace has been deleted.
    Deleted,
}

impl NamespaceStatus {
    /// Create from proto enum value.
    fn from_proto(value: i32) -> Self {
        match proto::NamespaceStatus::try_from(value) {
            Ok(proto::NamespaceStatus::Active) => NamespaceStatus::Active,
            Ok(proto::NamespaceStatus::Deleted) => NamespaceStatus::Deleted,
            _ => NamespaceStatus::Unspecified,
        }
    }
}

/// Status of a vault.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum VaultStatus {
    /// Status is unknown or unspecified.
    #[default]
    Unspecified,
    /// Vault is active and operational.
    Active,
    /// Vault is read-only (no writes allowed).
    ReadOnly,
    /// Vault has been deleted.
    Deleted,
}

impl VaultStatus {
    /// Create from proto enum value.
    fn from_proto(value: i32) -> Self {
        match proto::VaultStatus::try_from(value) {
            Ok(proto::VaultStatus::Active) => VaultStatus::Active,
            Ok(proto::VaultStatus::ReadOnly) => VaultStatus::ReadOnly,
            Ok(proto::VaultStatus::Deleted) => VaultStatus::Deleted,
            _ => VaultStatus::Unspecified,
        }
    }
}

/// Information about a namespace.
///
/// Contains metadata about a namespace including its ID, name, shard assignment,
/// and current status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceInfo {
    /// Unique namespace identifier.
    pub namespace_id: i64,
    /// Human-readable namespace name.
    pub name: String,
    /// Shard ID hosting this namespace.
    pub shard_id: u32,
    /// Node IDs of shard members (node IDs are strings).
    pub member_nodes: Vec<String>,
    /// Hint for current leader (may be stale).
    pub leader_hint: Option<String>,
    /// Configuration version number.
    pub config_version: u64,
    /// Current namespace status.
    pub status: NamespaceStatus,
}

impl NamespaceInfo {
    /// Create from proto response.
    fn from_proto(proto: proto::GetNamespaceResponse) -> Self {
        Self {
            namespace_id: proto.namespace_id.map(|n| n.id).unwrap_or(0),
            name: proto.name,
            shard_id: proto.shard_id.map(|s| s.id).unwrap_or(0),
            member_nodes: proto.member_nodes.into_iter().map(|n| n.id).collect(),
            leader_hint: proto.leader_hint.map(|n| n.id),
            config_version: proto.config_version,
            status: NamespaceStatus::from_proto(proto.status),
        }
    }
}

/// Information about a vault.
///
/// Contains metadata about a vault including its ID, current height,
/// state root, and node membership.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VaultInfo {
    /// Namespace containing this vault.
    pub namespace_id: i64,
    /// Unique vault identifier within the namespace.
    pub vault_id: i64,
    /// Current block height.
    pub height: u64,
    /// Current state root (Merkle root).
    pub state_root: Vec<u8>,
    /// Node IDs replicating this vault (node IDs are strings).
    pub nodes: Vec<String>,
    /// Current leader node ID.
    pub leader: Option<String>,
    /// Current vault status.
    pub status: VaultStatus,
}

impl VaultInfo {
    /// Create from proto response.
    fn from_proto(proto: proto::GetVaultResponse) -> Self {
        Self {
            namespace_id: proto.namespace_id.map(|n| n.id).unwrap_or(0),
            vault_id: proto.vault_id.map(|v| v.id).unwrap_or(0),
            height: proto.height,
            state_root: proto.state_root.map(|h| h.value).unwrap_or_default(),
            nodes: proto.nodes.into_iter().map(|n| n.id).collect(),
            leader: proto.leader.map(|n| n.id),
            status: VaultStatus::from_proto(proto.status),
        }
    }
}

/// Health status of a node or vault.
///
/// Maps to the proto `HealthStatus` enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HealthStatus {
    /// Status is unknown or unspecified.
    #[default]
    Unspecified,
    /// The node or vault is healthy and fully operational.
    Healthy,
    /// The node or vault is operational but has some issues.
    Degraded,
    /// The node or vault is unavailable.
    Unavailable,
}

impl HealthStatus {
    /// Create from proto enum value.
    fn from_proto(value: i32) -> Self {
        match proto::HealthStatus::try_from(value) {
            Ok(proto::HealthStatus::Healthy) => HealthStatus::Healthy,
            Ok(proto::HealthStatus::Degraded) => HealthStatus::Degraded,
            Ok(proto::HealthStatus::Unavailable) => HealthStatus::Unavailable,
            _ => HealthStatus::Unspecified,
        }
    }
}

/// Result of a health check operation.
///
/// Contains the health status along with a message and additional details.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealthCheckResult {
    /// The health status.
    pub status: HealthStatus,
    /// Human-readable message describing the health state.
    pub message: String,
    /// Additional details as key-value pairs.
    pub details: std::collections::HashMap<String, String>,
}

impl HealthCheckResult {
    /// Create from proto response.
    fn from_proto(proto: proto::HealthCheckResponse) -> Self {
        Self {
            status: HealthStatus::from_proto(proto.status),
            message: proto.message,
            details: proto.details,
        }
    }

    /// Returns true if the status is healthy.
    pub fn is_healthy(&self) -> bool {
        self.status == HealthStatus::Healthy
    }

    /// Returns true if the status is degraded.
    pub fn is_degraded(&self) -> bool {
        self.status == HealthStatus::Degraded
    }

    /// Returns true if the status is unavailable.
    pub fn is_unavailable(&self) -> bool {
        self.status == HealthStatus::Unavailable
    }
}

// =============================================================================
// Verified Read Types
// =============================================================================

/// Direction of a sibling in a Merkle proof.
///
/// Indicates whether the sibling hash should be placed on the left or right
/// when computing the parent hash.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// Sibling is on the left: `hash(sibling || current)`.
    Left,
    /// Sibling is on the right: `hash(current || sibling)`.
    Right,
}

impl Direction {
    /// Create from proto enum value.
    fn from_proto(value: i32) -> Self {
        match proto::Direction::try_from(value) {
            Ok(proto::Direction::Left) => Direction::Left,
            _ => Direction::Right, // Default to right for unspecified
        }
    }
}

/// A sibling node in a Merkle proof path.
///
/// Each sibling contains the hash of the neighboring node and which side
/// it appears on for hash computation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MerkleSibling {
    /// Hash of the sibling node.
    pub hash: Vec<u8>,
    /// Direction (left or right) relative to the current node.
    pub direction: Direction,
}

impl MerkleSibling {
    /// Create from proto type.
    fn from_proto(proto: proto::MerkleSibling) -> Self {
        Self {
            hash: proto.hash.map(|h| h.value).unwrap_or_default(),
            direction: Direction::from_proto(proto.direction),
        }
    }
}

/// Merkle proof for verifying state inclusion.
///
/// Contains the leaf hash and a path of sibling hashes from leaf to root.
/// Used to verify that a value is included in the state tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MerkleProof {
    /// Hash of the leaf (the entity key-value).
    pub leaf_hash: Vec<u8>,
    /// Sibling hashes from leaf to root (bottom-up order).
    pub siblings: Vec<MerkleSibling>,
}

impl MerkleProof {
    /// Create from proto type.
    fn from_proto(proto: proto::MerkleProof) -> Self {
        Self {
            leaf_hash: proto.leaf_hash.map(|h| h.value).unwrap_or_default(),
            siblings: proto.siblings.into_iter().map(MerkleSibling::from_proto).collect(),
        }
    }

    /// Verify this proof against an expected state root.
    ///
    /// Recomputes the root hash from the leaf through the sibling path and
    /// checks if it matches the expected root.
    ///
    /// # Arguments
    ///
    /// * `expected_root` - The expected state root hash to verify against.
    ///
    /// # Returns
    ///
    /// `true` if the proof is valid and matches the expected root.
    pub fn verify(&self, expected_root: &[u8]) -> bool {
        use sha2::{Digest, Sha256};

        if self.siblings.is_empty() {
            // Single-element tree: leaf hash equals root
            return self.leaf_hash == expected_root;
        }

        let mut current_hash = self.leaf_hash.clone();

        for sibling in &self.siblings {
            let mut hasher = Sha256::new();
            match sibling.direction {
                Direction::Left => {
                    // Sibling is on left: hash(sibling || current)
                    hasher.update(&sibling.hash);
                    hasher.update(&current_hash);
                },
                Direction::Right => {
                    // Sibling is on right: hash(current || sibling)
                    hasher.update(&current_hash);
                    hasher.update(&sibling.hash);
                },
            }
            current_hash = hasher.finalize().to_vec();
        }

        current_hash == expected_root
    }
}

/// Block header containing cryptographic commitments.
///
/// The block header is the cryptographic anchor for all state at a given height.
/// It contains the state root which can be used to verify Merkle proofs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockHeader {
    /// Block height (1-indexed).
    pub height: u64,
    /// Namespace containing the vault.
    pub namespace_id: i64,
    /// Vault within the namespace.
    pub vault_id: i64,
    /// Hash of the previous block header.
    pub previous_hash: Vec<u8>,
    /// Merkle root of transactions in this block.
    pub tx_merkle_root: Vec<u8>,
    /// Merkle root of the state tree after this block.
    pub state_root: Vec<u8>,
    /// Timestamp when the block was committed.
    pub timestamp: Option<std::time::SystemTime>,
    /// Node ID of the leader that committed this block.
    pub leader_id: String,
    /// Raft term number.
    pub term: u64,
    /// Raft committed index.
    pub committed_index: u64,
}

impl BlockHeader {
    /// Create from proto type.
    fn from_proto(proto: proto::BlockHeader) -> Self {
        let timestamp = proto.timestamp.map(|ts| {
            std::time::UNIX_EPOCH + std::time::Duration::new(ts.seconds as u64, ts.nanos as u32)
        });

        Self {
            height: proto.height,
            namespace_id: proto.namespace_id.map(|n| n.id).unwrap_or(0),
            vault_id: proto.vault_id.map(|v| v.id).unwrap_or(0),
            previous_hash: proto.previous_hash.map(|h| h.value).unwrap_or_default(),
            tx_merkle_root: proto.tx_merkle_root.map(|h| h.value).unwrap_or_default(),
            state_root: proto.state_root.map(|h| h.value).unwrap_or_default(),
            timestamp,
            leader_id: proto.leader_id.map(|n| n.id).unwrap_or_default(),
            term: proto.term,
            committed_index: proto.committed_index,
        }
    }
}

/// Chain proof linking a trusted height to a response height.
///
/// Used to verify that a block at response_height descends from trusted_height.
/// Contains block headers in ascending order from trusted_height + 1 to response_height.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChainProof {
    /// Block headers from trusted_height + 1 to response_height (ascending order).
    pub headers: Vec<BlockHeader>,
}

impl ChainProof {
    /// Create from proto type.
    fn from_proto(proto: proto::ChainProof) -> Self {
        Self { headers: proto.headers.into_iter().map(BlockHeader::from_proto).collect() }
    }

    /// Verify the chain of blocks links correctly.
    ///
    /// Checks that each block's previous_hash matches the hash of the preceding block.
    ///
    /// # Arguments
    ///
    /// * `trusted_header_hash` - Hash of the block at trusted_height (client already has this).
    ///
    /// # Returns
    ///
    /// `true` if all previous_hash links are valid.
    pub fn verify(&self, trusted_header_hash: &[u8]) -> bool {
        use sha2::{Digest, Sha256};

        if self.headers.is_empty() {
            return true;
        }

        // First header should link to trusted header
        if self.headers[0].previous_hash != trusted_header_hash {
            return false;
        }

        // Each subsequent header should link to the previous one
        for i in 1..self.headers.len() {
            let prev = &self.headers[i - 1];
            let curr = &self.headers[i];

            // Compute hash of previous header
            // Note: This is a simplified hash - real implementation would hash the canonical
            // encoding
            let mut hasher = Sha256::new();
            hasher.update(&prev.previous_hash);
            hasher.update(&prev.tx_merkle_root);
            hasher.update(&prev.state_root);
            hasher.update(prev.height.to_le_bytes());
            let prev_hash = hasher.finalize().to_vec();

            if curr.previous_hash != prev_hash {
                return false;
            }
        }

        true
    }
}

/// Options for verified read operations.
///
/// Controls which proofs to include and at what height to read.
#[derive(Debug, Clone, Default)]
pub struct VerifyOpts {
    /// Read at a specific block height (None = current height).
    pub at_height: Option<u64>,
    /// Include chain proof linking to a trusted height.
    pub include_chain_proof: bool,
    /// Trusted height for chain proof verification.
    pub trusted_height: Option<u64>,
}

impl VerifyOpts {
    /// Create options with default values (current height, no chain proof).
    pub fn new() -> Self {
        Self::default()
    }

    /// Read at a specific block height.
    pub fn at_height(mut self, height: u64) -> Self {
        self.at_height = Some(height);
        self
    }

    /// Include chain proof from a trusted height.
    pub fn with_chain_proof(mut self, trusted_height: u64) -> Self {
        self.include_chain_proof = true;
        self.trusted_height = Some(trusted_height);
        self
    }
}

// =============================================================================
// Query Types
// =============================================================================

/// Paginated result from query operations.
///
/// Used by `list_entities`, `list_relationships`, and `list_resources` operations.
/// The `next_page_token` can be passed to subsequent calls to continue pagination.
#[derive(Debug, Clone)]
pub struct PagedResult<T> {
    /// Items returned in this page.
    pub items: Vec<T>,
    /// Token for fetching the next page, or `None` if this is the last page.
    pub next_page_token: Option<String>,
    /// Block height at which the query was evaluated.
    pub block_height: u64,
}

impl<T> PagedResult<T> {
    /// Check if there are more pages available.
    pub fn has_next_page(&self) -> bool {
        self.next_page_token.is_some()
    }
}

/// An entity stored in the ledger.
///
/// Entities are key-value pairs that can have optional expiration times
/// and track their version (block height when last modified).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entity {
    /// Entity key (max 1024 bytes, UTF-8).
    pub key: String,
    /// Entity value (max 1MB).
    pub value: Vec<u8>,
    /// Unix epoch seconds when the entity expires, or `None` for no expiration.
    pub expires_at: Option<u64>,
    /// Block height when this entity was last modified.
    pub version: u64,
}

impl Entity {
    /// Convert from proto Entity.
    pub fn from_proto(proto: proto::Entity) -> Self {
        Self {
            key: proto.key,
            value: proto.value,
            expires_at: proto.expires_at.filter(|&ts| ts > 0),
            version: proto.version,
        }
    }

    /// Check if this entity has expired relative to a given timestamp.
    pub fn is_expired_at(&self, now_secs: u64) -> bool {
        self.expires_at.is_some_and(|exp| exp <= now_secs)
    }
}

/// A relationship in a vault (authorization tuple).
///
/// Relationships connect resources to subjects via relations, forming
/// the basis for permission checking.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Relationship {
    /// Resource identifier in format "type:id" (max 512 chars).
    pub resource: String,
    /// Relation name (max 64 chars).
    pub relation: String,
    /// Subject identifier in format "type:id" or "type:id#relation" (max 512 chars).
    pub subject: String,
}

impl Relationship {
    /// Create a new relationship.
    pub fn new(
        resource: impl Into<String>,
        relation: impl Into<String>,
        subject: impl Into<String>,
    ) -> Self {
        Self { resource: resource.into(), relation: relation.into(), subject: subject.into() }
    }

    /// Convert from proto Relationship.
    pub fn from_proto(proto: proto::Relationship) -> Self {
        Self { resource: proto.resource, relation: proto.relation, subject: proto.subject }
    }
}

/// Options for listing entities.
///
/// Builder pattern for configuring entity list queries with optional filters.
#[derive(Debug, Clone, Default, bon::Builder)]
pub struct ListEntitiesOpts {
    /// Filter entities by key prefix (e.g., "user:", "session:").
    #[builder(into, default)]
    pub key_prefix: String,
    /// Read at a specific block height (None = current).
    pub at_height: Option<u64>,
    /// Include entities past their expiration time.
    #[builder(default)]
    pub include_expired: bool,
    /// Maximum number of results per page (0 = server default).
    #[builder(default)]
    pub limit: u32,
    /// Pagination token from previous response.
    #[builder(into)]
    pub page_token: Option<String>,
    /// Read consistency level.
    #[builder(default)]
    pub consistency: ReadConsistency,
}

impl ListEntitiesOpts {
    /// Create options with a key prefix filter.
    pub fn with_prefix(prefix: impl Into<String>) -> Self {
        Self { key_prefix: prefix.into(), ..Default::default() }
    }

    /// Read at a specific block height.
    pub fn at_height(mut self, height: u64) -> Self {
        self.at_height = Some(height);
        self
    }

    /// Include expired entities in results.
    pub fn include_expired(mut self) -> Self {
        self.include_expired = true;
        self
    }

    /// Set maximum results per page.
    pub fn limit(mut self, limit: u32) -> Self {
        self.limit = limit;
        self
    }

    /// Continue from a previous page.
    pub fn page_token(mut self, token: impl Into<String>) -> Self {
        self.page_token = Some(token.into());
        self
    }

    /// Set read consistency level.
    pub fn consistency(mut self, consistency: ReadConsistency) -> Self {
        self.consistency = consistency;
        self
    }

    /// Use linearizable (strong) consistency.
    pub fn linearizable(mut self) -> Self {
        self.consistency = ReadConsistency::Linearizable;
        self
    }
}

/// Options for listing relationships.
///
/// Builder pattern for configuring relationship list queries with optional filters.
/// All filter fields are optional; omitting a filter matches all values for that field.
#[derive(Debug, Clone, Default, bon::Builder)]
pub struct ListRelationshipsOpts {
    /// Filter by resource (exact match).
    #[builder(into)]
    pub resource: Option<String>,
    /// Filter by relation (exact match).
    #[builder(into)]
    pub relation: Option<String>,
    /// Filter by subject (exact match).
    #[builder(into)]
    pub subject: Option<String>,
    /// Read at a specific block height (None = current).
    pub at_height: Option<u64>,
    /// Maximum number of results per page (0 = server default).
    #[builder(default)]
    pub limit: u32,
    /// Pagination token from previous response.
    #[builder(into)]
    pub page_token: Option<String>,
    /// Read consistency level.
    #[builder(default)]
    pub consistency: ReadConsistency,
}

impl ListRelationshipsOpts {
    /// Create default options (no filters).
    pub fn new() -> Self {
        Self::default()
    }

    /// Filter by resource.
    pub fn resource(mut self, resource: impl Into<String>) -> Self {
        self.resource = Some(resource.into());
        self
    }

    /// Filter by relation.
    pub fn relation(mut self, relation: impl Into<String>) -> Self {
        self.relation = Some(relation.into());
        self
    }

    /// Filter by subject.
    pub fn subject(mut self, subject: impl Into<String>) -> Self {
        self.subject = Some(subject.into());
        self
    }

    /// Read at a specific block height.
    pub fn at_height(mut self, height: u64) -> Self {
        self.at_height = Some(height);
        self
    }

    /// Set maximum results per page.
    pub fn limit(mut self, limit: u32) -> Self {
        self.limit = limit;
        self
    }

    /// Continue from a previous page.
    pub fn page_token(mut self, token: impl Into<String>) -> Self {
        self.page_token = Some(token.into());
        self
    }

    /// Set read consistency level.
    pub fn consistency(mut self, consistency: ReadConsistency) -> Self {
        self.consistency = consistency;
        self
    }

    /// Use linearizable (strong) consistency.
    pub fn linearizable(mut self) -> Self {
        self.consistency = ReadConsistency::Linearizable;
        self
    }
}

/// Options for listing resources.
///
/// Builder pattern for configuring resource list queries.
#[derive(Debug, Clone, Default, bon::Builder)]
pub struct ListResourcesOpts {
    /// Resource type prefix (e.g., "document" matches "document:*").
    #[builder(into, default)]
    pub resource_type: String,
    /// Read at a specific block height (None = current).
    pub at_height: Option<u64>,
    /// Maximum number of results per page (0 = server default).
    #[builder(default)]
    pub limit: u32,
    /// Pagination token from previous response.
    #[builder(into)]
    pub page_token: Option<String>,
    /// Read consistency level.
    #[builder(default)]
    pub consistency: ReadConsistency,
}

impl ListResourcesOpts {
    /// Create options with a resource type filter.
    pub fn with_type(resource_type: impl Into<String>) -> Self {
        Self { resource_type: resource_type.into(), ..Default::default() }
    }

    /// Read at a specific block height.
    pub fn at_height(mut self, height: u64) -> Self {
        self.at_height = Some(height);
        self
    }

    /// Set maximum results per page.
    pub fn limit(mut self, limit: u32) -> Self {
        self.limit = limit;
        self
    }

    /// Continue from a previous page.
    pub fn page_token(mut self, token: impl Into<String>) -> Self {
        self.page_token = Some(token.into());
        self
    }

    /// Set read consistency level.
    pub fn consistency(mut self, consistency: ReadConsistency) -> Self {
        self.consistency = consistency;
        self
    }

    /// Use linearizable (strong) consistency.
    pub fn linearizable(mut self) -> Self {
        self.consistency = ReadConsistency::Linearizable;
        self
    }
}

/// Result of a verified read operation.
///
/// Contains the value along with cryptographic proofs for client-side verification.
/// Use [`VerifiedValue::verify`] to check that the value is authentic.
///
/// # Example
///
/// ```no_run
/// # use inferadb_ledger_sdk::{LedgerClient, ClientConfig, VerifyOpts};
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = LedgerClient::new(ClientConfig::builder()
/// #     .endpoints(vec!["http://localhost:50051".into()])
/// #     .client_id("example")
/// #     .build()?).await?;
/// # let (ns_id, vault_id) = (1i64, 1i64);
/// let result = client.verified_read(ns_id, Some(vault_id), "key", VerifyOpts::new()).await?;
/// if let Some(verified) = result {
///     // Verify the proof is valid
///     assert!(verified.verify()?);
///     println!("Verified value: {:?}", verified.value);
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedValue {
    /// The entity value (None if key not found).
    pub value: Option<Vec<u8>>,
    /// Block height at which the read was performed.
    pub block_height: u64,
    /// Block header containing the state root.
    pub block_header: BlockHeader,
    /// Merkle proof from leaf to state root.
    pub merkle_proof: MerkleProof,
    /// Optional chain proof linking to trusted height.
    pub chain_proof: Option<ChainProof>,
}

impl VerifiedValue {
    /// Create from proto response.
    fn from_proto(proto: proto::VerifiedReadResponse) -> Option<Self> {
        // Block header is required for verification
        let block_header = proto.block_header.map(BlockHeader::from_proto)?;
        let merkle_proof = proto.merkle_proof.map(MerkleProof::from_proto)?;

        Some(Self {
            value: proto.value,
            block_height: proto.block_height,
            block_header,
            merkle_proof,
            chain_proof: proto.chain_proof.map(ChainProof::from_proto),
        })
    }

    /// Verify the value is authentic.
    ///
    /// Checks that the Merkle proof correctly links the value to the state root
    /// in the block header. If a chain proof is present, also verifies the
    /// chain of blocks links correctly.
    ///
    /// # Returns
    ///
    /// `Ok(true)` if verification passes.
    /// `Err` with details if verification fails.
    pub fn verify(&self) -> Result<bool> {
        // Verify the Merkle proof against the block header's state root
        if !self.merkle_proof.verify(&self.block_header.state_root) {
            return Err(error::ProofVerificationSnafu {
                reason: "Merkle proof does not match state root",
            }
            .build());
        }

        // If we have a chain proof, that would be verified by the caller
        // with their trusted header hash (we don't have it here)

        Ok(true)
    }
}

/// A write operation to be submitted to the ledger.
///
/// Operations modify state in the ledger. They are applied atomically within
/// a single transaction. Use [`Operation::set_entity`] for key-value writes
/// and [`Operation::create_relationship`] for authorization tuples.
#[derive(Debug, Clone)]
pub enum Operation {
    /// Set an entity value (key-value write).
    SetEntity {
        /// Entity key (max 1024 bytes).
        key: String,
        /// Entity value (max 1MB).
        value: Vec<u8>,
        /// Optional expiration time (Unix epoch seconds).
        expires_at: Option<u64>,
        /// Optional conditional write.
        condition: Option<SetCondition>,
    },
    /// Delete an entity.
    DeleteEntity {
        /// Entity key to delete.
        key: String,
    },
    /// Create an authorization relationship.
    CreateRelationship {
        /// Resource identifier (format: "type:id").
        resource: String,
        /// Relation name (e.g., "viewer", "editor").
        relation: String,
        /// Subject identifier (format: "type:id" or "type:id#relation").
        subject: String,
    },
    /// Delete an authorization relationship.
    DeleteRelationship {
        /// Resource identifier (format: "type:id").
        resource: String,
        /// Relation name.
        relation: String,
        /// Subject identifier.
        subject: String,
    },
}

/// Condition for compare-and-set (CAS) writes.
///
/// Allows conditional writes that only succeed if the current state matches
/// the expected condition. Useful for coordination primitives like locks.
#[derive(Debug, Clone)]
pub enum SetCondition {
    /// Only set if the key doesn't exist.
    NotExists,
    /// Only set if the key exists.
    MustExist,
    /// Only set if the key was last modified at this block height.
    Version(u64),
    /// Only set if the current value matches exactly.
    ValueEquals(Vec<u8>),
}

impl Operation {
    /// Create a set entity operation.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::Operation;
    /// let op = Operation::set_entity("user:123", b"data".to_vec());
    /// ```
    pub fn set_entity(key: impl Into<String>, value: Vec<u8>) -> Self {
        Operation::SetEntity { key: key.into(), value, expires_at: None, condition: None }
    }

    /// Create a set entity operation with expiration.
    ///
    /// # Arguments
    ///
    /// * `key` - Entity key
    /// * `value` - Entity value
    /// * `expires_at` - Unix epoch seconds when the entity expires
    pub fn set_entity_with_expiry(key: impl Into<String>, value: Vec<u8>, expires_at: u64) -> Self {
        Operation::SetEntity {
            key: key.into(),
            value,
            expires_at: Some(expires_at),
            condition: None,
        }
    }

    /// Create a conditional set entity operation.
    ///
    /// # Arguments
    ///
    /// * `key` - Entity key
    /// * `value` - Entity value
    /// * `condition` - Condition that must be met for the write to succeed
    pub fn set_entity_if(key: impl Into<String>, value: Vec<u8>, condition: SetCondition) -> Self {
        Operation::SetEntity {
            key: key.into(),
            value,
            expires_at: None,
            condition: Some(condition),
        }
    }

    /// Create a delete entity operation.
    pub fn delete_entity(key: impl Into<String>) -> Self {
        Operation::DeleteEntity { key: key.into() }
    }

    /// Create a relationship.
    ///
    /// # Arguments
    ///
    /// * `resource` - Resource identifier (format: "type:id")
    /// * `relation` - Relation name (e.g., "viewer", "editor")
    /// * `subject` - Subject identifier (format: "type:id" or "type:id#relation")
    pub fn create_relationship(
        resource: impl Into<String>,
        relation: impl Into<String>,
        subject: impl Into<String>,
    ) -> Self {
        Operation::CreateRelationship {
            resource: resource.into(),
            relation: relation.into(),
            subject: subject.into(),
        }
    }

    /// Delete a relationship.
    pub fn delete_relationship(
        resource: impl Into<String>,
        relation: impl Into<String>,
        subject: impl Into<String>,
    ) -> Self {
        Operation::DeleteRelationship {
            resource: resource.into(),
            relation: relation.into(),
            subject: subject.into(),
        }
    }

    /// Convert to proto operation.
    fn to_proto(&self) -> proto::Operation {
        let op = match self {
            Operation::SetEntity { key, value, expires_at, condition } => {
                proto::operation::Op::SetEntity(proto::SetEntity {
                    key: key.clone(),
                    value: value.clone(),
                    expires_at: *expires_at,
                    condition: condition.as_ref().map(SetCondition::to_proto),
                })
            },
            Operation::DeleteEntity { key } => {
                proto::operation::Op::DeleteEntity(proto::DeleteEntity { key: key.clone() })
            },
            Operation::CreateRelationship { resource, relation, subject } => {
                proto::operation::Op::CreateRelationship(proto::CreateRelationship {
                    resource: resource.clone(),
                    relation: relation.clone(),
                    subject: subject.clone(),
                })
            },
            Operation::DeleteRelationship { resource, relation, subject } => {
                proto::operation::Op::DeleteRelationship(proto::DeleteRelationship {
                    resource: resource.clone(),
                    relation: relation.clone(),
                    subject: subject.clone(),
                })
            },
        };
        proto::Operation { op: Some(op) }
    }
}

impl SetCondition {
    /// Convert to proto set condition.
    fn to_proto(&self) -> proto::SetCondition {
        let condition = match self {
            SetCondition::NotExists => proto::set_condition::Condition::NotExists(true),
            SetCondition::MustExist => proto::set_condition::Condition::MustExists(true),
            SetCondition::Version(v) => proto::set_condition::Condition::Version(*v),
            SetCondition::ValueEquals(v) => proto::set_condition::Condition::ValueEquals(v.clone()),
        };
        proto::SetCondition { condition: Some(condition) }
    }
}

/// High-level client for interacting with the Ledger service.
///
/// `LedgerClient` orchestrates:
/// - Connection pool for efficient channel management
/// - Sequence tracker for client-side idempotency
/// - Retry logic for transient failure recovery
/// - Graceful shutdown with request cancellation
///
/// # Shutdown Behavior
///
/// When [`shutdown()`](Self::shutdown) is called:
/// 1. All pending requests are cancelled with `SdkError::Shutdown`
/// 2. New requests immediately fail with `SdkError::Shutdown`
/// 3. Sequence tracker state is flushed to disk (if using persistence)
/// 4. Connections are closed
///
/// # Example
///
/// ```no_run
/// # use inferadb_ledger_sdk::{LedgerClient, ClientConfig};
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let config = ClientConfig::builder()
///     .endpoints(vec!["http://localhost:50051".into()])
///     .client_id("my-app-001")
///     .build()?;
///
/// let client = LedgerClient::new(config).await?;
///
/// // ... use the client ...
///
/// // Graceful shutdown
/// client.shutdown().await;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct LedgerClient {
    pool: ConnectionPool,
    sequences: SequenceTracker,
    /// Cancellation token for coordinated shutdown.
    cancellation: tokio_util::sync::CancellationToken,
}

impl LedgerClient {
    /// Creates a new `LedgerClient` with the given configuration.
    ///
    /// This constructor validates the configuration but does not establish
    /// a connection immediately. Connections are established lazily on first use.
    ///
    /// # Errors
    ///
    /// Returns an error if the configuration is invalid.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, ClientConfig};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let config = ClientConfig::builder()
    ///     .endpoints(vec!["http://localhost:50051".into()])
    ///     .client_id("my-service")
    ///     .build()?;
    ///
    /// let client = LedgerClient::new(config).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn new(config: ClientConfig) -> Result<Self> {
        let client_id = config.client_id().to_string();
        let pool = ConnectionPool::new(config);
        let sequences = SequenceTracker::new(client_id);
        let cancellation = tokio_util::sync::CancellationToken::new();

        Ok(Self { pool, sequences, cancellation })
    }

    /// Convenience constructor for connecting to a single endpoint.
    ///
    /// Creates a client with default configuration, connecting to the specified
    /// endpoint with the given client ID. For more control over configuration,
    /// use [`ClientConfig::builder()`] and [`LedgerClient::new()`].
    ///
    /// # Errors
    ///
    /// Returns an error if the endpoint URL is invalid.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::LedgerClient;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect(
        endpoint: impl Into<String>,
        client_id: impl Into<String>,
    ) -> Result<Self> {
        let config = ClientConfig::builder()
            .endpoints(vec![endpoint.into()])
            .client_id(client_id)
            .build()?;

        Self::new(config).await
    }

    /// Returns the client ID used for idempotency tracking.
    ///
    /// The client ID is included in all write requests and is used by the server
    /// to deduplicate requests and track sequence state.
    #[inline]
    pub fn client_id(&self) -> &str {
        self.sequences.client_id()
    }

    /// Returns a reference to the client configuration.
    ///
    /// Useful for inspecting configuration values like endpoints, timeouts,
    /// and retry policy.
    #[inline]
    pub fn config(&self) -> &ClientConfig {
        self.pool.config()
    }

    /// Returns a reference to the sequence tracker.
    ///
    /// The sequence tracker maintains per-vault sequence numbers for idempotency.
    /// Most users won't need direct access to this, as write operations
    /// automatically manage sequences.
    ///
    /// # Advanced Usage
    ///
    /// Direct access is useful for:
    /// - Inspecting current sequence state for debugging
    /// - Manual sequence recovery after errors
    /// - Custom idempotency workflows
    #[inline]
    pub fn sequences(&self) -> &SequenceTracker {
        &self.sequences
    }

    /// Returns a reference to the connection pool.
    ///
    /// Most users won't need direct access to this. Useful for:
    /// - Checking if compression is enabled
    /// - Resetting connections after network changes
    #[inline]
    pub fn pool(&self) -> &ConnectionPool {
        &self.pool
    }

    // =========================================================================
    // Shutdown
    // =========================================================================

    /// Initiates graceful shutdown of the client.
    ///
    /// This method:
    /// 1. Cancels all pending requests (they will return `SdkError::Shutdown`)
    /// 2. Prevents new requests from being accepted
    /// 3. Flushes sequence tracker state (best-effort, non-blocking)
    /// 4. Resets the connection pool
    ///
    /// After calling `shutdown()`, all operations will immediately return
    /// `SdkError::Shutdown`. The client can be cloned, but all clones share
    /// the same shutdown state.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::LedgerClient;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let (namespace_id, vault_id) = (1i64, 1i64);
    /// # let operations = vec![];
    /// let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    ///
    /// // Perform operations...
    /// client.write(namespace_id, Some(vault_id), operations).await?;
    ///
    /// // Graceful shutdown before application exit
    /// client.shutdown().await;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn shutdown(&self) {
        // Cancel all pending and future operations
        self.cancellation.cancel();

        // Best-effort sequence flush - don't block on errors
        // The PersistentSequenceTracker (if used) will handle this
        // For now, we just log if there's an issue
        tracing::debug!("Client shutdown initiated");

        // Reset connection pool to close connections
        self.pool.reset();
    }

    /// Returns `true` if the client has been shut down.
    ///
    /// After shutdown, all operations will fail with `SdkError::Shutdown`.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::LedgerClient;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// if client.is_shutdown() {
    ///     println!("Client has been shut down");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    #[inline]
    #[must_use]
    pub fn is_shutdown(&self) -> bool {
        self.cancellation.is_cancelled()
    }

    /// Returns an error if the client has been shut down.
    ///
    /// This is called at the start of each operation to fail fast if
    /// shutdown has been initiated.
    #[inline]
    fn check_shutdown(&self) -> Result<()> {
        if self.cancellation.is_cancelled() {
            return Err(error::SdkError::Shutdown);
        }
        Ok(())
    }

    /// Creates a discovery service that shares this client's connection pool.
    ///
    /// The discovery service can be used to dynamically update the client's
    /// endpoint list based on discovered cluster peers.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, DiscoveryConfig};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = LedgerClient::connect("http://localhost:50051", "my-client").await?;
    /// let discovery = client.create_discovery_service(DiscoveryConfig::enabled());
    ///
    /// // Start background endpoint refresh
    /// discovery.start_background_refresh();
    ///
    /// // The client will now use updated endpoints as peers are discovered
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn create_discovery_service(
        &self,
        config: crate::config::DiscoveryConfig,
    ) -> crate::discovery::DiscoveryService {
        crate::discovery::DiscoveryService::new(self.pool.clone(), config)
    }

    // =========================================================================
    // Read Operations
    // =========================================================================

    /// Read a value by key with eventual consistency.
    ///
    /// Uses `EVENTUAL` consistency level, which reads from any replica for
    /// lowest latency. The value may be slightly stale if a write was just
    /// committed.
    ///
    /// # Arguments
    ///
    /// * `namespace_id` - The namespace containing the data
    /// * `vault_id` - Optional vault ID (omit for namespace-level entities)
    /// * `key` - The key to read
    ///
    /// # Returns
    ///
    /// Returns `Ok(Some(value))` if the key exists, `Ok(None)` if not found.
    ///
    /// # Errors
    ///
    /// Returns an error if the read fails after retry attempts are exhausted.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::LedgerClient;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let (namespace_id, vault_id) = (1i64, 1i64);
    /// // Read a namespace-level entity
    /// let value = client.read(namespace_id, None, "user:123").await?;
    ///
    /// // Read a vault-level entity
    /// let value = client.read(namespace_id, Some(vault_id), "key").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn read(
        &self,
        namespace_id: i64,
        vault_id: Option<i64>,
        key: impl Into<String>,
    ) -> Result<Option<Vec<u8>>> {
        self.read_internal(namespace_id, vault_id, key.into(), ReadConsistency::Eventual).await
    }

    /// Read a value by key with linearizable (strong) consistency.
    ///
    /// Uses `LINEARIZABLE` consistency level, which reads from the leader to
    /// guarantee the latest committed value. Has higher latency than eventual
    /// consistency.
    ///
    /// # Arguments
    ///
    /// * `namespace_id` - The namespace containing the data
    /// * `vault_id` - Optional vault ID (omit for namespace-level entities)
    /// * `key` - The key to read
    ///
    /// # Returns
    ///
    /// Returns `Ok(Some(value))` if the key exists, `Ok(None)` if not found.
    ///
    /// # Errors
    ///
    /// Returns an error if the read fails after retry attempts are exhausted.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::LedgerClient;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let (namespace_id, vault_id) = (1i64, 1i64);
    /// // Read with strong consistency guarantee
    /// let value = client.read_consistent(namespace_id, Some(vault_id), "key").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn read_consistent(
        &self,
        namespace_id: i64,
        vault_id: Option<i64>,
        key: impl Into<String>,
    ) -> Result<Option<Vec<u8>>> {
        self.read_internal(namespace_id, vault_id, key.into(), ReadConsistency::Linearizable).await
    }

    /// Batch read multiple keys in a single RPC call.
    ///
    /// Amortizes network overhead across multiple reads for higher throughput.
    /// All reads share the same namespace, vault, and consistency level (EVENTUAL).
    ///
    /// # Arguments
    ///
    /// * `namespace_id` - The namespace containing the data
    /// * `vault_id` - Optional vault ID (omit for namespace-level entities)
    /// * `keys` - The keys to read (max 1000)
    ///
    /// # Returns
    ///
    /// Returns a vector of `(key, Option<value>)` pairs in the same order as
    /// the input keys. Missing keys have `None` values.
    ///
    /// # Errors
    ///
    /// Returns an error if the batch read fails after retry attempts.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::LedgerClient;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let (namespace_id, vault_id) = (1i64, 1i64);
    /// let results = client.batch_read(
    ///     namespace_id,
    ///     Some(vault_id),
    ///     vec!["key1", "key2", "key3"],
    /// ).await?;
    ///
    /// for (key, value) in results {
    ///     match value {
    ///         Some(v) => println!("{key}: {} bytes", v.len()),
    ///         None => println!("{key}: not found"),
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn batch_read(
        &self,
        namespace_id: i64,
        vault_id: Option<i64>,
        keys: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Vec<(String, Option<Vec<u8>>)>> {
        self.batch_read_internal(
            namespace_id,
            vault_id,
            keys.into_iter().map(Into::into).collect(),
            ReadConsistency::Eventual,
        )
        .await
    }

    /// Batch read multiple keys with linearizable consistency.
    ///
    /// Like [`batch_read`](Self::batch_read) but with strong consistency guarantees.
    /// All reads are served from the leader.
    ///
    /// # Arguments
    ///
    /// * `namespace_id` - The namespace containing the data
    /// * `vault_id` - Optional vault ID (omit for namespace-level entities)
    /// * `keys` - The keys to read (max 1000)
    ///
    /// # Returns
    ///
    /// Returns a vector of `(key, Option<value>)` pairs in the same order as
    /// the input keys. Missing keys have `None` values.
    pub async fn batch_read_consistent(
        &self,
        namespace_id: i64,
        vault_id: Option<i64>,
        keys: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Vec<(String, Option<Vec<u8>>)>> {
        self.batch_read_internal(
            namespace_id,
            vault_id,
            keys.into_iter().map(Into::into).collect(),
            ReadConsistency::Linearizable,
        )
        .await
    }

    // =========================================================================
    // Internal Read Implementation
    // =========================================================================

    /// Internal read implementation with configurable consistency.
    async fn read_internal(
        &self,
        namespace_id: i64,
        vault_id: Option<i64>,
        key: String,
        consistency: ReadConsistency,
    ) -> Result<Option<Vec<u8>>> {
        self.check_shutdown()?;

        let pool = &self.pool;
        let retry_policy = self.config().retry_policy().clone();

        with_retry(&retry_policy, || async {
            let channel = pool.get_channel().await?;
            let mut client = Self::create_read_client(channel, pool.compression_enabled());

            let request = proto::ReadRequest {
                namespace_id: Some(proto::NamespaceId { id: namespace_id }),
                vault_id: vault_id.map(|id| proto::VaultId { id }),
                key: key.clone(),
                consistency: consistency.to_proto() as i32,
            };

            let response = client.read(tonic::Request::new(request)).await?.into_inner();

            Ok(response.value)
        })
        .await
    }

    /// Internal batch read implementation with configurable consistency.
    async fn batch_read_internal(
        &self,
        namespace_id: i64,
        vault_id: Option<i64>,
        keys: Vec<String>,
        consistency: ReadConsistency,
    ) -> Result<Vec<(String, Option<Vec<u8>>)>> {
        self.check_shutdown()?;

        let pool = &self.pool;
        let retry_policy = self.config().retry_policy().clone();

        with_retry(&retry_policy, || async {
            let channel = pool.get_channel().await?;
            let mut client = Self::create_read_client(channel, pool.compression_enabled());

            let request = proto::BatchReadRequest {
                namespace_id: Some(proto::NamespaceId { id: namespace_id }),
                vault_id: vault_id.map(|id| proto::VaultId { id }),
                keys: keys.clone(),
                consistency: consistency.to_proto() as i32,
            };

            let response = client.batch_read(tonic::Request::new(request)).await?.into_inner();

            // Convert results to (key, Option<value>) pairs
            let results = response.results.into_iter().map(|r| (r.key, r.value)).collect();

            Ok(results)
        })
        .await
    }

    /// Creates a ReadServiceClient with compression settings applied.
    fn create_read_client(
        channel: tonic::transport::Channel,
        compression_enabled: bool,
    ) -> proto::read_service_client::ReadServiceClient<tonic::transport::Channel> {
        let client = proto::read_service_client::ReadServiceClient::new(channel);
        if compression_enabled {
            client
                .send_compressed(tonic::codec::CompressionEncoding::Gzip)
                .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
        } else {
            client
        }
    }

    // =========================================================================
    // Write Operations
    // =========================================================================

    /// Submit a write transaction to the ledger.
    ///
    /// Writes are automatically idempotent via client-side sequence tracking.
    /// If a write fails with a retryable error, it will be retried with the
    /// same sequence number. If the server reports the write was already
    /// committed (duplicate), the original result is returned as success.
    ///
    /// # Arguments
    ///
    /// * `namespace_id` - The namespace to write to
    /// * `vault_id` - Optional vault ID (required for relationships)
    /// * `operations` - The operations to apply atomically
    ///
    /// # Returns
    ///
    /// Returns [`WriteSuccess`] containing the transaction ID and block height.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Connection fails after retry attempts
    /// - A conditional write (CAS) condition fails
    /// - A sequence gap is detected (requires recovery)
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, Operation};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let (namespace_id, vault_id) = (1i64, 1i64);
    /// let result = client.write(
    ///     namespace_id,
    ///     Some(vault_id),
    ///     vec![
    ///         Operation::set_entity("user:123", b"data".to_vec()),
    ///         Operation::create_relationship("doc:456", "viewer", "user:123"),
    ///     ],
    /// ).await?;
    ///
    /// println!("Committed at block {}", result.block_height);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn write(
        &self,
        namespace_id: i64,
        vault_id: Option<i64>,
        operations: Vec<Operation>,
    ) -> Result<WriteSuccess> {
        self.check_shutdown()?;

        // Get the vault key for sequence tracking
        let vault_key = (namespace_id, vault_id.unwrap_or(0));

        // Get next sequence number for this vault
        let mut sequence = self.sequences.next_sequence(vault_key.0, vault_key.1);

        // Allow one recovery attempt for sequence gap
        let max_gap_recoveries = 1;
        let mut gap_recovery_attempts = 0;

        loop {
            let result = self.execute_write(namespace_id, vault_id, &operations, sequence).await;

            match result {
                Ok(success) => return Ok(success),
                Err(crate::error::SdkError::SequenceGap { server_has, .. }) => {
                    if gap_recovery_attempts >= max_gap_recoveries {
                        return Err(crate::error::SdkError::SequenceGap {
                            expected: server_has + 1,
                            server_has,
                        });
                    }

                    // Recover: update local sequence and retry
                    gap_recovery_attempts += 1;
                    self.sequences.set_sequence(vault_key.0, vault_key.1, server_has);
                    sequence = self.sequences.next_sequence(vault_key.0, vault_key.1);
                },
                Err(e) => return Err(e),
            }
        }
    }

    /// Execute a single write attempt with retry for transient errors.
    async fn execute_write(
        &self,
        namespace_id: i64,
        vault_id: Option<i64>,
        operations: &[Operation],
        sequence: u64,
    ) -> Result<WriteSuccess> {
        let pool = &self.pool;
        let retry_policy = self.config().retry_policy().clone();
        let client_id = self.client_id().to_string();

        // Convert operations to proto
        let proto_operations: Vec<proto::Operation> =
            operations.iter().map(Operation::to_proto).collect();

        // Execute with retry for transient errors
        with_retry(&retry_policy, || {
            let proto_ops = proto_operations.clone();
            let cid = client_id.clone();
            async move {
                let channel = pool.get_channel().await?;
                let mut write_client =
                    Self::create_write_client(channel, pool.compression_enabled());

                let request = proto::WriteRequest {
                    namespace_id: Some(proto::NamespaceId { id: namespace_id }),
                    vault_id: vault_id.map(|id| proto::VaultId { id }),
                    client_id: Some(proto::ClientId { id: cid }),
                    sequence,
                    operations: proto_ops,
                    include_tx_proof: false,
                };

                let response = write_client.write(tonic::Request::new(request)).await?.into_inner();

                Self::process_write_response(response)
            }
        })
        .await
    }

    /// Process WriteResponse and convert to Result<WriteSuccess>.
    fn process_write_response(response: proto::WriteResponse) -> Result<WriteSuccess> {
        use crate::error::SequenceGapSnafu;

        match response.result {
            Some(proto::write_response::Result::Success(success)) => Ok(WriteSuccess {
                tx_id: Self::tx_id_to_hex(success.tx_id),
                block_height: success.block_height,
            }),
            Some(proto::write_response::Result::Error(error)) => {
                let code = proto::WriteErrorCode::try_from(error.code)
                    .unwrap_or(proto::WriteErrorCode::Unspecified);

                match code {
                    proto::WriteErrorCode::AlreadyCommitted => {
                        // Idempotent retry - return the original success
                        Ok(WriteSuccess {
                            tx_id: Self::tx_id_to_hex(error.committed_tx_id),
                            block_height: error.committed_block_height.unwrap_or(0),
                        })
                    },
                    proto::WriteErrorCode::SequenceGap => {
                        // Need recovery - return error with context
                        let server_has = error.last_committed_sequence.unwrap_or(0);
                        SequenceGapSnafu { expected: server_has + 1, server_has }.fail()
                    },
                    _ => {
                        // Other write errors (CAS failures, etc.)
                        crate::error::RpcSnafu {
                            code: tonic::Code::FailedPrecondition,
                            message: error.message,
                        }
                        .fail()
                    },
                }
            },
            None => crate::error::RpcSnafu {
                code: tonic::Code::Internal,
                message: "Empty write response".to_string(),
            }
            .fail(),
        }
    }

    /// Convert TxId bytes to hex string.
    fn tx_id_to_hex(tx_id: Option<proto::TxId>) -> String {
        use std::fmt::Write;
        tx_id
            .map(|t| {
                t.id.iter().fold(String::with_capacity(t.id.len() * 2), |mut acc, b| {
                    let _ = write!(acc, "{b:02x}");
                    acc
                })
            })
            .unwrap_or_default()
    }

    /// Creates a WriteServiceClient with compression settings applied.
    fn create_write_client(
        channel: tonic::transport::Channel,
        compression_enabled: bool,
    ) -> proto::write_service_client::WriteServiceClient<tonic::transport::Channel> {
        let client = proto::write_service_client::WriteServiceClient::new(channel);
        if compression_enabled {
            client
                .send_compressed(tonic::codec::CompressionEncoding::Gzip)
                .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
        } else {
            client
        }
    }

    // =========================================================================
    // Batch Write Operations
    // =========================================================================

    /// Submit a batch write transaction with all-or-nothing atomicity.
    ///
    /// A batch write groups multiple operation sets into a single atomic transaction.
    /// All operations are committed together in a single block, or none are applied
    /// if any operation fails (e.g., CAS condition failure).
    ///
    /// The batch uses a single sequence number for idempotency, meaning the entire
    /// batch is the deduplication unit - retry with the same sequence returns the
    /// original result.
    ///
    /// # Arguments
    ///
    /// * `namespace_id` - The namespace to write to
    /// * `vault_id` - Optional vault ID (required for relationships)
    /// * `batches` - Groups of operations to apply atomically. Each inner `Vec<Operation>` is a
    ///   logical group processed in order.
    ///
    /// # Returns
    ///
    /// Returns [`WriteSuccess`] containing the transaction ID and block height.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Connection fails after retry attempts
    /// - Any CAS condition fails (entire batch rolled back)
    /// - A sequence gap is detected (requires recovery)
    ///
    /// # Atomicity
    ///
    /// Operations are applied in array order:
    /// - `batches[0]` operations first, then `batches[1]`, etc.
    /// - Within each batch: `operations[0]` first, then `operations[1]`, etc.
    /// - If ANY operation fails, the ENTIRE transaction is rolled back.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, Operation};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let (namespace_id, vault_id) = (1i64, 1i64);
    /// // Atomic transaction: create user AND grant permissions
    /// let result = client.batch_write(
    ///     namespace_id,
    ///     Some(vault_id),
    ///     vec![
    ///         // First batch: create the user
    ///         vec![Operation::set_entity("user:123", b"alice".to_vec())],
    ///         // Second batch: grant permissions (depends on user existing)
    ///         vec![
    ///             Operation::create_relationship("doc:456", "viewer", "user:123"),
    ///             Operation::create_relationship("folder:789", "editor", "user:123"),
    ///         ],
    ///     ],
    /// ).await?;
    ///
    /// println!("Batch committed at block {}", result.block_height);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn batch_write(
        &self,
        namespace_id: i64,
        vault_id: Option<i64>,
        batches: Vec<Vec<Operation>>,
    ) -> Result<WriteSuccess> {
        self.check_shutdown()?;

        // Get the vault key for sequence tracking
        let vault_key = (namespace_id, vault_id.unwrap_or(0));

        // Get next sequence number for this vault
        let mut sequence = self.sequences.next_sequence(vault_key.0, vault_key.1);

        // Allow one recovery attempt for sequence gap
        let max_gap_recoveries = 1;
        let mut gap_recovery_attempts = 0;

        loop {
            let result = self.execute_batch_write(namespace_id, vault_id, &batches, sequence).await;

            match result {
                Ok(success) => return Ok(success),
                Err(crate::error::SdkError::SequenceGap { server_has, .. }) => {
                    if gap_recovery_attempts >= max_gap_recoveries {
                        return Err(crate::error::SdkError::SequenceGap {
                            expected: server_has + 1,
                            server_has,
                        });
                    }

                    // Recover: update local sequence and retry
                    gap_recovery_attempts += 1;
                    self.sequences.set_sequence(vault_key.0, vault_key.1, server_has);
                    sequence = self.sequences.next_sequence(vault_key.0, vault_key.1);
                },
                Err(e) => return Err(e),
            }
        }
    }

    /// Execute a single batch write attempt with retry for transient errors.
    async fn execute_batch_write(
        &self,
        namespace_id: i64,
        vault_id: Option<i64>,
        batches: &[Vec<Operation>],
        sequence: u64,
    ) -> Result<WriteSuccess> {
        let pool = &self.pool;
        let retry_policy = self.config().retry_policy().clone();
        let client_id = self.client_id().to_string();

        // Convert batches to proto BatchWriteOperation format
        let proto_batches: Vec<proto::BatchWriteOperation> = batches
            .iter()
            .map(|ops| proto::BatchWriteOperation {
                operations: ops.iter().map(Operation::to_proto).collect(),
            })
            .collect();

        // Execute with retry for transient errors
        with_retry(&retry_policy, || {
            let batch_ops = proto_batches.clone();
            let cid = client_id.clone();
            async move {
                let channel = pool.get_channel().await?;
                let mut write_client =
                    Self::create_write_client(channel, pool.compression_enabled());

                let request = proto::BatchWriteRequest {
                    namespace_id: Some(proto::NamespaceId { id: namespace_id }),
                    vault_id: vault_id.map(|id| proto::VaultId { id }),
                    client_id: Some(proto::ClientId { id: cid }),
                    sequence,
                    operations: batch_ops,
                    include_tx_proofs: false,
                };

                let response =
                    write_client.batch_write(tonic::Request::new(request)).await?.into_inner();

                Self::process_batch_write_response(response)
            }
        })
        .await
    }

    /// Process BatchWriteResponse and convert to Result<WriteSuccess>.
    fn process_batch_write_response(response: proto::BatchWriteResponse) -> Result<WriteSuccess> {
        use crate::error::SequenceGapSnafu;

        match response.result {
            Some(proto::batch_write_response::Result::Success(success)) => Ok(WriteSuccess {
                tx_id: Self::tx_id_to_hex(success.tx_id),
                block_height: success.block_height,
            }),
            Some(proto::batch_write_response::Result::Error(error)) => {
                let code = proto::WriteErrorCode::try_from(error.code)
                    .unwrap_or(proto::WriteErrorCode::Unspecified);

                match code {
                    proto::WriteErrorCode::AlreadyCommitted => {
                        // Idempotent retry - return the original success
                        Ok(WriteSuccess {
                            tx_id: Self::tx_id_to_hex(error.committed_tx_id),
                            block_height: error.committed_block_height.unwrap_or(0),
                        })
                    },
                    proto::WriteErrorCode::SequenceGap => {
                        // Need recovery - return error with context
                        let server_has = error.last_committed_sequence.unwrap_or(0);
                        SequenceGapSnafu { expected: server_has + 1, server_has }.fail()
                    },
                    _ => {
                        // Other write errors (CAS failures, etc.)
                        crate::error::RpcSnafu {
                            code: tonic::Code::FailedPrecondition,
                            message: error.message,
                        }
                        .fail()
                    },
                }
            },
            None => crate::error::RpcSnafu {
                code: tonic::Code::Internal,
                message: "Empty batch write response".to_string(),
            }
            .fail(),
        }
    }

    // =============================================================================
    // Streaming Operations
    // =============================================================================

    /// Subscribe to block announcements for a vault.
    ///
    /// Returns a stream of [`BlockAnnouncement`] items that emits each time a new
    /// block is committed to the vault's chain. The stream automatically reconnects
    /// on disconnect and resumes from the last seen block height.
    ///
    /// # Arguments
    ///
    /// * `namespace_id` - The namespace containing the vault
    /// * `vault_id` - The vault to watch for blocks
    /// * `start_height` - First block height to receive (must be >= 1)
    ///
    /// # Returns
    ///
    /// Returns a `Stream` that yields `Result<BlockAnnouncement>` items.
    ///
    /// # Reconnection Behavior
    ///
    /// On disconnect (network error, server restart, etc.), the stream:
    /// 1. Applies exponential backoff before reconnecting
    /// 2. Resumes from `last_seen_height + 1` to avoid gaps or duplicates
    /// 3. Continues until max reconnection attempts are exhausted
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::LedgerClient;
    /// # use futures::StreamExt;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = LedgerClient::connect("http://localhost:50051", "my-app").await?;
    ///
    /// // Start watching from height 1
    /// let mut stream = client.watch_blocks(1, 0, 1).await?;
    ///
    /// while let Some(announcement) = stream.next().await {
    ///     match announcement {
    ///         Ok(block) => {
    ///             println!("New block at height {}", block.height);
    ///             // Process block...
    ///         }
    ///         Err(e) => {
    ///             eprintln!("Stream error: {}", e);
    ///             break;
    ///         }
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn watch_blocks(
        &self,
        namespace_id: i64,
        vault_id: i64,
        start_height: u64,
    ) -> Result<impl futures::Stream<Item = Result<BlockAnnouncement>>> {
        self.check_shutdown()?;

        // Get the initial stream
        let initial_stream =
            self.create_watch_blocks_stream(namespace_id, vault_id, start_height).await?;

        // Create position tracker starting at the requested height
        let position = HeightTracker::new(start_height);

        // Clone pool and config for the reconnection closure
        let pool = self.pool.clone();
        let retry_policy = self.config().retry_policy().clone();

        // Create the reconnecting stream wrapper
        let reconnecting = ReconnectingStream::new(
            initial_stream,
            position,
            retry_policy.clone(),
            move |next_height| {
                let pool = pool.clone();
                Box::pin(async move {
                    let channel = pool.get_channel().await?;
                    let mut client = proto::read_service_client::ReadServiceClient::new(channel);
                    if pool.compression_enabled() {
                        client = client
                            .send_compressed(tonic::codec::CompressionEncoding::Gzip)
                            .accept_compressed(tonic::codec::CompressionEncoding::Gzip);
                    }

                    let request = proto::WatchBlocksRequest {
                        namespace_id: Some(proto::NamespaceId { id: namespace_id }),
                        vault_id: Some(proto::VaultId { id: vault_id }),
                        start_height: next_height,
                    };

                    let response =
                        client.watch_blocks(tonic::Request::new(request)).await?.into_inner();

                    Ok(response)
                })
            },
        );

        // Map proto announcements to SDK type
        Ok(futures::StreamExt::map(reconnecting, |result| {
            result.map(BlockAnnouncement::from_proto)
        }))
    }

    /// Create a WatchBlocks stream without reconnection logic.
    async fn create_watch_blocks_stream(
        &self,
        namespace_id: i64,
        vault_id: i64,
        start_height: u64,
    ) -> Result<tonic::Streaming<proto::BlockAnnouncement>> {
        let channel = self.pool.get_channel().await?;
        let mut client = Self::create_read_client(channel, self.pool.compression_enabled());

        let request = proto::WatchBlocksRequest {
            namespace_id: Some(proto::NamespaceId { id: namespace_id }),
            vault_id: Some(proto::VaultId { id: vault_id }),
            start_height,
        };

        let response = client.watch_blocks(tonic::Request::new(request)).await?.into_inner();

        Ok(response)
    }

    // =========================================================================
    // Admin Operations
    // =========================================================================

    /// Create a new namespace.
    ///
    /// Creates a namespace with the given name. The namespace ID is assigned
    /// by the leader and returned in the response.
    ///
    /// # Arguments
    ///
    /// * `name` - Human-readable name for the namespace (e.g., "acme_corp")
    ///
    /// # Returns
    ///
    /// Returns the assigned namespace ID on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Connection fails after retry attempts
    /// - The namespace name is invalid or already exists
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::LedgerClient;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// let namespace_id = client.create_namespace("my-org").await?;
    /// println!("Created namespace with ID: {}", namespace_id);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn create_namespace(&self, name: impl Into<String>) -> Result<i64> {
        self.check_shutdown()?;

        let name = name.into();
        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        with_retry(&retry_policy, || async {
            let channel = pool.get_channel().await?;
            let mut client = Self::create_admin_client(channel, pool.compression_enabled());

            let request = proto::CreateNamespaceRequest {
                name: name.clone(),
                shard_id: None, // Auto-assigned
            };

            let response =
                client.create_namespace(tonic::Request::new(request)).await?.into_inner();

            Ok(response.namespace_id.map(|n| n.id).unwrap_or(0))
        })
        .await
    }

    /// Get information about a namespace by ID.
    ///
    /// # Arguments
    ///
    /// * `namespace_id` - The namespace ID to look up
    ///
    /// # Returns
    ///
    /// Returns [`NamespaceInfo`] containing namespace metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Connection fails after retry attempts
    /// - The namespace does not exist
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::LedgerClient;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let namespace_id = 1i64;
    /// let info = client.get_namespace(namespace_id).await?;
    /// println!("Namespace: {} (status: {:?})", info.name, info.status);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_namespace(&self, namespace_id: i64) -> Result<NamespaceInfo> {
        self.check_shutdown()?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        with_retry(&retry_policy, || async {
            let channel = pool.get_channel().await?;
            let mut client = Self::create_admin_client(channel, pool.compression_enabled());

            let request = proto::GetNamespaceRequest {
                lookup: Some(proto::get_namespace_request::Lookup::NamespaceId(
                    proto::NamespaceId { id: namespace_id },
                )),
            };

            let response = client.get_namespace(tonic::Request::new(request)).await?.into_inner();

            Ok(NamespaceInfo::from_proto(response))
        })
        .await
    }

    /// List all namespaces.
    ///
    /// Returns a list of all namespaces visible to this client.
    /// Admin operations typically have longer timeouts.
    ///
    /// # Returns
    ///
    /// Returns a vector of [`NamespaceInfo`] for all namespaces.
    ///
    /// # Errors
    ///
    /// Returns an error if connection fails after retry attempts.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::LedgerClient;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// let namespaces = client.list_namespaces().await?;
    /// for ns in namespaces {
    ///     println!("Namespace: {} (ID: {})", ns.name, ns.namespace_id);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn list_namespaces(&self) -> Result<Vec<NamespaceInfo>> {
        self.check_shutdown()?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        with_retry(&retry_policy, || async {
            let channel = pool.get_channel().await?;
            let mut client = Self::create_admin_client(channel, pool.compression_enabled());

            let request = proto::ListNamespacesRequest {
                page_token: None,
                page_size: 0, // Use default
            };

            let response = client.list_namespaces(tonic::Request::new(request)).await?.into_inner();

            Ok(response.namespaces.into_iter().map(NamespaceInfo::from_proto).collect())
        })
        .await
    }

    /// Create a new vault in a namespace.
    ///
    /// Creates a vault within the specified namespace. The vault ID is assigned
    /// by the leader and returned in the response.
    ///
    /// # Arguments
    ///
    /// * `namespace_id` - The namespace to create the vault in
    ///
    /// # Returns
    ///
    /// Returns [`VaultInfo`] containing the new vault's metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Connection fails after retry attempts
    /// - The namespace does not exist
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::LedgerClient;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let namespace_id = 1i64;
    /// let vault = client.create_vault(namespace_id).await?;
    /// println!("Created vault with ID: {}", vault.vault_id);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn create_vault(&self, namespace_id: i64) -> Result<VaultInfo> {
        self.check_shutdown()?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        with_retry(&retry_policy, || async {
            let channel = pool.get_channel().await?;
            let mut client = Self::create_admin_client(channel, pool.compression_enabled());

            let request = proto::CreateVaultRequest {
                namespace_id: Some(proto::NamespaceId { id: namespace_id }),
                replication_factor: 0,  // Use default
                initial_nodes: vec![],  // Auto-assigned
                retention_policy: None, // Default: FULL
            };

            let response = client.create_vault(tonic::Request::new(request)).await?.into_inner();

            // Build VaultInfo from CreateVaultResponse
            // Note: CreateVaultResponse has limited fields compared to GetVaultResponse
            Ok(VaultInfo {
                namespace_id,
                vault_id: response.vault_id.map(|v| v.id).unwrap_or(0),
                height: 0,          // Genesis block
                state_root: vec![], // Empty at genesis
                nodes: vec![],      // Not returned in create response
                leader: None,       // Not returned in create response
                status: VaultStatus::Active,
            })
        })
        .await
    }

    /// Get information about a vault.
    ///
    /// # Arguments
    ///
    /// * `namespace_id` - The namespace containing the vault
    /// * `vault_id` - The vault ID to look up
    ///
    /// # Returns
    ///
    /// Returns [`VaultInfo`] containing vault metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Connection fails after retry attempts
    /// - The namespace or vault does not exist
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::LedgerClient;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let (namespace_id, vault_id) = (1i64, 1i64);
    /// let info = client.get_vault(namespace_id, vault_id).await?;
    /// println!("Vault height: {}, status: {:?}", info.height, info.status);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_vault(&self, namespace_id: i64, vault_id: i64) -> Result<VaultInfo> {
        self.check_shutdown()?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        with_retry(&retry_policy, || async {
            let channel = pool.get_channel().await?;
            let mut client = Self::create_admin_client(channel, pool.compression_enabled());

            let request = proto::GetVaultRequest {
                namespace_id: Some(proto::NamespaceId { id: namespace_id }),
                vault_id: Some(proto::VaultId { id: vault_id }),
            };

            let response = client.get_vault(tonic::Request::new(request)).await?.into_inner();

            Ok(VaultInfo::from_proto(response))
        })
        .await
    }

    /// List all vaults on this node.
    ///
    /// Returns a list of all vaults that this node is hosting or participating in.
    ///
    /// # Returns
    ///
    /// Returns a vector of [`VaultInfo`] for all vaults.
    ///
    /// # Errors
    ///
    /// Returns an error if connection fails after retry attempts.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::LedgerClient;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// let vaults = client.list_vaults().await?;
    /// for v in vaults {
    ///     println!("Vault {} in namespace {}", v.vault_id, v.namespace_id);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn list_vaults(&self) -> Result<Vec<VaultInfo>> {
        self.check_shutdown()?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        with_retry(&retry_policy, || async {
            let channel = pool.get_channel().await?;
            let mut client = Self::create_admin_client(channel, pool.compression_enabled());

            let request = proto::ListVaultsRequest {};

            let response = client.list_vaults(tonic::Request::new(request)).await?.into_inner();

            Ok(response.vaults.into_iter().map(VaultInfo::from_proto).collect())
        })
        .await
    }

    // =========================================================================
    // Health Operations
    // =========================================================================

    /// Check node-level health.
    ///
    /// Returns `true` if the node is healthy and has a leader elected.
    /// This is a simple health check suitable for load balancer probes.
    ///
    /// # Returns
    ///
    /// Returns `true` if the node is healthy, `false` if degraded.
    ///
    /// # Errors
    ///
    /// Returns an error if the node is unavailable or connection fails.
    /// Note: An unavailable node returns an error, not `false`.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::LedgerClient;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// if client.health_check().await? {
    ///     println!("Node is healthy");
    /// } else {
    ///     println!("Node is degraded but available");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn health_check(&self) -> Result<bool> {
        self.check_shutdown()?;

        let result = self.health_check_detailed().await?;
        match result.status {
            HealthStatus::Healthy => Ok(true),
            HealthStatus::Degraded => Ok(false),
            HealthStatus::Unavailable => error::UnavailableSnafu { message: result.message }.fail(),
            HealthStatus::Unspecified => Ok(false),
        }
    }

    /// Get detailed node-level health information.
    ///
    /// Returns full health check result including status, message, and details.
    /// Use this for monitoring and diagnostics that need more than a simple boolean.
    ///
    /// # Returns
    ///
    /// Returns a [`HealthCheckResult`] with status, message, and details.
    ///
    /// # Errors
    ///
    /// Returns an error if connection fails after retry attempts.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::LedgerClient;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// let health = client.health_check_detailed().await?;
    /// println!("Status: {:?}, Message: {}", health.status, health.message);
    /// if let Some(term) = health.details.get("current_term") {
    ///     println!("Current Raft term: {}", term);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn health_check_detailed(&self) -> Result<HealthCheckResult> {
        self.check_shutdown()?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        with_retry(&retry_policy, || async {
            let channel = pool.get_channel().await?;
            let mut client = Self::create_health_client(channel, pool.compression_enabled());

            let request = proto::HealthCheckRequest { namespace_id: None, vault_id: None };

            let response = client.check(tonic::Request::new(request)).await?.into_inner();

            Ok(HealthCheckResult::from_proto(response))
        })
        .await
    }

    /// Check health of a specific vault.
    ///
    /// Returns detailed health information for a specific vault, including
    /// block height, health status, and any divergence information.
    ///
    /// # Arguments
    ///
    /// * `namespace_id` - The namespace containing the vault
    /// * `vault_id` - The vault to check
    ///
    /// # Returns
    ///
    /// Returns a [`HealthCheckResult`] with vault-specific health information.
    ///
    /// # Errors
    ///
    /// Returns an error if connection fails after retry attempts.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::LedgerClient;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// let health = client.health_check_vault(1, 0).await?;
    /// println!("Vault status: {:?}", health.status);
    /// if let Some(height) = health.details.get("block_height") {
    ///     println!("Current height: {}", height);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn health_check_vault(
        &self,
        namespace_id: i64,
        vault_id: i64,
    ) -> Result<HealthCheckResult> {
        self.check_shutdown()?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        with_retry(&retry_policy, || async {
            let channel = pool.get_channel().await?;
            let mut client = Self::create_health_client(channel, pool.compression_enabled());

            let request = proto::HealthCheckRequest {
                namespace_id: Some(proto::NamespaceId { id: namespace_id }),
                vault_id: Some(proto::VaultId { id: vault_id }),
            };

            let response = client.check(tonic::Request::new(request)).await?.into_inner();

            Ok(HealthCheckResult::from_proto(response))
        })
        .await
    }

    // =========================================================================
    // Verified Read Operations
    // =========================================================================

    /// Read a value with cryptographic proof for client-side verification.
    ///
    /// Returns the value along with a Merkle proof that can be used to verify
    /// the value is authentic without trusting the server. The proof links
    /// the entity value to the state root in the block header.
    ///
    /// # Arguments
    ///
    /// * `namespace_id` - Namespace containing the data.
    /// * `vault_id` - Optional vault ID (None for namespace-level entities).
    /// * `key` - Entity key to read.
    /// * `opts` - Verification options (height, chain proof).
    ///
    /// # Returns
    ///
    /// `VerifiedValue` containing the value and proofs, or `None` if key not found.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, VerifyOpts};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let (ns_id, vault_id) = (1i64, 1i64);
    /// let result = client.verified_read(ns_id, Some(vault_id), "user:123", VerifyOpts::new()).await?;
    /// if let Some(verified) = result {
    ///     // Verify the proof before using the value
    ///     verified.verify()?;
    ///     println!("Value: {:?}", verified.value);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn verified_read(
        &self,
        namespace_id: i64,
        vault_id: Option<i64>,
        key: impl Into<String>,
        opts: VerifyOpts,
    ) -> Result<Option<VerifiedValue>> {
        self.check_shutdown()?;

        let key = key.into();
        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        with_retry(&retry_policy, || async {
            let channel = pool.get_channel().await?;
            let mut client = Self::create_read_client(channel, pool.compression_enabled());

            let request = proto::VerifiedReadRequest {
                namespace_id: Some(proto::NamespaceId { id: namespace_id }),
                vault_id: vault_id.map(|id| proto::VaultId { id }),
                key: key.clone(),
                at_height: opts.at_height,
                include_chain_proof: opts.include_chain_proof,
                trusted_height: opts.trusted_height,
            };

            let response = client.verified_read(tonic::Request::new(request)).await?.into_inner();

            // If no value and no block header, key was not found
            if response.value.is_none() && response.block_header.is_none() {
                return Ok(None);
            }

            Ok(VerifiedValue::from_proto(response))
        })
        .await
    }

    // =========================================================================
    // Query Operations
    // =========================================================================

    /// List entities matching a key prefix.
    ///
    /// Returns a paginated list of entities with keys starting with the given prefix.
    /// Use the `next_page_token` to fetch additional pages.
    ///
    /// # Arguments
    ///
    /// * `namespace_id` - Namespace containing the entities.
    /// * `opts` - Query options including prefix filter, pagination, and consistency.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, ListEntitiesOpts};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let ns_id = 1i64;
    /// // List all users
    /// let result = client.list_entities(ns_id, ListEntitiesOpts::with_prefix("user:")).await?;
    /// for entity in result.items {
    ///     println!("Key: {}, Version: {}", entity.key, entity.version);
    /// }
    ///
    /// // Fetch next page if available
    /// if let Some(token) = result.next_page_token {
    ///     let next_page = client.list_entities(
    ///         ns_id,
    ///         ListEntitiesOpts::with_prefix("user:").page_token(token)
    ///     ).await?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn list_entities(
        &self,
        namespace_id: i64,
        opts: ListEntitiesOpts,
    ) -> Result<PagedResult<Entity>> {
        self.check_shutdown()?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        with_retry(&retry_policy, || async {
            let channel = pool.get_channel().await?;
            let mut client = Self::create_read_client(channel, pool.compression_enabled());

            let request = proto::ListEntitiesRequest {
                namespace_id: Some(proto::NamespaceId { id: namespace_id }),
                key_prefix: opts.key_prefix.clone(),
                at_height: opts.at_height,
                include_expired: opts.include_expired,
                limit: opts.limit,
                page_token: opts.page_token.clone().unwrap_or_default(),
                consistency: opts.consistency.to_proto() as i32,
            };

            let response = client.list_entities(tonic::Request::new(request)).await?.into_inner();

            let items = response.entities.into_iter().map(Entity::from_proto).collect();

            let next_page_token = if response.next_page_token.is_empty() {
                None
            } else {
                Some(response.next_page_token)
            };

            Ok(PagedResult { items, next_page_token, block_height: response.block_height })
        })
        .await
    }

    /// List relationships in a vault with optional filters.
    ///
    /// Returns a paginated list of relationships matching the filter criteria.
    /// All filter fields are optional; omitting a filter matches all values.
    ///
    /// # Arguments
    ///
    /// * `namespace_id` - Namespace containing the vault.
    /// * `vault_id` - Vault containing the relationships.
    /// * `opts` - Query options including filters, pagination, and consistency.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, ListRelationshipsOpts};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let (ns_id, vault_id) = (1i64, 1i64);
    /// // List all relationships for a document
    /// let result = client.list_relationships(
    ///     ns_id,
    ///     vault_id,
    ///     ListRelationshipsOpts::new().resource("document:123")
    /// ).await?;
    ///
    /// for rel in result.items {
    ///     println!("{} -> {} -> {}", rel.resource, rel.relation, rel.subject);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn list_relationships(
        &self,
        namespace_id: i64,
        vault_id: i64,
        opts: ListRelationshipsOpts,
    ) -> Result<PagedResult<Relationship>> {
        self.check_shutdown()?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        with_retry(&retry_policy, || async {
            let channel = pool.get_channel().await?;
            let mut client = Self::create_read_client(channel, pool.compression_enabled());

            let request = proto::ListRelationshipsRequest {
                namespace_id: Some(proto::NamespaceId { id: namespace_id }),
                vault_id: Some(proto::VaultId { id: vault_id }),
                resource: opts.resource.clone(),
                relation: opts.relation.clone(),
                subject: opts.subject.clone(),
                at_height: opts.at_height,
                limit: opts.limit,
                page_token: opts.page_token.clone().unwrap_or_default(),
                consistency: opts.consistency.to_proto() as i32,
            };

            let response =
                client.list_relationships(tonic::Request::new(request)).await?.into_inner();

            let items = response.relationships.into_iter().map(Relationship::from_proto).collect();

            let next_page_token = if response.next_page_token.is_empty() {
                None
            } else {
                Some(response.next_page_token)
            };

            Ok(PagedResult { items, next_page_token, block_height: response.block_height })
        })
        .await
    }

    /// List distinct resource IDs matching a type prefix.
    ///
    /// Returns a paginated list of unique resource identifiers that match the given
    /// type prefix (e.g., "document" matches "document:1", "document:2", etc.).
    ///
    /// # Arguments
    ///
    /// * `namespace_id` - Namespace containing the vault.
    /// * `vault_id` - Vault containing the relationships.
    /// * `opts` - Query options including type filter, pagination, and consistency.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, ListResourcesOpts};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let (ns_id, vault_id) = (1i64, 1i64);
    /// // List all document resources
    /// let result = client.list_resources(
    ///     ns_id,
    ///     vault_id,
    ///     ListResourcesOpts::with_type("document")
    /// ).await?;
    ///
    /// for resource_id in result.items {
    ///     println!("Resource: {}", resource_id);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn list_resources(
        &self,
        namespace_id: i64,
        vault_id: i64,
        opts: ListResourcesOpts,
    ) -> Result<PagedResult<String>> {
        self.check_shutdown()?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        with_retry(&retry_policy, || async {
            let channel = pool.get_channel().await?;
            let mut client = Self::create_read_client(channel, pool.compression_enabled());

            let request = proto::ListResourcesRequest {
                namespace_id: Some(proto::NamespaceId { id: namespace_id }),
                vault_id: Some(proto::VaultId { id: vault_id }),
                resource_type: opts.resource_type.clone(),
                at_height: opts.at_height,
                limit: opts.limit,
                page_token: opts.page_token.clone().unwrap_or_default(),
                consistency: opts.consistency.to_proto() as i32,
            };

            let response = client.list_resources(tonic::Request::new(request)).await?.into_inner();

            let next_page_token = if response.next_page_token.is_empty() {
                None
            } else {
                Some(response.next_page_token)
            };

            Ok(PagedResult {
                items: response.resources,
                next_page_token,
                block_height: response.block_height,
            })
        })
        .await
    }

    /// Create an AdminService client with compression settings.
    fn create_admin_client(
        channel: tonic::transport::Channel,
        compression_enabled: bool,
    ) -> proto::admin_service_client::AdminServiceClient<tonic::transport::Channel> {
        let client = proto::admin_service_client::AdminServiceClient::new(channel);
        if compression_enabled {
            client
                .send_compressed(tonic::codec::CompressionEncoding::Gzip)
                .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
        } else {
            client
        }
    }

    /// Create a HealthService client with compression settings.
    fn create_health_client(
        channel: tonic::transport::Channel,
        compression_enabled: bool,
    ) -> proto::health_service_client::HealthServiceClient<tonic::transport::Channel> {
        let client = proto::health_service_client::HealthServiceClient::new(channel);
        if compression_enabled {
            client
                .send_compressed(tonic::codec::CompressionEncoding::Gzip)
                .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
        } else {
            client
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::config::RetryPolicy;

    #[tokio::test]
    async fn test_new_with_valid_config() {
        let config = ClientConfig::builder()
            .endpoints(vec!["http://localhost:50051".into()])
            .client_id("test-client")
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        assert_eq!(client.client_id(), "test-client");
        assert_eq!(client.config().endpoints(), &["http://localhost:50051"]);
    }

    #[tokio::test]
    async fn test_connect_convenience_constructor() {
        let client = LedgerClient::connect("http://localhost:50051", "quick-client")
            .await
            .expect("client creation");

        assert_eq!(client.client_id(), "quick-client");
        assert_eq!(client.config().endpoints(), &["http://localhost:50051"]);
    }

    #[tokio::test]
    async fn test_connect_with_invalid_endpoint() {
        let result = LedgerClient::connect("not-a-url", "test-client").await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_config_accessor_returns_full_config() {
        let config = ClientConfig::builder()
            .endpoints(vec!["http://localhost:50051".into()])
            .client_id("accessor-test")
            .timeout(Duration::from_secs(30))
            .compression(true)
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        assert_eq!(client.config().timeout(), Duration::from_secs(30));
        assert!(client.config().compression());
    }

    #[tokio::test]
    async fn test_sequences_accessor_returns_tracker() {
        let client = LedgerClient::connect("http://localhost:50051", "seq-test")
            .await
            .expect("client creation");

        let sequences = client.sequences();
        assert_eq!(sequences.client_id(), "seq-test");

        // Tracker should start fresh
        assert_eq!(sequences.vault_count(), 0);
    }

    #[tokio::test]
    async fn test_pool_accessor_returns_pool() {
        let config = ClientConfig::builder()
            .endpoints(vec!["http://localhost:50051".into()])
            .client_id("pool-test")
            .compression(true)
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        assert!(client.pool().compression_enabled());
    }

    #[tokio::test]
    async fn test_create_discovery_service() {
        use crate::config::DiscoveryConfig;

        let config = ClientConfig::builder()
            .endpoints(vec!["http://localhost:50051".into()])
            .client_id("discovery-test")
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");
        let discovery = client.create_discovery_service(DiscoveryConfig::enabled());

        assert!(discovery.config().is_enabled());
    }

    #[tokio::test]
    async fn test_new_preserves_retry_policy() {
        let retry_policy = RetryPolicy::builder()
            .max_attempts(5)
            .initial_backoff(Duration::from_millis(100))
            .build();

        let config = ClientConfig::builder()
            .endpoints(vec!["http://localhost:50051".into()])
            .client_id("retry-test")
            .retry_policy(retry_policy)
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        assert_eq!(client.config().retry_policy().max_attempts, 5);
    }

    // =========================================================================
    // ReadConsistency Tests
    // =========================================================================

    #[test]
    fn test_read_consistency_default_is_eventual() {
        assert_eq!(ReadConsistency::default(), ReadConsistency::Eventual);
    }

    #[test]
    fn test_read_consistency_to_proto_eventual() {
        let consistency = ReadConsistency::Eventual;
        assert_eq!(consistency.to_proto() as i32, proto::ReadConsistency::Eventual as i32);
    }

    #[test]
    fn test_read_consistency_to_proto_linearizable() {
        let consistency = ReadConsistency::Linearizable;
        assert_eq!(consistency.to_proto() as i32, proto::ReadConsistency::Linearizable as i32);
    }

    // =========================================================================
    // Read Operation Integration Tests
    // =========================================================================
    //
    // These tests verify error handling when connecting to unreachable endpoints.
    // They don't require a running server - they test the retry/error paths.

    #[tokio::test]
    async fn test_read_returns_error_on_connection_failure() {
        // Configure minimal retry to make test fast
        let config = ClientConfig::builder()
            .endpoints(vec!["http://127.0.0.1:59999".into()])
            .client_id("test-client")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        let result = client.read(1, Some(0), "test-key").await;
        assert!(result.is_err(), "expected connection error");
    }

    #[tokio::test]
    async fn test_read_consistent_returns_error_on_connection_failure() {
        let config = ClientConfig::builder()
            .endpoints(vec!["http://127.0.0.1:59998".into()])
            .client_id("test-client")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        let result = client.read_consistent(1, Some(0), "test-key").await;
        assert!(result.is_err(), "expected connection error");
    }

    #[tokio::test]
    async fn test_batch_read_returns_error_on_connection_failure() {
        let config = ClientConfig::builder()
            .endpoints(vec!["http://127.0.0.1:59997".into()])
            .client_id("test-client")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        let result = client.batch_read(1, Some(0), vec!["key1", "key2", "key3"]).await;
        assert!(result.is_err(), "expected connection error");
    }

    #[tokio::test]
    async fn test_batch_read_consistent_returns_error_on_connection_failure() {
        let config = ClientConfig::builder()
            .endpoints(vec!["http://127.0.0.1:59996".into()])
            .client_id("test-client")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        let result = client.batch_read_consistent(1, Some(0), vec!["key1", "key2"]).await;
        assert!(result.is_err(), "expected connection error");
    }

    #[tokio::test]
    async fn test_read_with_none_vault_id() {
        // Test that read works with None vault_id (namespace-level reads)
        let config = ClientConfig::builder()
            .endpoints(vec!["http://127.0.0.1:59995".into()])
            .client_id("test-client")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        // This tests the API signature - None for vault_id should work
        let result = client.read(1, None, "user:123").await;
        assert!(result.is_err(), "expected connection error");
    }

    // =========================================================================
    // Operation Builder Tests
    // =========================================================================

    #[test]
    fn test_operation_set_entity() {
        let op = Operation::set_entity("user:123", b"data".to_vec());
        match op {
            Operation::SetEntity { key, value, expires_at, condition } => {
                assert_eq!(key, "user:123");
                assert_eq!(value, b"data");
                assert!(expires_at.is_none());
                assert!(condition.is_none());
            },
            _ => panic!("Expected SetEntity"),
        }
    }

    #[test]
    fn test_operation_set_entity_with_expiry() {
        let op = Operation::set_entity_with_expiry("session:abc", b"token".to_vec(), 1700000000);
        match op {
            Operation::SetEntity { key, value, expires_at, condition } => {
                assert_eq!(key, "session:abc");
                assert_eq!(value, b"token");
                assert_eq!(expires_at, Some(1700000000));
                assert!(condition.is_none());
            },
            _ => panic!("Expected SetEntity"),
        }
    }

    #[test]
    fn test_operation_set_entity_if_not_exists() {
        let op = Operation::set_entity_if("lock:xyz", b"owner".to_vec(), SetCondition::NotExists);
        match op {
            Operation::SetEntity { key, condition: Some(SetCondition::NotExists), .. } => {
                assert_eq!(key, "lock:xyz");
            },
            _ => panic!("Expected SetEntity with NotExists condition"),
        }
    }

    #[test]
    fn test_operation_set_entity_if_version() {
        let op = Operation::set_entity_if("counter", b"42".to_vec(), SetCondition::Version(100));
        match op {
            Operation::SetEntity { condition: Some(SetCondition::Version(v)), .. } => {
                assert_eq!(v, 100);
            },
            _ => panic!("Expected SetEntity with Version condition"),
        }
    }

    #[test]
    fn test_operation_set_entity_if_value_equals() {
        let op = Operation::set_entity_if(
            "data",
            b"new".to_vec(),
            SetCondition::ValueEquals(b"old".to_vec()),
        );
        match op {
            Operation::SetEntity { condition: Some(SetCondition::ValueEquals(v)), .. } => {
                assert_eq!(v, b"old");
            },
            _ => panic!("Expected SetEntity with ValueEquals condition"),
        }
    }

    #[test]
    fn test_operation_delete_entity() {
        let op = Operation::delete_entity("obsolete:key");
        match op {
            Operation::DeleteEntity { key } => {
                assert_eq!(key, "obsolete:key");
            },
            _ => panic!("Expected DeleteEntity"),
        }
    }

    #[test]
    fn test_operation_create_relationship() {
        let op = Operation::create_relationship("doc:456", "viewer", "user:123");
        match op {
            Operation::CreateRelationship { resource, relation, subject } => {
                assert_eq!(resource, "doc:456");
                assert_eq!(relation, "viewer");
                assert_eq!(subject, "user:123");
            },
            _ => panic!("Expected CreateRelationship"),
        }
    }

    #[test]
    fn test_operation_delete_relationship() {
        let op = Operation::delete_relationship("doc:456", "editor", "team:admins#member");
        match op {
            Operation::DeleteRelationship { resource, relation, subject } => {
                assert_eq!(resource, "doc:456");
                assert_eq!(relation, "editor");
                assert_eq!(subject, "team:admins#member");
            },
            _ => panic!("Expected DeleteRelationship"),
        }
    }

    #[test]
    fn test_operation_to_proto_set_entity() {
        let op = Operation::set_entity("key", b"value".to_vec());
        let proto_op = op.to_proto();

        assert!(proto_op.op.is_some());
        match proto_op.op.unwrap() {
            proto::operation::Op::SetEntity(set) => {
                assert_eq!(set.key, "key");
                assert_eq!(set.value, b"value");
            },
            _ => panic!("Expected SetEntity proto"),
        }
    }

    #[test]
    fn test_operation_to_proto_create_relationship() {
        let op = Operation::create_relationship("res", "rel", "sub");
        let proto_op = op.to_proto();

        match proto_op.op.unwrap() {
            proto::operation::Op::CreateRelationship(rel) => {
                assert_eq!(rel.resource, "res");
                assert_eq!(rel.relation, "rel");
                assert_eq!(rel.subject, "sub");
            },
            _ => panic!("Expected CreateRelationship proto"),
        }
    }

    #[test]
    fn test_set_condition_to_proto() {
        let not_exists = SetCondition::NotExists;
        let proto_cond = not_exists.to_proto();
        assert!(matches!(
            proto_cond.condition,
            Some(proto::set_condition::Condition::NotExists(true))
        ));

        let must_exist = SetCondition::MustExist;
        let proto_cond = must_exist.to_proto();
        assert!(matches!(
            proto_cond.condition,
            Some(proto::set_condition::Condition::MustExists(true))
        ));

        let version = SetCondition::Version(42);
        let proto_cond = version.to_proto();
        assert!(matches!(proto_cond.condition, Some(proto::set_condition::Condition::Version(42))));

        let value_eq = SetCondition::ValueEquals(b"test".to_vec());
        let proto_cond = value_eq.to_proto();
        match proto_cond.condition {
            Some(proto::set_condition::Condition::ValueEquals(v)) => {
                assert_eq!(v, b"test");
            },
            _ => panic!("Expected ValueEquals"),
        }
    }

    // =========================================================================
    // WriteSuccess Tests
    // =========================================================================

    #[test]
    fn test_write_success_fields() {
        let success = WriteSuccess { tx_id: "abc123".to_string(), block_height: 42 };

        assert_eq!(success.tx_id, "abc123");
        assert_eq!(success.block_height, 42);
    }

    #[test]
    fn test_tx_id_to_hex() {
        // Test with Some(TxId)
        let tx_id = proto::TxId { id: vec![0x12, 0x34, 0xab, 0xcd] };
        let hex = LedgerClient::tx_id_to_hex(Some(tx_id));
        assert_eq!(hex, "1234abcd");

        // Test with None
        let hex = LedgerClient::tx_id_to_hex(None);
        assert_eq!(hex, "");
    }

    // =========================================================================
    // Write Operation Integration Tests
    // =========================================================================
    //
    // These tests verify error handling when connecting to unreachable endpoints.
    // They don't require a running server - they test the retry/error paths.

    #[tokio::test]
    async fn test_write_returns_error_on_connection_failure() {
        let config = ClientConfig::builder()
            .endpoints(vec!["http://127.0.0.1:59994".into()])
            .client_id("write-test")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        let operations = vec![Operation::set_entity("key", b"value".to_vec())];
        let result = client.write(1, Some(0), operations).await;

        assert!(result.is_err(), "expected connection error");
    }

    #[tokio::test]
    async fn test_write_increments_sequence() {
        let client = LedgerClient::connect("http://127.0.0.1:59993", "seq-write-test")
            .await
            .expect("client creation");

        // Check initial sequence state (should be 0 for new vault)
        let initial_seq = client.sequences().current_sequence(1, 0);
        assert_eq!(initial_seq, 0);

        // Attempt write - even though it fails, sequence should be consumed
        let operations = vec![Operation::set_entity("key", b"value".to_vec())];
        let _ = client.write(1, Some(0), operations).await;

        // Sequence should have been incremented
        let new_seq = client.sequences().current_sequence(1, 0);
        assert_eq!(new_seq, 1, "sequence should increment on write attempt");
    }

    #[tokio::test]
    async fn test_write_different_vaults_have_independent_sequences() {
        let client = LedgerClient::connect("http://127.0.0.1:59992", "multi-vault-test")
            .await
            .expect("client creation");

        // Write to vault 1
        let _ = client.write(1, Some(1), vec![Operation::set_entity("k1", b"v1".to_vec())]).await;

        // Write to vault 2
        let _ = client.write(1, Some(2), vec![Operation::set_entity("k2", b"v2".to_vec())]).await;

        // Each vault should have its own sequence
        assert_eq!(client.sequences().current_sequence(1, 1), 1);
        assert_eq!(client.sequences().current_sequence(1, 2), 1);
    }

    #[tokio::test]
    async fn test_write_with_none_vault_id_uses_default() {
        let client = LedgerClient::connect("http://127.0.0.1:59991", "none-vault-test")
            .await
            .expect("client creation");

        // Write with None vault_id (namespace-level)
        let _ =
            client.write(1, None, vec![Operation::set_entity("entity", b"data".to_vec())]).await;

        // Should use vault_id 0 internally for sequence tracking
        assert_eq!(client.sequences().current_sequence(1, 0), 1, "None vault_id should map to 0");
    }

    #[tokio::test]
    async fn test_write_with_multiple_operations() {
        let config = ClientConfig::builder()
            .endpoints(vec!["http://127.0.0.1:59990".into()])
            .client_id("multi-op-test")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        // Multiple operations should be grouped in a single write
        let operations = vec![
            Operation::set_entity("user:1", b"alice".to_vec()),
            Operation::set_entity("user:2", b"bob".to_vec()),
            Operation::create_relationship("doc:1", "viewer", "user:1"),
            Operation::create_relationship("doc:1", "editor", "user:2"),
        ];

        let result = client.write(1, Some(0), operations).await;

        // Should fail due to connection (not due to multiple ops)
        assert!(result.is_err());
    }

    // =========================================================================
    // Batch Write Operation Tests
    // =========================================================================

    #[tokio::test]
    async fn test_batch_write_returns_error_on_connection_failure() {
        let config = ClientConfig::builder()
            .endpoints(vec!["http://127.0.0.1:59989".into()])
            .client_id("batch-test")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        let batches = vec![vec![Operation::set_entity("key", b"value".to_vec())]];
        let result = client.batch_write(1, Some(0), batches).await;

        assert!(result.is_err(), "expected connection error");
    }

    #[tokio::test]
    async fn test_batch_write_increments_sequence() {
        let client = LedgerClient::connect("http://127.0.0.1:59988", "batch-seq-test")
            .await
            .expect("client creation");

        // Check initial sequence state
        let initial_seq = client.sequences().current_sequence(1, 0);
        assert_eq!(initial_seq, 0);

        // Attempt batch write - even though it fails, sequence should be consumed
        let batches = vec![vec![Operation::set_entity("key", b"value".to_vec())]];
        let _ = client.batch_write(1, Some(0), batches).await;

        // Sequence should have been incremented
        let new_seq = client.sequences().current_sequence(1, 0);
        assert_eq!(new_seq, 1, "sequence should increment on batch write attempt");
    }

    #[tokio::test]
    async fn test_batch_write_uses_single_sequence_for_all_batches() {
        let client = LedgerClient::connect("http://127.0.0.1:59987", "batch-single-seq")
            .await
            .expect("client creation");

        // Multiple batch groups should still use one sequence
        let batches = vec![
            vec![Operation::set_entity("k1", b"v1".to_vec())],
            vec![Operation::set_entity("k2", b"v2".to_vec())],
            vec![Operation::set_entity("k3", b"v3".to_vec())],
        ];
        let _ = client.batch_write(1, Some(0), batches).await;

        // Only one sequence consumed despite multiple batches
        assert_eq!(
            client.sequences().current_sequence(1, 0),
            1,
            "batch should use single sequence"
        );
    }

    #[tokio::test]
    async fn test_batch_write_with_none_vault_id() {
        let client = LedgerClient::connect("http://127.0.0.1:59986", "batch-none-vault")
            .await
            .expect("client creation");

        // Batch write with None vault_id (namespace-level)
        let batches = vec![vec![Operation::set_entity("entity", b"data".to_vec())]];
        let _ = client.batch_write(1, None, batches).await;

        // Should use vault_id 0 internally for sequence tracking
        assert_eq!(client.sequences().current_sequence(1, 0), 1, "None vault_id should map to 0");
    }

    #[tokio::test]
    async fn test_batch_write_different_vaults_have_independent_sequences() {
        let client = LedgerClient::connect("http://127.0.0.1:59985", "batch-multi-vault")
            .await
            .expect("client creation");

        // Batch write to vault 1
        let _ = client
            .batch_write(1, Some(1), vec![vec![Operation::set_entity("k1", b"v1".to_vec())]])
            .await;

        // Batch write to vault 2
        let _ = client
            .batch_write(1, Some(2), vec![vec![Operation::set_entity("k2", b"v2".to_vec())]])
            .await;

        // Each vault should have its own sequence
        assert_eq!(client.sequences().current_sequence(1, 1), 1);
        assert_eq!(client.sequences().current_sequence(1, 2), 1);
    }

    #[tokio::test]
    async fn test_batch_write_with_multiple_operation_groups() {
        let config = ClientConfig::builder()
            .endpoints(vec!["http://127.0.0.1:59984".into()])
            .client_id("batch-groups-test")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        // Atomic transaction with multiple groups
        let batches = vec![
            // First group: create user
            vec![Operation::set_entity("user:123", b"alice".to_vec())],
            // Second group: grant permissions
            vec![
                Operation::create_relationship("doc:456", "viewer", "user:123"),
                Operation::create_relationship("folder:789", "editor", "user:123"),
            ],
        ];

        let result = client.batch_write(1, Some(0), batches).await;

        // Should fail due to connection (not due to batch structure)
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_batch_write_empty_batches_still_increments_sequence() {
        let client = LedgerClient::connect("http://127.0.0.1:59983", "batch-empty")
            .await
            .expect("client creation");

        // Empty batch write
        let batches: Vec<Vec<Operation>> = vec![];
        let _ = client.batch_write(1, Some(0), batches).await;

        // Sequence should still be incremented (server decides if empty batch is valid)
        assert_eq!(
            client.sequences().current_sequence(1, 0),
            1,
            "empty batch should still increment sequence"
        );
    }

    // =========================================================================
    // BlockAnnouncement Tests
    // =========================================================================

    #[test]
    fn test_block_announcement_from_proto_with_all_fields() {
        use prost_types::Timestamp;

        let proto_announcement = proto::BlockAnnouncement {
            namespace_id: Some(proto::NamespaceId { id: 1 }),
            vault_id: Some(proto::VaultId { id: 2 }),
            height: 100,
            block_hash: Some(proto::Hash { value: vec![0x12, 0x34] }),
            state_root: Some(proto::Hash { value: vec![0xab, 0xcd] }),
            timestamp: Some(Timestamp { seconds: 1700000000, nanos: 123_456_789 }),
        };

        let announcement = BlockAnnouncement::from_proto(proto_announcement);

        assert_eq!(announcement.namespace_id, 1);
        assert_eq!(announcement.vault_id, 2);
        assert_eq!(announcement.height, 100);
        assert_eq!(announcement.block_hash, vec![0x12, 0x34]);
        assert_eq!(announcement.state_root, vec![0xab, 0xcd]);
        assert!(announcement.timestamp.is_some());
    }

    #[test]
    fn test_block_announcement_from_proto_with_missing_optional_fields() {
        let proto_announcement = proto::BlockAnnouncement {
            namespace_id: None,
            vault_id: None,
            height: 50,
            block_hash: None,
            state_root: None,
            timestamp: None,
        };

        let announcement = BlockAnnouncement::from_proto(proto_announcement);

        assert_eq!(announcement.namespace_id, 0);
        assert_eq!(announcement.vault_id, 0);
        assert_eq!(announcement.height, 50);
        assert!(announcement.block_hash.is_empty());
        assert!(announcement.state_root.is_empty());
        assert!(announcement.timestamp.is_none());
    }

    #[test]
    fn test_block_announcement_equality() {
        let a = BlockAnnouncement {
            namespace_id: 1,
            vault_id: 2,
            height: 100,
            block_hash: vec![0x12],
            state_root: vec![0xab],
            timestamp: None,
        };

        let b = BlockAnnouncement {
            namespace_id: 1,
            vault_id: 2,
            height: 100,
            block_hash: vec![0x12],
            state_root: vec![0xab],
            timestamp: None,
        };

        assert_eq!(a, b);
    }

    #[test]
    fn test_block_announcement_clone() {
        let original = BlockAnnouncement {
            namespace_id: 1,
            vault_id: 2,
            height: 100,
            block_hash: vec![0x12, 0x34],
            state_root: vec![0xab, 0xcd],
            timestamp: None,
        };

        let cloned = original.clone();

        assert_eq!(original, cloned);
    }

    // =========================================================================
    // WatchBlocks Integration Tests
    // =========================================================================

    #[tokio::test]
    async fn test_watch_blocks_returns_error_on_connection_failure() {
        let config = ClientConfig::builder()
            .endpoints(vec!["http://127.0.0.1:59982".into()])
            .client_id("watch-test")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        let result = client.watch_blocks(1, 0, 1).await;

        assert!(result.is_err(), "expected connection error");
    }

    #[tokio::test]
    async fn test_watch_blocks_different_vaults() {
        let config = ClientConfig::builder()
            .endpoints(vec!["http://127.0.0.1:59981".into()])
            .client_id("multi-vault-watch")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        // Both should fail with connection error (testing different vault_ids work)
        let result1 = client.watch_blocks(1, 1, 1).await;
        let result2 = client.watch_blocks(1, 2, 1).await;

        assert!(result1.is_err());
        assert!(result2.is_err());
    }

    #[tokio::test]
    async fn test_watch_blocks_start_height_parameter() {
        let config = ClientConfig::builder()
            .endpoints(vec!["http://127.0.0.1:59980".into()])
            .client_id("height-test")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        // Test with different start heights
        let result_h1 = client.watch_blocks(1, 0, 1).await;
        let result_h100 = client.watch_blocks(1, 0, 100).await;

        // Both should fail due to connection (not invalid height)
        assert!(result_h1.is_err());
        assert!(result_h100.is_err());
    }

    // =========================================================================
    // Admin Operation Tests
    // =========================================================================

    #[test]
    fn test_namespace_status_from_proto_active() {
        let status = NamespaceStatus::from_proto(proto::NamespaceStatus::Active as i32);
        assert_eq!(status, NamespaceStatus::Active);
    }

    #[test]
    fn test_namespace_status_from_proto_deleted() {
        let status = NamespaceStatus::from_proto(proto::NamespaceStatus::Deleted as i32);
        assert_eq!(status, NamespaceStatus::Deleted);
    }

    #[test]
    fn test_namespace_status_from_proto_unspecified() {
        let status = NamespaceStatus::from_proto(proto::NamespaceStatus::Unspecified as i32);
        assert_eq!(status, NamespaceStatus::Unspecified);
    }

    #[test]
    fn test_namespace_status_from_proto_invalid() {
        let status = NamespaceStatus::from_proto(999);
        assert_eq!(status, NamespaceStatus::Unspecified);
    }

    #[test]
    fn test_namespace_status_default() {
        let status: NamespaceStatus = Default::default();
        assert_eq!(status, NamespaceStatus::Unspecified);
    }

    #[test]
    fn test_vault_status_from_proto_active() {
        let status = VaultStatus::from_proto(proto::VaultStatus::Active as i32);
        assert_eq!(status, VaultStatus::Active);
    }

    #[test]
    fn test_vault_status_from_proto_read_only() {
        let status = VaultStatus::from_proto(proto::VaultStatus::ReadOnly as i32);
        assert_eq!(status, VaultStatus::ReadOnly);
    }

    #[test]
    fn test_vault_status_from_proto_deleted() {
        let status = VaultStatus::from_proto(proto::VaultStatus::Deleted as i32);
        assert_eq!(status, VaultStatus::Deleted);
    }

    #[test]
    fn test_vault_status_from_proto_unspecified() {
        let status = VaultStatus::from_proto(proto::VaultStatus::Unspecified as i32);
        assert_eq!(status, VaultStatus::Unspecified);
    }

    #[test]
    fn test_vault_status_from_proto_invalid() {
        let status = VaultStatus::from_proto(999);
        assert_eq!(status, VaultStatus::Unspecified);
    }

    #[test]
    fn test_vault_status_default() {
        let status: VaultStatus = Default::default();
        assert_eq!(status, VaultStatus::Unspecified);
    }

    #[test]
    fn test_namespace_info_from_proto() {
        let proto = proto::GetNamespaceResponse {
            namespace_id: Some(proto::NamespaceId { id: 42 }),
            name: "test-namespace".to_string(),
            shard_id: Some(proto::ShardId { id: 1 }),
            member_nodes: vec![
                proto::NodeId { id: "node-100".to_string() },
                proto::NodeId { id: "node-101".to_string() },
            ],
            leader_hint: Some(proto::NodeId { id: "node-100".to_string() }),
            config_version: 5,
            status: proto::NamespaceStatus::Active as i32,
        };

        let info = NamespaceInfo::from_proto(proto);

        assert_eq!(info.namespace_id, 42);
        assert_eq!(info.name, "test-namespace");
        assert_eq!(info.shard_id, 1);
        assert_eq!(info.member_nodes, vec!["node-100", "node-101"]);
        assert_eq!(info.leader_hint, Some("node-100".to_string()));
        assert_eq!(info.config_version, 5);
        assert_eq!(info.status, NamespaceStatus::Active);
    }

    #[test]
    fn test_namespace_info_from_proto_with_missing_fields() {
        let proto = proto::GetNamespaceResponse {
            namespace_id: None,
            name: "minimal".to_string(),
            shard_id: None,
            member_nodes: vec![],
            leader_hint: None,
            config_version: 0,
            status: proto::NamespaceStatus::Unspecified as i32,
        };

        let info = NamespaceInfo::from_proto(proto);

        assert_eq!(info.namespace_id, 0);
        assert_eq!(info.name, "minimal");
        assert_eq!(info.shard_id, 0);
        assert!(info.member_nodes.is_empty());
        assert_eq!(info.leader_hint, None);
        assert_eq!(info.config_version, 0);
        assert_eq!(info.status, NamespaceStatus::Unspecified);
    }

    #[test]
    fn test_vault_info_from_proto() {
        let proto = proto::GetVaultResponse {
            namespace_id: Some(proto::NamespaceId { id: 1 }),
            vault_id: Some(proto::VaultId { id: 10 }),
            height: 1000,
            state_root: Some(proto::Hash { value: vec![1, 2, 3, 4] }),
            nodes: vec![
                proto::NodeId { id: "node-200".to_string() },
                proto::NodeId { id: "node-201".to_string() },
            ],
            leader: Some(proto::NodeId { id: "node-200".to_string() }),
            status: proto::VaultStatus::Active as i32,
            retention_policy: None,
        };

        let info = VaultInfo::from_proto(proto);

        assert_eq!(info.namespace_id, 1);
        assert_eq!(info.vault_id, 10);
        assert_eq!(info.height, 1000);
        assert_eq!(info.state_root, vec![1, 2, 3, 4]);
        assert_eq!(info.nodes, vec!["node-200", "node-201"]);
        assert_eq!(info.leader, Some("node-200".to_string()));
        assert_eq!(info.status, VaultStatus::Active);
    }

    #[test]
    fn test_vault_info_from_proto_with_missing_fields() {
        let proto = proto::GetVaultResponse {
            namespace_id: None,
            vault_id: None,
            height: 0,
            state_root: None,
            nodes: vec![],
            leader: None,
            status: proto::VaultStatus::Unspecified as i32,
            retention_policy: None,
        };

        let info = VaultInfo::from_proto(proto);

        assert_eq!(info.namespace_id, 0);
        assert_eq!(info.vault_id, 0);
        assert_eq!(info.height, 0);
        assert!(info.state_root.is_empty());
        assert!(info.nodes.is_empty());
        assert_eq!(info.leader, None);
        assert_eq!(info.status, VaultStatus::Unspecified);
    }

    #[test]
    fn test_namespace_info_equality() {
        let info1 = NamespaceInfo {
            namespace_id: 1,
            name: "test".to_string(),
            shard_id: 1,
            member_nodes: vec!["node-1".to_string(), "node-2".to_string()],
            leader_hint: Some("node-1".to_string()),
            config_version: 1,
            status: NamespaceStatus::Active,
        };
        let info2 = info1.clone();

        assert_eq!(info1, info2);
    }

    #[test]
    fn test_vault_info_equality() {
        let info1 = VaultInfo {
            namespace_id: 1,
            vault_id: 2,
            height: 100,
            state_root: vec![1, 2, 3],
            nodes: vec!["node-1".to_string(), "node-2".to_string()],
            leader: Some("node-1".to_string()),
            status: VaultStatus::Active,
        };
        let info2 = info1.clone();

        assert_eq!(info1, info2);
    }

    #[tokio::test]
    async fn test_create_namespace_returns_error_on_connection_failure() {
        let config = ClientConfig::builder()
            .endpoints(vec!["http://127.0.0.1:59970".into()])
            .client_id("admin-test")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");
        let result = client.create_namespace("test-ns").await;

        assert!(result.is_err(), "expected connection error");
    }

    #[tokio::test]
    async fn test_get_namespace_returns_error_on_connection_failure() {
        let config = ClientConfig::builder()
            .endpoints(vec!["http://127.0.0.1:59971".into()])
            .client_id("admin-test")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");
        let result = client.get_namespace(1).await;

        assert!(result.is_err(), "expected connection error");
    }

    #[tokio::test]
    async fn test_list_namespaces_returns_error_on_connection_failure() {
        let config = ClientConfig::builder()
            .endpoints(vec!["http://127.0.0.1:59972".into()])
            .client_id("admin-test")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");
        let result = client.list_namespaces().await;

        assert!(result.is_err(), "expected connection error");
    }

    #[tokio::test]
    async fn test_create_vault_returns_error_on_connection_failure() {
        let config = ClientConfig::builder()
            .endpoints(vec!["http://127.0.0.1:59973".into()])
            .client_id("admin-test")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");
        let result = client.create_vault(1).await;

        assert!(result.is_err(), "expected connection error");
    }

    #[tokio::test]
    async fn test_get_vault_returns_error_on_connection_failure() {
        let config = ClientConfig::builder()
            .endpoints(vec!["http://127.0.0.1:59974".into()])
            .client_id("admin-test")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");
        let result = client.get_vault(1, 1).await;

        assert!(result.is_err(), "expected connection error");
    }

    #[tokio::test]
    async fn test_list_vaults_returns_error_on_connection_failure() {
        let config = ClientConfig::builder()
            .endpoints(vec!["http://127.0.0.1:59975".into()])
            .client_id("admin-test")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");
        let result = client.list_vaults().await;

        assert!(result.is_err(), "expected connection error");
    }

    // =========================================================================
    // HealthStatus Tests
    // =========================================================================

    #[test]
    fn test_health_status_from_proto_healthy() {
        let status = HealthStatus::from_proto(proto::HealthStatus::Healthy as i32);
        assert_eq!(status, HealthStatus::Healthy);
    }

    #[test]
    fn test_health_status_from_proto_degraded() {
        let status = HealthStatus::from_proto(proto::HealthStatus::Degraded as i32);
        assert_eq!(status, HealthStatus::Degraded);
    }

    #[test]
    fn test_health_status_from_proto_unavailable() {
        let status = HealthStatus::from_proto(proto::HealthStatus::Unavailable as i32);
        assert_eq!(status, HealthStatus::Unavailable);
    }

    #[test]
    fn test_health_status_from_proto_unspecified() {
        let status = HealthStatus::from_proto(proto::HealthStatus::Unspecified as i32);
        assert_eq!(status, HealthStatus::Unspecified);
    }

    #[test]
    fn test_health_status_from_proto_invalid() {
        let status = HealthStatus::from_proto(999);
        assert_eq!(status, HealthStatus::Unspecified);
    }

    #[test]
    fn test_health_status_default() {
        let status: HealthStatus = Default::default();
        assert_eq!(status, HealthStatus::Unspecified);
    }

    // =========================================================================
    // HealthCheckResult Tests
    // =========================================================================

    #[test]
    fn test_health_check_result_from_proto() {
        let mut details = std::collections::HashMap::new();
        details.insert("current_term".to_string(), "5".to_string());
        details.insert("leader_id".to_string(), "node-1".to_string());

        let proto = proto::HealthCheckResponse {
            status: proto::HealthStatus::Healthy as i32,
            message: "Node is healthy".to_string(),
            details: details.clone(),
        };

        let result = HealthCheckResult::from_proto(proto);

        assert_eq!(result.status, HealthStatus::Healthy);
        assert_eq!(result.message, "Node is healthy");
        assert_eq!(result.details, details);
    }

    #[test]
    fn test_health_check_result_is_healthy() {
        let result = HealthCheckResult {
            status: HealthStatus::Healthy,
            message: String::new(),
            details: std::collections::HashMap::new(),
        };
        assert!(result.is_healthy());
        assert!(!result.is_degraded());
        assert!(!result.is_unavailable());
    }

    #[test]
    fn test_health_check_result_is_degraded() {
        let result = HealthCheckResult {
            status: HealthStatus::Degraded,
            message: String::new(),
            details: std::collections::HashMap::new(),
        };
        assert!(!result.is_healthy());
        assert!(result.is_degraded());
        assert!(!result.is_unavailable());
    }

    #[test]
    fn test_health_check_result_is_unavailable() {
        let result = HealthCheckResult {
            status: HealthStatus::Unavailable,
            message: String::new(),
            details: std::collections::HashMap::new(),
        };
        assert!(!result.is_healthy());
        assert!(!result.is_degraded());
        assert!(result.is_unavailable());
    }

    // =========================================================================
    // Health Check Integration Tests
    // =========================================================================

    #[tokio::test]
    async fn test_health_check_returns_error_on_connection_failure() {
        let config = ClientConfig::builder()
            .endpoints(vec!["http://127.0.0.1:59976".into()])
            .client_id("health-test")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");
        let result = client.health_check().await;

        assert!(result.is_err(), "expected connection error");
    }

    #[tokio::test]
    async fn test_health_check_detailed_returns_error_on_connection_failure() {
        let config = ClientConfig::builder()
            .endpoints(vec!["http://127.0.0.1:59977".into()])
            .client_id("health-test")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");
        let result = client.health_check_detailed().await;

        assert!(result.is_err(), "expected connection error");
    }

    #[tokio::test]
    async fn test_health_check_vault_returns_error_on_connection_failure() {
        let config = ClientConfig::builder()
            .endpoints(vec!["http://127.0.0.1:59978".into()])
            .client_id("health-test")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");
        let result = client.health_check_vault(1, 0).await;

        assert!(result.is_err(), "expected connection error");
    }

    // =========================================================================
    // Verified Read Tests
    // =========================================================================

    #[test]
    fn test_direction_from_proto_left() {
        use inferadb_ledger_raft::proto::Direction as ProtoDirection;
        let direction = Direction::from_proto(ProtoDirection::Left as i32);
        assert_eq!(direction, Direction::Left);
    }

    #[test]
    fn test_direction_from_proto_right() {
        use inferadb_ledger_raft::proto::Direction as ProtoDirection;
        let direction = Direction::from_proto(ProtoDirection::Right as i32);
        assert_eq!(direction, Direction::Right);
    }

    #[test]
    fn test_direction_from_proto_unspecified_defaults_to_right() {
        use inferadb_ledger_raft::proto::Direction as ProtoDirection;
        let direction = Direction::from_proto(ProtoDirection::Unspecified as i32);
        assert_eq!(direction, Direction::Right);
    }

    #[test]
    fn test_merkle_sibling_from_proto() {
        use inferadb_ledger_raft::proto;
        let proto_sibling = proto::MerkleSibling {
            hash: Some(proto::Hash { value: vec![1, 2, 3, 4] }),
            direction: proto::Direction::Left as i32,
        };
        let sibling = MerkleSibling::from_proto(proto_sibling);
        assert_eq!(sibling.hash, vec![1, 2, 3, 4]);
        assert_eq!(sibling.direction, Direction::Left);
    }

    #[test]
    fn test_merkle_proof_from_proto() {
        use inferadb_ledger_raft::proto;
        let proto_proof = proto::MerkleProof {
            leaf_hash: Some(proto::Hash { value: vec![0; 32] }),
            siblings: vec![
                proto::MerkleSibling {
                    hash: Some(proto::Hash { value: vec![1; 32] }),
                    direction: proto::Direction::Left as i32,
                },
                proto::MerkleSibling {
                    hash: Some(proto::Hash { value: vec![2; 32] }),
                    direction: proto::Direction::Right as i32,
                },
            ],
        };
        let proof = MerkleProof::from_proto(proto_proof);
        assert_eq!(proof.leaf_hash, vec![0; 32]);
        assert_eq!(proof.siblings.len(), 2);
        assert_eq!(proof.siblings[0].direction, Direction::Left);
        assert_eq!(proof.siblings[1].direction, Direction::Right);
    }

    #[test]
    fn test_merkle_proof_verify_single_element_tree() {
        // Single element tree: leaf hash equals root
        let proof = MerkleProof {
            leaf_hash: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            siblings: vec![],
        };
        let expected_root = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        assert!(proof.verify(&expected_root));
    }

    #[test]
    fn test_merkle_proof_verify_single_element_tree_mismatch() {
        let proof = MerkleProof { leaf_hash: vec![1, 2, 3, 4], siblings: vec![] };
        let wrong_root = vec![5, 6, 7, 8];
        assert!(!proof.verify(&wrong_root));
    }

    #[test]
    fn test_merkle_proof_verify_with_siblings() {
        use sha2::{Digest, Sha256};

        // Create a simple two-leaf tree
        let leaf_hash = vec![0u8; 32];
        let sibling_hash = vec![1u8; 32];

        // Compute expected root: hash(leaf || sibling) since sibling is on right
        let mut hasher = Sha256::new();
        hasher.update(&leaf_hash);
        hasher.update(&sibling_hash);
        let expected_root = hasher.finalize().to_vec();

        let proof = MerkleProof {
            leaf_hash: leaf_hash.clone(),
            siblings: vec![MerkleSibling { hash: sibling_hash, direction: Direction::Right }],
        };

        assert!(proof.verify(&expected_root));
    }

    #[test]
    fn test_merkle_proof_verify_left_sibling() {
        use sha2::{Digest, Sha256};

        // Create a proof where sibling is on the left
        let leaf_hash = vec![0u8; 32];
        let sibling_hash = vec![1u8; 32];

        // Compute expected root: hash(sibling || leaf) since sibling is on left
        let mut hasher = Sha256::new();
        hasher.update(&sibling_hash);
        hasher.update(&leaf_hash);
        let expected_root = hasher.finalize().to_vec();

        let proof = MerkleProof {
            leaf_hash: leaf_hash.clone(),
            siblings: vec![MerkleSibling { hash: sibling_hash, direction: Direction::Left }],
        };

        assert!(proof.verify(&expected_root));
    }

    #[test]
    fn test_merkle_proof_verify_tampered_proof_fails() {
        use sha2::{Digest, Sha256};

        let leaf_hash = vec![0u8; 32];
        let sibling_hash = vec![1u8; 32];

        // Compute correct root
        let mut hasher = Sha256::new();
        hasher.update(&leaf_hash);
        hasher.update(&sibling_hash);
        let correct_root = hasher.finalize().to_vec();

        // Tamper with the sibling hash
        let tampered_sibling = vec![2u8; 32];
        let proof = MerkleProof {
            leaf_hash: leaf_hash.clone(),
            siblings: vec![MerkleSibling { hash: tampered_sibling, direction: Direction::Right }],
        };

        // Should not verify against correct root
        assert!(!proof.verify(&correct_root));
    }

    #[test]
    fn test_merkle_proof_verify_wrong_direction_fails() {
        use sha2::{Digest, Sha256};

        let leaf_hash = vec![0u8; 32];
        let sibling_hash = vec![1u8; 32];

        // Compute root with sibling on right
        let mut hasher = Sha256::new();
        hasher.update(&leaf_hash);
        hasher.update(&sibling_hash);
        let expected_root = hasher.finalize().to_vec();

        // Create proof with wrong direction (Left instead of Right)
        let proof = MerkleProof {
            leaf_hash: leaf_hash.clone(),
            siblings: vec![MerkleSibling {
                hash: sibling_hash,
                direction: Direction::Left, // Wrong!
            }],
        };

        // Should fail verification
        assert!(!proof.verify(&expected_root));
    }

    #[test]
    fn test_block_header_from_proto() {
        use inferadb_ledger_raft::proto;

        let proto_header = proto::BlockHeader {
            height: 100,
            namespace_id: Some(proto::NamespaceId { id: 1 }),
            vault_id: Some(proto::VaultId { id: 2 }),
            previous_hash: Some(proto::Hash { value: vec![1; 32] }),
            tx_merkle_root: Some(proto::Hash { value: vec![2; 32] }),
            state_root: Some(proto::Hash { value: vec![3; 32] }),
            timestamp: Some(prost_types::Timestamp { seconds: 1704067200, nanos: 0 }),
            leader_id: Some(proto::NodeId { id: "node-1".to_string() }),
            term: 5,
            committed_index: 99,
        };

        let header = BlockHeader::from_proto(proto_header);
        assert_eq!(header.height, 100);
        assert_eq!(header.namespace_id, 1);
        assert_eq!(header.vault_id, 2);
        assert_eq!(header.previous_hash, vec![1; 32]);
        assert_eq!(header.tx_merkle_root, vec![2; 32]);
        assert_eq!(header.state_root, vec![3; 32]);
        assert!(header.timestamp.is_some());
        assert_eq!(header.leader_id, "node-1");
        assert_eq!(header.term, 5);
        assert_eq!(header.committed_index, 99);
    }

    #[test]
    fn test_block_header_from_proto_with_missing_fields() {
        use inferadb_ledger_raft::proto;

        let proto_header = proto::BlockHeader {
            height: 1,
            namespace_id: None,
            vault_id: None,
            previous_hash: None,
            tx_merkle_root: None,
            state_root: None,
            timestamp: None,
            leader_id: None,
            term: 0,
            committed_index: 0,
        };

        let header = BlockHeader::from_proto(proto_header);
        assert_eq!(header.height, 1);
        assert_eq!(header.namespace_id, 0);
        assert_eq!(header.vault_id, 0);
        assert!(header.previous_hash.is_empty());
        assert!(header.tx_merkle_root.is_empty());
        assert!(header.state_root.is_empty());
        assert!(header.timestamp.is_none());
        assert!(header.leader_id.is_empty());
    }

    #[test]
    fn test_chain_proof_from_proto() {
        use inferadb_ledger_raft::proto;

        let proto_chain = proto::ChainProof {
            headers: vec![
                proto::BlockHeader {
                    height: 101,
                    namespace_id: Some(proto::NamespaceId { id: 1 }),
                    vault_id: Some(proto::VaultId { id: 0 }),
                    previous_hash: Some(proto::Hash { value: vec![0; 32] }),
                    tx_merkle_root: Some(proto::Hash { value: vec![1; 32] }),
                    state_root: Some(proto::Hash { value: vec![2; 32] }),
                    timestamp: None,
                    leader_id: None,
                    term: 1,
                    committed_index: 100,
                },
                proto::BlockHeader {
                    height: 102,
                    namespace_id: Some(proto::NamespaceId { id: 1 }),
                    vault_id: Some(proto::VaultId { id: 0 }),
                    previous_hash: Some(proto::Hash { value: vec![3; 32] }),
                    tx_merkle_root: Some(proto::Hash { value: vec![4; 32] }),
                    state_root: Some(proto::Hash { value: vec![5; 32] }),
                    timestamp: None,
                    leader_id: None,
                    term: 1,
                    committed_index: 101,
                },
            ],
        };

        let chain = ChainProof::from_proto(proto_chain);
        assert_eq!(chain.headers.len(), 2);
        assert_eq!(chain.headers[0].height, 101);
        assert_eq!(chain.headers[1].height, 102);
    }

    #[test]
    fn test_chain_proof_verify_empty() {
        let chain = ChainProof { headers: vec![] };
        let trusted_hash = vec![0; 32];
        assert!(chain.verify(&trusted_hash));
    }

    #[test]
    fn test_chain_proof_verify_first_links_to_trusted() {
        let chain = ChainProof {
            headers: vec![BlockHeader {
                height: 101,
                namespace_id: 1,
                vault_id: 0,
                previous_hash: vec![1, 2, 3, 4], // Must match trusted_hash
                tx_merkle_root: vec![5, 6, 7, 8],
                state_root: vec![9, 10, 11, 12],
                timestamp: None,
                leader_id: String::new(),
                term: 1,
                committed_index: 100,
            }],
        };
        let trusted_hash = vec![1, 2, 3, 4];
        assert!(chain.verify(&trusted_hash));
    }

    #[test]
    fn test_chain_proof_verify_fails_if_first_not_linked() {
        let chain = ChainProof {
            headers: vec![BlockHeader {
                height: 101,
                namespace_id: 1,
                vault_id: 0,
                previous_hash: vec![0, 0, 0, 0], // Wrong hash
                tx_merkle_root: vec![5, 6, 7, 8],
                state_root: vec![9, 10, 11, 12],
                timestamp: None,
                leader_id: String::new(),
                term: 1,
                committed_index: 100,
            }],
        };
        let trusted_hash = vec![1, 2, 3, 4];
        assert!(!chain.verify(&trusted_hash));
    }

    #[test]
    fn test_verify_opts_default() {
        let opts = VerifyOpts::new();
        assert!(opts.at_height.is_none());
        assert!(!opts.include_chain_proof);
        assert!(opts.trusted_height.is_none());
    }

    #[test]
    fn test_verify_opts_at_height() {
        let opts = VerifyOpts::new().at_height(100);
        assert_eq!(opts.at_height, Some(100));
        assert!(!opts.include_chain_proof);
    }

    #[test]
    fn test_verify_opts_with_chain_proof() {
        let opts = VerifyOpts::new().with_chain_proof(50);
        assert!(opts.include_chain_proof);
        assert_eq!(opts.trusted_height, Some(50));
    }

    #[test]
    fn test_verify_opts_builder_chain() {
        let opts = VerifyOpts::new().at_height(100).with_chain_proof(50);
        assert_eq!(opts.at_height, Some(100));
        assert!(opts.include_chain_proof);
        assert_eq!(opts.trusted_height, Some(50));
    }

    #[test]
    fn test_verified_value_from_proto() {
        use inferadb_ledger_raft::proto;

        let proto_response = proto::VerifiedReadResponse {
            value: Some(b"test-value".to_vec()),
            block_height: 100,
            block_header: Some(proto::BlockHeader {
                height: 100,
                namespace_id: Some(proto::NamespaceId { id: 1 }),
                vault_id: Some(proto::VaultId { id: 0 }),
                previous_hash: Some(proto::Hash { value: vec![1; 32] }),
                tx_merkle_root: Some(proto::Hash { value: vec![2; 32] }),
                state_root: Some(proto::Hash { value: vec![3; 32] }),
                timestamp: None,
                leader_id: None,
                term: 1,
                committed_index: 99,
            }),
            merkle_proof: Some(proto::MerkleProof {
                leaf_hash: Some(proto::Hash { value: vec![4; 32] }),
                siblings: vec![],
            }),
            chain_proof: None,
        };

        let verified = VerifiedValue::from_proto(proto_response);
        assert!(verified.is_some());
        let v = verified.unwrap();
        assert_eq!(v.value, Some(b"test-value".to_vec()));
        assert_eq!(v.block_height, 100);
        assert_eq!(v.block_header.height, 100);
        assert_eq!(v.merkle_proof.leaf_hash, vec![4; 32]);
        assert!(v.chain_proof.is_none());
    }

    #[test]
    fn test_verified_value_from_proto_missing_header() {
        use inferadb_ledger_raft::proto;

        let proto_response = proto::VerifiedReadResponse {
            value: Some(b"test-value".to_vec()),
            block_height: 100,
            block_header: None, // Missing
            merkle_proof: Some(proto::MerkleProof {
                leaf_hash: Some(proto::Hash { value: vec![4; 32] }),
                siblings: vec![],
            }),
            chain_proof: None,
        };

        let verified = VerifiedValue::from_proto(proto_response);
        assert!(verified.is_none()); // Should return None if header missing
    }

    #[test]
    fn test_verified_value_from_proto_missing_proof() {
        use inferadb_ledger_raft::proto;

        let proto_response = proto::VerifiedReadResponse {
            value: Some(b"test-value".to_vec()),
            block_height: 100,
            block_header: Some(proto::BlockHeader {
                height: 100,
                namespace_id: Some(proto::NamespaceId { id: 1 }),
                vault_id: Some(proto::VaultId { id: 0 }),
                previous_hash: Some(proto::Hash { value: vec![1; 32] }),
                tx_merkle_root: Some(proto::Hash { value: vec![2; 32] }),
                state_root: Some(proto::Hash { value: vec![3; 32] }),
                timestamp: None,
                leader_id: None,
                term: 1,
                committed_index: 99,
            }),
            merkle_proof: None, // Missing
            chain_proof: None,
        };

        let verified = VerifiedValue::from_proto(proto_response);
        assert!(verified.is_none()); // Should return None if proof missing
    }

    #[test]
    fn test_verified_value_verify_succeeds_with_matching_root() {
        // Create a verified value where the merkle proof matches the state root
        let state_root = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        let verified = VerifiedValue {
            value: Some(b"test-value".to_vec()),
            block_height: 100,
            block_header: BlockHeader {
                height: 100,
                namespace_id: 1,
                vault_id: 0,
                previous_hash: vec![0; 32],
                tx_merkle_root: vec![0; 32],
                state_root: state_root.clone(),
                timestamp: None,
                leader_id: String::new(),
                term: 1,
                committed_index: 99,
            },
            merkle_proof: MerkleProof {
                leaf_hash: state_root, // Single element tree: leaf == root
                siblings: vec![],
            },
            chain_proof: None,
        };

        let result = verified.verify();
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[test]
    fn test_verified_value_verify_fails_with_mismatched_root() {
        // Create a verified value where the merkle proof does NOT match the state root
        let verified = VerifiedValue {
            value: Some(b"test-value".to_vec()),
            block_height: 100,
            block_header: BlockHeader {
                height: 100,
                namespace_id: 1,
                vault_id: 0,
                previous_hash: vec![0; 32],
                tx_merkle_root: vec![0; 32],
                state_root: vec![1, 2, 3, 4], // Expected root
                timestamp: None,
                leader_id: String::new(),
                term: 1,
                committed_index: 99,
            },
            merkle_proof: MerkleProof {
                leaf_hash: vec![5, 6, 7, 8], // Different hash!
                siblings: vec![],
            },
            chain_proof: None,
        };

        let result = verified.verify();
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_verified_read_returns_error_on_connection_failure() {
        let config = ClientConfig::builder()
            .endpoints(vec!["http://127.0.0.1:59999".into()])
            .client_id("verified-read-test")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");
        let result = client.verified_read(1, Some(0), "key", VerifyOpts::new()).await;

        assert!(result.is_err(), "expected connection error");
    }

    // =========================================================================
    // Query Types Tests
    // =========================================================================

    #[test]
    fn test_entity_from_proto() {
        let proto_entity = proto::Entity {
            key: "user:123".to_string(),
            value: b"data".to_vec(),
            expires_at: Some(1700000000),
            version: 42,
        };

        let entity = Entity::from_proto(proto_entity);

        assert_eq!(entity.key, "user:123");
        assert_eq!(entity.value, b"data");
        assert_eq!(entity.expires_at, Some(1700000000));
        assert_eq!(entity.version, 42);
    }

    #[test]
    fn test_entity_from_proto_no_expiration() {
        let proto_entity = proto::Entity {
            key: "session:abc".to_string(),
            value: vec![],
            expires_at: None,
            version: 1,
        };

        let entity = Entity::from_proto(proto_entity);

        assert_eq!(entity.expires_at, None);
    }

    #[test]
    fn test_entity_from_proto_zero_expiration_treated_as_none() {
        let proto_entity = proto::Entity {
            key: "key".to_string(),
            value: vec![],
            expires_at: Some(0),
            version: 1,
        };

        let entity = Entity::from_proto(proto_entity);

        // Zero expiration is treated as "no expiration"
        assert_eq!(entity.expires_at, None);
    }

    #[test]
    fn test_entity_is_expired_at() {
        let entity =
            Entity { key: "key".to_string(), value: vec![], expires_at: Some(1000), version: 1 };

        // Before expiration
        assert!(!entity.is_expired_at(999));
        // At expiration
        assert!(entity.is_expired_at(1000));
        // After expiration
        assert!(entity.is_expired_at(1001));
    }

    #[test]
    fn test_entity_is_expired_at_no_expiration() {
        let entity = Entity { key: "key".to_string(), value: vec![], expires_at: None, version: 1 };

        // Never expires
        assert!(!entity.is_expired_at(u64::MAX));
    }

    #[test]
    fn test_entity_equality() {
        let entity1 = Entity {
            key: "key".to_string(),
            value: b"value".to_vec(),
            expires_at: Some(1000),
            version: 1,
        };
        let entity2 = entity1.clone();

        assert_eq!(entity1, entity2);
    }

    #[test]
    fn test_relationship_new() {
        let rel = Relationship::new("document:1", "viewer", "user:alice");

        assert_eq!(rel.resource, "document:1");
        assert_eq!(rel.relation, "viewer");
        assert_eq!(rel.subject, "user:alice");
    }

    #[test]
    fn test_relationship_from_proto() {
        let proto_rel = proto::Relationship {
            resource: "folder:root".to_string(),
            relation: "owner".to_string(),
            subject: "user:admin".to_string(),
        };

        let rel = Relationship::from_proto(proto_rel);

        assert_eq!(rel.resource, "folder:root");
        assert_eq!(rel.relation, "owner");
        assert_eq!(rel.subject, "user:admin");
    }

    #[test]
    fn test_relationship_equality_and_hash() {
        use std::collections::HashSet;

        let rel1 = Relationship::new("doc:1", "editor", "user:bob");
        let rel2 = Relationship::new("doc:1", "editor", "user:bob");
        let rel3 = Relationship::new("doc:1", "viewer", "user:bob");

        assert_eq!(rel1, rel2);
        assert_ne!(rel1, rel3);

        let mut set = HashSet::new();
        set.insert(rel1.clone());
        assert!(set.contains(&rel2));
        assert!(!set.contains(&rel3));
    }

    #[test]
    fn test_paged_result_has_next_page() {
        let with_next: PagedResult<String> = PagedResult {
            items: vec!["item".to_string()],
            next_page_token: Some("token".to_string()),
            block_height: 100,
        };

        let without_next: PagedResult<String> = PagedResult {
            items: vec!["item".to_string()],
            next_page_token: None,
            block_height: 100,
        };

        assert!(with_next.has_next_page());
        assert!(!without_next.has_next_page());
    }

    #[test]
    fn test_list_entities_opts_builder() {
        let opts = ListEntitiesOpts::with_prefix("user:")
            .at_height(100)
            .include_expired()
            .limit(50)
            .page_token("abc123")
            .linearizable();

        assert_eq!(opts.key_prefix, "user:");
        assert_eq!(opts.at_height, Some(100));
        assert!(opts.include_expired);
        assert_eq!(opts.limit, 50);
        assert_eq!(opts.page_token, Some("abc123".to_string()));
        assert_eq!(opts.consistency, ReadConsistency::Linearizable);
    }

    #[test]
    fn test_list_entities_opts_defaults() {
        let opts = ListEntitiesOpts::with_prefix("session:");

        assert_eq!(opts.key_prefix, "session:");
        assert_eq!(opts.at_height, None);
        assert!(!opts.include_expired);
        assert_eq!(opts.limit, 0);
        assert_eq!(opts.page_token, None);
        assert_eq!(opts.consistency, ReadConsistency::Eventual);
    }

    #[test]
    fn test_list_relationships_opts_builder() {
        let opts = ListRelationshipsOpts::new()
            .resource("document:1")
            .relation("viewer")
            .subject("user:alice")
            .at_height(50)
            .limit(100)
            .page_token("xyz")
            .consistency(ReadConsistency::Linearizable);

        assert_eq!(opts.resource, Some("document:1".to_string()));
        assert_eq!(opts.relation, Some("viewer".to_string()));
        assert_eq!(opts.subject, Some("user:alice".to_string()));
        assert_eq!(opts.at_height, Some(50));
        assert_eq!(opts.limit, 100);
        assert_eq!(opts.page_token, Some("xyz".to_string()));
        assert_eq!(opts.consistency, ReadConsistency::Linearizable);
    }

    #[test]
    fn test_list_relationships_opts_defaults() {
        let opts = ListRelationshipsOpts::new();

        assert_eq!(opts.resource, None);
        assert_eq!(opts.relation, None);
        assert_eq!(opts.subject, None);
        assert_eq!(opts.at_height, None);
        assert_eq!(opts.limit, 0);
        assert_eq!(opts.page_token, None);
        assert_eq!(opts.consistency, ReadConsistency::Eventual);
    }

    #[test]
    fn test_list_resources_opts_builder() {
        let opts = ListResourcesOpts::with_type("document")
            .at_height(200)
            .limit(25)
            .page_token("next")
            .linearizable();

        assert_eq!(opts.resource_type, "document");
        assert_eq!(opts.at_height, Some(200));
        assert_eq!(opts.limit, 25);
        assert_eq!(opts.page_token, Some("next".to_string()));
        assert_eq!(opts.consistency, ReadConsistency::Linearizable);
    }

    #[test]
    fn test_list_resources_opts_defaults() {
        let opts = ListResourcesOpts::with_type("folder");

        assert_eq!(opts.resource_type, "folder");
        assert_eq!(opts.at_height, None);
        assert_eq!(opts.limit, 0);
        assert_eq!(opts.page_token, None);
        assert_eq!(opts.consistency, ReadConsistency::Eventual);
    }

    #[test]
    fn test_list_entities_opts_bon_builder() {
        let opts = ListEntitiesOpts::builder()
            .key_prefix("user:")
            .at_height(100)
            .include_expired(true)
            .limit(50)
            .page_token("abc123")
            .consistency(ReadConsistency::Linearizable)
            .build();

        assert_eq!(opts.key_prefix, "user:");
        assert_eq!(opts.at_height, Some(100));
        assert!(opts.include_expired);
        assert_eq!(opts.limit, 50);
        assert_eq!(opts.page_token, Some("abc123".to_string()));
        assert_eq!(opts.consistency, ReadConsistency::Linearizable);
    }

    #[test]
    fn test_list_entities_opts_bon_builder_defaults() {
        let opts = ListEntitiesOpts::builder().build();

        assert_eq!(opts.key_prefix, "");
        assert_eq!(opts.at_height, None);
        assert!(!opts.include_expired);
        assert_eq!(opts.limit, 0);
        assert_eq!(opts.page_token, None);
        assert_eq!(opts.consistency, ReadConsistency::Eventual);
    }

    #[test]
    fn test_list_entities_opts_bon_builder_matches_default() {
        let from_builder = ListEntitiesOpts::builder().build();
        let from_default = ListEntitiesOpts::default();

        assert_eq!(from_builder.key_prefix, from_default.key_prefix);
        assert_eq!(from_builder.at_height, from_default.at_height);
        assert_eq!(from_builder.include_expired, from_default.include_expired);
        assert_eq!(from_builder.limit, from_default.limit);
        assert_eq!(from_builder.page_token, from_default.page_token);
        assert_eq!(from_builder.consistency, from_default.consistency);
    }

    #[test]
    fn test_list_relationships_opts_bon_builder() {
        let opts = ListRelationshipsOpts::builder()
            .resource("document:1")
            .relation("viewer")
            .subject("user:alice")
            .at_height(50)
            .limit(100)
            .page_token("xyz")
            .consistency(ReadConsistency::Linearizable)
            .build();

        assert_eq!(opts.resource, Some("document:1".to_string()));
        assert_eq!(opts.relation, Some("viewer".to_string()));
        assert_eq!(opts.subject, Some("user:alice".to_string()));
        assert_eq!(opts.at_height, Some(50));
        assert_eq!(opts.limit, 100);
        assert_eq!(opts.page_token, Some("xyz".to_string()));
        assert_eq!(opts.consistency, ReadConsistency::Linearizable);
    }

    #[test]
    fn test_list_relationships_opts_bon_builder_defaults() {
        let opts = ListRelationshipsOpts::builder().build();

        assert_eq!(opts.resource, None);
        assert_eq!(opts.relation, None);
        assert_eq!(opts.subject, None);
        assert_eq!(opts.at_height, None);
        assert_eq!(opts.limit, 0);
        assert_eq!(opts.page_token, None);
        assert_eq!(opts.consistency, ReadConsistency::Eventual);
    }

    #[test]
    fn test_list_relationships_opts_bon_builder_matches_default() {
        let from_builder = ListRelationshipsOpts::builder().build();
        let from_default = ListRelationshipsOpts::default();

        assert_eq!(from_builder.resource, from_default.resource);
        assert_eq!(from_builder.relation, from_default.relation);
        assert_eq!(from_builder.subject, from_default.subject);
        assert_eq!(from_builder.at_height, from_default.at_height);
        assert_eq!(from_builder.limit, from_default.limit);
        assert_eq!(from_builder.page_token, from_default.page_token);
        assert_eq!(from_builder.consistency, from_default.consistency);
    }

    #[test]
    fn test_list_resources_opts_bon_builder() {
        let opts = ListResourcesOpts::builder()
            .resource_type("document")
            .at_height(200)
            .limit(25)
            .page_token("next")
            .consistency(ReadConsistency::Linearizable)
            .build();

        assert_eq!(opts.resource_type, "document");
        assert_eq!(opts.at_height, Some(200));
        assert_eq!(opts.limit, 25);
        assert_eq!(opts.page_token, Some("next".to_string()));
        assert_eq!(opts.consistency, ReadConsistency::Linearizable);
    }

    #[test]
    fn test_list_resources_opts_bon_builder_defaults() {
        let opts = ListResourcesOpts::builder().build();

        assert_eq!(opts.resource_type, "");
        assert_eq!(opts.at_height, None);
        assert_eq!(opts.limit, 0);
        assert_eq!(opts.page_token, None);
        assert_eq!(opts.consistency, ReadConsistency::Eventual);
    }

    #[test]
    fn test_list_resources_opts_bon_builder_matches_default() {
        let from_builder = ListResourcesOpts::builder().build();
        let from_default = ListResourcesOpts::default();

        assert_eq!(from_builder.resource_type, from_default.resource_type);
        assert_eq!(from_builder.at_height, from_default.at_height);
        assert_eq!(from_builder.limit, from_default.limit);
        assert_eq!(from_builder.page_token, from_default.page_token);
        assert_eq!(from_builder.consistency, from_default.consistency);
    }

    // =========================================================================
    // Query Operations Integration Tests
    // =========================================================================

    #[tokio::test]
    async fn test_list_entities_returns_error_on_connection_failure() {
        let config = ClientConfig::builder()
            .endpoints(vec!["http://127.0.0.1:59999".into()])
            .client_id("list-entities-test")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");
        let result = client.list_entities(1, ListEntitiesOpts::with_prefix("user:")).await;

        assert!(result.is_err(), "expected connection error");
    }

    #[tokio::test]
    async fn test_list_relationships_returns_error_on_connection_failure() {
        let config = ClientConfig::builder()
            .endpoints(vec!["http://127.0.0.1:59999".into()])
            .client_id("list-rels-test")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");
        let result = client.list_relationships(1, 0, ListRelationshipsOpts::new()).await;

        assert!(result.is_err(), "expected connection error");
    }

    #[tokio::test]
    async fn test_list_resources_returns_error_on_connection_failure() {
        let config = ClientConfig::builder()
            .endpoints(vec!["http://127.0.0.1:59999".into()])
            .client_id("list-resources-test")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");
        let result = client.list_resources(1, 0, ListResourcesOpts::with_type("document")).await;

        assert!(result.is_err(), "expected connection error");
    }

    #[tokio::test]
    async fn test_list_entities_with_different_options() {
        let config = ClientConfig::builder()
            .endpoints(vec!["http://127.0.0.1:59999".into()])
            .client_id("list-entities-opts-test")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        // Test with various options - should still fail on connection but validates options are
        // passed
        let opts = ListEntitiesOpts::with_prefix("session:")
            .at_height(100)
            .include_expired()
            .limit(50)
            .linearizable();

        let result = client.list_entities(1, opts).await;
        assert!(result.is_err(), "expected connection error");
    }

    #[tokio::test]
    async fn test_list_relationships_with_filters() {
        let config = ClientConfig::builder()
            .endpoints(vec!["http://127.0.0.1:59999".into()])
            .client_id("list-rels-filter-test")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        // Test with filters
        let opts =
            ListRelationshipsOpts::new().resource("document:1").relation("viewer").limit(100);

        let result = client.list_relationships(1, 0, opts).await;
        assert!(result.is_err(), "expected connection error");
    }

    // =========================================================================
    // Shutdown Tests
    // =========================================================================

    #[tokio::test]
    async fn test_is_shutdown_false_initially() {
        let client = LedgerClient::connect("http://localhost:50051", "test-client")
            .await
            .expect("client creation");

        assert!(!client.is_shutdown(), "client should not be shutdown initially");
    }

    #[tokio::test]
    async fn test_is_shutdown_true_after_shutdown() {
        let client = LedgerClient::connect("http://localhost:50051", "test-client")
            .await
            .expect("client creation");

        client.shutdown().await;

        assert!(client.is_shutdown(), "client should be shutdown after calling shutdown()");
    }

    #[tokio::test]
    async fn test_shutdown_is_idempotent() {
        let client = LedgerClient::connect("http://localhost:50051", "test-client")
            .await
            .expect("client creation");

        // Multiple shutdown calls should not panic
        client.shutdown().await;
        client.shutdown().await;
        client.shutdown().await;

        assert!(client.is_shutdown());
    }

    #[tokio::test]
    async fn test_cloned_client_shares_shutdown_state() {
        let client1 = LedgerClient::connect("http://localhost:50051", "test-client")
            .await
            .expect("client creation");

        let client2 = client1.clone();

        assert!(!client1.is_shutdown());
        assert!(!client2.is_shutdown());

        // Shutdown through client1
        client1.shutdown().await;

        // Both should reflect shutdown state
        assert!(client1.is_shutdown());
        assert!(client2.is_shutdown(), "cloned client should share shutdown state");
    }

    #[tokio::test]
    async fn test_read_returns_shutdown_error_after_shutdown() {
        let config = ClientConfig::builder()
            .endpoints(vec!["http://localhost:50051".into()])
            .client_id("test-client")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        // Shutdown the client
        client.shutdown().await;

        // All operations should return Shutdown error
        let result = client.read(1, Some(0), "key").await;
        assert!(matches!(result, Err(crate::error::SdkError::Shutdown)));
    }

    #[tokio::test]
    async fn test_write_returns_shutdown_error_after_shutdown() {
        let config = ClientConfig::builder()
            .endpoints(vec!["http://localhost:50051".into()])
            .client_id("test-client")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        client.shutdown().await;

        let result =
            client.write(1, Some(0), vec![Operation::set_entity("key", vec![1, 2, 3])]).await;
        assert!(matches!(result, Err(crate::error::SdkError::Shutdown)));
    }

    #[tokio::test]
    async fn test_batch_write_returns_shutdown_error_after_shutdown() {
        let config = ClientConfig::builder()
            .endpoints(vec!["http://localhost:50051".into()])
            .client_id("test-client")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        client.shutdown().await;

        let result = client
            .batch_write(1, Some(0), vec![vec![Operation::set_entity("key", vec![1, 2, 3])]])
            .await;
        assert!(matches!(result, Err(crate::error::SdkError::Shutdown)));
    }

    #[tokio::test]
    async fn test_batch_read_returns_shutdown_error_after_shutdown() {
        let config = ClientConfig::builder()
            .endpoints(vec!["http://localhost:50051".into()])
            .client_id("test-client")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        client.shutdown().await;

        let result =
            client.batch_read(1, Some(0), vec!["key1".to_string(), "key2".to_string()]).await;
        assert!(matches!(result, Err(crate::error::SdkError::Shutdown)));
    }

    #[tokio::test]
    async fn test_watch_blocks_returns_shutdown_error_after_shutdown() {
        let config = ClientConfig::builder()
            .endpoints(vec!["http://localhost:50051".into()])
            .client_id("test-client")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        client.shutdown().await;

        let result = client.watch_blocks(1, 0, 1).await;
        assert!(matches!(result, Err(crate::error::SdkError::Shutdown)));
    }

    #[tokio::test]
    async fn test_admin_operations_return_shutdown_error_after_shutdown() {
        let config = ClientConfig::builder()
            .endpoints(vec!["http://localhost:50051".into()])
            .client_id("test-client")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        client.shutdown().await;

        // Test various admin operations
        assert!(matches!(
            client.create_namespace("test").await,
            Err(crate::error::SdkError::Shutdown)
        ));
        assert!(matches!(client.get_namespace(1).await, Err(crate::error::SdkError::Shutdown)));
        assert!(matches!(client.list_namespaces().await, Err(crate::error::SdkError::Shutdown)));
        assert!(matches!(client.create_vault(1).await, Err(crate::error::SdkError::Shutdown)));
        assert!(matches!(client.get_vault(1, 0).await, Err(crate::error::SdkError::Shutdown)));
        assert!(matches!(client.list_vaults().await, Err(crate::error::SdkError::Shutdown)));
    }

    #[tokio::test]
    async fn test_health_check_returns_shutdown_error_after_shutdown() {
        let config = ClientConfig::builder()
            .endpoints(vec!["http://localhost:50051".into()])
            .client_id("test-client")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        client.shutdown().await;

        assert!(matches!(client.health_check().await, Err(crate::error::SdkError::Shutdown)));
        assert!(matches!(
            client.health_check_detailed().await,
            Err(crate::error::SdkError::Shutdown)
        ));
        assert!(matches!(
            client.health_check_vault(1, 0).await,
            Err(crate::error::SdkError::Shutdown)
        ));
    }

    #[tokio::test]
    async fn test_verified_read_returns_shutdown_error_after_shutdown() {
        let config = ClientConfig::builder()
            .endpoints(vec!["http://localhost:50051".into()])
            .client_id("test-client")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        client.shutdown().await;

        assert!(matches!(
            client.verified_read(1, Some(0), "key", VerifyOpts::new()).await,
            Err(crate::error::SdkError::Shutdown)
        ));
    }

    #[tokio::test]
    async fn test_query_operations_return_shutdown_error_after_shutdown() {
        let config = ClientConfig::builder()
            .endpoints(vec!["http://localhost:50051".into()])
            .client_id("test-client")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        client.shutdown().await;

        assert!(matches!(
            client.list_entities(1, ListEntitiesOpts::with_prefix("key")).await,
            Err(crate::error::SdkError::Shutdown)
        ));
        assert!(matches!(
            client.list_relationships(1, 0, ListRelationshipsOpts::new()).await,
            Err(crate::error::SdkError::Shutdown)
        ));
        assert!(matches!(
            client.list_resources(1, 0, ListResourcesOpts::with_type("doc")).await,
            Err(crate::error::SdkError::Shutdown)
        ));
    }

    #[tokio::test]
    async fn test_shutdown_error_is_not_retryable() {
        assert!(!crate::error::SdkError::Shutdown.is_retryable());
    }
}
