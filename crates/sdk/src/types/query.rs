//! Query and write operation types: entities, relationships, operations, pagination.

use inferadb_ledger_proto::proto;
use inferadb_ledger_types::VaultSlug;

use crate::types::{
    read::ReadConsistency,
    verified_read::{BlockHeader, ChainProof, MerkleProof},
};

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
    /// Checks if there are more pages available.
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
    /// Converts from protobuf Entity.
    pub(crate) fn from_proto(proto: proto::Entity) -> Self {
        Self {
            key: proto.key,
            value: proto.value,
            expires_at: proto.expires_at.filter(|&ts| ts > 0),
            version: proto.version,
        }
    }

    /// Checks if this entity has expired relative to a given timestamp.
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
    /// Creates a new relationship.
    pub fn new(
        resource: impl Into<String>,
        relation: impl Into<String>,
        subject: impl Into<String>,
    ) -> Self {
        Self { resource: resource.into(), relation: relation.into(), subject: subject.into() }
    }

    /// Converts from protobuf Relationship.
    pub(crate) fn from_proto(proto: proto::Relationship) -> Self {
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
    /// Reads at a specific block height (None = current).
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
    /// Vault for vault-scoped entities (None = organization-level).
    pub vault: Option<VaultSlug>,
}

impl ListEntitiesOpts {
    /// Creates options with a key prefix filter.
    pub fn with_prefix(prefix: impl Into<String>) -> Self {
        Self { key_prefix: prefix.into(), ..Default::default() }
    }

    /// Reads at a specific block height.
    pub fn at_height(mut self, height: u64) -> Self {
        self.at_height = Some(height);
        self
    }

    /// Includes expired entities in results.
    pub fn include_expired(mut self) -> Self {
        self.include_expired = true;
        self
    }

    /// Sets maximum results per page.
    pub fn limit(mut self, limit: u32) -> Self {
        self.limit = limit;
        self
    }

    /// Continues from a previous page.
    pub fn page_token(mut self, token: impl Into<String>) -> Self {
        self.page_token = Some(token.into());
        self
    }

    /// Sets read consistency level.
    pub fn consistency(mut self, consistency: ReadConsistency) -> Self {
        self.consistency = consistency;
        self
    }

    /// Uses linearizable (strong) consistency.
    pub fn linearizable(mut self) -> Self {
        self.consistency = ReadConsistency::Linearizable;
        self
    }

    /// Scopes to a specific vault (for vault-level entities).
    pub fn vault(mut self, vault: VaultSlug) -> Self {
        self.vault = Some(vault);
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
    /// Reads at a specific block height (None = current).
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
    /// Creates default options (no filters).
    pub fn new() -> Self {
        Self::default()
    }

    /// Filters by resource.
    pub fn resource(mut self, resource: impl Into<String>) -> Self {
        self.resource = Some(resource.into());
        self
    }

    /// Filters by relation.
    pub fn relation(mut self, relation: impl Into<String>) -> Self {
        self.relation = Some(relation.into());
        self
    }

    /// Filters by subject.
    pub fn subject(mut self, subject: impl Into<String>) -> Self {
        self.subject = Some(subject.into());
        self
    }

    /// Reads at a specific block height.
    pub fn at_height(mut self, height: u64) -> Self {
        self.at_height = Some(height);
        self
    }

    /// Sets maximum results per page.
    pub fn limit(mut self, limit: u32) -> Self {
        self.limit = limit;
        self
    }

    /// Continues from a previous page.
    pub fn page_token(mut self, token: impl Into<String>) -> Self {
        self.page_token = Some(token.into());
        self
    }

    /// Sets read consistency level.
    pub fn consistency(mut self, consistency: ReadConsistency) -> Self {
        self.consistency = consistency;
        self
    }

    /// Uses linearizable (strong) consistency.
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
    /// Reads at a specific block height (None = current).
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
    /// Creates options with a resource type filter.
    pub fn with_type(resource_type: impl Into<String>) -> Self {
        Self { resource_type: resource_type.into(), ..Default::default() }
    }

    /// Reads at a specific block height.
    pub fn at_height(mut self, height: u64) -> Self {
        self.at_height = Some(height);
        self
    }

    /// Sets maximum results per page.
    pub fn limit(mut self, limit: u32) -> Self {
        self.limit = limit;
        self
    }

    /// Continues from a previous page.
    pub fn page_token(mut self, token: impl Into<String>) -> Self {
        self.page_token = Some(token.into());
        self
    }

    /// Sets read consistency level.
    pub fn consistency(mut self, consistency: ReadConsistency) -> Self {
        self.consistency = consistency;
        self
    }

    /// Uses linearizable (strong) consistency.
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
/// # use inferadb_ledger_sdk::{LedgerClient, ClientConfig, OrganizationSlug, VaultSlug, VerifyOpts, ServerSource};
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = LedgerClient::new(ClientConfig::builder()
/// #     .servers(ServerSource::from_static(["http://localhost:50051"]))
/// #     .client_id("example")
/// #     .build()?).await?;
/// # let (organization, vault) = (OrganizationSlug::new(1), VaultSlug::new(1));
/// let result = client.verified_read(organization, Some(vault), "key", VerifyOpts::new()).await?;
/// if let Some(verified) = result {
///     // Verify the proof is valid
///     verified.verify()?;
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
    /// Creates from protobuf response.
    pub(crate) fn from_proto(proto: proto::VerifiedReadResponse) -> Option<Self> {
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

    /// Verifies the value is authentic.
    ///
    /// Checks that the Merkle proof correctly links the value to the state root
    /// in the block header. If a chain proof is present, also verifies the
    /// chain of blocks links correctly.
    ///
    /// # Errors
    ///
    /// Returns `SdkError::ProofVerification` if the Merkle proof does not
    /// match the block header's state root.
    pub fn verify(&self) -> crate::error::Result<()> {
        // Verify the Merkle proof against the block header's state root
        if !self.merkle_proof.verify(&self.block_header.state_root) {
            return Err(crate::error::SdkError::ProofVerification {
                reason: "Merkle proof does not match state root",
            });
        }

        Ok(())
    }
}

/// A write operation to be submitted to the ledger.
///
/// Operations modify state in the ledger. They are applied atomically within
/// a single transaction. Use [`Operation::set_entity`] for key-value writes
/// and [`Operation::create_relationship`] for authorization tuples.
#[derive(Debug, Clone)]
pub enum Operation {
    /// Sets an entity value (key-value write).
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
    /// Deletes an entity.
    DeleteEntity {
        /// Entity key to delete.
        key: String,
    },
    /// Creates an authorization relationship.
    CreateRelationship {
        /// Resource identifier (format: "type:id").
        resource: String,
        /// Relation name (e.g., "viewer", "editor").
        relation: String,
        /// Subject identifier (format: "type:id" or "type:id#relation").
        subject: String,
    },
    /// Deletes an authorization relationship.
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
    /// Creates an operation that sets an entity's key-value pair.
    ///
    /// # Arguments
    ///
    /// * `key` - Entity key.
    /// * `value` - Entity value.
    /// * `expires_at` - Optional Unix epoch seconds when the entity expires.
    /// * `condition` - Optional condition that must be met for the write to succeed.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{Operation, SetCondition};
    /// // Simple set
    /// let op = Operation::set_entity("user:123", b"data".to_vec(), None, None);
    ///
    /// // Set with expiry
    /// let op = Operation::set_entity("session:abc", b"token".to_vec(), Some(1700000000), None);
    ///
    /// // Conditional set (create-if-not-exists)
    /// let op = Operation::set_entity("lock:xyz", b"owner".to_vec(), None, Some(SetCondition::NotExists));
    /// ```
    pub fn set_entity(
        key: impl Into<String>,
        value: Vec<u8>,
        expires_at: Option<u64>,
        condition: Option<SetCondition>,
    ) -> Self {
        Operation::SetEntity { key: key.into(), value, expires_at, condition }
    }

    /// Creates an operation that deletes an entity by key.
    pub fn delete_entity(key: impl Into<String>) -> Self {
        Operation::DeleteEntity { key: key.into() }
    }

    /// Creates an operation that establishes a relationship between a resource and subject.
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

    /// Creates an operation that removes a relationship between a resource and subject.
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

    /// Validates this operation against the given validation configuration.
    ///
    /// Checks field sizes and character whitelists. Call this before
    /// sending operations to the server for fast client-side validation.
    ///
    /// # Errors
    ///
    /// Returns `ValidationError` if key length, value size, or character constraints are violated.
    pub fn validate(
        &self,
        config: &inferadb_ledger_types::config::ValidationConfig,
    ) -> std::result::Result<(), inferadb_ledger_types::validation::ValidationError> {
        use inferadb_ledger_types::validation;
        match self {
            Operation::SetEntity { key, value, .. } => {
                validation::validate_key(key, config)?;
                validation::validate_value(value, config)?;
            },
            Operation::DeleteEntity { key } => {
                validation::validate_key(key, config)?;
            },
            Operation::CreateRelationship { resource, relation, subject } => {
                validation::validate_relationship_string(resource, "resource", config)?;
                validation::validate_relationship_string(relation, "relation", config)?;
                validation::validate_relationship_string(subject, "subject", config)?;
            },
            Operation::DeleteRelationship { resource, relation, subject } => {
                validation::validate_relationship_string(resource, "resource", config)?;
                validation::validate_relationship_string(relation, "relation", config)?;
                validation::validate_relationship_string(subject, "subject", config)?;
            },
        }
        Ok(())
    }

    /// Returns the estimated wire size of this operation in bytes.
    ///
    /// Used for aggregate payload size validation before sending to the server.
    pub(crate) fn estimated_size_bytes(&self) -> usize {
        match self {
            Operation::SetEntity { key, value, .. } => key.len() + value.len(),
            Operation::DeleteEntity { key } => key.len(),
            Operation::CreateRelationship { resource, relation, subject }
            | Operation::DeleteRelationship { resource, relation, subject } => {
                resource.len() + relation.len() + subject.len()
            },
        }
    }

    /// Converts to protobuf operation.
    pub(crate) fn to_proto(&self) -> proto::Operation {
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
    /// Creates a condition for compare-and-set operations from an expected
    /// previous value.
    ///
    /// - `None` → [`SetCondition::NotExists`] (create-if-absent)
    /// - `Some(value)` → [`SetCondition::ValueEquals`] (update-if-unchanged)
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use inferadb_ledger_sdk::SetCondition;
    ///
    /// // Insert only if key doesn't exist
    /// let cond = SetCondition::from_expected(None::<Vec<u8>>);
    /// assert!(matches!(cond, SetCondition::NotExists));
    ///
    /// // Update only if current value matches
    /// let cond = SetCondition::from_expected(Some(b"old-value".to_vec()));
    /// assert!(matches!(cond, SetCondition::ValueEquals(_)));
    /// ```
    pub fn from_expected(expected: Option<impl Into<Vec<u8>>>) -> Self {
        match expected {
            None => SetCondition::NotExists,
            Some(value) => SetCondition::ValueEquals(value.into()),
        }
    }

    /// Converts to protobuf set condition.
    pub(crate) fn to_proto(&self) -> proto::SetCondition {
        let condition = match self {
            SetCondition::NotExists => proto::set_condition::Condition::NotExists(true),
            SetCondition::MustExist => proto::set_condition::Condition::MustExists(true),
            SetCondition::Version(v) => proto::set_condition::Condition::Version(*v),
            SetCondition::ValueEquals(v) => proto::set_condition::Condition::ValueEquals(v.clone()),
        };
        proto::SetCondition { condition: Some(condition) }
    }
}
