//! Data read/write operations.

use inferadb_ledger_proto::proto;
use inferadb_ledger_types::{OrganizationSlug, UserSlug, VaultSlug};

use crate::{
    LedgerClient,
    error::{self, Result},
    proto_util::missing_response_field,
    retry::with_retry_cancellable,
    streaming::{HeightTracker, ReconnectingStream},
    types::{
        query::{Operation, SetCondition},
        read::{ReadConsistency, WriteSuccess},
        streaming::BlockAnnouncement,
    },
};

impl LedgerClient {
    // =========================================================================
    // Read Operations
    // =========================================================================

    /// Reads a value by key.
    ///
    /// By default uses `EVENTUAL` consistency, which reads from any replica
    /// for lowest latency. Pass `Some(ReadConsistency::Linearizable)` for
    /// strong consistency reads served from the leader.
    ///
    /// # Arguments
    ///
    /// * `caller` - Identity of the user performing this operation (external slug).
    /// * `organization` - Organization slug (external identifier).
    /// * `vault` - Optional vault slug (omit for organization-level entities).
    /// * `key` - The key to read.
    /// * `consistency` - Optional consistency level (`None` = eventual).
    /// * `token` - Optional per-request cancellation token.
    ///
    /// # Returns
    ///
    /// Returns `Ok(Some(value))` if the key exists, `Ok(None)` if not found.
    ///
    /// # Errors
    ///
    /// Returns an error if the read fails after retry attempts are exhausted,
    /// the client has been shut down, or the cancellation token is triggered.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, ReadConsistency, UserSlug, VaultSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let (organization, vault) = (OrganizationSlug::new(1), VaultSlug::new(1));
    /// // Eventual consistency read (default)
    /// let value = client.read(UserSlug::new(42), organization, None, "user:123", None, None).await?;
    ///
    /// // Linearizable consistency read
    /// let value = client.read(UserSlug::new(42), organization, Some(vault), "key",
    ///     Some(ReadConsistency::Linearizable), None).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn read(
        &self,
        caller: UserSlug,
        organization: OrganizationSlug,
        vault: Option<VaultSlug>,
        key: impl Into<String>,
        consistency: Option<ReadConsistency>,
        token: Option<tokio_util::sync::CancellationToken>,
    ) -> Result<Option<Vec<u8>>> {
        self.read_internal(
            caller,
            organization,
            vault,
            key.into(),
            consistency.unwrap_or(ReadConsistency::Eventual),
            token.as_ref(),
        )
        .await
    }

    /// Batch read multiple keys in a single RPC call.
    ///
    /// Amortizes network overhead across multiple reads for higher throughput.
    /// All reads share the same organization, vault, and consistency level.
    ///
    /// # Arguments
    ///
    /// * `caller` - Identity of the user performing this operation (external slug).
    /// * `organization` - Organization slug (external identifier).
    /// * `vault` - Optional vault slug (omit for organization-level entities).
    /// * `keys` - The keys to read (max 1000).
    /// * `consistency` - Optional consistency level (`None` = eventual).
    /// * `token` - Optional per-request cancellation token.
    ///
    /// # Returns
    ///
    /// Returns a vector of `(key, Option<value>)` pairs in the same order as
    /// the input keys. Missing keys have `None` values.
    ///
    /// # Errors
    ///
    /// Returns an error if the batch read fails after retry attempts are
    /// exhausted, the client has been shut down, or the cancellation token
    /// is triggered.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, UserSlug, VaultSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let (organization, vault) = (OrganizationSlug::new(1), VaultSlug::new(1));
    /// let results = client.batch_read(
    ///     UserSlug::new(42),
    ///     organization,
    ///     Some(vault),
    ///     vec!["key1", "key2", "key3"],
    ///     None,
    ///     None,
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
        caller: UserSlug,
        organization: OrganizationSlug,
        vault: Option<VaultSlug>,
        keys: impl IntoIterator<Item = impl Into<String>>,
        consistency: Option<ReadConsistency>,
        token: Option<tokio_util::sync::CancellationToken>,
    ) -> Result<Vec<(String, Option<Vec<u8>>)>> {
        self.batch_read_internal(
            caller,
            organization,
            vault,
            keys.into_iter().map(Into::into).collect(),
            consistency.unwrap_or(ReadConsistency::Eventual),
            token.as_ref(),
        )
        .await
    }

    // =========================================================================
    // Internal Read Implementation
    // =========================================================================

    /// Internal read implementation with configurable consistency and
    /// optional per-request cancellation.
    async fn read_internal(
        &self,
        caller: UserSlug,
        organization: OrganizationSlug,
        vault: Option<VaultSlug>,
        key: String,
        consistency: ReadConsistency,
        request_token: Option<&tokio_util::sync::CancellationToken>,
    ) -> Result<Option<Vec<u8>>> {
        self.check_shutdown(request_token)?;

        let token = self.effective_token(request_token);
        let pool = &self.pool;
        let retry_policy = self.config().retry_policy().clone();

        self.with_metrics(
            "read",
            with_retry_cancellable(&retry_policy, &token, Some(pool), "read", || async {
                let mut client = match vault {
                    Some(v) => {
                        crate::connected_client_for_vault!(
                            pool,
                            create_read_client,
                            organization,
                            v
                        )
                    },
                    None => crate::connected_client!(pool, create_read_client),
                };

                let request = proto::ReadRequest {
                    organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                    vault: vault.map(|v| proto::VaultSlug { slug: v.value() }),
                    key: key.clone(),
                    consistency: consistency.to_proto() as i32,
                    caller: Some(proto::UserSlug { slug: caller.value() }),
                };

                let response = client.read(tonic::Request::new(request)).await?.into_inner();

                Ok(response.value)
            }),
        )
        .await
    }

    /// Internal batch read implementation with configurable consistency and
    /// optional per-request cancellation.
    async fn batch_read_internal(
        &self,
        caller: UserSlug,
        organization: OrganizationSlug,
        vault: Option<VaultSlug>,
        keys: Vec<String>,
        consistency: ReadConsistency,
        request_token: Option<&tokio_util::sync::CancellationToken>,
    ) -> Result<Vec<(String, Option<Vec<u8>>)>> {
        self.check_shutdown(request_token)?;

        let token = self.effective_token(request_token);
        let pool = &self.pool;
        let retry_policy = self.config().retry_policy().clone();

        self.with_metrics(
            "batch_read",
            with_retry_cancellable(&retry_policy, &token, Some(pool), "batch_read", || async {
                let mut client = match vault {
                    Some(v) => {
                        crate::connected_client_for_vault!(
                            pool,
                            create_read_client,
                            organization,
                            v
                        )
                    },
                    None => crate::connected_client!(pool, create_read_client),
                };

                let request = proto::BatchReadRequest {
                    organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                    vault: vault.map(|v| proto::VaultSlug { slug: v.value() }),
                    keys: keys.clone(),
                    consistency: consistency.to_proto() as i32,
                    caller: Some(proto::UserSlug { slug: caller.value() }),
                };

                let response = client.batch_read(tonic::Request::new(request)).await?.into_inner();

                // Convert results to (key, Option<value>) pairs
                let results = response.results.into_iter().map(|r| (r.key, r.value)).collect();

                Ok(results)
            }),
        )
        .await
    }

    // =========================================================================
    // Write Operations
    // =========================================================================

    /// Submits a write transaction to the ledger.
    ///
    /// Writes are automatically idempotent via server-assigned sequence numbers.
    /// The server assigns monotonically increasing sequences at Raft commit time.
    /// If a write fails with a retryable error, it will be retried with the
    /// same idempotency key. If the server reports the write was already
    /// committed (duplicate), the original result is returned as success.
    ///
    /// # Arguments
    ///
    /// * `caller` - Identity of the user performing this operation (external slug).
    /// * `organization` - Organization slug (external identifier).
    /// * `vault` - Optional vault slug (required for relationships).
    /// * `operations` - The operations to apply atomically.
    /// * `token` - Optional per-request cancellation token.
    ///
    /// # Durability
    ///
    /// A successful response indicates the write is **WAL-durable**: the Raft
    /// log has fsynced the committed entry (durable against crash) and every
    /// replica has applied it in-memory. State-DB materialization (the B-tree
    /// dual-slot persist) lands on the next `StateCheckpointer` tick —
    /// typically within 500ms — or immediately on a graceful shutdown,
    /// snapshot, or backup.
    ///
    /// On crash, the state DB is rebuilt by replaying the WAL tail during
    /// recovery; replay is idempotent and transparent to callers. See
    /// `docs/architecture/durability.md` for the full contract.
    ///
    /// # Returns
    ///
    /// Returns [`WriteSuccess`] containing the transaction ID, block height, and
    /// server-assigned sequence number.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Connection fails after retry attempts
    /// - A conditional write (CAS) condition fails
    /// - An idempotency key is reused with different payload
    /// - The client has been shut down or the cancellation token is triggered
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, Operation, OrganizationSlug, UserSlug, VaultSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let (organization, vault) = (OrganizationSlug::new(1), VaultSlug::new(1));
    /// let result = client.write(
    ///     UserSlug::new(42),
    ///     organization,
    ///     Some(vault),
    ///     vec![
    ///         Operation::set_entity("user:123", b"data".to_vec(), None, None),
    ///         Operation::create_relationship("doc:456", "viewer", "user:123"),
    ///     ],
    ///     None,
    /// ).await?;
    ///
    /// println!("Committed at block {} with sequence {}", result.block_height, result.assigned_sequence);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn write(
        &self,
        caller: UserSlug,
        organization: OrganizationSlug,
        vault: Option<VaultSlug>,
        operations: Vec<Operation>,
        token: Option<tokio_util::sync::CancellationToken>,
    ) -> Result<WriteSuccess> {
        self.check_shutdown(token.as_ref())?;

        // Generate UUID idempotency key once for this request
        // The same key is reused across all retry attempts
        let idempotency_key = uuid::Uuid::new_v4();

        self.execute_write(
            caller,
            organization,
            vault,
            &operations,
            idempotency_key,
            token.as_ref(),
        )
        .await
    }

    /// Executes a single write attempt with retry for transient errors.
    ///
    /// The idempotency key is preserved across retry attempts to ensure
    /// at-most-once semantics even with network failures.
    async fn execute_write(
        &self,
        caller: UserSlug,
        organization: OrganizationSlug,
        vault: Option<VaultSlug>,
        operations: &[Operation],
        idempotency_key: uuid::Uuid,
        request_token: Option<&tokio_util::sync::CancellationToken>,
    ) -> Result<WriteSuccess> {
        // Client-side validation: fast rejection before network round-trip
        let validation_config = self.config().validation();
        inferadb_ledger_types::validation::validate_operations_count(
            operations.len(),
            validation_config,
        )
        .map_err(|e| error::SdkError::Validation { message: e.to_string() })?;
        let mut total_bytes: usize = 0;
        for op in operations {
            op.validate(validation_config)
                .map_err(|e| error::SdkError::Validation { message: e.to_string() })?;
            total_bytes += op.estimated_size_bytes();
        }
        inferadb_ledger_types::validation::validate_batch_payload_bytes(
            total_bytes,
            validation_config,
        )
        .map_err(|e| error::SdkError::Validation { message: e.to_string() })?;

        let token = self.effective_token(request_token);
        let pool = &self.pool;
        let retry_policy = self.config().retry_policy().clone();
        let client_id = self.client_id().to_string();

        // Convert operations to proto
        let proto_operations: Vec<proto::Operation> =
            operations.iter().map(Operation::to_proto).collect();

        // Convert UUID to bytes (16 bytes)
        let idempotency_key_bytes = idempotency_key.as_bytes().to_vec();

        // Execute with retry for transient errors
        self.with_metrics(
            "write",
            with_retry_cancellable(&retry_policy, &token, Some(pool), "write", || {
                let proto_ops = proto_operations.clone();
                let cid = client_id.clone();
                let key_bytes = idempotency_key_bytes.clone();
                async move {
                    let mut write_client = match vault {
                        Some(v) => crate::connected_client_for_vault!(
                            pool,
                            create_write_client,
                            organization,
                            v
                        ),
                        None => crate::connected_client!(pool, create_write_client),
                    };

                    let request = proto::WriteRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        vault: vault.map(|v| proto::VaultSlug { slug: v.value() }),
                        client_id: Some(proto::ClientId { id: cid }),
                        idempotency_key: key_bytes,
                        operations: proto_ops,
                        include_tx_proof: false,
                        caller: Some(proto::UserSlug { slug: caller.value() }),
                    };

                    let response =
                        write_client.write(tonic::Request::new(request)).await?.into_inner();

                    Self::process_write_response(response)
                }
            }),
        )
        .await
    }

    /// Processes a WriteResponse and converts to Result<WriteSuccess>.
    fn process_write_response(response: proto::WriteResponse) -> Result<WriteSuccess> {
        match response.result {
            Some(proto::write_response::Result::Success(success)) => Ok(WriteSuccess {
                tx_id: Self::tx_id_to_hex(success.tx_id),
                block_height: success.block_height,
                assigned_sequence: success.assigned_sequence,
            }),
            Some(proto::write_response::Result::Error(error)) => {
                let code = proto::WriteErrorCode::try_from(error.code)
                    .unwrap_or(proto::WriteErrorCode::Unspecified);

                match code {
                    proto::WriteErrorCode::AlreadyCommitted => {
                        // Idempotent retry - return the original success with assigned_sequence
                        Ok(WriteSuccess {
                            tx_id: Self::tx_id_to_hex(error.committed_tx_id),
                            block_height: error.committed_block_height.unwrap_or(0),
                            assigned_sequence: error.assigned_sequence.unwrap_or(0),
                        })
                    },
                    proto::WriteErrorCode::IdempotencyKeyReused => {
                        // Client reused idempotency key with different payload
                        Err(crate::error::SdkError::Idempotency {
                            message: format!(
                                "Idempotency key reused with different payload: {}",
                                error.message
                            ),
                            conflict_key: None,
                            original_tx_id: Some(Self::tx_id_to_hex(error.committed_tx_id.clone())),
                        })
                    },
                    _ => {
                        // Other write errors (CAS failures, etc.)
                        Err(crate::error::SdkError::Rpc {
                            code: tonic::Code::FailedPrecondition,
                            message: error.message,
                            request_id: None,
                            trace_id: None,
                            error_details: None,
                        })
                    },
                }
            },
            None => Err(missing_response_field("result", "WriteResponse")),
        }
    }

    /// Converts TxId bytes to hex string.
    pub(crate) fn tx_id_to_hex(tx_id: Option<proto::TxId>) -> String {
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

    // =============================================================================
    // Single-Operation Convenience Methods
    // =============================================================================

    /// Writes a single entity (set), optionally with expiration and/or a
    /// condition.
    ///
    /// Convenience wrapper around [`write`](Self::write) for the common case of
    /// setting a single key-value pair. Generates an idempotency key
    /// automatically. See [`write`](Self::write) for durability semantics.
    ///
    /// # Arguments
    ///
    /// * `caller` - Identity of the user performing this operation (external slug).
    /// * `organization` - Organization slug (external identifier).
    /// * `vault` - Optional vault slug (omit for organization-level entities).
    /// * `key` - The entity key.
    /// * `value` - The entity value.
    /// * `expires_at` - Optional Unix timestamp (seconds) when the entity expires.
    /// * `condition` - Optional condition for compare-and-swap writes.
    /// * `token` - Optional cancellation token for this request. When triggered, the operation is
    ///   cancelled at the next retry boundary. Pass `None` to rely on the client-level token only.
    ///
    /// # Errors
    ///
    /// Returns an error if validation fails, the condition is not met, or the
    /// write fails after retry attempts.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, UserSlug, VaultSlug, SetCondition};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let (organization, vault) = (OrganizationSlug::new(1), VaultSlug::new(1));
    /// // Simple set:
    /// client.set_entity(UserSlug::new(42), organization, Some(vault), "user:123", b"data".to_vec(), None, None, None).await?;
    ///
    /// // With expiration:
    /// client.set_entity(UserSlug::new(42), organization, Some(vault), "session:abc", b"token".to_vec(), Some(1700000000), None, None).await?;
    ///
    /// // Conditional (create-if-not-exists):
    /// client.set_entity(UserSlug::new(42), organization, Some(vault), "lock:xyz", b"owner".to_vec(), None, Some(SetCondition::NotExists), None).await?;
    /// # Ok(())
    /// # }
    /// ```
    #[allow(clippy::too_many_arguments)]
    pub async fn set_entity(
        &self,
        caller: UserSlug,
        organization: OrganizationSlug,
        vault: Option<VaultSlug>,
        key: impl Into<String>,
        value: Vec<u8>,
        expires_at: Option<u64>,
        condition: Option<SetCondition>,
        token: Option<tokio_util::sync::CancellationToken>,
    ) -> Result<WriteSuccess> {
        self.write(
            caller,
            organization,
            vault,
            vec![Operation::set_entity(key, value, expires_at, condition)],
            token,
        )
        .await
    }

    /// Deletes a single entity.
    ///
    /// Convenience wrapper around [`write`](Self::write) for deleting a single
    /// key. See [`write`](Self::write) for durability semantics.
    ///
    /// # Arguments
    ///
    /// * `caller` - Identity of the user performing this operation (external slug).
    /// * `organization` - Organization slug (external identifier).
    /// * `vault` - Optional vault slug (omit for organization-level entities).
    /// * `key` - The entity key to delete.
    /// * `token` - Optional cancellation token for this request. When triggered, the operation is
    ///   cancelled at the next retry boundary. Pass `None` to rely on the client-level token only.
    ///
    /// # Errors
    ///
    /// Returns an error if validation fails or the write fails after retry
    /// attempts.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, UserSlug, VaultSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let (organization, vault) = (OrganizationSlug::new(1), VaultSlug::new(1));
    /// client.delete_entity(UserSlug::new(42), organization, Some(vault), "user:123", None).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn delete_entity(
        &self,
        caller: UserSlug,
        organization: OrganizationSlug,
        vault: Option<VaultSlug>,
        key: impl Into<String>,
        token: Option<tokio_util::sync::CancellationToken>,
    ) -> Result<WriteSuccess> {
        self.write(caller, organization, vault, vec![Operation::delete_entity(key)], token).await
    }

    // =============================================================================
    // Block Operations
    // =============================================================================

    /// Reads a value at a specific past block height.
    ///
    /// Useful for audits, compliance queries, and debugging past state.
    /// Optionally includes Merkle and chain proofs for cryptographic verification.
    ///
    /// # Arguments
    ///
    /// * `organization` - Organization slug (external identifier).
    /// * `vault` - Optional vault slug (omit for organization-level reads).
    /// * `key` - The key to read.
    /// * `at_height` - Block height to read from (required, must be >= 1).
    /// * `include_proof` - When `true`, the response includes `block_header` and `merkle_proof`.
    /// * `include_chain_proof` - When `true` (requires `include_proof`), includes a chain proof.
    /// * `trusted_height` - Starting point for chain proof; used only when `include_chain_proof` is
    ///   `true`.
    /// * `token` - Optional per-request cancellation token.
    ///
    /// # Returns
    ///
    /// Returns a [`crate::HistoricalRead`] containing the value and any requested proofs.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails after retry attempts are exhausted,
    /// the client has been shut down, or the cancellation token is triggered.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, VaultSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let (organization, vault) = (OrganizationSlug::new(1), VaultSlug::new(1));
    /// let result = client.historical_read(
    ///     organization,
    ///     Some(vault),
    ///     "user:123",
    ///     42,
    ///     false,
    ///     false,
    ///     None,
    ///     None,
    /// ).await?;
    /// println!("Value at height {}: {:?}", result.block_height, result.value);
    /// # Ok(())
    /// # }
    /// ```
    #[allow(clippy::too_many_arguments)]
    pub async fn historical_read(
        &self,
        organization: OrganizationSlug,
        vault: Option<VaultSlug>,
        key: impl Into<String>,
        at_height: u64,
        include_proof: bool,
        include_chain_proof: bool,
        trusted_height: Option<u64>,
        token: Option<tokio_util::sync::CancellationToken>,
    ) -> Result<crate::types::verified_read::HistoricalRead> {
        self.check_shutdown(token.as_ref())?;

        let token = self.effective_token(token.as_ref());
        let pool = &self.pool;
        let retry_policy = self.config().retry_policy().clone();
        let key = key.into();

        self.with_metrics(
            "historical_read",
            with_retry_cancellable(
                &retry_policy,
                &token,
                Some(pool),
                "historical_read",
                || async {
                    let mut client = crate::connected_client!(pool, create_read_client);

                    let request = proto::HistoricalReadRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        vault: vault.map(|v| proto::VaultSlug { slug: v.value() }),
                        key: key.clone(),
                        at_height,
                        include_proof,
                        include_chain_proof,
                        trusted_height,
                    };

                    let response =
                        client.historical_read(tonic::Request::new(request)).await?.into_inner();

                    Ok(crate::types::verified_read::HistoricalRead {
                        value: response.value,
                        block_height: response.block_height,
                        block_header: response
                            .block_header
                            .map(crate::types::verified_read::BlockHeader::from_proto),
                        merkle_proof: response
                            .merkle_proof
                            .map(crate::types::verified_read::MerkleProof::from_proto),
                        chain_proof: response
                            .chain_proof
                            .map(crate::types::verified_read::ChainProof::from_proto),
                    })
                },
            ),
        )
        .await
    }

    /// Retrieves a complete block by height.
    ///
    /// Returns the block header and all transactions committed at `height`.
    ///
    /// # Arguments
    ///
    /// * `organization` - Organization slug (external identifier).
    /// * `vault` - Optional vault slug (omit for organization-level blocks).
    /// * `height` - The block height to fetch.
    /// * `token` - Optional per-request cancellation token.
    ///
    /// # Returns
    ///
    /// Returns a [`crate::Block`] containing the header and all transactions.
    ///
    /// # Errors
    ///
    /// Returns an error if the block does not exist, the request fails after
    /// retry attempts are exhausted, the client has been shut down, or the
    /// cancellation token is triggered.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, VaultSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let (organization, vault) = (OrganizationSlug::new(1), VaultSlug::new(1));
    /// let block = client.get_block(organization, Some(vault), 42, None).await?;
    /// if let Some(header) = &block.header {
    ///     println!("Block {} has {} transactions", header.height, block.transactions.len());
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_block(
        &self,
        organization: OrganizationSlug,
        vault: Option<VaultSlug>,
        height: u64,
        token: Option<tokio_util::sync::CancellationToken>,
    ) -> Result<crate::types::verified_read::Block> {
        self.check_shutdown(token.as_ref())?;

        let token = self.effective_token(token.as_ref());
        let pool = &self.pool;
        let retry_policy = self.config().retry_policy().clone();

        self.with_metrics(
            "get_block",
            with_retry_cancellable(&retry_policy, &token, Some(pool), "get_block", || async {
                let mut client = crate::connected_client!(pool, create_read_client);

                let request = proto::GetBlockRequest {
                    organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                    vault: vault.map(|v| proto::VaultSlug { slug: v.value() }),
                    height,
                };

                let response = client.get_block(tonic::Request::new(request)).await?.into_inner();

                let block = response
                    .block
                    .ok_or_else(|| missing_response_field("block", "GetBlockResponse"))?;

                Ok(crate::types::verified_read::Block::from_proto(block))
            }),
        )
        .await
    }

    /// Retrieves a range of blocks for sync and catchup.
    ///
    /// Returns blocks from `start_height` through `end_height` (both inclusive),
    /// ordered by height ascending. Maximum range is 1000 blocks per request.
    ///
    /// # Arguments
    ///
    /// * `organization` - Organization slug (external identifier).
    /// * `vault` - Optional vault slug (omit for organization-level blocks).
    /// * `start_height` - First block height to return (inclusive).
    /// * `end_height` - Last block height to return (inclusive, max `start + 999`).
    /// * `token` - Optional per-request cancellation token.
    ///
    /// # Returns
    ///
    /// Returns a tuple `(Vec<Block>, u64)` where the second element is the
    /// current chain tip height (useful for detecting whether more syncing is needed).
    ///
    /// # Errors
    ///
    /// Returns an error if the range is invalid, the request fails after retry
    /// attempts are exhausted, the client has been shut down, or the cancellation
    /// token is triggered.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, VaultSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let (organization, vault) = (OrganizationSlug::new(1), VaultSlug::new(1));
    /// let (blocks, current_tip) = client.get_block_range(organization, Some(vault), 10, 20, None).await?;
    /// println!("Got {} blocks, chain tip is {}", blocks.len(), current_tip);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_block_range(
        &self,
        organization: OrganizationSlug,
        vault: Option<VaultSlug>,
        start_height: u64,
        end_height: u64,
        token: Option<tokio_util::sync::CancellationToken>,
    ) -> Result<(Vec<crate::types::verified_read::Block>, u64)> {
        self.check_shutdown(token.as_ref())?;

        let token = self.effective_token(token.as_ref());
        let pool = &self.pool;
        let retry_policy = self.config().retry_policy().clone();

        self.with_metrics(
            "get_block_range",
            with_retry_cancellable(
                &retry_policy,
                &token,
                Some(pool),
                "get_block_range",
                || async {
                    let mut client = crate::connected_client!(pool, create_read_client);

                    let request = proto::GetBlockRangeRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        vault: vault.map(|v| proto::VaultSlug { slug: v.value() }),
                        start_height,
                        end_height,
                    };

                    let response =
                        client.get_block_range(tonic::Request::new(request)).await?.into_inner();

                    let blocks = response
                        .blocks
                        .into_iter()
                        .map(crate::types::verified_read::Block::from_proto)
                        .collect();

                    Ok((blocks, response.current_tip))
                },
            ),
        )
        .await
    }

    /// Retrieves the current chain tip for a vault.
    ///
    /// Returns the latest committed block height and its cryptographic commitments.
    /// Useful for checking chain progress and as an anchor for chain proofs.
    ///
    /// # Arguments
    ///
    /// * `organization` - Organization slug (external identifier).
    /// * `vault` - Optional vault slug (omit for organization-level tip).
    /// * `token` - Optional per-request cancellation token.
    ///
    /// # Returns
    ///
    /// Returns a [`crate::ChainTip`] with the current block height, block hash, and state root.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails after retry attempts are exhausted,
    /// the client has been shut down, or the cancellation token is triggered.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, VaultSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let (organization, vault) = (OrganizationSlug::new(1), VaultSlug::new(1));
    /// let tip = client.get_tip(organization, Some(vault), None).await?;
    /// println!("Current chain tip: height={}", tip.height);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_tip(
        &self,
        organization: OrganizationSlug,
        vault: Option<VaultSlug>,
        token: Option<tokio_util::sync::CancellationToken>,
    ) -> Result<crate::types::verified_read::ChainTip> {
        self.check_shutdown(token.as_ref())?;

        let token = self.effective_token(token.as_ref());
        let pool = &self.pool;
        let retry_policy = self.config().retry_policy().clone();

        self.with_metrics(
            "get_tip",
            with_retry_cancellable(&retry_policy, &token, Some(pool), "get_tip", || async {
                let mut client = crate::connected_client!(pool, create_read_client);

                let request = proto::GetTipRequest {
                    organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                    vault: vault.map(|v| proto::VaultSlug { slug: v.value() }),
                };

                let response = client.get_tip(tonic::Request::new(request)).await?.into_inner();

                Ok(crate::types::verified_read::ChainTip {
                    height: response.height,
                    block_hash: response.block_hash.map(|h| h.value).unwrap_or_default(),
                    state_root: response.state_root.map(|h| h.value).unwrap_or_default(),
                })
            }),
        )
        .await
    }

    // =============================================================================
    // Streaming Operations
    // =============================================================================

    /// Subscribes to block announcements for a vault.
    ///
    /// Returns a stream of [`BlockAnnouncement`] items that emits each time a new
    /// block is committed to the vault's chain. The stream automatically reconnects
    /// on disconnect and resumes from the last seen block height.
    ///
    /// # Arguments
    ///
    /// * `caller` - Identity of the user performing this operation (external slug).
    /// * `organization` - Organization slug (external identifier).
    /// * `vault` - Vault slug (external identifier).
    /// * `start_height` - First block height to receive (must be >= 1).
    ///
    /// # Returns
    ///
    /// Returns a `Stream` that yields `Result<BlockAnnouncement>` items.
    ///
    /// # Errors
    ///
    /// Returns `SdkError::Shutdown` if the client has been shut down.
    /// Returns `SdkError::Rpc` if the initial stream connection fails.
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
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, UserSlug, VaultSlug};
    /// # use futures::StreamExt;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = LedgerClient::connect("http://localhost:50051", "my-app").await?;
    /// # let organization = OrganizationSlug::new(1);
    ///
    /// // Start watching from height 1
    /// let mut stream = client.watch_blocks(UserSlug::new(42), organization, VaultSlug::new(0), 1).await?;
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
        caller: UserSlug,
        organization: OrganizationSlug,
        vault: VaultSlug,
        start_height: u64,
    ) -> Result<impl futures::Stream<Item = Result<BlockAnnouncement>>> {
        self.check_shutdown(None)?;

        // Get the initial stream
        let initial_stream =
            self.create_watch_blocks_stream(caller, organization, vault, start_height).await?;

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
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        vault: Some(proto::VaultSlug { slug: vault.value() }),
                        start_height: next_height,
                        caller: Some(proto::UserSlug { slug: caller.value() }),
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

    /// Creates a WatchBlocks stream without reconnection logic.
    async fn create_watch_blocks_stream(
        &self,
        caller: UserSlug,
        organization: OrganizationSlug,
        vault: VaultSlug,
        start_height: u64,
    ) -> Result<tonic::Streaming<proto::BlockAnnouncement>> {
        let channel = self.pool.get_channel().await?;
        let mut client = Self::create_read_client(
            channel,
            self.pool.compression_enabled(),
            self.trace_interceptor(),
        );

        let request = proto::WatchBlocksRequest {
            organization: Some(proto::OrganizationSlug { slug: organization.value() }),
            vault: Some(proto::VaultSlug { slug: vault.value() }),
            start_height,
            caller: Some(proto::UserSlug { slug: caller.value() }),
        };

        let response = client.watch_blocks(tonic::Request::new(request)).await?.into_inner();

        Ok(response)
    }
}
