//! Data read/write operations.

use inferadb_ledger_proto::proto;
use inferadb_ledger_types::{OrganizationSlug, VaultSlug};

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
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, ReadConsistency, VaultSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let (organization, vault) = (OrganizationSlug::new(1), VaultSlug::new(1));
    /// // Eventual consistency read (default)
    /// let value = client.read(organization, None, "user:123", None, None).await?;
    ///
    /// // Linearizable consistency read
    /// let value = client.read(organization, Some(vault), "key",
    ///     Some(ReadConsistency::Linearizable), None).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn read(
        &self,
        organization: OrganizationSlug,
        vault: Option<VaultSlug>,
        key: impl Into<String>,
        consistency: Option<ReadConsistency>,
        token: Option<tokio_util::sync::CancellationToken>,
    ) -> Result<Option<Vec<u8>>> {
        self.read_internal(
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
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, VaultSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let (organization, vault) = (OrganizationSlug::new(1), VaultSlug::new(1));
    /// let results = client.batch_read(
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
        organization: OrganizationSlug,
        vault: Option<VaultSlug>,
        keys: impl IntoIterator<Item = impl Into<String>>,
        consistency: Option<ReadConsistency>,
        token: Option<tokio_util::sync::CancellationToken>,
    ) -> Result<Vec<(String, Option<Vec<u8>>)>> {
        self.batch_read_internal(
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
                let mut client = crate::connected_client!(pool, create_read_client);

                let request = proto::ReadRequest {
                    organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                    vault: vault.map(|v| proto::VaultSlug { slug: v.value() }),
                    key: key.clone(),
                    consistency: consistency.to_proto() as i32,
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
                let mut client = crate::connected_client!(pool, create_read_client);

                let request = proto::BatchReadRequest {
                    organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                    vault: vault.map(|v| proto::VaultSlug { slug: v.value() }),
                    keys: keys.clone(),
                    consistency: consistency.to_proto() as i32,
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
    /// * `organization` - Organization slug (external identifier).
    /// * `vault` - Optional vault slug (required for relationships).
    /// * `operations` - The operations to apply atomically.
    /// * `token` - Optional per-request cancellation token.
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
    /// # use inferadb_ledger_sdk::{LedgerClient, Operation, OrganizationSlug, VaultSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let (organization, vault) = (OrganizationSlug::new(1), VaultSlug::new(1));
    /// let result = client.write(
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
        organization: OrganizationSlug,
        vault: Option<VaultSlug>,
        operations: Vec<Operation>,
        token: Option<tokio_util::sync::CancellationToken>,
    ) -> Result<WriteSuccess> {
        self.check_shutdown(token.as_ref())?;

        // Generate UUID idempotency key once for this request
        // The same key is reused across all retry attempts
        let idempotency_key = uuid::Uuid::new_v4();

        self.execute_write(organization, vault, &operations, idempotency_key, token.as_ref()).await
    }

    /// Executes a single write attempt with retry for transient errors.
    ///
    /// The idempotency key is preserved across retry attempts to ensure
    /// at-most-once semantics even with network failures.
    async fn execute_write(
        &self,
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
                    let mut write_client = crate::connected_client!(pool, create_write_client);

                    let request = proto::WriteRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        vault: vault.map(|v| proto::VaultSlug { slug: v.value() }),
                        client_id: Some(proto::ClientId { id: cid }),
                        idempotency_key: key_bytes,
                        operations: proto_ops,
                        include_tx_proof: false,
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

    // =========================================================================
    // Batch Write Operations
    // =========================================================================

    /// Submits a batch write transaction with all-or-nothing atomicity.
    ///
    /// A batch write groups multiple operation sets into a single atomic transaction.
    /// All operations are committed together in a single block, or none are applied
    /// if any operation fails (e.g., CAS condition failure).
    ///
    /// The batch uses a single idempotency key, meaning the entire batch is the
    /// deduplication unit - retry with the same idempotency key returns the
    /// original result.
    ///
    /// # Arguments
    ///
    /// * `organization` - Organization slug (external identifier).
    /// * `vault` - Optional vault slug (required for relationships).
    /// * `batches` - Groups of operations to apply atomically. Each inner `Vec<Operation>` is a
    ///   logical group processed in order.
    /// * `token` - Optional per-request cancellation token. If `None`, the client-level
    ///   cancellation token is used.
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
    /// - Any CAS condition fails (entire batch rolled back)
    /// - An idempotency key is reused with different payload
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
    /// # use inferadb_ledger_sdk::{LedgerClient, Operation, OrganizationSlug, VaultSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let (organization, vault) = (OrganizationSlug::new(1), VaultSlug::new(1));
    /// // Atomic transaction: create user AND grant permissions
    /// let result = client.batch_write(
    ///     organization,
    ///     Some(vault),
    ///     vec![
    ///         // First batch: create the user
    ///         vec![Operation::set_entity("user:123", b"alice".to_vec(), None, None)],
    ///         // Second batch: grant permissions (depends on user existing)
    ///         vec![
    ///             Operation::create_relationship("doc:456", "viewer", "user:123"),
    ///             Operation::create_relationship("folder:789", "editor", "user:123"),
    ///         ],
    ///     ],
    ///     None,
    /// ).await?;
    ///
    /// println!("Batch committed at block {} with sequence {}", result.block_height, result.assigned_sequence);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn batch_write(
        &self,
        organization: OrganizationSlug,
        vault: Option<VaultSlug>,
        batches: Vec<Vec<Operation>>,
        token: Option<tokio_util::sync::CancellationToken>,
    ) -> Result<WriteSuccess> {
        self.check_shutdown(token.as_ref())?;

        // Generate UUID idempotency key once for this request
        // The same key is reused across all retry attempts
        let idempotency_key = uuid::Uuid::new_v4();

        self.execute_batch_write(organization, vault, &batches, idempotency_key, token.as_ref())
            .await
    }

    /// Executes a single batch write attempt with retry for transient errors.
    ///
    /// The idempotency key is preserved across retry attempts to ensure
    /// at-most-once semantics even with network failures.
    async fn execute_batch_write(
        &self,
        organization: OrganizationSlug,
        vault: Option<VaultSlug>,
        batches: &[Vec<Operation>],
        idempotency_key: uuid::Uuid,
        request_token: Option<&tokio_util::sync::CancellationToken>,
    ) -> Result<WriteSuccess> {
        // Client-side validation: fast rejection before network round-trip
        let validation_config = self.config().validation();
        let total_ops: usize = batches.iter().map(|b| b.len()).sum();
        inferadb_ledger_types::validation::validate_operations_count(total_ops, validation_config)
            .map_err(|e| error::SdkError::Validation { message: e.to_string() })?;
        let mut total_bytes: usize = 0;
        for batch in batches {
            for op in batch {
                op.validate(validation_config)
                    .map_err(|e| error::SdkError::Validation { message: e.to_string() })?;
                total_bytes += op.estimated_size_bytes();
            }
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

        // Convert batches to proto BatchWriteOperation format
        let proto_batches: Vec<proto::BatchWriteOperation> = batches
            .iter()
            .map(|ops| proto::BatchWriteOperation {
                operations: ops.iter().map(Operation::to_proto).collect(),
            })
            .collect();

        // Convert UUID to bytes (16 bytes)
        let idempotency_key_bytes = idempotency_key.as_bytes().to_vec();

        // Execute with retry for transient errors
        self.with_metrics(
            "batch_write",
            with_retry_cancellable(&retry_policy, &token, Some(pool), "batch_write", || {
                let batch_ops = proto_batches.clone();
                let cid = client_id.clone();
                let key_bytes = idempotency_key_bytes.clone();
                async move {
                    let mut write_client = crate::connected_client!(pool, create_write_client);

                    let request = proto::BatchWriteRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        vault: vault.map(|v| proto::VaultSlug { slug: v.value() }),
                        client_id: Some(proto::ClientId { id: cid }),
                        idempotency_key: key_bytes,
                        operations: batch_ops,
                        include_tx_proofs: false,
                    };

                    let response =
                        write_client.batch_write(tonic::Request::new(request)).await?.into_inner();

                    Self::process_batch_write_response(response)
                }
            }),
        )
        .await
    }

    /// Processes a BatchWriteResponse and converts to Result<WriteSuccess>.
    fn process_batch_write_response(response: proto::BatchWriteResponse) -> Result<WriteSuccess> {
        match response.result {
            Some(proto::batch_write_response::Result::Success(success)) => Ok(WriteSuccess {
                tx_id: Self::tx_id_to_hex(success.tx_id),
                block_height: success.block_height,
                assigned_sequence: success.assigned_sequence,
            }),
            Some(proto::batch_write_response::Result::Error(error)) => {
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
            None => Err(missing_response_field("result", "BatchWriteResponse")),
        }
    }

    // =============================================================================
    // Single-Operation Convenience Methods
    // =============================================================================

    /// Writes a single entity (set), optionally with expiration and/or a
    /// condition.
    ///
    /// Convenience wrapper around [`write`](Self::write) for the common case of
    /// setting a single key-value pair. Generates an idempotency key
    /// automatically.
    ///
    /// # Arguments
    ///
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
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, VaultSlug, SetCondition};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let (organization, vault) = (OrganizationSlug::new(1), VaultSlug::new(1));
    /// // Simple set:
    /// client.set_entity(organization, Some(vault), "user:123", b"data".to_vec(), None, None, None).await?;
    ///
    /// // With expiration:
    /// client.set_entity(organization, Some(vault), "session:abc", b"token".to_vec(), Some(1700000000), None, None).await?;
    ///
    /// // Conditional (create-if-not-exists):
    /// client.set_entity(organization, Some(vault), "lock:xyz", b"owner".to_vec(), None, Some(SetCondition::NotExists), None).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn set_entity(
        &self,
        organization: OrganizationSlug,
        vault: Option<VaultSlug>,
        key: impl Into<String>,
        value: Vec<u8>,
        expires_at: Option<u64>,
        condition: Option<SetCondition>,
        token: Option<tokio_util::sync::CancellationToken>,
    ) -> Result<WriteSuccess> {
        self.write(
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
    /// key.
    ///
    /// # Arguments
    ///
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
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, VaultSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let (organization, vault) = (OrganizationSlug::new(1), VaultSlug::new(1));
    /// client.delete_entity(organization, Some(vault), "user:123", None).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn delete_entity(
        &self,
        organization: OrganizationSlug,
        vault: Option<VaultSlug>,
        key: impl Into<String>,
        token: Option<tokio_util::sync::CancellationToken>,
    ) -> Result<WriteSuccess> {
        self.write(organization, vault, vec![Operation::delete_entity(key)], token).await
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
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, VaultSlug};
    /// # use futures::StreamExt;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = LedgerClient::connect("http://localhost:50051", "my-app").await?;
    /// # let organization = OrganizationSlug::new(1);
    ///
    /// // Start watching from height 1
    /// let mut stream = client.watch_blocks(organization, VaultSlug::new(0), 1).await?;
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
        organization: OrganizationSlug,
        vault: VaultSlug,
        start_height: u64,
    ) -> Result<impl futures::Stream<Item = Result<BlockAnnouncement>>> {
        self.check_shutdown(None)?;

        // Get the initial stream
        let initial_stream =
            self.create_watch_blocks_stream(organization, vault, start_height).await?;

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
        };

        let response = client.watch_blocks(tonic::Request::new(request)).await?.into_inner();

        Ok(response)
    }
}
