//! Data read/write operations.

use inferadb_ledger_types::{OrganizationSlug, UserSlug, VaultSlug};

use crate::{
    LedgerClient,
    error::{self, Result},
    retry::with_retry_cancellable,
    types::{
        query::{Operation, SetCondition},
        read::{ReadConsistency, WriteSuccess},
    },
};

impl LedgerClient {
    // =========================================================================
    // Read Operations
    // =========================================================================

    /// Reads a value by key.
    ///
    /// Returns `Ok(Some(value))` when the key exists, or `Ok(None)` when it
    /// does not. Reads default to eventual consistency, served from any
    /// replica for lowest latency. Pass
    /// `Some(`[`ReadConsistency::Linearizable`]`)` to read from the leader
    /// for strong consistency.
    ///
    /// Omit `vault` for organization-level keys; supply it for vault-scoped
    /// keys. Pass a [`CancellationToken`](tokio_util::sync::CancellationToken)
    /// as `token` to cancel this specific read without shutting down the
    /// client.
    ///
    /// # Errors
    ///
    /// Returns [`crate::SdkError::RetryExhausted`] if the read fails after all
    /// retry attempts, [`crate::SdkError::Shutdown`] if the client has been
    /// shut down, or [`crate::SdkError::Cancelled`] if `token` is cancelled.
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
    /// // Linearizable read from the leader
    /// let value = client.read(
    ///     UserSlug::new(42),
    ///     organization,
    ///     Some(vault),
    ///     "key",
    ///     Some(ReadConsistency::Linearizable),
    ///     None,
    /// ).await?;
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

    /// Reads multiple keys in a single RPC call.
    ///
    /// Amortizes network overhead across multiple reads. Returns a
    /// `Vec<(key, Option<value>)>` in the same order as the input `keys`.
    /// Missing keys have `None` values. All reads share the same
    /// organization, vault, and consistency level.
    ///
    /// The server enforces a maximum batch size of 1000 keys.
    ///
    /// # Errors
    ///
    /// Returns [`crate::SdkError::RetryExhausted`] if the read fails after all
    /// retry attempts, [`crate::SdkError::Shutdown`] if the client has been
    /// shut down, or [`crate::SdkError::Cancelled`] if `token` is cancelled.
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
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::data::read(
                    wire_client,
                    request_id,
                    caller,
                    organization,
                    vault,
                    key.clone(),
                    consistency,
                )
                .await
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
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::data::batch_read(
                    wire_client,
                    request_id,
                    caller,
                    organization,
                    vault,
                    keys.clone(),
                    consistency,
                )
                .await
            }),
        )
        .await
    }

    // =========================================================================
    // Write Operations
    // =========================================================================

    /// Submits a write transaction to the ledger.
    ///
    /// All `operations` are applied atomically. Writes are automatically
    /// idempotent: the SDK generates a per-call idempotency key and preserves
    /// it across retries. If the server reports a duplicate commit,
    /// [`WriteSuccess`] from the original commit is returned transparently.
    ///
    /// Supply `vault` for vault-scoped operations (including relationships);
    /// omit it for organization-level entities.
    ///
    /// # Durability
    ///
    /// A successful response means the write is **WAL-durable**: the Raft log
    /// has fsynced the committed entry on every replica and it has been applied
    /// in-memory. State-DB materialization (B-tree checkpoint) lands within
    /// ~500 ms or immediately on graceful shutdown. See the
    /// [crate-level durability section](crate) for details.
    ///
    /// # Errors
    ///
    /// - [`crate::SdkError::RetryExhausted`] — connection failed after all retry attempts.
    /// - [`crate::SdkError::Rpc`] with `FailedPrecondition` — a [`SetCondition`] (CAS) check
    ///   failed; re-read and retry with the updated value.
    /// - [`crate::SdkError::Idempotency`] — the idempotency key was reused with a different
    ///   payload.
    /// - [`crate::SdkError::Shutdown`] — client has been shut down.
    /// - [`crate::SdkError::Cancelled`] — `token` was cancelled.
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
    /// println!(
    ///     "Committed at block {} with sequence {}",
    ///     result.block_height, result.assigned_sequence,
    /// );
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

        // UUID bytes (16 bytes) reused across every retry attempt so the
        // server-side idempotency dedupe sees the same key.
        let idempotency_key_array: [u8; 16] = *idempotency_key.as_bytes();

        // Keep an owned copy of the domain operations for the wire path —
        // each retry attempt clones them since the wire entrypoint
        // consumes the `Vec<Operation>`.
        let domain_operations: Vec<Operation> = operations.to_vec();

        // Execute with retry for transient errors
        self.with_metrics(
            "write",
            with_retry_cancellable(&retry_policy, &token, Some(pool), "write", || {
                let cid = client_id.clone();
                let domain_ops = domain_operations.clone();
                async move {
                    let wire_client = crate::connected_wire_client!(pool);
                    let request_id: u128 = rand::random();
                    crate::ops_wire::data::write(
                        wire_client,
                        request_id,
                        caller,
                        organization,
                        vault,
                        cid,
                        idempotency_key_array,
                        domain_ops,
                    )
                    .await
                }
            }),
        )
        .await
    }

    // =============================================================================
    // Single-Operation Convenience Methods
    // =============================================================================

    /// Writes a single entity (set), optionally with expiration and/or a
    /// condition.
    ///
    /// Convenience wrapper around [`write`](Self::write) for the common case of
    /// setting a single key-value pair. Generates an idempotency key
    /// automatically. Omit `vault` for organization-level entities; supply it
    /// for vault-scoped keys. Pass `expires_at` as a Unix timestamp (seconds)
    /// to make the entity expire automatically. Supply a [`SetCondition`] for
    /// compare-and-swap semantics. Pass a `token` to cancel the operation at
    /// the next retry boundary without shutting down the client. See
    /// [`write`](Self::write) for durability semantics.
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
    /// key. Omit `vault` for organization-level entities; supply it for
    /// vault-scoped keys. Pass a `token` to cancel the operation at the next
    /// retry boundary without shutting down the client. See
    /// [`write`](Self::write) for durability semantics.
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
}
