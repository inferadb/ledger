//! Fluent builder for single-vault write requests.
//!
//! Allows chaining multiple operations (entity sets/deletes, relationship
//! creates/deletes) into a single atomic write. Uses type-state to prevent
//! calling `.execute()` without at least one operation.
//!
//! # Condition modifiers
//!
//! The `.if_not_exists()`, `.if_exists()`, `.if_version()`, and
//! `.if_value_equals()` methods set a **pending condition** that is consumed
//! by the *next* `.set()` or `.set_with_expiry()` call.  Non-set operations
//! (`.delete()`, `.create_relationship()`, `.delete_relationship()`) pass
//! through without consuming the condition, so the condition attaches to the
//! next `.set()` regardless of intervening non-set operations.
//!
//! # Example
//!
//! ```no_run
//! # use inferadb_ledger_sdk::LedgerClient;
//! # async fn example(client: &LedgerClient) -> inferadb_ledger_sdk::Result<()> {
//! let result = client
//!     .write_builder(1, Some(1))
//!     .set("user:123", b"data".to_vec())
//!     .if_not_exists()
//!     .delete("old-key")
//!     .create_relationship("doc:1", "viewer", "user:123")
//!     .execute()
//!     .await?;
//!
//! println!("block: {}, tx: {}", result.block_height, result.tx_id);
//! # Ok(())
//! # }
//! ```

use std::marker::PhantomData;

use crate::{
    LedgerClient,
    client::{Operation, SetCondition, WriteSuccess},
    error::Result,
};

/// Type-state marker: builder has no operations yet.
pub struct NoOps(());

/// Type-state marker: builder has at least one operation.
pub struct HasOps(());

/// Fluent builder for constructing write requests.
///
/// Chain operations using `.set()`, `.delete()`, `.create_relationship()`, and
/// `.delete_relationship()`, then call `.execute()` to submit.
///
/// The type parameter `S` tracks whether operations have been added:
/// - `WriteBuilder<'_, NoOps>` — `.execute()` is not available
/// - `WriteBuilder<'_, HasOps>` — `.execute()` is available
///
/// This prevents submitting empty writes at compile time.
pub struct WriteBuilder<'a, S = NoOps> {
    client: &'a LedgerClient,
    namespace_id: i64,
    vault_id: Option<i64>,
    operations: Vec<Operation>,
    /// Condition to apply to the *next* `.set()` call.
    pending_condition: Option<SetCondition>,
    cancellation: Option<tokio_util::sync::CancellationToken>,
    _state: PhantomData<S>,
}

impl<'a> WriteBuilder<'a, NoOps> {
    /// Create a new write builder targeting a namespace and optional vault.
    pub(crate) fn new(client: &'a LedgerClient, namespace_id: i64, vault_id: Option<i64>) -> Self {
        Self {
            client,
            namespace_id,
            vault_id,
            operations: Vec::new(),
            pending_condition: None,
            cancellation: None,
            _state: PhantomData,
        }
    }
}

impl<'a, S> WriteBuilder<'a, S> {
    /// Transition to `HasOps` state, preserving all fields.
    fn into_has_ops(self) -> WriteBuilder<'a, HasOps> {
        WriteBuilder {
            client: self.client,
            namespace_id: self.namespace_id,
            vault_id: self.vault_id,
            operations: self.operations,
            pending_condition: self.pending_condition,
            cancellation: self.cancellation,
            _state: PhantomData,
        }
    }

    /// Set an entity value.
    ///
    /// If a condition was set via `.if_not_exists()`, `.if_exists()`,
    /// `.if_version()`, or `.if_value_equals()`, it is consumed by this call.
    pub fn set(mut self, key: impl Into<String>, value: Vec<u8>) -> WriteBuilder<'a, HasOps> {
        let condition = self.pending_condition.take();
        self.operations.push(Operation::SetEntity {
            key: key.into(),
            value,
            expires_at: None,
            condition,
        });
        self.into_has_ops()
    }

    /// Set an entity value with a TTL expiration.
    ///
    /// If a condition was previously set, it is consumed by this call.
    pub fn set_with_expiry(
        mut self,
        key: impl Into<String>,
        value: Vec<u8>,
        expires_at: u64,
    ) -> WriteBuilder<'a, HasOps> {
        let condition = self.pending_condition.take();
        self.operations.push(Operation::SetEntity {
            key: key.into(),
            value,
            expires_at: Some(expires_at),
            condition,
        });
        self.into_has_ops()
    }

    /// Delete an entity by key.
    pub fn delete(mut self, key: impl Into<String>) -> WriteBuilder<'a, HasOps> {
        self.operations.push(Operation::DeleteEntity { key: key.into() });
        self.into_has_ops()
    }

    /// Create an authorization relationship.
    pub fn create_relationship(
        mut self,
        resource: impl Into<String>,
        relation: impl Into<String>,
        subject: impl Into<String>,
    ) -> WriteBuilder<'a, HasOps> {
        self.operations.push(Operation::CreateRelationship {
            resource: resource.into(),
            relation: relation.into(),
            subject: subject.into(),
        });
        self.into_has_ops()
    }

    /// Delete an authorization relationship.
    pub fn delete_relationship(
        mut self,
        resource: impl Into<String>,
        relation: impl Into<String>,
        subject: impl Into<String>,
    ) -> WriteBuilder<'a, HasOps> {
        self.operations.push(Operation::DeleteRelationship {
            resource: resource.into(),
            relation: relation.into(),
            subject: subject.into(),
        });
        self.into_has_ops()
    }

    // -- Condition modifiers (apply to the next `.set()` call) --

    /// The next `.set()` call will only succeed if the key does not exist.
    pub fn if_not_exists(mut self) -> Self {
        self.pending_condition = Some(SetCondition::NotExists);
        self
    }

    /// The next `.set()` call will only succeed if the key already exists.
    pub fn if_exists(mut self) -> Self {
        self.pending_condition = Some(SetCondition::MustExist);
        self
    }

    /// The next `.set()` call will only succeed if the key's version matches.
    pub fn if_version(mut self, version: u64) -> Self {
        self.pending_condition = Some(SetCondition::Version(version));
        self
    }

    /// The next `.set()` call will only succeed if the key's current value
    /// matches exactly.
    pub fn if_value_equals(mut self, value: Vec<u8>) -> Self {
        self.pending_condition = Some(SetCondition::ValueEquals(value));
        self
    }

    /// Attach a cancellation token for per-request cancellation.
    ///
    /// Cancelling the token will abort the in-flight write. Without this,
    /// the client-level cancellation token is used.
    pub fn with_cancellation(mut self, token: tokio_util::sync::CancellationToken) -> Self {
        self.cancellation = Some(token);
        self
    }
}

impl<'a> WriteBuilder<'a, HasOps> {
    /// Submit the accumulated operations as a single atomic write.
    ///
    /// Uses the client's retry and metrics infrastructure. The write is
    /// idempotent — retries reuse the same server-generated idempotency key.
    ///
    /// # Errors
    ///
    /// Returns an error if the write RPC fails after retry attempts are
    /// exhausted, the client has been shut down, a CAS condition is not met,
    /// or the cancellation token is triggered.
    pub async fn execute(self) -> Result<WriteSuccess> {
        match self.cancellation {
            Some(token) => {
                self.client
                    .write_with_token(self.namespace_id, self.vault_id, self.operations, token)
                    .await
            },
            None => self.client.write(self.namespace_id, self.vault_id, self.operations).await,
        }
    }
}

/// Extract operations from a builder for testing purposes.
/// Available only in test builds.
#[cfg(test)]
impl<'a, S> WriteBuilder<'a, S> {
    fn into_operations(self) -> Vec<Operation> {
        self.operations
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]
mod tests {
    use super::{super::test_helpers::test_client, *};

    #[tokio::test]
    async fn write_builder_set_produces_correct_operation() {
        let client = test_client().await;
        let ops = client.write_builder(1, None).set("key1", b"val1".to_vec()).into_operations();
        assert_eq!(ops.len(), 1);
        assert!(matches!(
            &ops[0],
            Operation::SetEntity { key, value, expires_at: None, condition: None }
            if key == "key1" && value == b"val1"
        ));
    }

    #[tokio::test]
    async fn write_builder_set_with_expiry() {
        let client = test_client().await;
        let ops = client
            .write_builder(1, Some(1))
            .set_with_expiry("k", b"v".to_vec(), 9999)
            .into_operations();
        assert!(matches!(&ops[0], Operation::SetEntity { expires_at: Some(9999), .. }));
    }

    #[tokio::test]
    async fn write_builder_delete() {
        let client = test_client().await;
        let ops = client.write_builder(1, None).delete("gone").into_operations();
        assert!(matches!(
            &ops[0],
            Operation::DeleteEntity { key } if key == "gone"
        ));
    }

    #[tokio::test]
    async fn write_builder_create_relationship() {
        let client = test_client().await;
        let ops = client
            .write_builder(1, Some(1))
            .create_relationship("doc:1", "viewer", "user:2")
            .into_operations();
        assert!(matches!(
            &ops[0],
            Operation::CreateRelationship { resource, relation, subject }
            if resource == "doc:1" && relation == "viewer" && subject == "user:2"
        ));
    }

    #[tokio::test]
    async fn write_builder_delete_relationship() {
        let client = test_client().await;
        let ops = client
            .write_builder(1, Some(1))
            .delete_relationship("doc:1", "viewer", "user:2")
            .into_operations();
        assert!(matches!(
            &ops[0],
            Operation::DeleteRelationship { resource, relation, subject }
            if resource == "doc:1" && relation == "viewer" && subject == "user:2"
        ));
    }

    #[tokio::test]
    async fn write_builder_if_not_exists_applies_to_next_set() {
        let client = test_client().await;
        let ops = client
            .write_builder(1, None)
            .if_not_exists()
            .set("key", b"val".to_vec())
            .into_operations();
        assert!(matches!(
            &ops[0],
            Operation::SetEntity { condition: Some(SetCondition::NotExists), .. }
        ));
    }

    #[tokio::test]
    async fn write_builder_if_exists_applies_to_next_set() {
        let client = test_client().await;
        let ops =
            client.write_builder(1, None).if_exists().set("key", b"val".to_vec()).into_operations();
        assert!(matches!(
            &ops[0],
            Operation::SetEntity { condition: Some(SetCondition::MustExist), .. }
        ));
    }

    #[tokio::test]
    async fn write_builder_if_version_applies_to_next_set() {
        let client = test_client().await;
        let ops = client
            .write_builder(1, None)
            .if_version(42)
            .set("key", b"val".to_vec())
            .into_operations();
        assert!(matches!(
            &ops[0],
            Operation::SetEntity { condition: Some(SetCondition::Version(42)), .. }
        ));
    }

    #[tokio::test]
    async fn write_builder_if_value_equals_applies_to_next_set() {
        let client = test_client().await;
        let ops = client
            .write_builder(1, None)
            .if_value_equals(b"old".to_vec())
            .set("key", b"new".to_vec())
            .into_operations();
        assert!(matches!(
            &ops[0],
            Operation::SetEntity { condition: Some(SetCondition::ValueEquals(v)), .. }
            if v == b"old"
        ));
    }

    #[tokio::test]
    async fn write_builder_condition_consumed_after_set() {
        let client = test_client().await;
        let ops = client
            .write_builder(1, None)
            .if_not_exists()
            .set("key1", b"v1".to_vec())
            .set("key2", b"v2".to_vec())
            .into_operations();
        assert_eq!(ops.len(), 2);
        // First set has condition
        assert!(matches!(
            &ops[0],
            Operation::SetEntity { condition: Some(SetCondition::NotExists), .. }
        ));
        // Second set: condition was consumed by first
        assert!(matches!(&ops[1], Operation::SetEntity { condition: None, .. }));
    }

    #[tokio::test]
    async fn write_builder_condition_survives_non_set_ops() {
        let client = test_client().await;
        // Condition set, then delete (non-set), then set — condition attaches to the set
        let ops = client
            .write_builder(1, None)
            .if_not_exists()
            .delete("old-key")
            .set("new-key", b"val".to_vec())
            .into_operations();
        assert_eq!(ops.len(), 2);
        assert!(matches!(&ops[0], Operation::DeleteEntity { .. }));
        assert!(matches!(
            &ops[1],
            Operation::SetEntity { condition: Some(SetCondition::NotExists), .. }
        ));
    }

    #[tokio::test]
    async fn write_builder_chained_mixed_operations() {
        let client = test_client().await;
        let ops = client
            .write_builder(1, Some(1))
            .set("k1", b"v1".to_vec())
            .delete("k2")
            .create_relationship("doc:1", "editor", "user:1")
            .delete_relationship("doc:1", "viewer", "user:2")
            .set_with_expiry("session", b"token".to_vec(), 1000)
            .into_operations();
        assert_eq!(ops.len(), 5);
        assert!(matches!(&ops[0], Operation::SetEntity { .. }));
        assert!(matches!(&ops[1], Operation::DeleteEntity { .. }));
        assert!(matches!(&ops[2], Operation::CreateRelationship { .. }));
        assert!(matches!(&ops[3], Operation::DeleteRelationship { .. }));
        assert!(matches!(&ops[4], Operation::SetEntity { expires_at: Some(1000), .. }));
    }

    #[tokio::test]
    async fn write_builder_preserves_namespace_and_vault() {
        let client = test_client().await;
        let builder = client.write_builder(42, Some(99)).set("key", b"val".to_vec());
        assert_eq!(builder.namespace_id, 42);
        assert_eq!(builder.vault_id, Some(99));
    }

    #[tokio::test]
    async fn write_builder_vault_id_none() {
        let client = test_client().await;
        let builder = client.write_builder(1, None).set("key", b"val".to_vec());
        assert_eq!(builder.vault_id, None);
    }

    #[tokio::test]
    async fn write_builder_with_cancellation() {
        let client = test_client().await;
        let token = tokio_util::sync::CancellationToken::new();
        let builder =
            client.write_builder(1, None).with_cancellation(token).set("k", b"v".to_vec());
        assert!(builder.cancellation.is_some());
    }
}
