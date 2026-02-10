//! Fluent builder for batch read requests.
//!
//! Provides a chainable API for constructing multi-key read requests with
//! optional per-key configuration. Uses type-state to prevent executing
//! an empty read.
//!
//! # Example
//!
//! ```no_run
//! # use inferadb_ledger_sdk::LedgerClient;
//! # async fn example(client: &LedgerClient) -> inferadb_ledger_sdk::Result<()> {
//! let results = client
//!     .batch_read_builder(1, Some(1))
//!     .key("user:123")
//!     .key("user:456")
//!     .keys(["session:a", "session:b"])
//!     .linearizable()
//!     .execute()
//!     .await?;
//!
//! for (key, value) in &results {
//!     println!("{key}: {:?}", value);
//! }
//! # Ok(())
//! # }
//! ```

use std::marker::PhantomData;

use crate::{LedgerClient, client::ReadConsistency, error::Result};

/// Type-state marker: builder has no keys yet.
pub struct NoKeys(());

/// Type-state marker: builder has at least one key.
pub struct HasKeys(());

/// Fluent builder for batch read requests.
///
/// Add keys using `.key()` or `.keys()`, optionally set consistency level,
/// then call `.execute()` to submit.
///
/// The type parameter `S` tracks whether keys have been added:
/// - `BatchReadBuilder<'_, NoKeys>` — `.execute()` is not available
/// - `BatchReadBuilder<'_, HasKeys>` — `.execute()` is available
///
/// Only `.key()` (single key) transitions the type-state to `HasKeys`.
/// `.keys()` extends the key list without transitioning, so it can be
/// used on either state to add more keys from an iterator. Use
/// `.key(first).keys(rest)` to combine an iterator with the type-state
/// guarantee.
pub struct BatchReadBuilder<'a, S = NoKeys> {
    client: &'a LedgerClient,
    namespace_id: i64,
    vault_id: Option<i64>,
    keys: Vec<String>,
    consistency: ReadConsistency,
    cancellation: Option<tokio_util::sync::CancellationToken>,
    _state: PhantomData<S>,
}

impl<'a> BatchReadBuilder<'a, NoKeys> {
    /// Create a new batch read builder targeting a namespace and optional vault.
    pub(crate) fn new(client: &'a LedgerClient, namespace_id: i64, vault_id: Option<i64>) -> Self {
        Self {
            client,
            namespace_id,
            vault_id,
            keys: Vec::new(),
            consistency: ReadConsistency::Eventual,
            cancellation: None,
            _state: PhantomData,
        }
    }
}

impl<'a, S> BatchReadBuilder<'a, S> {
    /// Transition to `HasKeys` state, preserving all fields.
    fn into_has_keys(self) -> BatchReadBuilder<'a, HasKeys> {
        BatchReadBuilder {
            client: self.client,
            namespace_id: self.namespace_id,
            vault_id: self.vault_id,
            keys: self.keys,
            consistency: self.consistency,
            cancellation: self.cancellation,
            _state: PhantomData,
        }
    }

    /// Add a single key to read. This transitions the builder to `HasKeys`,
    /// guaranteeing at least one key is present for `.execute()`.
    pub fn key(mut self, key: impl Into<String>) -> BatchReadBuilder<'a, HasKeys> {
        self.keys.push(key.into());
        self.into_has_keys()
    }

    /// Add multiple keys to read from an iterator.
    ///
    /// This does **not** transition the type-state (an empty iterator adds
    /// no keys). Use `.key(first).keys(rest)` to guarantee at least one key.
    pub fn keys(mut self, keys: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.keys.extend(keys.into_iter().map(Into::into));
        self
    }

    /// Use linearizable (strong) consistency for the read.
    ///
    /// Reads from the leader to guarantee the latest committed value.
    /// Has higher latency than the default eventual consistency.
    pub fn linearizable(mut self) -> Self {
        self.consistency = ReadConsistency::Linearizable;
        self
    }

    /// Use eventual consistency for the read (default).
    pub fn eventual(mut self) -> Self {
        self.consistency = ReadConsistency::Eventual;
        self
    }

    /// Attach a cancellation token for per-request cancellation.
    ///
    /// Cancelling the token will abort the in-flight request. Without this,
    /// the client-level cancellation token is used.
    pub fn with_cancellation(mut self, token: tokio_util::sync::CancellationToken) -> Self {
        self.cancellation = Some(token);
        self
    }
}

impl<'a> BatchReadBuilder<'a, HasKeys> {
    /// Execute the batch read and return results.
    ///
    /// Returns a vector of `(key, Option<value>)` pairs in the same order as
    /// the keys were added. Missing keys have `None` values.
    ///
    /// # Errors
    ///
    /// Returns an error if the read RPC fails after retry attempts are
    /// exhausted, the client has been shut down, or the cancellation token
    /// is triggered.
    pub async fn execute(self) -> Result<Vec<(String, Option<Vec<u8>>)>> {
        match (&self.consistency, &self.cancellation) {
            (ReadConsistency::Eventual, Some(token)) => {
                self.client
                    .batch_read_with_token(
                        self.namespace_id,
                        self.vault_id,
                        self.keys,
                        token.clone(),
                    )
                    .await
            },
            (ReadConsistency::Eventual, None) => {
                self.client.batch_read(self.namespace_id, self.vault_id, self.keys).await
            },
            (ReadConsistency::Linearizable, _) => {
                // batch_read_consistent doesn't have a _with_token variant;
                // client-level cancellation still applies.
                self.client.batch_read_consistent(self.namespace_id, self.vault_id, self.keys).await
            },
        }
    }
}

#[cfg(test)]
impl<'a, S> BatchReadBuilder<'a, S> {
    fn collected_keys(&self) -> &[String] {
        &self.keys
    }

    fn collected_consistency(&self) -> ReadConsistency {
        self.consistency
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]
mod tests {
    use super::{super::test_helpers::test_client, *};

    #[tokio::test]
    async fn batch_read_single_key() {
        let client = test_client().await;
        let builder = client.batch_read_builder(1, None).key("user:123");
        assert_eq!(builder.collected_keys(), &["user:123"]);
    }

    #[tokio::test]
    async fn batch_read_multiple_keys() {
        let client = test_client().await;
        let builder = client.batch_read_builder(1, Some(1)).key("k1").key("k2").key("k3");
        assert_eq!(builder.collected_keys(), &["k1", "k2", "k3"]);
    }

    #[tokio::test]
    async fn batch_read_keys_from_iterator() {
        let client = test_client().await;
        // .keys() alone doesn't transition to HasKeys — need .key() first
        let builder = client.batch_read_builder(1, None).key("first").keys(["a", "b", "c"]);
        assert_eq!(builder.collected_keys(), &["first", "a", "b", "c"]);
    }

    #[tokio::test]
    async fn batch_read_mixed_key_and_keys() {
        let client = test_client().await;
        let builder =
            client.batch_read_builder(1, None).key("first").keys(["second", "third"]).key("fourth");
        assert_eq!(builder.collected_keys(), &["first", "second", "third", "fourth"]);
    }

    #[tokio::test]
    async fn batch_read_default_consistency_is_eventual() {
        let client = test_client().await;
        let builder = client.batch_read_builder(1, None).key("k");
        assert!(matches!(builder.collected_consistency(), ReadConsistency::Eventual));
    }

    #[tokio::test]
    async fn batch_read_linearizable_consistency() {
        let client = test_client().await;
        let builder = client.batch_read_builder(1, None).linearizable().key("k");
        assert!(matches!(builder.collected_consistency(), ReadConsistency::Linearizable));
    }

    #[tokio::test]
    async fn batch_read_eventual_override() {
        let client = test_client().await;
        let builder = client.batch_read_builder(1, None).linearizable().eventual().key("k");
        assert!(matches!(builder.collected_consistency(), ReadConsistency::Eventual));
    }

    #[tokio::test]
    async fn batch_read_preserves_namespace_and_vault() {
        let client = test_client().await;
        let builder = client.batch_read_builder(42, Some(99)).key("k");
        assert_eq!(builder.namespace_id, 42);
        assert_eq!(builder.vault_id, Some(99));
    }

    #[tokio::test]
    async fn batch_read_string_keys() {
        let client = test_client().await;
        let owned = String::from("owned-key");
        let builder = client.batch_read_builder(1, None).key(owned).key("borrowed-key");
        assert_eq!(builder.collected_keys(), &["owned-key", "borrowed-key"]);
    }

    #[tokio::test]
    async fn batch_read_keys_does_not_transition_state() {
        let client = test_client().await;
        // .keys() on NoKeys returns NoKeys — no .execute() available
        let builder: BatchReadBuilder<'_, NoKeys> =
            client.batch_read_builder(1, None).keys(["a", "b"]);
        assert_eq!(builder.collected_keys(), &["a", "b"]);
        // Can still add a .key() to get HasKeys
        let builder = builder.key("c");
        assert_eq!(builder.collected_keys(), &["a", "b", "c"]);
    }

    #[tokio::test]
    async fn batch_read_with_cancellation() {
        let client = test_client().await;
        let token = tokio_util::sync::CancellationToken::new();
        let builder = client.batch_read_builder(1, None).with_cancellation(token).key("k");
        assert!(builder.cancellation.is_some());
    }
}
