//! Fluent builder APIs for common SDK operations.
//!
//! These builders reduce boilerplate when constructing write requests,
//! batch reads, and relationship queries. They compose with the existing
//! retry, cancellation, and metrics infrastructure transparently.
//!
//! # Example
//!
//! ```no_run
//! # use inferadb_ledger_sdk::{LedgerClient, WriteBuilder};
//! # async fn example(client: &LedgerClient) -> inferadb_ledger_sdk::Result<()> {
//! let result = client
//!     .write_builder(1, Some(1))
//!     .set("user:123", b"alice".to_vec())
//!     .set_with_expiry("session:abc", b"token".to_vec(), 1700000000)
//!     .create_relationship("doc:1", "viewer", "user:123")
//!     .execute()
//!     .await?;
//! # Ok(())
//! # }
//! ```

mod read;
mod relationship;
mod write;

pub use read::BatchReadBuilder;
pub use relationship::RelationshipQueryBuilder;
pub use write::WriteBuilder;

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]
mod test_helpers {
    use crate::{ClientConfig, LedgerClient, ServerSource};

    /// Create a `LedgerClient` for builder tests.
    /// Connection to 127.0.0.1:1 won't succeed, but client construction
    /// does — we only test builder construction, not execution.
    pub(super) async fn test_client() -> LedgerClient {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://127.0.0.1:1"]))
            .client_id("test-builder")
            .build()
            .expect("valid config");
        LedgerClient::new(config).await.expect("client init")
    }
}
