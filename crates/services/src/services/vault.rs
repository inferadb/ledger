//! Vault service implementation.
//!
//! Handles vault lifecycle: creation, deletion, retrieval, and listing.
//! Write operations (create, delete, update) flow through Raft for consistency;
//! read operations (get, list) hit the local applied state directly.
//!
//! Vault creation generates a Snowflake slug and initializes the vault's
//! blockchain with a genesis block via a single Raft entry.
//!
//! The wire-protocol implementation lives in [`super::vault_wire`]; this
//! module owns the [`VaultService`] type and its constructor only.

use super::service_infra::ServiceContext;

/// gRPC handler for vault lifecycle operations.
pub struct VaultService {
    pub(super) ctx: ServiceContext,
}

impl VaultService {
    /// Creates a new `VaultService` from shared service infrastructure.
    pub(crate) fn new(ctx: ServiceContext) -> Self {
        Self { ctx }
    }
}
