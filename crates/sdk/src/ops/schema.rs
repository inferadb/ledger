//! Schema management operations.
//!
//! Schemas are stored as JSON blobs in the vault's entity store using
//! well-known key prefixes:
//! - `schema:v{version}` — schema definition + description for a specific version
//! - `schema:current` — version pointer for the currently active version
//! - `schema:latest` — version pointer for the highest deployed version
//! - `schema:history` — JSON array of previously activated versions (for rollback)

use inferadb_ledger_types::{OrganizationSlug, UserSlug, VaultSlug};

// Re-export public types from types module
pub use crate::types::schema::{
    SchemaDeployResult, SchemaDiffChange, SchemaVersion, SchemaVersionSummary,
};
use crate::{LedgerClient, error::Result};

impl LedgerClient {
    /// Deploys a new schema version.
    ///
    /// Writes the schema definition to `schema:v{version}` and updates the
    /// `schema:latest` pointer. When `version` is `None`, auto-increments from
    /// the current latest version.
    ///
    /// # Errors
    ///
    /// Returns an error if the version is zero, serialization fails, or the
    /// underlying read/write operations fail.
    pub async fn deploy_schema(
        &self,
        caller: UserSlug,
        organization: OrganizationSlug,
        vault: VaultSlug,
        definition: serde_json::Value,
        version: Option<u32>,
        description: Option<String>,
    ) -> Result<SchemaDeployResult> {
        let wire_client = crate::connected_wire_client!(self.pool);
        let request_id_seed: u128 = rand::random();
        crate::ops_wire::schema::deploy_schema(
            wire_client,
            request_id_seed,
            caller,
            organization,
            vault,
            self.client_id().to_owned(),
            definition,
            version,
            description,
        )
        .await
    }

    /// Lists all schema versions for a vault.
    ///
    /// Reads the `schema:latest` pointer and checks each version from 1 to
    /// latest for existence. Also reads `schema:current` to mark the active
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying read operations fail.
    pub async fn list_schema_versions(
        &self,
        caller: UserSlug,
        organization: OrganizationSlug,
        vault: VaultSlug,
    ) -> Result<Vec<SchemaVersionSummary>> {
        let wire_client = crate::connected_wire_client!(self.pool);
        let request_id_seed: u128 = rand::random();
        crate::ops_wire::schema::list_schema_versions(
            wire_client,
            request_id_seed,
            caller,
            organization,
            vault,
        )
        .await
    }

    /// Gets a specific schema version.
    ///
    /// # Errors
    ///
    /// Returns an error if the version does not exist or the underlying read
    /// operation fails.
    pub async fn get_schema(
        &self,
        caller: UserSlug,
        organization: OrganizationSlug,
        vault: VaultSlug,
        version: u32,
    ) -> Result<SchemaVersion> {
        let wire_client = crate::connected_wire_client!(self.pool);
        let request_id: u128 = rand::random();
        crate::ops_wire::schema::get_schema(
            wire_client,
            request_id,
            caller,
            organization,
            vault,
            version,
        )
        .await
    }

    /// Activates a specific schema version.
    ///
    /// Verifies the version exists, pushes the current active version onto the
    /// history stack, and updates `schema:current` to point to the given
    /// version. Returns the version number that was activated.
    ///
    /// # Errors
    ///
    /// Returns an error if the version does not exist or the underlying
    /// read/write operations fail.
    pub async fn activate_schema(
        &self,
        caller: UserSlug,
        organization: OrganizationSlug,
        vault: VaultSlug,
        version: u32,
    ) -> Result<u32> {
        let wire_client = crate::connected_wire_client!(self.pool);
        let request_id_seed: u128 = rand::random();
        crate::ops_wire::schema::activate_schema(
            wire_client,
            request_id_seed,
            caller,
            organization,
            vault,
            self.client_id().to_owned(),
            version,
        )
        .await
    }

    /// Rolls back to the previous schema version.
    ///
    /// Reads the activation history, pops the last entry, and sets
    /// `schema:current` to that version. Returns the version number that was
    /// restored as active.
    ///
    /// # Errors
    ///
    /// Returns an error if there is no activation history to rollback to,
    /// or the underlying read/write operations fail.
    pub async fn rollback_schema(
        &self,
        caller: UserSlug,
        organization: OrganizationSlug,
        vault: VaultSlug,
    ) -> Result<u32> {
        let wire_client = crate::connected_wire_client!(self.pool);
        let request_id_seed: u128 = rand::random();
        crate::ops_wire::schema::rollback_schema(
            wire_client,
            request_id_seed,
            caller,
            organization,
            vault,
            self.client_id().to_owned(),
        )
        .await
    }

    /// Gets the currently active schema.
    ///
    /// Reads the `schema:current` pointer, then reads that version's definition.
    ///
    /// # Errors
    ///
    /// Returns an error if no schema is active, the active version's definition
    /// is missing, or the underlying read operations fail.
    pub async fn get_active_schema(
        &self,
        caller: UserSlug,
        organization: OrganizationSlug,
        vault: VaultSlug,
    ) -> Result<SchemaVersion> {
        let wire_client = crate::connected_wire_client!(self.pool);
        let request_id_seed: u128 = rand::random();
        crate::ops_wire::schema::get_active_schema(
            wire_client,
            request_id_seed,
            caller,
            organization,
            vault,
        )
        .await
    }

    /// Computes a flat key-level diff between two schema versions.
    ///
    /// # Errors
    ///
    /// Returns an error if either version does not exist or the underlying
    /// read operations fail.
    pub async fn diff_schemas(
        &self,
        caller: UserSlug,
        organization: OrganizationSlug,
        vault: VaultSlug,
        from: u32,
        to: u32,
    ) -> Result<Vec<SchemaDiffChange>> {
        let wire_client = crate::connected_wire_client!(self.pool);
        let request_id_seed: u128 = rand::random();
        crate::ops_wire::schema::diff_schemas(
            wire_client,
            request_id_seed,
            caller,
            organization,
            vault,
            from,
            to,
        )
        .await
    }
}
