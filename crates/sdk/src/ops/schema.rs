//! Schema management operations.
//!
//! Schemas are stored as JSON blobs in the vault's entity store using
//! well-known key prefixes:
//! - `schema:v{version}` — schema definition + description for a specific version
//! - `schema:current` — version pointer for the currently active version
//! - `schema:latest` — version pointer for the highest deployed version
//! - `schema:history` — JSON array of previously activated versions (for rollback)

use inferadb_ledger_types::{OrganizationSlug, VaultSlug};
use serde::{Deserialize, Serialize};

use crate::{
    LedgerClient, Operation,
    error::{Result, SdkError},
};

// ── Internal Storage Types ──────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredSchema {
    definition: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct VersionPointer {
    version: u32,
}

// Re-export public types from types module
pub use crate::types::schema::{
    SchemaDeployResult, SchemaDiffChange, SchemaVersion, SchemaVersionSummary,
};

// ── Key Helpers ─────────────────────────────────────────────────────

fn schema_version_key(version: u32) -> String {
    format!("schema:v{version}")
}

const SCHEMA_CURRENT_KEY: &str = "schema:current";
const SCHEMA_LATEST_KEY: &str = "schema:latest";
const SCHEMA_HISTORY_KEY: &str = "schema:history";

// ── LedgerClient Implementation ────────────────────────────────────

impl LedgerClient {
    /// Deploys a new schema version.
    ///
    /// Writes the schema definition to `schema:v{version}` and updates the
    /// `schema:latest` pointer. If `version` is `None`, auto-increments from
    /// the current latest version.
    ///
    /// # Errors
    ///
    /// Returns an error if the version is zero, serialization fails, or the
    /// underlying read/write operations fail.
    pub async fn deploy_schema(
        &self,
        organization: OrganizationSlug,
        vault: VaultSlug,
        definition: serde_json::Value,
        version: Option<u32>,
        description: Option<String>,
    ) -> Result<SchemaDeployResult> {
        let version = match version {
            Some(v) => {
                if v == 0 {
                    return Err(SdkError::Validation {
                        message: "schema version must be greater than 0".to_string(),
                    });
                }
                v
            },
            None => {
                let latest =
                    self.read_version_pointer(organization, vault, SCHEMA_LATEST_KEY).await?;
                latest.map_or(1, |p| p.version + 1)
            },
        };

        let stored = StoredSchema { definition, description };
        let bytes = serde_json::to_vec(&stored).map_err(|e| SdkError::Validation {
            message: format!("failed to serialize schema: {e}"),
        })?;

        let latest_pointer = VersionPointer { version };
        let latest_bytes = serde_json::to_vec(&latest_pointer).map_err(|e| {
            SdkError::Validation { message: format!("failed to serialize version pointer: {e}") }
        })?;

        let key = schema_version_key(version);
        let ops = vec![
            Operation::set_entity(&key, bytes, None, None),
            Operation::set_entity(SCHEMA_LATEST_KEY, latest_bytes, None, None),
        ];
        self.write(organization, Some(vault), ops, None).await?;

        Ok(SchemaDeployResult { version })
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
        organization: OrganizationSlug,
        vault: VaultSlug,
    ) -> Result<Vec<SchemaVersionSummary>> {
        let latest = self.read_version_pointer(organization, vault, SCHEMA_LATEST_KEY).await?;

        let max_version = match latest {
            Some(p) => p.version,
            None => return Ok(vec![]),
        };

        let current = self.read_version_pointer(organization, vault, SCHEMA_CURRENT_KEY).await?;
        let active_version = current.map(|p| p.version);

        let mut schemas = Vec::with_capacity(max_version as usize);
        for v in 1..=max_version {
            let key = schema_version_key(v);
            let exists = self.read(organization, Some(vault), &key, None, None).await?.is_some();
            schemas.push(SchemaVersionSummary {
                version: v,
                has_definition: exists,
                is_active: active_version == Some(v),
            });
        }

        Ok(schemas)
    }

    /// Gets a specific schema version.
    ///
    /// # Errors
    ///
    /// Returns an error if the version does not exist or the underlying read
    /// operation fails.
    pub async fn get_schema(
        &self,
        organization: OrganizationSlug,
        vault: VaultSlug,
        version: u32,
    ) -> Result<SchemaVersion> {
        let stored =
            self.read_stored_schema(organization, vault, version).await?.ok_or_else(|| {
                SdkError::Rpc {
                    code: tonic::Code::NotFound,
                    message: format!("schema version {version} not found"),
                    request_id: None,
                    trace_id: None,
                    error_details: None,
                }
            })?;

        Ok(SchemaVersion {
            version,
            definition: stored.definition,
            description: stored.description,
        })
    }

    /// Activates a specific schema version.
    ///
    /// Verifies the version exists, pushes the current active version onto the
    /// history stack, and updates `schema:current` to point to the given version.
    ///
    /// # Returns
    ///
    /// The version number that was activated.
    ///
    /// # Errors
    ///
    /// Returns an error if the version does not exist or the underlying
    /// read/write operations fail.
    pub async fn activate_schema(
        &self,
        organization: OrganizationSlug,
        vault: VaultSlug,
        version: u32,
    ) -> Result<u32> {
        // Verify the version exists
        let key = schema_version_key(version);
        let exists = self.read(organization, Some(vault), &key, None, None).await?.is_some();
        if !exists {
            return Err(SdkError::Rpc {
                code: tonic::Code::NotFound,
                message: format!("schema version {version} not found"),
                request_id: None,
                trace_id: None,
                error_details: None,
            });
        }

        // Read current active version and history
        let current = self.read_version_pointer(organization, vault, SCHEMA_CURRENT_KEY).await?;
        let mut history = self.read_history(organization, vault).await?;

        // Push current active version onto history (if one exists)
        if let Some(current_pointer) = current {
            history.push(current_pointer.version);
        }

        let pointer = VersionPointer { version };
        let pointer_bytes = serde_json::to_vec(&pointer).map_err(|e| SdkError::Validation {
            message: format!("failed to serialize version pointer: {e}"),
        })?;
        let history_bytes = serde_json::to_vec(&history).map_err(|e| SdkError::Validation {
            message: format!("failed to serialize history: {e}"),
        })?;

        let ops = vec![
            Operation::set_entity(SCHEMA_CURRENT_KEY, pointer_bytes, None, None),
            Operation::set_entity(SCHEMA_HISTORY_KEY, history_bytes, None, None),
        ];
        self.write(organization, Some(vault), ops, None).await?;

        Ok(version)
    }

    /// Rolls back to the previous schema version.
    ///
    /// Reads the activation history, pops the last entry, and sets
    /// `schema:current` to that version.
    ///
    /// # Returns
    ///
    /// The version number that was restored as active.
    ///
    /// # Errors
    ///
    /// Returns an error if there is no activation history to rollback to,
    /// or the underlying read/write operations fail.
    pub async fn rollback_schema(
        &self,
        organization: OrganizationSlug,
        vault: VaultSlug,
    ) -> Result<u32> {
        let mut history = self.read_history(organization, vault).await?;

        let previous = history.pop().ok_or_else(|| SdkError::Validation {
            message: "no activation history to rollback to".to_string(),
        })?;

        let pointer = VersionPointer { version: previous };
        let pointer_bytes = serde_json::to_vec(&pointer).map_err(|e| SdkError::Validation {
            message: format!("failed to serialize version pointer: {e}"),
        })?;
        let history_bytes = serde_json::to_vec(&history).map_err(|e| SdkError::Validation {
            message: format!("failed to serialize history: {e}"),
        })?;

        let ops = vec![
            Operation::set_entity(SCHEMA_CURRENT_KEY, pointer_bytes, None, None),
            Operation::set_entity(SCHEMA_HISTORY_KEY, history_bytes, None, None),
        ];
        self.write(organization, Some(vault), ops, None).await?;

        Ok(previous)
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
        organization: OrganizationSlug,
        vault: VaultSlug,
    ) -> Result<SchemaVersion> {
        let current = self
            .read_version_pointer(organization, vault, SCHEMA_CURRENT_KEY)
            .await?
            .ok_or_else(|| SdkError::Rpc {
                code: tonic::Code::NotFound,
                message: "no active schema version".to_string(),
                request_id: None,
                trace_id: None,
                error_details: None,
            })?;

        let stored = self
            .read_stored_schema(organization, vault, current.version)
            .await?
            .ok_or_else(|| SdkError::Rpc {
                code: tonic::Code::NotFound,
                message: format!("active schema version {} has no definition", current.version),
                request_id: None,
                trace_id: None,
                error_details: None,
            })?;

        Ok(SchemaVersion {
            version: current.version,
            definition: stored.definition,
            description: stored.description,
        })
    }

    /// Computes a flat key-level diff between two schema versions.
    ///
    /// # Errors
    ///
    /// Returns an error if either version does not exist or the underlying
    /// read operations fail.
    pub async fn diff_schemas(
        &self,
        organization: OrganizationSlug,
        vault: VaultSlug,
        from: u32,
        to: u32,
    ) -> Result<Vec<SchemaDiffChange>> {
        let from_schema =
            self.read_stored_schema(organization, vault, from).await?.ok_or_else(|| {
                SdkError::Rpc {
                    code: tonic::Code::NotFound,
                    message: format!("schema version {from} not found"),
                    request_id: None,
                    trace_id: None,
                    error_details: None,
                }
            })?;

        let to_schema =
            self.read_stored_schema(organization, vault, to).await?.ok_or_else(|| {
                SdkError::Rpc {
                    code: tonic::Code::NotFound,
                    message: format!("schema version {to} not found"),
                    request_id: None,
                    trace_id: None,
                    error_details: None,
                }
            })?;

        Ok(diff_json_objects(&from_schema.definition, &to_schema.definition))
    }

    // ── Private Helpers ─────────────────────────────────────────────

    async fn read_version_pointer(
        &self,
        organization: OrganizationSlug,
        vault: VaultSlug,
        key: &str,
    ) -> Result<Option<VersionPointer>> {
        let data = self.read(organization, Some(vault), key, None, None).await?;
        match data {
            Some(bytes) => {
                let pointer: VersionPointer =
                    serde_json::from_slice(&bytes).map_err(|e| SdkError::Validation {
                        message: format!("corrupt version pointer at {key}: {e}"),
                    })?;
                Ok(Some(pointer))
            },
            None => Ok(None),
        }
    }

    async fn read_stored_schema(
        &self,
        organization: OrganizationSlug,
        vault: VaultSlug,
        version: u32,
    ) -> Result<Option<StoredSchema>> {
        let key = schema_version_key(version);
        let data = self.read(organization, Some(vault), &key, None, None).await?;
        match data {
            Some(bytes) => {
                let schema: StoredSchema = serde_json::from_slice(&bytes).map_err(|e| {
                    SdkError::Validation { message: format!("corrupt schema at {key}: {e}") }
                })?;
                Ok(Some(schema))
            },
            None => Ok(None),
        }
    }

    async fn read_history(
        &self,
        organization: OrganizationSlug,
        vault: VaultSlug,
    ) -> Result<Vec<u32>> {
        let data = self.read(organization, Some(vault), SCHEMA_HISTORY_KEY, None, None).await?;
        match data {
            Some(bytes) => {
                let history: Vec<u32> = serde_json::from_slice(&bytes).map_err(|e| {
                    SdkError::Validation { message: format!("corrupt schema history: {e}") }
                })?;
                Ok(history)
            },
            None => Ok(vec![]),
        }
    }
}

/// Computes flat key-level differences between two JSON objects.
fn diff_json_objects(from: &serde_json::Value, to: &serde_json::Value) -> Vec<SchemaDiffChange> {
    let empty = serde_json::Map::new();
    let from_obj = from.as_object().unwrap_or(&empty);
    let to_obj = to.as_object().unwrap_or(&empty);

    let mut changes = Vec::new();

    for key in from_obj.keys() {
        if !to_obj.contains_key(key) {
            changes
                .push(SchemaDiffChange { field: key.clone(), change_type: "removed".to_string() });
        } else if from_obj.get(key) != to_obj.get(key) {
            changes
                .push(SchemaDiffChange { field: key.clone(), change_type: "changed".to_string() });
        }
    }

    for key in to_obj.keys() {
        if !from_obj.contains_key(key) {
            changes.push(SchemaDiffChange { field: key.clone(), change_type: "added".to_string() });
        }
    }

    changes.sort_by(|a, b| a.field.cmp(&b.field));
    changes
}
