//! Schema domain types for the SDK public API.

use serde::{Deserialize, Serialize};

/// A full schema version with its definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaVersion {
    /// The version number.
    pub version: u32,
    /// The schema definition as a JSON value.
    pub definition: serde_json::Value,
    /// Optional human-readable description.
    pub description: Option<String>,
}

/// Summary of a single schema version in a listing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaVersionSummary {
    /// The version number.
    pub version: u32,
    /// Whether a definition exists for this version.
    pub has_definition: bool,
    /// Whether this version is the currently active one.
    pub is_active: bool,
}

/// Result of deploying a schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaDeployResult {
    /// The version number that was deployed.
    pub version: u32,
}

/// A single field-level change in a schema diff.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaDiffChange {
    /// The field name that changed.
    pub field: String,
    /// The type of change: "added", "removed", or "changed".
    pub change_type: String,
}
