//! Region Master Key management configuration.
//!
//! Defines the configuration types for the key management hierarchy:
//! each region has its own versioned RMK loaded from an external source
//! (secrets manager, environment variables, or local files).

use std::{collections::HashMap, path::PathBuf};

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::types::Region;

/// Status of a Region Master Key version.
///
/// RMK versions progress through this lifecycle:
/// `Active` → `Deprecated` → `Decommissioned`.
///
/// Only `Active` versions are used for new writes.
/// `Deprecated` versions can still decrypt existing artifacts.
/// `Decommissioned` versions are no longer loadable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum RmkStatus {
    /// Currently used for new writes. Multiple active versions may
    /// exist briefly during rotation (highest version wins).
    Active,
    /// No longer used for new writes but still loadable for reading
    /// artifacts encrypted with this version's DEKs.
    Deprecated,
    /// Permanently unloadable. All artifacts must have been re-wrapped
    /// to a newer version before decommissioning.
    Decommissioned,
}

/// Metadata about a single RMK version (no key material).
///
/// Returned by `RegionKeyManager::list_versions()` for operational
/// visibility without exposing sensitive key bytes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RmkVersionInfo {
    /// Region this key belongs to.
    pub region: Region,
    /// Monotonically increasing version number.
    pub version: u32,
    /// Current lifecycle status.
    pub status: RmkStatus,
    /// When this version was created.
    pub created_at: DateTime<Utc>,
}

/// Key manager backend selection.
///
/// Determines how Region Master Keys are loaded at runtime.
/// The backend hierarchy: `SecretsManager` for production,
/// `Env` for staging/CI, `File` for local development only.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type")]
pub enum KeyManagerBackend {
    /// Load RMKs from an external secrets manager (production).
    SecretsManager(SecretsManagerConfig),
    /// Load RMKs from environment variables (staging/CI).
    ///
    /// Variable naming: `LEDGER_RMK_{REGION}_V{VERSION}`, hex-encoded
    /// 32 bytes. Example: `LEDGER_RMK_US_EAST_VA_V1=aabb...ff`.
    Env,
    /// Load RMKs from versioned files (local development only).
    ///
    /// Path pattern: `{key_dir}/{region}/v{version}.key`, raw 32 bytes.
    /// Emits a warning in release builds.
    File {
        /// Directory containing region key subdirectories.
        key_dir: PathBuf,
    },
}

/// Secrets manager configuration for production RMK loading.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SecretsManagerConfig {
    /// Secrets manager provider.
    pub provider: SecretsProvider,
    /// Provider endpoint URL.
    pub endpoint: String,
    /// Project or account identifier.
    pub project_id: String,
    /// Environment name (e.g., "production", "staging").
    pub environment: String,
    /// Mapping of region → secret path in the provider.
    ///
    /// Each region's RMK is stored under its own secret path.
    /// The provider handles multi-version storage natively.
    pub region_secret_paths: HashMap<Region, String>,
}

/// Supported secrets manager providers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum SecretsProvider {
    /// Infisical (self-hostable, cloud-agnostic). Reference implementation.
    Infisical,
    /// HashiCorp Vault.
    Vault,
    /// AWS Key Management Service.
    AwsKms,
    /// Google Cloud Key Management Service.
    GcpKms,
    /// Azure Key Vault.
    AzureKeyVault,
}
