//! Region Master Key management: trait, backends, and provisioning validation.
//!
//! The [`RegionKeyManager`] trait abstracts RMK loading. Implementations
//! fetch key material from external sources (files, env vars, secrets
//! managers). The loaded keys populate the [`RmkCache`](super::cache::RmkCache)
//! in the [`EncryptedBackend`](super::backend::EncryptedBackend).
//!
//! The [`SecretsClient`] trait enables pluggable secrets manager backends
//! for production deployments (Infisical, Vault, AWS KMS, etc.).

use std::{
    collections::HashMap,
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    sync::RwLock,
};

use inferadb_ledger_types::{
    config::{RmkStatus, RmkVersionInfo, SecretsManagerConfig},
    types::{ALL_REGIONS, Region},
};
use serde::{Deserialize, Serialize};

use super::types::RegionMasterKey;
use crate::error::{Error, Result};

/// Trait for loading Region Master Keys from external sources.
///
/// Implementations are responsible for:
/// - Loading key material from their backend (file, env, secrets manager)
/// - Supporting multiple versions per region
/// - Providing health checks for operational monitoring
///
/// All methods are synchronous. For async backends (secrets managers),
/// implementations should use blocking calls or pre-load at startup.
pub trait RegionKeyManager: Send + Sync {
    /// Returns the current (highest-version, active) RMK for a region.
    fn current_rmk(&self, region: Region) -> Result<RegionMasterKey>;

    /// Returns a specific RMK version for a region.
    ///
    /// Used during reads when a page's `rmk_version` differs from current.
    fn rmk_by_version(&self, region: Region, version: u32) -> Result<RegionMasterKey>;

    /// Lists all known versions for a region.
    fn list_versions(&self, region: Region) -> Result<Vec<RmkVersionInfo>>;

    /// Generates a new RMK version for a region.
    ///
    /// Returns the new version number. Old versions remain loadable.
    fn rotate_rmk(&self, region: Region) -> Result<u32>;

    /// Marks a version as decommissioned (no longer loadable).
    ///
    /// Fails if any artifacts still reference this version.
    fn decommission_rmk(&self, region: Region, version: u32) -> Result<()>;

    /// Verifies the current RMK is loadable and functional.
    ///
    /// Performs a test wrap/unwrap cycle. Called by health checks.
    fn health_check(&self, region: Region) -> Result<()>;
}

/// Returns the set of regions a node must hold RMKs for.
///
/// A node holds RMKs for:
/// - All non-protected regions (`!requires_residency()`: GLOBAL, US_EAST_VA, US_WEST_OR)
/// - Its own region if it requires residency
///
/// Deduplicated — if `node_region` is non-protected, it's already included.
pub fn required_regions(node_region: Region) -> Vec<Region> {
    let mut regions: Vec<Region> =
        ALL_REGIONS.iter().filter(|r| !r.requires_residency()).copied().collect();
    if node_region.requires_residency() {
        regions.push(node_region);
    }
    regions
}

/// Validates that a node has all required RMK versions provisioned.
///
/// For each region in `required_regions(node_region)`, checks that:
/// - At least one active version exists
/// - All active and deprecated versions are loadable
///
/// Returns an error naming the specific missing version if validation
/// fails. Called during startup to gate Raft group membership.
pub fn validate_rmk_provisioning(
    manager: &dyn RegionKeyManager,
    node_region: Region,
) -> Result<()> {
    let regions = required_regions(node_region);

    for region in regions {
        let versions = manager.list_versions(region).map_err(|_| Error::Encryption {
            reason: format!("Failed to list RMK versions for region {region}"),
        })?;

        if versions.is_empty() {
            return Err(Error::Encryption {
                reason: format!(
                    "No RMK versions provisioned for required region {region}. \
                     Node region {node_region} requires keys for this region."
                ),
            });
        }

        let has_active = versions.iter().any(|v| v.status == RmkStatus::Active);
        if !has_active {
            return Err(Error::Encryption {
                reason: format!(
                    "No active RMK version for required region {region}. \
                     Found {} version(s) but none are active.",
                    versions.len()
                ),
            });
        }

        // Verify all non-decommissioned versions are actually loadable
        for info in &versions {
            if info.status == RmkStatus::Decommissioned {
                continue;
            }
            if let Err(e) = manager.rmk_by_version(region, info.version) {
                return Err(Error::Encryption {
                    reason: format!(
                        "RMK v{} for region {region} is listed as {:?} but failed to load: {e}. \
                         Provision this key version before joining the cluster.",
                        info.version, info.status
                    ),
                });
            }
        }
    }

    Ok(())
}

/// Returns loaded RMK version info per region for health reporting.
///
/// For each region in `required_regions(node_region)`, calls `list_versions()`
/// and collects the results. Used by `DependencyHealthChecker` to expose
/// loaded key versions for rotation coordination.
pub fn rmk_versions_for_health(
    manager: &dyn RegionKeyManager,
    node_region: Region,
) -> HashMap<Region, Vec<RmkVersionInfo>> {
    let regions = required_regions(node_region);
    let mut result = HashMap::new();

    for region in regions {
        match manager.list_versions(region) {
            Ok(versions) => {
                result.insert(region, versions);
            },
            Err(_) => {
                result.insert(region, Vec::new());
            },
        }
    }

    result
}

// ─── VersionSidecar ─────────────────────────────────────────────

/// A single entry in the `versions.json` sidecar file.
///
/// Tracks version number and lifecycle status for a region's RMK.
/// The sidecar is the authoritative version registry; key files
/// without a sidecar entry are ignored.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VersionSidecarEntry {
    /// RMK version number.
    pub version: u32,
    /// Current lifecycle status.
    pub status: RmkStatus,
}

// ─── FileKeyManager ──────────────────────────────────────────────

/// Loads RMKs from versioned files on disk.
///
/// Key path pattern: `{key_dir}/{region}/v{version}.key`
/// Each file contains exactly 32 raw bytes (256-bit key material).
///
/// **Local development only.** Emits a warning in release builds.
pub struct FileKeyManager {
    key_dir: PathBuf,
}

impl FileKeyManager {
    /// Creates a new file-based key manager.
    ///
    /// Emits a warning in release builds.
    pub fn new(key_dir: PathBuf) -> Self {
        #[cfg(not(debug_assertions))]
        tracing::warn!(
            key_dir = %key_dir.display(),
            "FileKeyManager is intended for local development only. \
             Use SecretsManagerKeyManager for production deployments."
        );
        Self { key_dir }
    }

    /// Returns the path for a specific key version.
    fn key_path(&self, region: Region, version: u32) -> PathBuf {
        self.key_dir.join(region.as_str()).join(format!("v{version}.key"))
    }

    /// Loads raw key bytes from a file.
    fn load_key_file(&self, path: &Path) -> Result<[u8; 32]> {
        let data = fs::read(path).map_err(|_| Error::Encryption {
            reason: format!("Failed to read key file: {}", path.display()),
        })?;

        if data.len() != 32 {
            return Err(Error::Encryption {
                reason: format!(
                    "Key file {} has {} bytes, expected 32",
                    path.display(),
                    data.len()
                ),
            });
        }

        // Check file permissions (warn if too permissive)
        if let Ok(metadata) = fs::metadata(path) {
            let mode = metadata.permissions().mode();
            let world_or_group_bits = mode & 0o077;
            if world_or_group_bits != 0 {
                tracing::warn!(
                    path = %path.display(),
                    mode = format!("{mode:04o}"),
                    "Key file has permissive permissions. Recommend 0400."
                );
            }
        }

        let mut key = [0u8; 32];
        key.copy_from_slice(&data);
        Ok(key)
    }

    /// Returns the path to the `versions.json` sidecar for a region.
    fn sidecar_path(&self, region: Region) -> PathBuf {
        self.key_dir.join(region.as_str()).join("versions.json")
    }

    /// Reads the `versions.json` sidecar for a region.
    ///
    /// Returns `None` if the file doesn't exist (legacy fallback).
    fn read_versions_json(&self, region: Region) -> Result<Option<Vec<VersionSidecarEntry>>> {
        let path = self.sidecar_path(region);
        if !path.exists() {
            return Ok(None);
        }
        let data = fs::read_to_string(&path).map_err(|_| Error::Encryption {
            reason: format!("Failed to read versions.json: {}", path.display()),
        })?;
        let entries: Vec<VersionSidecarEntry> =
            serde_json::from_str(&data).map_err(|e| Error::Encryption {
                reason: format!("Invalid versions.json at {}: {e}", path.display()),
            })?;
        Ok(Some(entries))
    }

    /// Writes the `versions.json` sidecar for a region.
    fn write_versions_json(&self, region: Region, entries: &[VersionSidecarEntry]) -> Result<()> {
        let path = self.sidecar_path(region);
        let data = serde_json::to_string_pretty(entries).map_err(|e| Error::Encryption {
            reason: format!("Failed to serialize versions.json: {e}"),
        })?;
        fs::write(&path, data).map_err(|_| Error::Encryption {
            reason: format!("Failed to write versions.json: {}", path.display()),
        })?;
        Ok(())
    }

    /// Discovers all version files for a region from the filesystem.
    ///
    /// Scans for `v{N}.key` files. Used as fallback when no `versions.json`
    /// sidecar exists (legacy clusters).
    fn discover_versions_from_files(&self, region: Region) -> Result<Vec<u32>> {
        let region_dir = self.key_dir.join(region.as_str());
        if !region_dir.exists() {
            return Ok(Vec::new());
        }

        let mut versions = Vec::new();
        let entries = fs::read_dir(&region_dir).map_err(|_| Error::Encryption {
            reason: format!("Failed to read key directory: {}", region_dir.display()),
        })?;

        for entry in entries {
            let entry = entry.map_err(|_| Error::Encryption {
                reason: "Failed to read directory entry".to_string(),
            })?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            // Parse "v{N}.key" pattern
            if let Some(rest) = name_str.strip_prefix('v')
                && let Some(num_str) = rest.strip_suffix(".key")
                && let Ok(version) = num_str.parse::<u32>()
            {
                versions.push(version);
            }
        }

        versions.sort_unstable();
        Ok(versions)
    }

    /// Returns version info by consulting the sidecar, falling back to file
    /// discovery for legacy clusters without `versions.json`.
    fn resolve_versions(&self, region: Region) -> Result<Vec<VersionSidecarEntry>> {
        // Prefer sidecar if present
        if let Some(entries) = self.read_versions_json(region)? {
            return Ok(entries);
        }

        // Legacy fallback: derive status from file discovery
        let versions = self.discover_versions_from_files(region)?;
        let highest = versions.last().copied().unwrap_or(0);
        Ok(versions
            .into_iter()
            .map(|version| {
                let status =
                    if version == highest { RmkStatus::Active } else { RmkStatus::Deprecated };
                VersionSidecarEntry { version, status }
            })
            .collect())
    }
}

impl RegionKeyManager for FileKeyManager {
    fn current_rmk(&self, region: Region) -> Result<RegionMasterKey> {
        let entries = self.resolve_versions(region)?;
        // Find highest active version
        let active = entries
            .iter()
            .filter(|e| e.status == RmkStatus::Active)
            .max_by_key(|e| e.version)
            .ok_or_else(|| Error::Encryption {
                reason: format!("No active RMK versions found for region {region}"),
            })?;
        let path = self.key_path(region, active.version);
        let key = self.load_key_file(&path)?;
        Ok(RegionMasterKey::new(active.version, key))
    }

    fn rmk_by_version(&self, region: Region, version: u32) -> Result<RegionMasterKey> {
        let path = self.key_path(region, version);
        let key = self.load_key_file(&path)?;
        Ok(RegionMasterKey::new(version, key))
    }

    fn list_versions(&self, region: Region) -> Result<Vec<RmkVersionInfo>> {
        let entries = self.resolve_versions(region)?;
        Ok(entries
            .into_iter()
            .map(|e| RmkVersionInfo {
                region,
                version: e.version,
                status: e.status,
                created_at: chrono::Utc::now(),
            })
            .collect())
    }

    fn rotate_rmk(&self, region: Region) -> Result<u32> {
        let mut entries = self.resolve_versions(region)?;
        let new_version = entries.last().map_or(1, |e| e.version + 1);

        // Demote previous active versions to deprecated
        for entry in &mut entries {
            if entry.status == RmkStatus::Active {
                entry.status = RmkStatus::Deprecated;
            }
        }

        // Generate random key
        let mut key = [0u8; 32];
        rand::Rng::fill_bytes(&mut rand::rng(), &mut key);

        // Create directory if needed
        let region_dir = self.key_dir.join(region.as_str());
        fs::create_dir_all(&region_dir).map_err(|_| Error::Encryption {
            reason: format!("Failed to create key directory: {}", region_dir.display()),
        })?;

        // Write key file
        let path = self.key_path(region, new_version);
        fs::write(&path, key).map_err(|_| Error::Encryption {
            reason: format!("Failed to write key file: {}", path.display()),
        })?;

        // Set restrictive permissions
        #[cfg(unix)]
        {
            let perms = fs::Permissions::from_mode(0o400);
            let _ = fs::set_permissions(&path, perms);
        }

        // Zeroize the local copy
        zeroize::Zeroize::zeroize(&mut key);

        // Update sidecar
        entries.push(VersionSidecarEntry { version: new_version, status: RmkStatus::Active });
        self.write_versions_json(region, &entries)?;

        Ok(new_version)
    }

    fn decommission_rmk(&self, region: Region, version: u32) -> Result<()> {
        let path = self.key_path(region, version);
        if !path.exists() {
            return Err(Error::Encryption {
                reason: format!("RMK version {version} not found for region {region}"),
            });
        }

        // Rename to .decommissioned to prevent loading
        let decommissioned_path = path.with_extension("key.decommissioned");
        fs::rename(&path, &decommissioned_path).map_err(|_| Error::Encryption {
            reason: format!("Failed to decommission key: {}", path.display()),
        })?;

        // Update sidecar
        let mut entries = self.resolve_versions(region)?;
        for entry in &mut entries {
            if entry.version == version {
                entry.status = RmkStatus::Decommissioned;
            }
        }
        self.write_versions_json(region, &entries)?;

        Ok(())
    }

    fn health_check(&self, region: Region) -> Result<()> {
        use super::operations::{unwrap_dek, wrap_dek};

        // Verify sidecar matches actual key files
        if let Some(entries) = self.read_versions_json(region)? {
            for entry in &entries {
                if entry.status == RmkStatus::Decommissioned {
                    continue;
                }
                let path = self.key_path(region, entry.version);
                if !path.exists() {
                    return Err(Error::Encryption {
                        reason: format!(
                            "versions.json lists v{} as {:?} for region {region}, \
                             but key file {} is missing",
                            entry.version,
                            entry.status,
                            path.display()
                        ),
                    });
                }
            }
        }

        // Test wrap/unwrap cycle with current key
        let rmk = self.current_rmk(region)?;
        let test_dek = super::operations::generate_dek();
        let wrapped = wrap_dek(&test_dek, &rmk)?;
        let unwrapped = unwrap_dek(&wrapped, &rmk)?;
        if test_dek.as_bytes() != unwrapped.as_bytes() {
            return Err(Error::Encryption {
                reason: format!("Health check failed: wrap/unwrap mismatch for region {region}"),
            });
        }
        Ok(())
    }
}

// ─── EnvKeyManager ───────────────────────────────────────────────

/// Loads RMKs from environment variables.
///
/// Variable naming: `LEDGER_RMK_{REGION_UPPER}_{VERSION}` where
/// `REGION_UPPER` is the region name in uppercase with hyphens
/// replaced by underscores, and `VERSION` is "V{N}".
///
/// Example: `LEDGER_RMK_US_EAST_VA_V1=aabbccdd...` (hex-encoded 32 bytes).
///
/// **Rotation limitation**: new versions require setting a new env var
/// and restarting the process. Not suitable for live-rotation.
pub struct EnvKeyManager {
    /// Pre-loaded keys from environment at construction time.
    keys: HashMap<(Region, u32), [u8; 32]>,
}

impl EnvKeyManager {
    /// Creates a new env-based key manager by scanning environment variables.
    ///
    /// Loads all `LEDGER_RMK_*` variables at construction time.
    ///
    /// Note: ideally we would clear these env vars after loading to prevent
    /// key material from appearing in `/proc/self/environ`. However,
    /// `std::env::remove_var` and `std::env::set_var` are `unsafe` in
    /// edition 2024, and this crate denies `unsafe_code`. Operators should
    /// use the SecretsManager backend for production deployments.
    pub fn new() -> Self {
        let mut keys = HashMap::new();

        for (name, value) in std::env::vars() {
            if let Some(rest) = name.strip_prefix("LEDGER_RMK_")
                && let Some((region, version)) = Self::parse_env_key(rest)
            {
                if let Some(key_bytes) = Self::decode_hex(&value) {
                    keys.insert((region, version), key_bytes);
                } else {
                    tracing::warn!(var = %name, "Invalid hex encoding in RMK env var, skipping");
                }
            }
        }

        Self { keys }
    }

    /// Parses `US_EAST_VA_V1` → (Region::US_EAST_VA, 1).
    fn parse_env_key(suffix: &str) -> Option<(Region, u32)> {
        // Find the last "_V{N}" pattern
        let v_idx = suffix.rfind("_V")?;
        let version_str = &suffix[v_idx + 2..];
        let version: u32 = version_str.parse().ok()?;

        let region_upper = &suffix[..v_idx];
        // Convert UPPER_SNAKE to kebab-case: US_EAST_VA → us-east-va
        let region_kebab = region_upper.to_lowercase().replace('_', "-");
        let region: Region = region_kebab.parse().ok()?;

        Some((region, version))
    }

    /// Decodes hex-encoded key bytes.
    fn decode_hex(hex: &str) -> Option<[u8; 32]> {
        let hex = hex.trim();
        if hex.len() != 64 {
            return None;
        }

        let mut bytes = [0u8; 32];
        for (i, chunk) in hex.as_bytes().chunks(2).enumerate() {
            let high = hex_digit(chunk[0])?;
            let low = hex_digit(chunk[1])?;
            bytes[i] = (high << 4) | low;
        }
        Some(bytes)
    }

    /// Returns sorted versions for a region.
    fn versions_for(&self, region: Region) -> Vec<u32> {
        let mut versions: Vec<u32> =
            self.keys.keys().filter(|(r, _)| *r == region).map(|(_, v)| *v).collect();
        versions.sort_unstable();
        versions
    }
}

impl Default for EnvKeyManager {
    fn default() -> Self {
        Self::new()
    }
}

impl RegionKeyManager for EnvKeyManager {
    fn current_rmk(&self, region: Region) -> Result<RegionMasterKey> {
        let versions = self.versions_for(region);
        let version = versions.last().copied().ok_or_else(|| Error::Encryption {
            reason: format!("No RMK env vars found for region {region}"),
        })?;
        let key = self.keys.get(&(region, version)).ok_or_else(|| Error::Encryption {
            reason: format!("RMK v{version} not found for region {region}"),
        })?;
        Ok(RegionMasterKey::new(version, *key))
    }

    fn rmk_by_version(&self, region: Region, version: u32) -> Result<RegionMasterKey> {
        let key = self.keys.get(&(region, version)).ok_or_else(|| Error::Encryption {
            reason: format!("RMK v{version} not found for region {region} (env var not set)"),
        })?;
        Ok(RegionMasterKey::new(version, *key))
    }

    fn list_versions(&self, region: Region) -> Result<Vec<RmkVersionInfo>> {
        let versions = self.versions_for(region);
        let highest = versions.last().copied().unwrap_or(0);

        Ok(versions
            .into_iter()
            .map(|version| {
                let status =
                    if version == highest { RmkStatus::Active } else { RmkStatus::Deprecated };
                RmkVersionInfo { region, version, status, created_at: chrono::Utc::now() }
            })
            .collect())
    }

    fn rotate_rmk(&self, _region: Region) -> Result<u32> {
        Err(Error::Encryption {
            reason: "EnvKeyManager does not support runtime rotation. \
                     Set a new LEDGER_RMK_{REGION}_V{N} env var and restart."
                .to_string(),
        })
    }

    fn decommission_rmk(&self, _region: Region, _version: u32) -> Result<()> {
        Err(Error::Encryption {
            reason: "EnvKeyManager does not support decommission. \
                     Remove the env var and restart."
                .to_string(),
        })
    }

    fn health_check(&self, region: Region) -> Result<()> {
        use super::operations::{unwrap_dek, wrap_dek};

        let rmk = self.current_rmk(region)?;
        let test_dek = super::operations::generate_dek();
        let wrapped = wrap_dek(&test_dek, &rmk)?;
        let unwrapped = unwrap_dek(&wrapped, &rmk)?;
        if test_dek.as_bytes() != unwrapped.as_bytes() {
            return Err(Error::Encryption {
                reason: format!("Health check failed: wrap/unwrap mismatch for region {region}"),
            });
        }
        Ok(())
    }
}

// ─── SecretsClient + SecretsManagerKeyManager ───────────────────

/// Trait for fetching secrets from an external provider.
///
/// Abstracts the HTTP/API layer so `SecretsManagerKeyManager` can be
/// tested with a mock. Production implementations connect to Infisical,
/// Vault, AWS KMS, etc.
///
/// All methods are synchronous. For async providers, implementations
/// should use a blocking runtime handle or pre-load at construction.
pub trait SecretsClient: Send + Sync {
    /// Fetches a secret's raw value for a specific version.
    ///
    /// Returns the raw key bytes (32 bytes for AES-256).
    fn fetch_secret(&self, path: &str, version: u32) -> Result<Vec<u8>>;

    /// Lists all available version numbers for a secret.
    ///
    /// Versions are returned in ascending order.
    fn list_secret_versions(&self, path: &str) -> Result<Vec<u32>>;

    /// Creates a new version of a secret with the given value.
    ///
    /// Returns the new version number.
    fn create_secret_version(&self, path: &str, value: &[u8]) -> Result<u32>;

    /// Marks a secret version as decommissioned (no longer fetchable).
    fn decommission_secret_version(&self, path: &str, version: u32) -> Result<()>;
}

/// Loads RMKs from an external secrets manager.
///
/// Production backend for key management. Keys are fetched at runtime
/// and cached in memory — no key material touches disk or environment
/// variables.
///
/// # Access Policy Guidance
///
/// Configure your secrets manager to enforce region-scoped access:
///
/// - **Infisical**: Use machine identity with environment-scoped access. Each node's identity
///   should only have read access to secrets for `required_regions(node.region)`. Path pattern:
///   `ledger/rmk/{region}`.
///
/// - **HashiCorp Vault**: Use AppRole or Kubernetes auth. Vault policies should grant `read` on
///   `secret/data/ledger/rmk/{region}` paths matching the node's required regions only.
///
/// - **AWS KMS**: Use IAM policies scoped to specific KMS key ARNs per region. Each node's IAM role
///   should only allow `kms:Decrypt` for keys in its required regions.
///
/// - **GCP KMS**: Use IAM bindings on individual CryptoKey resources. Service account permissions
///   scoped to required regions.
///
/// - **Azure Key Vault**: Use RBAC roles scoped to specific vault instances per region. Managed
///   Identity for node authentication.
pub struct SecretsManagerKeyManager {
    /// Pluggable secrets client (mock in tests, HTTP in production).
    client: Box<dyn SecretsClient>,
    /// Configuration with region → secret path mappings.
    config: SecretsManagerConfig,
    /// In-memory key cache: `(region, version) → key bytes`.
    cache: RwLock<HashMap<(Region, u32), [u8; 32]>>,
}

impl SecretsManagerKeyManager {
    /// Creates a new secrets-manager-backed key manager.
    pub fn new(client: Box<dyn SecretsClient>, config: SecretsManagerConfig) -> Self {
        Self { client, config, cache: RwLock::new(HashMap::new()) }
    }

    /// Returns the secret path for a region, or an error if unmapped.
    fn secret_path(&self, region: Region) -> Result<&str> {
        self.config.region_secret_paths.get(&region).map(String::as_str).ok_or_else(|| {
            Error::Encryption {
                reason: format!(
                    "No secret path configured for region {region}. \
                     Add it to region_secret_paths in SecretsManagerConfig."
                ),
            }
        })
    }

    /// Fetches and caches a key, returning it.
    fn fetch_and_cache(&self, region: Region, version: u32) -> Result<[u8; 32]> {
        let path = self.secret_path(region)?;
        let bytes = self.client.fetch_secret(path, version)?;
        if bytes.len() != 32 {
            return Err(Error::Encryption {
                reason: format!(
                    "Secret at {path} v{version} has {} bytes, expected 32",
                    bytes.len()
                ),
            });
        }
        let mut key = [0u8; 32];
        key.copy_from_slice(&bytes);

        // Cache it
        if let Ok(mut cache) = self.cache.write() {
            cache.insert((region, version), key);
        }
        Ok(key)
    }
}

impl RegionKeyManager for SecretsManagerKeyManager {
    fn current_rmk(&self, region: Region) -> Result<RegionMasterKey> {
        let path = self.secret_path(region)?;
        let versions = self.client.list_secret_versions(path)?;
        let version = versions.last().copied().ok_or_else(|| Error::Encryption {
            reason: format!("No secret versions found at {path} for region {region}"),
        })?;

        // Check cache first
        if let Ok(cache) = self.cache.read()
            && let Some(key) = cache.get(&(region, version))
        {
            return Ok(RegionMasterKey::new(version, *key));
        }

        let key = self.fetch_and_cache(region, version)?;
        Ok(RegionMasterKey::new(version, key))
    }

    fn rmk_by_version(&self, region: Region, version: u32) -> Result<RegionMasterKey> {
        // Check cache first
        if let Ok(cache) = self.cache.read()
            && let Some(key) = cache.get(&(region, version))
        {
            return Ok(RegionMasterKey::new(version, *key));
        }

        let key = self.fetch_and_cache(region, version)?;
        Ok(RegionMasterKey::new(version, key))
    }

    fn list_versions(&self, region: Region) -> Result<Vec<RmkVersionInfo>> {
        let path = self.secret_path(region)?;
        let versions = self.client.list_secret_versions(path)?;
        let highest = versions.last().copied().unwrap_or(0);

        Ok(versions
            .into_iter()
            .map(|version| {
                let status =
                    if version == highest { RmkStatus::Active } else { RmkStatus::Deprecated };
                RmkVersionInfo { region, version, status, created_at: chrono::Utc::now() }
            })
            .collect())
    }

    fn rotate_rmk(&self, region: Region) -> Result<u32> {
        let path = self.secret_path(region)?;

        // Generate new key material
        let mut key = [0u8; 32];
        rand::Rng::fill_bytes(&mut rand::rng(), &mut key);

        let new_version = self.client.create_secret_version(path, &key)?;

        // Cache it
        if let Ok(mut cache) = self.cache.write() {
            cache.insert((region, new_version), key);
        }

        // Zeroize local copy
        zeroize::Zeroize::zeroize(&mut key);

        Ok(new_version)
    }

    fn decommission_rmk(&self, region: Region, version: u32) -> Result<()> {
        let path = self.secret_path(region)?;
        self.client.decommission_secret_version(path, version)?;

        // Remove from cache
        if let Ok(mut cache) = self.cache.write() {
            cache.remove(&(region, version));
        }

        Ok(())
    }

    fn health_check(&self, region: Region) -> Result<()> {
        use super::operations::{unwrap_dek, wrap_dek};

        let rmk = self.current_rmk(region)?;
        let test_dek = super::operations::generate_dek();
        let wrapped = wrap_dek(&test_dek, &rmk)?;
        let unwrapped = unwrap_dek(&wrapped, &rmk)?;
        if test_dek.as_bytes() != unwrapped.as_bytes() {
            return Err(Error::Encryption {
                reason: format!("Health check failed: wrap/unwrap mismatch for region {region}"),
            });
        }
        Ok(())
    }
}

// ─── InMemoryKeyManager ─────────────────────────────────────────

/// In-memory key manager for testing and development.
///
/// Stores RMK material in a `HashMap` behind a `RwLock`. Supports
/// programmatic key insertion, rotation, and decommission without
/// external dependencies (no files, env vars, or secrets managers).
///
/// # Usage
///
/// ```no_run
/// # use inferadb_ledger_store::crypto::InMemoryKeyManager;
/// # use inferadb_ledger_types::types::Region;
/// let km = InMemoryKeyManager::generate_for_regions(&[Region::GLOBAL]);
/// ```
pub struct InMemoryKeyManager {
    /// Key material: `(region, version) → key bytes`.
    keys: RwLock<HashMap<(Region, u32), [u8; 32]>>,
}

impl InMemoryKeyManager {
    /// Creates an empty key manager with no keys.
    #[must_use]
    pub fn new() -> Self {
        Self { keys: RwLock::new(HashMap::new()) }
    }

    /// Creates a key manager pre-seeded with a random version-1 key for each region.
    #[must_use]
    pub fn generate_for_regions(regions: &[Region]) -> Self {
        let mut keys = HashMap::new();
        for &region in regions {
            let mut key = [0u8; 32];
            rand::Rng::fill_bytes(&mut rand::rng(), &mut key);
            keys.insert((region, 1), key);
        }
        Self { keys: RwLock::new(keys) }
    }

    /// Inserts a key for a specific region and version.
    pub fn insert(&self, region: Region, version: u32, key: [u8; 32]) {
        if let Ok(mut keys) = self.keys.write() {
            keys.insert((region, version), key);
        }
    }

    /// Returns the highest version number for a region, or `None` if no keys exist.
    fn highest_version(&self, region: Region) -> Option<u32> {
        self.keys.read().ok()?.keys().filter(|(r, _)| *r == region).map(|(_, v)| *v).max()
    }
}

impl Default for InMemoryKeyManager {
    fn default() -> Self {
        Self::new()
    }
}

impl RegionKeyManager for InMemoryKeyManager {
    fn current_rmk(&self, region: Region) -> Result<RegionMasterKey> {
        let version = self.highest_version(region).ok_or_else(|| Error::Encryption {
            reason: format!("No RMK configured for region {region}"),
        })?;
        self.rmk_by_version(region, version)
    }

    fn rmk_by_version(&self, region: Region, version: u32) -> Result<RegionMasterKey> {
        let keys = self
            .keys
            .read()
            .map_err(|_| Error::Encryption { reason: "RwLock poisoned".to_string() })?;
        let key = keys.get(&(region, version)).ok_or_else(|| Error::Encryption {
            reason: format!("No RMK for region {region} version {version}"),
        })?;
        Ok(RegionMasterKey::new(version, *key))
    }

    fn list_versions(&self, region: Region) -> Result<Vec<RmkVersionInfo>> {
        let keys = self
            .keys
            .read()
            .map_err(|_| Error::Encryption { reason: "RwLock poisoned".to_string() })?;
        let mut versions: Vec<u32> =
            keys.keys().filter(|(r, _)| *r == region).map(|(_, v)| *v).collect();
        versions.sort_unstable();
        let highest = versions.last().copied().unwrap_or(0);
        Ok(versions
            .into_iter()
            .map(|version| {
                let status =
                    if version == highest { RmkStatus::Active } else { RmkStatus::Deprecated };
                RmkVersionInfo { region, version, status, created_at: chrono::Utc::now() }
            })
            .collect())
    }

    fn rotate_rmk(&self, region: Region) -> Result<u32> {
        let new_version = self.highest_version(region).unwrap_or(0) + 1;
        let mut key = [0u8; 32];
        rand::Rng::fill_bytes(&mut rand::rng(), &mut key);
        self.insert(region, new_version, key);
        zeroize::Zeroize::zeroize(&mut key);
        Ok(new_version)
    }

    fn decommission_rmk(&self, region: Region, version: u32) -> Result<()> {
        let mut keys = self
            .keys
            .write()
            .map_err(|_| Error::Encryption { reason: "RwLock poisoned".to_string() })?;
        if keys.remove(&(region, version)).is_some() {
            Ok(())
        } else {
            Err(Error::Encryption {
                reason: format!("No RMK for region {region} version {version}"),
            })
        }
    }

    fn health_check(&self, region: Region) -> Result<()> {
        // Verify we can load the current key
        let _ = self.current_rmk(region)?;
        Ok(())
    }
}

/// Converts a hex ASCII digit to its numeric value.
fn hex_digit(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    // ─── required_regions tests ─────────────────────────────

    #[test]
    fn test_required_regions_non_protected() {
        let regions = required_regions(Region::US_EAST_VA);
        assert_eq!(regions.len(), 3);
        assert!(regions.contains(&Region::GLOBAL));
        assert!(regions.contains(&Region::US_EAST_VA));
        assert!(regions.contains(&Region::US_WEST_OR));
    }

    #[test]
    fn test_required_regions_protected() {
        let regions = required_regions(Region::DE_CENTRAL_FRANKFURT);
        assert_eq!(regions.len(), 4);
        assert!(regions.contains(&Region::GLOBAL));
        assert!(regions.contains(&Region::US_EAST_VA));
        assert!(regions.contains(&Region::US_WEST_OR));
        assert!(regions.contains(&Region::DE_CENTRAL_FRANKFURT));
    }

    #[test]
    fn test_required_regions_global() {
        let regions = required_regions(Region::GLOBAL);
        assert_eq!(regions.len(), 3);
    }

    // ─── EnvKeyManager parsing tests ────────────────────────

    #[test]
    fn test_parse_env_key_us_east_va_v1() {
        let result = EnvKeyManager::parse_env_key("US_EAST_VA_V1");
        assert_eq!(result, Some((Region::US_EAST_VA, 1)));
    }

    #[test]
    fn test_parse_env_key_de_central_frankfurt_v3() {
        let result = EnvKeyManager::parse_env_key("DE_CENTRAL_FRANKFURT_V3");
        assert_eq!(result, Some((Region::DE_CENTRAL_FRANKFURT, 3)));
    }

    #[test]
    fn test_parse_env_key_global_v1() {
        let result = EnvKeyManager::parse_env_key("GLOBAL_V1");
        assert_eq!(result, Some((Region::GLOBAL, 1)));
    }

    #[test]
    fn test_parse_env_key_invalid_region() {
        let result = EnvKeyManager::parse_env_key("INVALID_REGION_V1");
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_env_key_no_version() {
        let result = EnvKeyManager::parse_env_key("US_EAST_VA");
        assert!(result.is_none());
    }

    #[test]
    fn test_decode_hex_valid() {
        let hex = "aa".repeat(32);
        let result = EnvKeyManager::decode_hex(&hex);
        assert_eq!(result, Some([0xAA; 32]));
    }

    #[test]
    fn test_decode_hex_too_short() {
        let result = EnvKeyManager::decode_hex("aabb");
        assert!(result.is_none());
    }

    #[test]
    fn test_decode_hex_invalid_char() {
        let hex = "gg".repeat(32);
        let result = EnvKeyManager::decode_hex(&hex);
        assert!(result.is_none());
    }

    #[test]
    fn test_decode_hex_uppercase() {
        let hex = "AA".repeat(32);
        let result = EnvKeyManager::decode_hex(&hex);
        assert_eq!(result, Some([0xAA; 32]));
    }

    // ─── FileKeyManager tests ───────────────────────────────

    #[test]
    fn test_file_key_manager_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let manager = FileKeyManager::new(dir.path().to_path_buf());

        let version = manager.rotate_rmk(Region::US_EAST_VA).unwrap();
        assert_eq!(version, 1);

        let rmk = manager.current_rmk(Region::US_EAST_VA).unwrap();
        assert_eq!(rmk.version, 1);

        let rmk_v1 = manager.rmk_by_version(Region::US_EAST_VA, 1).unwrap();
        assert_eq!(rmk_v1.as_bytes(), rmk.as_bytes());
    }

    #[test]
    fn test_file_key_manager_multi_version() {
        let dir = tempfile::tempdir().unwrap();
        let manager = FileKeyManager::new(dir.path().to_path_buf());

        manager.rotate_rmk(Region::GLOBAL).unwrap();
        manager.rotate_rmk(Region::GLOBAL).unwrap();
        let v3 = manager.rotate_rmk(Region::GLOBAL).unwrap();
        assert_eq!(v3, 3);

        let current = manager.current_rmk(Region::GLOBAL).unwrap();
        assert_eq!(current.version, 3);

        let v1 = manager.rmk_by_version(Region::GLOBAL, 1).unwrap();
        assert_eq!(v1.version, 1);

        assert_ne!(
            manager.rmk_by_version(Region::GLOBAL, 1).unwrap().as_bytes(),
            manager.rmk_by_version(Region::GLOBAL, 2).unwrap().as_bytes()
        );
    }

    #[test]
    fn test_file_key_manager_list_versions() {
        let dir = tempfile::tempdir().unwrap();
        let manager = FileKeyManager::new(dir.path().to_path_buf());

        manager.rotate_rmk(Region::US_WEST_OR).unwrap();
        manager.rotate_rmk(Region::US_WEST_OR).unwrap();

        let versions = manager.list_versions(Region::US_WEST_OR).unwrap();
        assert_eq!(versions.len(), 2);
        assert_eq!(versions[0].version, 1);
        assert_eq!(versions[0].status, RmkStatus::Deprecated);
        assert_eq!(versions[1].version, 2);
        assert_eq!(versions[1].status, RmkStatus::Active);
    }

    #[test]
    fn test_file_key_manager_decommission() {
        let dir = tempfile::tempdir().unwrap();
        let manager = FileKeyManager::new(dir.path().to_path_buf());

        manager.rotate_rmk(Region::GLOBAL).unwrap();
        manager.rotate_rmk(Region::GLOBAL).unwrap();

        manager.decommission_rmk(Region::GLOBAL, 1).unwrap();

        assert!(manager.rmk_by_version(Region::GLOBAL, 1).is_err());
        assert!(manager.rmk_by_version(Region::GLOBAL, 2).is_ok());
    }

    #[test]
    fn test_file_key_manager_missing_region() {
        let dir = tempfile::tempdir().unwrap();
        let manager = FileKeyManager::new(dir.path().to_path_buf());

        assert!(manager.current_rmk(Region::US_EAST_VA).is_err());
    }

    #[test]
    fn test_file_key_manager_health_check() {
        let dir = tempfile::tempdir().unwrap();
        let manager = FileKeyManager::new(dir.path().to_path_buf());

        manager.rotate_rmk(Region::GLOBAL).unwrap();
        assert!(manager.health_check(Region::GLOBAL).is_ok());
    }

    #[test]
    fn test_file_key_manager_health_check_missing() {
        let dir = tempfile::tempdir().unwrap();
        let manager = FileKeyManager::new(dir.path().to_path_buf());

        assert!(manager.health_check(Region::GLOBAL).is_err());
    }

    #[test]
    fn test_file_key_manager_bad_key_size() {
        let dir = tempfile::tempdir().unwrap();
        let region_dir = dir.path().join("global");
        fs::create_dir_all(&region_dir).unwrap();
        fs::write(region_dir.join("v1.key"), [0xAA; 16]).unwrap();

        let manager = FileKeyManager::new(dir.path().to_path_buf());
        assert!(manager.rmk_by_version(Region::GLOBAL, 1).is_err());
    }

    // ─── versions.json sidecar tests ────────────────────────

    #[test]
    fn test_file_key_manager_versions_json_created_on_rotate() {
        let dir = tempfile::tempdir().unwrap();
        let manager = FileKeyManager::new(dir.path().to_path_buf());

        manager.rotate_rmk(Region::US_EAST_VA).unwrap();

        // versions.json should exist
        let sidecar_path = dir.path().join("us-east-va").join("versions.json");
        assert!(sidecar_path.exists());

        // Parse and verify
        let data = fs::read_to_string(&sidecar_path).unwrap();
        let entries: Vec<VersionSidecarEntry> = serde_json::from_str(&data).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].version, 1);
        assert_eq!(entries[0].status, RmkStatus::Active);
    }

    #[test]
    fn test_file_key_manager_versions_json_rotation_demotes() {
        let dir = tempfile::tempdir().unwrap();
        let manager = FileKeyManager::new(dir.path().to_path_buf());

        manager.rotate_rmk(Region::GLOBAL).unwrap();
        manager.rotate_rmk(Region::GLOBAL).unwrap();

        let entries = manager.read_versions_json(Region::GLOBAL).unwrap().unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].status, RmkStatus::Deprecated);
        assert_eq!(entries[1].status, RmkStatus::Active);
    }

    #[test]
    fn test_file_key_manager_versions_json_decommission_updates() {
        let dir = tempfile::tempdir().unwrap();
        let manager = FileKeyManager::new(dir.path().to_path_buf());

        manager.rotate_rmk(Region::GLOBAL).unwrap();
        manager.rotate_rmk(Region::GLOBAL).unwrap();
        manager.decommission_rmk(Region::GLOBAL, 1).unwrap();

        let entries = manager.read_versions_json(Region::GLOBAL).unwrap().unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].status, RmkStatus::Decommissioned);
        assert_eq!(entries[1].status, RmkStatus::Active);
    }

    #[test]
    fn test_file_key_manager_versions_json_mismatch_detected() {
        let dir = tempfile::tempdir().unwrap();
        let manager = FileKeyManager::new(dir.path().to_path_buf());

        manager.rotate_rmk(Region::GLOBAL).unwrap();
        manager.rotate_rmk(Region::GLOBAL).unwrap();

        // Manually delete v1.key — sidecar still lists it as deprecated
        fs::remove_file(dir.path().join("global").join("v1.key")).unwrap();

        // Health check detects the mismatch
        let err = manager.health_check(Region::GLOBAL).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("v1"));
        assert!(msg.contains("missing"));
    }

    #[test]
    fn test_file_key_manager_legacy_fallback_no_sidecar() {
        let dir = tempfile::tempdir().unwrap();
        let region_dir = dir.path().join("global");
        fs::create_dir_all(&region_dir).unwrap();

        // Manually write key files without versions.json
        fs::write(region_dir.join("v1.key"), [0xAA; 32]).unwrap();
        fs::write(region_dir.join("v2.key"), [0xBB; 32]).unwrap();

        let manager = FileKeyManager::new(dir.path().to_path_buf());

        // Should still work via file discovery
        let versions = manager.list_versions(Region::GLOBAL).unwrap();
        assert_eq!(versions.len(), 2);
        assert_eq!(versions[0].version, 1);
        assert_eq!(versions[0].status, RmkStatus::Deprecated);
        assert_eq!(versions[1].version, 2);
        assert_eq!(versions[1].status, RmkStatus::Active);
    }

    // ─── EnvKeyManager integration tests ────────────────────

    #[test]
    fn test_env_key_manager_rotate_unsupported() {
        let manager = EnvKeyManager { keys: HashMap::new() };
        assert!(manager.rotate_rmk(Region::GLOBAL).is_err());
    }

    #[test]
    fn test_env_key_manager_decommission_unsupported() {
        let manager = EnvKeyManager { keys: HashMap::new() };
        assert!(manager.decommission_rmk(Region::GLOBAL, 1).is_err());
    }

    #[test]
    fn test_env_key_manager_with_preloaded_keys() {
        let mut keys = HashMap::new();
        keys.insert((Region::US_EAST_VA, 1), [0xAA; 32]);
        keys.insert((Region::US_EAST_VA, 2), [0xBB; 32]);
        let manager = EnvKeyManager { keys };

        let current = manager.current_rmk(Region::US_EAST_VA).unwrap();
        assert_eq!(current.version, 2);
        assert_eq!(current.as_bytes(), &[0xBB; 32]);

        let v1 = manager.rmk_by_version(Region::US_EAST_VA, 1).unwrap();
        assert_eq!(v1.as_bytes(), &[0xAA; 32]);
    }

    #[test]
    fn test_env_key_manager_missing_region() {
        let manager = EnvKeyManager { keys: HashMap::new() };
        assert!(manager.current_rmk(Region::GLOBAL).is_err());
    }

    #[test]
    fn test_env_key_manager_health_check() {
        let mut keys = HashMap::new();
        keys.insert((Region::GLOBAL, 1), [0xAA; 32]);
        let manager = EnvKeyManager { keys };

        assert!(manager.health_check(Region::GLOBAL).is_ok());
    }

    // ─── MockSecretsClient ──────────────────────────────────

    /// In-memory secrets client for testing `SecretsManagerKeyManager`.
    struct MockSecretsClient {
        secrets: Mutex<HashMap<String, Vec<(u32, Vec<u8>)>>>,
    }

    impl MockSecretsClient {
        fn new() -> Self {
            Self { secrets: Mutex::new(HashMap::new()) }
        }

        fn add_secret(&self, path: &str, version: u32, value: &[u8]) {
            let mut secrets = self.secrets.lock().unwrap();
            let entries = secrets.entry(path.to_string()).or_default();
            entries.push((version, value.to_vec()));
            entries.sort_by_key(|(v, _)| *v);
        }
    }

    impl SecretsClient for MockSecretsClient {
        fn fetch_secret(&self, path: &str, version: u32) -> Result<Vec<u8>> {
            let secrets = self.secrets.lock().unwrap();
            let entries = secrets
                .get(path)
                .ok_or_else(|| Error::Encryption { reason: format!("No secret at path {path}") })?;
            let (_, value) = entries.iter().find(|(v, _)| *v == version).ok_or_else(|| {
                Error::Encryption { reason: format!("No version {version} at path {path}") }
            })?;
            Ok(value.clone())
        }

        fn list_secret_versions(&self, path: &str) -> Result<Vec<u32>> {
            let secrets = self.secrets.lock().unwrap();
            match secrets.get(path) {
                Some(entries) => Ok(entries.iter().map(|(v, _)| *v).collect()),
                None => Ok(Vec::new()),
            }
        }

        fn create_secret_version(&self, path: &str, value: &[u8]) -> Result<u32> {
            let mut secrets = self.secrets.lock().unwrap();
            let entries = secrets.entry(path.to_string()).or_default();
            let new_version = entries.last().map_or(1, |(v, _)| v + 1);
            entries.push((new_version, value.to_vec()));
            Ok(new_version)
        }

        fn decommission_secret_version(&self, path: &str, version: u32) -> Result<()> {
            let mut secrets = self.secrets.lock().unwrap();
            let entries = secrets
                .get_mut(path)
                .ok_or_else(|| Error::Encryption { reason: format!("No secret at path {path}") })?;
            entries.retain(|(v, _)| *v != version);
            Ok(())
        }
    }

    fn mock_config() -> SecretsManagerConfig {
        let mut paths = HashMap::new();
        paths.insert(Region::GLOBAL, "ledger/rmk/global".to_string());
        paths.insert(Region::US_EAST_VA, "ledger/rmk/us-east-va".to_string());
        paths.insert(Region::US_WEST_OR, "ledger/rmk/us-west-or".to_string());
        SecretsManagerConfig {
            provider: inferadb_ledger_types::config::SecretsProvider::Infisical,
            endpoint: "http://localhost:8080".to_string(),
            project_id: "test-project".to_string(),
            environment: "test".to_string(),
            region_secret_paths: paths,
        }
    }

    // ─── SecretsManagerKeyManager tests ─────────────────────

    #[test]
    fn test_secrets_manager_fetch_and_cache() {
        let client = MockSecretsClient::new();
        client.add_secret("ledger/rmk/global", 1, &[0xAA; 32]);
        let config = mock_config();
        let manager = SecretsManagerKeyManager::new(Box::new(client), config);

        let rmk = manager.current_rmk(Region::GLOBAL).unwrap();
        assert_eq!(rmk.version, 1);
        assert_eq!(rmk.as_bytes(), &[0xAA; 32]);

        // Second call should hit cache
        let rmk2 = manager.current_rmk(Region::GLOBAL).unwrap();
        assert_eq!(rmk2.as_bytes(), rmk.as_bytes());
    }

    #[test]
    fn test_secrets_manager_multi_version() {
        let client = MockSecretsClient::new();
        client.add_secret("ledger/rmk/global", 1, &[0xAA; 32]);
        client.add_secret("ledger/rmk/global", 2, &[0xBB; 32]);
        let config = mock_config();
        let manager = SecretsManagerKeyManager::new(Box::new(client), config);

        let current = manager.current_rmk(Region::GLOBAL).unwrap();
        assert_eq!(current.version, 2);

        let v1 = manager.rmk_by_version(Region::GLOBAL, 1).unwrap();
        assert_eq!(v1.as_bytes(), &[0xAA; 32]);
    }

    #[test]
    fn test_secrets_manager_rotate() {
        let client = MockSecretsClient::new();
        client.add_secret("ledger/rmk/global", 1, &[0xAA; 32]);
        let config = mock_config();
        let manager = SecretsManagerKeyManager::new(Box::new(client), config);

        let new_version = manager.rotate_rmk(Region::GLOBAL).unwrap();
        assert_eq!(new_version, 2);

        // New version should be loadable
        let rmk = manager.rmk_by_version(Region::GLOBAL, 2).unwrap();
        assert_eq!(rmk.version, 2);
    }

    #[test]
    fn test_secrets_manager_decommission() {
        let client = MockSecretsClient::new();
        client.add_secret("ledger/rmk/global", 1, &[0xAA; 32]);
        client.add_secret("ledger/rmk/global", 2, &[0xBB; 32]);
        let config = mock_config();
        let manager = SecretsManagerKeyManager::new(Box::new(client), config);

        manager.decommission_rmk(Region::GLOBAL, 1).unwrap();
        assert!(manager.rmk_by_version(Region::GLOBAL, 1).is_err());
        assert!(manager.rmk_by_version(Region::GLOBAL, 2).is_ok());
    }

    #[test]
    fn test_secrets_manager_list_versions() {
        let client = MockSecretsClient::new();
        client.add_secret("ledger/rmk/global", 1, &[0xAA; 32]);
        client.add_secret("ledger/rmk/global", 2, &[0xBB; 32]);
        let config = mock_config();
        let manager = SecretsManagerKeyManager::new(Box::new(client), config);

        let versions = manager.list_versions(Region::GLOBAL).unwrap();
        assert_eq!(versions.len(), 2);
        assert_eq!(versions[0].version, 1);
        assert_eq!(versions[0].status, RmkStatus::Deprecated);
        assert_eq!(versions[1].version, 2);
        assert_eq!(versions[1].status, RmkStatus::Active);
    }

    #[test]
    fn test_secrets_manager_health_check() {
        let client = MockSecretsClient::new();
        client.add_secret("ledger/rmk/global", 1, &[0xAA; 32]);
        let config = mock_config();
        let manager = SecretsManagerKeyManager::new(Box::new(client), config);

        assert!(manager.health_check(Region::GLOBAL).is_ok());
    }

    #[test]
    fn test_secrets_manager_unmapped_region() {
        let client = MockSecretsClient::new();
        let config = mock_config();
        let manager = SecretsManagerKeyManager::new(Box::new(client), config);

        // DE_CENTRAL_FRANKFURT not in mock config paths
        assert!(manager.current_rmk(Region::DE_CENTRAL_FRANKFURT).is_err());
    }

    #[test]
    fn test_secrets_manager_bad_key_size() {
        let client = MockSecretsClient::new();
        client.add_secret("ledger/rmk/global", 1, &[0xAA; 16]); // Only 16 bytes
        let config = mock_config();
        let manager = SecretsManagerKeyManager::new(Box::new(client), config);

        let err = manager.current_rmk(Region::GLOBAL).unwrap_err();
        assert!(err.to_string().contains("16 bytes"));
    }

    // ─── validate_rmk_provisioning tests ────────────────────

    #[test]
    fn test_validate_provisioning_all_regions_ok() {
        let dir = tempfile::tempdir().unwrap();
        let manager = FileKeyManager::new(dir.path().to_path_buf());

        // Provision all required regions for a non-protected node
        manager.rotate_rmk(Region::GLOBAL).unwrap();
        manager.rotate_rmk(Region::US_EAST_VA).unwrap();
        manager.rotate_rmk(Region::US_WEST_OR).unwrap();

        assert!(validate_rmk_provisioning(&manager, Region::US_EAST_VA).is_ok());
    }

    #[test]
    fn test_validate_provisioning_missing_region_fails() {
        let dir = tempfile::tempdir().unwrap();
        let manager = FileKeyManager::new(dir.path().to_path_buf());

        // Only provision GLOBAL, missing US_EAST_VA and US_WEST_OR
        manager.rotate_rmk(Region::GLOBAL).unwrap();

        let err = validate_rmk_provisioning(&manager, Region::US_EAST_VA).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("No RMK versions provisioned"));
    }

    #[test]
    fn test_validate_provisioning_protected_region_required() {
        let dir = tempfile::tempdir().unwrap();
        let manager = FileKeyManager::new(dir.path().to_path_buf());

        // Provision non-protected regions but not the protected one
        manager.rotate_rmk(Region::GLOBAL).unwrap();
        manager.rotate_rmk(Region::US_EAST_VA).unwrap();
        manager.rotate_rmk(Region::US_WEST_OR).unwrap();

        // Protected node needs its own region
        let err = validate_rmk_provisioning(&manager, Region::DE_CENTRAL_FRANKFURT).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("de-central-frankfurt") || msg.contains("No RMK versions"));
    }

    #[test]
    fn test_validate_provisioning_deprecated_version_must_load() {
        let dir = tempfile::tempdir().unwrap();
        let manager = FileKeyManager::new(dir.path().to_path_buf());

        // Create two versions for all regions
        for region in [Region::GLOBAL, Region::US_EAST_VA, Region::US_WEST_OR] {
            manager.rotate_rmk(region).unwrap();
            manager.rotate_rmk(region).unwrap();
        }

        // Delete the deprecated v1 key file for GLOBAL (but sidecar still lists it)
        fs::remove_file(dir.path().join("global").join("v1.key")).unwrap();

        let err = validate_rmk_provisioning(&manager, Region::US_EAST_VA).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("v1") || msg.contains("failed to load"));
    }

    // ─── rmk_versions_for_health tests ──────────────────────

    #[test]
    fn test_rmk_versions_for_health_populated() {
        let dir = tempfile::tempdir().unwrap();
        let manager = FileKeyManager::new(dir.path().to_path_buf());

        manager.rotate_rmk(Region::GLOBAL).unwrap();
        manager.rotate_rmk(Region::US_EAST_VA).unwrap();
        manager.rotate_rmk(Region::US_WEST_OR).unwrap();

        let versions = rmk_versions_for_health(&manager, Region::US_EAST_VA);
        assert_eq!(versions.len(), 3);
        assert_eq!(versions[&Region::GLOBAL].len(), 1);
        assert_eq!(versions[&Region::US_EAST_VA].len(), 1);
        assert_eq!(versions[&Region::US_WEST_OR].len(), 1);
    }

    #[test]
    fn test_rmk_versions_for_health_missing_region() {
        let dir = tempfile::tempdir().unwrap();
        let manager = FileKeyManager::new(dir.path().to_path_buf());

        // Only GLOBAL provisioned
        manager.rotate_rmk(Region::GLOBAL).unwrap();

        let versions = rmk_versions_for_health(&manager, Region::US_EAST_VA);
        assert_eq!(versions.len(), 3);
        assert_eq!(versions[&Region::GLOBAL].len(), 1);
        assert!(versions[&Region::US_EAST_VA].is_empty());
        assert!(versions[&Region::US_WEST_OR].is_empty());
    }

    // ─── InMemoryKeyManager tests ──────────────────────────────

    #[test]
    fn test_in_memory_key_manager_new_empty() {
        let manager = InMemoryKeyManager::new();
        assert!(manager.current_rmk(Region::GLOBAL).is_err());
        assert!(manager.list_versions(Region::GLOBAL).unwrap().is_empty());
    }

    #[test]
    fn test_in_memory_key_manager_default() {
        let manager = InMemoryKeyManager::default();
        assert!(manager.current_rmk(Region::GLOBAL).is_err());
    }

    #[test]
    fn test_in_memory_key_manager_generate_for_regions() {
        let manager =
            InMemoryKeyManager::generate_for_regions(&[Region::GLOBAL, Region::US_EAST_VA]);
        assert!(manager.current_rmk(Region::GLOBAL).is_ok());
        assert!(manager.current_rmk(Region::US_EAST_VA).is_ok());
        // Unprovisioned region
        assert!(manager.current_rmk(Region::US_WEST_OR).is_err());
    }

    #[test]
    fn test_in_memory_key_manager_insert_and_retrieve() {
        let manager = InMemoryKeyManager::new();
        manager.insert(Region::GLOBAL, 1, [0xAA; 32]);
        let rmk = manager.rmk_by_version(Region::GLOBAL, 1).unwrap();
        assert_eq!(rmk.version, 1);
        assert_eq!(rmk.as_bytes(), &[0xAA; 32]);
    }

    #[test]
    fn test_in_memory_key_manager_current_returns_highest() {
        let manager = InMemoryKeyManager::new();
        manager.insert(Region::GLOBAL, 1, [0xAA; 32]);
        manager.insert(Region::GLOBAL, 3, [0xCC; 32]);
        manager.insert(Region::GLOBAL, 2, [0xBB; 32]);
        let rmk = manager.current_rmk(Region::GLOBAL).unwrap();
        assert_eq!(rmk.version, 3);
    }

    #[test]
    fn test_in_memory_key_manager_rotate() {
        let manager = InMemoryKeyManager::new();
        manager.insert(Region::GLOBAL, 1, [0xAA; 32]);

        let v2 = manager.rotate_rmk(Region::GLOBAL).unwrap();
        assert_eq!(v2, 2);
        assert!(manager.rmk_by_version(Region::GLOBAL, 2).is_ok());
    }

    #[test]
    fn test_in_memory_key_manager_rotate_empty() {
        let manager = InMemoryKeyManager::new();
        let v1 = manager.rotate_rmk(Region::GLOBAL).unwrap();
        assert_eq!(v1, 1);
    }

    #[test]
    fn test_in_memory_key_manager_decommission() {
        let manager = InMemoryKeyManager::new();
        manager.insert(Region::GLOBAL, 1, [0xAA; 32]);
        manager.insert(Region::GLOBAL, 2, [0xBB; 32]);

        manager.decommission_rmk(Region::GLOBAL, 1).unwrap();
        assert!(manager.rmk_by_version(Region::GLOBAL, 1).is_err());
        assert!(manager.rmk_by_version(Region::GLOBAL, 2).is_ok());
    }

    #[test]
    fn test_in_memory_key_manager_decommission_nonexistent() {
        let manager = InMemoryKeyManager::new();
        assert!(manager.decommission_rmk(Region::GLOBAL, 99).is_err());
    }

    #[test]
    fn test_in_memory_key_manager_list_versions() {
        let manager = InMemoryKeyManager::new();
        manager.insert(Region::GLOBAL, 1, [0xAA; 32]);
        manager.insert(Region::GLOBAL, 2, [0xBB; 32]);

        let versions = manager.list_versions(Region::GLOBAL).unwrap();
        assert_eq!(versions.len(), 2);
        assert_eq!(versions[0].version, 1);
        assert_eq!(versions[0].status, RmkStatus::Deprecated);
        assert_eq!(versions[1].version, 2);
        assert_eq!(versions[1].status, RmkStatus::Active);
    }

    #[test]
    fn test_in_memory_key_manager_health_check_ok() {
        let manager = InMemoryKeyManager::generate_for_regions(&[Region::GLOBAL]);
        assert!(manager.health_check(Region::GLOBAL).is_ok());
    }

    #[test]
    fn test_in_memory_key_manager_health_check_missing() {
        let manager = InMemoryKeyManager::new();
        assert!(manager.health_check(Region::GLOBAL).is_err());
    }

    // ─── hex_digit tests ───────────────────────────────────────

    #[test]
    fn test_hex_digit_lowercase() {
        assert_eq!(hex_digit(b'0'), Some(0));
        assert_eq!(hex_digit(b'9'), Some(9));
        assert_eq!(hex_digit(b'a'), Some(10));
        assert_eq!(hex_digit(b'f'), Some(15));
    }

    #[test]
    fn test_hex_digit_uppercase() {
        assert_eq!(hex_digit(b'A'), Some(10));
        assert_eq!(hex_digit(b'F'), Some(15));
    }

    #[test]
    fn test_hex_digit_invalid() {
        assert_eq!(hex_digit(b'g'), None);
        assert_eq!(hex_digit(b'G'), None);
        assert_eq!(hex_digit(b' '), None);
        assert_eq!(hex_digit(b'\0'), None);
    }

    // ─── EnvKeyManager list_versions tests ─────────────────────

    #[test]
    fn test_env_key_manager_list_versions_multiple() {
        let mut keys = HashMap::new();
        keys.insert((Region::GLOBAL, 1), [0xAA; 32]);
        keys.insert((Region::GLOBAL, 2), [0xBB; 32]);
        keys.insert((Region::GLOBAL, 3), [0xCC; 32]);
        let manager = EnvKeyManager { keys };

        let versions = manager.list_versions(Region::GLOBAL).unwrap();
        assert_eq!(versions.len(), 3);
        assert_eq!(versions[0].status, RmkStatus::Deprecated);
        assert_eq!(versions[1].status, RmkStatus::Deprecated);
        assert_eq!(versions[2].status, RmkStatus::Active);
    }

    #[test]
    fn test_env_key_manager_list_versions_empty() {
        let manager = EnvKeyManager { keys: HashMap::new() };
        let versions = manager.list_versions(Region::GLOBAL).unwrap();
        assert!(versions.is_empty());
    }

    #[test]
    fn test_env_key_manager_rmk_by_version_missing() {
        let manager = EnvKeyManager { keys: HashMap::new() };
        assert!(manager.rmk_by_version(Region::GLOBAL, 1).is_err());
    }

    #[test]
    fn test_env_key_manager_health_check_missing_region() {
        let manager = EnvKeyManager { keys: HashMap::new() };
        assert!(manager.health_check(Region::GLOBAL).is_err());
    }

    // ─── SecretsManagerKeyManager edge cases ───────────────────

    #[test]
    fn test_secrets_manager_rmk_by_version_cached() {
        let client = MockSecretsClient::new();
        client.add_secret("ledger/rmk/global", 1, &[0xAA; 32]);
        let config = mock_config();
        let manager = SecretsManagerKeyManager::new(Box::new(client), config);

        // First call fetches and caches
        let rmk1 = manager.rmk_by_version(Region::GLOBAL, 1).unwrap();
        // Second call hits cache
        let rmk2 = manager.rmk_by_version(Region::GLOBAL, 1).unwrap();
        assert_eq!(rmk1.as_bytes(), rmk2.as_bytes());
    }

    #[test]
    fn test_secrets_manager_empty_versions() {
        let client = MockSecretsClient::new();
        let config = mock_config();
        let manager = SecretsManagerKeyManager::new(Box::new(client), config);

        assert!(manager.current_rmk(Region::GLOBAL).is_err());
    }

    #[test]
    fn test_secrets_manager_list_versions_empty() {
        let client = MockSecretsClient::new();
        let config = mock_config();
        let manager = SecretsManagerKeyManager::new(Box::new(client), config);

        let versions = manager.list_versions(Region::GLOBAL).unwrap();
        assert!(versions.is_empty());
    }

    // ─── EnvKeyManager parse_env_key edge cases ────────────────

    #[test]
    fn test_parse_env_key_version_zero() {
        let result = EnvKeyManager::parse_env_key("GLOBAL_V0");
        assert_eq!(result, Some((Region::GLOBAL, 0)));
    }

    #[test]
    fn test_parse_env_key_high_version() {
        let result = EnvKeyManager::parse_env_key("GLOBAL_V999");
        assert_eq!(result, Some((Region::GLOBAL, 999)));
    }

    #[test]
    fn test_parse_env_key_non_numeric_version() {
        let result = EnvKeyManager::parse_env_key("GLOBAL_Vabc");
        assert!(result.is_none());
    }

    // ─── VersionSidecarEntry tests ─────────────────────────────

    #[test]
    fn test_version_sidecar_entry_serde_roundtrip() {
        let entries = vec![
            VersionSidecarEntry { version: 1, status: RmkStatus::Active },
            VersionSidecarEntry { version: 2, status: RmkStatus::Deprecated },
            VersionSidecarEntry { version: 3, status: RmkStatus::Decommissioned },
        ];
        let json = serde_json::to_string(&entries).unwrap();
        let recovered: Vec<VersionSidecarEntry> = serde_json::from_str(&json).unwrap();
        assert_eq!(entries, recovered);
    }

    // ─── validate_rmk_provisioning with no active version ──────

    #[test]
    fn test_validate_provisioning_no_active_version() {
        let manager = InMemoryKeyManager::new();
        // Insert key but then decommission it — list_versions returns empty
        // after decommission since InMemoryKeyManager removes the key entirely
        manager.insert(Region::GLOBAL, 1, [0xAA; 32]);
        manager.insert(Region::US_EAST_VA, 1, [0xBB; 32]);
        manager.insert(Region::US_WEST_OR, 1, [0xCC; 32]);

        // This should pass since all regions have version 1 active
        assert!(validate_rmk_provisioning(&manager, Region::US_EAST_VA).is_ok());

        // Decommission one region's only key
        manager.decommission_rmk(Region::GLOBAL, 1).unwrap();

        // Now validation should fail
        let err = validate_rmk_provisioning(&manager, Region::US_EAST_VA).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("No RMK versions") || msg.contains("No active"));
    }

    // ─── FileKeyManager discover_versions_from_files ────────────

    #[test]
    fn test_discover_versions_from_files_no_dir() {
        let dir = tempfile::tempdir().unwrap();
        let manager = FileKeyManager::new(dir.path().to_path_buf());

        let versions = manager.discover_versions_from_files(Region::GLOBAL).unwrap();
        assert!(versions.is_empty());
    }

    #[test]
    fn test_discover_versions_from_files_ignores_non_key_files() {
        let dir = tempfile::tempdir().unwrap();
        let region_dir = dir.path().join("global");
        fs::create_dir_all(&region_dir).unwrap();

        // Write valid key files and some non-key files
        fs::write(region_dir.join("v1.key"), [0xAA; 32]).unwrap();
        fs::write(region_dir.join("v2.key"), [0xBB; 32]).unwrap();
        fs::write(region_dir.join("notes.txt"), "not a key").unwrap();
        fs::write(region_dir.join("versions.json"), "[]").unwrap();

        let manager = FileKeyManager::new(dir.path().to_path_buf());
        let versions = manager.discover_versions_from_files(Region::GLOBAL).unwrap();
        assert_eq!(versions, vec![1, 2]);
    }

    // ─── FileKeyManager decommission nonexistent ────────────────

    #[test]
    fn test_file_key_manager_decommission_nonexistent() {
        let dir = tempfile::tempdir().unwrap();
        let manager = FileKeyManager::new(dir.path().to_path_buf());

        assert!(manager.decommission_rmk(Region::GLOBAL, 99).is_err());
    }

    // ─── SecretsManagerKeyManager rotation caches ──────────────

    #[test]
    fn test_secrets_manager_rotate_caches_new_key() {
        let client = MockSecretsClient::new();
        client.add_secret("ledger/rmk/global", 1, &[0xAA; 32]);
        let config = mock_config();
        let manager = SecretsManagerKeyManager::new(Box::new(client), config);

        // Rotate creates version 2
        let v2 = manager.rotate_rmk(Region::GLOBAL).unwrap();
        assert_eq!(v2, 2);

        // Newly rotated key should be retrievable (cached)
        let rmk = manager.rmk_by_version(Region::GLOBAL, 2).unwrap();
        assert_eq!(rmk.version, 2);
    }

    #[test]
    fn test_secrets_manager_decommission_removes_from_cache() {
        let client = MockSecretsClient::new();
        client.add_secret("ledger/rmk/global", 1, &[0xAA; 32]);
        client.add_secret("ledger/rmk/global", 2, &[0xBB; 32]);
        let config = mock_config();
        let manager = SecretsManagerKeyManager::new(Box::new(client), config);

        // Fetch v1 to populate cache
        let _rmk = manager.rmk_by_version(Region::GLOBAL, 1).unwrap();

        // Decommission removes from both backend and cache
        manager.decommission_rmk(Region::GLOBAL, 1).unwrap();

        // Should fail (removed from backend)
        assert!(manager.rmk_by_version(Region::GLOBAL, 1).is_err());
    }

    // ─── validate_rmk_provisioning: all decommissioned ─────────

    #[test]
    fn test_validate_provisioning_all_decommissioned_no_active() {
        // Manually set up a FileKeyManager with versions.json listing only
        // decommissioned versions
        let dir = tempfile::tempdir().unwrap();
        let region_dir = dir.path().join("global");
        fs::create_dir_all(&region_dir).unwrap();

        // Write versions.json with only decommissioned entries
        let entries = vec![VersionSidecarEntry { version: 1, status: RmkStatus::Decommissioned }];
        let data = serde_json::to_string_pretty(&entries).unwrap();
        fs::write(region_dir.join("versions.json"), data).unwrap();

        // Provision the other required regions normally
        let manager = FileKeyManager::new(dir.path().to_path_buf());
        manager.rotate_rmk(Region::US_EAST_VA).unwrap();
        manager.rotate_rmk(Region::US_WEST_OR).unwrap();

        let err = validate_rmk_provisioning(&manager, Region::US_EAST_VA).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("No active"), "expected 'No active' in error message, got: {msg}");
    }

    // ─── InMemoryKeyManager: multi-region scenarios ────────────

    #[test]
    fn test_in_memory_key_manager_multi_region_isolation() {
        let manager = InMemoryKeyManager::new();
        manager.insert(Region::GLOBAL, 1, [0xAA; 32]);
        manager.insert(Region::US_EAST_VA, 1, [0xBB; 32]);
        manager.insert(Region::US_EAST_VA, 2, [0xCC; 32]);

        // GLOBAL should have 1 version
        let global_versions = manager.list_versions(Region::GLOBAL).unwrap();
        assert_eq!(global_versions.len(), 1);

        // US_EAST_VA should have 2 versions
        let va_versions = manager.list_versions(Region::US_EAST_VA).unwrap();
        assert_eq!(va_versions.len(), 2);

        // Decommission from one region doesn't affect another
        manager.decommission_rmk(Region::US_EAST_VA, 1).unwrap();
        assert!(manager.rmk_by_version(Region::US_EAST_VA, 1).is_err());
        assert!(manager.rmk_by_version(Region::GLOBAL, 1).is_ok());
    }

    #[test]
    fn test_in_memory_key_manager_rotate_increments_from_highest() {
        let manager = InMemoryKeyManager::new();
        // Insert non-sequential versions
        manager.insert(Region::GLOBAL, 5, [0xAA; 32]);
        manager.insert(Region::GLOBAL, 10, [0xBB; 32]);

        let new_version = manager.rotate_rmk(Region::GLOBAL).unwrap();
        assert_eq!(new_version, 11, "should increment from highest (10)");
    }

    // ─── SecretsManagerKeyManager: unmapped region ─────────────

    #[test]
    fn test_secrets_manager_rotate_unmapped_region() {
        let client = MockSecretsClient::new();
        let config = mock_config();
        let manager = SecretsManagerKeyManager::new(Box::new(client), config);

        let err = manager.rotate_rmk(Region::DE_CENTRAL_FRANKFURT).unwrap_err();
        assert!(err.to_string().contains("No secret path configured"));
    }

    #[test]
    fn test_secrets_manager_decommission_unmapped_region() {
        let client = MockSecretsClient::new();
        let config = mock_config();
        let manager = SecretsManagerKeyManager::new(Box::new(client), config);

        let err = manager.decommission_rmk(Region::DE_CENTRAL_FRANKFURT, 1).unwrap_err();
        assert!(err.to_string().contains("No secret path configured"));
    }

    #[test]
    fn test_secrets_manager_health_check_unmapped_region() {
        let client = MockSecretsClient::new();
        let config = mock_config();
        let manager = SecretsManagerKeyManager::new(Box::new(client), config);

        let err = manager.health_check(Region::DE_CENTRAL_FRANKFURT).unwrap_err();
        assert!(err.to_string().contains("No secret path configured"));
    }

    // ─── FileKeyManager: rotate multiple regions independently ──

    #[test]
    fn test_file_key_manager_multiple_regions_independent() {
        let dir = tempfile::tempdir().unwrap();
        let manager = FileKeyManager::new(dir.path().to_path_buf());

        // Rotate different numbers of times in different regions
        manager.rotate_rmk(Region::GLOBAL).unwrap();
        manager.rotate_rmk(Region::US_EAST_VA).unwrap();
        manager.rotate_rmk(Region::US_EAST_VA).unwrap();
        manager.rotate_rmk(Region::US_EAST_VA).unwrap();

        let global_versions = manager.list_versions(Region::GLOBAL).unwrap();
        let va_versions = manager.list_versions(Region::US_EAST_VA).unwrap();

        assert_eq!(global_versions.len(), 1);
        assert_eq!(va_versions.len(), 3);

        // Current of each should be the latest
        assert_eq!(manager.current_rmk(Region::GLOBAL).unwrap().version, 1);
        assert_eq!(manager.current_rmk(Region::US_EAST_VA).unwrap().version, 3);
    }

    // ─── rmk_versions_for_health with protected region ─────────

    #[test]
    fn test_rmk_versions_for_health_protected_region() {
        let dir = tempfile::tempdir().unwrap();
        let manager = FileKeyManager::new(dir.path().to_path_buf());

        manager.rotate_rmk(Region::GLOBAL).unwrap();
        manager.rotate_rmk(Region::US_EAST_VA).unwrap();
        manager.rotate_rmk(Region::US_WEST_OR).unwrap();
        manager.rotate_rmk(Region::DE_CENTRAL_FRANKFURT).unwrap();

        // Protected region node needs 4 regions
        let versions = rmk_versions_for_health(&manager, Region::DE_CENTRAL_FRANKFURT);
        assert_eq!(versions.len(), 4);
        assert!(versions.contains_key(&Region::DE_CENTRAL_FRANKFURT));
    }

    // ─── decode_hex edge cases ─────────────────────────────────

    #[test]
    fn test_decode_hex_with_whitespace() {
        // decode_hex trims whitespace
        let hex = format!("  {}  ", "ab".repeat(32));
        let result = EnvKeyManager::decode_hex(&hex);
        assert_eq!(result, Some([0xAB; 32]));
    }

    #[test]
    fn test_decode_hex_mixed_case() {
        // Mix of upper and lowercase hex digits
        let hex = "aAbBcCdDeEfF0011223344556677889900aAbBcCdDeEfF001122334455667788";
        let result = EnvKeyManager::decode_hex(hex);
        assert!(result.is_some());
    }
}
