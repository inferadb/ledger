//! JWT token configuration.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::ConfigError;

// ─── Defaults ────────────────────────────────────────────────────────

fn default_issuer() -> String {
    "inferadb".to_string()
}

const fn default_session_access_ttl() -> u64 {
    1800
}

const fn default_session_refresh_ttl() -> u64 {
    1_209_600
}

const fn default_vault_access_ttl() -> u64 {
    900
}

const fn default_vault_refresh_ttl() -> u64 {
    3600
}

const fn default_clock_skew() -> u64 {
    30
}

const fn default_key_rotation_grace() -> u64 {
    14400
}

const fn default_max_family_lifetime() -> u64 {
    2_592_000 // 30 days
}

// ─── Config ──────────────────────────────────────────────────────────

/// JWT signing and validation configuration.
///
/// Controls token lifetimes, clock skew tolerance, and signing key
/// rotation grace periods. All TTL values are in seconds.
///
/// Fields are `pub` for serde deserialization. Prefer
/// [`JwtConfig::builder()`] for programmatic construction (it enforces
/// constraints). After deserialization, call [`validate()`](Self::validate).
///
/// # Example
///
/// ```no_run
/// # use inferadb_ledger_types::config::JwtConfig;
/// let config = JwtConfig::builder()
///     .issuer("my-cluster")
///     .session_access_ttl_secs(900)
///     .build()
///     .expect("valid JWT config");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct JwtConfig {
    /// JWT issuer claim value.
    #[serde(default = "default_issuer")]
    pub issuer: String,

    /// User session access token TTL in seconds (default: 1800 = 30 min).
    #[serde(default = "default_session_access_ttl")]
    pub session_access_ttl_secs: u64,

    /// User session refresh token TTL in seconds (default: 1209600 = 14 days).
    #[serde(default = "default_session_refresh_ttl")]
    pub session_refresh_ttl_secs: u64,

    /// Vault access token TTL in seconds (default: 900 = 15 min).
    #[serde(default = "default_vault_access_ttl")]
    pub vault_access_ttl_secs: u64,

    /// Vault refresh token TTL in seconds (default: 3600 = 1 hour).
    #[serde(default = "default_vault_refresh_ttl")]
    pub vault_refresh_ttl_secs: u64,

    /// Clock skew leeway for token validation in seconds (default: 30).
    #[serde(default = "default_clock_skew")]
    pub clock_skew_secs: u64,

    /// Grace period after signing key rotation in seconds (default: 14400 = 4 hours).
    ///
    /// Rotated keys remain valid for verification during this window.
    #[serde(default = "default_key_rotation_grace")]
    pub key_rotation_grace_secs: u64,

    /// Maximum lifetime of a refresh token family in seconds (default: 2592000 = 30 days).
    ///
    /// Limits how long a session can be extended by refreshing tokens. After this
    /// duration elapses from the initial authentication, the user must re-authenticate.
    #[serde(default = "default_max_family_lifetime")]
    pub max_family_lifetime_secs: u64,
}

#[bon::bon]
impl JwtConfig {
    /// Creates a new JWT configuration with validation.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Validation`] if any TTL is zero, refresh TTL
    /// is not greater than its corresponding access TTL, or clock skew
    /// exceeds the shortest access TTL.
    #[builder]
    pub fn new(
        #[builder(default = default_issuer(), into)] issuer: String,
        #[builder(default = default_session_access_ttl())] session_access_ttl_secs: u64,
        #[builder(default = default_session_refresh_ttl())] session_refresh_ttl_secs: u64,
        #[builder(default = default_vault_access_ttl())] vault_access_ttl_secs: u64,
        #[builder(default = default_vault_refresh_ttl())] vault_refresh_ttl_secs: u64,
        #[builder(default = default_clock_skew())] clock_skew_secs: u64,
        #[builder(default = default_key_rotation_grace())] key_rotation_grace_secs: u64,
        #[builder(default = default_max_family_lifetime())] max_family_lifetime_secs: u64,
    ) -> Result<Self, ConfigError> {
        let config = Self {
            issuer,
            session_access_ttl_secs,
            session_refresh_ttl_secs,
            vault_access_ttl_secs,
            vault_refresh_ttl_secs,
            clock_skew_secs,
            key_rotation_grace_secs,
            max_family_lifetime_secs,
        };
        config.validate()?;
        Ok(config)
    }
}

impl JwtConfig {
    /// Validates the configuration values.
    ///
    /// Call after deserialization to ensure values are within valid ranges.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Validation`] if any constraint is violated.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.issuer.is_empty() {
            return Err(ConfigError::Validation {
                message: "issuer must not be empty".to_string(),
            });
        }
        if self.session_access_ttl_secs == 0 {
            return Err(ConfigError::Validation {
                message: "session_access_ttl_secs must be > 0".to_string(),
            });
        }
        if self.session_refresh_ttl_secs == 0 {
            return Err(ConfigError::Validation {
                message: "session_refresh_ttl_secs must be > 0".to_string(),
            });
        }
        if self.vault_access_ttl_secs == 0 {
            return Err(ConfigError::Validation {
                message: "vault_access_ttl_secs must be > 0".to_string(),
            });
        }
        if self.vault_refresh_ttl_secs == 0 {
            return Err(ConfigError::Validation {
                message: "vault_refresh_ttl_secs must be > 0".to_string(),
            });
        }
        if self.key_rotation_grace_secs == 0 {
            return Err(ConfigError::Validation {
                message: "key_rotation_grace_secs must be > 0".to_string(),
            });
        }
        if self.session_refresh_ttl_secs <= self.session_access_ttl_secs {
            return Err(ConfigError::Validation {
                message: format!(
                    "session_refresh_ttl_secs ({}) must be > session_access_ttl_secs ({})",
                    self.session_refresh_ttl_secs, self.session_access_ttl_secs
                ),
            });
        }
        if self.vault_refresh_ttl_secs <= self.vault_access_ttl_secs {
            return Err(ConfigError::Validation {
                message: format!(
                    "vault_refresh_ttl_secs ({}) must be > vault_access_ttl_secs ({})",
                    self.vault_refresh_ttl_secs, self.vault_access_ttl_secs
                ),
            });
        }
        if self.max_family_lifetime_secs == 0 {
            return Err(ConfigError::Validation {
                message: "max_family_lifetime_secs must be > 0".to_string(),
            });
        }
        if self.max_family_lifetime_secs < self.session_refresh_ttl_secs {
            return Err(ConfigError::Validation {
                message: format!(
                    "max_family_lifetime_secs ({}) must be >= session_refresh_ttl_secs ({})",
                    self.max_family_lifetime_secs, self.session_refresh_ttl_secs
                ),
            });
        }
        let min_access_ttl = self.session_access_ttl_secs.min(self.vault_access_ttl_secs);
        if self.clock_skew_secs >= min_access_ttl {
            return Err(ConfigError::Validation {
                message: format!(
                    "clock_skew_secs ({}) must be < min access TTL ({})",
                    self.clock_skew_secs, min_access_ttl
                ),
            });
        }

        Ok(())
    }
}

impl Default for JwtConfig {
    fn default() -> Self {
        Self {
            issuer: default_issuer(),
            session_access_ttl_secs: default_session_access_ttl(),
            session_refresh_ttl_secs: default_session_refresh_ttl(),
            vault_access_ttl_secs: default_vault_access_ttl(),
            vault_refresh_ttl_secs: default_vault_refresh_ttl(),
            clock_skew_secs: default_clock_skew(),
            key_rotation_grace_secs: default_key_rotation_grace(),
            max_family_lifetime_secs: default_max_family_lifetime(),
        }
    }
}
