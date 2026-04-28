//! Service layer for `_system` organization operations.
//!
//! Provides high-level operations on the `_system` organization:
//! - Node registration and discovery
//! - Organization routing table management
//! - Sequence counter management for ID generation
//!
//! The `_system` organization uses `organization_id = 0` and `vault_id = 0`.

use std::sync::Arc;

use inferadb_ledger_store::StorageBackend;
use inferadb_ledger_types::{Operation, OrganizationId, VaultId};
use snafu::{ResultExt, Snafu};

use super::{
    keys::{KeyTier, SystemKeys},
    types::SigningKey,
};
use crate::state::{StateError, StateLayer};

pub mod audit;
mod credentials;
mod email;
mod erasure;
mod invitations;
mod migration;
mod nodes;
mod onboarding;
mod organizations;
mod user_directory;
mod users;
mod vault_slugs;

/// The reserved organization ID for `_system`.
pub const SYSTEM_ORGANIZATION_ID: OrganizationId = OrganizationId::new(0);

/// The reserved vault ID for `_system` entities.
pub const SYSTEM_VAULT_ID: VaultId = VaultId::new(0);

/// Errors from system organization operations.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(super)))]
pub enum SystemError {
    /// Underlying state layer operation failed.
    #[snafu(display("State layer error: {source}"))]
    State {
        /// The underlying state layer error.
        #[snafu(source(from(StateError, Box::new)))]
        source: Box<StateError>,
    },

    /// Codec error during serialization/deserialization.
    #[snafu(display("Codec error: {source}"))]
    Codec {
        /// The underlying codec error.
        source: inferadb_ledger_types::CodecError,
    },

    /// Requested entity was not found.
    #[snafu(display("Not found: {entity}"))]
    NotFound {
        /// Description of the entity that was not found.
        entity: String,
    },

    /// Entity already exists (duplicate key).
    #[snafu(display("Already exists: {entity}"))]
    AlreadyExists {
        /// Description of the entity that already exists.
        entity: String,
    },

    /// A revocation scan was truncated at `MAX_TOKEN_SCAN`, meaning not all
    /// tokens were processed. This is a security error — partial revocation
    /// leaves tokens valid that should have been revoked.
    #[snafu(display(
        "Revocation scan truncated at {limit} entries for {operation} — {revoked} tokens revoked before truncation"
    ))]
    RevocationIncomplete {
        /// The operation that was truncated.
        operation: String,
        /// The scan limit that was hit.
        limit: usize,
        /// Number of tokens successfully revoked before truncation.
        revoked: u64,
    },

    /// A precondition for the operation was not met.
    ///
    /// Examples: cannot delete last credential, challenge expired,
    /// max TOTP attempts exceeded.
    #[snafu(display("Failed precondition: {message}"))]
    FailedPrecondition {
        /// Description of the violated precondition.
        message: String,
    },

    /// A resource limit has been reached.
    ///
    /// Example: too many active TOTP challenges for a user.
    #[snafu(display("Resource exhausted: {message}"))]
    ResourceExhausted {
        /// Description of the exhausted resource.
        message: String,
    },

    /// A storage key was written to the wrong tier (GLOBAL vs REGIONAL).
    ///
    /// This indicates a data residency bug — the key would land in the
    /// wrong state layer, potentially leaking PII to GLOBAL or breaking
    /// cross-region resolution.
    #[snafu(display("Key tier mismatch: {message}"))]
    TierMismatch {
        /// Descriptive message from `validate_key_tier`.
        message: String,
    },
}

/// Result type for system organization operations.
pub type Result<T> = std::result::Result<T, SystemError>;

/// Validates that a key belongs to the expected tier, returning a
/// [`SystemError::TierMismatch`] on violation.
pub(super) fn require_tier(key: &str, expected: KeyTier) -> Result<()> {
    SystemKeys::validate_key_tier(key, expected)
        .map_err(|message| SystemError::TierMismatch { message })
}

/// Process-wide cache for signing keys keyed by kid (UUID).
///
/// Created once per process (typically on `RaftManager` and the
/// `ServiceContext`) and shared across every `SystemOrganizationService`
/// constructed on the hot path. The kid namespace is globally unique so
/// sharing the cache across regions and call sites is correct.
///
/// Constructing a fresh `moka::sync::Cache` per request triggers
/// `crossbeam_epoch::Global::try_advance` GC churn that dominated CPU
/// self-time on the write path before this type was introduced — see
/// `docs/superpowers/specs/2026-04-27-reactor-wal-investigation.md`.
pub type SigningKeyCache = Arc<moka::sync::Cache<String, SigningKey>>;

/// Builds a fresh process-wide [`SigningKeyCache`] with the standard
/// capacity (100 entries) and TTL (60s).
///
/// Server bootstrap calls this exactly once and threads the resulting
/// `Arc` through `RaftManager` and `ServiceContext`. Tests and helpers
/// that need a private cache can also use this constructor.
#[must_use]
pub fn new_signing_key_cache() -> SigningKeyCache {
    Arc::new(
        moka::sync::Cache::builder()
            .max_capacity(100)
            .time_to_live(std::time::Duration::from_secs(60))
            .build(),
    )
}

/// Service for reading from and writing to the `_system` organization.
///
/// All `_system` data is stored in `organization_id=0`, `vault_id=0`.
/// [`StateLayer`] is internally thread-safe via `inferadb-ledger-store`'s MVCC.
pub struct SystemOrganizationService<B: StorageBackend> {
    pub(super) state: Arc<StateLayer<B>>,
    /// Shared in-memory cache for signing keys keyed by kid (UUID key identifier).
    ///
    /// Avoids two B+ tree lookups per JWT verification on the hot path.
    /// Sharing the `Arc` across requests prevents per-request `moka::Cache`
    /// construct/destroy churn that previously dominated CPU self-time.
    pub(super) signing_key_cache: SigningKeyCache,
}

impl<B: StorageBackend> SystemOrganizationService<B> {
    /// Creates a new system organization service with a private signing-key cache.
    ///
    /// Allocates a fresh [`SigningKeyCache`] for this service instance. Suitable
    /// for cold paths (admin, maintenance, recovery, tests) where no shared cache
    /// is available. Hot paths (per-request handlers, routing) MUST use
    /// [`with_signing_key_cache`](Self::with_signing_key_cache) instead, passing a
    /// long-lived cache from `RaftManager` or `ServiceContext`.
    pub fn new(state: Arc<StateLayer<B>>) -> Self {
        Self::with_signing_key_cache(state, new_signing_key_cache())
    }

    /// Creates a new system organization service that shares an existing signing-key cache.
    ///
    /// Pass the long-lived cache from `RaftManager::signing_key_cache()` or
    /// `ServiceContext::signing_key_cache` to avoid the per-request `moka::Cache`
    /// allocation that triggers `crossbeam_epoch::Global::try_advance` GC churn.
    pub fn with_signing_key_cache(
        state: Arc<StateLayer<B>>,
        signing_key_cache: SigningKeyCache,
    ) -> Self {
        Self { state, signing_key_cache }
    }

    // =========================================================================
    // Sequence Counters
    // =========================================================================

    /// Returns the current value of a sequence counter, then increments it.
    ///
    /// On first call (counter absent), returns `start_value` and stores `start_value + 1`.
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::State`] if the underlying read or write fails.
    pub fn next_sequence(&self, key: &str, start_value: i64) -> Result<i64> {
        // StateLayer is internally thread-safe via inferadb-ledger-store MVCC
        // Read current value
        let entity_opt =
            self.state.get_entity(SYSTEM_VAULT_ID, key.as_bytes()).context(StateSnafu)?;
        let current = match entity_opt {
            Some(entity) => {
                let value_str = String::from_utf8_lossy(&entity.value);
                value_str.parse::<i64>().unwrap_or(start_value)
            },
            None => start_value,
        };

        // Increment and save
        let next_value = current + 1;
        let ops = vec![Operation::SetEntity {
            key: key.to_string(),
            value: next_value.to_string().into_bytes(),
            condition: None,
            expires_at: None,
        }];

        self.state
            .apply_operations_lazy(SYSTEM_VAULT_ID, &ops, 0) // height 0 for system ops
            .context(StateSnafu)?;

        Ok(current)
    }

    /// Returns the next organization ID.
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::State`] if the sequence counter read or write fails.
    pub fn next_organization_id(&self) -> Result<OrganizationId> {
        // Start at 1 because 0 is reserved for _system
        self.next_sequence(SystemKeys::ORG_SEQ_KEY, 1).map(OrganizationId::new)
    }

    /// Returns the next vault ID.
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::State`] if the sequence counter read or write fails.
    pub fn next_vault_id(&self) -> Result<VaultId> {
        self.next_sequence(SystemKeys::VAULT_SEQ_KEY, 1).map(VaultId::new)
    }
}
