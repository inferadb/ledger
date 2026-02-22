//! Per-organization resource quota enforcement.
//!
//! Quotas limit cumulative resource consumption per organization: vault count,
//! estimated storage bytes, and per-organization operation rates. Quota checks
//! run at the service layer before operations enter the Raft pipeline.
//!
//! Quota resolution order:
//! 1. Per-organization quota stored in `OrganizationMeta` (set at creation time)
//! 2. Server-wide `default_quota` from `RuntimeConfig`
//! 3. No quota (unlimited) if neither is set

use inferadb_ledger_types::{OrganizationId, config::OrganizationQuota};

use crate::{log_storage::AppliedStateAccessor, runtime_config::RuntimeConfigHandle};

/// Checks per-organization resource quotas against current usage.
///
/// Resolves the effective quota for an organization by checking the per-organization
/// override first, then falling back to the server-wide default from
/// `RuntimeConfig`. If neither is set, all checks pass (unlimited).
#[derive(Clone)]
pub struct QuotaChecker {
    /// Accessor for applied state (vault counts, organization metadata).
    applied_state: AppliedStateAccessor,
    /// Runtime config for server-wide default quotas.
    runtime_config: Option<RuntimeConfigHandle>,
}

/// Result of a quota check that exceeded the limit.
#[derive(Debug, Clone)]
pub struct QuotaExceeded {
    /// Which resource limit was exceeded.
    pub resource: QuotaResource,
    /// Current usage value.
    pub current: u64,
    /// Maximum allowed value.
    pub limit: u64,
    /// The organization that exceeded its quota.
    pub organization_id: OrganizationId,
}

/// The type of resource that exceeded its quota.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuotaResource {
    /// Maximum number of vaults per organization.
    VaultCount,
    /// Maximum estimated storage bytes per organization.
    StorageBytes,
}

impl std::fmt::Display for QuotaResource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::VaultCount => write!(f, "vault_count"),
            Self::StorageBytes => write!(f, "storage_bytes"),
        }
    }
}

impl std::fmt::Display for QuotaExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Organization {} exceeded {} quota: current={}, limit={}",
            self.organization_id, self.resource, self.current, self.limit
        )
    }
}

impl QuotaChecker {
    /// Creates a new quota checker.
    pub fn new(
        applied_state: AppliedStateAccessor,
        runtime_config: Option<RuntimeConfigHandle>,
    ) -> Self {
        Self { applied_state, runtime_config }
    }

    /// Resolves the effective quota for an organization.
    ///
    /// Returns the per-organization override if set, otherwise the server-wide
    /// default from `RuntimeConfig`, or `None` if neither is configured.
    pub fn effective_quota(&self, organization_id: OrganizationId) -> Option<OrganizationQuota> {
        // Per-organization override takes priority
        if let Some(quota) = self.applied_state.organization_quota(organization_id) {
            return Some(quota);
        }

        // Fall back to server-wide default
        if let Some(ref handle) = self.runtime_config {
            let config = handle.load();
            if let Some(ref default_quota) = config.default_quota {
                return Some(default_quota.clone());
            }
        }

        None
    }

    /// Checks whether creating a new vault would exceed the organization vault quota.
    ///
    /// Returns `Ok(())` if the vault can be created or no quota is configured.
    ///
    /// # Errors
    ///
    /// Returns [`QuotaExceeded`] if the organization has reached its maximum vault count.
    pub fn check_vault_count(&self, organization_id: OrganizationId) -> Result<(), QuotaExceeded> {
        let quota = match self.effective_quota(organization_id) {
            Some(q) => q,
            None => return Ok(()),
        };

        let current = self.applied_state.vault_count(organization_id);
        if current >= quota.max_vaults {
            return Err(QuotaExceeded {
                resource: QuotaResource::VaultCount,
                current: u64::from(current),
                limit: u64::from(quota.max_vaults),
                organization_id,
            });
        }

        Ok(())
    }

    /// Checks whether a write operation's estimated payload would exceed
    /// the organization storage quota.
    ///
    /// Compares `current_usage + estimated_bytes` against `max_storage_bytes`.
    /// Current usage is tracked cumulatively in `AppliedState` and updated
    /// on every committed write.
    ///
    /// `estimated_bytes` is the sum of key + value sizes for all operations
    /// in the write request.
    ///
    /// # Errors
    ///
    /// Returns [`QuotaExceeded`] if `current_usage + estimated_bytes` exceeds
    /// the organization's `max_storage_bytes` quota.
    pub fn check_storage_estimate(
        &self,
        organization_id: OrganizationId,
        estimated_bytes: u64,
    ) -> Result<(), QuotaExceeded> {
        let quota = match self.effective_quota(organization_id) {
            Some(q) => q,
            None => return Ok(()),
        };

        let current_usage = self.applied_state.organization_storage_bytes(organization_id);
        let projected = current_usage.saturating_add(estimated_bytes);
        if projected > quota.max_storage_bytes {
            return Err(QuotaExceeded {
                resource: QuotaResource::StorageBytes,
                current: current_usage,
                limit: quota.max_storage_bytes,
                organization_id,
            });
        }

        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::disallowed_methods)]
mod tests {
    use std::sync::Arc;

    use inferadb_ledger_state::system::OrganizationStatus;
    use inferadb_ledger_types::{OrganizationSlug, ShardId, VaultSlug, config::RuntimeConfig};
    use parking_lot::RwLock;

    use super::*;
    use crate::{
        log_storage::{AppliedState, OrganizationMeta},
        runtime_config::RuntimeConfigHandle,
    };

    fn make_accessor(state: AppliedState) -> AppliedStateAccessor {
        AppliedStateAccessor::new_for_test(Arc::new(RwLock::new(state)))
    }

    fn default_state() -> AppliedState {
        AppliedState::default()
    }

    #[test]
    fn test_no_quota_allows_everything() {
        let state = default_state();
        let checker = QuotaChecker::new(make_accessor(state), None);
        let ns = OrganizationId::new(1);

        assert!(checker.effective_quota(ns).is_none());
        assert!(checker.check_vault_count(ns).is_ok());
        assert!(checker.check_storage_estimate(ns, u64::MAX).is_ok());
    }

    #[test]
    fn test_per_organization_quota_overrides_default() {
        let mut state = default_state();
        let ns = OrganizationId::new(1);
        let ns_quota = OrganizationQuota {
            max_vaults: 5,
            max_storage_bytes: 1000,
            max_write_ops_per_sec: 100,
            max_read_ops_per_sec: 500,
        };
        state.organizations.insert(
            ns,
            OrganizationMeta {
                organization_id: ns,
                slug: OrganizationSlug::new(1),
                name: "test".to_owned(),
                shard_id: ShardId::new(0),
                status: OrganizationStatus::Active,
                pending_shard_id: None,
                quota: Some(ns_quota.clone()),
            },
        );

        // Set a different server-wide default to prove per-organization wins
        let server_default = OrganizationQuota {
            max_vaults: 100,
            max_storage_bytes: 999_999,
            max_write_ops_per_sec: 1000,
            max_read_ops_per_sec: 5000,
        };
        let runtime_config = RuntimeConfigHandle::new(
            RuntimeConfig::builder().default_quota(server_default).build(),
        );

        let checker = QuotaChecker::new(make_accessor(state), Some(runtime_config));
        let effective = checker.effective_quota(ns);
        assert_eq!(effective, Some(ns_quota));
    }

    #[test]
    fn test_vault_count_quota_enforcement() {
        let mut state = default_state();
        let ns = OrganizationId::new(1);
        state.organizations.insert(
            ns,
            OrganizationMeta {
                organization_id: ns,
                slug: OrganizationSlug::new(1),
                name: "test".to_owned(),
                shard_id: ShardId::new(0),
                status: OrganizationStatus::Active,
                pending_shard_id: None,
                quota: Some(OrganizationQuota {
                    max_vaults: 2,
                    max_storage_bytes: u64::MAX,
                    max_write_ops_per_sec: u32::MAX,
                    max_read_ops_per_sec: u32::MAX,
                }),
            },
        );

        // Add 2 active vaults
        use inferadb_ledger_types::VaultId;

        use crate::{log_storage::VaultMeta, types::BlockRetentionPolicy};

        for i in 1..=2 {
            let vid = VaultId::new(i);
            state.vaults.insert(
                (ns, vid),
                VaultMeta {
                    organization_id: ns,
                    vault_id: vid,
                    slug: VaultSlug::new(vid.value() as u64),
                    name: None,
                    deleted: false,
                    last_write_timestamp: 0,
                    retention_policy: BlockRetentionPolicy::default(),
                },
            );
        }

        let checker = QuotaChecker::new(make_accessor(state), None);

        // Should fail — already at max
        let err = checker.check_vault_count(ns).unwrap_err();
        assert_eq!(err.resource, QuotaResource::VaultCount);
        assert_eq!(err.current, 2);
        assert_eq!(err.limit, 2);
    }

    #[test]
    fn test_vault_count_deleted_vaults_not_counted() {
        let mut state = default_state();
        let ns = OrganizationId::new(1);
        state.organizations.insert(
            ns,
            OrganizationMeta {
                organization_id: ns,
                slug: OrganizationSlug::new(1),
                name: "test".to_owned(),
                shard_id: ShardId::new(0),
                status: OrganizationStatus::Active,
                pending_shard_id: None,
                quota: Some(OrganizationQuota {
                    max_vaults: 2,
                    max_storage_bytes: u64::MAX,
                    max_write_ops_per_sec: u32::MAX,
                    max_read_ops_per_sec: u32::MAX,
                }),
            },
        );

        use inferadb_ledger_types::VaultId;

        use crate::{log_storage::VaultMeta, types::BlockRetentionPolicy};

        // 1 active + 1 deleted = only 1 counted
        state.vaults.insert(
            (ns, VaultId::new(1)),
            VaultMeta {
                organization_id: ns,
                vault_id: VaultId::new(1),
                slug: VaultSlug::new(1),
                name: None,
                deleted: false,
                last_write_timestamp: 0,
                retention_policy: BlockRetentionPolicy::default(),
            },
        );
        state.vaults.insert(
            (ns, VaultId::new(2)),
            VaultMeta {
                organization_id: ns,
                vault_id: VaultId::new(2),
                slug: VaultSlug::new(2),
                name: None,
                deleted: true,
                last_write_timestamp: 0,
                retention_policy: BlockRetentionPolicy::default(),
            },
        );

        let checker = QuotaChecker::new(make_accessor(state), None);
        assert!(checker.check_vault_count(ns).is_ok());
    }

    #[test]
    fn test_storage_estimate_quota_enforcement() {
        let mut state = default_state();
        let ns = OrganizationId::new(1);
        state.organizations.insert(
            ns,
            OrganizationMeta {
                organization_id: ns,
                slug: OrganizationSlug::new(1),
                name: "test".to_owned(),
                shard_id: ShardId::new(0),
                status: OrganizationStatus::Active,
                pending_shard_id: None,
                quota: Some(OrganizationQuota {
                    max_vaults: u32::MAX,
                    max_storage_bytes: 1_000_000, // 1 MB
                    max_write_ops_per_sec: u32::MAX,
                    max_read_ops_per_sec: u32::MAX,
                }),
            },
        );

        let checker = QuotaChecker::new(make_accessor(state), None);

        // Under limit — passes (current=0, estimated=500k)
        assert!(checker.check_storage_estimate(ns, 500_000).is_ok());

        // Over limit — fails (current=0, estimated=2M > 1M limit)
        let err = checker.check_storage_estimate(ns, 2_000_000).unwrap_err();
        assert_eq!(err.resource, QuotaResource::StorageBytes);
        assert_eq!(err.current, 0); // no prior usage
        assert_eq!(err.limit, 1_000_000);
    }

    #[test]
    fn test_storage_estimate_cumulative_tracking() {
        let mut state = default_state();
        let ns = OrganizationId::new(1);
        state.organizations.insert(
            ns,
            OrganizationMeta {
                organization_id: ns,
                slug: OrganizationSlug::new(1),
                name: "test".to_owned(),
                shard_id: ShardId::new(0),
                status: OrganizationStatus::Active,
                pending_shard_id: None,
                quota: Some(OrganizationQuota {
                    max_vaults: u32::MAX,
                    max_storage_bytes: 1_000_000,
                    max_write_ops_per_sec: u32::MAX,
                    max_read_ops_per_sec: u32::MAX,
                }),
            },
        );

        // Simulate 800KB already used
        state.organization_storage_bytes.insert(ns, 800_000);
        let checker = QuotaChecker::new(make_accessor(state), None);

        // 100KB more — fits (800k + 100k = 900k < 1M)
        assert!(checker.check_storage_estimate(ns, 100_000).is_ok());

        // 300KB more — exceeds (800k + 300k = 1.1M > 1M)
        let err = checker.check_storage_estimate(ns, 300_000).unwrap_err();
        assert_eq!(err.resource, QuotaResource::StorageBytes);
        assert_eq!(err.current, 800_000);
        assert_eq!(err.limit, 1_000_000);
    }

    #[test]
    fn test_quota_exceeded_display() {
        let exceeded = QuotaExceeded {
            resource: QuotaResource::VaultCount,
            current: 10,
            limit: 5,
            organization_id: OrganizationId::new(42),
        };
        let display = format!("{}", exceeded);
        assert!(display.contains("42"));
        assert!(display.contains("vault_count"));
        assert!(display.contains("current=10"));
        assert!(display.contains("limit=5"));
    }

    #[test]
    fn test_server_default_quota_fallback() {
        // Organization exists but has no per-organization quota
        let mut state = default_state();
        let ns = OrganizationId::new(1);
        state.organizations.insert(
            ns,
            OrganizationMeta {
                organization_id: ns,
                slug: OrganizationSlug::new(1),
                name: "test".to_owned(),
                shard_id: ShardId::new(0),
                status: OrganizationStatus::Active,
                pending_shard_id: None,
                quota: None,
            },
        );

        // Server-wide default quota should be used as fallback
        let server_default = OrganizationQuota {
            max_vaults: 10,
            max_storage_bytes: 500_000,
            max_write_ops_per_sec: 200,
            max_read_ops_per_sec: 1000,
        };
        let runtime_config = RuntimeConfigHandle::new(
            RuntimeConfig::builder().default_quota(server_default.clone()).build(),
        );

        let checker = QuotaChecker::new(make_accessor(state), Some(runtime_config));

        // Effective quota is the server default (not None)
        assert_eq!(checker.effective_quota(ns), Some(server_default));
        // Vault count check passes (0 vaults < 10 limit)
        assert!(checker.check_vault_count(ns).is_ok());
        // Storage check passes under limit (current=0, estimated=100k < 500k)
        assert!(checker.check_storage_estimate(ns, 100_000).is_ok());
        // Storage check fails over limit (current=0, estimated=1M > 500k)
        assert!(checker.check_storage_estimate(ns, 1_000_000).is_err());
    }
}
