//! Per-organization resource accounting types.

/// Snapshot of per-organization resource consumption.
///
/// Used by the quota checker for enforcement and by operators for
/// capacity planning. All values are point-in-time snapshots from
/// Raft-replicated state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrganizationUsage {
    /// Cumulative estimated storage bytes for this organization.
    ///
    /// Updated on every committed write. Approximate — does not track
    /// exact on-disk overhead.
    pub storage_bytes: u64,
    /// Number of active (non-deleted) vaults in this organization.
    pub vault_count: u32,
}
