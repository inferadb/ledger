//! Core types for the Raft state machine.
//!
//! Defines [`AppliedState`], [`OrganizationMeta`], [`VaultMeta`],
//! [`SequenceCounters`], and snapshot types used by the log store.

use std::collections::HashMap;

use inferadb_ledger_state::system::OrganizationStatus;
use inferadb_ledger_types::{
    Hash, Operation, OrganizationId, OrganizationSlug, ShardId, Transaction, VaultId, VaultSlug,
};
use openraft::{LogId, StoredMembership};
use serde::{Deserialize, Serialize};

use crate::types::{BlockRetentionPolicy, LedgerNodeId};

// ============================================================================
// Applied State (State Machine)
// ============================================================================

/// Applied state that is tracked for snapshots.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AppliedState {
    /// Last applied log ID.
    pub last_applied: Option<LogId<LedgerNodeId>>,
    /// Stored membership configuration.
    pub membership: StoredMembership<LedgerNodeId, openraft::BasicNode>,
    /// Sequence counters for ID generation.
    pub sequences: SequenceCounters,
    /// Per-vault heights for deterministic block heights.
    pub vault_heights: HashMap<(OrganizationId, VaultId), u64>,
    /// Vault health status.
    pub vault_health: HashMap<(OrganizationId, VaultId), VaultHealthStatus>,
    /// Organization registry (replicated via Raft).
    pub organizations: HashMap<OrganizationId, OrganizationMeta>,
    /// Vault registry (replicated via Raft).
    pub vaults: HashMap<(OrganizationId, VaultId), VaultMeta>,
    /// Previous vault block hashes (for chaining).
    pub previous_vault_hashes: HashMap<(OrganizationId, VaultId), Hash>,
    /// Current shard height for block creation.
    #[serde(default)]
    pub shard_height: u64,
    /// Previous shard hash for chain continuity.
    #[serde(default)]
    pub previous_shard_hash: Hash,
    /// Per-client sequence tracking with cross-failover idempotency metadata.
    /// Key: (organization, vault, client_id), Value: sequence + dedup fields.
    #[serde(default)]
    pub client_sequences: HashMap<(OrganizationId, VaultId, String), ClientSequenceEntry>,
    /// Cumulative estimated storage bytes per organization.
    ///
    /// Updated on every write (increment by key+value sizes) and delete
    /// (decrement by key size). This is an estimate — not exact byte-level
    /// accounting — but sufficient for quota enforcement and capacity planning.
    ///
    /// Replicated via Raft consensus and survives restarts via snapshot.
    #[serde(default)]
    pub organization_storage_bytes: HashMap<OrganizationId, u64>,
    /// Organization slug → internal ID mapping for fast resolution.
    #[serde(default)]
    pub slug_index: HashMap<OrganizationSlug, OrganizationId>,
    /// Internal organization ID → slug reverse mapping for response construction.
    #[serde(default)]
    pub id_to_slug: HashMap<OrganizationId, OrganizationSlug>,
    /// Vault slug → internal vault ID mapping for fast resolution.
    #[serde(default)]
    pub vault_slug_index: HashMap<VaultSlug, VaultId>,
    /// Internal vault ID → slug reverse mapping for response construction.
    #[serde(default)]
    pub vault_id_to_slug: HashMap<VaultId, VaultSlug>,
    /// Deterministic timestamp (nanoseconds since epoch) from the last applied
    /// Raft entry's `proposed_at`. Used as the reference "now" for snapshot event
    /// collection — ensures two snapshots of the same state produce identical
    /// event sets regardless of wall-clock time.
    #[serde(default)]
    pub last_applied_timestamp_ns: i64,
}

/// Metadata for an organization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrganizationMeta {
    /// Internal organization identifier (`OrganizationId`).
    pub organization: OrganizationId,
    /// External slug for API lookups.
    pub slug: OrganizationSlug,
    /// Human-readable name.
    pub name: String,
    /// Shard hosting this organization (0 for default).
    pub shard_id: ShardId,
    /// Organization lifecycle status.
    #[serde(default)]
    pub status: OrganizationStatus,
    /// Target shard for pending migration (only set when status is Migrating).
    #[serde(default)]
    pub pending_shard_id: Option<ShardId>,
    /// Per-organization resource quota (None means use server default).
    #[serde(default)]
    pub quota: Option<inferadb_ledger_types::config::OrganizationQuota>,
    /// Accumulated storage bytes for this organization.
    ///
    /// Persisted within the `OrganizationMeta` table entry so it survives restarts
    /// without requiring a separate table.
    #[serde(default)]
    pub storage_bytes: u64,
}

/// Metadata for a vault.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VaultMeta {
    /// Internal organization identifier (`OrganizationId`) owning this vault.
    pub organization: OrganizationId,
    /// Internal vault identifier (`VaultId`).
    pub vault: VaultId,
    /// External Snowflake slug for API lookups (generated before Raft proposal).
    pub slug: VaultSlug,
    /// Human-readable name (optional).
    pub name: Option<String>,
    /// Whether the vault is deleted.
    pub deleted: bool,
    /// Timestamp of last write (Unix seconds).
    #[serde(default)]
    pub last_write_timestamp: u64,
    /// Block retention policy for this vault.
    #[serde(default)]
    pub retention_policy: BlockRetentionPolicy,
}

/// Per-client sequence tracking entry with idempotency metadata.
///
/// Stores the high-water sequence number along with cross-failover
/// deduplication fields. The moka cache remains the fast path for
/// same-leader retries; this replicated entry provides deduplication
/// across leader changes within the TTL window.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientSequenceEntry {
    /// High-water sequence number for this client.
    #[serde(default)]
    pub sequence: u64,
    /// Last seen timestamp (Unix seconds from apply-phase `proposed_at`).
    /// Used for TTL eviction: entries where `proposed_at - last_seen > ttl` are evicted.
    #[serde(default)]
    pub last_seen: i64,
    /// Most recent idempotency UUID (16 bytes).
    /// Used for cross-failover duplicate detection.
    #[serde(default)]
    pub last_idempotency_key: [u8; 16],
    /// Hash of last request payload (seahash).
    /// Used to distinguish retries (same hash) from key reuse (different hash).
    #[serde(default)]
    pub last_request_hash: u64,
}

/// Sequence counters for deterministic ID generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SequenceCounters {
    /// Next organization ID (starts at 1, 0 = _system).
    pub organization: OrganizationId,
    /// Next vault ID.
    pub vault: VaultId,
    /// Next user ID.
    pub user: i64,
    /// Next user email ID.
    pub user_email: i64,
    /// Next email verification token ID.
    pub email_verify: i64,
}

impl Default for SequenceCounters {
    fn default() -> Self {
        Self {
            organization: OrganizationId::new(0),
            vault: VaultId::new(0),
            user: 0,
            user_email: 0,
            email_verify: 0,
        }
    }
}

impl SequenceCounters {
    /// Creates new counters with initial values.
    pub fn new() -> Self {
        Self {
            organization: OrganizationId::new(1),
            vault: VaultId::new(1),
            user: 1,
            user_email: 1,
            email_verify: 1,
        }
    }

    /// Returns and increments the next organization ID.
    pub fn next_organization(&mut self) -> OrganizationId {
        let id = self.organization;
        self.organization = OrganizationId::new(id.value() + 1);
        id
    }

    /// Returns and increments the next vault ID.
    pub fn next_vault(&mut self) -> VaultId {
        let id = self.vault;
        self.vault = VaultId::new(id.value() + 1);
        id
    }

    /// Returns and increments the next user ID.
    pub fn next_user(&mut self) -> i64 {
        let id = self.user;
        self.user += 1;
        id
    }

    /// Returns and increments the next user email ID.
    pub fn next_user_email(&mut self) -> i64 {
        let id = self.user_email;
        self.user_email += 1;
        id
    }

    /// Returns and increments the next email verification token ID.
    pub fn next_email_verify(&mut self) -> i64 {
        let id = self.email_verify;
        self.email_verify += 1;
        id
    }
}

// ============================================================================
// Applied State Core (Persistence-Only Subset)
// ============================================================================

/// Minimal persistence struct containing only the fixed-size structural fields
/// from [`AppliedState`].
///
/// This replaces the full `AppliedState` blob in the `RaftState` B+ tree table.
/// The remaining fields (11 `HashMap`s + `SequenceCounters`) are externalized
/// to dedicated B+ tree tables, keeping this blob well under any page size
/// regardless of cluster scale.
///
/// `AppliedStateCore` is only for persistence — the in-memory `AppliedState`
/// remains the authoritative read path.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AppliedStateCore {
    /// Last applied log ID.
    pub last_applied: Option<LogId<LedgerNodeId>>,
    /// Stored membership configuration.
    pub membership: StoredMembership<LedgerNodeId, openraft::BasicNode>,
    /// Current shard height for block creation.
    pub shard_height: u64,
    /// Previous shard hash for chain continuity.
    pub previous_shard_hash: Hash,
    /// Deterministic timestamp (nanoseconds since epoch) from the last applied
    /// Raft entry's `proposed_at`. Used for snapshot event collection cutoff.
    #[serde(default)]
    pub last_applied_timestamp_ns: i64,
}

// ============================================================================
// Pending External Writes (Apply-Phase Accumulator)
// ============================================================================

/// Accumulator for B+ tree write operations generated during the apply phase.
///
/// Each Raft apply batch processes log entries that mutate [`AppliedState`]
/// in-memory. Corresponding B+ tree writes are accumulated here and flushed
/// in a single `WriteTransaction` alongside the `AppliedStateCore` blob,
/// ensuring atomicity.
///
/// Stack-allocated per apply batch — no heap allocation if no fields are dirty
/// (all fields are empty `Vec`s initially).
#[derive(Debug, Clone, Default)]
pub struct PendingExternalWrites {
    /// `OrganizationMeta` table inserts/updates (full serialized blob including slug +
    /// storage_bytes).
    pub organizations: Vec<(OrganizationId, Vec<u8>)>,
    /// `OrganizationMeta` table deletes.
    pub organizations_deleted: Vec<OrganizationId>,
    /// `VaultMeta` table inserts/updates (keyed by vault_id alone, blob includes org_id + slug).
    pub vaults: Vec<(VaultId, Vec<u8>)>,
    /// `VaultMeta` table deletes.
    pub vaults_deleted: Vec<VaultId>,
    /// `VaultHeights` table inserts/updates (composite key: org_id + vault_id).
    pub vault_heights: Vec<((OrganizationId, VaultId), u64)>,
    /// `VaultHashes` table inserts/updates (composite key: org_id + vault_id).
    pub vault_hashes: Vec<((OrganizationId, VaultId), Hash)>,
    /// `VaultHealth` table inserts/updates (composite key: org_id + vault_id).
    pub vault_health: Vec<((OrganizationId, VaultId), VaultHealthStatus)>,
    /// `Sequences` table inserts/updates (individual keys like "organization", "vault", etc.).
    pub sequences: Vec<(String, u64)>,
    /// `ClientSequences` table inserts/updates (composite bytes key, postcard value).
    pub client_sequences: Vec<(Vec<u8>, Vec<u8>)>,
    /// `ClientSequences` table deletes (for eviction).
    pub client_sequences_deleted: Vec<Vec<u8>>,
    /// `OrganizationSlugIndex` table inserts/updates.
    pub slug_index: Vec<(OrganizationSlug, OrganizationId)>,
    /// `OrganizationSlugIndex` table deletes (on org deletion).
    pub slug_index_deleted: Vec<OrganizationSlug>,
    /// `VaultSlugIndex` table inserts/updates.
    pub vault_slug_index: Vec<(VaultSlug, VaultId)>,
    /// `VaultSlugIndex` table deletes (on vault deletion).
    pub vault_slug_index_deleted: Vec<VaultSlug>,
}

impl PendingExternalWrites {
    /// Creates an empty accumulator with no pending writes.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns `true` when no writes are pending across any table.
    pub fn is_empty(&self) -> bool {
        self.organizations.is_empty()
            && self.organizations_deleted.is_empty()
            && self.vaults.is_empty()
            && self.vaults_deleted.is_empty()
            && self.vault_heights.is_empty()
            && self.vault_hashes.is_empty()
            && self.vault_health.is_empty()
            && self.sequences.is_empty()
            && self.client_sequences.is_empty()
            && self.client_sequences_deleted.is_empty()
            && self.slug_index.is_empty()
            && self.slug_index_deleted.is_empty()
            && self.vault_slug_index.is_empty()
            && self.vault_slug_index_deleted.is_empty()
    }

    /// Encodes a composite key for vault-scoped tables (`VaultHeights`,
    /// `VaultHashes`, `VaultHealth`).
    ///
    /// Format: `{organization:8BE}{vault:8BE}` (16 bytes total).
    ///
    /// Big-endian encoding ensures lexicographic byte ordering matches numeric
    /// ordering for non-negative IDs, enabling efficient B+ tree range scans
    /// scoped to a single organization.
    ///
    /// * `organization` - Internal organization identifier (`OrganizationId`).
    /// * `vault` - Internal vault identifier (`VaultId`).
    pub fn vault_composite_key(organization: OrganizationId, vault: VaultId) -> Vec<u8> {
        let mut key = Vec::with_capacity(16);
        key.extend_from_slice(&organization.value().to_be_bytes());
        key.extend_from_slice(&vault.value().to_be_bytes());
        key
    }

    /// Encodes a composite key for the `ClientSequences` table.
    ///
    /// Format: `{organization:8BE}{vault:8BE}{client_id:raw}` (16 + len bytes).
    ///
    /// The 16-byte fixed-length prefix ensures no collisions between different
    /// org/vault combinations regardless of `client_id` content (including
    /// `\0` bytes). The `client_id` is appended raw without a length prefix
    /// because it occupies the key suffix — no ambiguity is possible.
    ///
    /// * `organization` - Internal organization identifier (`OrganizationId`).
    /// * `vault` - Internal vault identifier (`VaultId`).
    pub fn client_sequence_key(
        organization: OrganizationId,
        vault: VaultId,
        client_id: &[u8],
    ) -> Vec<u8> {
        let mut key = Vec::with_capacity(16 + client_id.len());
        key.extend_from_slice(&organization.value().to_be_bytes());
        key.extend_from_slice(&vault.value().to_be_bytes());
        key.extend_from_slice(client_id);
        key
    }
}

impl From<&AppliedState> for AppliedStateCore {
    fn from(state: &AppliedState) -> Self {
        Self {
            last_applied: state.last_applied,
            membership: state.membership.clone(),
            shard_height: state.shard_height,
            previous_shard_hash: state.previous_shard_hash,
            last_applied_timestamp_ns: state.last_applied_timestamp_ns,
        }
    }
}

/// Selects the least-loaded shard for a new organization.
///
/// Returns the shard with the fewest active (non-deleted) organizations.
/// Tie-breaker: lowest shard_id wins.
/// Fallback: shard 0 if no organizations exist.
pub fn select_least_loaded_shard(
    organizations: &HashMap<OrganizationId, OrganizationMeta>,
) -> ShardId {
    // Count active organizations per shard
    let mut shard_counts: HashMap<ShardId, usize> = HashMap::new();

    for meta in organizations.values() {
        if meta.status != OrganizationStatus::Deleted {
            *shard_counts.entry(meta.shard_id).or_insert(0) += 1;
        }
    }

    // If no organizations exist, default to shard 0
    if shard_counts.is_empty() {
        return ShardId::new(0);
    }

    // Find shard with minimum count, preferring lower shard_id on ties
    shard_counts
        .into_iter()
        .min_by(|(shard_a, count_a), (shard_b, count_b)| {
            count_a.cmp(count_b).then_with(|| shard_a.cmp(shard_b))
        })
        .map(|(shard_id, _)| shard_id)
        .unwrap_or(ShardId::new(0))
}

/// Estimates the net storage delta (in bytes) for a batch of transactions.
///
/// Sets and relationship creates add bytes (key + value sizes).
/// Deletes subtract bytes (key sizes only — we don't know the old value size).
/// Returns a signed value: positive for net growth, negative for net shrinkage.
pub(super) fn estimate_write_storage_delta(transactions: &[Transaction]) -> i64 {
    let mut delta: i64 = 0;
    for tx in transactions {
        for op in &tx.operations {
            match op {
                Operation::SetEntity { key, value, .. } => {
                    delta = delta.saturating_add(key.len() as i64);
                    delta = delta.saturating_add(value.len() as i64);
                },
                Operation::DeleteEntity { key } => {
                    // Subtract estimated key bytes (conservative: we don't know
                    // the value size being removed, only the key)
                    delta = delta.saturating_sub(key.len() as i64);
                },
                Operation::CreateRelationship { resource, relation, subject } => {
                    delta = delta.saturating_add(resource.len() as i64);
                    delta = delta.saturating_add(relation.len() as i64);
                    delta = delta.saturating_add(subject.len() as i64);
                },
                Operation::DeleteRelationship { resource, relation, subject } => {
                    delta = delta.saturating_sub(resource.len() as i64);
                    delta = delta.saturating_sub(relation.len() as i64);
                    delta = delta.saturating_sub(subject.len() as i64);
                },
                Operation::ExpireEntity { key, .. } => {
                    // TTL expiration removes the entity
                    delta = delta.saturating_sub(key.len() as i64);
                },
            }
        }
    }
    delta
}

/// Maximum recovery attempts before requiring manual intervention.
pub const MAX_RECOVERY_ATTEMPTS: u8 = 3;

/// Health status for a vault.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum VaultHealthStatus {
    /// Vault is healthy and accepting writes.
    #[default]
    Healthy,
    /// Vault has diverged and needs recovery.
    Diverged {
        /// Expected state root.
        expected: Hash,
        /// Actual computed state root.
        computed: Hash,
        /// Height at which divergence was detected.
        at_height: u64,
    },
    /// Vault is currently recovering from divergence.
    Recovering {
        /// When recovery started (Unix timestamp).
        started_at: i64,
        /// Current recovery attempt (1-based, max MAX_RECOVERY_ATTEMPTS).
        attempt: u8,
    },
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods, clippy::panic)]
mod tests {
    use std::collections::BTreeMap;

    use openraft::{CommittedLeaderId, LogId, Membership, StoredMembership};

    use super::*;

    /// Helper: build a StoredMembership with the given node count per config.
    fn make_membership(
        nodes_per_config: &[usize],
    ) -> StoredMembership<LedgerNodeId, openraft::BasicNode> {
        use std::collections::BTreeSet;

        // Collect all nodes across all configs into a single node map
        let mut all_nodes = BTreeMap::new();
        let mut configs: Vec<BTreeSet<u64>> = Vec::new();

        for &count in nodes_per_config {
            let mut voter_ids = BTreeSet::new();
            for i in 0..count as u64 {
                let node_id = u64::MAX - i;
                voter_ids.insert(node_id);
                all_nodes.insert(node_id, openraft::BasicNode::default());
            }
            configs.push(voter_ids);
        }

        let membership = Membership::new(configs, all_nodes);

        StoredMembership::new(Some(LogId::new(CommittedLeaderId::new(1, 0), 1)), membership)
    }

    #[test]
    fn test_applied_state_core_postcard_round_trip() {
        let hash = [0xAB_u8; 32];
        let membership = make_membership(&[3]);

        let core = AppliedStateCore {
            last_applied: Some(LogId::new(CommittedLeaderId::new(5, 42), 100)),
            membership: membership.clone(),
            shard_height: 999,
            previous_shard_hash: hash,
            last_applied_timestamp_ns: 0,
        };

        let encoded = postcard::to_allocvec(&core).expect("serialize");
        let decoded: AppliedStateCore = postcard::from_bytes(&encoded).expect("deserialize");

        assert_eq!(decoded.last_applied, core.last_applied);
        assert_eq!(decoded.shard_height, core.shard_height);
        assert_eq!(decoded.previous_shard_hash, core.previous_shard_hash);
        assert_eq!(decoded, core);
    }

    #[test]
    fn test_applied_state_core_size_10_node_joint_consensus() {
        // Worst case: 10-node joint consensus (two configs, 10 nodes each, u64::MAX IDs)
        let membership = make_membership(&[10, 10]);

        let core = AppliedStateCore {
            last_applied: Some(LogId::new(CommittedLeaderId::new(u64::MAX, u64::MAX), u64::MAX)),
            membership,
            shard_height: u64::MAX,
            previous_shard_hash: [0xFF; 32],
            last_applied_timestamp_ns: i64::MAX,
        };

        let encoded = postcard::to_allocvec(&core).expect("serialize");
        assert!(
            encoded.len() < 512,
            "AppliedStateCore serialized to {} bytes, exceeds 512-byte limit",
            encoded.len()
        );
    }

    #[test]
    fn test_applied_state_core_size_3_node_cluster() {
        // Typical production: 3-node single config
        let membership = make_membership(&[3]);

        let core = AppliedStateCore {
            last_applied: Some(LogId::new(CommittedLeaderId::new(1, 1), 1000)),
            membership,
            shard_height: 500,
            previous_shard_hash: [0x42; 32],
            last_applied_timestamp_ns: 0,
        };

        let encoded = postcard::to_allocvec(&core).expect("serialize");
        assert!(
            encoded.len() < 128,
            "AppliedStateCore serialized to {} bytes for 3-node cluster, exceeds 128-byte limit",
            encoded.len()
        );
    }

    #[test]
    fn test_applied_state_core_from_applied_state() {
        let mut state = AppliedState {
            last_applied: Some(LogId::new(CommittedLeaderId::new(2, 10), 50)),
            shard_height: 42,
            previous_shard_hash: [0xDE; 32],
            ..Default::default()
        };
        // Add some HashMap data to prove it's excluded from the core
        state.vault_heights.insert((OrganizationId::new(1), VaultId::new(1)), 100);
        state.organizations.insert(
            OrganizationId::new(1),
            OrganizationMeta {
                organization: OrganizationId::new(1),
                slug: OrganizationSlug::new(0),
                shard_id: ShardId::new(0),
                name: "test".to_string(),
                status: OrganizationStatus::Active,
                pending_shard_id: None,
                quota: None,
                storage_bytes: 0,
            },
        );

        let core = AppliedStateCore::from(&state);

        assert_eq!(core.last_applied, state.last_applied);
        assert_eq!(core.shard_height, state.shard_height);
        assert_eq!(core.previous_shard_hash, state.previous_shard_hash);
        // Membership comparison via Debug format (StoredMembership may not impl PartialEq)
        assert_eq!(format!("{:?}", core.membership), format!("{:?}", state.membership));
    }

    #[test]
    fn test_applied_state_core_default_round_trip() {
        // Verify the "empty" state also round-trips correctly
        let core = AppliedStateCore {
            last_applied: None,
            membership: StoredMembership::default(),
            shard_height: 0,
            previous_shard_hash: [0u8; 32],
            last_applied_timestamp_ns: 0,
        };

        let encoded = postcard::to_allocvec(&core).expect("serialize");
        let decoded: AppliedStateCore = postcard::from_bytes(&encoded).expect("deserialize");
        assert_eq!(decoded, core);

        // Default case should be very small
        assert!(
            encoded.len() < 64,
            "Default AppliedStateCore is {} bytes, should be tiny",
            encoded.len()
        );
    }

    // ========================================================================
    // PendingExternalWrites tests
    // ========================================================================

    #[test]
    fn test_pending_external_writes_new_is_empty() {
        let writes = PendingExternalWrites::new();
        assert!(writes.is_empty());
    }

    #[test]
    fn test_pending_external_writes_default_is_empty() {
        let writes = PendingExternalWrites::default();
        assert!(writes.is_empty());
    }

    #[test]
    fn test_pending_external_writes_not_empty_after_push_organizations() {
        let mut writes = PendingExternalWrites::new();
        writes.organizations.push((OrganizationId::new(1), vec![0x01]));
        assert!(!writes.is_empty());
    }

    #[test]
    fn test_pending_external_writes_not_empty_after_push_organizations_deleted() {
        let mut writes = PendingExternalWrites::new();
        writes.organizations_deleted.push(OrganizationId::new(1));
        assert!(!writes.is_empty());
    }

    #[test]
    fn test_pending_external_writes_not_empty_after_push_vaults() {
        let mut writes = PendingExternalWrites::new();
        writes.vaults.push((VaultId::new(1), vec![0x01]));
        assert!(!writes.is_empty());
    }

    #[test]
    fn test_pending_external_writes_not_empty_after_push_vaults_deleted() {
        let mut writes = PendingExternalWrites::new();
        writes.vaults_deleted.push(VaultId::new(1));
        assert!(!writes.is_empty());
    }

    #[test]
    fn test_pending_external_writes_not_empty_after_push_vault_heights() {
        let mut writes = PendingExternalWrites::new();
        writes.vault_heights.push(((OrganizationId::new(1), VaultId::new(1)), 42));
        assert!(!writes.is_empty());
    }

    #[test]
    fn test_pending_external_writes_not_empty_after_push_vault_hashes() {
        let mut writes = PendingExternalWrites::new();
        writes.vault_hashes.push(((OrganizationId::new(1), VaultId::new(1)), [0xAB; 32]));
        assert!(!writes.is_empty());
    }

    #[test]
    fn test_pending_external_writes_not_empty_after_push_vault_health() {
        let mut writes = PendingExternalWrites::new();
        writes
            .vault_health
            .push(((OrganizationId::new(1), VaultId::new(1)), VaultHealthStatus::Healthy));
        assert!(!writes.is_empty());
    }

    #[test]
    fn test_pending_external_writes_not_empty_after_push_sequences() {
        let mut writes = PendingExternalWrites::new();
        writes.sequences.push(("organization".to_string(), 42));
        assert!(!writes.is_empty());
    }

    #[test]
    fn test_pending_external_writes_not_empty_after_push_client_sequences() {
        let mut writes = PendingExternalWrites::new();
        writes.client_sequences.push((vec![0x01], vec![0x02]));
        assert!(!writes.is_empty());
    }

    #[test]
    fn test_pending_external_writes_not_empty_after_push_client_sequences_deleted() {
        let mut writes = PendingExternalWrites::new();
        writes.client_sequences_deleted.push(vec![0x01]);
        assert!(!writes.is_empty());
    }

    #[test]
    fn test_pending_external_writes_not_empty_after_push_slug_index() {
        let mut writes = PendingExternalWrites::new();
        writes.slug_index.push((OrganizationSlug::new(100), OrganizationId::new(1)));
        assert!(!writes.is_empty());
    }

    #[test]
    fn test_pending_external_writes_not_empty_after_push_slug_index_deleted() {
        let mut writes = PendingExternalWrites::new();
        writes.slug_index_deleted.push(OrganizationSlug::new(100));
        assert!(!writes.is_empty());
    }

    #[test]
    fn test_pending_external_writes_not_empty_after_push_vault_slug_index() {
        let mut writes = PendingExternalWrites::new();
        writes.vault_slug_index.push((VaultSlug::new(200), VaultId::new(1)));
        assert!(!writes.is_empty());
    }

    #[test]
    fn test_pending_external_writes_not_empty_after_push_vault_slug_index_deleted() {
        let mut writes = PendingExternalWrites::new();
        writes.vault_slug_index_deleted.push(VaultSlug::new(200));
        assert!(!writes.is_empty());
    }

    // ========================================================================
    // Composite key encoding tests
    // ========================================================================

    #[test]
    fn test_vault_composite_key_encoding() {
        let key =
            PendingExternalWrites::vault_composite_key(OrganizationId::new(1), VaultId::new(2));
        assert_eq!(key.len(), 16);
        // org_id=1 big-endian: 0x0000000000000001
        assert_eq!(&key[..8], &1_i64.to_be_bytes());
        // vault_id=2 big-endian: 0x0000000000000002
        assert_eq!(&key[8..], &2_i64.to_be_bytes());
    }

    #[test]
    fn test_vault_composite_key_lexicographic_ordering() {
        // Non-negative IDs should produce correct lexicographic ordering
        // when compared byte-by-byte (which is how B+ tree keys are compared).
        let ids: &[i64] = &[0, 1, 255, 65536, i64::MAX];

        for i in 0..ids.len() {
            for j in (i + 1)..ids.len() {
                let org_a = OrganizationId::new(ids[i]);
                let org_b = OrganizationId::new(ids[j]);
                let vault = VaultId::new(1);

                let key_a = PendingExternalWrites::vault_composite_key(org_a, vault);
                let key_b = PendingExternalWrites::vault_composite_key(org_b, vault);

                assert!(
                    key_a < key_b,
                    "vault_composite_key({}, 1) should be < vault_composite_key({}, 1), \
                     got {:?} vs {:?}",
                    ids[i],
                    ids[j],
                    key_a,
                    key_b
                );
            }
        }

        // Also verify vault_id ordering within the same org
        for i in 0..ids.len() {
            for j in (i + 1)..ids.len() {
                let org = OrganizationId::new(1);
                let vault_a = VaultId::new(ids[i]);
                let vault_b = VaultId::new(ids[j]);

                let key_a = PendingExternalWrites::vault_composite_key(org, vault_a);
                let key_b = PendingExternalWrites::vault_composite_key(org, vault_b);

                assert!(
                    key_a < key_b,
                    "vault_composite_key(1, {}) should be < vault_composite_key(1, {}), \
                     got {:?} vs {:?}",
                    ids[i],
                    ids[j],
                    key_a,
                    key_b
                );
            }
        }
    }

    #[test]
    fn test_client_sequence_key_encoding() {
        let key = PendingExternalWrites::client_sequence_key(
            OrganizationId::new(1),
            VaultId::new(2),
            b"client-abc",
        );
        assert_eq!(key.len(), 16 + 10); // 16-byte prefix + 10-byte client_id
        assert_eq!(&key[..8], &1_i64.to_be_bytes());
        assert_eq!(&key[8..16], &2_i64.to_be_bytes());
        assert_eq!(&key[16..], b"client-abc");
    }

    #[test]
    fn test_client_sequence_key_collision_free_with_null_bytes() {
        // A client_id containing \0 bytes must not collide with a different
        // org/vault/client triple.
        let key1 = PendingExternalWrites::client_sequence_key(
            OrganizationId::new(1),
            VaultId::new(2),
            b"client\x00id",
        );
        let key2 = PendingExternalWrites::client_sequence_key(
            OrganizationId::new(1),
            VaultId::new(2),
            b"client",
        );
        let key3 = PendingExternalWrites::client_sequence_key(
            OrganizationId::new(1),
            VaultId::new(2),
            b"client\x00id\x00extra",
        );

        // All keys must be distinct
        assert_ne!(key1, key2);
        assert_ne!(key1, key3);
        assert_ne!(key2, key3);

        // The 16-byte prefix must be identical (same org + vault)
        assert_eq!(&key1[..16], &key2[..16]);
        assert_eq!(&key1[..16], &key3[..16]);
    }

    #[test]
    fn test_client_sequence_key_various_client_id_lengths() {
        let org = OrganizationId::new(42);
        let vault = VaultId::new(7);

        let lengths: &[usize] = &[0, 1, 255, 4096];

        for &len in lengths {
            let client_id = vec![0xAA_u8; len];
            let key = PendingExternalWrites::client_sequence_key(org, vault, &client_id);

            // Total length = 16-byte prefix + client_id length
            assert_eq!(key.len(), 16 + len, "Key length mismatch for client_id of length {}", len);

            // 16-byte prefix must be stable regardless of client_id length
            assert_eq!(
                &key[..8],
                &42_i64.to_be_bytes(),
                "org_id prefix wrong for client_id of length {}",
                len
            );
            assert_eq!(
                &key[8..16],
                &7_i64.to_be_bytes(),
                "vault_id prefix wrong for client_id of length {}",
                len
            );

            // Suffix must be the raw client_id
            assert_eq!(&key[16..], &client_id[..], "client_id suffix wrong for length {}", len);
        }
    }
}
