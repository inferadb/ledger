//! Wire types for `RaftService`.
//!
//! Internal Raft RPCs: replication transport, committed-index queries,
//! regional proposals, and snapshot streaming.

use std::collections::BTreeMap;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::services::shared::{OrganizationSlug, VaultSlug};

// ---------------------------------------------------------------------------
// CommittedIndex
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CommittedIndexRequest {
    pub region: String,
    pub organization: Option<OrganizationSlug>,
    pub vault: Option<VaultSlug>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CommittedIndexResponse {
    pub committed_index: u64,
    pub leader_term: u64,
}

// ---------------------------------------------------------------------------
// Replication transport
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ConsensusEnvelope {
    pub shard_id: u64,
    pub from_node: u64,
    pub region: Option<String>,
    /// Postcard-serialized consensus `Message` payload.
    pub payload: Bytes,
    pub from_address: String,
    pub cluster_id: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ConsensusAck {}

// ---------------------------------------------------------------------------
// Regional proposal
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RegionalProposalRequest {
    pub region: Option<String>,
    /// Postcard-serialized `RaftPayload<R>`.
    pub request_payload: Bytes,
    pub caller: u64,
    pub timeout_ms: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RegionalProposalResult {
    /// Postcard-serialized `LedgerResponse`.
    pub response_payload: Bytes,
    pub status_code: i32,
    pub error_message: String,
    pub committed_index: u64,
}

// ---------------------------------------------------------------------------
// Install snapshot streaming
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct InstallSnapshotChunk {
    pub payload: Option<InstallSnapshotPayload>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum InstallSnapshotPayload {
    Header(InstallSnapshotHeader),
    Data(Bytes),
    Footer(InstallSnapshotFooter),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct InstallSnapshotHeader {
    pub leader_term: u64,
    pub leader_id: u64,
    pub scope: Option<InstallSnapshotScope>,
    pub snapshot_index: u64,
    pub snapshot_term: u64,
    pub total_bytes: u64,
    pub chunk_size_bytes: u64,
    pub lsnp_version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum InstallSnapshotScope {
    Org(InstallSnapshotOrgScope),
    Vault(InstallSnapshotVaultScope),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct InstallSnapshotOrgScope {
    pub region: String,
    pub organization_id: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct InstallSnapshotVaultScope {
    pub region: String,
    pub organization_id: i64,
    pub vault_id: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct InstallSnapshotFooter {
    pub stream_crc32c: u32,
    pub final_byte_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct InstallSnapshotStreamResponse {
    pub follower_term: u64,
    pub success: bool,
    pub error_message: String,
    pub staged_at_index: u64,
}

/// Receiver-to-sender backpressure ack on the snapshot streaming
/// sub-protocol. The receiver writes an ack on the bidi stream's send
/// side after every committed chunk; the sender uses the most-recent
/// `offset_received` to bound in-flight bytes.
///
/// See `inferadb_ledger_wire_transport::snapshot_stream` for the full
/// sub-protocol specification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct InstallSnapshotAck {
    /// Number of bytes the receiver has durably accepted on the staging
    /// file so far. Monotonically non-decreasing across the lifetime of a
    /// single snapshot stream.
    pub offset_received: u64,
}

// ---------------------------------------------------------------------------
// Raft primitives (vote, log id, snapshot meta, membership)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RaftVote {
    pub term: u64,
    pub node_id: u64,
    pub committed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RaftLogId {
    pub term: u64,
    pub index: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RaftSnapshotMeta {
    pub last_log_id: Option<RaftLogId>,
    pub last_membership: Option<RaftMembership>,
    pub snapshot_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RaftMembership {
    pub configs: Vec<RaftMembershipConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RaftMembershipConfig {
    /// node_id → address.
    pub members: BTreeMap<u64, String>,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    #[test]
    fn install_snapshot_chunk_with_header_roundtrips() {
        let chunk = InstallSnapshotChunk {
            payload: Some(InstallSnapshotPayload::Header(InstallSnapshotHeader {
                leader_term: 7,
                leader_id: 1,
                scope: Some(InstallSnapshotScope::Vault(InstallSnapshotVaultScope {
                    region: "us-east-va".to_owned(),
                    organization_id: 42,
                    vault_id: 99,
                })),
                snapshot_index: 1000,
                snapshot_term: 7,
                total_bytes: 1_048_576,
                chunk_size_bytes: 65_536,
                lsnp_version: "LSNP-v2".to_owned(),
            })),
        };
        let bytes = postcard::to_allocvec(&chunk).unwrap();
        let decoded: InstallSnapshotChunk = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(chunk, decoded);
    }

    #[test]
    fn raft_membership_with_btreemap_roundtrips() {
        let mut members = BTreeMap::new();
        members.insert(1, "127.0.0.1:5000".to_owned());
        members.insert(2, "127.0.0.1:5001".to_owned());
        members.insert(3, "127.0.0.1:5002".to_owned());

        let m = RaftMembership { configs: vec![RaftMembershipConfig { members }] };
        let bytes = postcard::to_allocvec(&m).unwrap();
        let decoded: RaftMembership = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(m, decoded);
    }
}
