//! Wire types for `ReadService`.
//!
//! Field-type rules: see [`crate::services`] module-level docs.

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::services::shared::{
    Block, BlockHeader, ChainProof, ClientIdMessage, Entity, Hash, MerkleProof, OrganizationSlug,
    ReadConsistency, Relationship, UserSlug, VaultSlug,
};

// ---------------------------------------------------------------------------
// Read / BatchRead / VerifiedRead / HistoricalRead
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ReadRequest {
    pub organization: Option<OrganizationSlug>,
    pub vault: Option<VaultSlug>,
    pub key: String,
    pub consistency: ReadConsistency,
    pub caller: Option<UserSlug>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReadResponse {
    pub value: Option<Bytes>,
    pub block_height: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BatchReadRequest {
    pub organization: Option<OrganizationSlug>,
    pub vault: Option<VaultSlug>,
    pub keys: Vec<String>,
    pub consistency: ReadConsistency,
    pub caller: Option<UserSlug>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BatchReadResponse {
    pub results: Vec<BatchReadResult>,
    pub block_height: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BatchReadResult {
    pub key: String,
    pub value: Option<Bytes>,
    pub found: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct VerifiedReadRequest {
    pub organization: Option<OrganizationSlug>,
    pub vault: Option<VaultSlug>,
    pub key: String,
    pub at_height: Option<u64>,
    pub include_chain_proof: bool,
    pub trusted_height: Option<u64>,
    pub caller: Option<UserSlug>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VerifiedReadResponse {
    pub value: Option<Bytes>,
    pub block_height: u64,
    pub block_header: Option<BlockHeader>,
    pub merkle_proof: Option<MerkleProof>,
    pub chain_proof: Option<ChainProof>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct HistoricalReadRequest {
    pub organization: Option<OrganizationSlug>,
    pub vault: Option<VaultSlug>,
    pub key: String,
    pub at_height: u64,
    pub include_proof: bool,
    pub include_chain_proof: bool,
    pub trusted_height: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HistoricalReadResponse {
    pub value: Option<Bytes>,
    pub block_height: u64,
    pub block_header: Option<BlockHeader>,
    pub merkle_proof: Option<MerkleProof>,
    pub chain_proof: Option<ChainProof>,
}

// ---------------------------------------------------------------------------
// Block / Tip queries
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WatchBlocksRequest {
    pub organization: Option<OrganizationSlug>,
    pub vault: Option<VaultSlug>,
    pub start_height: u64,
    pub caller: Option<UserSlug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GetBlockRequest {
    pub organization: Option<OrganizationSlug>,
    pub vault: Option<VaultSlug>,
    pub height: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GetBlockResponse {
    pub block: Option<Block>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GetBlockRangeRequest {
    pub organization: Option<OrganizationSlug>,
    pub vault: Option<VaultSlug>,
    pub start_height: u64,
    pub end_height: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GetBlockRangeResponse {
    pub blocks: Vec<Block>,
    pub current_tip: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GetTipRequest {
    pub organization: Option<OrganizationSlug>,
    pub vault: Option<VaultSlug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GetTipResponse {
    pub height: u64,
    pub block_hash: Option<Hash>,
    pub state_root: Option<Hash>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GetClientStateRequest {
    pub organization: Option<OrganizationSlug>,
    pub vault: Option<VaultSlug>,
    pub client_id: Option<ClientIdMessage>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GetClientStateResponse {
    pub last_committed_sequence: u64,
}

// ---------------------------------------------------------------------------
// Relationship / resource / entity queries
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ListRelationshipsRequest {
    pub organization: Option<OrganizationSlug>,
    pub vault: Option<VaultSlug>,
    pub resource: Option<String>,
    pub relation: Option<String>,
    pub subject: Option<String>,
    pub at_height: Option<u64>,
    pub limit: u32,
    pub page_token: String,
    pub consistency: ReadConsistency,
    pub caller: Option<UserSlug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ListRelationshipsResponse {
    pub relationships: Vec<Relationship>,
    pub block_height: u64,
    pub next_page_token: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CheckRelationshipRequest {
    pub organization: Option<OrganizationSlug>,
    pub vault: Option<VaultSlug>,
    pub resource: String,
    pub relation: String,
    pub subject: String,
    pub consistency: ReadConsistency,
    pub caller: Option<UserSlug>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CheckRelationshipResponse {
    pub exists: bool,
    pub checked_at_height: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ListResourcesRequest {
    pub organization: Option<OrganizationSlug>,
    pub vault: Option<VaultSlug>,
    pub resource_type: String,
    pub at_height: Option<u64>,
    pub limit: u32,
    pub page_token: String,
    pub consistency: ReadConsistency,
    pub caller: Option<UserSlug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ListResourcesResponse {
    pub resources: Vec<String>,
    pub block_height: u64,
    pub next_page_token: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ListEntitiesRequest {
    pub organization: Option<OrganizationSlug>,
    pub key_prefix: String,
    pub at_height: Option<u64>,
    pub include_expired: bool,
    pub limit: u32,
    pub page_token: String,
    pub consistency: ReadConsistency,
    pub vault: Option<VaultSlug>,
    pub caller: Option<UserSlug>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ListEntitiesResponse {
    pub entities: Vec<Entity>,
    pub block_height: u64,
    pub next_page_token: String,
}

// ---------------------------------------------------------------------------
// Read error code (used in error frame context, mirrors proto enum)
// ---------------------------------------------------------------------------

#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ReadErrorCode {
    Unspecified = 0,
    HeightUnavailable = 1,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    #[test]
    fn read_request_roundtrips_via_postcard() {
        let req = ReadRequest {
            organization: Some(OrganizationSlug::new(1)),
            vault: Some(VaultSlug::new(2)),
            key: "user:42".to_owned(),
            consistency: ReadConsistency::Linearizable,
            caller: Some(UserSlug::new(99)),
        };
        let bytes = postcard::to_allocvec(&req).unwrap();
        let decoded: ReadRequest = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(req, decoded);
    }

    #[test]
    fn list_relationships_request_with_filters_roundtrips() {
        let req = ListRelationshipsRequest {
            organization: Some(OrganizationSlug::new(1)),
            vault: Some(VaultSlug::new(2)),
            resource: Some("document:42".to_owned()),
            relation: Some("viewer".to_owned()),
            subject: None,
            at_height: Some(1000),
            limit: 50,
            page_token: "cursor-abc".to_owned(),
            consistency: ReadConsistency::Eventual,
            caller: Some(UserSlug::new(99)),
        };
        let bytes = postcard::to_allocvec(&req).unwrap();
        let decoded: ListRelationshipsRequest = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(req, decoded);
    }
}
