//! Wire types for `VaultService`.

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::services::shared::{
    BlockHeader, BlockRetentionPolicy, Hash, NodeId, OrganizationSlug, UserSlug, VaultSlug,
    VaultStatus,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CreateVaultRequest {
    pub organization: Option<OrganizationSlug>,
    pub replication_factor: u32,
    pub initial_nodes: Vec<NodeId>,
    pub retention_policy: Option<BlockRetentionPolicy>,
    pub caller: Option<UserSlug>,
    pub slug: Option<VaultSlug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CreateVaultResponse {
    pub vault: Option<VaultSlug>,
    pub genesis: Option<BlockHeader>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DeleteVaultRequest {
    pub organization: Option<OrganizationSlug>,
    pub vault: Option<VaultSlug>,
    pub caller: Option<UserSlug>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DeleteVaultResponse {
    /// UNIX nanoseconds.
    pub deleted_at: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GetVaultRequest {
    pub organization: Option<OrganizationSlug>,
    pub vault: Option<VaultSlug>,
    pub caller: Option<UserSlug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GetVaultResponse {
    pub organization: Option<OrganizationSlug>,
    pub vault: Option<VaultSlug>,
    pub height: u64,
    pub state_root: Option<Hash>,
    pub nodes: Vec<NodeId>,
    pub leader: Option<NodeId>,
    pub status: VaultStatus,
    pub retention_policy: Option<BlockRetentionPolicy>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ListVaultsRequest {
    pub page_token: Option<Bytes>,
    pub page_size: u32,
    pub organization: Option<OrganizationSlug>,
    pub caller: Option<UserSlug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ListVaultsResponse {
    pub vaults: Vec<GetVaultResponse>,
    pub next_page_token: Option<Bytes>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct UpdateVaultRequest {
    pub organization: Option<OrganizationSlug>,
    pub vault: Option<VaultSlug>,
    pub retention_policy: Option<BlockRetentionPolicy>,
    pub caller: Option<UserSlug>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct UpdateVaultResponse {}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;
    use crate::services::shared::BlockRetentionMode;

    #[test]
    fn create_vault_request_roundtrips_via_postcard() {
        let req = CreateVaultRequest {
            organization: Some(OrganizationSlug::new(1)),
            replication_factor: 3,
            initial_nodes: vec![NodeId::new("node-1"), NodeId::new("node-2")],
            retention_policy: Some(BlockRetentionPolicy {
                mode: Some(BlockRetentionMode::Compacted),
                retention_blocks: Some(10_000),
            }),
            caller: Some(UserSlug::new(42)),
            slug: Some(VaultSlug::new(0xC0FFEE)),
        };
        let bytes = postcard::to_allocvec(&req).unwrap();
        let decoded: CreateVaultRequest = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(req, decoded);
    }
}
