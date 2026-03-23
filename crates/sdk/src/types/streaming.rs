//! Block announcement types for streaming subscriptions.

use inferadb_ledger_proto::proto;
use inferadb_ledger_types::{OrganizationSlug, VaultSlug};

use crate::proto_util::proto_timestamp_to_system_time;

/// A block announcement from the WatchBlocks stream.
///
/// Contains metadata about a newly committed block in a vault's chain.
/// Used for real-time notifications of state changes.
///
/// # Example
///
/// ```no_run
/// # use inferadb_ledger_sdk::{LedgerClient, ClientConfig, OrganizationSlug, VaultSlug, ServerSource};
/// # use futures::StreamExt;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = LedgerClient::new(ClientConfig::builder()
/// #     .servers(ServerSource::from_static(["http://localhost:50051"]))
/// #     .client_id("example")
/// #     .build()?).await?;
/// # let (organization, vault, start_height) = (OrganizationSlug::new(1), VaultSlug::new(1), 1u64);
/// let mut stream = client.watch_blocks(organization, vault, start_height).await?;
/// while let Some(announcement) = stream.next().await {
///     let block = announcement?;
///     println!("New block at height {}: {:?}", block.height, block.block_hash);
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct BlockAnnouncement {
    /// Organization containing the vault.
    pub organization: OrganizationSlug,
    /// Vault (Snowflake ID) within the organization.
    pub vault: VaultSlug,
    /// Block height (1-indexed).
    pub height: u64,
    /// Hash of the block header.
    pub block_hash: Vec<u8>,
    /// Merkle root of the state at this block.
    pub state_root: Vec<u8>,
    /// Timestamp when the block was committed.
    pub timestamp: Option<std::time::SystemTime>,
}

impl BlockAnnouncement {
    /// Converts from protobuf message.
    pub(crate) fn from_proto(proto: proto::BlockAnnouncement) -> Self {
        let timestamp = proto.timestamp.and_then(|ts| proto_timestamp_to_system_time(&ts));

        Self {
            organization: OrganizationSlug::new(proto.organization.map_or(0, |n| n.slug)),
            vault: VaultSlug::new(proto.vault.map_or(0, |v| v.slug)),
            height: proto.height,
            block_hash: proto.block_hash.map(|h| h.value).unwrap_or_default(),
            state_root: proto.state_root.map(|h| h.value).unwrap_or_default(),
            timestamp,
        }
    }
}
