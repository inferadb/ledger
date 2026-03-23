//! Block retention policy types controlling how long transaction bodies are preserved.

use serde::{Deserialize, Serialize};

/// Block retention mode for storage/compliance trade-off.
///
/// Configurable retention policy determines whether transaction bodies
/// are preserved after snapshots.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum BlockRetentionMode {
    /// All transaction bodies preserved indefinitely.
    ///
    /// Suitable for audit and compliance requirements where full history
    /// must remain accessible.
    #[default]
    Full,
    /// Transaction bodies removed for blocks older than `retention_blocks` from tip.
    ///
    /// Headers (`state_root`, `tx_merkle_root`) are preserved for verification.
    /// Suitable for high-volume workloads prioritizing storage efficiency.
    Compacted,
}

/// Block retention policy for a vault.
///
/// Controls how long transaction bodies are preserved vs. compacted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockRetentionPolicy {
    /// Retention mode (Full or Compacted).
    pub mode: BlockRetentionMode,
    /// For COMPACTED mode: blocks newer than tip - retention_blocks keep full transactions.
    /// Ignored for FULL mode. Default: 10000 blocks.
    pub retention_blocks: u64,
}

impl Default for BlockRetentionPolicy {
    fn default() -> Self {
        Self { mode: BlockRetentionMode::Full, retention_blocks: 10_000 }
    }
}
