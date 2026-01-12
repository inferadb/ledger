//! Core types for OpenRaft integration.
//!
//! This module defines the type configuration for OpenRaft, including:
//! - Node identification
//! - Log entry format
//! - Response types
//! - Snapshot data format

use std::fmt;
use std::io::Cursor;

use openraft::BasicNode;
use openraft::impls::OneshotResponder;
use serde::{Deserialize, Serialize};

use ledger_types::{Hash, NamespaceId, Transaction, VaultId};

// ============================================================================
// Type Configuration
// ============================================================================

/// Node identifier in the Raft cluster.
///
/// We use u64 for efficient storage and comparison. The mapping from
/// human-readable node names (e.g., "node-1") to numeric IDs is maintained
/// in the `_system` namespace.
pub type LedgerNodeId = u64;

// Use the declare_raft_types macro for type configuration.
// This macro generates a `LedgerTypeConfig` struct that implements `RaftTypeConfig`.
//
// Type parameters:
// - `D`: Application data (LedgerRequest)
// - `R`: Application response (LedgerResponse)
// - `NodeId`: Node identifier type (u64)
// - `Node`: Node metadata (BasicNode with address info)
// - `Entry`: Log entry format (default Entry)
// - `SnapshotData`: Snapshot format (in-memory cursor for MVP)
// - `AsyncRuntime`: Tokio runtime
// - `Responder`: One-shot channel responder
openraft::declare_raft_types!(
    /// Ledger Raft type configuration.
    pub LedgerTypeConfig:
        D = LedgerRequest,
        R = LedgerResponse,
        NodeId = LedgerNodeId,
        Node = BasicNode,
        Entry = openraft::Entry<LedgerTypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
        Responder = OneshotResponder<LedgerTypeConfig>
);

// ============================================================================
// Request/Response Types
// ============================================================================

/// Request to the Raft state machine.
///
/// This is the "D" (data) type in OpenRaft's type configuration.
/// Each request targets a specific namespace and vault.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LedgerRequest {
    /// Write transactions to a vault.
    Write {
        /// Target namespace.
        namespace_id: NamespaceId,
        /// Target vault within the namespace.
        vault_id: VaultId,
        /// Transactions to apply atomically.
        transactions: Vec<Transaction>,
    },

    /// Create a new namespace (applied to `_system`).
    CreateNamespace {
        /// Requested namespace name.
        name: String,
    },

    /// Create a new vault within a namespace.
    CreateVault {
        /// Namespace to create the vault in.
        namespace_id: NamespaceId,
        /// Optional vault name (for display).
        name: Option<String>,
    },

    /// Delete a namespace.
    DeleteNamespace {
        /// Namespace ID to delete.
        namespace_id: NamespaceId,
    },

    /// Delete a vault.
    DeleteVault {
        /// Namespace containing the vault.
        namespace_id: NamespaceId,
        /// Vault ID to delete.
        vault_id: VaultId,
    },

    /// Update vault health status (used during recovery).
    UpdateVaultHealth {
        /// Namespace containing the vault.
        namespace_id: NamespaceId,
        /// Vault ID to update.
        vault_id: VaultId,
        /// New health status.
        healthy: bool,
        /// If diverged, the expected state root.
        expected_root: Option<Hash>,
        /// If diverged, the computed state root.
        computed_root: Option<Hash>,
        /// If diverged, the height at which divergence was detected.
        diverged_at_height: Option<u64>,
    },

    /// System operation (user management, node membership, etc.).
    System(SystemRequest),
}

/// System-level requests that modify `_system` namespace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SystemRequest {
    /// Create a new user.
    CreateUser {
        /// User's display name.
        name: String,
        /// User's email address.
        email: String,
    },

    /// Add a node to the cluster.
    AddNode {
        /// Numeric node ID.
        node_id: LedgerNodeId,
        /// Node's gRPC address.
        address: String,
    },

    /// Remove a node from the cluster.
    RemoveNode {
        /// Node ID to remove.
        node_id: LedgerNodeId,
    },

    /// Update namespace-to-shard mapping.
    UpdateNamespaceRouting {
        /// Namespace to update.
        namespace_id: NamespaceId,
        /// New shard assignment.
        shard_id: i32,
    },
}

/// Response from the Raft state machine.
///
/// This is the "R" (response) type in OpenRaft's type configuration.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum LedgerResponse {
    /// Empty response (for operations that don't return data).
    #[default]
    Empty,

    /// Write operation completed.
    Write {
        /// Block height where the write was committed.
        block_height: u64,
        /// Block hash.
        block_hash: Hash,
    },

    /// Namespace created.
    NamespaceCreated {
        /// Assigned namespace ID.
        namespace_id: NamespaceId,
    },

    /// Vault created.
    VaultCreated {
        /// Assigned vault ID.
        vault_id: VaultId,
    },

    /// Namespace deleted.
    NamespaceDeleted {
        /// Whether the deletion was successful.
        success: bool,
    },

    /// Vault deleted.
    VaultDeleted {
        /// Whether the deletion was successful.
        success: bool,
    },

    /// Vault health updated.
    VaultHealthUpdated {
        /// Whether the update was successful.
        success: bool,
    },

    /// User created.
    UserCreated {
        /// Assigned user ID.
        user_id: i64,
    },

    /// Error response.
    Error {
        /// Error message.
        message: String,
    },
}

impl fmt::Display for LedgerResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LedgerResponse::Empty => write!(f, "Empty"),
            LedgerResponse::Write { block_height, .. } => {
                write!(f, "Write(height={})", block_height)
            }
            LedgerResponse::NamespaceCreated { namespace_id } => {
                write!(f, "NamespaceCreated(id={})", namespace_id)
            }
            LedgerResponse::VaultCreated { vault_id } => {
                write!(f, "VaultCreated(id={})", vault_id)
            }
            LedgerResponse::UserCreated { user_id } => {
                write!(f, "UserCreated(id={})", user_id)
            }
            LedgerResponse::NamespaceDeleted { success } => {
                write!(f, "NamespaceDeleted(success={})", success)
            }
            LedgerResponse::VaultDeleted { success } => {
                write!(f, "VaultDeleted(success={})", success)
            }
            LedgerResponse::VaultHealthUpdated { success } => {
                write!(f, "VaultHealthUpdated(success={})", success)
            }
            LedgerResponse::Error { message } => {
                write!(f, "Error({})", message)
            }
        }
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::disallowed_methods,
    clippy::panic
)]
mod tests {
    use super::*;

    #[test]
    fn test_ledger_request_serialization() {
        let request = LedgerRequest::CreateNamespace {
            name: "test-namespace".to_string(),
        };

        let bytes = bincode::serialize(&request).expect("serialize");
        let deserialized: LedgerRequest = bincode::deserialize(&bytes).expect("deserialize");

        match deserialized {
            LedgerRequest::CreateNamespace { name } => {
                assert_eq!(name, "test-namespace");
            }
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn test_ledger_response_display() {
        let response = LedgerResponse::Write {
            block_height: 42,
            block_hash: [0u8; 32],
        };
        assert_eq!(format!("{}", response), "Write(height=42)");
    }

    #[test]
    fn test_system_request_serialization() {
        let request = SystemRequest::CreateUser {
            name: "Alice".to_string(),
            email: "alice@example.com".to_string(),
        };

        let bytes = bincode::serialize(&request).expect("serialize");
        let deserialized: SystemRequest = bincode::deserialize(&bytes).expect("deserialize");

        match deserialized {
            SystemRequest::CreateUser { name, email } => {
                assert_eq!(name, "Alice");
                assert_eq!(email, "alice@example.com");
            }
            _ => panic!("unexpected variant"),
        }
    }
}
