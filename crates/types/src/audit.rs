//! Audit logging types for compliance-ready event tracking.
//!
//! Provides structured audit events for SOC2 and HIPAA compliance. Each event
//! captures who did what, to which resource, and the outcome. Audit logs are
//! tamper-evident via sequential event IDs and timestamps.
//!
//! # Event Structure
//!
//! Every audit event includes:
//! - **Timestamp**: When the action occurred (UTC)
//! - **Event ID**: Unique identifier for deduplication and ordering
//! - **Principal**: Who performed the action (client ID or system)
//! - **Action**: What was done (write, create namespace, delete vault, etc.)
//! - **Resource**: Target of the action (namespace, vault, entity)
//! - **Outcome**: Success or failure with optional detail

use serde::{Deserialize, Serialize};

use crate::types::{NamespaceId, ShardId, VaultId};

/// Outcome of an audited operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditOutcome {
    /// Operation completed successfully.
    Success,
    /// Operation failed with an error.
    Failed {
        /// Error code or category.
        code: String,
        /// Human-readable error description.
        detail: String,
    },
    /// Operation was denied (rate limited, unauthorized, etc.).
    Denied {
        /// Reason for denial.
        reason: String,
    },
}

/// Auditable action categories.
///
/// Covers all operations that require audit trails for SOC2/HIPAA compliance:
/// - Data mutations (writes, deletes)
/// - Administrative operations (namespace/vault lifecycle)
/// - Security events (rate limiting, precondition failures)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditAction {
    // --- Data Mutations ---
    /// Writes transactions to a vault.
    Write,
    /// Batch write to one or more vaults.
    BatchWrite,

    // --- Namespace Lifecycle ---
    /// Creates a new namespace.
    CreateNamespace,
    /// Deletes a namespace.
    DeleteNamespace,
    /// Suspends a namespace.
    SuspendNamespace,
    /// Resumes a suspended namespace.
    ResumeNamespace,

    // --- Vault Lifecycle ---
    /// Creates a new vault.
    CreateVault,
    /// Deletes a vault.
    DeleteVault,

    // --- Cluster Operations ---
    /// Node joining the cluster.
    JoinCluster,
    /// Node leaving the cluster.
    LeaveCluster,

    // --- Recovery & Maintenance ---
    /// Snapshot creation.
    CreateSnapshot,
    /// Integrity check.
    CheckIntegrity,
    /// Vault recovery initiated.
    RecoverVault,
    /// Forced garbage collection.
    ForceGc,

    // --- Migration ---
    /// Starts namespace migration.
    StartMigration,
    /// Completes namespace migration.
    CompleteMigration,

    // --- Configuration ---
    /// Runtime configuration update.
    UpdateConfig,
}

impl AuditAction {
    /// Returns the action as a static string label for metrics.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Write => "write",
            Self::BatchWrite => "batch_write",
            Self::CreateNamespace => "create_namespace",
            Self::DeleteNamespace => "delete_namespace",
            Self::SuspendNamespace => "suspend_namespace",
            Self::ResumeNamespace => "resume_namespace",
            Self::CreateVault => "create_vault",
            Self::DeleteVault => "delete_vault",
            Self::JoinCluster => "join_cluster",
            Self::LeaveCluster => "leave_cluster",
            Self::CreateSnapshot => "create_snapshot",
            Self::CheckIntegrity => "check_integrity",
            Self::RecoverVault => "recover_vault",
            Self::ForceGc => "force_gc",
            Self::StartMigration => "start_migration",
            Self::CompleteMigration => "complete_migration",
            Self::UpdateConfig => "update_config",
        }
    }
}

/// The target resource of an audited operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditResource {
    /// Target namespace (if applicable).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace_id: Option<NamespaceId>,
    /// Target vault (if applicable).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vault_id: Option<VaultId>,
    /// Target shard (if applicable).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shard_id: Option<ShardId>,
    /// Additional resource context (e.g., namespace name, entity key).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

impl AuditResource {
    /// Creates a resource targeting a namespace.
    pub fn namespace(namespace_id: NamespaceId) -> Self {
        Self { namespace_id: Some(namespace_id), vault_id: None, shard_id: None, detail: None }
    }

    /// Creates a resource targeting a vault within a namespace.
    pub fn vault(namespace_id: NamespaceId, vault_id: VaultId) -> Self {
        Self {
            namespace_id: Some(namespace_id),
            vault_id: Some(vault_id),
            shard_id: None,
            detail: None,
        }
    }

    /// Creates a resource targeting a shard.
    pub fn shard(shard_id: ShardId) -> Self {
        Self { namespace_id: None, vault_id: None, shard_id: Some(shard_id), detail: None }
    }

    /// Creates a resource targeting the cluster.
    pub fn cluster() -> Self {
        Self { namespace_id: None, vault_id: None, shard_id: None, detail: None }
    }

    /// Adds a detail string to the resource.
    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into());
        self
    }
}

/// A structured audit event for compliance logging.
///
/// Each event captures a single auditable operation with all context needed
/// for SOC2/HIPAA compliance. Events are serialized as JSON for queryability.
///
/// # Fields
///
/// - `timestamp`: ISO 8601 UTC timestamp
/// - `event_id`: UUID v4 for deduplication
/// - `principal`: Who performed the action (client ID, "system", or node ID)
/// - `action`: What was done
/// - `resource`: Target of the action
/// - `outcome`: Success, failure, or denial
/// - `node_id`: Which node processed the request
/// - `trace_id`: Distributed tracing correlation (if available)
/// - `operations_count`: Number of operations in a write/batch (if applicable)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditEvent {
    /// ISO 8601 UTC timestamp of the event.
    pub timestamp: String,
    /// Unique event identifier (UUID v4).
    pub event_id: String,
    /// Principal who performed the action.
    pub principal: String,
    /// The audited action.
    pub action: AuditAction,
    /// Target resource.
    pub resource: AuditResource,
    /// Operation outcome.
    pub outcome: AuditOutcome,
    /// Node that processed the request.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_id: Option<u64>,
    /// Distributed trace ID for correlation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,
    /// Number of operations (for writes/batch writes).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operations_count: Option<usize>,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    #[test]
    fn test_audit_action_as_str_covers_all_variants() {
        let actions = [
            (AuditAction::Write, "write"),
            (AuditAction::BatchWrite, "batch_write"),
            (AuditAction::CreateNamespace, "create_namespace"),
            (AuditAction::DeleteNamespace, "delete_namespace"),
            (AuditAction::SuspendNamespace, "suspend_namespace"),
            (AuditAction::ResumeNamespace, "resume_namespace"),
            (AuditAction::CreateVault, "create_vault"),
            (AuditAction::DeleteVault, "delete_vault"),
            (AuditAction::JoinCluster, "join_cluster"),
            (AuditAction::LeaveCluster, "leave_cluster"),
            (AuditAction::CreateSnapshot, "create_snapshot"),
            (AuditAction::CheckIntegrity, "check_integrity"),
            (AuditAction::RecoverVault, "recover_vault"),
            (AuditAction::ForceGc, "force_gc"),
            (AuditAction::StartMigration, "start_migration"),
            (AuditAction::CompleteMigration, "complete_migration"),
        ];
        for (action, expected) in actions {
            assert_eq!(action.as_str(), expected);
        }
    }

    #[test]
    fn test_audit_resource_namespace() {
        let r = AuditResource::namespace(NamespaceId::new(42));
        assert_eq!(r.namespace_id, Some(NamespaceId::new(42)));
        assert_eq!(r.vault_id, None);
        assert_eq!(r.shard_id, None);
        assert_eq!(r.detail, None);
    }

    #[test]
    fn test_audit_resource_vault() {
        let r = AuditResource::vault(NamespaceId::new(1), VaultId::new(2));
        assert_eq!(r.namespace_id, Some(NamespaceId::new(1)));
        assert_eq!(r.vault_id, Some(VaultId::new(2)));
    }

    #[test]
    fn test_audit_resource_shard() {
        let r = AuditResource::shard(ShardId::new(5));
        assert_eq!(r.shard_id, Some(ShardId::new(5)));
        assert_eq!(r.namespace_id, None);
    }

    #[test]
    fn test_audit_resource_cluster() {
        let r = AuditResource::cluster();
        assert_eq!(r.namespace_id, None);
        assert_eq!(r.vault_id, None);
        assert_eq!(r.shard_id, None);
        assert_eq!(r.detail, None);
    }

    #[test]
    fn test_audit_resource_with_detail() {
        let r = AuditResource::namespace(NamespaceId::new(1)).with_detail("test-ns");
        assert_eq!(r.detail, Some("test-ns".to_string()));
    }

    #[test]
    fn test_audit_outcome_success_serde() {
        let outcome = AuditOutcome::Success;
        let json = serde_json::to_string(&outcome).expect("serialize");
        assert_eq!(json, "\"success\"");
        let deserialized: AuditOutcome = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(deserialized, outcome);
    }

    #[test]
    fn test_audit_outcome_failed_serde() {
        let outcome =
            AuditOutcome::Failed { code: "NOT_FOUND".to_string(), detail: "gone".to_string() };
        let json = serde_json::to_string(&outcome).expect("serialize");
        assert!(json.contains("NOT_FOUND"));
        let deserialized: AuditOutcome = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(deserialized, outcome);
    }

    #[test]
    fn test_audit_outcome_denied_serde() {
        let outcome = AuditOutcome::Denied { reason: "rate_limited".to_string() };
        let json = serde_json::to_string(&outcome).expect("serialize");
        assert!(json.contains("rate_limited"));
        let deserialized: AuditOutcome = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(deserialized, outcome);
    }

    #[test]
    fn test_audit_event_serde_roundtrip() {
        let event = AuditEvent {
            timestamp: "2025-01-15T10:30:00Z".to_string(),
            event_id: "550e8400-e29b-41d4-a716-446655440000".to_string(),
            principal: "client:app-1".to_string(),
            action: AuditAction::Write,
            resource: AuditResource::vault(NamespaceId::new(1), VaultId::new(2)),
            outcome: AuditOutcome::Success,
            node_id: Some(1),
            trace_id: Some("abc123".to_string()),
            operations_count: Some(5),
        };
        let json = serde_json::to_string(&event).expect("serialize");
        let deserialized: AuditEvent = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(deserialized, event);
    }

    #[test]
    fn test_audit_event_skips_none_fields() {
        let event = AuditEvent {
            timestamp: "2025-01-15T10:30:00Z".to_string(),
            event_id: "test-id".to_string(),
            principal: "system".to_string(),
            action: AuditAction::CreateSnapshot,
            resource: AuditResource::cluster(),
            outcome: AuditOutcome::Success,
            node_id: None,
            trace_id: None,
            operations_count: None,
        };
        let json = serde_json::to_string(&event).expect("serialize");
        assert!(!json.contains("node_id"));
        assert!(!json.contains("trace_id"));
        assert!(!json.contains("operations_count"));
    }

    #[test]
    fn test_audit_event_all_actions_serialize() {
        let actions = vec![
            AuditAction::Write,
            AuditAction::BatchWrite,
            AuditAction::CreateNamespace,
            AuditAction::DeleteNamespace,
            AuditAction::SuspendNamespace,
            AuditAction::ResumeNamespace,
            AuditAction::CreateVault,
            AuditAction::DeleteVault,
            AuditAction::JoinCluster,
            AuditAction::LeaveCluster,
            AuditAction::CreateSnapshot,
            AuditAction::CheckIntegrity,
            AuditAction::RecoverVault,
            AuditAction::ForceGc,
            AuditAction::StartMigration,
            AuditAction::CompleteMigration,
        ];
        for action in actions {
            let event = AuditEvent {
                timestamp: "2025-01-15T10:30:00Z".to_string(),
                event_id: "test".to_string(),
                principal: "test".to_string(),
                action: action.clone(),
                resource: AuditResource::cluster(),
                outcome: AuditOutcome::Success,
                node_id: None,
                trace_id: None,
                operations_count: None,
            };
            let json = serde_json::to_string(&event).expect("serialize");
            assert!(json.contains(action.as_str()), "action {action:?} not in JSON: {json}");
        }
    }

    #[test]
    fn test_audit_resource_none_fields_skipped() {
        let r = AuditResource::cluster();
        let json = serde_json::to_string(&r).expect("serialize");
        assert!(!json.contains("namespace_id"));
        assert!(!json.contains("vault_id"));
        assert!(!json.contains("shard_id"));
        assert!(!json.contains("detail"));
    }
}
