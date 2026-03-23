//! Blockchain-backed audit trail for security-sensitive operations.
//!
//! Audit records are stored under `_audit:{category}:{entity_id}:{action}:{timestamp_ns}`
//! keys in the GLOBAL state layer, within the same transaction as the operation
//! they describe. This ensures atomicity: the audit record is committed in the
//! same block as the action.
//!
//! Timestamps are deterministic — derived from `block_timestamp` (the leader's
//! proposal timestamp, identical on all replicas), not `Utc::now()`.

use inferadb_ledger_types::Operation;
use serde::{Deserialize, Serialize};

/// A blockchain-backed audit record for a security-sensitive operation.
///
/// Serialized with postcard and stored as a `SetEntity` operation in the
/// consensus-replicated state layer. Each record captures the action, the
/// actor, the target, a deterministic timestamp, and action-specific details.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditRecord {
    /// Action performed (e.g., `"create_signing_key"`, `"delete_organization"`).
    pub action: String,
    /// Who performed the action (caller slug, 0 for system-initiated).
    pub caller: u64,
    /// What the action targeted (entity identifier).
    pub target: String,
    /// Timestamp in nanoseconds (from `block_timestamp`, deterministic).
    pub timestamp_ns: i64,
    /// Additional context (action-specific key-value details).
    pub details: Vec<(String, String)>,
}

/// Appends a `SetEntity` operation for an audit record to a list of operations.
///
/// The caller is responsible for applying these operations to the state layer
/// within the same transaction as the audited action.
///
/// # Errors
///
/// Returns `None` (and logs a warning) if the record cannot be serialized.
/// Audit serialization failures must not abort the primary operation.
pub fn write_audit_record(operations: &mut Vec<Operation>, key: String, record: &AuditRecord) {
    match inferadb_ledger_types::encode(record) {
        Ok(value) => {
            operations.push(Operation::SetEntity { key, value, condition: None, expires_at: None });
        },
        Err(e) => {
            tracing::warn!(audit_key = %key, error = %e, "Failed to encode audit record");
        },
    }
}

/// Audit key categories for building storage keys.
pub struct AuditKeys;

impl AuditKeys {
    /// Builds an audit key for a signing key operation.
    ///
    /// Format: `_audit:signing_key:{kid}:{action}:{timestamp_ns}`
    pub fn signing_key(kid: &str, action: &str, timestamp_ns: i64) -> String {
        format!("_audit:signing_key:{kid}:{action}:{timestamp_ns}")
    }

    /// Builds an audit key for an organization lifecycle operation.
    ///
    /// Format: `_audit:org:{org_id}:{action}:{timestamp_ns}`
    pub fn organization(org_id: i64, action: &str, timestamp_ns: i64) -> String {
        format!("_audit:org:{org_id}:{action}:{timestamp_ns}")
    }

    /// Builds an audit key for a user role change.
    ///
    /// Format: `_audit:user_role:{user_id}:{action}:{timestamp_ns}`
    pub fn user_role(user_id: i64, action: &str, timestamp_ns: i64) -> String {
        format!("_audit:user_role:{user_id}:{action}:{timestamp_ns}")
    }

    /// Builds an audit key for an app lifecycle operation.
    ///
    /// Format: `_audit:app:{org_id}:{app_id}:{action}:{timestamp_ns}`
    pub fn app(org_id: i64, app_id: i64, action: &str, timestamp_ns: i64) -> String {
        format!("_audit:app:{org_id}:{app_id}:{action}:{timestamp_ns}")
    }

    /// Builds an audit key for a vault lifecycle operation.
    ///
    /// Format: `_audit:vault:{vault_slug}:{action}:{timestamp_ns}`
    pub fn vault(vault_slug: u64, action: &str, timestamp_ns: i64) -> String {
        format!("_audit:vault:{vault_slug}:{action}:{timestamp_ns}")
    }

    /// Builds an audit key for a user lifecycle operation.
    ///
    /// Format: `_audit:user:{user_slug}:{action}:{timestamp_ns}`
    pub fn user(user_slug: u64, action: &str, timestamp_ns: i64) -> String {
        format!("_audit:user:{user_slug}:{action}:{timestamp_ns}")
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods, clippy::panic)]
mod tests {
    use inferadb_ledger_types::{Operation, decode};

    use super::*;

    fn sample_record() -> AuditRecord {
        AuditRecord {
            action: "create_signing_key".into(),
            caller: 12345,
            target: "key-uuid-abc".into(),
            timestamp_ns: 1_700_000_000_000_000_000,
            details: vec![("scope".into(), "Global".into())],
        }
    }

    #[test]
    fn audit_record_roundtrip() {
        let record = sample_record();
        let encoded = inferadb_ledger_types::encode(&record).unwrap();
        let decoded: AuditRecord = decode(&encoded).unwrap();
        assert_eq!(record, decoded);
    }

    #[test]
    fn audit_record_empty_details_roundtrip() {
        let record = AuditRecord {
            action: "delete_organization".into(),
            caller: 0,
            target: "org-42".into(),
            timestamp_ns: 1_700_000_000_000_000_000,
            details: vec![],
        };
        let encoded = inferadb_ledger_types::encode(&record).unwrap();
        let decoded: AuditRecord = decode(&encoded).unwrap();
        assert_eq!(record, decoded);
    }

    #[test]
    fn write_audit_record_appends_set_entity() {
        let mut ops = Vec::new();
        let record = sample_record();
        let key = AuditKeys::signing_key("kid-1", "create", record.timestamp_ns);
        write_audit_record(&mut ops, key.clone(), &record);
        assert_eq!(ops.len(), 1);
        match &ops[0] {
            Operation::SetEntity { key: k, value, condition, expires_at } => {
                assert_eq!(k, &key);
                assert!(condition.is_none());
                assert!(expires_at.is_none());
                let decoded: AuditRecord = decode(value).unwrap();
                assert_eq!(decoded, record);
            },
            other => panic!("Expected SetEntity, got {other:?}"),
        }
    }

    #[test]
    fn signing_key_audit_key_format() {
        let key = AuditKeys::signing_key("kid-abc", "create", 1_700_000_000_000_000_000);
        assert_eq!(key, "_audit:signing_key:kid-abc:create:1700000000000000000");
    }

    #[test]
    fn organization_audit_key_format() {
        let key = AuditKeys::organization(42, "delete", 1_700_000_000_000_000_000);
        assert_eq!(key, "_audit:org:42:delete:1700000000000000000");
    }

    #[test]
    fn user_role_audit_key_format() {
        let key = AuditKeys::user_role(99, "update_role", 1_700_000_000_000_000_000);
        assert_eq!(key, "_audit:user_role:99:update_role:1700000000000000000");
    }

    #[test]
    fn app_audit_key_format() {
        let key = AuditKeys::app(1, 5, "create", 1_700_000_000_000_000_000);
        assert_eq!(key, "_audit:app:1:5:create:1700000000000000000");
    }

    #[test]
    fn vault_audit_key_format() {
        let key = AuditKeys::vault(999, "create", 1_700_000_000_000_000_000);
        assert_eq!(key, "_audit:vault:999:create:1700000000000000000");
    }

    #[test]
    fn user_audit_key_format() {
        let key = AuditKeys::user(42, "delete", 1_700_000_000_000_000_000);
        assert_eq!(key, "_audit:user:42:delete:1700000000000000000");
    }

    #[test]
    fn multiple_details_roundtrip() {
        let record = AuditRecord {
            action: "rotate_signing_key".into(),
            caller: 999,
            target: "kid-old".into(),
            timestamp_ns: 1_700_000_000_000_000_000,
            details: vec![
                ("old_kid".into(), "kid-old".into()),
                ("new_kid".into(), "kid-new".into()),
                ("grace_period_secs".into(), "3600".into()),
            ],
        };
        let encoded = inferadb_ledger_types::encode(&record).unwrap();
        let decoded: AuditRecord = decode(&encoded).unwrap();
        assert_eq!(record, decoded);
        assert_eq!(decoded.details.len(), 3);
    }
}
