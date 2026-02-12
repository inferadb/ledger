//! Shared service helpers for audit logging, rate limiting, validation, and hot key detection.
//!
//! These functions consolidate logic that was previously duplicated across
//! `WriteServiceImpl`, `AdminServiceImpl`, and `MultiShardWriteService`.

use std::sync::Arc;

use inferadb_ledger_proto::proto;
use inferadb_ledger_types::{
    NamespaceId, VaultId,
    audit::{AuditAction, AuditEvent, AuditOutcome, AuditResource},
    config::ValidationConfig,
    validation,
};
use tonic::Status;
use tracing::warn;
use uuid::Uuid;

use crate::{metrics, rate_limit::RateLimiter};

/// Emits an audit event and records the corresponding Prometheus metric.
///
/// If the audit logger is not configured, this is a no-op (only metrics recorded).
/// If the audit log write fails, logs a warning but does not propagate the error —
/// the primary operation's durability takes precedence over audit logging.
pub(crate) fn emit_audit_event(
    audit_logger: Option<&Arc<dyn crate::audit::AuditLogger>>,
    event: &AuditEvent,
) {
    let outcome_str = match &event.outcome {
        AuditOutcome::Success => "success",
        AuditOutcome::Failed { .. } => "failed",
        AuditOutcome::Denied { .. } => "denied",
    };
    metrics::record_audit_event(event.action.as_str(), outcome_str);

    if let Some(logger) = audit_logger
        && let Err(e) = logger.log(event)
    {
        warn!(
            event_id = %event.event_id,
            action = %event.action.as_str(),
            error = %e,
            "Failed to write audit log event"
        );
    }
}

/// Builds an audit event with standard fields.
///
/// The `operations_count` field is set for write-path events and `None` for admin events.
pub(crate) fn build_audit_event(
    action: AuditAction,
    principal: &str,
    resource: AuditResource,
    outcome: AuditOutcome,
    node_id: Option<u64>,
    trace_id: Option<&str>,
    operations_count: Option<usize>,
) -> AuditEvent {
    AuditEvent {
        timestamp: chrono::Utc::now().to_rfc3339(),
        event_id: Uuid::new_v4().to_string(),
        principal: principal.to_string(),
        action,
        resource,
        outcome,
        node_id,
        trace_id: trace_id.map(String::from),
        operations_count,
    }
}

/// Checks all rate limit tiers (backpressure, namespace, client).
///
/// Returns `Status::resource_exhausted` with `retry-after-ms` metadata and
/// structured [`ErrorDetails`](inferadb_ledger_proto::proto::ErrorDetails) if rejected.
pub(crate) fn check_rate_limit(
    rate_limiter: Option<&Arc<RateLimiter>>,
    client_id: &str,
    namespace_id: NamespaceId,
) -> Result<(), Status> {
    if let Some(limiter) = rate_limiter {
        limiter.check(client_id, namespace_id).map_err(|rejection| {
            warn!(
                namespace_id = namespace_id.value(),
                level = rejection.level.as_str(),
                reason = rejection.reason.as_str(),
                "Rate limit exceeded"
            );
            metrics::record_rate_limit_rejected(
                rejection.level.as_str(),
                rejection.reason.as_str(),
            );

            // Build structured error details with rate limit context
            let retry_ms = i32::try_from(rejection.retry_after_ms).unwrap_or(i32::MAX);
            let mut context = std::collections::HashMap::new();
            context.insert("namespace_id".to_owned(), namespace_id.value().to_string());
            context.insert("level".to_owned(), rejection.level.as_str().to_owned());
            context.insert("reason".to_owned(), rejection.reason.as_str().to_owned());

            let details = super::error_details::build_error_details(
                inferadb_ledger_types::ErrorCode::AppInternal.as_u16(),
                true,
                Some(retry_ms),
                context,
                Some("Reduce request rate or wait before retrying"),
            );
            let encoded = prost::Message::encode_to_vec(&details);
            let mut status = Status::with_details(
                tonic::Code::ResourceExhausted,
                rejection.to_string(),
                encoded.into(),
            );

            // Backward-compatible: also set retry-after-ms metadata header
            if let Ok(val) =
                tonic::metadata::MetadataValue::try_from(rejection.retry_after_ms.to_string())
            {
                status.metadata_mut().insert("retry-after-ms", val);
            }
            status
        })?;
    }
    Ok(())
}

/// Checks whether creating a new vault would exceed the namespace vault quota.
///
/// Returns `Status::resource_exhausted` with structured
/// [`ErrorDetails`](inferadb_ledger_proto::proto::ErrorDetails) including current/max values if the
/// quota is exceeded.
pub(crate) fn check_vault_quota(
    quota_checker: Option<&crate::quota::QuotaChecker>,
    namespace_id: NamespaceId,
) -> Result<(), Status> {
    let checker = match quota_checker {
        Some(c) => c,
        None => return Ok(()),
    };

    if let Err(exceeded) = checker.check_vault_count(namespace_id) {
        warn!(
            namespace_id = namespace_id.value(),
            current = exceeded.current,
            limit = exceeded.limit,
            "Vault count quota exceeded"
        );

        let mut context = std::collections::HashMap::new();
        context.insert("namespace_id".to_owned(), namespace_id.value().to_string());
        context.insert("resource".to_owned(), exceeded.resource.to_string());
        context.insert("current".to_owned(), exceeded.current.to_string());
        context.insert("limit".to_owned(), exceeded.limit.to_string());

        let details = super::error_details::build_error_details(
            inferadb_ledger_types::ErrorCode::AppQuotaExceeded.as_u16(),
            true,
            None,
            context,
            Some(inferadb_ledger_types::ErrorCode::AppQuotaExceeded.suggested_action()),
        );
        let encoded = prost::Message::encode_to_vec(&details);
        return Err(Status::with_details(
            tonic::Code::ResourceExhausted,
            exceeded.to_string(),
            encoded.into(),
        ));
    }

    Ok(())
}

/// Checks whether a write operation's estimated payload would exceed
/// the namespace storage quota.
///
/// `estimated_bytes` is the sum of key + value sizes for all operations.
/// Returns `Status::resource_exhausted` with structured
/// [`ErrorDetails`](inferadb_ledger_proto::proto::ErrorDetails) including current/max values if the
/// quota is exceeded.
pub(crate) fn check_storage_quota(
    quota_checker: Option<&crate::quota::QuotaChecker>,
    namespace_id: NamespaceId,
    estimated_bytes: u64,
) -> Result<(), Status> {
    let checker = match quota_checker {
        Some(c) => c,
        None => return Ok(()),
    };

    if let Err(exceeded) = checker.check_storage_estimate(namespace_id, estimated_bytes) {
        warn!(
            namespace_id = namespace_id.value(),
            estimated_bytes,
            limit = exceeded.limit,
            "Storage quota exceeded"
        );

        let mut context = std::collections::HashMap::new();
        context.insert("namespace_id".to_owned(), namespace_id.value().to_string());
        context.insert("resource".to_owned(), exceeded.resource.to_string());
        context.insert("estimated_bytes".to_owned(), estimated_bytes.to_string());
        context.insert("limit".to_owned(), exceeded.limit.to_string());

        let details = super::error_details::build_error_details(
            inferadb_ledger_types::ErrorCode::AppQuotaExceeded.as_u16(),
            true,
            None,
            context,
            Some(inferadb_ledger_types::ErrorCode::AppQuotaExceeded.suggested_action()),
        );
        let encoded = prost::Message::encode_to_vec(&details);
        return Err(Status::with_details(
            tonic::Code::ResourceExhausted,
            exceeded.to_string(),
            encoded.into(),
        ));
    }

    Ok(())
}

/// Estimates the total byte size of operations for storage quota checking.
///
/// Sums key + value sizes across all operations as an approximate storage estimate.
pub(crate) fn estimate_operations_bytes(
    operations: &[inferadb_ledger_proto::proto::Operation],
) -> u64 {
    let mut total: u64 = 0;
    for op in operations {
        if let Some(ref inner) = op.op {
            match inner {
                inferadb_ledger_proto::proto::operation::Op::SetEntity(set) => {
                    total = total.saturating_add(set.key.len() as u64);
                    total = total.saturating_add(set.value.len() as u64);
                },
                inferadb_ledger_proto::proto::operation::Op::DeleteEntity(del) => {
                    total = total.saturating_add(del.key.len() as u64);
                },
                inferadb_ledger_proto::proto::operation::Op::CreateRelationship(rel) => {
                    total = total.saturating_add(rel.resource.len() as u64);
                    total = total.saturating_add(rel.relation.len() as u64);
                    total = total.saturating_add(rel.subject.len() as u64);
                },
                inferadb_ledger_proto::proto::operation::Op::DeleteRelationship(rel) => {
                    total = total.saturating_add(rel.resource.len() as u64);
                    total = total.saturating_add(rel.relation.len() as u64);
                    total = total.saturating_add(rel.subject.len() as u64);
                },
                inferadb_ledger_proto::proto::operation::Op::ExpireEntity(exp) => {
                    total = total.saturating_add(exp.key.len() as u64);
                },
            }
        }
    }
    total
}

/// Validates all operations in a proto operation list.
///
/// Checks per-operation field limits (key/value size, character whitelist)
/// and aggregate limits (operations count, total payload size). Returns
/// `Status::invalid_argument` with structured [`ErrorDetails`] on failure.
pub(crate) fn validate_operations(
    operations: &[proto::Operation],
    config: &ValidationConfig,
) -> Result<(), Status> {
    validation::validate_operations_count(operations.len(), config)
        .map_err(|e| validation_status(e.to_string()))?;

    let mut total_bytes: usize = 0;
    for proto_op in operations {
        let Some(ref op) = proto_op.op else {
            return Err(validation_status("Operation missing op field"));
        };
        match op {
            proto::operation::Op::SetEntity(se) => {
                validation::validate_key(&se.key, config)
                    .map_err(|e| validation_status(e.to_string()))?;
                validation::validate_value(&se.value, config)
                    .map_err(|e| validation_status(e.to_string()))?;
                total_bytes += se.key.len() + se.value.len();
            },
            proto::operation::Op::DeleteEntity(de) => {
                validation::validate_key(&de.key, config)
                    .map_err(|e| validation_status(e.to_string()))?;
                total_bytes += de.key.len();
            },
            proto::operation::Op::ExpireEntity(ee) => {
                validation::validate_key(&ee.key, config)
                    .map_err(|e| validation_status(e.to_string()))?;
                total_bytes += ee.key.len();
            },
            proto::operation::Op::CreateRelationship(cr) => {
                validation::validate_relationship_string(&cr.resource, "resource", config)
                    .map_err(|e| validation_status(e.to_string()))?;
                validation::validate_relationship_string(&cr.relation, "relation", config)
                    .map_err(|e| validation_status(e.to_string()))?;
                validation::validate_relationship_string(&cr.subject, "subject", config)
                    .map_err(|e| validation_status(e.to_string()))?;
                total_bytes += cr.resource.len() + cr.relation.len() + cr.subject.len();
            },
            proto::operation::Op::DeleteRelationship(dr) => {
                validation::validate_relationship_string(&dr.resource, "resource", config)
                    .map_err(|e| validation_status(e.to_string()))?;
                validation::validate_relationship_string(&dr.relation, "relation", config)
                    .map_err(|e| validation_status(e.to_string()))?;
                validation::validate_relationship_string(&dr.subject, "subject", config)
                    .map_err(|e| validation_status(e.to_string()))?;
                total_bytes += dr.resource.len() + dr.relation.len() + dr.subject.len();
            },
        }
    }

    validation::validate_batch_payload_bytes(total_bytes, config)
        .map_err(|e| validation_status(e.to_string()))?;

    Ok(())
}

/// Records key accesses from operations for hot key detection.
///
/// Extracts entity/relationship keys from each operation and feeds them
/// to the hot key detector. This runs after rate limiting but before
/// Raft proposal, tracking all non-duplicate, non-rate-limited accesses.
pub(crate) fn record_hot_keys(
    hot_key_detector: Option<&Arc<crate::hot_key_detector::HotKeyDetector>>,
    vault_id: VaultId,
    operations: &[proto::Operation],
) {
    if let Some(detector) = hot_key_detector {
        use proto::operation::Op;
        for op in operations {
            if let Some(ref inner) = op.op {
                let key = match inner {
                    Op::SetEntity(set) => &set.key,
                    Op::DeleteEntity(del) => &del.key,
                    Op::CreateRelationship(rel) => &rel.resource,
                    Op::DeleteRelationship(rel) => &rel.resource,
                    Op::ExpireEntity(exp) => &exp.key,
                };
                detector.record_access(vault_id, key);
            }
        }
    }
}

/// Builds a validation error `Status` with structured [`ErrorDetails`].
///
/// Wraps `Status::invalid_argument` with a binary-encoded `ErrorDetails`
/// containing `AppInvalidArgument` code and recovery guidance.
fn validation_status(message: impl Into<String>) -> Status {
    let message = message.into();
    let details = super::error_details::build_error_details(
        inferadb_ledger_types::ErrorCode::AppInvalidArgument.as_u16(),
        false,
        None,
        std::collections::HashMap::new(),
        Some("Fix the request parameters to conform to field limits"),
    );
    let encoded = prost::Message::encode_to_vec(&details);
    Status::with_details(tonic::Code::InvalidArgument, message, encoded.into())
}

/// Computes a hash of operations for idempotency payload comparison.
///
/// Uses a simple concatenation of operation fields to create a deterministic
/// byte sequence that can be hashed with seahash.
pub(crate) fn hash_operations(operations: &[proto::Operation]) -> Vec<u8> {
    use proto::operation::Op;

    let mut bytes = Vec::new();
    for op in operations {
        if let Some(inner) = &op.op {
            match inner {
                Op::CreateRelationship(cr) => {
                    bytes.extend_from_slice(b"CR");
                    bytes.extend_from_slice(cr.resource.as_bytes());
                    bytes.extend_from_slice(cr.relation.as_bytes());
                    bytes.extend_from_slice(cr.subject.as_bytes());
                },
                Op::DeleteRelationship(dr) => {
                    bytes.extend_from_slice(b"DR");
                    bytes.extend_from_slice(dr.resource.as_bytes());
                    bytes.extend_from_slice(dr.relation.as_bytes());
                    bytes.extend_from_slice(dr.subject.as_bytes());
                },
                Op::SetEntity(se) => {
                    bytes.extend_from_slice(b"SE");
                    bytes.extend_from_slice(se.key.as_bytes());
                    bytes.extend_from_slice(&se.value);
                    if let Some(cond) = &se.condition {
                        bytes.extend_from_slice(&format!("{:?}", cond).into_bytes());
                    }
                    if let Some(exp) = se.expires_at {
                        bytes.extend_from_slice(&exp.to_le_bytes());
                    }
                },
                Op::DeleteEntity(de) => {
                    bytes.extend_from_slice(b"DE");
                    bytes.extend_from_slice(de.key.as_bytes());
                },
                Op::ExpireEntity(ee) => {
                    bytes.extend_from_slice(b"EE");
                    bytes.extend_from_slice(ee.key.as_bytes());
                    bytes.extend_from_slice(&ee.expired_at.to_le_bytes());
                },
            }
        }
    }
    bytes
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    #[test]
    fn test_estimate_operations_bytes_empty() {
        let ops: Vec<inferadb_ledger_proto::proto::Operation> = vec![];
        assert_eq!(estimate_operations_bytes(&ops), 0);
    }

    #[test]
    fn test_estimate_operations_bytes_set_entity() {
        let ops = vec![inferadb_ledger_proto::proto::Operation {
            op: Some(inferadb_ledger_proto::proto::operation::Op::SetEntity(
                inferadb_ledger_proto::proto::SetEntity {
                    key: "hello".to_owned(),
                    value: vec![0u8; 100],
                    condition: None,
                    expires_at: None,
                },
            )),
        }];
        // "hello" = 5 bytes + 100 bytes value
        assert_eq!(estimate_operations_bytes(&ops), 105);
    }

    #[test]
    fn test_estimate_operations_bytes_delete_entity() {
        let ops = vec![inferadb_ledger_proto::proto::Operation {
            op: Some(inferadb_ledger_proto::proto::operation::Op::DeleteEntity(
                inferadb_ledger_proto::proto::DeleteEntity { key: "mykey".to_owned() },
            )),
        }];
        assert_eq!(estimate_operations_bytes(&ops), 5);
    }

    #[test]
    fn test_estimate_operations_bytes_relationship() {
        let ops = vec![inferadb_ledger_proto::proto::Operation {
            op: Some(inferadb_ledger_proto::proto::operation::Op::CreateRelationship(
                inferadb_ledger_proto::proto::CreateRelationship {
                    resource: "doc:123".to_owned(),
                    relation: "viewer".to_owned(),
                    subject: "user:456".to_owned(),
                },
            )),
        }];
        // "doc:123" = 7 + "viewer" = 6 + "user:456" = 8 = 21
        assert_eq!(estimate_operations_bytes(&ops), 21);
    }

    #[test]
    fn test_estimate_operations_bytes_mixed() {
        let ops = vec![
            inferadb_ledger_proto::proto::Operation {
                op: Some(inferadb_ledger_proto::proto::operation::Op::SetEntity(
                    inferadb_ledger_proto::proto::SetEntity {
                        key: "k".to_owned(),
                        value: vec![0u8; 10],
                        condition: None,
                        expires_at: None,
                    },
                )),
            },
            inferadb_ledger_proto::proto::Operation {
                op: Some(inferadb_ledger_proto::proto::operation::Op::DeleteEntity(
                    inferadb_ledger_proto::proto::DeleteEntity { key: "kk".to_owned() },
                )),
            },
            inferadb_ledger_proto::proto::Operation { op: None },
        ];
        // 1 + 10 + 2 + 0 = 13
        assert_eq!(estimate_operations_bytes(&ops), 13);
    }

    #[test]
    fn test_estimate_operations_bytes_expire_entity() {
        let ops = vec![inferadb_ledger_proto::proto::Operation {
            op: Some(inferadb_ledger_proto::proto::operation::Op::ExpireEntity(
                inferadb_ledger_proto::proto::ExpireEntity {
                    key: "expkey".to_owned(),
                    expired_at: 0,
                },
            )),
        }];
        // "expkey" = 6 bytes
        assert_eq!(estimate_operations_bytes(&ops), 6);
    }

    #[test]
    fn test_check_vault_quota_none_checker_passes() {
        // No quota checker → always passes
        let result = check_vault_quota(None, NamespaceId::new(1));
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_storage_quota_none_checker_passes() {
        let result = check_storage_quota(None, NamespaceId::new(1), u64::MAX);
        assert!(result.is_ok());
    }
}
