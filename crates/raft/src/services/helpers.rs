//! Shared service helpers for rate limiting, validation, and hot key detection.
//!
//! These functions consolidate logic that was previously duplicated across
//! `WriteServiceImpl`, `AdminServiceImpl`, and `MultiRegionWriteService`.

use std::sync::Arc;

use inferadb_ledger_proto::proto;
use inferadb_ledger_types::{OrganizationId, VaultId, config::ValidationConfig, validation};
use tonic::Status;
use tracing::warn;

use crate::{graceful_shutdown::HealthState, metrics, rate_limit::RateLimiter};

/// Rejects the request if the node is draining (not accepting new proposals).
///
/// Returns `Status::unavailable` so clients retry on a different node.
/// If `health_state` is `None`, the check is a no-op (accepts everything).
pub(crate) fn check_not_draining(health_state: Option<&HealthState>) -> Result<(), Status> {
    if let Some(hs) = health_state
        && !hs.is_accepting_proposals()
    {
        return Err(Status::unavailable("Node is draining \u{2014} not accepting new proposals"));
    }
    Ok(())
}

/// Checks all rate limit tiers (backpressure, organization, client).
///
/// Returns `Status::resource_exhausted` with `retry-after-ms` metadata and
/// structured [`ErrorDetails`](inferadb_ledger_proto::proto::ErrorDetails) if rejected.
pub(crate) fn check_rate_limit(
    rate_limiter: Option<&Arc<RateLimiter>>,
    client_id: &str,
    organization: OrganizationId,
) -> Result<(), Status> {
    if let Some(limiter) = rate_limiter {
        limiter.check(client_id, organization).map_err(|rejection| {
            warn!(
                organization = organization.value(),
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
            context.insert("organization".to_owned(), organization.value().to_string());
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
    vault: VaultId,
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
                detector.record_access(vault, key);
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
/// Uses concatenation of operation fields to create a deterministic
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
