//! Shared service helpers for rate limiting, validation, and hot key detection.
//!
//! These functions consolidate logic that was previously duplicated across
//! `WriteService` and `AdminService`.

use std::{fmt::Display, sync::Arc};

use inferadb_ledger_proto::proto;
use inferadb_ledger_raft::{graceful_shutdown::HealthState, metrics, rate_limit::RateLimiter};
use inferadb_ledger_state::{
    StateLayer,
    system::{App, AppVaultConnection, SYSTEM_VAULT_ID, SystemKeys},
};
use inferadb_ledger_store::{Database, FileBackend};
use inferadb_ledger_types::{
    AppId, ErrorCode, OrganizationId, VaultId, config::ValidationConfig, decode, validation,
};
use tempfile::TempDir;
use tonic::Status;
use tracing::warn;

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
                inferadb_ledger_types::DiagnosticCode::AppInternal.as_u16(),
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
    hot_key_detector: Option<&Arc<inferadb_ledger_raft::hot_key_detector::HotKeyDetector>>,
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
        inferadb_ledger_types::DiagnosticCode::AppInvalidArgument.as_u16(),
        false,
        None,
        std::collections::HashMap::new(),
        Some("Fix the request parameters to conform to field limits"),
    );
    let encoded = prost::Message::encode_to_vec(&details);
    Status::with_details(tonic::Code::InvalidArgument, message, encoded.into())
}

/// Loads an app from state by organization and app ID.
///
/// Reads the system vault, decodes the [`App`] record, and returns it.
/// Returns `Status::not_found` if the app doesn't exist, or
/// `Status::internal` on storage/decoding failures.
pub(crate) fn load_app(
    state: &StateLayer<FileBackend>,
    org_id: OrganizationId,
    app_id: AppId,
) -> Result<App, Status> {
    let key = SystemKeys::app_key(org_id, app_id);
    let entity = state
        .get_entity(SYSTEM_VAULT_ID, key.as_bytes())
        .map_err(|e| Status::internal(format!("Failed to read app: {e}")))?
        .ok_or_else(|| Status::not_found(format!("App {} not found", app_id)))?;
    decode::<App>(&entity.value).map_err(|e| Status::internal(format!("Failed to decode app: {e}")))
}

/// Computes a hash of operations for idempotency payload comparison.
///
/// Uses length-prefixed encoding of operation fields to create a
/// deterministic, collision-resistant byte sequence. Each variable-length
/// field is preceded by its `u32 LE` length to prevent boundary ambiguity
/// (e.g., `("ab","cd")` vs `("abc","d")` producing distinct byte sequences).
pub(crate) fn hash_operations(operations: &[proto::Operation]) -> Vec<u8> {
    use proto::operation::Op;

    fn push_lp(bytes: &mut Vec<u8>, data: &[u8]) {
        bytes.extend_from_slice(&(data.len() as u32).to_le_bytes());
        bytes.extend_from_slice(data);
    }

    let mut bytes = Vec::new();
    for op in operations {
        if let Some(inner) = &op.op {
            match inner {
                Op::CreateRelationship(cr) => {
                    bytes.extend_from_slice(b"CR");
                    push_lp(&mut bytes, cr.resource.as_bytes());
                    push_lp(&mut bytes, cr.relation.as_bytes());
                    push_lp(&mut bytes, cr.subject.as_bytes());
                },
                Op::DeleteRelationship(dr) => {
                    bytes.extend_from_slice(b"DR");
                    push_lp(&mut bytes, dr.resource.as_bytes());
                    push_lp(&mut bytes, dr.relation.as_bytes());
                    push_lp(&mut bytes, dr.subject.as_bytes());
                },
                Op::SetEntity(se) => {
                    bytes.extend_from_slice(b"SE");
                    push_lp(&mut bytes, se.key.as_bytes());
                    push_lp(&mut bytes, &se.value);
                    if let Some(cond) = &se.condition {
                        let cond_bytes = format!("{cond:?}");
                        push_lp(&mut bytes, cond_bytes.as_bytes());
                    }
                    if let Some(exp) = se.expires_at {
                        bytes.extend_from_slice(&exp.to_le_bytes());
                    }
                },
                Op::DeleteEntity(de) => {
                    bytes.extend_from_slice(b"DE");
                    push_lp(&mut bytes, de.key.as_bytes());
                },
                Op::ExpireEntity(ee) => {
                    bytes.extend_from_slice(b"EE");
                    push_lp(&mut bytes, ee.key.as_bytes());
                    bytes.extend_from_slice(&ee.expired_at.to_le_bytes());
                },
            }
        }
    }
    bytes
}

/// Creates a temporary `StateLayer` for historical block replay.
///
/// Returns the temp directory (whose lifetime keeps the temp files alive),
/// and a fresh `StateLayer` backed by an empty database.
///
/// # Errors
///
/// Returns `tonic::Status::internal` if temp dir or database creation fails.
pub(crate) fn create_replay_context() -> Result<(TempDir, StateLayer<FileBackend>), Status> {
    let temp_dir =
        TempDir::new().map_err(|e| Status::internal(format!("Failed to create temp dir: {e}")))?;
    let temp_db = Arc::new(
        Database::<FileBackend>::create(temp_dir.path().join("replay.db"))
            .map_err(|e| Status::internal(format!("Failed to create temp db: {e}")))?,
    );
    let temp_state = StateLayer::new(temp_db);
    Ok((temp_dir, temp_state))
}

/// Maps a storage error to `Status::internal`.
///
/// Consolidates the repeated `.map_err(|e| Status::internal(format!("Storage error: {e}")))`
/// pattern used across read service methods.
pub(crate) fn storage_err(e: impl Display) -> Status {
    Status::internal(format!("Storage error: {e}"))
}

/// Reads and decodes an [`AppVaultConnection`] from the system vault.
///
/// The caller provides the `not_found` status so that post-mutation reads
/// (which indicate a bug) can use `Status::internal` while user-facing
/// lookups use `Status::not_found`.
pub(crate) fn read_vault_connection(
    state: &StateLayer<FileBackend>,
    org_id: OrganizationId,
    app_id: AppId,
    vault_id: VaultId,
    not_found: Status,
) -> Result<AppVaultConnection, Status> {
    let key = SystemKeys::app_vault_key(org_id, app_id, vault_id);
    let entity = state
        .get_entity(SYSTEM_VAULT_ID, key.as_bytes())
        .map_err(|e| Status::internal(format!("Failed to read vault connection: {e}")))?
        .ok_or(not_found)?;
    decode::<AppVaultConnection>(&entity.value)
        .map_err(|e| Status::internal(format!("Failed to decode vault connection: {e}")))
}

/// Maps a [`ErrorCode`] to the corresponding gRPC [`Status`].
///
/// Used by service-layer error handlers to convert structured state-machine
/// error codes into the correct gRPC status without string matching.
pub(crate) fn error_code_to_status(code: ErrorCode, message: String) -> Status {
    match code {
        ErrorCode::NotFound => Status::not_found(message),
        ErrorCode::AlreadyExists => Status::already_exists(message),
        ErrorCode::FailedPrecondition => Status::failed_precondition(message),
        ErrorCode::PermissionDenied => Status::permission_denied(message),
        ErrorCode::InvalidArgument => Status::invalid_argument(message),
        ErrorCode::Internal => Status::internal(message),
        ErrorCode::Unauthenticated => Status::unauthenticated(message),
    }
}
