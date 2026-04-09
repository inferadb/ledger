//! Shared service helpers for rate limiting, validation, and hot key detection.
//!
//! Centralizes logic used by `WriteService`, `AdminService`, and other
//! gRPC service implementations.

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

use super::error_classify;

/// Extracts the caller's user slug from a gRPC request and sets it on the
/// request context for canonical log line emission.
///
/// No-op if the caller field is absent (proto3 optional messages default to `None`).
pub(crate) fn extract_caller(
    ctx: &mut inferadb_ledger_raft::logging::RequestContext,
    caller: &Option<proto::UserSlug>,
) {
    if let Some(ref c) = *caller {
        ctx.set_caller(c.slug);
    }
}

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
        .map_err(|e| error_classify::storage_error(&e))?
        .ok_or_else(|| Status::not_found(format!("App {} not found", app_id)))?;
    decode::<App>(&entity.value).map_err(|e| error_classify::serialization_error(&e))
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
    let temp_dir = TempDir::new().map_err(|e| error_classify::storage_error(&e))?;
    let temp_db = Arc::new(
        Database::<FileBackend>::create(temp_dir.path().join("replay.db"))
            .map_err(|e| error_classify::storage_error(&e))?,
    );
    let temp_state = StateLayer::new(temp_db);
    Ok((temp_dir, temp_state))
}

/// Maps a storage error to `Status::internal`.
///
/// Consolidates the repeated storage error mapping pattern used across read
/// service methods. Logs the full error and returns a generic `Status::internal`.
pub(crate) fn storage_err(e: impl Display) -> Status {
    error_classify::storage_error(&e)
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
        .map_err(|e| error_classify::storage_error(&e))?
        .ok_or(not_found)?;
    decode::<AppVaultConnection>(&entity.value).map_err(|e| error_classify::serialization_error(&e))
}

/// Maps an [`ErrorCode`] to the corresponding gRPC [`Status`].
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
        ErrorCode::RateLimited | ErrorCode::InvitationRateLimited => {
            Status::resource_exhausted(message)
        },
        ErrorCode::Expired | ErrorCode::TooManyAttempts | ErrorCode::InvitationAlreadyResolved => {
            Status::failed_precondition(message)
        },
        ErrorCode::InvitationEmailMismatch => Status::not_found(message),
        ErrorCode::InvitationAlreadyMember | ErrorCode::InvitationDuplicatePending => {
            Status::already_exists(message)
        },
    }
}

/// Ensures GLOBAL applied state on this node is at least as fresh as the
/// leader's committed index.
///
/// On follower nodes, data from recent GLOBAL proposals (vault creation, org
/// provisioning, slug registration) may not have replicated yet. This function
/// calls ReadIndex on the GLOBAL leader to learn the leader's committed index,
/// then waits for the local applied index to reach it.
///
/// On the leader, this is a no-op. Typically completes in <5ms when caught up.
pub(crate) async fn ensure_global_consistency(
    manager: Option<&inferadb_ledger_raft::raft_manager::RaftManager>,
) {
    let Some(manager) = manager else { return };
    let Ok(global) = manager.system_region() else { return };
    if global.handle().is_leader() {
        return;
    }

    // Get the leader's actual committed index via ReadIndex RPC.
    let leader_id = match global.handle().current_leader() {
        Some(id) => id,
        None => return,
    };
    let leader_addr = match manager.peer_addresses().get(leader_id) {
        Some(addr) => addr,
        None => return,
    };

    let committed_index = match read_index_from_leader(leader_id, &leader_addr).await {
        Some(idx) => idx,
        None => {
            // Fallback: use local commit_index (may be stale).
            let idx = global.handle().commit_index();
            if idx == 0 {
                return;
            }
            idx
        },
    };

    let mut watch = global.applied_index_watch();
    let _ = inferadb_ledger_raft::wait_for_apply(
        &mut watch,
        committed_index,
        std::time::Duration::from_secs(5),
    )
    .await;
}

/// Cached channel for ReadIndex RPCs to the GLOBAL leader.
/// Avoids creating a new TCP connection for every follower-side RPC.
static READ_INDEX_CHANNEL: parking_lot::Mutex<Option<(u64, tonic::transport::Channel)>> =
    parking_lot::Mutex::new(None);

/// Calls ReadIndex RPC on the GLOBAL leader to get its committed index.
/// Caches the gRPC channel so all calls to the same leader reuse one TCP connection.
async fn read_index_from_leader(leader_id: u64, leader_addr: &str) -> Option<u64> {
    use inferadb_ledger_proto::proto::raft_service_client::RaftServiceClient;

    // Check the cache — reuse the channel if the leader hasn't changed.
    let channel = {
        let cache = READ_INDEX_CHANNEL.lock();
        if let Some((cached_id, ref ch)) = *cache {
            if cached_id == leader_id { Some(ch.clone()) } else { None }
        } else {
            None
        }
    };

    let channel = match channel {
        Some(ch) => ch,
        None => {
            let endpoint = format!("http://{leader_addr}");
            let ch = tonic::transport::Channel::from_shared(endpoint).ok()?.connect_lazy();
            let mut cache = READ_INDEX_CHANNEL.lock();
            *cache = Some((leader_id, ch.clone()));
            ch
        },
    };

    let mut client = RaftServiceClient::new(channel);
    let resp = client
        .read_index(inferadb_ledger_proto::proto::ReadIndexRequest { region: String::new() })
        .await
        .ok()?;

    Some(resp.into_inner().committed_index)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods, clippy::panic)]
mod tests {
    use inferadb_ledger_proto::proto;
    use inferadb_ledger_raft::graceful_shutdown::HealthState;
    use inferadb_ledger_types::{ErrorCode, VaultId, config::ValidationConfig};

    use super::*;

    // =========================================================================
    // check_not_draining
    // =========================================================================

    #[test]
    fn check_not_draining_none_health_state_accepts() {
        assert!(check_not_draining(None).is_ok());
    }

    #[test]
    fn check_not_draining_starting_phase_rejects() {
        let hs = HealthState::new(); // starts in Starting phase
        let result = check_not_draining(Some(&hs));
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::Unavailable);
    }

    #[test]
    fn check_not_draining_ready_phase_accepts() {
        let hs = HealthState::new();
        hs.mark_ready();
        assert!(check_not_draining(Some(&hs)).is_ok());
    }

    #[test]
    fn check_not_draining_draining_phase_rejects() {
        let hs = HealthState::new();
        hs.mark_ready();
        hs.mark_draining();
        let result = check_not_draining(Some(&hs));
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::Unavailable);
    }

    #[test]
    fn check_not_draining_shutting_down_phase_rejects() {
        let hs = HealthState::new();
        hs.mark_shutting_down();
        let result = check_not_draining(Some(&hs));
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::Unavailable);
    }

    // =========================================================================
    // extract_caller
    // =========================================================================

    #[test]
    fn extract_caller_sets_slug_when_present() {
        let mut ctx = inferadb_ledger_raft::logging::RequestContext::new("test", "method");
        let caller = Some(proto::UserSlug { slug: 42 });
        extract_caller(&mut ctx, &caller);
        assert_eq!(ctx.caller_or_zero(), 42);
    }

    #[test]
    fn extract_caller_noop_when_none() {
        let mut ctx = inferadb_ledger_raft::logging::RequestContext::new("test", "method");
        extract_caller(&mut ctx, &None);
        assert_eq!(ctx.caller_or_zero(), 0);
    }

    // =========================================================================
    // validation_status
    // =========================================================================

    #[test]
    fn validation_status_returns_invalid_argument() {
        let status = validation_status("field too long");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert_eq!(status.message(), "field too long");
        // Should have binary error details
        assert!(!status.details().is_empty());
    }

    #[test]
    fn validation_status_details_contain_error_code() {
        let status = validation_status("bad input");
        let details =
            <proto::ErrorDetails as prost::Message>::decode(status.details()).expect("decode");
        assert!(!details.is_retryable);
        assert!(details.suggested_action.is_some());
    }

    // =========================================================================
    // validate_operations
    // =========================================================================

    #[test]
    fn validate_operations_empty_list_rejected() {
        let config = ValidationConfig::default();
        let err = validate_operations(&[], &config).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn validate_operations_valid_set_entity() {
        let config = ValidationConfig::default();
        let ops = vec![proto::Operation {
            op: Some(proto::operation::Op::SetEntity(proto::SetEntity {
                key: "user:1".to_owned(),
                value: b"data".to_vec(),
                condition: None,
                expires_at: None,
            })),
        }];
        assert!(validate_operations(&ops, &config).is_ok());
    }

    #[test]
    fn validate_operations_valid_delete_entity() {
        let config = ValidationConfig::default();
        let ops = vec![proto::Operation {
            op: Some(proto::operation::Op::DeleteEntity(proto::DeleteEntity {
                key: "user:1".to_owned(),
            })),
        }];
        assert!(validate_operations(&ops, &config).is_ok());
    }

    #[test]
    fn validate_operations_valid_expire_entity() {
        let config = ValidationConfig::default();
        let ops = vec![proto::Operation {
            op: Some(proto::operation::Op::ExpireEntity(proto::ExpireEntity {
                key: "user:1".to_owned(),
                expired_at: 1000,
            })),
        }];
        assert!(validate_operations(&ops, &config).is_ok());
    }

    #[test]
    fn validate_operations_valid_create_relationship() {
        let config = ValidationConfig::default();
        let ops = vec![proto::Operation {
            op: Some(proto::operation::Op::CreateRelationship(proto::CreateRelationship {
                resource: "doc:readme".to_owned(),
                relation: "viewer".to_owned(),
                subject: "user:alice".to_owned(),
            })),
        }];
        assert!(validate_operations(&ops, &config).is_ok());
    }

    #[test]
    fn validate_operations_valid_delete_relationship() {
        let config = ValidationConfig::default();
        let ops = vec![proto::Operation {
            op: Some(proto::operation::Op::DeleteRelationship(proto::DeleteRelationship {
                resource: "doc:readme".to_owned(),
                relation: "viewer".to_owned(),
                subject: "user:alice".to_owned(),
            })),
        }];
        assert!(validate_operations(&ops, &config).is_ok());
    }

    #[test]
    fn validate_operations_missing_op_field_rejects() {
        let config = ValidationConfig::default();
        let ops = vec![proto::Operation { op: None }];
        let err = validate_operations(&ops, &config).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("missing op field"));
    }

    #[test]
    fn validate_operations_system_key_rejected() {
        let config = ValidationConfig::default();
        let ops = vec![proto::Operation {
            op: Some(proto::operation::Op::SetEntity(proto::SetEntity {
                key: "_meta:seq".to_owned(),
                value: b"x".to_vec(),
                condition: None,
                expires_at: None,
            })),
        }];
        let err = validate_operations(&ops, &config).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn validate_operations_too_many_operations_rejected() {
        let config = ValidationConfig::builder().max_operations_per_write(2).build().unwrap();
        let ops: Vec<_> = (0..3)
            .map(|i| proto::Operation {
                op: Some(proto::operation::Op::DeleteEntity(proto::DeleteEntity {
                    key: format!("k{i}"),
                })),
            })
            .collect();
        let err = validate_operations(&ops, &config).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn validate_operations_oversized_payload_rejected() {
        let config = ValidationConfig::builder().max_batch_payload_bytes(10).build().unwrap();
        let ops = vec![proto::Operation {
            op: Some(proto::operation::Op::SetEntity(proto::SetEntity {
                key: "k".to_owned(),
                value: vec![0u8; 20],
                condition: None,
                expires_at: None,
            })),
        }];
        let err = validate_operations(&ops, &config).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    // =========================================================================
    // record_hot_keys
    // =========================================================================

    #[test]
    fn record_hot_keys_none_detector_is_noop() {
        let ops = vec![proto::Operation {
            op: Some(proto::operation::Op::SetEntity(proto::SetEntity {
                key: "k".to_owned(),
                value: vec![],
                condition: None,
                expires_at: None,
            })),
        }];
        // Should not panic with None detector
        record_hot_keys(None, VaultId::new(1), &ops);
    }

    #[test]
    fn record_hot_keys_with_detector_records_accesses() {
        let detector = Arc::new(inferadb_ledger_raft::hot_key_detector::HotKeyDetector::new(
            &inferadb_ledger_types::config::HotKeyConfig::default(),
        ));
        let ops = vec![
            proto::Operation {
                op: Some(proto::operation::Op::SetEntity(proto::SetEntity {
                    key: "entity:1".to_owned(),
                    value: vec![],
                    condition: None,
                    expires_at: None,
                })),
            },
            proto::Operation {
                op: Some(proto::operation::Op::DeleteEntity(proto::DeleteEntity {
                    key: "entity:2".to_owned(),
                })),
            },
            proto::Operation {
                op: Some(proto::operation::Op::CreateRelationship(proto::CreateRelationship {
                    resource: "doc:1".to_owned(),
                    relation: "viewer".to_owned(),
                    subject: "user:1".to_owned(),
                })),
            },
            proto::Operation {
                op: Some(proto::operation::Op::DeleteRelationship(proto::DeleteRelationship {
                    resource: "doc:2".to_owned(),
                    relation: "editor".to_owned(),
                    subject: "user:2".to_owned(),
                })),
            },
            proto::Operation {
                op: Some(proto::operation::Op::ExpireEntity(proto::ExpireEntity {
                    key: "entity:3".to_owned(),
                    expired_at: 1000,
                })),
            },
        ];
        // Should not panic and should record all 5 keys
        record_hot_keys(Some(&detector), VaultId::new(1), &ops);
    }

    #[test]
    fn record_hot_keys_skips_none_op() {
        let detector = Arc::new(inferadb_ledger_raft::hot_key_detector::HotKeyDetector::new(
            &inferadb_ledger_types::config::HotKeyConfig::default(),
        ));
        let ops = vec![proto::Operation { op: None }];
        record_hot_keys(Some(&detector), VaultId::new(1), &ops);
    }

    // =========================================================================
    // hash_operations
    // =========================================================================

    #[test]
    fn hash_operations_empty_returns_empty() {
        assert!(hash_operations(&[]).is_empty());
    }

    #[test]
    fn hash_operations_deterministic() {
        let ops = vec![proto::Operation {
            op: Some(proto::operation::Op::SetEntity(proto::SetEntity {
                key: "k".to_owned(),
                value: b"v".to_vec(),
                condition: None,
                expires_at: None,
            })),
        }];
        let h1 = hash_operations(&ops);
        let h2 = hash_operations(&ops);
        assert_eq!(h1, h2);
    }

    #[test]
    fn hash_operations_different_ops_differ() {
        let ops_a = vec![proto::Operation {
            op: Some(proto::operation::Op::SetEntity(proto::SetEntity {
                key: "a".to_owned(),
                value: b"v".to_vec(),
                condition: None,
                expires_at: None,
            })),
        }];
        let ops_b = vec![proto::Operation {
            op: Some(proto::operation::Op::SetEntity(proto::SetEntity {
                key: "b".to_owned(),
                value: b"v".to_vec(),
                condition: None,
                expires_at: None,
            })),
        }];
        assert_ne!(hash_operations(&ops_a), hash_operations(&ops_b));
    }

    #[test]
    fn hash_operations_length_prefix_prevents_boundary_collision() {
        // "ab" + "cd" vs "abc" + "d" should differ due to length prefixing
        let ops_a = vec![proto::Operation {
            op: Some(proto::operation::Op::CreateRelationship(proto::CreateRelationship {
                resource: "ab".to_owned(),
                relation: "cd".to_owned(),
                subject: "ef".to_owned(),
            })),
        }];
        let ops_b = vec![proto::Operation {
            op: Some(proto::operation::Op::CreateRelationship(proto::CreateRelationship {
                resource: "abc".to_owned(),
                relation: "d".to_owned(),
                subject: "ef".to_owned(),
            })),
        }];
        assert_ne!(hash_operations(&ops_a), hash_operations(&ops_b));
    }

    #[test]
    fn hash_operations_all_variants_produce_nonempty() {
        let ops = vec![
            proto::Operation {
                op: Some(proto::operation::Op::SetEntity(proto::SetEntity {
                    key: "k".to_owned(),
                    value: b"v".to_vec(),
                    condition: None,
                    expires_at: Some(12345),
                })),
            },
            proto::Operation {
                op: Some(proto::operation::Op::DeleteEntity(proto::DeleteEntity {
                    key: "k".to_owned(),
                })),
            },
            proto::Operation {
                op: Some(proto::operation::Op::ExpireEntity(proto::ExpireEntity {
                    key: "k".to_owned(),
                    expired_at: 999,
                })),
            },
            proto::Operation {
                op: Some(proto::operation::Op::CreateRelationship(proto::CreateRelationship {
                    resource: "r".to_owned(),
                    relation: "rel".to_owned(),
                    subject: "s".to_owned(),
                })),
            },
            proto::Operation {
                op: Some(proto::operation::Op::DeleteRelationship(proto::DeleteRelationship {
                    resource: "r".to_owned(),
                    relation: "rel".to_owned(),
                    subject: "s".to_owned(),
                })),
            },
        ];
        let hash = hash_operations(&ops);
        assert!(!hash.is_empty());
    }

    #[test]
    fn hash_operations_set_with_condition_differs_from_without() {
        let ops_no_cond = vec![proto::Operation {
            op: Some(proto::operation::Op::SetEntity(proto::SetEntity {
                key: "k".to_owned(),
                value: b"v".to_vec(),
                condition: None,
                expires_at: None,
            })),
        }];
        let ops_with_cond = vec![proto::Operation {
            op: Some(proto::operation::Op::SetEntity(proto::SetEntity {
                key: "k".to_owned(),
                value: b"v".to_vec(),
                condition: Some(proto::SetCondition {
                    condition: Some(proto::set_condition::Condition::NotExists(true)),
                }),
                expires_at: None,
            })),
        }];
        assert_ne!(hash_operations(&ops_no_cond), hash_operations(&ops_with_cond));
    }

    #[test]
    fn hash_operations_skips_none_op() {
        let ops = vec![proto::Operation { op: None }];
        assert!(hash_operations(&ops).is_empty());
    }

    #[test]
    fn hash_operations_create_vs_delete_relationship_differ() {
        let ops_create = vec![proto::Operation {
            op: Some(proto::operation::Op::CreateRelationship(proto::CreateRelationship {
                resource: "r".to_owned(),
                relation: "rel".to_owned(),
                subject: "s".to_owned(),
            })),
        }];
        let ops_delete = vec![proto::Operation {
            op: Some(proto::operation::Op::DeleteRelationship(proto::DeleteRelationship {
                resource: "r".to_owned(),
                relation: "rel".to_owned(),
                subject: "s".to_owned(),
            })),
        }];
        assert_ne!(hash_operations(&ops_create), hash_operations(&ops_delete));
    }

    // =========================================================================
    // error_code_to_status
    // =========================================================================

    #[test]
    fn error_code_not_found_maps_to_grpc_not_found() {
        let s = error_code_to_status(ErrorCode::NotFound, "gone".into());
        assert_eq!(s.code(), tonic::Code::NotFound);
        assert_eq!(s.message(), "gone");
    }

    #[test]
    fn error_code_already_exists_maps_to_grpc_already_exists() {
        let s = error_code_to_status(ErrorCode::AlreadyExists, "dup".into());
        assert_eq!(s.code(), tonic::Code::AlreadyExists);
    }

    #[test]
    fn error_code_failed_precondition_maps_correctly() {
        let s = error_code_to_status(ErrorCode::FailedPrecondition, "pre".into());
        assert_eq!(s.code(), tonic::Code::FailedPrecondition);
    }

    #[test]
    fn error_code_permission_denied_maps_correctly() {
        let s = error_code_to_status(ErrorCode::PermissionDenied, "denied".into());
        assert_eq!(s.code(), tonic::Code::PermissionDenied);
    }

    #[test]
    fn error_code_invalid_argument_maps_correctly() {
        let s = error_code_to_status(ErrorCode::InvalidArgument, "bad".into());
        assert_eq!(s.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn error_code_internal_maps_correctly() {
        let s = error_code_to_status(ErrorCode::Internal, "oops".into());
        assert_eq!(s.code(), tonic::Code::Internal);
    }

    #[test]
    fn error_code_unauthenticated_maps_correctly() {
        let s = error_code_to_status(ErrorCode::Unauthenticated, "no auth".into());
        assert_eq!(s.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn error_code_rate_limited_maps_to_resource_exhausted() {
        let s = error_code_to_status(ErrorCode::RateLimited, "slow down".into());
        assert_eq!(s.code(), tonic::Code::ResourceExhausted);
    }

    #[test]
    fn error_code_invitation_rate_limited_maps_to_resource_exhausted() {
        let s = error_code_to_status(ErrorCode::InvitationRateLimited, "too many".into());
        assert_eq!(s.code(), tonic::Code::ResourceExhausted);
    }

    #[test]
    fn error_code_expired_maps_to_failed_precondition() {
        let s = error_code_to_status(ErrorCode::Expired, "expired".into());
        assert_eq!(s.code(), tonic::Code::FailedPrecondition);
    }

    #[test]
    fn error_code_too_many_attempts_maps_to_failed_precondition() {
        let s = error_code_to_status(ErrorCode::TooManyAttempts, "locked".into());
        assert_eq!(s.code(), tonic::Code::FailedPrecondition);
    }

    #[test]
    fn error_code_invitation_already_resolved_maps_to_failed_precondition() {
        let s = error_code_to_status(ErrorCode::InvitationAlreadyResolved, "done".into());
        assert_eq!(s.code(), tonic::Code::FailedPrecondition);
    }

    #[test]
    fn error_code_invitation_email_mismatch_maps_to_not_found() {
        let s = error_code_to_status(ErrorCode::InvitationEmailMismatch, "mismatch".into());
        assert_eq!(s.code(), tonic::Code::NotFound);
    }

    #[test]
    fn error_code_invitation_already_member_maps_to_already_exists() {
        let s = error_code_to_status(ErrorCode::InvitationAlreadyMember, "member".into());
        assert_eq!(s.code(), tonic::Code::AlreadyExists);
    }

    #[test]
    fn error_code_invitation_duplicate_pending_maps_to_already_exists() {
        let s = error_code_to_status(ErrorCode::InvitationDuplicatePending, "pending".into());
        assert_eq!(s.code(), tonic::Code::AlreadyExists);
    }

    // =========================================================================
    // storage_err
    // =========================================================================

    #[test]
    fn storage_err_returns_internal_status() {
        let s = storage_err("disk failure");
        assert_eq!(s.code(), tonic::Code::Internal);
        assert_eq!(s.message(), "storage error");
    }

    // =========================================================================
    // create_replay_context
    // =========================================================================

    #[test]
    fn create_replay_context_returns_valid_state_layer() {
        let (_temp_dir, _state) = create_replay_context().expect("should succeed");
        // The temp_dir and state should be usable (no panic)
    }
}
