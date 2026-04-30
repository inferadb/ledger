//! Shared service helpers for rate limiting, validation, and hot key detection.
//!
//! Centralizes logic used by `WriteService`, `AdminService`, and other
//! gRPC service implementations.
//!
//! All fallible helpers return [`WireError`] — the wire-protocol-native error
//! type. Tonic call sites bridge via
//! [`super::wire_helpers::wire_error_to_tonic_status`]; once the wire-trait
//! impls inline (F.1.f.1), the bridge disappears and these helpers become the
//! canonical primitives.

use std::{collections::BTreeMap, fmt::Display, sync::Arc};

use inferadb_ledger_raft::{graceful_shutdown::HealthState, metrics, rate_limit::RateLimiter};
use inferadb_ledger_state::{
    StateLayer,
    system::{App, AppVaultConnection, SYSTEM_VAULT_ID, SystemKeys},
};
use inferadb_ledger_store::{Database, FileBackend};
use inferadb_ledger_types::{
    AppId, OrganizationId, VaultId, config::ValidationConfig, decode, validation,
};
use inferadb_ledger_wire::{ErrorCode, WireError};
use tempfile::TempDir;
use tracing::warn;

use super::{error_classify, wire_helpers::build_wire_error};

/// Extracts the authenticated caller from a wire `RequestContext`.
///
/// Reads the authenticated user from the dispatcher-built
/// [`inferadb_ledger_wire::RequestContext::caller_identity`] and stores
/// `user_id.value() as u64` on the log context's `caller` field. The log
/// field is an opaque numeric identifier (see
/// [`inferadb_ledger_raft::logging::RequestContext::set_caller`]) — under
/// wire mode it is the internal `UserId(i64)` rather than the external
/// `UserSlug(u64)` that proto handlers carried, but the field itself does
/// not distinguish the two id spaces. F.1.f.0.4 (which builds the synthetic
/// wire `RequestContext` for tonic adapters) will populate `user_id` from
/// the proto slug via [`super::slug_resolver::SlugResolver::resolve_user`]
/// before invoking handler code; this helper does not perform any
/// resolution itself.
///
/// No-op when `caller_identity.user_id` is `None` (system-initiated requests,
/// `Anonymous` connections, peer-to-peer Raft traffic).
#[allow(dead_code)] // Wired by F.1.f.0.4 once tonic adapters build the synthetic wire RequestContext.
pub(crate) fn extract_caller_from_wire(
    ctx: &mut inferadb_ledger_raft::logging::RequestContext,
    wire_ctx: &inferadb_ledger_wire::RequestContext,
) {
    if let Some(uid) = wire_ctx.caller_identity.user_id {
        // `set_caller` accepts a u64 opaque numeric. Internal user ids fit
        // (i64 → u64 cast preserves the bit pattern; negative ids never
        // appear on a populated `UserId`).
        let raw = uid.value();
        if raw >= 0 {
            // Defensive: only emit non-negative ids on the log line.
            ctx.set_caller(raw as u64);
        }
    }
}

/// Rejects the request if the node is draining (not accepting new proposals).
///
/// Returns a `WireError` with `ErrorCode::StaleRouting` so clients retry on a
/// different node — `StaleRouting` is the wire-side analogue of gRPC
/// `Unavailable` (see `wire_code_to_tonic` in
/// [`super::wire_helpers`]). If `health_state` is `None`, the check is a
/// no-op (accepts everything).
pub(crate) fn check_not_draining(health_state: Option<&HealthState>) -> Result<(), WireError> {
    if let Some(hs) = health_state
        && !hs.is_accepting_proposals()
    {
        return Err(build_wire_error(
            ErrorCode::StaleRouting,
            "Node is draining \u{2014} not accepting new proposals",
            "",
            false,
            0,
            BTreeMap::new(),
            "",
        ));
    }
    Ok(())
}

/// Checks all rate limit tiers (backpressure, organization, client).
///
/// Returns a `WireError` with `ErrorCode::RateLimited`, populated
/// `retry_after_ms`, and structured `context` (`organization`, `level`,
/// `reason`) when rejected. The wire transport surfaces the context map
/// directly; the tonic adapter
/// (`wire_helpers::wire_error_to_tonic_status`) round-trips it back into a
/// proto `ErrorDetails` for legacy SDKs.
pub(crate) fn check_rate_limit(
    rate_limiter: Option<&Arc<RateLimiter>>,
    client_id: &str,
    organization: OrganizationId,
) -> Result<(), WireError> {
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

            let mut context = BTreeMap::new();
            context.insert("organization".to_owned(), organization.value().to_string());
            context.insert("level".to_owned(), rejection.level.as_str().to_owned());
            context.insert("reason".to_owned(), rejection.reason.as_str().to_owned());

            build_wire_error(
                ErrorCode::RateLimited,
                rejection.to_string(),
                inferadb_ledger_types::DiagnosticCode::AppInternal.as_u16().to_string(),
                true,
                rejection.retry_after_ms,
                context,
                "Reduce request rate or wait before retrying",
            )
        })?;
    }
    Ok(())
}

/// Validates all operations in a wire operation list.
///
/// Checks per-operation field limits (key/value size, character whitelist)
/// and aggregate limits (operations count, total payload size). Returns a
/// `WireError` with `ErrorCode::InvalidArgument` and a recovery suggestion
/// on failure.
pub(crate) fn validate_operations(
    operations: &[inferadb_ledger_wire::services::shared::Operation],
    config: &ValidationConfig,
) -> Result<(), WireError> {
    use inferadb_ledger_wire::services::shared::OperationKind;

    validation::validate_operations_count(operations.len(), config)
        .map_err(|e| validation_wire_error(e.to_string()))?;

    let mut total_bytes: usize = 0;
    for wire_op in operations {
        let Some(ref op) = wire_op.op else {
            return Err(validation_wire_error("Operation missing op field"));
        };
        match op {
            OperationKind::SetEntity(se) => {
                validation::validate_key(&se.key, config)
                    .map_err(|e| validation_wire_error(e.to_string()))?;
                validation::validate_value(&se.value, config)
                    .map_err(|e| validation_wire_error(e.to_string()))?;
                total_bytes += se.key.len() + se.value.len();
            },
            OperationKind::DeleteEntity(de) => {
                validation::validate_key(&de.key, config)
                    .map_err(|e| validation_wire_error(e.to_string()))?;
                total_bytes += de.key.len();
            },
            OperationKind::ExpireEntity(ee) => {
                validation::validate_key(&ee.key, config)
                    .map_err(|e| validation_wire_error(e.to_string()))?;
                total_bytes += ee.key.len();
            },
            OperationKind::CreateRelationship(cr) => {
                validation::validate_relationship_string(&cr.resource, "resource", config)
                    .map_err(|e| validation_wire_error(e.to_string()))?;
                validation::validate_relationship_string(&cr.relation, "relation", config)
                    .map_err(|e| validation_wire_error(e.to_string()))?;
                validation::validate_relationship_string(&cr.subject, "subject", config)
                    .map_err(|e| validation_wire_error(e.to_string()))?;
                total_bytes += cr.resource.len() + cr.relation.len() + cr.subject.len();
            },
            OperationKind::DeleteRelationship(dr) => {
                validation::validate_relationship_string(&dr.resource, "resource", config)
                    .map_err(|e| validation_wire_error(e.to_string()))?;
                validation::validate_relationship_string(&dr.relation, "relation", config)
                    .map_err(|e| validation_wire_error(e.to_string()))?;
                validation::validate_relationship_string(&dr.subject, "subject", config)
                    .map_err(|e| validation_wire_error(e.to_string()))?;
                total_bytes += dr.resource.len() + dr.relation.len() + dr.subject.len();
            },
        }
    }

    validation::validate_batch_payload_bytes(total_bytes, config)
        .map_err(|e| validation_wire_error(e.to_string()))?;

    Ok(())
}

/// Records key accesses from operations for hot key detection.
///
/// Extracts entity/relationship keys from each wire operation and feeds them
/// to the hot key detector. This runs after rate limiting but before
/// Raft proposal, tracking all non-duplicate, non-rate-limited accesses.
pub(crate) fn record_hot_keys(
    hot_key_detector: Option<&Arc<inferadb_ledger_raft::hot_key_detector::HotKeyDetector>>,
    vault: VaultId,
    operations: &[inferadb_ledger_wire::services::shared::Operation],
) {
    if let Some(detector) = hot_key_detector {
        use inferadb_ledger_wire::services::shared::OperationKind;
        for op in operations {
            if let Some(ref inner) = op.op {
                let key = match inner {
                    OperationKind::SetEntity(set) => set.key.as_str(),
                    OperationKind::DeleteEntity(del) => del.key.as_str(),
                    OperationKind::CreateRelationship(rel) => rel.resource.as_str(),
                    OperationKind::DeleteRelationship(rel) => rel.resource.as_str(),
                    OperationKind::ExpireEntity(exp) => exp.key.as_str(),
                };
                detector.record_access(vault, key);
            }
        }
    }
}

/// Builds a validation `WireError` with the `AppInvalidArgument` diagnostic
/// code and a recovery suggestion.
///
/// Replaces the legacy proto `ErrorDetails`-encoded `Status::invalid_argument`:
/// the diagnostic code lands in `WireError.context["error_code"]` (via
/// [`build_wire_error`]); tonic callers round-trip the context map back to
/// proto `ErrorDetails` through `wire_error_to_tonic_status`.
fn validation_wire_error(message: impl Into<String>) -> WireError {
    build_wire_error(
        ErrorCode::InvalidArgument,
        message,
        inferadb_ledger_types::DiagnosticCode::AppInvalidArgument.as_u16().to_string(),
        false,
        0,
        BTreeMap::new(),
        "Fix the request parameters to conform to field limits",
    )
}

/// Loads an app from state by organization and app ID.
///
/// Reads the system vault, decodes the [`App`] record, and returns it.
/// Returns `WireError` with `ErrorCode::NotFound` if the app doesn't exist,
/// or `ErrorCode::Internal` on storage/decoding failures (logged inside
/// `error_classify`).
pub(crate) fn load_app(
    state: &StateLayer<FileBackend>,
    org_id: OrganizationId,
    app_id: AppId,
) -> Result<App, WireError> {
    let key = SystemKeys::app_key(org_id, app_id);
    let entity = state
        .get_entity(SYSTEM_VAULT_ID, key.as_bytes())
        .map_err(|e| error_classify::storage_error(&e))?
        .ok_or_else(|| {
            build_wire_error(
                ErrorCode::NotFound,
                format!("App {} not found", app_id),
                "",
                false,
                0,
                BTreeMap::new(),
                "",
            )
        })?;
    decode::<App>(&entity.value).map_err(|e| error_classify::serialization_error(&e))
}

/// Computes a hash of operations for idempotency payload comparison.
///
/// Uses length-prefixed encoding of operation fields to create a
/// deterministic, collision-resistant byte sequence. Each variable-length
/// field is preceded by its `u32 LE` length to prevent boundary ambiguity
/// (e.g., `("ab","cd")` vs `("abc","d")` producing distinct byte sequences).
///
/// The condition encoding uses explicit per-variant tags so the byte output
/// is independent of the runtime `Debug` impl of the underlying types
/// (proto vs wire differ in how `Vec<u8>` / `Bytes` formats):
///
/// - `None`            → `b"NN"`
/// - `NotExists(b)`    → `b"NE" || u8(b)`
/// - `Version(v)`      → `b"VR" || v.to_le_bytes()`
/// - `ValueEquals(bs)` → `b"VE" || lp(bs)`
/// - `MustExists(b)`   → `b"ME" || u8(b)`
pub(crate) fn hash_operations(
    operations: &[inferadb_ledger_wire::services::shared::Operation],
) -> Vec<u8> {
    use inferadb_ledger_wire::services::shared::{OperationKind, SetConditionKind};

    fn push_lp(bytes: &mut Vec<u8>, data: &[u8]) {
        bytes.extend_from_slice(&(data.len() as u32).to_le_bytes());
        bytes.extend_from_slice(data);
    }

    fn push_condition(
        bytes: &mut Vec<u8>,
        condition: Option<&inferadb_ledger_wire::services::shared::SetCondition>,
    ) {
        match condition.and_then(|c| c.condition.as_ref()) {
            None => bytes.extend_from_slice(b"NN"),
            Some(SetConditionKind::NotExists(b)) => {
                bytes.extend_from_slice(b"NE");
                bytes.push(u8::from(*b));
            },
            Some(SetConditionKind::Version(v)) => {
                bytes.extend_from_slice(b"VR");
                bytes.extend_from_slice(&v.to_le_bytes());
            },
            Some(SetConditionKind::ValueEquals(bs)) => {
                bytes.extend_from_slice(b"VE");
                push_lp(bytes, bs.as_ref());
            },
            Some(SetConditionKind::MustExists(b)) => {
                bytes.extend_from_slice(b"ME");
                bytes.push(u8::from(*b));
            },
        }
    }

    let mut bytes = Vec::new();
    for op in operations {
        if let Some(inner) = &op.op {
            match inner {
                OperationKind::CreateRelationship(cr) => {
                    bytes.extend_from_slice(b"CR");
                    push_lp(&mut bytes, cr.resource.as_bytes());
                    push_lp(&mut bytes, cr.relation.as_bytes());
                    push_lp(&mut bytes, cr.subject.as_bytes());
                },
                OperationKind::DeleteRelationship(dr) => {
                    bytes.extend_from_slice(b"DR");
                    push_lp(&mut bytes, dr.resource.as_bytes());
                    push_lp(&mut bytes, dr.relation.as_bytes());
                    push_lp(&mut bytes, dr.subject.as_bytes());
                },
                OperationKind::SetEntity(se) => {
                    bytes.extend_from_slice(b"SE");
                    push_lp(&mut bytes, se.key.as_bytes());
                    push_lp(&mut bytes, se.value.as_ref());
                    push_condition(&mut bytes, se.condition.as_ref());
                    if let Some(exp) = se.expires_at {
                        bytes.extend_from_slice(&exp.to_le_bytes());
                    }
                },
                OperationKind::DeleteEntity(de) => {
                    bytes.extend_from_slice(b"DE");
                    push_lp(&mut bytes, de.key.as_bytes());
                },
                OperationKind::ExpireEntity(ee) => {
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
/// Returns `WireError` with `ErrorCode::Internal` if temp dir or database
/// creation fails.
pub(crate) fn create_replay_context() -> Result<(TempDir, StateLayer<FileBackend>), WireError> {
    let temp_dir = TempDir::new().map_err(|e| error_classify::storage_error(&e))?;
    let temp_meta_db = Arc::new(
        Database::<FileBackend>::create(temp_dir.path().join("replay_meta.db"))
            .map_err(|e| error_classify::storage_error(&e))?,
    );
    // P2b.0: the replay context owns no persistent state across calls,
    // so the factory opens a per-vault file under
    // `replay-state/vault-{id}/state.db` inside `temp_dir`. Every
    // replay gets a fresh temp dir, so opening by create vs. open
    // doesn't matter — the path never exists when the factory first
    // fires. The factory creates the per-vault directory lazily to
    // match the production factory in
    // `RaftManager::open_region_storage`.
    let state_dir = temp_dir.path().join("replay-state");
    std::fs::create_dir_all(&state_dir).map_err(|e| error_classify::storage_error(&e))?;
    let state_dir_for_factory = state_dir.clone();
    let factory = move |vault: inferadb_ledger_types::VaultId| {
        let vault_dir = state_dir_for_factory.join(format!("vault-{}", vault.value()));
        std::fs::create_dir_all(&vault_dir).map_err(|e| {
            inferadb_ledger_state::StateError::Store {
                source: inferadb_ledger_store::Error::Io { source: e },
                location: snafu::location!(),
            }
        })?;
        let path = vault_dir.join("state.db");
        let db = if path.exists() {
            Database::<FileBackend>::open(&path)
        } else {
            Database::<FileBackend>::create(&path)
        }
        .map_err(|e| inferadb_ledger_state::StateError::Store {
            source: e,
            location: snafu::location!(),
        })?;
        Ok(Arc::new(db))
    };
    let temp_state =
        StateLayer::new(factory, temp_meta_db).map_err(|e| error_classify::storage_error(&e))?;
    Ok((temp_dir, temp_state))
}

/// Maps a storage error to a `WireError` with `ErrorCode::Internal`.
///
/// Consolidates the repeated storage error mapping pattern used across read
/// service methods. Logs the full error (via `error_classify::storage_error`)
/// and returns a generic `WireError` carrying the `"storage error"` message.
pub(crate) fn storage_err(e: impl Display) -> WireError {
    error_classify::storage_error(&e)
}

/// Reads and decodes an [`AppVaultConnection`] from the system vault.
///
/// The caller provides the `not_found` `WireError` so that post-mutation
/// reads (which indicate a bug) can use `ErrorCode::Internal` while
/// user-facing lookups use `ErrorCode::NotFound`.
pub(crate) fn read_vault_connection(
    state: &StateLayer<FileBackend>,
    org_id: OrganizationId,
    app_id: AppId,
    vault_id: VaultId,
    not_found: WireError,
) -> Result<AppVaultConnection, WireError> {
    let key = SystemKeys::app_vault_key(org_id, app_id, vault_id);
    let entity = state
        .get_entity(SYSTEM_VAULT_ID, key.as_bytes())
        .map_err(|e| error_classify::storage_error(&e))?
        .ok_or(not_found)?;
    decode::<AppVaultConnection>(&entity.value).map_err(|e| error_classify::serialization_error(&e))
}

/// Maps a server-side [`inferadb_ledger_types::ErrorCode`] to a wire
/// [`WireError`] with the equivalent wire [`ErrorCode`].
///
/// Used by service-layer error handlers to convert structured state-machine
/// error codes into the correct wire error without string matching. The
/// types→wire mapping is provided by the
/// [`From<types::ErrorCode> for wire::ErrorCode`] impl in `crates/wire/src/error.rs`
/// (lockstep-checked by exhaustive match).
pub(crate) fn error_code_to_wire_error(
    code: inferadb_ledger_types::ErrorCode,
    message: String,
) -> WireError {
    let wire_code: ErrorCode = code.into();
    build_wire_error(wire_code, message, "", false, 0, BTreeMap::new(), "")
}

/// Ensures GLOBAL applied state on this node is at least as fresh as the
/// leader's committed index.
///
/// On follower nodes, data from recent GLOBAL proposals (vault creation, org
/// provisioning, slug registration) may not have replicated yet. This function
/// calls CommittedIndex on the GLOBAL leader to learn the leader's committed index,
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

    // Get the leader's actual committed index via CommittedIndex RPC.
    let leader_id = match global.handle().current_leader() {
        Some(id) => id,
        None => return,
    };
    let leader_addr = match manager.peer_addresses().get(leader_id) {
        Some(addr) => addr,
        None => return,
    };

    let committed_index = match read_index_from_leader(manager, leader_id, &leader_addr).await {
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

/// Calls CommittedIndex RPC on the GLOBAL leader to get its committed index.
///
/// The legacy tonic Raft transport was deleted in F.1.f.2.B; this function is
/// retained as a stub that always returns `None` so [`ensure_global_consistency`]
/// falls through to its local-commit-index fallback. Re-enabling the cross-node
/// committed-index probe requires the wire-transport sibling RPC.
async fn read_index_from_leader(
    _manager: &inferadb_ledger_raft::raft_manager::RaftManager,
    _leader_id: u64,
    _leader_addr: &str,
) -> Option<u64> {
    None
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods, clippy::panic)]
mod tests {
    use std::{net::SocketAddr, time::Instant};

    use bytes::Bytes;
    use inferadb_ledger_raft::graceful_shutdown::HealthState;
    use inferadb_ledger_types::{UserId, VaultId, config::ValidationConfig};
    use inferadb_ledger_wire::{
        AuthMethod, CallerIdentity, ConnectionMetrics, ErrorCode as WireErrorCode, RequestContext,
        services::shared as ws,
    };
    use tokio_util::sync::CancellationToken;

    use super::*;

    /// Build a minimal wire `RequestContext` for tests, populated with the
    /// given `user_id`. Mirrors the dispatcher-built context shape but stubs
    /// every observable field that the helpers under test do not read.
    fn make_test_wire_context_with_user(user_id: Option<UserId>) -> RequestContext {
        let peer_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        RequestContext {
            request_id: 0,
            idempotency_token: [0u8; 16],
            trace_id: [0u8; 16],
            span_id: [0u8; 8],
            trace_flags: 0,
            causality_commit_index: 0,
            deadline: None,
            caller_identity: CallerIdentity {
                user_id,
                app_id: None,
                organization_id: None,
                auth_method: AuthMethod::UserSession,
                token_expiry: Instant::now(),
                revocation_epoch: 0,
            },
            peer_addr,
            sdk_version: None,
            forwarded_for: None,
            correlation_id: 0,
            cancellation: CancellationToken::new(),
            conn_metrics: Arc::new(ConnectionMetrics),
        }
    }

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
        assert_eq!(result.unwrap_err().code, WireErrorCode::StaleRouting);
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
        assert_eq!(result.unwrap_err().code, WireErrorCode::StaleRouting);
    }

    #[test]
    fn check_not_draining_shutting_down_phase_rejects() {
        let hs = HealthState::new();
        hs.mark_shutting_down();
        let result = check_not_draining(Some(&hs));
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code, WireErrorCode::StaleRouting);
    }

    // =========================================================================
    // extract_caller_from_wire
    // =========================================================================

    #[test]
    fn extract_caller_from_wire_sets_user_id_when_present() {
        let mut ctx = inferadb_ledger_raft::logging::RequestContext::new("test", "method");
        let wire_ctx = make_test_wire_context_with_user(Some(UserId::new(99)));
        extract_caller_from_wire(&mut ctx, &wire_ctx);
        assert_eq!(ctx.caller_or_zero(), 99);
    }

    #[test]
    fn extract_caller_from_wire_noop_when_user_id_absent() {
        let mut ctx = inferadb_ledger_raft::logging::RequestContext::new("test", "method");
        let wire_ctx = make_test_wire_context_with_user(None);
        extract_caller_from_wire(&mut ctx, &wire_ctx);
        assert_eq!(ctx.caller_or_zero(), 0);
    }

    // =========================================================================
    // validation_wire_error
    // =========================================================================

    #[test]
    fn validation_wire_error_returns_invalid_argument() {
        let err = validation_wire_error("field too long");
        assert_eq!(err.code, WireErrorCode::InvalidArgument);
        assert_eq!(err.message, "field too long");
        // Diagnostic code lands in context["error_code"] (via build_wire_error)
        assert!(err.context.contains_key("error_code"));
    }

    #[test]
    fn validation_wire_error_carries_diagnostic_code_and_action() {
        let err = validation_wire_error("bad input");
        assert!(!err.retryable);
        assert!(!err.suggested_action.is_empty());
        assert_eq!(
            err.context.get("error_code").map(String::as_str),
            Some(
                inferadb_ledger_types::DiagnosticCode::AppInvalidArgument
                    .as_u16()
                    .to_string()
                    .as_str()
            )
        );
    }

    // =========================================================================
    // validate_operations (wire types)
    // =========================================================================

    fn wire_set_entity(
        key: &str,
        value: &[u8],
        condition: Option<ws::SetCondition>,
        expires_at: Option<u64>,
    ) -> ws::Operation {
        ws::Operation {
            op: Some(ws::OperationKind::SetEntity(ws::SetEntity {
                key: key.to_owned(),
                value: Bytes::copy_from_slice(value),
                expires_at,
                condition,
            })),
        }
    }

    fn wire_delete_entity(key: &str) -> ws::Operation {
        ws::Operation {
            op: Some(ws::OperationKind::DeleteEntity(ws::DeleteEntity { key: key.to_owned() })),
        }
    }

    fn wire_expire_entity(key: &str, expired_at: u64) -> ws::Operation {
        ws::Operation {
            op: Some(ws::OperationKind::ExpireEntity(ws::ExpireEntity {
                key: key.to_owned(),
                expired_at,
            })),
        }
    }

    fn wire_create_relationship(resource: &str, relation: &str, subject: &str) -> ws::Operation {
        ws::Operation {
            op: Some(ws::OperationKind::CreateRelationship(ws::CreateRelationship {
                resource: resource.to_owned(),
                relation: relation.to_owned(),
                subject: subject.to_owned(),
            })),
        }
    }

    fn wire_delete_relationship(resource: &str, relation: &str, subject: &str) -> ws::Operation {
        ws::Operation {
            op: Some(ws::OperationKind::DeleteRelationship(ws::DeleteRelationship {
                resource: resource.to_owned(),
                relation: relation.to_owned(),
                subject: subject.to_owned(),
            })),
        }
    }

    #[test]
    fn validate_operations_empty_list_rejected() {
        let config = ValidationConfig::default();
        let err = validate_operations(&[], &config).unwrap_err();
        assert_eq!(err.code, WireErrorCode::InvalidArgument);
    }

    #[test]
    fn validate_operations_valid_set_entity() {
        let config = ValidationConfig::default();
        let ops = vec![wire_set_entity("user:1", b"data", None, None)];
        assert!(validate_operations(&ops, &config).is_ok());
    }

    #[test]
    fn validate_operations_valid_delete_entity() {
        let config = ValidationConfig::default();
        let ops = vec![wire_delete_entity("user:1")];
        assert!(validate_operations(&ops, &config).is_ok());
    }

    #[test]
    fn validate_operations_valid_expire_entity() {
        let config = ValidationConfig::default();
        let ops = vec![wire_expire_entity("user:1", 1000)];
        assert!(validate_operations(&ops, &config).is_ok());
    }

    #[test]
    fn validate_operations_valid_create_relationship() {
        let config = ValidationConfig::default();
        let ops = vec![wire_create_relationship("doc:readme", "viewer", "user:alice")];
        assert!(validate_operations(&ops, &config).is_ok());
    }

    #[test]
    fn validate_operations_valid_delete_relationship() {
        let config = ValidationConfig::default();
        let ops = vec![wire_delete_relationship("doc:readme", "viewer", "user:alice")];
        assert!(validate_operations(&ops, &config).is_ok());
    }

    #[test]
    fn validate_operations_missing_op_field_rejects() {
        let config = ValidationConfig::default();
        let ops = vec![ws::Operation { op: None }];
        let err = validate_operations(&ops, &config).unwrap_err();
        assert_eq!(err.code, WireErrorCode::InvalidArgument);
        assert!(err.message.contains("missing op field"));
    }

    #[test]
    fn validate_operations_system_key_rejected() {
        let config = ValidationConfig::default();
        let ops = vec![wire_set_entity("_meta:seq", b"x", None, None)];
        let err = validate_operations(&ops, &config).unwrap_err();
        assert_eq!(err.code, WireErrorCode::InvalidArgument);
    }

    #[test]
    fn validate_operations_too_many_operations_rejected() {
        let config = ValidationConfig::builder().max_operations_per_write(2).build().unwrap();
        let ops: Vec<_> = (0..3).map(|i| wire_delete_entity(&format!("k{i}"))).collect();
        let err = validate_operations(&ops, &config).unwrap_err();
        assert_eq!(err.code, WireErrorCode::InvalidArgument);
    }

    #[test]
    fn validate_operations_oversized_payload_rejected() {
        let config = ValidationConfig::builder().max_batch_payload_bytes(10).build().unwrap();
        let ops = vec![wire_set_entity("k", &[0u8; 20], None, None)];
        let err = validate_operations(&ops, &config).unwrap_err();
        assert_eq!(err.code, WireErrorCode::InvalidArgument);
    }

    // =========================================================================
    // record_hot_keys (wire types)
    // =========================================================================

    #[test]
    fn record_hot_keys_none_detector_is_noop() {
        let ops = vec![wire_set_entity("k", &[], None, None)];
        // Should not panic with None detector
        record_hot_keys(None, VaultId::new(1), &ops);
    }

    #[test]
    fn record_hot_keys_with_detector_records_accesses() {
        let detector = Arc::new(inferadb_ledger_raft::hot_key_detector::HotKeyDetector::new(
            &inferadb_ledger_types::config::HotKeyConfig::default(),
        ));
        let ops = vec![
            wire_set_entity("entity:1", &[], None, None),
            wire_delete_entity("entity:2"),
            wire_create_relationship("doc:1", "viewer", "user:1"),
            wire_delete_relationship("doc:2", "editor", "user:2"),
            wire_expire_entity("entity:3", 1000),
        ];
        // Should not panic and should record all 5 keys
        record_hot_keys(Some(&detector), VaultId::new(1), &ops);
    }

    #[test]
    fn record_hot_keys_skips_none_op() {
        let detector = Arc::new(inferadb_ledger_raft::hot_key_detector::HotKeyDetector::new(
            &inferadb_ledger_types::config::HotKeyConfig::default(),
        ));
        let ops = vec![ws::Operation { op: None }];
        record_hot_keys(Some(&detector), VaultId::new(1), &ops);
    }

    // =========================================================================
    // hash_operations (wire types)
    // =========================================================================

    #[test]
    fn hash_operations_empty_returns_empty() {
        assert!(hash_operations(&[]).is_empty());
    }

    #[test]
    fn hash_operations_deterministic() {
        let ops = vec![wire_set_entity("k", b"v", None, None)];
        let h1 = hash_operations(&ops);
        let h2 = hash_operations(&ops);
        assert_eq!(h1, h2);
    }

    #[test]
    fn hash_operations_different_ops_differ() {
        let ops_a = vec![wire_set_entity("a", b"v", None, None)];
        let ops_b = vec![wire_set_entity("b", b"v", None, None)];
        assert_ne!(hash_operations(&ops_a), hash_operations(&ops_b));
    }

    #[test]
    fn hash_operations_length_prefix_prevents_boundary_collision() {
        // "ab" + "cd" vs "abc" + "d" should differ due to length prefixing
        let ops_a = vec![wire_create_relationship("ab", "cd", "ef")];
        let ops_b = vec![wire_create_relationship("abc", "d", "ef")];
        assert_ne!(hash_operations(&ops_a), hash_operations(&ops_b));
    }

    #[test]
    fn hash_operations_all_variants_produce_nonempty() {
        let ops = vec![
            wire_set_entity("k", b"v", None, Some(12345)),
            wire_delete_entity("k"),
            wire_expire_entity("k", 999),
            wire_create_relationship("r", "rel", "s"),
            wire_delete_relationship("r", "rel", "s"),
        ];
        let hash = hash_operations(&ops);
        assert!(!hash.is_empty());
    }

    #[test]
    fn hash_operations_set_with_condition_differs_from_without() {
        let ops_no_cond = vec![wire_set_entity("k", b"v", None, None)];
        let ops_with_cond = vec![wire_set_entity(
            "k",
            b"v",
            Some(ws::SetCondition { condition: Some(ws::SetConditionKind::NotExists(true)) }),
            None,
        )];
        assert_ne!(hash_operations(&ops_no_cond), hash_operations(&ops_with_cond));
    }

    #[test]
    fn hash_operations_skips_none_op() {
        let ops = vec![ws::Operation { op: None }];
        assert!(hash_operations(&ops).is_empty());
    }

    #[test]
    fn hash_operations_create_vs_delete_relationship_differ() {
        let ops_create = vec![wire_create_relationship("r", "rel", "s")];
        let ops_delete = vec![wire_delete_relationship("r", "rel", "s")];
        assert_ne!(hash_operations(&ops_create), hash_operations(&ops_delete));
    }

    // ---- Golden vector + condition variants pinning encoding ----

    fn lp(data: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(4 + data.len());
        out.extend_from_slice(&(data.len() as u32).to_le_bytes());
        out.extend_from_slice(data);
        out
    }

    #[test]
    fn hash_operations_golden_vector_wire_shape() {
        // One of each: CR, DR, SE-with-NotExists-cond-and-expires, DE, EE.
        let ops = vec![
            wire_create_relationship("doc:1", "viewer", "user:1"),
            wire_delete_relationship("doc:2", "editor", "user:2"),
            wire_set_entity(
                "k",
                b"v",
                Some(ws::SetCondition { condition: Some(ws::SetConditionKind::NotExists(true)) }),
                Some(0xCAFE),
            ),
            wire_delete_entity("dk"),
            wire_expire_entity("ek", 0x1234),
        ];

        let mut expected: Vec<u8> = Vec::new();
        // CR
        expected.extend_from_slice(b"CR");
        expected.extend_from_slice(&lp(b"doc:1"));
        expected.extend_from_slice(&lp(b"viewer"));
        expected.extend_from_slice(&lp(b"user:1"));
        // DR
        expected.extend_from_slice(b"DR");
        expected.extend_from_slice(&lp(b"doc:2"));
        expected.extend_from_slice(&lp(b"editor"));
        expected.extend_from_slice(&lp(b"user:2"));
        // SE
        expected.extend_from_slice(b"SE");
        expected.extend_from_slice(&lp(b"k"));
        expected.extend_from_slice(&lp(b"v"));
        // condition: NotExists(true) → "NE" || 0x01
        expected.extend_from_slice(b"NE");
        expected.push(1);
        // expires_at: u64 LE
        expected.extend_from_slice(&0xCAFE_u64.to_le_bytes());
        // DE
        expected.extend_from_slice(b"DE");
        expected.extend_from_slice(&lp(b"dk"));
        // EE
        expected.extend_from_slice(b"EE");
        expected.extend_from_slice(&lp(b"ek"));
        expected.extend_from_slice(&0x1234_u64.to_le_bytes());

        assert_eq!(hash_operations(&ops), expected);
    }

    #[test]
    fn hash_operations_condition_variants_distinguishable() {
        let mk = |cond: Option<ws::SetCondition>| {
            hash_operations(&[wire_set_entity("k", b"v", cond, None)])
        };

        let none_h = mk(None);
        let not_exists_true =
            mk(Some(ws::SetCondition { condition: Some(ws::SetConditionKind::NotExists(true)) }));
        let not_exists_false =
            mk(Some(ws::SetCondition { condition: Some(ws::SetConditionKind::NotExists(false)) }));
        let version = mk(Some(ws::SetCondition {
            condition: Some(ws::SetConditionKind::Version(0xDEAD_BEEF)),
        }));
        let value_equals = mk(Some(ws::SetCondition {
            condition: Some(ws::SetConditionKind::ValueEquals(Bytes::from_static(b"abc"))),
        }));
        let must_exists_true =
            mk(Some(ws::SetCondition { condition: Some(ws::SetConditionKind::MustExists(true)) }));

        let all = [
            ("None", &none_h),
            ("NotExists(true)", &not_exists_true),
            ("NotExists(false)", &not_exists_false),
            ("Version", &version),
            ("ValueEquals", &value_equals),
            ("MustExists(true)", &must_exists_true),
        ];
        for (i, (li, lh)) in all.iter().enumerate() {
            for (rj, (rj_label, rh)) in all.iter().enumerate() {
                if i != rj {
                    assert_ne!(lh, rh, "condition variants should differ: {li} vs {rj_label}");
                }
            }
        }
    }

    // =========================================================================
    // error_code_to_wire_error
    // =========================================================================

    #[test]
    fn error_code_not_found_maps_to_wire_not_found() {
        let err =
            error_code_to_wire_error(inferadb_ledger_types::ErrorCode::NotFound, "gone".into());
        assert_eq!(err.code, WireErrorCode::NotFound);
        assert_eq!(err.message, "gone");
    }

    #[test]
    fn error_code_already_exists_maps_to_wire_already_exists() {
        let err =
            error_code_to_wire_error(inferadb_ledger_types::ErrorCode::AlreadyExists, "dup".into());
        assert_eq!(err.code, WireErrorCode::AlreadyExists);
    }

    #[test]
    fn error_code_failed_precondition_maps_correctly() {
        let err = error_code_to_wire_error(
            inferadb_ledger_types::ErrorCode::FailedPrecondition,
            "pre".into(),
        );
        assert_eq!(err.code, WireErrorCode::FailedPrecondition);
    }

    #[test]
    fn error_code_permission_denied_maps_correctly() {
        let err = error_code_to_wire_error(
            inferadb_ledger_types::ErrorCode::PermissionDenied,
            "denied".into(),
        );
        assert_eq!(err.code, WireErrorCode::PermissionDenied);
    }

    #[test]
    fn error_code_invalid_argument_maps_correctly() {
        let err = error_code_to_wire_error(
            inferadb_ledger_types::ErrorCode::InvalidArgument,
            "bad".into(),
        );
        assert_eq!(err.code, WireErrorCode::InvalidArgument);
    }

    #[test]
    fn error_code_internal_maps_correctly() {
        let err =
            error_code_to_wire_error(inferadb_ledger_types::ErrorCode::Internal, "oops".into());
        assert_eq!(err.code, WireErrorCode::Internal);
    }

    #[test]
    fn error_code_unauthenticated_maps_correctly() {
        let err = error_code_to_wire_error(
            inferadb_ledger_types::ErrorCode::Unauthenticated,
            "no auth".into(),
        );
        assert_eq!(err.code, WireErrorCode::Unauthenticated);
    }

    #[test]
    fn error_code_rate_limited_maps_to_rate_limited() {
        let err = error_code_to_wire_error(
            inferadb_ledger_types::ErrorCode::RateLimited,
            "slow down".into(),
        );
        assert_eq!(err.code, WireErrorCode::RateLimited);
    }

    #[test]
    fn error_code_invitation_rate_limited_maps_to_invitation_rate_limited() {
        let err = error_code_to_wire_error(
            inferadb_ledger_types::ErrorCode::InvitationRateLimited,
            "too many".into(),
        );
        assert_eq!(err.code, WireErrorCode::InvitationRateLimited);
    }

    #[test]
    fn error_code_expired_maps_to_expired() {
        let err =
            error_code_to_wire_error(inferadb_ledger_types::ErrorCode::Expired, "expired".into());
        assert_eq!(err.code, WireErrorCode::Expired);
    }

    #[test]
    fn error_code_too_many_attempts_maps_to_too_many_attempts() {
        let err = error_code_to_wire_error(
            inferadb_ledger_types::ErrorCode::TooManyAttempts,
            "locked".into(),
        );
        assert_eq!(err.code, WireErrorCode::TooManyAttempts);
    }

    #[test]
    fn error_code_invitation_already_resolved_maps_correctly() {
        let err = error_code_to_wire_error(
            inferadb_ledger_types::ErrorCode::InvitationAlreadyResolved,
            "done".into(),
        );
        assert_eq!(err.code, WireErrorCode::InvitationAlreadyResolved);
    }

    #[test]
    fn error_code_invitation_email_mismatch_maps_correctly() {
        let err = error_code_to_wire_error(
            inferadb_ledger_types::ErrorCode::InvitationEmailMismatch,
            "mismatch".into(),
        );
        assert_eq!(err.code, WireErrorCode::InvitationEmailMismatch);
    }

    #[test]
    fn error_code_invitation_already_member_maps_correctly() {
        let err = error_code_to_wire_error(
            inferadb_ledger_types::ErrorCode::InvitationAlreadyMember,
            "member".into(),
        );
        assert_eq!(err.code, WireErrorCode::InvitationAlreadyMember);
    }

    #[test]
    fn error_code_invitation_duplicate_pending_maps_correctly() {
        let err = error_code_to_wire_error(
            inferadb_ledger_types::ErrorCode::InvitationDuplicatePending,
            "pending".into(),
        );
        assert_eq!(err.code, WireErrorCode::InvitationDuplicatePending);
    }

    #[test]
    fn error_code_stale_routing_maps_correctly() {
        let err = error_code_to_wire_error(
            inferadb_ledger_types::ErrorCode::StaleRouting,
            "stale".into(),
        );
        assert_eq!(err.code, WireErrorCode::StaleRouting);
    }

    // =========================================================================
    // storage_err
    // =========================================================================

    #[test]
    fn storage_err_returns_internal_wire_error() {
        let err = storage_err("disk failure");
        assert_eq!(err.code, WireErrorCode::Internal);
        assert_eq!(err.message, "storage error");
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
