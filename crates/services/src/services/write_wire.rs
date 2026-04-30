//! Wire-protocol shim for `WriteService` (Phase 0.0.E.4.3).
//!
//! Adds an [`inferadb_ledger_wire_services::WriteService`] impl alongside
//! the tonic impl on [`super::write::WriteService`]. The wire impl
//! converts wire types → proto types, delegates to the existing tonic
//! handler body, then converts the proto response → wire response. Both
//! impls coexist during migration; F.1 deletes the tonic impl and inlines
//! the body onto the wire trait directly.
//!
//! Conversions are mechanical field-by-field copies because the wire
//! types in [`inferadb_ledger_wire::services::write`] mirror the proto
//! layout exactly (E.2). Two minor shape differences resolved here:
//!
//! - `BlockHeader.timestamp` is `Option<prost_types::Timestamp>` in proto and `u64` (UNIX
//!   nanoseconds) on the wire — bridged by [`timestamp_proto_to_ns`] / [`ns_to_proto_timestamp`].
//! - Proto `Vec<u8>` for `idempotency_key` / `value` / `current_value` round-trips through `Bytes`
//!   on the wire.

use std::sync::Arc;

use bytes::Bytes;
use inferadb_ledger_raft::{
    event_writer::EventEmitter,
    idempotency::{IdempotencyCheckResult, IdempotencyKey, InFlightStatus},
    logging::{OperationType, RequestContext as LogRequestContext},
    metrics,
    types::LedgerResponse,
};
use inferadb_ledger_wire::{
    ErrorCode, RequestContext, WireError,
    services::{shared as ws, write as w},
};
use tracing::{debug, warn};
use uuid::Uuid;

use super::{
    region_resolver::ResolveResult, slug_resolver::SlugResolver,
    wire_helpers::tonic_status_to_wire_error, write::WriteService,
};

/// Numeric-tag mapping from proto `WriteErrorCode` (raw i32) to the wire enum.
///
/// Both enums share the same `Unspecified = 0`, `KeyExists = 1`,
/// `KeyNotFound = 2`, `VersionMismatch = 3`, `ValueMismatch = 4`,
/// `AlreadyCommitted = 5`, `IdempotencyKeyReused = 6` mapping. Unknown
/// tags collapse to `Unspecified` to match proto's open-enum semantics
/// — protects against future proto additions.
fn proto_write_error_code_to_wire(value: i32) -> w::WriteErrorCode {
    match value {
        x if x == w::WriteErrorCode::KeyExists as i32 => w::WriteErrorCode::KeyExists,
        x if x == w::WriteErrorCode::KeyNotFound as i32 => w::WriteErrorCode::KeyNotFound,
        x if x == w::WriteErrorCode::VersionMismatch as i32 => w::WriteErrorCode::VersionMismatch,
        x if x == w::WriteErrorCode::ValueMismatch as i32 => w::WriteErrorCode::ValueMismatch,
        x if x == w::WriteErrorCode::AlreadyCommitted as i32 => w::WriteErrorCode::AlreadyCommitted,
        x if x == w::WriteErrorCode::IdempotencyKeyReused as i32 => {
            w::WriteErrorCode::IdempotencyKeyReused
        },
        _ => w::WriteErrorCode::Unspecified,
    }
}

// ---------------------------------------------------------------------------
// Wire-trait implementation for `WriteService`.
//
// Inlined directly on the wire types — no proto round-trip, no UFCS
// dispatch through the tonic impl. The body mirrors the tonic
// `WriteService::write` in [`super::write`] one-for-one; both impls
// access the same `WriteService` private fields (made `pub(super)` for
// this purpose) and call into the same `SlugResolver` /
// `IdempotencyCache` / `ProposalService` primitives. The two paths
// share NO code.
//
// `SlugResolver::*`, `helpers::*` (rate-limit, drain check, validation),
// `error_classify::*`, and `ProposalService::propose_organization_request_to_vault`
// all return `WireError` directly (F.1.f.0). F.1.f.0-gap helpers
// (`RegionResolver::system_region` / `resolve_with_redirect`,
// `metadata::not_leader_status_from_handle` /
// `metadata::not_leader_remote_region`,
// `WriteService::check_not_migrating` /
// `WriteService::operations_to_request`) still return `tonic::Status`
// and are bridged via `tonic_status_to_wire_error`.
// ---------------------------------------------------------------------------

impl inferadb_ledger_wire_services::WriteService for WriteService {
    /// Mirrors the tonic [`write`](super::write::WriteService) handler:
    /// resolves the organization and vault slugs, runs migration / drain
    /// / leader / rate-limit / validation pre-flight, runs the
    /// in-flight + replicated idempotency dance, proposes the
    /// per-vault `OrganizationRequest::Write`, and returns the wire
    /// `WriteResponse` built from the resulting `LedgerResponse`.
    #[allow(clippy::too_many_lines)]
    async fn write(
        &self,
        request: w::WriteRequest,
        _ctx: RequestContext,
    ) -> Result<w::WriteResponse, WireError> {
        // Reject if node is draining. Helpers return WireError natively.
        super::helpers::check_not_draining(self.health_state.as_ref())?;

        // Build canonical-log-line context from the wire RequestContext.
        let event_handle: Option<Arc<dyn EventEmitter>> =
            self.event_handle.as_ref().map(|h| Arc::new(h.clone()) as _);
        let mut log_ctx =
            LogRequestContext::from_wire_context("WriteService", "write", &_ctx, event_handle);
        log_ctx.set_operation_type(OperationType::Write);
        if let Some(ref sampler) = self.sampler {
            log_ctx.set_sampler(sampler.clone());
        }
        if let Some(node_id) = self.node_id {
            log_ctx.set_node_id(node_id);
        }

        let req = request;

        // Extract client ID
        let client_id = req.client_id.as_ref().map(|c| c.id.clone()).unwrap_or_default();

        // Resolve organization slug → internal ID via system region.
        // F.1.f.0 gap: RegionResolver::system_region returns tonic::Status.
        let system = self.resolver.system_region().map_err(tonic_status_to_wire_error)?;
        let organization_id = SlugResolver::new(system.applied_state.clone())
            .extract_and_resolve(req.organization)?;

        // Set organization slug on context for event emission in early-exit paths.
        log_ctx.set_organization(req.organization.as_ref().map_or(0, |s| s.value()));

        // Reject writes to organizations undergoing migration.
        // F.1.f.0 gap: WriteService::check_not_migrating returns tonic::Status.
        self.check_not_migrating(&system, organization_id).map_err(tonic_status_to_wire_error)?;

        // Cross-region forwarding: pre-flight on this node, then redirect.
        // F.1.f.0 gap: RegionResolver::resolve_with_redirect returns tonic::Status.
        if self.resolver.supports_forwarding()
            && let ResolveResult::Redirect(remote) = self
                .resolver
                .resolve_with_redirect(organization_id)
                .map_err(tonic_status_to_wire_error)?
        {
            // Pre-flight: validation on originating node.
            if let Err(wire_err) = self.validate_operations_wire(&req.operations) {
                log_ctx.record_event(
                    inferadb_ledger_types::events::EventAction::RequestValidationFailed,
                    inferadb_ledger_types::events::EventOutcome::Denied {
                        reason: wire_err.message.clone(),
                    },
                    &[],
                );
                return Err(wire_err);
            }

            // Pre-flight: rate limit on originating node.
            if let Err(wire_err) = self.check_rate_limit_wire(&client_id, organization_id) {
                log_ctx.record_event(
                    inferadb_ledger_types::events::EventAction::RequestRateLimited,
                    inferadb_ledger_types::events::EventOutcome::Denied {
                        reason: "rate_limited".to_string(),
                    },
                    &[],
                );
                return Err(wire_err);
            }

            let source_region =
                self.manager.as_ref().map(|m| m.local_region().as_str()).unwrap_or("unknown");
            debug!(
                organization_id = organization_id.value(),
                target_region = remote.region.as_str(),
                source_region,
                "Redirecting write to remote region"
            );
            // F.1.f.0 gap: not_leader_remote_region returns tonic::Status.
            return Err(tonic_status_to_wire_error(super::metadata::not_leader_remote_region(
                &remote,
                "Organization hosted by a remote region; reconnect to that region",
            )));
        }

        // Ensure GLOBAL state is replicated before resolving vault slugs.
        super::helpers::ensure_global_consistency(self.manager.as_deref()).await;

        // Local processing: resolve vault slug via GLOBAL applied state.
        // F.1.f.0 gap: RegionResolver::resolve returns tonic::Status.
        let mut region =
            self.resolver.resolve(organization_id).map_err(tonic_status_to_wire_error)?;
        let vault_id =
            SlugResolver::new(system.applied_state.clone()).extract_and_resolve_vault(req.vault)?;

        // Per-vault apply records `ClientSequenceEntry` and vault height
        // updates in the per-vault Raft group's `AppliedState`; the
        // org-scoped accessor never sees those writes. Attach the per-vault
        // accessor when the vault group is live so the cross-failover
        // idempotency check below routes through it.
        if let Some(manager) = &self.manager
            && let Ok(vault_group) =
                manager.get_vault_group(region.region, organization_id, vault_id)
        {
            region.attach_vault_group(&vault_group);
        }

        // Reject on followers — clients use the NotLeader hint to retry
        // against the within-region leader directly.
        if !region.handle.is_leader() {
            // F.1.f.0 gap: not_leader_status_from_handle returns tonic::Status.
            return Err(tonic_status_to_wire_error(
                super::metadata::not_leader_status_from_handle(
                    region.handle.as_ref(),
                    self.peer_addresses.as_ref(),
                    "Not the leader for this region",
                    None,
                    None,
                    None,
                ),
            ));
        }

        // Extract caller identity for canonical log line. Wire request
        // already carries the caller `UserSlug` directly.
        if let Some(caller) = req.caller {
            log_ctx.set_caller(caller.value());
        }

        // Parse idempotency key (must be exactly 16 bytes for UUID).
        let idempotency_key: [u8; 16] = req.idempotency_key.as_ref().try_into().map_err(|_| {
            WireError::new(ErrorCode::InvalidArgument, "idempotency_key must be exactly 16 bytes")
        })?;

        // Validate all operations before any processing.
        if let Err(wire_err) = self.validate_operations_wire(&req.operations) {
            log_ctx.record_event(
                inferadb_ledger_types::events::EventAction::RequestValidationFailed,
                inferadb_ledger_types::events::EventOutcome::Denied {
                    reason: wire_err.message.clone(),
                },
                &[],
            );
            return Err(wire_err);
        }

        // Compute request hash for payload comparison (detects key reuse with different payload).
        let request_hash = seahash::hash(&super::helpers::hash_operations(&req.operations));

        // Populate logging context with request metadata.
        log_ctx.set_client_info(&client_id, 0);
        let organization_slug_u64 = req.organization.as_ref().map_or(0, |s| s.value());
        let vault_slug_u64 = req.vault.as_ref().map_or(0, |v| v.value());
        log_ctx.set_target(organization_slug_u64, vault_slug_u64);

        // Populate write operation fields.
        let operation_types = WriteService::extract_operation_types(&req.operations);
        log_ctx.set_write_operation(req.operations.len(), operation_types, req.include_tx_proof);
        log_ctx.set_bytes_written(WriteService::estimate_operations_bytes(&req.operations));

        // Serialize concurrent requests with the same idempotency key.
        // (See tonic impl for the rationale on why this guard precedes the
        // moka cache check during leader failover.)
        let _inflight_guard = loop {
            let inflight_key =
                IdempotencyKey::new(organization_id, vault_id, client_id.as_str(), idempotency_key);
            match self.idempotency.try_acquire_inflight(inflight_key) {
                InFlightStatus::Acquired(guard) => break guard,
                InFlightStatus::Waiting(notify) => {
                    let notified = notify.notified();
                    tokio::pin!(notified);
                    notified.as_mut().enable();

                    match self.idempotency.check(
                        organization_id,
                        vault_id,
                        &client_id,
                        idempotency_key,
                        request_hash,
                    ) {
                        IdempotencyCheckResult::Duplicate(cached) => {
                            log_ctx.set_idempotency_hit(true);
                            log_ctx.set_cached();
                            if let Some(ref header) = cached.block_header
                                && let Some(ref state_root) = header.state_root
                            {
                                log_ctx
                                    .set_state_root(&WriteService::bytes_to_hex(&state_root.value));
                            }
                            log_ctx.set_block_height(cached.block_height);
                            metrics::record_idempotency_operation("coalesced_fast");
                            metrics::record_idempotency_operation("hit");
                            return Ok(w::WriteResponse {
                                result: Some(w::WriteResponseResult::Success(cached)),
                            });
                        },
                        IdempotencyCheckResult::KeyReused => {
                            log_ctx.set_error(
                                "IdempotencyKeyReused",
                                "Idempotency key reused with different payload",
                            );
                            return Ok(w::WriteResponse {
                                result: Some(w::WriteResponseResult::Error(w::WriteError {
                                    code: w::WriteErrorCode::IdempotencyKeyReused,
                                    key: String::new(),
                                    current_version: None,
                                    current_value: None,
                                    message:
                                        "Idempotency key was already used with a different request payload"
                                            .to_string(),
                                    committed_tx_id: None,
                                    committed_block_height: None,
                                    assigned_sequence: None,
                                })),
                            });
                        },
                        IdempotencyCheckResult::NewRequest => {
                            // Acquirer is still in-flight (or crashed mid-insert).
                            notified.await;
                            metrics::record_idempotency_operation("coalesced");

                            match self.idempotency.check(
                                organization_id,
                                vault_id,
                                &client_id,
                                idempotency_key,
                                request_hash,
                            ) {
                                IdempotencyCheckResult::Duplicate(cached) => {
                                    log_ctx.set_idempotency_hit(true);
                                    log_ctx.set_cached();
                                    if let Some(ref header) = cached.block_header
                                        && let Some(ref state_root) = header.state_root
                                    {
                                        log_ctx.set_state_root(&WriteService::bytes_to_hex(
                                            &state_root.value,
                                        ));
                                    }
                                    log_ctx.set_block_height(cached.block_height);
                                    metrics::record_idempotency_operation("hit");
                                    return Ok(w::WriteResponse {
                                        result: Some(w::WriteResponseResult::Success(cached)),
                                    });
                                },
                                IdempotencyCheckResult::KeyReused => {
                                    log_ctx.set_error(
                                        "IdempotencyKeyReused",
                                        "Idempotency key reused with different payload",
                                    );
                                    return Ok(w::WriteResponse {
                                        result: Some(w::WriteResponseResult::Error(
                                            w::WriteError {
                                                code: w::WriteErrorCode::IdempotencyKeyReused,
                                                key: String::new(),
                                                current_version: None,
                                                current_value: None,
                                                message:
                                                    "Idempotency key was already used with a different request payload"
                                                        .to_string(),
                                                committed_tx_id: None,
                                                committed_block_height: None,
                                                assigned_sequence: None,
                                            },
                                        )),
                                    });
                                },
                                IdempotencyCheckResult::NewRequest => {
                                    // Acquirer released its guard without inserting (crash path).
                                    continue;
                                },
                            }
                        },
                    }
                },
            }
        };

        // Check idempotency cache for duplicate (fast path — moka hit).
        match self.idempotency.check(
            organization_id,
            vault_id,
            &client_id,
            idempotency_key,
            request_hash,
        ) {
            IdempotencyCheckResult::Duplicate(cached) => {
                log_ctx.set_idempotency_hit(true);
                log_ctx.set_cached();
                if let Some(ref header) = cached.block_header
                    && let Some(ref state_root) = header.state_root
                {
                    log_ctx.set_state_root(&WriteService::bytes_to_hex(&state_root.value));
                }
                log_ctx.set_block_height(cached.block_height);
                metrics::record_idempotency_operation("hit");
                return Ok(w::WriteResponse {
                    result: Some(w::WriteResponseResult::Success(cached)),
                });
            },
            IdempotencyCheckResult::KeyReused => {
                log_ctx.set_error(
                    "IdempotencyKeyReused",
                    "Idempotency key reused with different payload",
                );
                return Ok(w::WriteResponse {
                    result: Some(w::WriteResponseResult::Error(w::WriteError {
                        code: w::WriteErrorCode::IdempotencyKeyReused,
                        key: String::new(),
                        current_version: None,
                        current_value: None,
                        message:
                            "Idempotency key was already used with a different request payload"
                                .to_string(),
                        committed_tx_id: None,
                        committed_block_height: None,
                        assigned_sequence: None,
                    })),
                });
            },
            IdempotencyCheckResult::NewRequest => {
                // Moka miss — check replicated state for cross-failover dedup.
                log_ctx.set_idempotency_hit(false);
                {
                    use inferadb_ledger_raft::log_storage::IdempotencyCheckResult as ReplicatedCheck;
                    match region.client_idempotency_check(
                        organization_id,
                        vault_id,
                        &client_id,
                        &idempotency_key,
                        request_hash,
                    ) {
                        ReplicatedCheck::AlreadyCommitted { sequence } => {
                            metrics::record_idempotency_operation("hit");
                            return Ok(w::WriteResponse {
                                result: Some(w::WriteResponseResult::Error(w::WriteError {
                                    code: w::WriteErrorCode::AlreadyCommitted,
                                    key: String::new(),
                                    current_version: None,
                                    current_value: None,
                                    message: "Request already committed (cross-failover dedup)"
                                        .to_string(),
                                    committed_tx_id: None,
                                    committed_block_height: None,
                                    assigned_sequence: Some(sequence),
                                })),
                            });
                        },
                        ReplicatedCheck::KeyReused => {
                            log_ctx.set_error(
                                "IdempotencyKeyReused",
                                "Idempotency key reused with different payload (cross-failover)",
                            );
                            return Ok(w::WriteResponse {
                                result: Some(w::WriteResponseResult::Error(w::WriteError {
                                    code: w::WriteErrorCode::IdempotencyKeyReused,
                                    key: String::new(),
                                    current_version: None,
                                    current_value: None,
                                    message: "Idempotency key was already used with a different request payload".to_string(),
                                    committed_tx_id: None,
                                    committed_block_height: None,
                                    assigned_sequence: None,
                                })),
                            });
                        },
                        ReplicatedCheck::Miss => {},
                    }
                }
                metrics::record_idempotency_operation("miss");
            },
        }

        // Check rate limits (backpressure, organization, client).
        if let Err(wire_err) = self.check_rate_limit_wire(&client_id, organization_id) {
            log_ctx.set_rate_limited();
            log_ctx.record_event(
                inferadb_ledger_types::events::EventAction::RequestRateLimited,
                inferadb_ledger_types::events::EventOutcome::Denied {
                    reason: "rate_limited".to_string(),
                },
                &[],
            );
            return Err(wire_err);
        }

        // Track key access frequency for hot key detection.
        self.record_hot_keys(vault_id, &req.operations);

        // γ Phase 3a: capture the external slugs from the incoming request
        // so the apply handler can stamp them onto the emitted `VaultEntry`.
        let organization_slug_typed =
            inferadb_ledger_types::OrganizationSlug::new(organization_slug_u64);
        let vault_slug_typed = inferadb_ledger_types::VaultSlug::new(vault_slug_u64);

        let ledger_request = self.operations_to_request_wire(
            organization_id,
            Some(vault_id),
            &req.operations,
            &client_id,
            idempotency_key,
            request_hash,
            organization_slug_typed,
            vault_slug_typed,
        )?;

        // Re-validate vault slug before proposal submission. Between the
        // initial resolution and now, the GLOBAL state may have changed.
        let revalidated_vault_id =
            SlugResolver::new(system.applied_state.clone()).extract_and_resolve_vault(req.vault)?;
        if revalidated_vault_id != vault_id {
            warn!(
                vault_id = vault_id.value(),
                revalidated_vault_id = revalidated_vault_id.value(),
                "Vault routing changed during request processing"
            );
            return Err(super::helpers::error_code_to_wire_error(
                inferadb_ledger_types::ErrorCode::StaleRouting,
                "Stale routing: vault routing changed during request processing. Retry."
                    .to_string(),
            ));
        }

        // Compute effective timeout: min(proposal_timeout, wire_deadline).
        let timeout = match _ctx.deadline {
            Some(deadline) => {
                let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                self.proposal_timeout.min(remaining)
            },
            None => self.proposal_timeout,
        };

        // Route the proposal to the vault's per-vault Raft shard. The
        // typed `propose_organization_request_to_vault` helper handles
        // batch-writer-vs-direct-propose internally and returns
        // `WireError` directly.
        log_ctx.start_raft_timer();
        log_ctx.set_batch_info(false, 1);
        let organization_slug_hint = req.organization.as_ref().map(|s| s.value());
        let vault_slug_hint = req.vault.as_ref().map(|v| v.value());
        let response = match self
            .proposal_service
            .propose_organization_request_to_vault(
                region.region,
                organization_id,
                vault_id,
                organization_slug_hint,
                vault_slug_hint,
                ledger_request,
                log_ctx.caller_or_zero(),
                timeout,
            )
            .await
        {
            Ok(response) => response,
            Err(wire_err) => {
                log_ctx.end_raft_timer();
                if wire_err.code == ErrorCode::FailedPrecondition
                    && wire_err.message.contains("timed out")
                {
                    log_ctx.set_error("ProposalTimeout", "Raft proposal timed out");
                } else {
                    log_ctx.set_error("RaftError", &wire_err.message);
                }
                return Err(wire_err);
            },
        };
        log_ctx.end_raft_timer();

        match response {
            LedgerResponse::Write { block_height, block_hash, assigned_sequence } => {
                // Generate proof and block header if requested. The proof
                // module returns wire types directly; build the wire
                // `WriteSuccess` without a proto round-trip.
                let (block_header_wire, tx_proof_wire, _proof_error_reason) =
                    if req.include_tx_proof {
                        self.generate_write_proof(organization_id, vault_id, block_height)
                    } else {
                        (None, None, None)
                    };

                let success_wire = w::WriteSuccess {
                    tx_id: Some(ws::TxIdMessage {
                        id: Bytes::copy_from_slice(Uuid::new_v4().as_bytes()),
                    }),
                    block_height,
                    block_header: block_header_wire.clone(),
                    tx_proof: tx_proof_wire,
                    assigned_sequence,
                };

                // Cache the result for idempotency. The cache stores wire types.
                self.idempotency.insert(
                    organization_id,
                    vault_id,
                    client_id.clone(),
                    idempotency_key,
                    request_hash,
                    success_wire.clone(),
                );
                metrics::set_idempotency_cache_size(self.idempotency.len());

                // Set success outcome with block info.
                log_ctx.set_success();
                log_ctx.set_block_height(block_height);
                log_ctx.set_block_hash(&WriteService::bytes_to_hex(&block_hash));
                if let Some(ref header) = block_header_wire
                    && let Some(ref state_root) = header.state_root
                {
                    log_ctx.set_state_root(&WriteService::bytes_to_hex(&state_root.value));
                }

                let elapsed = log_ctx.elapsed_secs();
                metrics::record_organization_latency(organization_id, "write", elapsed);

                Ok(w::WriteResponse { result: Some(w::WriteResponseResult::Success(success_wire)) })
            },
            LedgerResponse::Error { code, message } => {
                log_ctx.set_error(code.grpc_code_name(), &message);

                Ok(w::WriteResponse {
                    result: Some(w::WriteResponseResult::Error(w::WriteError {
                        code: w::WriteErrorCode::Unspecified,
                        key: String::new(),
                        current_version: None,
                        current_value: None,
                        message,
                        committed_tx_id: None,
                        committed_block_height: None,
                        assigned_sequence: None,
                    })),
                })
            },
            LedgerResponse::PreconditionFailed {
                key,
                current_version,
                current_value,
                failed_condition,
            } => {
                let key_exists = current_version.is_some();
                let error_code = WriteService::map_condition_to_error_code(
                    failed_condition.as_ref(),
                    key_exists,
                );

                log_ctx.set_precondition_failed(Some(&key));

                Ok(w::WriteResponse {
                    result: Some(w::WriteResponseResult::Error(w::WriteError {
                        code: proto_write_error_code_to_wire(error_code as i32),
                        key,
                        current_version,
                        current_value: current_value.map(Bytes::from),
                        message: "Precondition failed".to_string(),
                        committed_tx_id: None,
                        committed_block_height: None,
                        assigned_sequence: None,
                    })),
                })
            },
            _ => {
                log_ctx.set_error("UnexpectedResponse", "Unexpected response type");
                Err(WireError::new(ErrorCode::Internal, "Unexpected response type"))
            },
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // Enum tag mappings.
    // -----------------------------------------------------------------------

    #[test]
    fn write_error_code_each_known_tag_maps_correctly() {
        for (tag, expected) in [
            (0, w::WriteErrorCode::Unspecified),
            (1, w::WriteErrorCode::KeyExists),
            (2, w::WriteErrorCode::KeyNotFound),
            (3, w::WriteErrorCode::VersionMismatch),
            (4, w::WriteErrorCode::ValueMismatch),
            (5, w::WriteErrorCode::AlreadyCommitted),
            (6, w::WriteErrorCode::IdempotencyKeyReused),
        ] {
            assert_eq!(proto_write_error_code_to_wire(tag), expected);
        }
    }

    #[test]
    fn write_error_code_unknown_tag_collapses_to_unspecified() {
        assert_eq!(proto_write_error_code_to_wire(99), w::WriteErrorCode::Unspecified);
    }

    // BlockHeader / MerkleProof / Direction round-trips are covered by `wire_shared::tests`.
    // Timestamp bridging is covered by `wire_shared::tests`.

    // End-to-end shim test (WriteService instantiation against a live
    // `ProposalService` / `RegionResolver` / `IdempotencyCache`) is deferred
    // to E.7 (TestCluster migration). The conversion helpers above are
    // unit-tested in isolation; tonic-status → WireError mapping is covered
    // by `wire_helpers::tests`.
}
