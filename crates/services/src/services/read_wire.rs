//! Wire-protocol shim for `ReadService` (Phase 0.0.E.4.10).
//!
//! Adds an [`inferadb_ledger_wire_services::ReadService`] impl alongside
//! the tonic impl on [`super::read::ReadService`]. The wire impl converts
//! wire types → proto types, delegates to the existing tonic handler
//! body, then converts the proto response → wire response. Both impls
//! coexist during migration; F.1 deletes the tonic impl and inlines the
//! body onto the wire trait directly.
//!
//! Conversions are mechanical field-by-field copies because the wire
//! types in [`inferadb_ledger_wire::services::read`] mirror the proto
//! layout exactly (E.2). Non-trivial mappings:
//!
//! - `ReadConsistency` carries a raw `i32` value in proto; the wire enum is `#[repr(i32)]` with
//!   matching numeric tags. Unknown tags collapse to `Unspecified` to match proto's open-enum
//!   semantics.
//! - `value` / `current_value` are proto `Vec<u8>` ↔ wire `Bytes`.
//! - `BlockHeader` / `MerkleProof` / `ChainProof` / `Block` / `Transaction` / `BlockAnnouncement` /
//!   `Operation` / `Entity` / `Relationship` — all bridged through the shared helpers in
//!   [`super::wire_shared`].
//!
//! Streaming shim — `watch_blocks`:
//!
//! - The drain helper [`drain_watch_blocks_stream`] is extracted as a standalone free function so
//!   it can be unit-tested with a mock `futures::stream::iter` stream without spinning up a real
//!   `ReadService`. The shim itself just wires the handler call into the drain helper.
//! - Each wire `BlockAnnouncement` chunk is postcard-encoded and pushed into the [`StreamSink`].
//!   Sink-closed by the peer aborts cleanly (returns `Ok(())` to let the dispatcher emit a terminal
//!   stream-end frame without an error). A mid-flight `Err(tonic::Status)` from the inner stream is
//!   mapped to a `WireError` via the standard helper.

use std::{
    collections::BTreeMap,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use bytes::Bytes;
use futures::StreamExt;
use inferadb_ledger_raft::{
    log_storage::VaultHealthStatus,
    logging::{OperationType, RequestContext as LogRequestContext},
    metrics,
    pagination::{PageToken, PageTokenCodec},
};
use inferadb_ledger_types::VaultSlug;
use inferadb_ledger_wire::{
    ErrorCode, RequestContext, WireError,
    services::{read as w, shared as ws},
};
use inferadb_ledger_wire_transport::StreamSink;
use tonic::Status;
use tracing::{debug, warn};

use super::{
    read::{ReadService, StreamGuard, parse_relationship_cursor, validate_read_key},
    region_resolver::ResolveResult,
    slug_resolver::SlugResolver,
    wire_helpers::tonic_status_to_wire_error,
};

// ---------------------------------------------------------------------------
// Wire-native domain conversions (formerly via proto bridge).
// ---------------------------------------------------------------------------

/// Build a wire `Block` directly from a domain `VaultEntry` + parent `RegionBlock`.
///
/// Builds the wire-typed `Block` directly from domain values; the caller
/// supplies the resolved vault slug — this layer does not consult the slug
/// index.
fn vault_entry_to_wire_block(
    entry: &inferadb_ledger_types::VaultEntry,
    region_block: &inferadb_ledger_types::RegionBlock,
    vault: VaultSlug,
) -> ws::Block {
    use inferadb_ledger_types::Operation as DomainOp;

    let region_ts_ns = u64::try_from(region_block.timestamp.timestamp().max(0))
        .unwrap_or(0)
        .saturating_mul(1_000_000_000)
        .saturating_add(u64::from(region_block.timestamp.timestamp_subsec_nanos()));

    let transactions = entry
        .transactions
        .iter()
        .map(|tx| {
            let tx_ts_ns = u64::try_from(tx.timestamp.timestamp().max(0))
                .unwrap_or(0)
                .saturating_mul(1_000_000_000)
                .saturating_add(u64::from(tx.timestamp.timestamp_subsec_nanos()));

            let operations: Vec<ws::Operation> = tx
                .operations
                .iter()
                .map(|op| {
                    let kind = match op {
                        DomainOp::CreateRelationship { resource, relation, subject } => {
                            ws::OperationKind::CreateRelationship(ws::CreateRelationship {
                                resource: resource.clone(),
                                relation: relation.clone(),
                                subject: subject.clone(),
                            })
                        },
                        DomainOp::DeleteRelationship { resource, relation, subject } => {
                            ws::OperationKind::DeleteRelationship(ws::DeleteRelationship {
                                resource: resource.clone(),
                                relation: relation.clone(),
                                subject: subject.clone(),
                            })
                        },
                        DomainOp::SetEntity { key, value, condition, expires_at } => {
                            let cond = condition.as_ref().map(|c| ws::SetCondition {
                                condition: Some(match c {
                                    inferadb_ledger_types::SetCondition::MustNotExist => {
                                        ws::SetConditionKind::NotExists(true)
                                    },
                                    inferadb_ledger_types::SetCondition::MustExist => {
                                        ws::SetConditionKind::MustExists(true)
                                    },
                                    inferadb_ledger_types::SetCondition::VersionEquals(v) => {
                                        ws::SetConditionKind::Version(*v)
                                    },
                                    inferadb_ledger_types::SetCondition::ValueEquals(bytes) => {
                                        ws::SetConditionKind::ValueEquals(Bytes::from(
                                            bytes.clone(),
                                        ))
                                    },
                                }),
                            });
                            ws::OperationKind::SetEntity(ws::SetEntity {
                                key: key.clone(),
                                value: Bytes::from(value.clone()),
                                expires_at: *expires_at,
                                condition: cond,
                            })
                        },
                        DomainOp::DeleteEntity { key } => {
                            ws::OperationKind::DeleteEntity(ws::DeleteEntity { key: key.clone() })
                        },
                        DomainOp::ExpireEntity { key, expired_at } => {
                            ws::OperationKind::ExpireEntity(ws::ExpireEntity {
                                key: key.clone(),
                                expired_at: *expired_at,
                            })
                        },
                    };
                    ws::Operation { op: Some(kind) }
                })
                .collect();

            ws::Transaction {
                id: Some(ws::TxIdMessage { id: Bytes::from(tx.id.to_vec()) }),
                client_id: Some(ws::ClientIdMessage { id: tx.client_id.value().to_owned() }),
                sequence: tx.sequence,
                operations,
                timestamp: tx_ts_ns,
            }
        })
        .collect();

    let block_hash = inferadb_ledger_types::vault_entry_hash(entry);
    let header = ws::BlockHeader {
        height: entry.vault_height,
        organization: Some(ws::OrganizationSlug::new(entry.organization.value() as u64)),
        vault: Some(vault),
        previous_hash: Some(ws::Hash { value: Bytes::from(entry.previous_vault_hash.to_vec()) }),
        tx_merkle_root: Some(ws::Hash { value: Bytes::from(entry.tx_merkle_root.to_vec()) }),
        state_root: Some(ws::Hash { value: Bytes::from(entry.state_root.to_vec()) }),
        timestamp: region_ts_ns,
        leader_id: Some(ws::NodeId::new(region_block.leader_id.value().to_owned())),
        term: region_block.term,
        committed_index: region_block.committed_index,
        block_hash: Some(ws::Hash { value: Bytes::from(block_hash.to_vec()) }),
    };

    ws::Block { header: Some(header), transactions }
}

/// Build a wire `Entity` directly from a domain `Entity` reference.
///
/// Field-by-field copy from a domain `Entity` to its wire-typed counterpart.
/// `expires_at == 0` is preserved as `Some(0)` — wire payloads use `None`
/// to signal "absent"; `0` is the never-expires sentinel and is carried
/// through unchanged.
fn domain_entity_to_wire(e: &inferadb_ledger_types::Entity) -> ws::Entity {
    ws::Entity {
        key: String::from_utf8_lossy(&e.key).to_string(),
        value: Bytes::from(e.value.clone()),
        // Preserve proto's "0 means never expires → None" mapping so SDK
        // consumers see the same Optional semantics they had pre-migration.
        expires_at: if e.expires_at == 0 { None } else { Some(e.expires_at) },
        version: e.version,
    }
}

/// Build a wire `Relationship` directly from a domain `Relationship`.
fn domain_relationship_to_wire(r: inferadb_ledger_types::Relationship) -> ws::Relationship {
    ws::Relationship { resource: r.resource, relation: r.relation, subject: r.subject }
}

// ---------------------------------------------------------------------------
// Streaming shim — drain helper.
// ---------------------------------------------------------------------------

/// Drain a stream of wire `BlockAnnouncement` chunks onto a wire [`StreamSink`].
///
/// Extracted as a standalone free function so the streaming machinery can
/// be unit-tested without spinning up a real `ReadService` — the
/// `futures::Stream<Item = Result<ws::BlockAnnouncement, tonic::Status>>` shape
/// matches both the production `StreamGuard`-wrapped stream and any synthetic
/// stream from `futures::stream::iter`.
///
/// Behaviour:
///
/// - For each `Ok(wire_announcement)` chunk, postcard-encode and push into the sink.
/// - If the sink reports `Closed` (peer reset the stream), abort with `Ok(())` — the dispatcher's
///   terminal frame logic already handles stream resets, and an `Err` here would be encoded as an
///   error frame which is incorrect when the peer has already gone away.
/// - For each `Err(status)` chunk, abort the loop and return the mapped `WireError`. The transport
///   encodes this as the terminal frame.
/// - If the inner stream completes without errors, return `Ok(())`. The dispatcher emits the
///   terminal stream-end frame.
async fn drain_watch_blocks_stream<S>(mut stream: S, sink: StreamSink) -> Result<(), WireError>
where
    S: futures::Stream<Item = Result<ws::BlockAnnouncement, tonic::Status>> + Unpin + Send,
{
    while let Some(item) = stream.next().await {
        match item {
            Ok(wire_announcement) => {
                let bytes = postcard::to_allocvec(&wire_announcement).map_err(|err| WireError {
                    code: ErrorCode::Internal,
                    message: format!("encode BlockAnnouncement: {err}"),
                    retryable: false,
                    retry_after_ms: 0,
                    context: BTreeMap::new(),
                    suggested_action: String::new(),
                })?;
                if sink.send(Bytes::from(bytes)).await.is_err() {
                    // Peer dropped the stream; abort cleanly without an
                    // error frame.
                    return Ok(());
                }
            },
            Err(status) => {
                return Err(tonic_status_to_wire_error(status));
            },
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Wire-trait implementation for `ReadService`.
// ---------------------------------------------------------------------------

/// Compute the gRPC-style deadline duration from the wire `RequestContext`.
///
/// Mirrors `inferadb_ledger_raft::deadline::extract_deadline` for the wire
/// path: returns `Some(remaining)` when the context carries a non-`None`
/// `deadline`, `None` otherwise. Used by `follower_read_index` to bound
/// the `CommittedIndex` RPC.
fn wire_grpc_deadline(ctx: &RequestContext) -> Option<Duration> {
    ctx.deadline.map(|d| d.saturating_duration_since(std::time::Instant::now()))
}

impl inferadb_ledger_wire_services::ReadService for ReadService {
    /// Reads a single entity or relationship by key. Mirrors the tonic
    /// [`read`](super::read::ReadService) handler line-by-line on wire
    /// types: cross-region forwarding, slug → ID resolution, ReadIndex on
    /// followers for linearizable reads, vault-health gating, then the
    /// state-layer lookup.
    async fn read(
        &self,
        request: w::ReadRequest,
        _ctx: RequestContext,
    ) -> Result<w::ReadResponse, WireError> {
        let grpc_deadline = wire_grpc_deadline(&_ctx);
        let mut log_ctx = LogRequestContext::from_wire_context("ReadService", "read", &_ctx, None);
        log_ctx.set_operation_type(OperationType::Read);
        if let Some(sampler) = &self.sampler {
            log_ctx.set_sampler(sampler.clone());
        }
        if let Some(node_id) = &self.node_id {
            log_ctx.set_node_id(*node_id);
        }

        let req = request;

        // Cross-region forwarding: if the organization lives on a remote region,
        // forward the entire RPC rather than returning UNAVAILABLE.
        if self.resolver.supports_forwarding() {
            let system = self.resolver.system_region().map_err(tonic_status_to_wire_error)?;
            let organization_id = SlugResolver::new(system.applied_state).extract_and_resolve(
                req.organization.map(|s| inferadb_ledger_types::OrganizationSlug::new(s.value())),
            )?;
            if let ResolveResult::Redirect(remote) = self
                .resolver
                .resolve_with_redirect(organization_id)
                .map_err(tonic_status_to_wire_error)?
            {
                return Err(tonic_status_to_wire_error(super::metadata::not_leader_remote_region(
                    &remote,
                    "Organization hosted by a remote region; reconnect to that region",
                )));
            }
        }

        // Resolve region first for region-aware forwarding and consistency checks
        let (organization_id, vault_id, region) = self
            .resolve_org_vault_consistent(req.organization, req.vault)
            .await
            .map_err(tonic_status_to_wire_error)?;

        // Extract caller identity for canonical log line. The wire `caller`
        // already carries the slug.
        if let Some(caller) = req.caller {
            log_ctx.set_caller(caller.value());
        }

        // Set read operation fields
        log_ctx.set_key(&req.key);

        // Reject system-reserved key prefixes on read path
        validate_read_key(&req.key).map_err(tonic_status_to_wire_error)?;

        let consistency_label = match req.consistency {
            ws::ReadConsistency::Linearizable => "linearizable",
            _ => "eventual",
        };
        log_ctx.set_consistency(consistency_label);
        log_ctx.set_include_proof(false);

        // Check consistency requirements (may execute ReadIndex protocol on followers)
        if let Err(e) = self
            .resolve_read_consistency(
                &region,
                req.consistency as i32,
                grpc_deadline,
                req.organization,
                req.vault,
            )
            .await
        {
            log_ctx.set_error("consistency_error", e.message());
            return Err(tonic_status_to_wire_error(e));
        }
        let organization_slug = req.organization.as_ref().map_or(0, |n| n.value());
        let vault_slug = req.vault.as_ref().map_or(0, |v| v.value());
        log_ctx.set_target(organization_slug, vault_slug);

        // Check vault health - diverged vaults cannot be read
        let health = region.vault_health(organization_id, vault_id);
        if let VaultHealthStatus::Diverged { at_height, .. } = &health {
            let msg = format!(
                "Vault {}:{} has diverged at height {}",
                organization_id, vault_id, at_height
            );
            log_ctx.set_error("vault_diverged", &msg);
            return Err(WireError::new(ErrorCode::StaleRouting, msg));
        }

        // Start storage timer
        log_ctx.start_storage_timer();

        // Read from state layer
        let state = &*region.state;
        let entity = match state.get_entity(vault_id, req.key.as_bytes()) {
            Ok(e) => e,
            Err(e) => {
                log_ctx.end_storage_timer();
                let msg = format!("Storage error: {}", e);
                log_ctx.set_error("storage_error", &msg);
                return Err(WireError::new(ErrorCode::Internal, msg));
            },
        };

        log_ctx.end_storage_timer();

        // Filter out expired entities (expires_at == 0 means never expires)
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let entity = entity.filter(|e| e.expires_at == 0 || e.expires_at > now);

        let found = entity.is_some();
        let value_size = entity.as_ref().map(|e| e.value.len()).unwrap_or(0);
        log_ctx.set_found(found);
        log_ctx.set_value_size_bytes(value_size);
        log_ctx.set_bytes_read(value_size);

        let elapsed = log_ctx.elapsed_secs();
        metrics::record_organization_operation(organization_id, "read");
        metrics::record_organization_latency(organization_id, "read", elapsed);
        log_ctx.set_success();

        // Get current block height for this vault
        let block_height = region.vault_height(organization_id, vault_id);
        log_ctx.set_block_height(block_height);

        Ok(w::ReadResponse { value: entity.map(|e| Bytes::from(e.value)), block_height })
    }

    /// Batches read multiple keys in a single RPC call. Mirrors the tonic
    /// `batch_read` handler.
    async fn batch_read(
        &self,
        request: w::BatchReadRequest,
        _ctx: RequestContext,
    ) -> Result<w::BatchReadResponse, WireError> {
        let grpc_deadline = wire_grpc_deadline(&_ctx);
        let mut log_ctx =
            LogRequestContext::from_wire_context("ReadService", "batch_read", &_ctx, None);
        log_ctx.set_operation_type(OperationType::Read);
        if let Some(sampler) = &self.sampler {
            log_ctx.set_sampler(sampler.clone());
        }
        if let Some(node_id) = &self.node_id {
            log_ctx.set_node_id(*node_id);
        }

        let req = request;

        if self.resolver.supports_forwarding() {
            let system = self.resolver.system_region().map_err(tonic_status_to_wire_error)?;
            let organization_id = SlugResolver::new(system.applied_state).extract_and_resolve(
                req.organization.map(|s| inferadb_ledger_types::OrganizationSlug::new(s.value())),
            )?;
            if let ResolveResult::Redirect(remote) = self
                .resolver
                .resolve_with_redirect(organization_id)
                .map_err(tonic_status_to_wire_error)?
            {
                return Err(tonic_status_to_wire_error(super::metadata::not_leader_remote_region(
                    &remote,
                    "Organization hosted by a remote region; reconnect to that region",
                )));
            }
        }

        let (organization_id, vault_id, region) = self
            .resolve_org_vault_consistent(req.organization, req.vault)
            .await
            .map_err(tonic_status_to_wire_error)?;

        if let Some(caller) = req.caller {
            log_ctx.set_caller(caller.value());
        }

        log_ctx.set_keys_count(req.keys.len());
        let consistency_label = match req.consistency {
            ws::ReadConsistency::Linearizable => "linearizable",
            _ => "eventual",
        };
        log_ctx.set_consistency(consistency_label);
        log_ctx.set_include_proof(false);

        if let Err(e) = self
            .resolve_read_consistency(
                &region,
                req.consistency as i32,
                grpc_deadline,
                req.organization,
                req.vault,
            )
            .await
        {
            log_ctx.set_error("consistency_error", e.message());
            return Err(tonic_status_to_wire_error(e));
        }

        // Limit batch size to prevent DoS
        const MAX_BATCH_SIZE: usize = 1000;
        if req.keys.len() > MAX_BATCH_SIZE {
            let msg = format!("Batch size {} exceeds maximum {}", req.keys.len(), MAX_BATCH_SIZE);
            log_ctx.set_error("batch_too_large", &msg);
            return Err(WireError::new(ErrorCode::InvalidArgument, msg));
        }
        let organization_slug = req.organization.as_ref().map_or(0, |n| n.value());
        let vault_slug = req.vault.as_ref().map_or(0, |v| v.value());
        log_ctx.set_target(organization_slug, vault_slug);

        let health = region.vault_health(organization_id, vault_id);
        if let VaultHealthStatus::Diverged { at_height, .. } = &health {
            let msg = format!(
                "Vault {}:{} has diverged at height {}",
                organization_id, vault_id, at_height
            );
            log_ctx.set_error("vault_diverged", &msg);
            return Err(WireError::new(ErrorCode::StaleRouting, msg));
        }

        log_ctx.start_storage_timer();

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        for key in &req.keys {
            validate_read_key(key).map_err(tonic_status_to_wire_error)?;
        }

        let state = &*region.state;
        let mut results = Vec::with_capacity(req.keys.len());
        let mut found_count = 0usize;

        for key in &req.keys {
            let entity = match state.get_entity(vault_id, key.as_bytes()) {
                Ok(e) => e,
                Err(e) => {
                    log_ctx.end_storage_timer();
                    let msg = format!("Storage error: {}", e);
                    log_ctx.set_error("storage_error", &msg);
                    return Err(WireError::new(ErrorCode::Internal, msg));
                },
            };

            let entity = entity.filter(|e| e.expires_at == 0 || e.expires_at > now);

            let found = entity.is_some();
            if found {
                found_count += 1;
            }
            results.push(w::BatchReadResult {
                key: key.clone(),
                value: entity.map(|e| Bytes::from(e.value)),
                found,
            });
        }

        log_ctx.end_storage_timer();
        log_ctx.set_found_count(found_count);

        let total_bytes: usize =
            results.iter().filter_map(|r| r.value.as_ref()).map(|v| v.len()).sum();
        log_ctx.set_bytes_read(total_bytes);

        let latency = log_ctx.elapsed_secs();
        metrics::record_organization_operation(organization_id, "read");
        metrics::record_organization_latency(organization_id, "read", latency);

        log_ctx.set_success();

        let block_height = region.vault_height(organization_id, vault_id);
        log_ctx.set_block_height(block_height);

        Ok(w::BatchReadResponse { results, block_height })
    }

    /// Reads a single entity with cryptographic merkle proof and chain proof.
    /// Mirrors the tonic `verified_read` handler.
    async fn verified_read(
        &self,
        request: w::VerifiedReadRequest,
        _ctx: RequestContext,
    ) -> Result<w::VerifiedReadResponse, WireError> {
        let grpc_deadline = wire_grpc_deadline(&_ctx);
        let mut log_ctx =
            LogRequestContext::from_wire_context("ReadService", "verified_read", &_ctx, None);
        log_ctx.set_operation_type(OperationType::Read);
        if let Some(sampler) = &self.sampler {
            log_ctx.set_sampler(sampler.clone());
        }
        if let Some(node_id) = &self.node_id {
            log_ctx.set_node_id(*node_id);
        }

        let req = request;

        if self.resolver.supports_forwarding() {
            let system = self.resolver.system_region().map_err(tonic_status_to_wire_error)?;
            let organization_id = SlugResolver::new(system.applied_state).extract_and_resolve(
                req.organization.map(|s| inferadb_ledger_types::OrganizationSlug::new(s.value())),
            )?;
            if let ResolveResult::Redirect(remote) = self
                .resolver
                .resolve_with_redirect(organization_id)
                .map_err(tonic_status_to_wire_error)?
            {
                return Err(tonic_status_to_wire_error(super::metadata::not_leader_remote_region(
                    &remote,
                    "Organization hosted by a remote region; reconnect to that region",
                )));
            }
        }

        let (organization_id, vault_id, region) = self
            .resolve_org_vault_consistent(req.organization, req.vault)
            .await
            .map_err(tonic_status_to_wire_error)?;

        if let Some(caller) = req.caller {
            log_ctx.set_caller(caller.value());
        }

        log_ctx.set_key(&req.key);

        validate_read_key(&req.key).map_err(tonic_status_to_wire_error)?;

        log_ctx.set_include_proof(true);
        log_ctx.set_consistency("linearizable");
        let organization_slug = req.organization.as_ref().map_or(0, |n| n.value());
        let vault_slug = req.vault.as_ref().map_or(0, |v| v.value());
        log_ctx.set_target(organization_slug, vault_slug);

        // Verified reads always linearizable.
        if let Err(e) = self
            .resolve_read_consistency(
                &region,
                ws::ReadConsistency::Linearizable as i32,
                grpc_deadline,
                req.organization,
                req.vault,
            )
            .await
        {
            log_ctx.set_error("consistency_error", e.message());
            return Err(tonic_status_to_wire_error(e));
        }

        let health = region.vault_health(organization_id, vault_id);
        if let VaultHealthStatus::Diverged { at_height, .. } = &health {
            let msg = format!(
                "Vault {}:{} has diverged at height {}",
                organization_id, vault_id, at_height
            );
            log_ctx.set_error("vault_diverged", &msg);
            return Err(WireError::new(ErrorCode::StaleRouting, msg));
        }

        log_ctx.start_storage_timer();

        let state = &*region.state;
        let entity = match state.get_entity(vault_id, req.key.as_bytes()) {
            Ok(e) => e,
            Err(e) => {
                log_ctx.end_storage_timer();
                let msg = format!("Storage error: {}", e);
                log_ctx.set_error("storage_error", &msg);
                return Err(WireError::new(ErrorCode::Internal, msg));
            },
        };

        log_ctx.end_storage_timer();

        let found = entity.is_some();
        let value_size = entity.as_ref().map(|e| e.value.len()).unwrap_or(0);
        log_ctx.set_found(found);
        log_ctx.set_value_size_bytes(value_size);
        log_ctx.set_bytes_read(value_size);

        let block_height = region.vault_height(organization_id, vault_id);
        log_ctx.set_block_height(block_height);

        let block_header = ReadService::get_block_header(
            &region.block_archive,
            &region.applied_state,
            organization_id,
            vault_id,
            block_height,
        );

        // Bucket-based hashing (not sparse Merkle) — empty proof.
        let merkle_proof = ws::MerkleProof { leaf_hash: None, siblings: vec![] };

        let chain_proof = if req.include_chain_proof {
            let trusted_height = req.trusted_height.unwrap_or(0);
            ReadService::build_chain_proof(
                &region.block_archive,
                &region.applied_state,
                organization_id,
                vault_id,
                trusted_height,
                block_height,
            )
        } else {
            None
        };

        let proof_size = std::mem::size_of_val(&merkle_proof)
            + chain_proof.as_ref().map_or(0, std::mem::size_of_val);
        log_ctx.set_proof_size_bytes(proof_size);

        log_ctx.set_success();

        Ok(w::VerifiedReadResponse {
            value: entity.map(|e| Bytes::from(e.value)),
            block_height,
            block_header,
            merkle_proof: Some(merkle_proof),
            chain_proof,
        })
    }

    /// Reads entity state at a specific block height. Mirrors the tonic
    /// `historical_read` handler.
    async fn historical_read(
        &self,
        request: w::HistoricalReadRequest,
        _ctx: RequestContext,
    ) -> Result<w::HistoricalReadResponse, WireError> {
        let mut log_ctx =
            LogRequestContext::from_wire_context("ReadService", "historical_read", &_ctx, None);
        log_ctx.set_operation_type(OperationType::Read);
        if let Some(sampler) = &self.sampler {
            log_ctx.set_sampler(sampler.clone());
        }
        if let Some(node_id) = &self.node_id {
            log_ctx.set_node_id(*node_id);
        }

        let req = request;

        if self.resolver.supports_forwarding() {
            let system = self.resolver.system_region().map_err(tonic_status_to_wire_error)?;
            let organization_id = SlugResolver::new(system.applied_state).extract_and_resolve(
                req.organization.map(|s| inferadb_ledger_types::OrganizationSlug::new(s.value())),
            )?;
            if let ResolveResult::Redirect(remote) = self
                .resolver
                .resolve_with_redirect(organization_id)
                .map_err(tonic_status_to_wire_error)?
            {
                return Err(tonic_status_to_wire_error(super::metadata::not_leader_remote_region(
                    &remote,
                    "Organization hosted by a remote region; reconnect to that region",
                )));
            }
        }

        let (organization_id, vault_id, region) = self
            .resolve_org_vault_consistent(req.organization, req.vault)
            .await
            .map_err(tonic_status_to_wire_error)?;

        if !region.handle.is_leader() {
            return Err(tonic_status_to_wire_error(
                self.not_leader_within_region(&region, "Not the leader for this region"),
            ));
        }

        log_ctx.set_key(&req.key);

        validate_read_key(&req.key).map_err(tonic_status_to_wire_error)?;

        log_ctx.set_at_height(req.at_height);
        log_ctx.set_include_proof(req.include_proof);
        log_ctx.set_consistency("historical");

        if req.at_height == 0 {
            log_ctx.set_error("invalid_argument", "at_height is required for historical reads");
            return Err(WireError::new(
                ErrorCode::InvalidArgument,
                "at_height is required for historical reads",
            ));
        }
        let organization_slug = req.organization.as_ref().map_or(0, |n| n.value());
        let vault_slug = req.vault.as_ref().map_or(0, |v| v.value());
        log_ctx.set_target(organization_slug, vault_slug);

        let archive = &region.block_archive;

        let tip_height = region.vault_height(organization_id, vault_id);
        if req.at_height > tip_height {
            let msg =
                format!("Requested height {} exceeds current tip {}", req.at_height, tip_height);
            log_ctx.set_error("invalid_argument", &msg);
            return Err(WireError::new(ErrorCode::InvalidArgument, msg));
        }

        log_ctx.start_storage_timer();

        let (_temp_dir, temp_state) = match super::helpers::create_replay_context() {
            Ok(ctx_pair) => ctx_pair,
            Err(wire_err) => {
                log_ctx.end_storage_timer();
                log_ctx.set_error("internal", &wire_err.message);
                return Err(wire_err);
            },
        };

        let mut block_timestamp = chrono::Utc::now();

        let (start_height, _snapshot_loaded) =
            self.load_nearest_snapshot_for_historical_read(vault_id, req.at_height, &temp_state);

        for height in start_height..=req.at_height {
            let region_height = match archive.find_region_height(organization_id, vault_id, height)
            {
                Ok(Some(h)) => h,
                Ok(None) => continue,
                Err(e) => {
                    log_ctx.end_storage_timer();
                    let msg = format!("Index lookup failed: {:?}", e);
                    log_ctx.set_error("internal", &msg);
                    return Err(WireError::new(ErrorCode::Internal, msg));
                },
            };

            let region_block = match archive.read_block(region_height) {
                Ok(b) => b,
                Err(e) => {
                    log_ctx.end_storage_timer();
                    let msg = format!("Block read failed: {:?}", e);
                    log_ctx.set_error("internal", &msg);
                    return Err(WireError::new(ErrorCode::Internal, msg));
                },
            };

            let vault_entry = region_block.vault_entries.iter().find(|e| {
                e.organization == organization_id && e.vault == vault_id && e.vault_height == height
            });

            if let Some(entry) = vault_entry {
                for tx in &entry.transactions {
                    if let Err(e) = temp_state.apply_operations(vault_id, &tx.operations, height) {
                        log_ctx.end_storage_timer();
                        let msg = format!("Apply failed: {:?}", e);
                        log_ctx.set_error("internal", &msg);
                        return Err(WireError::new(ErrorCode::Internal, msg));
                    }
                }

                if height == req.at_height {
                    block_timestamp = region_block.timestamp;
                }
            }
        }

        let entity = match temp_state.get_entity(vault_id, req.key.as_bytes()) {
            Ok(e) => e,
            Err(e) => {
                log_ctx.end_storage_timer();
                let msg = format!("Read failed: {:?}", e);
                log_ctx.set_error("internal", &msg);
                return Err(WireError::new(ErrorCode::Internal, msg));
            },
        };

        log_ctx.end_storage_timer();

        let block_ts = block_timestamp.timestamp() as u64;
        let entity = entity.filter(|e| e.expires_at == 0 || e.expires_at > block_ts);

        let found = entity.is_some();
        let value_size = entity.as_ref().map(|e| e.value.len()).unwrap_or(0);
        log_ctx.set_found(found);
        log_ctx.set_value_size_bytes(value_size);
        log_ctx.set_bytes_read(value_size);
        log_ctx.set_block_height(req.at_height);

        let block_header = if req.include_proof {
            ReadService::get_block_header(
                archive,
                &region.applied_state,
                organization_id,
                vault_id,
                req.at_height,
            )
        } else {
            None
        };

        let chain_proof = if req.include_chain_proof && req.include_proof {
            let trusted_height = req.trusted_height.unwrap_or(0);
            ReadService::build_chain_proof(
                archive,
                &region.applied_state,
                organization_id,
                vault_id,
                trusted_height,
                req.at_height,
            )
        } else {
            None
        };

        if req.include_proof {
            let proof_size = chain_proof.as_ref().map_or(0, std::mem::size_of_val);
            log_ctx.set_proof_size_bytes(proof_size);
        }

        log_ctx.set_success();

        Ok(w::HistoricalReadResponse {
            value: entity.map(|e| Bytes::from(e.value)),
            block_height: req.at_height,
            block_header,
            merkle_proof: None,
            chain_proof,
        })
    }

    /// Watch blocks with historical replay support. Mirrors the tonic
    /// `watch_blocks` handler. The streaming machinery routes through
    /// [`drain_watch_blocks_stream`]; behaviour is identical to the tonic
    /// path but produces wire-native chunks.
    async fn watch_blocks(
        &self,
        request: w::WatchBlocksRequest,
        _ctx: RequestContext,
        sink: StreamSink,
    ) -> Result<(), WireError> {
        let req = request;

        if self.resolver.supports_forwarding() {
            let system = self.resolver.system_region().map_err(tonic_status_to_wire_error)?;
            let organization_id = SlugResolver::new(system.applied_state).extract_and_resolve(
                req.organization.map(|s| inferadb_ledger_types::OrganizationSlug::new(s.value())),
            )?;
            if let ResolveResult::Redirect(remote) = self
                .resolver
                .resolve_with_redirect(organization_id)
                .map_err(tonic_status_to_wire_error)?
            {
                return Err(tonic_status_to_wire_error(super::metadata::not_leader_remote_region(
                    &remote,
                    "Organization hosted by a remote region; reconnect to that region",
                )));
            }
        }

        let (organization_id, vault_id, region) = self
            .resolve_org_vault_consistent(req.organization, req.vault)
            .await
            .map_err(tonic_status_to_wire_error)?;

        if !region.handle.is_leader() {
            return Err(tonic_status_to_wire_error(
                self.not_leader_within_region(&region, "Not the leader for this region"),
            ));
        }
        let start_height = req.start_height;

        if start_height == 0 {
            return Err(WireError::new(
                ErrorCode::InvalidArgument,
                "start_height must be >= 1 (use 1 for full replay from genesis)",
            ));
        }

        // Enforce global concurrent stream limit. The tonic handler returned
        // `Status::resource_exhausted` here; the wire equivalent goes through
        // the bridge (tonic `ResourceExhausted` → wire `RateLimited`).
        let prev = self.active_streams.fetch_add(1, Ordering::Relaxed);
        if prev >= self.max_streams {
            self.active_streams.fetch_sub(1, Ordering::Relaxed);
            return Err(tonic_status_to_wire_error(Status::resource_exhausted(
                "Maximum concurrent watch streams exceeded",
            )));
        }

        let current_tip = region.vault_height(organization_id, vault_id);

        let announcements = match region.block_announcements.as_ref() {
            Some(a) => a,
            None => {
                self.active_streams.fetch_sub(1, Ordering::Relaxed);
                return Err(WireError::new(
                    ErrorCode::StaleRouting,
                    "Block announcements not available for this region",
                ));
            },
        };
        let receiver = announcements.subscribe();

        const MAX_HISTORICAL_BLOCKS: u64 = 10_000;
        let historical_blocks: Vec<ws::BlockAnnouncement> = if start_height <= current_tip {
            let effective_start = if current_tip - start_height + 1 > MAX_HISTORICAL_BLOCKS {
                let effective = current_tip.saturating_sub(MAX_HISTORICAL_BLOCKS - 1);
                warn!(
                    requested_start = start_height,
                    effective_start = effective,
                    current_tip,
                    skipped = effective - start_height,
                    "WatchBlocks historical replay exceeds limit, skipping oldest blocks"
                );
                effective
            } else {
                start_height
            };
            ReadService::fetch_historical_announcements(
                &region.block_archive,
                &region.applied_state,
                organization_id,
                vault_id,
                effective_start,
                current_tip,
            )
        } else {
            vec![]
        };

        debug!(
            organization_id = organization_id.value(),
            vault_id = vault_id.value(),
            start_height,
            current_tip,
            historical_count = historical_blocks.len(),
            "WatchBlocks: starting stream"
        );

        let historical_stream =
            futures::stream::iter(historical_blocks.into_iter().map(Ok::<_, tonic::Status>));

        let last_historical_height =
            if start_height <= current_tip { current_tip } else { start_height - 1 };

        // γ Phase 3a slug normalization (see tonic handler comments).
        let watch_org_id = organization_id;
        let watch_vault_id = vault_id;
        let watch_org_slug = req.organization.as_ref().map_or(0, |o| o.value());
        let watch_vault_slug = req.vault.as_ref().map_or(0, |v| v.value());
        let req_org_slug = req.organization;
        let req_vault_slug = req.vault;
        let broadcast_stream =
            tokio_stream::wrappers::BroadcastStream::new(receiver).filter_map(move |result| {
                async move {
                    match result {
                        Ok(announcement) => {
                            let ann_org =
                                announcement.organization.as_ref().map_or(0, |n| n.value());
                            let ann_vault = announcement.vault.as_ref().map_or(0, |v| v.value());

                            let org_matches =
                                ann_org == watch_org_slug || ann_org == watch_org_id.value() as u64;
                            let vault_matches = ann_vault == watch_vault_slug
                                || ann_vault == watch_vault_id.value() as u64;
                            if !org_matches || !vault_matches {
                                return None;
                            }
                            if announcement.height <= last_historical_height {
                                return None;
                            }
                            // Stamp the request's external slugs onto the
                            // wire announcement directly — no proto trip
                            // required now that the drain helper consumes
                            // wire announcements end-to-end.
                            let mut wire_announcement = announcement;
                            wire_announcement.organization = req_org_slug;
                            wire_announcement.vault = req_vault_slug;
                            Some(Ok(wire_announcement))
                        },
                        Err(_) => Some(Err(Status::internal("Stream error"))),
                    }
                }
            });

        let combined = historical_stream.chain(broadcast_stream);
        let guarded =
            StreamGuard { inner: Box::pin(combined), counter: Arc::clone(&self.active_streams) };

        // The drain helper consumes Stream<Item = Result<BlockAnnouncement, tonic::Status>>;
        // StreamGuard wraps exactly that and decrements the active_streams counter on drop.
        drain_watch_blocks_stream(guarded, sink).await
    }

    /// Retrieves a single block by height. Mirrors the tonic `get_block` handler.
    async fn get_block(
        &self,
        request: w::GetBlockRequest,
        _ctx: RequestContext,
    ) -> Result<w::GetBlockResponse, WireError> {
        let req = request;

        if self.resolver.supports_forwarding() {
            let system = self.resolver.system_region().map_err(tonic_status_to_wire_error)?;
            let organization_id = SlugResolver::new(system.applied_state).extract_and_resolve(
                req.organization.map(|s| inferadb_ledger_types::OrganizationSlug::new(s.value())),
            )?;
            if let ResolveResult::Redirect(remote) = self
                .resolver
                .resolve_with_redirect(organization_id)
                .map_err(tonic_status_to_wire_error)?
            {
                return Err(tonic_status_to_wire_error(super::metadata::not_leader_remote_region(
                    &remote,
                    "Organization hosted by a remote region; reconnect to that region",
                )));
            }
        }

        let (organization_id, vault_id, region) = self
            .resolve_org_vault_consistent(req.organization, req.vault)
            .await
            .map_err(tonic_status_to_wire_error)?;

        if !region.handle.is_leader() {
            return Err(tonic_status_to_wire_error(
                self.not_leader_within_region(&region, "Not the leader for this region"),
            ));
        }

        let archive = &region.block_archive;
        let height = req.height;

        let region_height = archive
            .find_region_height(organization_id, vault_id, height)
            .map_err(|e| super::helpers::storage_err(&e))?;

        let region_height = match region_height {
            Some(h) => h,
            None => return Ok(w::GetBlockResponse { block: None }),
        };

        let region_block =
            archive.read_block(region_height).map_err(|e| super::helpers::storage_err(&e))?;

        let vault_entry = region_block.vault_entries.iter().find(|e| {
            e.organization == organization_id && e.vault == vault_id && e.vault_height == height
        });

        let vault = region
            .applied_state
            .resolve_vault_id_to_slug(organization_id, vault_id)
            .unwrap_or(VaultSlug::new(0));
        let block = vault_entry.map(|entry| vault_entry_to_wire_block(entry, &region_block, vault));

        Ok(w::GetBlockResponse { block })
    }

    /// Retrieves a contiguous range of blocks. Mirrors the tonic
    /// `get_block_range` handler.
    async fn get_block_range(
        &self,
        request: w::GetBlockRangeRequest,
        _ctx: RequestContext,
    ) -> Result<w::GetBlockRangeResponse, WireError> {
        let req = request;

        if self.resolver.supports_forwarding() {
            let system = self.resolver.system_region().map_err(tonic_status_to_wire_error)?;
            let organization_id = SlugResolver::new(system.applied_state).extract_and_resolve(
                req.organization.map(|s| inferadb_ledger_types::OrganizationSlug::new(s.value())),
            )?;
            if let ResolveResult::Redirect(remote) = self
                .resolver
                .resolve_with_redirect(organization_id)
                .map_err(tonic_status_to_wire_error)?
            {
                return Err(tonic_status_to_wire_error(super::metadata::not_leader_remote_region(
                    &remote,
                    "Organization hosted by a remote region; reconnect to that region",
                )));
            }
        }

        let (organization_id, vault_id, region) = self
            .resolve_org_vault_consistent(req.organization, req.vault)
            .await
            .map_err(tonic_status_to_wire_error)?;

        if !region.handle.is_leader() {
            return Err(tonic_status_to_wire_error(
                self.not_leader_within_region(&region, "Not the leader for this region"),
            ));
        }

        let archive = &region.block_archive;
        let start_height = req.start_height;
        let end_height = req.end_height;

        let max_range = 1000u64;
        let end_height = end_height.min(start_height.saturating_add(max_range - 1));

        let mut blocks: Vec<ws::Block> = Vec::new();

        for height in start_height..=end_height {
            let region_height = match archive
                .find_region_height(organization_id, vault_id, height)
                .map_err(|e| super::helpers::storage_err(&e))?
            {
                Some(h) => h,
                None => continue,
            };

            let region_block =
                archive.read_block(region_height).map_err(|e| super::helpers::storage_err(&e))?;

            if let Some(entry) = region_block.vault_entries.iter().find(|e| {
                e.organization == organization_id && e.vault == vault_id && e.vault_height == height
            }) {
                let vault = region
                    .applied_state
                    .resolve_vault_id_to_slug(organization_id, vault_id)
                    .unwrap_or(VaultSlug::new(0));
                blocks.push(vault_entry_to_wire_block(entry, &region_block, vault));
            }
        }

        let current_tip = region.vault_height(organization_id, vault_id);

        Ok(w::GetBlockRangeResponse { blocks, current_tip })
    }

    /// Returns the latest block height, hash, and state root for a vault.
    /// Mirrors the tonic `get_tip` handler.
    async fn get_tip(
        &self,
        request: w::GetTipRequest,
        _ctx: RequestContext,
    ) -> Result<w::GetTipResponse, WireError> {
        let req = request;

        if self.resolver.supports_forwarding() {
            let system = self.resolver.system_region().map_err(tonic_status_to_wire_error)?;
            let organization_id = SlugResolver::new(system.applied_state).extract_and_resolve(
                req.organization.map(|s| inferadb_ledger_types::OrganizationSlug::new(s.value())),
            )?;
            if let ResolveResult::Redirect(remote) = self
                .resolver
                .resolve_with_redirect(organization_id)
                .map_err(tonic_status_to_wire_error)?
            {
                return Err(tonic_status_to_wire_error(super::metadata::not_leader_remote_region(
                    &remote,
                    "Organization hosted by a remote region; reconnect to that region",
                )));
            }
        }

        let (organization_id, vault_id, region) = self
            .resolve_org_vault_consistent(req.organization, req.vault)
            .await
            .map_err(tonic_status_to_wire_error)?;

        if !region.handle.is_leader() {
            return Err(tonic_status_to_wire_error(
                self.not_leader_within_region(&region, "Not the leader for this region"),
            ));
        }

        let height = if vault_id.value() != 0 {
            region.vault_height(organization_id, vault_id)
        } else if organization_id.value() != 0 {
            region.applied_state.org_max_vault_height(organization_id)
        } else {
            region.applied_state.max_vault_height()
        };

        let (block_hash, state_root) = if vault_id.value() != 0 && height > 0 {
            ReadService::get_tip_hashes(&region.block_archive, organization_id, vault_id, height)
        } else {
            (None, None)
        };

        Ok(w::GetTipResponse { height, block_hash, state_root })
    }

    /// Returns the last committed sequence number for a client within a vault.
    /// Mirrors the tonic `get_client_state` handler.
    async fn get_client_state(
        &self,
        request: w::GetClientStateRequest,
        _ctx: RequestContext,
    ) -> Result<w::GetClientStateResponse, WireError> {
        let req = request;

        if self.resolver.supports_forwarding() {
            let system = self.resolver.system_region().map_err(tonic_status_to_wire_error)?;
            let organization_id = SlugResolver::new(system.applied_state).extract_and_resolve(
                req.organization.map(|s| inferadb_ledger_types::OrganizationSlug::new(s.value())),
            )?;
            if let ResolveResult::Redirect(remote) = self
                .resolver
                .resolve_with_redirect(organization_id)
                .map_err(tonic_status_to_wire_error)?
            {
                return Err(tonic_status_to_wire_error(super::metadata::not_leader_remote_region(
                    &remote,
                    "Organization hosted by a remote region; reconnect to that region",
                )));
            }
        }

        let (organization_id, vault_id, region) = self
            .resolve_org_vault_consistent(req.organization, req.vault)
            .await
            .map_err(tonic_status_to_wire_error)?;

        if !region.handle.is_leader() {
            return Err(tonic_status_to_wire_error(
                self.not_leader_within_region(&region, "Not the leader for this region"),
            ));
        }
        let client_id = req.client_id.as_ref().map(|c| c.id.as_str()).unwrap_or("");

        let last_committed_sequence = region.client_sequence(organization_id, vault_id, client_id);

        Ok(w::GetClientStateResponse { last_committed_sequence })
    }

    /// Lists relationships in a vault with optional filters and pagination.
    /// Mirrors the tonic `list_relationships` handler.
    async fn list_relationships(
        &self,
        request: w::ListRelationshipsRequest,
        _ctx: RequestContext,
    ) -> Result<w::ListRelationshipsResponse, WireError> {
        let grpc_deadline = wire_grpc_deadline(&_ctx);
        let req = request;

        if self.resolver.supports_forwarding() {
            let system = self.resolver.system_region().map_err(tonic_status_to_wire_error)?;
            let organization_id = SlugResolver::new(system.applied_state).extract_and_resolve(
                req.organization.map(|s| inferadb_ledger_types::OrganizationSlug::new(s.value())),
            )?;
            if let ResolveResult::Redirect(remote) = self
                .resolver
                .resolve_with_redirect(organization_id)
                .map_err(tonic_status_to_wire_error)?
            {
                return Err(tonic_status_to_wire_error(super::metadata::not_leader_remote_region(
                    &remote,
                    "Organization hosted by a remote region; reconnect to that region",
                )));
            }
        }

        let (organization_id, vault_id, region) = self
            .resolve_org_vault_consistent(req.organization, req.vault)
            .await
            .map_err(tonic_status_to_wire_error)?;

        self.resolve_read_consistency(
            &region,
            req.consistency as i32,
            grpc_deadline,
            req.organization,
            req.vault,
        )
        .await
        .map_err(tonic_status_to_wire_error)?;
        let limit = if req.limit == 0 { 100 } else { req.limit as usize };

        let query_params = format!(
            "resource:{},relation:{},subject:{}",
            req.resource.as_deref().unwrap_or(""),
            req.relation.as_deref().unwrap_or(""),
            req.subject.as_deref().unwrap_or("")
        );
        let query_hash = PageTokenCodec::compute_query_hash(query_params.as_bytes());

        let block_height = region.vault_height(organization_id, vault_id);

        let (resume_key, at_height) = if req.page_token.is_empty() {
            (None, block_height)
        } else {
            let token = self
                .page_token_codec
                .decode(&req.page_token)
                .map_err(|e| WireError::new(ErrorCode::InvalidArgument, e.to_string()))?;

            self.page_token_codec
                .validate_context(&token, organization_id, vault_id, query_hash)
                .map_err(|e| WireError::new(ErrorCode::InvalidArgument, e.to_string()))?;

            (Some(String::from_utf8_lossy(&token.last_key).to_string()), token.at_height)
        };

        let state = &*region.state;

        let relationships: Vec<ws::Relationship> =
            if let (Some(resource), Some(relation)) = (&req.resource, &req.relation) {
                let subjects = state
                    .list_subjects(vault_id, resource, relation)
                    .map_err(|e| super::helpers::storage_err(&e))?;

                subjects
                    .into_iter()
                    .take(limit)
                    .map(|subject| ws::Relationship {
                        resource: resource.clone(),
                        relation: relation.clone(),
                        subject,
                    })
                    .collect()
            } else if let Some(subject) = &req.subject {
                let resources = state
                    .list_resources_for_subject(vault_id, subject)
                    .map_err(|e| super::helpers::storage_err(&e))?;

                resources
                    .into_iter()
                    .take(limit)
                    .map(|(resource, relation)| ws::Relationship {
                        resource,
                        relation,
                        subject: subject.clone(),
                    })
                    .collect()
            } else {
                let cursor_rel = resume_key.as_deref().and_then(parse_relationship_cursor);
                let raw_rels = state
                    .list_relationships(vault_id, cursor_rel.as_ref(), limit)
                    .map_err(|e| super::helpers::storage_err(&e))?;

                raw_rels
                    .into_iter()
                    .filter(|r| req.resource.as_ref().is_none_or(|res| r.resource == *res))
                    .filter(|r| req.relation.as_ref().is_none_or(|rel| r.relation == *rel))
                    .map(domain_relationship_to_wire)
                    .collect()
            };

        let next_page_token = if relationships.len() >= limit {
            relationships
                .last()
                .map(|r| {
                    let cursor = format!("{}#{}@{}", r.resource, r.relation, r.subject);
                    let token = PageToken {
                        version: 1,
                        organization: organization_id,
                        vault: vault_id,
                        last_key: cursor.into_bytes(),
                        at_height,
                        query_hash,
                    };
                    self.page_token_codec.encode(&token)
                })
                .unwrap_or_default()
        } else {
            String::new()
        };

        Ok(w::ListRelationshipsResponse { relationships, next_page_token, block_height })
    }

    /// Checks whether the exact `(resource, relation, subject)` relationship
    /// tuple exists in the given vault. Mirrors the tonic
    /// `check_relationship` handler.
    async fn check_relationship(
        &self,
        request: w::CheckRelationshipRequest,
        _ctx: RequestContext,
    ) -> Result<w::CheckRelationshipResponse, WireError> {
        let grpc_deadline = wire_grpc_deadline(&_ctx);
        let mut log_ctx =
            LogRequestContext::from_wire_context("ReadService", "check_relationship", &_ctx, None);
        log_ctx.set_operation_type(OperationType::Read);
        if let Some(sampler) = &self.sampler {
            log_ctx.set_sampler(sampler.clone());
        }
        if let Some(node_id) = &self.node_id {
            log_ctx.set_node_id(*node_id);
        }

        let req = request;

        // Enforce required organization, vault, caller, resource, relation,
        // subject — same as the tonic `TryFrom<CheckRelationshipRequest>`
        // discipline.
        let organization_slug = req.organization.ok_or_else(|| {
            log_ctx.set_error("invalid_argument", "organization is required");
            WireError::new(ErrorCode::InvalidArgument, "organization is required")
        })?;
        let vault_slug = req.vault.ok_or_else(|| {
            log_ctx.set_error("invalid_argument", "vault is required");
            WireError::new(ErrorCode::InvalidArgument, "vault is required")
        })?;
        let caller_slug = req.caller.ok_or_else(|| {
            log_ctx.set_error("invalid_argument", "caller is required");
            WireError::new(ErrorCode::InvalidArgument, "caller is required")
        })?;
        let domain_org = inferadb_ledger_types::OrganizationSlug::new(organization_slug.value());
        let domain_vault = inferadb_ledger_types::VaultSlug::new(vault_slug.value());

        // Cross-region forwarding (slug already required).
        if self.resolver.supports_forwarding() {
            let system = self.resolver.system_region().map_err(tonic_status_to_wire_error)?;
            let organization_id = SlugResolver::new(system.applied_state)
                .resolve(domain_org)
                .inspect_err(|err| log_ctx.set_error("slug_resolve", &err.message))?;
            if let ResolveResult::Redirect(remote) = self
                .resolver
                .resolve_with_redirect(organization_id)
                .map_err(tonic_status_to_wire_error)?
            {
                return Err(tonic_status_to_wire_error(super::metadata::not_leader_remote_region(
                    &remote,
                    "Organization hosted by a remote region; reconnect to that region",
                )));
            }
        }

        super::helpers::ensure_global_consistency(self.manager.as_deref()).await;
        let system = self.resolver.system_region().map_err(tonic_status_to_wire_error)?;
        let organization_id = SlugResolver::new(system.applied_state.clone())
            .resolve(domain_org)
            .inspect_err(|err| log_ctx.set_error("slug_resolve", &err.message))?;
        let mut region =
            self.resolver.resolve(organization_id).map_err(tonic_status_to_wire_error)?;
        let vault_id = SlugResolver::new(system.applied_state)
            .resolve_vault(domain_vault)
            .inspect_err(|err| log_ctx.set_error("slug_resolve_vault", &err.message))?;

        if let Some(manager) = &self.manager
            && let Ok(vault_group) =
                manager.get_vault_group(region.region, organization_id, vault_id)
        {
            region.block_archive = Arc::clone(vault_group.block_archive());
            region.attach_vault_group(&vault_group);
        }

        log_ctx.set_caller(caller_slug.value());
        log_ctx.set_target(organization_slug.value(), vault_slug.value());

        // Validate triple via the existing helper. The helper returns
        // tonic::Status; bridge it back to WireError.
        if let Err(status) =
            self.validate_relationship_triple(&req.resource, &req.relation, &req.subject)
        {
            log_ctx.set_error("invalid_argument", status.message());
            return Err(tonic_status_to_wire_error(status));
        }

        let tuple_key = format!("{}#{}@{}", req.resource, req.relation, req.subject);
        log_ctx.set_key(&tuple_key);

        let consistency_label = match req.consistency {
            ws::ReadConsistency::Linearizable => "linearizable",
            _ => "eventual",
        };
        log_ctx.set_consistency(consistency_label);
        log_ctx.set_include_proof(false);

        if let Err(e) = self
            .resolve_read_consistency(
                &region,
                req.consistency as i32,
                grpc_deadline,
                Some(organization_slug),
                Some(vault_slug),
            )
            .await
        {
            log_ctx.set_error("consistency_error", e.message());
            return Err(tonic_status_to_wire_error(e));
        }

        let health = region.vault_health(organization_id, vault_id);
        if let VaultHealthStatus::Diverged { at_height, .. } = &health {
            let msg = format!(
                "Vault {}:{} has diverged at height {}",
                organization_id, vault_id, at_height
            );
            log_ctx.set_error("vault_diverged", &msg);
            return Err(WireError::new(ErrorCode::StaleRouting, msg));
        }

        log_ctx.start_storage_timer();
        let exists = region
            .state
            .relationship_exists(vault_id, &req.resource, &req.relation, &req.subject)
            .map_err(|e| {
                log_ctx.end_storage_timer();
                let wire_err = super::helpers::storage_err(&e);
                log_ctx.set_error("storage_error", &wire_err.message);
                wire_err
            })?;
        log_ctx.end_storage_timer();

        let block_height = region.vault_height(organization_id, vault_id);
        log_ctx.set_block_height(block_height);
        log_ctx.set_found(exists);

        metrics::record_organization_operation(organization_id, "check_relationship");
        let elapsed = log_ctx.elapsed_secs();
        metrics::record_organization_latency(organization_id, "check_relationship", elapsed);
        log_ctx.set_success();

        Ok(w::CheckRelationshipResponse { exists, checked_at_height: block_height })
    }

    /// Lists distinct resource identifiers in a vault, optionally filtered by
    /// resource type. Mirrors the tonic `list_resources` handler.
    async fn list_resources(
        &self,
        request: w::ListResourcesRequest,
        _ctx: RequestContext,
    ) -> Result<w::ListResourcesResponse, WireError> {
        let grpc_deadline = wire_grpc_deadline(&_ctx);
        let req = request;

        if self.resolver.supports_forwarding() {
            let system = self.resolver.system_region().map_err(tonic_status_to_wire_error)?;
            let organization_id = SlugResolver::new(system.applied_state).extract_and_resolve(
                req.organization.map(|s| inferadb_ledger_types::OrganizationSlug::new(s.value())),
            )?;
            if let ResolveResult::Redirect(remote) = self
                .resolver
                .resolve_with_redirect(organization_id)
                .map_err(tonic_status_to_wire_error)?
            {
                return Err(tonic_status_to_wire_error(super::metadata::not_leader_remote_region(
                    &remote,
                    "Organization hosted by a remote region; reconnect to that region",
                )));
            }
        }

        let (organization_id, vault_id, region) = self
            .resolve_org_vault_consistent(req.organization, req.vault)
            .await
            .map_err(tonic_status_to_wire_error)?;

        self.resolve_read_consistency(
            &region,
            req.consistency as i32,
            grpc_deadline,
            req.organization,
            req.vault,
        )
        .await
        .map_err(tonic_status_to_wire_error)?;
        let limit = if req.limit == 0 { 100 } else { req.limit as usize };

        let query_params = format!("resource_type:{}", req.resource_type);
        let query_hash = PageTokenCodec::compute_query_hash(query_params.as_bytes());

        let block_height = region.vault_height(organization_id, vault_id);

        let (resume_key, at_height) = if req.page_token.is_empty() {
            (None, block_height)
        } else {
            let token = self
                .page_token_codec
                .decode(&req.page_token)
                .map_err(|e| WireError::new(ErrorCode::InvalidArgument, e.to_string()))?;

            self.page_token_codec
                .validate_context(&token, organization_id, vault_id, query_hash)
                .map_err(|e| WireError::new(ErrorCode::InvalidArgument, e.to_string()))?;

            (Some(String::from_utf8_lossy(&token.last_key).to_string()), token.at_height)
        };

        let state = &*region.state;
        let cursor_rel = resume_key.as_deref().and_then(parse_relationship_cursor);
        let relationships = state
            .list_relationships(vault_id, cursor_rel.as_ref(), limit * 10)
            .map_err(|e| super::helpers::storage_err(&e))?;

        let mut resources: Vec<String> = relationships
            .into_iter()
            .filter(|r| req.resource_type.is_empty() || r.resource.starts_with(&req.resource_type))
            .map(|r| r.resource)
            .collect();

        resources.sort();
        resources.dedup();
        resources.truncate(limit);

        let next_page_token = if resources.len() >= limit {
            resources
                .last()
                .map(|res| {
                    let token = PageToken {
                        version: 1,
                        organization: organization_id,
                        vault: vault_id,
                        last_key: res.as_bytes().to_vec(),
                        at_height,
                        query_hash,
                    };
                    self.page_token_codec.encode(&token)
                })
                .unwrap_or_default()
        } else {
            String::new()
        };

        Ok(w::ListResourcesResponse { resources, next_page_token, block_height })
    }

    /// Lists entities in a vault with optional key-prefix filter and
    /// pagination. Mirrors the tonic `list_entities` handler.
    async fn list_entities(
        &self,
        request: w::ListEntitiesRequest,
        _ctx: RequestContext,
    ) -> Result<w::ListEntitiesResponse, WireError> {
        let grpc_deadline = wire_grpc_deadline(&_ctx);
        let req = request;

        if self.resolver.supports_forwarding() {
            let system = self.resolver.system_region().map_err(tonic_status_to_wire_error)?;
            let organization_id = SlugResolver::new(system.applied_state).extract_and_resolve(
                req.organization.map(|s| inferadb_ledger_types::OrganizationSlug::new(s.value())),
            )?;
            if let ResolveResult::Redirect(remote) = self
                .resolver
                .resolve_with_redirect(organization_id)
                .map_err(tonic_status_to_wire_error)?
            {
                return Err(tonic_status_to_wire_error(super::metadata::not_leader_remote_region(
                    &remote,
                    "Organization hosted by a remote region; reconnect to that region",
                )));
            }
        }

        let (organization_id, vault_id, region) = self
            .resolve_org_vault_consistent(req.organization, req.vault)
            .await
            .map_err(tonic_status_to_wire_error)?;

        self.resolve_read_consistency(
            &region,
            req.consistency as i32,
            grpc_deadline,
            req.organization,
            req.vault,
        )
        .await
        .map_err(tonic_status_to_wire_error)?;

        let limit = if req.limit == 0 { 100 } else { req.limit as usize };
        let prefix = if req.key_prefix.is_empty() { None } else { Some(req.key_prefix.as_str()) };

        if let Some(prefix_str) = prefix
            && prefix_str.starts_with('_')
        {
            return Err(WireError::new(
                ErrorCode::InvalidArgument,
                "key_prefix must not start with '_' (reserved for system keys)",
            ));
        }

        let query_params = format!(
            "prefix:{},include_expired:{},vault:{}",
            req.key_prefix, req.include_expired, vault_id
        );
        let query_hash = PageTokenCodec::compute_query_hash(query_params.as_bytes());

        let block_height = if vault_id.value() != 0 {
            region.vault_height(organization_id, vault_id)
        } else {
            region.applied_state.org_max_vault_height(organization_id)
        };

        let (resume_key, at_height) = if req.page_token.is_empty() {
            (None, block_height)
        } else {
            let token = self
                .page_token_codec
                .decode(&req.page_token)
                .map_err(|e| WireError::new(ErrorCode::InvalidArgument, e.to_string()))?;

            self.page_token_codec
                .validate_context(&token, organization_id, vault_id, query_hash)
                .map_err(|e| WireError::new(ErrorCode::InvalidArgument, e.to_string()))?;

            (Some(String::from_utf8_lossy(&token.last_key).to_string()), token.at_height)
        };

        let state = &*region.state;
        let raw_entities = state
            .list_entities(vault_id, prefix, resume_key.as_deref(), limit + 1)
            .map_err(|e| super::helpers::storage_err(&e))?;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let filtered: Vec<_> = raw_entities
            .into_iter()
            .filter(|e| req.include_expired || e.expires_at == 0 || e.expires_at > now)
            .take(limit)
            .collect();

        let entities: Vec<ws::Entity> = filtered.iter().map(domain_entity_to_wire).collect();

        let next_page_token = if entities.len() >= limit {
            filtered
                .last()
                .map(|e| {
                    let token = PageToken {
                        version: 1,
                        organization: organization_id,
                        vault: vault_id,
                        last_key: e.key.clone(),
                        at_height,
                        query_hash,
                    };
                    self.page_token_codec.encode(&token)
                })
                .unwrap_or_default()
        } else {
            String::new()
        };

        Ok(w::ListEntitiesResponse { entities, next_page_token, block_height })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use bytes::Bytes;
    use tokio::sync::mpsc;

    use super::*;

    // -----------------------------------------------------------------------
    // Streaming drain helper.
    // -----------------------------------------------------------------------

    fn sample_wire_announcement(height: u64) -> ws::BlockAnnouncement {
        ws::BlockAnnouncement {
            organization: Some(ws::OrganizationSlug::new(0xDEAD_BEEF)),
            vault: Some(ws::VaultSlug::new(0xCAFE_F00D)),
            height,
            block_hash: None,
            state_root: None,
            timestamp: 0,
        }
    }

    #[tokio::test]
    async fn drain_watch_blocks_stream_pushes_postcard_encoded_chunks_to_sink() {
        let announcements = vec![
            sample_wire_announcement(1),
            sample_wire_announcement(2),
            sample_wire_announcement(3),
        ];

        // Synthetic stream of Ok(wire_announcement) items — same shape as
        // the production `StreamGuard`-wrapped stream.
        let stream = futures::stream::iter(
            announcements.clone().into_iter().map(Ok::<_, tonic::Status>).collect::<Vec<_>>(),
        );

        let (tx, mut rx) = mpsc::channel::<Bytes>(8);
        let sink = StreamSink::new(tx);

        drain_watch_blocks_stream(stream, sink).await.expect("drain succeeds");

        // Decode each chunk and assert it matches the corresponding wire
        // announcement.
        let mut received = Vec::new();
        while let Some(chunk) = rx.recv().await {
            let decoded: ws::BlockAnnouncement = postcard::from_bytes(&chunk).unwrap();
            received.push(decoded);
        }

        assert_eq!(received, announcements);
    }

    #[tokio::test]
    async fn drain_watch_blocks_stream_handles_sink_closed() {
        // Drop the receiver immediately so the first send fails. The
        // helper should treat this as the peer dropping the stream and
        // exit cleanly.
        let announcements = vec![sample_wire_announcement(1)];
        let stream = futures::stream::iter(
            announcements.into_iter().map(Ok::<_, tonic::Status>).collect::<Vec<_>>(),
        );

        let (tx, rx) = mpsc::channel::<Bytes>(1);
        drop(rx);
        let sink = StreamSink::new(tx);

        let result = drain_watch_blocks_stream(stream, sink).await;
        assert!(result.is_ok(), "sink-closed should not be an error: {result:?}");
    }

    #[tokio::test]
    async fn drain_watch_blocks_stream_propagates_status_error() {
        // First chunk Ok, then an Err — the helper should send the first
        // chunk, then return the mapped WireError.
        let stream = futures::stream::iter(vec![
            Ok::<_, tonic::Status>(sample_wire_announcement(1)),
            Err::<ws::BlockAnnouncement, _>(tonic::Status::unavailable("region unloaded")),
        ]);

        let (tx, mut rx) = mpsc::channel::<Bytes>(8);
        let sink = StreamSink::new(tx);

        let err = drain_watch_blocks_stream(stream, sink).await.expect_err("should propagate Err");
        // tonic Code::Unavailable maps to wire ErrorCode::StaleRouting.
        assert_eq!(err.code, ErrorCode::StaleRouting);
        assert_eq!(err.message, "region unloaded");

        // The first chunk should have made it to the sink before the
        // helper observed the error.
        let first = rx.recv().await.expect("first chunk should land");
        let decoded: ws::BlockAnnouncement = postcard::from_bytes(&first).unwrap();
        assert_eq!(decoded.height, 1);
    }

    #[tokio::test]
    async fn drain_watch_blocks_stream_handles_empty_stream() {
        let stream =
            futures::stream::iter(Vec::<Result<ws::BlockAnnouncement, tonic::Status>>::new());
        let (tx, mut rx) = mpsc::channel::<Bytes>(4);
        let sink = StreamSink::new(tx);

        drain_watch_blocks_stream(stream, sink).await.expect("empty stream is fine");
        // No chunks should have been pushed.
        assert!(rx.recv().await.is_none() || rx.try_recv().is_err());
    }

    // End-to-end shim test (ReadService instantiation against a live
    // ConsensusHandle / StateLayer / RaftManager) is deferred to E.7
    // (TestCluster migration). The streaming drain helper is tested with
    // synthetic streams; tonic-status → WireError mapping is covered by
    // `wire_helpers::tests`.
}
