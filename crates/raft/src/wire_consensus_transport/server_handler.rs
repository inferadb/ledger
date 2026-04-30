//! Server-side Raft dispatcher for wire-transport (Phase 0.0.E.6b.4).
//!
//! Routes inter-node Raft opcodes (`0x0D00`–`0x0DFF`) to the local Raft
//! engine and impls
//! [`inferadb_ledger_wire_transport::SnapshotHandler`] for the snapshot
//! sub-protocol from E.6a. The four Raft RPCs the wire-services macro
//! declared as unary placeholders (`committed_index`, `replicate`,
//! `regional_proposal`, `install_snapshot_stream`) route here:
//!
//! - `committed_index` — discrete unary RPC; resolves the target group via
//!   [`RaftManager::lookup_by_consensus_shard`] / [`RaftManager::get_region_group`] /
//!   [`RaftManager::get_vault_group`] and replies with the shard's current commit index + leader
//!   term.
//! - `replicate` — long-lived bidi stream multiplexing
//!   [`inferadb_ledger_wire::services::raft::ConsensusEnvelope`] frames in both directions; runs
//!   the `super::peer_sender::WirePeerSender` reverse path on the server side. Driven by
//!   [`WireRaftServerHandler::handle_replicate_stream`] off the dispatcher's
//!   `OPCODE_RAFT_PROTOCOL_HANDSHAKE` branch — never goes through the macro-generated
//!   `RaftService::replicate` placeholder.
//! - `regional_proposal` — discrete unary RPC; forwards the postcard-encoded `RaftPayload<R>` bytes
//!   into the data-region group at `OrganizationId(0)` for the target region and waits for the
//!   apply pipeline to commit before replying.
//! - `install_snapshot_stream` — handled by the [`SnapshotHandler`] trait impl on this struct,
//!   plumbed through the dispatcher's `snapshot_handler()` hook.
//!
//! # Wiring (E.6b.5, follow-up)
//!
//! E.6b.4 (this task) only exposes the struct and its public methods. The
//! [`crate::raft_manager::RaftManager`] toggle (E.6b.5) wires the handler into the
//! dispatcher inside `crates/services/src/wire_server.rs` — the dispatcher
//! recognises `OPCODE_RAFT_PROTOCOL_HANDSHAKE` (Replicate) and
//! `OPCODE_INSTALL_SNAPSHOT_STREAM` (snapshot) and dispatches via the custom
//! routes; everything else in the Raft block flows through the macro-generated
//! `dispatch_raft_service_unary` against this handler's wire-trait impl.

use std::{path::PathBuf, sync::Arc};

use bytes::Bytes;
use inferadb_ledger_consensus::types::{ConsensusStateId, NodeId};
use inferadb_ledger_types::{OrganizationId, Region, VaultId};
use inferadb_ledger_wire::{
    CURRENT_PROTOCOL_VERSION, ErrorCode, Frame, FrameHeader, OPCODE_RAFT_PROTOCOL_HANDSHAKE,
    RAFT_PROTOCOL_VERSION, RequestContext, WireError,
    services::raft::{
        CommittedIndexRequest, CommittedIndexResponse, ConsensusAck, ConsensusEnvelope,
        InstallSnapshotHeader, InstallSnapshotScope, InstallSnapshotStreamResponse,
        RegionalProposalRequest, RegionalProposalResult,
    },
};
use inferadb_ledger_wire_transport::{
    error::TransportError,
    frame_io::{FLAG_IS_RESPONSE, read_frame, write_frame},
    replicate_stream::ReplicateHandler,
    snapshot_stream::SnapshotHandler,
};
use tokio_util::sync::CancellationToken;

use super::peer_sender::OPCODE_REPLICATE;
use crate::{raft_manager::RaftManager, snapshot::SnapshotScope};

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

/// Test-only callback fired from [`SnapshotHandler::on_snapshot_complete`].
///
/// Production callers leave this `None`; the install path is reactor-driven
/// via `crate::snapshot_installer::RaftManagerSnapshotInstaller` (Rule 22)
/// and does not depend on the receive-side hook firing. Tests inject a
/// closure to observe staging activity without spinning up a full apply
/// pipeline.
pub type SnapshotCompleteHook =
    Arc<dyn Fn(&InstallSnapshotHeader, &std::path::Path) + Send + Sync + 'static>;

/// Server-side Raft handler.
///
/// Owns:
///
/// - A reference to the local Raft state (via [`Arc<RaftManager>`]) used by every inbound consensus
///   message.
/// - An optional snapshot-completion hook that fires after the receiver-side atomic-rename.
///   Production constructs without one; the staged file is picked up by
///   `crate::snapshot_installer::RaftManagerSnapshotInstaller` via the reactor's
///   `Action::InstallSnapshot` dispatch (Rule 22). Tests may install a hook to observe completion
///   without driving a full install.
/// - An optional shared peer-liveness map, updated on every successful inbound consensus envelope
///   so the dependency-health checker observes reachability without an out-of-band probe. Mirrors
///   the gRPC `RaftService` path's `peer_liveness` plumbing exactly.
///
/// Held behind `Arc` by [`crate::raft_manager::RaftManager`] callers (the
/// dispatcher composition lands in E.6b.5). `Send + Sync + 'static` is required
/// by both the wire-services `RaftService` trait and
/// [`inferadb_ledger_wire_transport::SnapshotHandler`].
pub struct WireRaftServerHandler {
    manager: Arc<RaftManager>,
    on_snapshot_complete: Option<SnapshotCompleteHook>,
    peer_liveness: Option<crate::dependency_health::PeerLivenessMap>,
}

impl std::fmt::Debug for WireRaftServerHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WireRaftServerHandler")
            .field("on_snapshot_complete_hook", &self.on_snapshot_complete.is_some())
            .field("peer_liveness", &self.peer_liveness.is_some())
            .finish_non_exhaustive()
    }
}

impl WireRaftServerHandler {
    /// Construct a handler bound to the supplied [`RaftManager`].
    ///
    /// No snapshot-completion hook is installed; production wiring leaves it
    /// `None` and relies on the reactor-driven install path.
    pub fn new(manager: Arc<RaftManager>) -> Self {
        Self { manager, on_snapshot_complete: None, peer_liveness: None }
    }

    /// Builder-style override that installs a snapshot-completion hook.
    ///
    /// Used by tests to observe receive-side completion without instantiating
    /// the full apply pipeline. The hook fires once per successfully staged
    /// snapshot, after the atomic rename to `*.snap.staged` lands.
    #[must_use]
    pub fn with_snapshot_complete_hook(mut self, hook: SnapshotCompleteHook) -> Self {
        self.on_snapshot_complete = Some(hook);
        self
    }

    /// Builder-style override that attaches the shared peer-liveness map.
    ///
    /// Mirrors `crate::services::raft::RaftService::with_peer_liveness` from
    /// the gRPC path. Each successful inbound consensus envelope inserts the
    /// sender's node id with the current `Instant` into the map, so the
    /// dependency-health checker observes reachability through the same
    /// in-process state under either transport.
    #[must_use]
    pub fn with_peer_liveness(
        mut self,
        peer_liveness: crate::dependency_health::PeerLivenessMap,
    ) -> Self {
        self.peer_liveness = Some(peer_liveness);
        self
    }

    /// Returns the underlying manager handle. Used by E.6b.5 wiring.
    #[must_use]
    pub fn manager(&self) -> &Arc<RaftManager> {
        &self.manager
    }

    /// Drive a single inbound `Replicate` bidi stream to completion.
    ///
    /// Called from the dispatcher's per-stream task when an incoming bidi
    /// stream's first frame carries [`OPCODE_RAFT_PROTOCOL_HANDSHAKE`]. The
    /// dispatch loop has already read the first frame off the wire and
    /// passes its payload as `first_frame` so the handshake can be verified
    /// without re-reading. This function:
    ///
    /// 1. Verifies `first_frame` carries a 4-byte big-endian [`RAFT_PROTOCOL_VERSION`]. On mismatch
    ///    the response carries the local version and the connection drops; the peer-sender's outer
    ///    reconnect loop will back off accordingly.
    /// 2. Writes the local handshake response (echoing `RAFT_PROTOCOL_VERSION`).
    /// 3. Loops on [`read_frame`]: each frame's payload decodes to a [`ConsensusEnvelope`], routes
    ///    through [`handle_consensus_envelope`] into the consensus engine, and triggers an empty
    ///    `ConsensusAck` reply frame.
    /// 4. Exits on cancel / read failure / write failure. Acks are fire-and-forget.
    ///
    /// Errors are surfaced as [`TransportError`] for ergonomic propagation; the
    /// caller (the dispatcher's per-stream task) only logs.
    pub async fn handle_replicate_stream(
        &self,
        mut send: quinn::SendStream,
        mut recv: quinn::RecvStream,
        first_frame: Bytes,
        cancel: CancellationToken,
    ) -> Result<(), TransportError> {
        // --- Handshake ----------------------------------------------------
        // The dispatcher has already read the first frame and verified its
        // opcode is `OPCODE_RAFT_PROTOCOL_HANDSHAKE`; we just need to check
        // the payload before responding.
        if first_frame.len() != 4 {
            // Send our version anyway so a buggy peer surfaces a clear
            // mismatch rather than a hung stream, then close.
            let _ = self.write_handshake_response(&mut send).await;
            return Err(TransportError::ReadFrame {
                reason: format!("raft handshake payload size {} != 4", first_frame.len()),
            });
        }
        let mut bytes = [0u8; 4];
        bytes.copy_from_slice(&first_frame[..4]);
        let peer_version = u32::from_be_bytes(bytes);

        // Allow cancellation to short-circuit the handshake response — every
        // I/O point past the read is an explicit yield.
        if cancel.is_cancelled() {
            return Ok(());
        }

        // Always reply with our version so the peer's drain loop can decide
        // whether to retry or stay disconnected.
        self.write_handshake_response(&mut send).await?;

        if peer_version != RAFT_PROTOCOL_VERSION {
            tracing::warn!(
                peer_version,
                local_version = RAFT_PROTOCOL_VERSION,
                "wire_raft_server: protocol version mismatch on Replicate stream; closing",
            );
            // Best-effort flush of the response frame before tear-down so the
            // peer observes our local version (and can decide whether to
            // back off). `finish()` errors are swallowed at debug — the
            // stream is already in shutdown.
            if let Err(err) = send.finish() {
                tracing::debug!(error = %err, "wire_raft_server: send.finish() during version mismatch");
            }
            return Err(TransportError::ReadFrame {
                reason: format!(
                    "raft protocol version mismatch: local {RAFT_PROTOCOL_VERSION}, peer {peer_version}"
                ),
            });
        }

        // --- Drain envelopes ----------------------------------------------
        // Each envelope frame triggers a consensus dispatch + ack reply.
        // Errors decoding / dispatching a single envelope log + drop —
        // matches the legacy tonic `RaftService::replicate` semantics, where
        // an error reply was a `ConsensusAck` regardless of dispatch
        // outcome. We mirror that by always writing a fire-and-forget ack
        // frame on the wire even when dispatch returns Err.
        loop {
            let frame = tokio::select! {
                biased;
                () = cancel.cancelled() => return Ok(()),
                res = read_frame(&mut recv) => match res {
                    Ok(f) => f,
                    Err(err) => {
                        // EOF / peer reset / decode error — close cleanly.
                        tracing::debug!(error = %err, "wire_raft_server: replicate stream read closed");
                        return Ok(());
                    }
                },
            };

            if frame.header.opcode != OPCODE_REPLICATE {
                tracing::debug!(
                    opcode = frame.header.opcode,
                    "wire_raft_server: unexpected opcode on replicate stream; closing",
                );
                return Ok(());
            }

            // Decode the envelope payload. A decode failure on a single
            // frame is logged and the stream continues — a corrupt frame
            // shouldn't tear down a healthy stream.
            let envelope: ConsensusEnvelope = match postcard::from_bytes(&frame.payload) {
                Ok(env) => env,
                Err(err) => {
                    tracing::debug!(
                        error = %err,
                        payload_bytes = frame.payload.len(),
                        "wire_raft_server: failed to decode ConsensusEnvelope; ack-and-continue",
                    );
                    self.write_ack_frame(&mut send).await?;
                    continue;
                },
            };

            tracing::debug!(
                from_node = envelope.from_node,
                shard_id = envelope.shard_id,
                payload_bytes = envelope.payload.len(),
                "wire_raft_server: received envelope",
            );

            // Capture the sender's node id before `handle_consensus_envelope`
            // consumes the envelope. Updating `peer_liveness` requires the
            // ID even on a dispatch error (a delivered-but-rejected message
            // is still evidence the peer is reachable; matches the gRPC
            // path's behaviour).
            let from_node = envelope.from_node;

            if let Err(err) = handle_consensus_envelope(&self.manager, envelope).await {
                tracing::debug!(
                    error_code = ?err.code,
                    error_message = %err.message,
                    "wire_raft_server: dispatch failed",
                );
            }

            // Record peer liveness for the sender. Mirrors the gRPC
            // `RaftService::replicate` path so the dependency-health
            // checker's reachability probe observes the same in-process
            // signal under either transport.
            if let Some(ref liveness) = self.peer_liveness {
                liveness.write().insert(from_node, std::time::Instant::now());
            }

            self.write_ack_frame(&mut send).await?;
        }
    }

    /// Write the handshake response frame carrying [`RAFT_PROTOCOL_VERSION`].
    async fn write_handshake_response(
        &self,
        send: &mut quinn::SendStream,
    ) -> Result<(), TransportError> {
        let payload = Bytes::copy_from_slice(&RAFT_PROTOCOL_VERSION.to_be_bytes());
        let mut header = FrameHeader::default();
        header.opcode = OPCODE_RAFT_PROTOCOL_HANDSHAKE;
        header.version = CURRENT_PROTOCOL_VERSION;
        header.flags = FLAG_IS_RESPONSE;
        header.payload_length = 4;
        let frame = Frame { header, payload };
        write_frame(send, &frame).await
    }

    /// Write an empty `ConsensusAck` frame as a liveness signal back to the
    /// sender. Mirrors the test fixture in
    /// [`super::peer_sender`]'s `spawn_replicate_server` helper exactly.
    async fn write_ack_frame(&self, send: &mut quinn::SendStream) -> Result<(), TransportError> {
        let mut header = FrameHeader::default();
        header.opcode = OPCODE_REPLICATE;
        header.version = CURRENT_PROTOCOL_VERSION;
        header.flags = FLAG_IS_RESPONSE;
        header.payload_length = 0;
        let frame = Frame { header, payload: Bytes::new() };
        write_frame(send, &frame).await
    }
}

// ---------------------------------------------------------------------------
// Wire-trait impl
// ---------------------------------------------------------------------------

/// Server-trait impl emitted by the wire-services macro for `RaftService`.
///
/// Two of the four RPCs have real bodies: `committed_index` and
/// `regional_proposal` are unary RPCs invoked through the standard dispatcher
/// path (E.6b.5). The other two — `replicate` and `install_snapshot_stream` —
/// are routed via the dispatcher's special-case branches (the bidi handshake
/// for the former, [`SnapshotHandler`] for the latter); their wire-trait
/// methods return `Internal` and should never be reached in production.
impl inferadb_ledger_wire_services::RaftService for WireRaftServerHandler {
    async fn committed_index(
        &self,
        request: CommittedIndexRequest,
        _ctx: RequestContext,
    ) -> Result<CommittedIndexResponse, WireError> {
        committed_index_impl(&self.manager, request).await
    }

    async fn replicate(
        &self,
        _request: ConsensusEnvelope,
        _ctx: RequestContext,
    ) -> Result<ConsensusAck, WireError> {
        // Routed via the dispatcher's `OPCODE_RAFT_PROTOCOL_HANDSHAKE` branch
        // into [`Self::handle_replicate_stream`]; reaching this path means
        // an unknown caller invoked the unary placeholder directly.
        Err(WireError {
            code: ErrorCode::Internal,
            message: "replicate is routed via OPCODE_RAFT_PROTOCOL_HANDSHAKE bidi handler"
                .to_owned(),
            retryable: false,
            retry_after_ms: 0,
            context: Default::default(),
            suggested_action: "open a Replicate bidi stream beginning with the handshake frame"
                .to_owned(),
        })
    }

    async fn regional_proposal(
        &self,
        request: RegionalProposalRequest,
        _ctx: RequestContext,
    ) -> Result<RegionalProposalResult, WireError> {
        regional_proposal_impl(&self.manager, request).await
    }

    async fn install_snapshot_stream(
        &self,
        _request: inferadb_ledger_wire::services::raft::InstallSnapshotChunk,
        _ctx: RequestContext,
    ) -> Result<InstallSnapshotStreamResponse, WireError> {
        // Routed via the dispatcher's `OPCODE_INSTALL_SNAPSHOT_STREAM` branch
        // into [`SnapshotHandler::on_snapshot_complete`]; reaching this path
        // means an unknown caller invoked the unary placeholder directly.
        Err(WireError {
            code: ErrorCode::Internal,
            message: "install_snapshot_stream is routed via the snapshot sub-protocol".to_owned(),
            retryable: false,
            retry_after_ms: 0,
            context: Default::default(),
            suggested_action: "open an InstallSnapshotStream bidi stream via SnapshotSender"
                .to_owned(),
        })
    }
}

// ---------------------------------------------------------------------------
// SnapshotHandler impl
// ---------------------------------------------------------------------------

impl SnapshotHandler for WireRaftServerHandler {
    fn staging_dir(&self, header: &InstallSnapshotHeader) -> PathBuf {
        // Resolve `(region, scope)` from the wire header and delegate to
        // the persister's path layout so streamed staging files land in the
        // same directory the install path discovers them through
        // `SnapshotPersister::list_staged`. A header that carries no scope
        // (defensive: production senders always populate one) falls back to
        // the configured root with no per-scope subdir — the receiver will
        // fail downstream with `ScopeMismatch`, which is the right
        // observation point.
        match resolve_scope(header) {
            Some((region, scope)) => self.manager.snapshot_persister().scope_dir(region, scope),
            None => {
                tracing::warn!(
                    snapshot_index = header.snapshot_index,
                    "wire_raft_server: snapshot header carried no scope; using fallback dir",
                );
                // Best-effort: persister scope_dir is the canonical layout;
                // without a scope we can't compute it, so fall back to the
                // GLOBAL/org-0 directory and let downstream validation fail.
                self.manager.snapshot_persister().scope_dir(
                    Region::GLOBAL,
                    SnapshotScope::Org { organization_id: OrganizationId::new(0) },
                )
            },
        }
    }

    fn on_snapshot_complete(&self, header: &InstallSnapshotHeader, staged_path: &std::path::Path) {
        // Production install path is reactor-driven via `RaftManagerSnapshotInstaller`
        // (Rule 22): the consensus engine's `Action::InstallSnapshot` dispatch
        // resolves the staged file via `SnapshotPersister::list_staged` and
        // applies it through `RaftLogStore::install_snapshot`. The hook
        // here is purely observational.
        tracing::info!(
            snapshot_index = header.snapshot_index,
            snapshot_term = header.snapshot_term,
            staged_path = %staged_path.display(),
            "wire_raft_server: snapshot stream completed",
        );
        if let Some(hook) = self.on_snapshot_complete.as_ref() {
            hook(header, staged_path);
        }
    }
}

// ---------------------------------------------------------------------------
// ReplicateHandler impl
// ---------------------------------------------------------------------------

/// Trait-object hook for the wire transport's
/// [`OPCODE_RAFT_PROTOCOL_HANDSHAKE`] dispatch branch. Delegates to the
/// inherent [`WireRaftServerHandler::handle_replicate_stream`].
#[async_trait::async_trait]
impl ReplicateHandler for WireRaftServerHandler {
    async fn handle_replicate_stream(
        &self,
        send: quinn::SendStream,
        recv: quinn::RecvStream,
        first_frame: Bytes,
        cancel: CancellationToken,
    ) -> Result<(), TransportError> {
        WireRaftServerHandler::handle_replicate_stream(self, send, recv, first_frame, cancel).await
    }
}

// ---------------------------------------------------------------------------
// Implementation helpers
// ---------------------------------------------------------------------------

/// Translate the wire `InstallSnapshotHeader.scope` into the persister's
/// `(Region, SnapshotScope)` tuple. Kept minimal here because the wire
/// path doesn't need to surface a typed receiver error — the wire transport's
/// `run_snapshot_receive_loop` already handles framing / CRC / byte-count
/// validation before this hook fires.
fn resolve_scope(header: &InstallSnapshotHeader) -> Option<(Region, SnapshotScope)> {
    let scope = header.scope.as_ref()?;
    match scope {
        InstallSnapshotScope::Org(org) => {
            let region = parse_region(&org.region)?;
            Some((
                region,
                SnapshotScope::Org { organization_id: OrganizationId::new(org.organization_id) },
            ))
        },
        InstallSnapshotScope::Vault(v) => {
            let region = parse_region(&v.region)?;
            Some((
                region,
                SnapshotScope::Vault {
                    organization_id: OrganizationId::new(v.organization_id),
                    vault_id: VaultId::new(v.vault_id),
                },
            ))
        },
    }
}

fn parse_region(raw: &str) -> Option<Region> {
    if raw.is_empty() {
        return Some(Region::GLOBAL);
    }
    use std::str::FromStr;
    Region::from_str(raw).ok()
}

/// Wire-side counterpart to the legacy `services::raft::handle_consensus_message`.
///
/// Resolves the target shard, validates the sender is a known cluster
/// member (with the same sole-voter / non-member fall-throughs as the legacy
/// path), decodes the postcard `Message` payload, and routes it into the
/// consensus engine via `engine_arc().peer_message`. Returns a
/// [`WireError`] on any failure so the caller can log + drop.
///
/// Wire-vs-tonic notes:
/// - The wire path auto-registers the sender's transport via
///   [`super::WireConsensusTransport::set_peer_via_registry`] when the peer is not already known —
///   matches the legacy tonic [`crates/services/src/services/raft.rs`] behaviour. Without this the
///   wire transport silently drops every `AppendEntriesResponse` because there is no return channel
///   registered: the `set_peer` ingress path requires the address to be known *before* any inbound
///   envelope arrives, but in a healthy multi-node bootstrap the joining node's first contact IS
///   the inbound envelope. The peer-address map is updated alongside for symmetry with the tonic
///   path so cross-transport bootstraps see the same peer-address state.
/// - Peer-liveness tracking is a follow-up; the tonic path threads an `Arc<RwLock<HashMap>>` for
///   that. The wire path can adopt the same shape once the registry migration (E.6c) lands.
async fn handle_consensus_envelope(
    manager: &Arc<RaftManager>,
    envelope: ConsensusEnvelope,
) -> Result<(), WireError> {
    use std::str::FromStr;

    // Resolve the target region (None / empty → GLOBAL).
    let region = match envelope.region.as_deref() {
        None | Some("") => Region::GLOBAL,
        Some(slug) => Region::from_str(slug).map_err(|err| WireError {
            code: ErrorCode::InvalidArgument,
            message: format!("invalid region {slug:?}: {err}"),
            retryable: false,
            retry_after_ms: 0,
            context: Default::default(),
            suggested_action: "send a recognised region slug".to_owned(),
        })?,
    };

    // B.1.7 multi-engine routing: each per-organization Raft group runs
    // its own `ConsensusEngine`; route by `shard_id` rather than by region.
    // Falls back to the legacy data-region group when the shard isn't
    // found locally.
    let consensus_shard = ConsensusStateId(envelope.shard_id);
    let group = manager
        .lookup_by_consensus_shard(consensus_shard)
        .or_else(|| manager.lookup_region_inner(region))
        .ok_or_else(|| WireError {
            code: ErrorCode::NotFound,
            message: "region group not found".to_owned(),
            retryable: false,
            retry_after_ms: 0,
            context: Default::default(),
            suggested_action: "wait for region bootstrap to complete".to_owned(),
        })?;

    // Membership validation. Skip when:
    // - this node is the sole voter (freshly bootstrapped), or
    // - this node is not in the voter / learner set yet (joining node receiving its first
    //   membership update).
    let from_node = NodeId(envelope.from_node);
    let local_node = NodeId(group.handle().node_id());
    let state = group.handle().shard_state();
    let is_sole_voter = state.voters.len() == 1 && state.voters.contains(&local_node);
    let is_non_member =
        !state.voters.contains(&local_node) && !state.learners.contains(&local_node);
    if !is_sole_voter
        && !is_non_member
        && !state.voters.contains(&from_node)
        && !state.learners.contains(&from_node)
    {
        return Err(WireError {
            code: ErrorCode::PermissionDenied,
            message: format!("node {} is not a member of the cluster", envelope.from_node),
            retryable: false,
            retry_after_ms: 0,
            context: Default::default(),
            suggested_action: "verify cluster membership before sending consensus traffic"
                .to_owned(),
        });
    }

    // Auto-register the sender's transport channel if not yet known. This
    // enables the return path: when the leader sends AppendEntries to a
    // joining node, the joining node needs a channel back to the leader for
    // AppendEntriesResponse. Mirrors the legacy tonic path
    // (`crates/services/src/services/raft.rs`); without this the wire
    // transport silently drops every response because no return channel is
    // registered.
    if !envelope.from_address.is_empty() {
        if let Some(transport) = group.consensus_transport()
            && !transport.peers().contains(&envelope.from_node)
            && let Err(e) =
                transport.set_peer_via_registry(envelope.from_node, &envelope.from_address).await
        {
            tracing::warn!(
                from_node = envelope.from_node,
                from_address = %envelope.from_address,
                error = %e,
                "Failed to auto-register sender via registry",
            );
        }
        // Also update the peer address map so future lookups work.
        manager.peer_addresses().insert(envelope.from_node, envelope.from_address.clone());
    }

    // Decode + dispatch.
    let message: inferadb_ledger_consensus::Message = postcard::from_bytes(&envelope.payload)
        .map_err(|err| WireError {
            code: ErrorCode::InvalidArgument,
            message: format!("decode consensus message: {err}"),
            retryable: false,
            retry_after_ms: 0,
            context: Default::default(),
            suggested_action: "check sender's serializer version".to_owned(),
        })?;

    group
        .handle()
        .engine_arc()
        .peer_message(consensus_shard, NodeId(envelope.from_node), message)
        .await
        .map_err(|err| WireError {
            code: ErrorCode::Internal,
            message: format!("consensus dispatch: {err}"),
            retryable: true,
            retry_after_ms: 0,
            context: Default::default(),
            suggested_action: "engine state error; will retry on next heartbeat".to_owned(),
        })?;

    Ok(())
}

/// `committed_index` body shared between the wire-trait method + tests.
async fn committed_index_impl(
    manager: &Arc<RaftManager>,
    req: CommittedIndexRequest,
) -> Result<CommittedIndexResponse, WireError> {
    use std::str::FromStr;

    let region = if req.region.is_empty() {
        Region::GLOBAL
    } else {
        Region::from_str(&req.region).map_err(|err| WireError {
            code: ErrorCode::InvalidArgument,
            message: format!("invalid region {:?}: {err}", req.region),
            retryable: false,
            retry_after_ms: 0,
            context: Default::default(),
            suggested_action: "send a recognised region slug".to_owned(),
        })?
    };

    // Vault scope is all-or-nothing: a partial scope is a protocol bug.
    let vault_scope = match (req.organization, req.vault) {
        (Some(org), Some(vault)) => Some((org, vault)),
        (None, None) => None,
        _ => {
            return Err(WireError {
                code: ErrorCode::InvalidArgument,
                message: "organization and vault must both be set or both omitted".to_owned(),
                retryable: false,
                retry_after_ms: 0,
                context: Default::default(),
                suggested_action: "include both organization and vault slugs together".to_owned(),
            });
        },
    };

    // The legacy tonic path performs SlugResolver lookups against the
    // GLOBAL applied state to translate `OrganizationSlug`/`VaultSlug` →
    // internal IDs, then handles per-vault leadership / lease / read-index.
    // Per the rule that slug↔ID translation lives at the gRPC boundary,
    // this wire-side path does the same — but the resolver lives in
    // `crates/services/`, which the raft crate cannot depend on without a
    // cycle. Until the dispatcher wiring lands (E.6b.5), surface a clear
    // unimplemented vault-scope error so the SDK falls back to the
    // region-scope path or the legacy tonic transport. Region scope is
    // fully implemented below.
    if let Some((_org, _vault)) = vault_scope {
        // The legacy tonic path performs SlugResolver lookups against the
        // GLOBAL applied state to translate `OrganizationSlug`/`VaultSlug` →
        // internal IDs, then handles per-vault leadership / lease /
        // read-index. The resolver lives in `crates/services/`, which the
        // raft crate cannot depend on without a cycle. Until the dispatcher
        // wiring lands (E.6b.5), surface a clear `Internal` error so the
        // SDK falls back to region-scope reads or the legacy tonic path.
        return Err(WireError {
            code: ErrorCode::Internal,
            message:
                "wire CommittedIndex vault scope is wired in E.6b.5 (dispatcher composition); \
                 use region-scope reads or the gRPC RaftService until then"
                    .to_owned(),
            retryable: false,
            retry_after_ms: 0,
            context: Default::default(),
            suggested_action: "set both organization and vault to None for region-scope reads"
                .to_owned(),
        });
    }

    // Region scope (no vault scope provided): resolve the region group,
    // require leadership, and answer with `(commit_index, term)` from the
    // shard handle — fast path on a valid lease, slow path via
    // `engine_read_index` (quorum heartbeat) when the lease has expired.
    let group = manager.get_region_group(region).map_err(|_| WireError {
        code: ErrorCode::NotFound,
        message: "region group not found".to_owned(),
        retryable: false,
        retry_after_ms: 0,
        context: Default::default(),
        suggested_action: "wait for region bootstrap to complete".to_owned(),
    })?;

    let handle = group.handle();
    if !handle.is_leader() {
        return Err(WireError {
            code: ErrorCode::StaleRouting,
            message: "not the leader".to_owned(),
            retryable: true,
            retry_after_ms: 0,
            context: Default::default(),
            suggested_action: "redirect to the current region leader".to_owned(),
        });
    }

    if group.leader_lease().is_valid() {
        return Ok(CommittedIndexResponse {
            committed_index: handle.commit_index(),
            leader_term: handle.current_term(),
        });
    }

    let committed_index = handle.engine_read_index().await.map_err(|err| WireError {
        code: ErrorCode::StaleRouting,
        message: format!("CommittedIndex quorum check failed: {err}"),
        retryable: true,
        retry_after_ms: 0,
        context: Default::default(),
        suggested_action: "retry after a backoff".to_owned(),
    })?;

    Ok(CommittedIndexResponse { committed_index, leader_term: handle.current_term() })
}

/// `regional_proposal` body shared between the wire-trait method + tests.
async fn regional_proposal_impl(
    manager: &Arc<RaftManager>,
    req: RegionalProposalRequest,
) -> Result<RegionalProposalResult, WireError> {
    use std::str::FromStr;

    let region = match req.region.as_deref() {
        None | Some("") => Region::GLOBAL,
        Some(slug) => Region::from_str(slug).map_err(|err| WireError {
            code: ErrorCode::InvalidArgument,
            message: format!("invalid region {slug:?}: {err}"),
            retryable: false,
            retry_after_ms: 0,
            context: Default::default(),
            suggested_action: "send a recognised region slug".to_owned(),
        })?,
    };

    // Saga sends pre-serialized `postcard(RaftPayload<R>)` bytes; forward
    // them directly into the data-region group at OrganizationId(0).
    let group =
        manager.get_organization_group(region, OrganizationId::new(0)).map_err(|_| WireError {
            code: ErrorCode::NotFound,
            message: "region group not found".to_owned(),
            retryable: false,
            retry_after_ms: 0,
            context: Default::default(),
            suggested_action: "wait for region bootstrap to complete".to_owned(),
        })?;

    let timeout = std::time::Duration::from_millis(u64::from(req.timeout_ms));
    let request_payload = req.request_payload.to_vec();

    match group.handle().propose_bytes_and_wait(request_payload, timeout).await {
        Ok(response) => {
            let response_bytes = postcard::to_allocvec(&response).map_err(|err| WireError {
                code: ErrorCode::Internal,
                message: format!("serialize response: {err}"),
                retryable: false,
                retry_after_ms: 0,
                context: Default::default(),
                suggested_action: "report bug".to_owned(),
            })?;
            let committed_index = group.handle().commit_index();
            Ok(RegionalProposalResult {
                response_payload: Bytes::from(response_bytes),
                status_code: 0,
                error_message: String::new(),
                committed_index,
            })
        },
        // Surface the propose failure inline (matches the legacy tonic path,
        // which also embedded the apply-pipeline error in the result rather
        // than failing the RPC).
        Err(err) => Ok(RegionalProposalResult {
            response_payload: Bytes::new(),
            status_code: 13, // Internal in tonic::Code; preserved for parity.
            error_message: err.to_string(),
            committed_index: 0,
        }),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]
mod tests {
    use std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        path::Path,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use bytes::Bytes;
    use inferadb_ledger_consensus::Message;
    use inferadb_ledger_test_utils::TestDir;
    use inferadb_ledger_types::{Region, encode};
    use inferadb_ledger_wire::{
        Frame, FrameHeader, OPCODE_RAFT_PROTOCOL_HANDSHAKE, RAFT_PROTOCOL_VERSION, RequestContext,
        services::raft::{
            CommittedIndexRequest, ConsensusEnvelope, InstallSnapshotHeader,
            InstallSnapshotOrgScope, InstallSnapshotScope, InstallSnapshotVaultScope,
            RegionalProposalRequest,
        },
    };
    use inferadb_ledger_wire_transport::{
        frame_io::{FLAG_IS_RESPONSE, read_frame, write_frame},
        snapshot_stream::SnapshotHandler,
        tls,
    };
    use parking_lot::Mutex;
    use tokio_util::sync::CancellationToken;

    use super::{
        OPCODE_REPLICATE, WireRaftServerHandler, committed_index_impl, handle_consensus_envelope,
        regional_proposal_impl,
    };
    use crate::{
        RaftManager, RaftManagerConfig, RegionConfig, node_registry::NodeConnectionRegistry,
    };

    // ── Fixtures ─────────────────────────────────────────────────────────────

    fn make_manager() -> (Arc<RaftManager>, TestDir) {
        let temp = TestDir::new();
        let config = RaftManagerConfig::new(temp.path().to_path_buf(), 1, Region::GLOBAL);
        let manager = Arc::new(RaftManager::new(config, Arc::new(NodeConnectionRegistry::new())));
        (manager, temp)
    }

    async fn make_manager_with_region() -> (Arc<RaftManager>, TestDir) {
        let temp = TestDir::new();
        let config = RaftManagerConfig::new(temp.path().to_path_buf(), 1, Region::GLOBAL);
        let manager = Arc::new(RaftManager::new(config, Arc::new(NodeConnectionRegistry::new())));
        let region_config =
            RegionConfig::system(1, "127.0.0.1:50051".to_string()).without_background_jobs();
        manager.start_system_region(region_config).await.expect("start system region");
        (manager, temp)
    }

    fn loopback_zero() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)
    }

    fn build_test_endpoints() -> (quinn::Endpoint, quinn::Endpoint) {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let cert_der = cert.cert.der().clone();
        let key_der = rustls::pki_types::PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der());
        let chain = vec![cert_der];
        let key = rustls::pki_types::PrivateKeyDer::Pkcs8(key_der);
        let server_crypto = tls::rustls_server_crypto(chain, key).expect("server crypto");
        let server_config = tls::server_config(server_crypto);
        let server = quinn::Endpoint::server(server_config, loopback_zero()).unwrap();

        let client_crypto = tls::rustls_client_crypto_skip_verify();
        let client_config = tls::client_config(client_crypto);
        let mut client = quinn::Endpoint::client(loopback_zero()).unwrap();
        client.set_default_client_config(client_config);

        (server, client)
    }

    fn build_handshake_request_frame(version: u32) -> Frame {
        let payload = Bytes::copy_from_slice(&version.to_be_bytes());
        let mut header = FrameHeader::default();
        header.opcode = OPCODE_RAFT_PROTOCOL_HANDSHAKE;
        header.version = inferadb_ledger_wire::CURRENT_PROTOCOL_VERSION;
        header.payload_length = 4;
        Frame { header, payload }
    }

    fn build_envelope_request_frame(envelope: &ConsensusEnvelope) -> Frame {
        let payload = Bytes::from(postcard::to_allocvec(envelope).expect("encode envelope"));
        let mut header = FrameHeader::default();
        header.opcode = OPCODE_REPLICATE;
        header.version = inferadb_ledger_wire::CURRENT_PROTOCOL_VERSION;
        header.payload_length = u32::try_from(payload.len()).unwrap();
        Frame { header, payload }
    }

    /// Spawns a tiny QUIC server that accepts ONE connection and ONE bidi
    /// stream, reads the inbound handshake frame off the wire (mirroring
    /// what the production dispatch loop does at
    /// `crates/wire-transport/src/server/dispatch.rs`'s
    /// `OPCODE_RAFT_PROTOCOL_HANDSHAKE` branch), then drives
    /// `handler.handle_replicate_stream` against it. Returns the server
    /// addr + a join handle resolving to the handler's
    /// `Result<(), TransportError>`. The connection is held open until the
    /// client closes it — without that, dropping the connection from the
    /// handler-finished branch would RST the bidi stream before the
    /// in-flight QUIC datagrams flush, which the test reads would then
    /// observe as `connection lost`.
    async fn spawn_handler_server(
        handler: Arc<WireRaftServerHandler>,
    ) -> (SocketAddr, tokio::task::JoinHandle<Result<(), super::TransportError>>) {
        let (server, _client_unused) = build_test_endpoints();
        let server_addr = server.local_addr().unwrap();
        let join = tokio::spawn(async move {
            let incoming = server.accept().await.expect("incoming");
            let connection = incoming.await.expect("handshake");
            let (send, mut recv) = connection.accept_bi().await.expect("accept_bi");
            // Mirror the dispatch loop: read the first frame off the wire
            // to extract the handshake payload before handing the streams
            // to the handler.
            let first = read_frame(&mut recv).await.expect("read first frame");
            let cancel = CancellationToken::new();
            let outcome = handler.handle_replicate_stream(send, recv, first.payload, cancel).await;
            // Park on the connection until the client tears it down so any
            // in-flight bytes the handler emitted (including a final
            // handshake-mismatch response) drain to the peer before the
            // connection drops. The test side is responsible for driving
            // close; this wait is bounded by the test's outer timeout.
            let _ = connection.closed().await;
            drop(server);
            outcome
        });
        (server_addr, join)
    }

    /// Open a client bidi stream against `server_addr`. Returns the client
    /// endpoint + connection + (send, recv) halves.
    async fn open_client_bidi(
        server_addr: SocketAddr,
    ) -> (quinn::Endpoint, quinn::Connection, quinn::SendStream, quinn::RecvStream) {
        let crypto = tls::rustls_client_crypto_skip_verify();
        let client_config = tls::client_config(crypto);
        let mut client = quinn::Endpoint::client(loopback_zero()).unwrap();
        client.set_default_client_config(client_config);

        let connection = client.connect(server_addr, "localhost").unwrap().await.unwrap();
        let (send, recv) = connection.open_bi().await.unwrap();
        (client, connection, send, recv)
    }

    fn sample_ctx() -> RequestContext {
        use inferadb_ledger_wire::{AuthMethod, CallerIdentity, ConnectionMetrics};
        RequestContext {
            request_id: 0,
            idempotency_token: [0u8; 16],
            trace_id: [0u8; 16],
            span_id: [0u8; 8],
            trace_flags: 0,
            causality_commit_index: 0,
            deadline: None,
            caller_identity: CallerIdentity {
                user_id: None,
                app_id: None,
                organization_id: None,
                auth_method: AuthMethod::Anonymous,
                token_expiry: std::time::Instant::now() + Duration::from_secs(60),
                revocation_epoch: 0,
            },
            peer_addr: "127.0.0.1:0".parse().unwrap(),
            sdk_version: None,
            forwarded_for: None,
            correlation_id: 0,
            cancellation: CancellationToken::new(),
            conn_metrics: Arc::new(ConnectionMetrics),
        }
    }

    // ── Construction ─────────────────────────────────────────────────────────

    #[test]
    fn new_handler_has_no_hook() {
        let (manager, _temp) = make_manager();
        let handler = WireRaftServerHandler::new(manager);
        assert!(handler.on_snapshot_complete.is_none());
    }

    #[test]
    fn with_hook_installs_hook() {
        let (manager, _temp) = make_manager();
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_for_hook = Arc::clone(&counter);
        let handler = WireRaftServerHandler::new(manager).with_snapshot_complete_hook(Arc::new(
            move |_h, _p| {
                counter_for_hook.fetch_add(1, Ordering::Relaxed);
            },
        ));
        assert!(handler.on_snapshot_complete.is_some());
        assert_eq!(counter.load(Ordering::Relaxed), 0);
    }

    // ── handle_replicate_stream: handshake ───────────────────────────────────

    #[tokio::test]
    async fn handshake_replies_with_matching_version() {
        let (manager, _temp) = make_manager();
        let handler = Arc::new(WireRaftServerHandler::new(manager));
        let (server_addr, join) = spawn_handler_server(Arc::clone(&handler)).await;

        let (_client, _conn, mut send, mut recv) = open_client_bidi(server_addr).await;

        // Send our handshake.
        let frame = build_handshake_request_frame(RAFT_PROTOCOL_VERSION);
        write_frame(&mut send, &frame).await.unwrap();

        // Server replies.
        let response = read_frame(&mut recv).await.unwrap();
        assert_eq!(response.header.opcode, OPCODE_RAFT_PROTOCOL_HANDSHAKE);
        assert_eq!(response.header.flags & FLAG_IS_RESPONSE, FLAG_IS_RESPONSE);
        assert_eq!(response.header.payload_length, 4);
        let mut bytes = [0u8; 4];
        bytes.copy_from_slice(&response.payload[..4]);
        assert_eq!(u32::from_be_bytes(bytes), RAFT_PROTOCOL_VERSION);

        // Cleanly close client to let the server's loop exit.
        drop(send);
        drop(recv);
        let _ = tokio::time::timeout(Duration::from_secs(2), join).await;
    }

    #[tokio::test]
    async fn handshake_rejects_wrong_version() {
        let (manager, _temp) = make_manager();
        let handler = Arc::new(WireRaftServerHandler::new(manager));
        let (server_addr, join) = spawn_handler_server(Arc::clone(&handler)).await;

        let (client, conn, mut send, mut recv) = open_client_bidi(server_addr).await;

        // Send a handshake with a deliberately bad version.
        let bogus = RAFT_PROTOCOL_VERSION.wrapping_add(99);
        let frame = build_handshake_request_frame(bogus);
        write_frame(&mut send, &frame).await.unwrap();

        // Server still replies with our local version (per protocol — peer
        // observes the mismatch and decides whether to retry).
        let response = read_frame(&mut recv).await.unwrap();
        assert_eq!(response.header.opcode, OPCODE_RAFT_PROTOCOL_HANDSHAKE);
        let mut bytes = [0u8; 4];
        bytes.copy_from_slice(&response.payload[..4]);
        assert_eq!(u32::from_be_bytes(bytes), RAFT_PROTOCOL_VERSION);

        // Drive the close from the client side so the server's
        // `connection.closed()` parking resolves and the handler's
        // outcome surfaces.
        drop(send);
        drop(recv);
        conn.close(0u32.into(), b"test done");
        client.wait_idle().await;

        let outcome = tokio::time::timeout(Duration::from_secs(5), join).await.unwrap().unwrap();
        assert!(outcome.is_err(), "expected handler to error on version mismatch");
        let err_str = format!("{:?}", outcome.unwrap_err());
        assert!(
            err_str.contains("version mismatch") || err_str.contains("protocol"),
            "expected version-mismatch in error: {err_str}",
        );
    }

    // ── handle_replicate_stream: envelope drain + acks ───────────────────────

    #[tokio::test]
    async fn replicate_stream_decodes_envelopes_and_acks() {
        // Use a manager whose region has been started so the membership
        // path resolves (sole-voter accepts any sender; payload that doesn't
        // decode as Message is rejected as InvalidArgument and the server
        // still acks).
        let (manager, _temp) = make_manager_with_region().await;
        let handler = Arc::new(WireRaftServerHandler::new(manager));
        let (server_addr, join) = spawn_handler_server(Arc::clone(&handler)).await;

        let (_client, _conn, mut send, mut recv) = open_client_bidi(server_addr).await;

        // Handshake.
        write_frame(&mut send, &build_handshake_request_frame(RAFT_PROTOCOL_VERSION))
            .await
            .unwrap();
        let _ = read_frame(&mut recv).await.unwrap();

        // Send N envelopes (well-formed Message::TimeoutNow). Each must
        // produce one ack frame. The dispatch may succeed or surface an
        // engine error inline; either way the ack lands.
        const N: usize = 5;
        for i in 0..N {
            let payload = encode(&Message::TimeoutNow).unwrap();
            let envelope = ConsensusEnvelope {
                shard_id: 0,
                from_node: 1,
                region: Some(Region::GLOBAL.as_str().to_string()),
                payload: Bytes::from(payload),
                from_address: format!("127.0.0.1:{}", 5000 + i),
                cluster_id: 0,
            };
            write_frame(&mut send, &build_envelope_request_frame(&envelope)).await.unwrap();
        }

        // Read N acks.
        for i in 0..N {
            let ack = tokio::time::timeout(Duration::from_secs(5), read_frame(&mut recv))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(ack.header.opcode, OPCODE_REPLICATE, "ack #{i} opcode");
            assert_eq!(ack.header.flags & FLAG_IS_RESPONSE, FLAG_IS_RESPONSE, "ack #{i} flag");
            assert_eq!(ack.header.payload_length, 0, "ack #{i} payload");
        }

        drop(send);
        drop(recv);
        let _ = tokio::time::timeout(Duration::from_secs(2), join).await;
    }

    #[tokio::test]
    async fn replicate_stream_acks_corrupt_envelope() {
        let (manager, _temp) = make_manager_with_region().await;
        let handler = Arc::new(WireRaftServerHandler::new(manager));
        let (server_addr, join) = spawn_handler_server(Arc::clone(&handler)).await;

        let (_client, _conn, mut send, mut recv) = open_client_bidi(server_addr).await;

        // Handshake.
        write_frame(&mut send, &build_handshake_request_frame(RAFT_PROTOCOL_VERSION))
            .await
            .unwrap();
        let _ = read_frame(&mut recv).await.unwrap();

        // Frame with OPCODE_REPLICATE but garbage payload — must NOT tear
        // down the stream; the server logs + acks + continues.
        let mut header = FrameHeader::default();
        header.opcode = OPCODE_REPLICATE;
        header.version = inferadb_ledger_wire::CURRENT_PROTOCOL_VERSION;
        header.payload_length = 4;
        let frame = Frame { header, payload: Bytes::from_static(&[0xFF, 0xFF, 0xFF, 0xFF]) };
        write_frame(&mut send, &frame).await.unwrap();

        let ack = tokio::time::timeout(Duration::from_secs(5), read_frame(&mut recv))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(ack.header.opcode, OPCODE_REPLICATE);
        assert_eq!(ack.header.flags & FLAG_IS_RESPONSE, FLAG_IS_RESPONSE);

        drop(send);
        drop(recv);
        let _ = tokio::time::timeout(Duration::from_secs(2), join).await;
    }

    // ── handle_consensus_envelope: low-level dispatch ───────────────────────

    #[tokio::test]
    async fn handle_envelope_unknown_region_returns_not_found() {
        let (manager, _temp) = make_manager();
        let result = handle_consensus_envelope(
            &manager,
            ConsensusEnvelope {
                shard_id: 0,
                from_node: 1,
                region: Some(Region::GLOBAL.as_str().to_string()),
                payload: Bytes::from_static(&[0xde, 0xad]),
                from_address: String::new(),
                cluster_id: 0,
            },
        )
        .await;
        let err = result.expect_err("unknown region must error");
        assert_eq!(err.code, inferadb_ledger_wire::ErrorCode::NotFound);
    }

    #[tokio::test]
    async fn handle_envelope_corrupt_payload_invalid_argument() {
        let (manager, _temp) = make_manager_with_region().await;
        let result = handle_consensus_envelope(
            &manager,
            ConsensusEnvelope {
                shard_id: 0,
                from_node: 1,
                region: Some(Region::GLOBAL.as_str().to_string()),
                payload: Bytes::from_static(&[0xff, 0xff, 0xff, 0xff]),
                from_address: String::new(),
                cluster_id: 0,
            },
        )
        .await;
        let err = result.expect_err("corrupt payload must error");
        assert_eq!(err.code, inferadb_ledger_wire::ErrorCode::InvalidArgument);
    }

    #[tokio::test]
    async fn handle_envelope_records_peer_address() {
        let (manager, _temp) = make_manager_with_region().await;
        let payload = encode(&Message::TimeoutNow).unwrap();
        let _ = handle_consensus_envelope(
            &manager,
            ConsensusEnvelope {
                shard_id: 0,
                from_node: 7,
                region: Some(Region::GLOBAL.as_str().to_string()),
                payload: Bytes::from(payload),
                from_address: "10.0.0.42:5000".to_owned(),
                cluster_id: 0,
            },
        )
        .await;
        // Whatever the dispatch outcome, the peer-address map must have
        // recorded the sender for cross-transport bootstrap visibility.
        assert_eq!(manager.peer_addresses().get(7).as_deref(), Some("10.0.0.42:5000"));
    }

    // ── committed_index_impl ─────────────────────────────────────────────────

    #[tokio::test]
    async fn committed_index_unary_calls_engine() {
        let (manager, _temp) = make_manager_with_region().await;
        let req = CommittedIndexRequest { region: String::new(), organization: None, vault: None };
        let result = committed_index_impl(&manager, req).await;
        // The freshly bootstrapped sole-voter region may not have completed
        // its initial self-election yet (background jobs are disabled in
        // the test fixture), so `is_leader()` can return either true or
        // false depending on timing. Whichever it is, the response must
        // not be a `NotFound` / `InvalidArgument` error — those would
        // indicate a routing or validation regression rather than the
        // legitimate election-state outcome.
        match result {
            Ok(_) => {},
            Err(err) => {
                assert_ne!(err.code, inferadb_ledger_wire::ErrorCode::NotFound);
                assert_ne!(err.code, inferadb_ledger_wire::ErrorCode::InvalidArgument);
                assert_ne!(err.code, inferadb_ledger_wire::ErrorCode::Internal);
            },
        }
    }

    #[tokio::test]
    async fn committed_index_invalid_region_returns_invalid_argument() {
        let (manager, _temp) = make_manager_with_region().await;
        let req = CommittedIndexRequest {
            region: "::not-a-region::".to_owned(),
            organization: None,
            vault: None,
        };
        let err = committed_index_impl(&manager, req).await.expect_err("invalid region");
        assert_eq!(err.code, inferadb_ledger_wire::ErrorCode::InvalidArgument);
    }

    #[tokio::test]
    async fn committed_index_partial_vault_scope_rejected() {
        let (manager, _temp) = make_manager_with_region().await;
        let req = CommittedIndexRequest {
            region: String::new(),
            organization: Some(inferadb_ledger_wire::services::shared::OrganizationSlug::new(1)),
            vault: None,
        };
        let err = committed_index_impl(&manager, req).await.expect_err("partial scope");
        assert_eq!(err.code, inferadb_ledger_wire::ErrorCode::InvalidArgument);
    }

    // ── regional_proposal_impl ───────────────────────────────────────────────

    #[tokio::test]
    async fn regional_proposal_unary_returns_not_found_for_missing_region() {
        let (manager, _temp) = make_manager();
        // No region started — the data-region group lookup must miss.
        let req = RegionalProposalRequest {
            region: None,
            request_payload: Bytes::from_static(b"x"),
            caller: 1,
            timeout_ms: 100,
        };
        let err = regional_proposal_impl(&manager, req).await.expect_err("no region");
        assert_eq!(err.code, inferadb_ledger_wire::ErrorCode::NotFound);
    }

    #[tokio::test]
    async fn regional_proposal_invalid_region_returns_invalid_argument() {
        let (manager, _temp) = make_manager();
        let req = RegionalProposalRequest {
            region: Some("::not-a-region::".to_owned()),
            request_payload: Bytes::from_static(b"x"),
            caller: 1,
            timeout_ms: 100,
        };
        let err = regional_proposal_impl(&manager, req).await.expect_err("invalid region");
        assert_eq!(err.code, inferadb_ledger_wire::ErrorCode::InvalidArgument);
    }

    // ── SnapshotHandler impl ─────────────────────────────────────────────────

    fn org_header(snapshot_index: u64, organization_id: i64) -> InstallSnapshotHeader {
        InstallSnapshotHeader {
            leader_term: 1,
            leader_id: 1,
            scope: Some(InstallSnapshotScope::Org(InstallSnapshotOrgScope {
                region: Region::GLOBAL.as_str().to_owned(),
                organization_id,
            })),
            snapshot_index,
            snapshot_term: 1,
            total_bytes: 0,
            chunk_size_bytes: 256 * 1024,
            lsnp_version: "LSNP-v2".to_owned(),
        }
    }

    fn vault_header(
        snapshot_index: u64,
        organization_id: i64,
        vault_id: i64,
    ) -> InstallSnapshotHeader {
        InstallSnapshotHeader {
            leader_term: 1,
            leader_id: 1,
            scope: Some(InstallSnapshotScope::Vault(InstallSnapshotVaultScope {
                region: Region::GLOBAL.as_str().to_owned(),
                organization_id,
                vault_id,
            })),
            snapshot_index,
            snapshot_term: 1,
            total_bytes: 0,
            chunk_size_bytes: 256 * 1024,
            lsnp_version: "LSNP-v2".to_owned(),
        }
    }

    #[test]
    fn snapshot_handler_org_scope_resolves_to_persister_dir() {
        let (manager, _temp) = make_manager();
        let handler = WireRaftServerHandler::new(Arc::clone(&manager));
        let header = org_header(1000, 7);
        let dir = handler.staging_dir(&header);
        // Must be a real path beneath the manager's snapshot tree.
        assert!(dir.is_absolute() || dir.starts_with("."));
        // No `vault-` segment for org scope.
        let dir_str = dir.to_string_lossy();
        assert!(
            !dir_str.contains("/vault-"),
            "org-scope staging_dir should not contain vault- segment: {dir_str}",
        );
    }

    #[test]
    fn snapshot_handler_vault_scope_includes_vault_subdir() {
        let (manager, _temp) = make_manager();
        let handler = WireRaftServerHandler::new(Arc::clone(&manager));
        let header = vault_header(2000, 7, 99);
        let dir = handler.staging_dir(&header);
        let dir_str = dir.to_string_lossy();
        assert!(
            dir_str.contains("vault-99"),
            "vault-scope staging_dir should contain vault-99 segment: {dir_str}",
        );
    }

    #[test]
    fn snapshot_handler_invokes_install_on_complete() {
        let (manager, _temp) = make_manager();
        let observed: Arc<Mutex<Vec<(u64, std::path::PathBuf)>>> = Arc::new(Mutex::new(Vec::new()));
        let observed_clone = Arc::clone(&observed);
        let handler = WireRaftServerHandler::new(Arc::clone(&manager)).with_snapshot_complete_hook(
            Arc::new(move |hdr, path| {
                observed_clone.lock().push((hdr.snapshot_index, path.to_path_buf()));
            }),
        );

        let header = vault_header(3000, 5, 42);
        let staged = std::path::PathBuf::from("/tmp/some-staged.snap.staged");
        handler.on_snapshot_complete(&header, Path::new(&staged));

        let calls = observed.lock();
        assert_eq!(calls.len(), 1, "hook must fire exactly once");
        assert_eq!(calls[0].0, 3000);
        assert_eq!(calls[0].1, staged);
    }

    #[test]
    fn snapshot_handler_no_hook_is_silent() {
        // Without an installed hook, on_snapshot_complete must not panic.
        let (manager, _temp) = make_manager();
        let handler = WireRaftServerHandler::new(Arc::clone(&manager));
        let header = org_header(1, 1);
        handler.on_snapshot_complete(&header, Path::new("/tmp/x.snap.staged"));
    }

    // ── Wire-trait impl: replicate / install_snapshot_stream surface ─────────

    #[tokio::test]
    async fn wire_trait_replicate_returns_routing_error() {
        use inferadb_ledger_wire_services::RaftService as WireRaft;
        let (manager, _temp) = make_manager();
        let handler = WireRaftServerHandler::new(manager);
        let envelope = ConsensusEnvelope {
            shard_id: 0,
            from_node: 1,
            region: None,
            payload: Bytes::new(),
            from_address: String::new(),
            cluster_id: 0,
        };
        let err = handler.replicate(envelope, sample_ctx()).await.expect_err("must error");
        assert_eq!(err.code, inferadb_ledger_wire::ErrorCode::Internal);
        assert!(err.message.contains("OPCODE_RAFT_PROTOCOL_HANDSHAKE"));
    }

    #[tokio::test]
    async fn wire_trait_install_snapshot_stream_returns_routing_error() {
        use inferadb_ledger_wire_services::RaftService as WireRaft;
        let (manager, _temp) = make_manager();
        let handler = WireRaftServerHandler::new(manager);
        let chunk = inferadb_ledger_wire::services::raft::InstallSnapshotChunk { payload: None };
        let err =
            handler.install_snapshot_stream(chunk, sample_ctx()).await.expect_err("must error");
        assert_eq!(err.code, inferadb_ledger_wire::ErrorCode::Internal);
        assert!(err.message.contains("snapshot sub-protocol"));
    }
}
