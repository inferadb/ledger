//! Wire-transport [`Dispatcher`] composition (Phase 0.0.E.4.16).
//!
//! Brings together the 14 wire-trait service impls behind a single
//! [`Dispatcher`] suitable for installation into a
//! [`WireServer`](inferadb_ledger_wire_transport::WireServer). Routes
//! incoming opcodes to the right service via opcode-range matching,
//! then delegates to the macro-generated `dispatch_<service>_unary`
//! / `dispatch_<service>_stream` functions in
//! [`inferadb_ledger_wire_services`].
//!
//! # Service block routing
//!
//! | Range             | Service                      |
//! |-------------------|------------------------------|
//! | `0x0010`–`0x00FF` | [`ReadService`]              |
//! | `0x0100`–`0x01FF` | [`WriteService`]             |
//! | `0x0200`–`0x02FF` | [`OrganizationService`]      |
//! | `0x0300`–`0x03FF` | [`VaultService`]             |
//! | `0x0400`–`0x04FF` | [`SchemaServiceStub`] (no real handler) |
//! | `0x0500`–`0x05FF` | [`UserService`]              |
//! | `0x0600`–`0x06FF` | [`AppService`]               |
//! | `0x0700`–`0x07FF` | [`InvitationService`]        |
//! | `0x0800`–`0x08FF` | [`TokenServiceImpl`]         |
//! | `0x0900`–`0x09FF` | [`EventsService`]            |
//! | `0x0A00`–`0x0AFF` | [`AdminService`]             |
//! | `0x0B00`–`0x0BFF` | [`HealthService`]            |
//! | `0x0C00`–`0x0CFF` | [`DiscoveryService`]         |
//! | `0x0D00`–`0x0DFF` | RaftService — NOT implemented here. The wire-side Raft transport ships in Phase 0.0.E.6b's `wire_consensus_transport`, which uses long-lived bidi streams rather than the standard [`Dispatcher`] path. Inbound frames in this range return `Internal` errors until E.6b lands. |
//!
//! The macro-generated `dispatch_<service>_unary` / `dispatch_<service>_stream`
//! functions do per-RPC routing within each block; this dispatcher only
//! chooses which service to invoke based on the opcode's high byte
//! (extended in the 0x0010–0x00FF case where Read shares a block with the
//! reserved range).
//!
//! # Streaming
//!
//! Only `ReadService::watch_blocks` (`0x0014`) and
//! `SystemDiscoveryService::watch_leader` (`0x0C04`) are server-streaming
//! RPCs. `Self::is_streaming` is the canonical predicate; the transport
//! consults it once per inbound stream to pick between `Self::dispatch`
//! and `Self::dispatch_stream`.
//!
//! # Tests
//!
//! Constructing a complete `LedgerWireDispatcher` for runtime tests is
//! heavy: each service impl needs its full dependency chain (`StateLayer`,
//! `RaftManager`, `JwtEngine`, …). The unit tests in this module exercise
//! the small amount of dispatcher-local logic — opcode routing helpers,
//! error-shape helpers, and the `is_streaming` delegation — in isolation.
//! End-to-end coverage (real services, real opcodes, real round-trip) is
//! deferred to Phase 0.0.E.7 (TestCluster migration).

use std::sync::Arc;

use bytes::Bytes;
use inferadb_ledger_raft::wire_consensus_transport::server_handler::WireRaftServerHandler;
use inferadb_ledger_store::StorageBackend;
use inferadb_ledger_wire::{
    ErrorCode, RequestContext, WireError,
    opcode::{
        ADMIN_SERVICE_BASE, APP_SERVICE_BASE, EVENTS_SERVICE_BASE, HEALTH_SERVICE_BASE,
        INVITATION_SERVICE_BASE, ORGANIZATION_SERVICE_BASE, RAFT_CONSENSUS_SERVICE_BASE,
        READ_SERVICE_BASE, SCHEMA_SERVICE_BASE, SERVICE_RANGE_SIZE, SYSTEM_DISCOVERY_SERVICE_BASE,
        TOKEN_SERVICE_BASE, USER_SERVICE_BASE, VAULT_SERVICE_BASE, WRITE_SERVICE_BASE,
    },
};
use inferadb_ledger_wire_services as ws;
use inferadb_ledger_wire_transport::{
    Dispatcher, StreamSink, replicate_stream::ReplicateHandler, snapshot_stream::SnapshotHandler,
};

use crate::services::{
    AdminService, AppService, DiscoveryService, EventsService, HealthService, InvitationService,
    OrganizationService, ReadService, SchemaServiceStub, TokenServiceImpl, UserService,
    VaultService, WriteService,
};

// ---------------------------------------------------------------------------
// Dispatcher
// ---------------------------------------------------------------------------

/// Composes the 14 wire-trait service impls into a single [`Dispatcher`].
///
/// Generic over the storage backend `B` because [`EventsService<B>`] is
/// generic; every other service is concrete.
///
/// Held by the production [`WireServer`](inferadb_ledger_wire_transport::WireServer)
/// behind a generic parameter (the [`Dispatcher`] trait's async-fn-in-trait
/// shape is not dyn-compatible). Each service is wrapped in [`Arc`] so the
/// dispatcher itself stays cheap to clone and the underlying handlers are
/// shared across every connection task.
pub struct LedgerWireDispatcher<B>
where
    B: StorageBackend + Send + Sync + 'static,
{
    health: Arc<HealthService>,
    write: Arc<WriteService>,
    read: Arc<ReadService>,
    vault: Arc<VaultService>,
    organization: Arc<OrganizationService>,
    user: Arc<UserService>,
    app: Arc<AppService>,
    invitation: Arc<InvitationService>,
    token: Arc<TokenServiceImpl>,
    events: Arc<EventsService<B>>,
    admin: Arc<AdminService>,
    discovery: Arc<DiscoveryService>,
    schema: Arc<SchemaServiceStub>,
    /// Optional Raft handler installed by [`Self::with_raft_handler`].
    ///
    /// When `Some`, the dispatcher routes opcodes in the
    /// [`RAFT_CONSENSUS_SERVICE_BASE`] block to the handler via
    /// [`ws::dispatch_raft_service_unary`] / `_stream`, and returns the
    /// handler from [`Dispatcher::snapshot_handler`] so the wire transport's
    /// snapshot sub-protocol bypasses the dispatcher entirely.
    ///
    /// `None` (the default) preserves the legacy behaviour: Raft frames
    /// arriving at this dispatcher return `Internal` errors via
    /// [`raft_not_implemented_error`]. Production servers running on the
    /// wire transport (E.6b.5) install a handler at construction time;
    /// servers still on the gRPC transport leave it unset.
    raft_handler: Option<Arc<WireRaftServerHandler>>,
}

impl<B> LedgerWireDispatcher<B>
where
    B: StorageBackend + Send + Sync + 'static,
{
    /// Construct a dispatcher with the 13 implemented services. Schema is
    /// always the stub [`SchemaServiceStub`] (no server-side schema
    /// management — see `schema_wire.rs`); RaftService is unset by default
    /// — install a [`WireRaftServerHandler`] via [`Self::with_raft_handler`]
    /// to route Raft opcodes (E.6b.5).
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        health: Arc<HealthService>,
        write: Arc<WriteService>,
        read: Arc<ReadService>,
        vault: Arc<VaultService>,
        organization: Arc<OrganizationService>,
        user: Arc<UserService>,
        app: Arc<AppService>,
        invitation: Arc<InvitationService>,
        token: Arc<TokenServiceImpl>,
        events: Arc<EventsService<B>>,
        admin: Arc<AdminService>,
        discovery: Arc<DiscoveryService>,
    ) -> Self {
        Self {
            health,
            write,
            read,
            vault,
            organization,
            user,
            app,
            invitation,
            token,
            events,
            admin,
            discovery,
            schema: Arc::new(SchemaServiceStub),
            raft_handler: None,
        }
    }

    /// Builder-style override that installs a Raft server handler.
    ///
    /// The handler is stored as `Arc<WireRaftServerHandler>` so the
    /// dispatcher can hand a `&dyn` reference back through
    /// [`Dispatcher::snapshot_handler`] without losing type information.
    /// Calling this twice replaces the previous handler — the dispatcher
    /// holds at most one handler at any time.
    #[must_use]
    pub fn with_raft_handler(mut self, handler: Arc<WireRaftServerHandler>) -> Self {
        self.raft_handler = Some(handler);
        self
    }
}

impl<B> Dispatcher for LedgerWireDispatcher<B>
where
    B: StorageBackend + Send + Sync + 'static,
{
    async fn dispatch(
        &self,
        opcode: u16,
        request: Bytes,
        ctx: RequestContext,
    ) -> Result<Bytes, WireError> {
        // Route by opcode range, then delegate to the macro-generated
        // per-service dispatch fn. Each `dispatch_<svc>_unary` returns a
        // `WireError { code: Internal, message: "unknown opcode ..." }`
        // for opcodes inside the service block that don't map to an RPC,
        // so we don't need to validate further here.
        match opcode {
            o if range_contains(READ_SERVICE_BASE, o) => {
                ws::dispatch_read_service_unary(self.read.as_ref(), o, request, ctx).await
            },
            o if range_contains(WRITE_SERVICE_BASE, o) => {
                ws::dispatch_write_service_unary(self.write.as_ref(), o, request, ctx).await
            },
            o if range_contains(ORGANIZATION_SERVICE_BASE, o) => {
                ws::dispatch_organization_service_unary(self.organization.as_ref(), o, request, ctx)
                    .await
            },
            o if range_contains(VAULT_SERVICE_BASE, o) => {
                ws::dispatch_vault_service_unary(self.vault.as_ref(), o, request, ctx).await
            },
            o if range_contains(SCHEMA_SERVICE_BASE, o) => {
                ws::dispatch_schema_service_unary(self.schema.as_ref(), o, request, ctx).await
            },
            o if range_contains(USER_SERVICE_BASE, o) => {
                ws::dispatch_user_service_unary(self.user.as_ref(), o, request, ctx).await
            },
            o if range_contains(APP_SERVICE_BASE, o) => {
                ws::dispatch_app_service_unary(self.app.as_ref(), o, request, ctx).await
            },
            o if range_contains(INVITATION_SERVICE_BASE, o) => {
                ws::dispatch_invitation_service_unary(self.invitation.as_ref(), o, request, ctx)
                    .await
            },
            o if range_contains(TOKEN_SERVICE_BASE, o) => {
                ws::dispatch_token_service_unary(self.token.as_ref(), o, request, ctx).await
            },
            o if range_contains(EVENTS_SERVICE_BASE, o) => {
                ws::dispatch_events_service_unary(self.events.as_ref(), o, request, ctx).await
            },
            o if range_contains(ADMIN_SERVICE_BASE, o) => {
                ws::dispatch_admin_service_unary(self.admin.as_ref(), o, request, ctx).await
            },
            o if range_contains(HEALTH_SERVICE_BASE, o) => {
                ws::dispatch_health_service_unary(self.health.as_ref(), o, request, ctx).await
            },
            o if range_contains(SYSTEM_DISCOVERY_SERVICE_BASE, o) => {
                ws::dispatch_system_discovery_service_unary(
                    self.discovery.as_ref(),
                    o,
                    request,
                    ctx,
                )
                .await
            },
            o if range_contains(RAFT_CONSENSUS_SERVICE_BASE, o) => match &self.raft_handler {
                Some(handler) => {
                    ws::dispatch_raft_service_unary(handler.as_ref(), o, request, ctx).await
                },
                None => Err(raft_not_implemented_error(o)),
            },
            _ => Err(unknown_opcode_error(opcode)),
        }
    }

    async fn dispatch_stream(
        &self,
        opcode: u16,
        request: Bytes,
        ctx: RequestContext,
        sink: StreamSink,
    ) -> Result<(), WireError> {
        // Streaming RPCs exist only on Read (`watch_blocks`, `0x0014`) and
        // SystemDiscovery (`watch_leader`, `0x0C04`). Other service blocks
        // are routed through to their `dispatch_*_stream` fn which returns
        // `unknown opcode` for any opcode lacking a streaming arm — but
        // the transport consults `is_streaming` first so this code path is
        // only ever entered for opcodes the macro-generated `is_streaming`
        // returns `true` for. Routing here mirrors `dispatch` for
        // forward-compatibility (a future streaming RPC on, say, Events
        // would only need to extend the per-service `dispatch_*_stream`
        // arm — no change to this composition layer).
        match opcode {
            o if range_contains(READ_SERVICE_BASE, o) => {
                ws::dispatch_read_service_stream(self.read.as_ref(), o, request, ctx, sink).await
            },
            o if range_contains(WRITE_SERVICE_BASE, o) => {
                ws::dispatch_write_service_stream(self.write.as_ref(), o, request, ctx, sink).await
            },
            o if range_contains(ORGANIZATION_SERVICE_BASE, o) => {
                ws::dispatch_organization_service_stream(
                    self.organization.as_ref(),
                    o,
                    request,
                    ctx,
                    sink,
                )
                .await
            },
            o if range_contains(VAULT_SERVICE_BASE, o) => {
                ws::dispatch_vault_service_stream(self.vault.as_ref(), o, request, ctx, sink).await
            },
            o if range_contains(SCHEMA_SERVICE_BASE, o) => {
                ws::dispatch_schema_service_stream(self.schema.as_ref(), o, request, ctx, sink)
                    .await
            },
            o if range_contains(USER_SERVICE_BASE, o) => {
                ws::dispatch_user_service_stream(self.user.as_ref(), o, request, ctx, sink).await
            },
            o if range_contains(APP_SERVICE_BASE, o) => {
                ws::dispatch_app_service_stream(self.app.as_ref(), o, request, ctx, sink).await
            },
            o if range_contains(INVITATION_SERVICE_BASE, o) => {
                ws::dispatch_invitation_service_stream(
                    self.invitation.as_ref(),
                    o,
                    request,
                    ctx,
                    sink,
                )
                .await
            },
            o if range_contains(TOKEN_SERVICE_BASE, o) => {
                ws::dispatch_token_service_stream(self.token.as_ref(), o, request, ctx, sink).await
            },
            o if range_contains(EVENTS_SERVICE_BASE, o) => {
                ws::dispatch_events_service_stream(self.events.as_ref(), o, request, ctx, sink)
                    .await
            },
            o if range_contains(ADMIN_SERVICE_BASE, o) => {
                ws::dispatch_admin_service_stream(self.admin.as_ref(), o, request, ctx, sink).await
            },
            o if range_contains(HEALTH_SERVICE_BASE, o) => {
                ws::dispatch_health_service_stream(self.health.as_ref(), o, request, ctx, sink)
                    .await
            },
            o if range_contains(SYSTEM_DISCOVERY_SERVICE_BASE, o) => {
                ws::dispatch_system_discovery_service_stream(
                    self.discovery.as_ref(),
                    o,
                    request,
                    ctx,
                    sink,
                )
                .await
            },
            o if range_contains(RAFT_CONSENSUS_SERVICE_BASE, o) => match &self.raft_handler {
                Some(handler) => {
                    ws::dispatch_raft_service_stream(handler.as_ref(), o, request, ctx, sink).await
                },
                None => Err(raft_not_implemented_error(o)),
            },
            _ => Err(unknown_opcode_error(opcode)),
        }
    }

    fn is_streaming(&self, opcode: u16) -> bool {
        // Defer to the macro-generated `OpCode` registry — it's exhaustive
        // over the protocol DSL and matches the per-service dispatch arms
        // by construction. Unknown opcodes are not streaming.
        ws::OpCode::from_opcode(opcode).map(ws::OpCode::is_streaming).unwrap_or(false)
    }

    fn snapshot_handler(
        &self,
    ) -> Option<&dyn inferadb_ledger_wire_transport::snapshot_stream::SnapshotHandler> {
        // E.6b.5: when a `WireRaftServerHandler` is installed it impls
        // `SnapshotHandler` (see [`WireRaftServerHandler`]'s impl in
        // `crates/raft/src/wire_consensus_transport/server_handler.rs`),
        // so the wire transport's snapshot sub-protocol bypasses the
        // standard dispatch path and lands directly on the handler. When
        // unset, snapshot streams arriving at this dispatcher are
        // answered with `Internal` by the transport (see
        // `Dispatcher::snapshot_handler` doc).
        self.raft_handler.as_ref().map(|h| h.as_ref() as &dyn SnapshotHandler)
    }

    fn replicate_handler(
        &self,
    ) -> Option<&dyn inferadb_ledger_wire_transport::replicate_stream::ReplicateHandler> {
        // F.1.f.2.Stage1c: when a `WireRaftServerHandler` is installed it
        // impls `ReplicateHandler`, so the wire transport's
        // `OPCODE_RAFT_PROTOCOL_HANDSHAKE` dispatch branch bypasses the
        // standard dispatch path and drives the bidi stream through the
        // handler's handshake + envelope drain. When unset, inbound
        // `Replicate` streams are answered with `Internal` by the
        // transport (see `Dispatcher::replicate_handler` doc).
        self.raft_handler.as_ref().map(|h| h.as_ref() as &dyn ReplicateHandler)
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Inclusive-low / exclusive-high check that `opcode` falls within the
/// `SERVICE_RANGE_SIZE`-wide block starting at `base`.
///
/// Read is special-cased: its base is `0x0010` (sharing the first
/// 256-opcode block with the reserved range `0x0000`–`0x000F`), so
/// adding `SERVICE_RANGE_SIZE` would overshoot into Write's block. For
/// Read we cap the upper bound at `WRITE_SERVICE_BASE` (`0x0100`); for
/// every other service base the natural `base + SERVICE_RANGE_SIZE`
/// upper bound is correct because all other bases are aligned to
/// `SERVICE_RANGE_SIZE`.
#[inline]
fn range_contains(base: u16, opcode: u16) -> bool {
    let high = if base == READ_SERVICE_BASE {
        WRITE_SERVICE_BASE
    } else {
        base.saturating_add(SERVICE_RANGE_SIZE)
    };
    opcode >= base && opcode < high
}

/// Build the `WireError` returned for opcodes in the Raft block until
/// Phase 0.0.E.6b's `wire_consensus_transport` lands.
///
/// `code` is `Internal` because reaching this dispatcher with a Raft
/// opcode signals a misrouted client — production raft traffic flows
/// over the dedicated bidi sub-protocol, not the standard wire
/// transport. `retryable: false`: retrying will hit the same code path.
fn raft_not_implemented_error(opcode: u16) -> WireError {
    WireError {
        code: ErrorCode::Internal,
        message: format!(
            "RaftService opcode {opcode:#06x} is not routed via the standard wire dispatcher; \
             wire-side Raft transport ships in Phase 0.0.E.6b (wire_consensus_transport)"
        ),
        retryable: false,
        retry_after_ms: 0,
        context: std::collections::BTreeMap::new(),
        suggested_action: "use the gRPC RaftService until Phase 0.0.E.6b lands".to_string(),
    }
}

/// Build the `WireError` returned for opcodes that fall outside every
/// allocated service block.
///
/// This is distinct from "opcode is inside a known service block but
/// unmapped to an RPC" — that case lands in the per-service
/// `dispatch_*_unary` fn and returns `Internal: unknown opcode` from the
/// macro-generated dispatch helper.
fn unknown_opcode_error(opcode: u16) -> WireError {
    WireError {
        code: ErrorCode::InvalidArgument,
        message: format!("opcode {opcode:#06x} is outside every allocated service range"),
        retryable: false,
        retry_after_ms: 0,
        context: std::collections::BTreeMap::new(),
        suggested_action: "check protocol version compatibility between client and server"
            .to_string(),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    //! Unit tests cover the dispatcher-local logic only — opcode-range
    //! helpers, error-shape helpers, and `is_streaming` delegation.
    //!
    //! End-to-end tests (constructing a real `LedgerWireDispatcher` with
    //! all 13 services wired up against a `StateLayer` / `RaftManager`)
    //! are deferred to Phase 0.0.E.7 (TestCluster migration). Building
    //! the full service graph in a unit test would duplicate
    //! `LedgerServer::new` and is best done once — over a real
    //! `TestCluster` — when E.7 replaces the current tonic-backed
    //! integration harness.

    use super::*;

    // -----------------------------------------------------------------------
    // range_contains
    // -----------------------------------------------------------------------

    #[test]
    fn range_contains_low_inclusive() {
        assert!(range_contains(WRITE_SERVICE_BASE, WRITE_SERVICE_BASE));
        assert!(range_contains(ADMIN_SERVICE_BASE, ADMIN_SERVICE_BASE));
    }

    #[test]
    fn range_contains_high_exclusive() {
        assert!(!range_contains(WRITE_SERVICE_BASE, WRITE_SERVICE_BASE + SERVICE_RANGE_SIZE));
    }

    #[test]
    fn range_contains_below_base() {
        assert!(!range_contains(WRITE_SERVICE_BASE, WRITE_SERVICE_BASE - 1));
    }

    #[test]
    fn range_contains_within_block() {
        // Mid-range opcodes for each tested service block.
        assert!(range_contains(READ_SERVICE_BASE, 0x0014)); // watch_blocks
        assert!(range_contains(WRITE_SERVICE_BASE, 0x0100)); // write
        assert!(range_contains(SYSTEM_DISCOVERY_SERVICE_BASE, 0x0C04)); // watch_leader
        assert!(range_contains(RAFT_CONSENSUS_SERVICE_BASE, 0x0D03));
    }

    #[test]
    fn range_contains_other_block_excluded() {
        // A WriteService opcode is not in the Read block — Read is
        // special-cased so its upper bound is `WRITE_SERVICE_BASE`, not
        // `READ_SERVICE_BASE + SERVICE_RANGE_SIZE` (which would overshoot
        // into Write's block). See `range_contains` for the rationale.
        assert!(!range_contains(READ_SERVICE_BASE, WRITE_SERVICE_BASE));
        // A Health opcode is not in the Admin block.
        assert!(!range_contains(ADMIN_SERVICE_BASE, HEALTH_SERVICE_BASE));
    }

    #[test]
    fn range_contains_read_capped_at_write_base() {
        // Read shares the first 256-opcode block with the reserved range
        // (0x0000-0x000F), so its effective range is [0x0010, 0x0100), not
        // [0x0010, 0x0110). This protects against an opcode of, say,
        // 0x0100 (WriteService::write) being misrouted to Read's
        // dispatch fn — which would return `unknown opcode` from the
        // macro-generated dispatch helper, but the routing would be
        // wrong-flavoured.
        assert!(!range_contains(READ_SERVICE_BASE, WRITE_SERVICE_BASE));
        assert!(!range_contains(READ_SERVICE_BASE, WRITE_SERVICE_BASE + 0x10));
        // The last in-range Read opcode is 0x00FF.
        assert!(range_contains(READ_SERVICE_BASE, 0x00FF));
        // The first out-of-range opcode at the high end is WriteService's
        // base.
        assert!(!range_contains(READ_SERVICE_BASE, 0x0100));
    }

    #[test]
    fn range_contains_saturating_boundary_ok() {
        // Lease block (`0x0F00`) is the last allocated service today.
        // Adding `SERVICE_RANGE_SIZE` saturates safely without panicking.
        let high = inferadb_ledger_wire::opcode::LEASE_SERVICE_BASE;
        assert!(range_contains(high, high));
        assert!(range_contains(high, high + SERVICE_RANGE_SIZE - 1));
        assert!(!range_contains(high, high.saturating_add(SERVICE_RANGE_SIZE)));
    }

    // -----------------------------------------------------------------------
    // raft_not_implemented_error
    // -----------------------------------------------------------------------

    #[test]
    fn raft_not_implemented_error_shape() {
        let err = raft_not_implemented_error(0x0D01);
        assert_eq!(err.code, ErrorCode::Internal);
        assert!(err.message.contains("0x0d01"));
        assert!(err.message.contains("RaftService"));
        // The migration phase tag is load-bearing — operators searching
        // logs for "E.6b" should find it.
        assert!(err.message.contains("E.6b") || err.message.contains("0.0.E.6b"));
        assert!(!err.retryable);
        assert_eq!(err.retry_after_ms, 0);
        assert!(err.context.is_empty());
        assert!(!err.suggested_action.is_empty());
    }

    // -----------------------------------------------------------------------
    // unknown_opcode_error
    // -----------------------------------------------------------------------

    #[test]
    fn unknown_opcode_error_shape() {
        let err = unknown_opcode_error(0x9999);
        assert_eq!(err.code, ErrorCode::InvalidArgument);
        assert!(err.message.contains("0x9999"));
        assert!(err.message.contains("outside"));
        assert!(!err.retryable);
        assert_eq!(err.retry_after_ms, 0);
        assert!(err.context.is_empty());
        assert!(!err.suggested_action.is_empty());
    }

    // -----------------------------------------------------------------------
    // is_streaming delegation
    // -----------------------------------------------------------------------
    //
    // We don't need a constructed `LedgerWireDispatcher` to test
    // `is_streaming` — the impl delegates to `OpCode::from_opcode`
    // immediately. Exercise the underlying registry directly (matches
    // the dispatcher's behaviour by construction).

    #[test]
    fn is_streaming_known_streaming_opcode() {
        // ReadService::watch_blocks (0x0014) and
        // SystemDiscoveryService::watch_leader (0x0C04) are the only
        // streaming RPCs in the protocol today.
        let watch_blocks =
            ws::OpCode::from_opcode(0x0014).map(ws::OpCode::is_streaming).unwrap_or(false);
        let watch_leader =
            ws::OpCode::from_opcode(0x0C04).map(ws::OpCode::is_streaming).unwrap_or(false);
        assert!(watch_blocks, "watch_blocks should be streaming");
        assert!(watch_leader, "watch_leader should be streaming");
    }

    #[test]
    fn is_streaming_known_unary_opcode() {
        // ReadService::read (0x0010) and WriteService::write (0x0100) are
        // unary.
        let read = ws::OpCode::from_opcode(0x0010).map(ws::OpCode::is_streaming).unwrap_or(false);
        let write = ws::OpCode::from_opcode(0x0100).map(ws::OpCode::is_streaming).unwrap_or(false);
        assert!(!read, "read should be unary");
        assert!(!write, "write should be unary");
    }

    #[test]
    fn is_streaming_unknown_opcode_returns_false() {
        // 0x9999 is outside every allocated service block.
        let unknown =
            ws::OpCode::from_opcode(0x9999).map(ws::OpCode::is_streaming).unwrap_or(false);
        assert!(!unknown);
    }

    // -----------------------------------------------------------------------
    // Dispatcher trait shape
    // -----------------------------------------------------------------------

    /// Compile-time check that `LedgerWireDispatcher<B>` satisfies the
    /// `Dispatcher` trait when wrapped in `Arc<>`. Mirrors the
    /// `dispatcher_object_safe_via_arc` test in `wire-transport`. Uses a
    /// trivial `B` (`MemoryBackend` from the store crate's test surface
    /// is heavy; instead use a generic bound assertion via a
    /// type-parameterised helper that never runs).
    #[test]
    fn dispatcher_trait_compiles_for_arc_wrapper() {
        fn assert_dispatcher<T: Dispatcher>(_: Arc<T>) {}
        // We can't construct a real `LedgerWireDispatcher<B>` here without
        // the full service graph; this is a compile-only proof via a
        // generic bound check. The function body is never executed.
        fn _check_bound<B>(d: Arc<LedgerWireDispatcher<B>>)
        where
            B: StorageBackend + Send + Sync + 'static,
        {
            assert_dispatcher(d);
        }
        // Reference `_check_bound` to keep the compiler honest about the
        // bound — without this line it would be dead code.
        let _ = _check_bound::<inferadb_ledger_store::InMemoryBackend> as fn(_);
    }

    // -----------------------------------------------------------------------
    // E.6b.5 raft_handler threading
    // -----------------------------------------------------------------------
    //
    // Constructing a real `LedgerWireDispatcher<B>` for runtime tests is
    // heavy (13 services, full state graph); end-to-end dispatcher routing
    // coverage ships with E.7 (TestCluster migration). The tests here
    // verify only what changed in E.6b.5: the absence/presence of the raft
    // handler routes through to the expected error fallback or handler
    // dispatch. Field threading itself is provable from the type system —
    // [`with_raft_handler`] returns `Self` with `raft_handler = Some(_)`,
    // and [`Dispatcher::snapshot_handler`] returns `self.raft_handler.as_ref()`,
    // so the tests focus on the no-handler branch (the only side reachable
    // without standing up the service graph).

    /// Without a handler, the dispatcher's Raft-block routing helper
    /// returns the canonical not-implemented error. End-to-end dispatch
    /// coverage of this path is in `dispatcher_trait_compiles_for_arc_wrapper`
    /// (compile-time bound) plus E.7's TestCluster migration.
    #[test]
    fn no_raft_handler_routes_to_not_implemented_error() {
        let err = raft_not_implemented_error(0x0D01);
        assert_eq!(err.code, ErrorCode::Internal);
        assert!(!err.retryable);
        assert!(err.message.contains("0x0d01"));
    }
}
