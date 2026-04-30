//! QUIC-based wire transport for the InferaDB Ledger custom protocol.
//!
//! Wraps [`quinn`] with the fixed-size frame format defined in
//! [`inferadb_ledger_wire`]. Server and client share codec, TLS config,
//! and graceful-shutdown machinery; the [`server`] and [`client`] modules
//! diverge only on their entry points.
//!
//! Server lifecycle is [`WireServer::bind`] to start the accept loop and
//! [`WireServer::shutdown`] to drain cleanly — the shutdown path cancels
//! the root token, sends a QUIC `CONNECTION_CLOSE` to every active peer,
//! and awaits all in-flight tasks bounded by a caller-supplied timeout.
//!
//! Phase 0.0.B of the SDK rewrite plan
//! (`docs/superpowers/plans/2026-04-30-region-aware-sdk-multiplexer.md`).

#![deny(unsafe_code)]
#![warn(missing_docs)]

pub mod alpn;
pub mod auth;
pub mod client;
pub mod connection;
pub mod dispatcher;
pub mod error;
pub mod frame_io;
pub mod replicate_stream;
pub mod server;
pub mod snapshot_stream;
pub mod stream;
pub mod tls;

pub use auth::{AuthError, AuthVerifier, VerifiedAuth};
pub use client::{
    ClientConfig, ClientStatus, ConnectionState, ConnectionStateKind, ResponseStream, RpcError,
    UnaryRequest, WireClient,
};
pub use dispatcher::{Dispatcher, StreamSink, StreamSinkError};
pub use error::TransportError;
pub use replicate_stream::ReplicateHandler;
pub use server::{ShutdownOutcome, WireServer};
pub use snapshot_stream::{
    MAX_INFLIGHT_BYTES, SnapshotHandler, SnapshotReceiver, SnapshotSender, SnapshotStreamError,
    cleanup_partial_snapshots,
};
