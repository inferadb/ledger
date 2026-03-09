//! gRPC services and server assembly for InferaDB Ledger.
//!
//! This crate contains all gRPC service implementations and the
//! [`LedgerServer`] that wires them together with Raft consensus.

#![deny(unsafe_code)]
#![warn(missing_docs)]
#![allow(clippy::result_large_err)]

#[doc(hidden)]
pub mod api_version;
/// JWT signing, validation, and key management.
pub mod jwt;
#[doc(hidden)]
pub mod peer_maintenance;
pub(crate) mod proto_compat;
#[doc(hidden)]
pub mod server;
#[doc(hidden)]
pub mod services;

#[doc(hidden)]
pub use server::LedgerServer;
