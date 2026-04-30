//! ALPN identifiers for QUIC handshake.
//!
//! Re-exports the ALPN constants and helpers defined in
//! [`inferadb_ledger_wire::version`] so transport-layer callers don't need to
//! depend on the wire crate directly to configure quinn.

pub use inferadb_ledger_wire::{
    ALPN_LEDGER_WIRE_V1, SUPPORTED_ALPN_PROTOCOLS, supported_alpn_protocols_owned,
};
