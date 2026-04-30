//! Custom QUIC-based wire protocol for InferaDB Ledger.
//!
//! This crate defines the binary framing layer that replaces tonic/gRPC:
//! a fixed-size 88-byte [`FrameHeader`] followed by a variable-length payload.

pub mod codec;
pub mod context;
pub mod error;
pub mod frame;
pub mod opcode;
pub mod services;
pub mod version;

pub use codec::{CodecError, peek_payload_length};
pub use context::{AuthMethod, CallerIdentity, ConnectionMetrics, RequestContext};
pub use error::{ErrorCode, UnknownErrorCode, WireError};
pub use frame::{Frame, FrameHeader, HEADER_SIZE, MAX_FRAME_SIZE};
pub use opcode::{
    ADMIN_SERVICE_BASE, APP_SERVICE_BASE, EVENTS_SERVICE_BASE, HEALTH_SERVICE_BASE,
    INVITATION_SERVICE_BASE, LEASE_SERVICE_BASE, MAINTENANCE_SERVICE_BASE, OPCODE_AUTH_ESTABLISH,
    OPCODE_AUTH_REFRESH, OPCODE_HANDSHAKE_REQUEST, OPCODE_INSTALL_SNAPSHOT_STREAM, OPCODE_PING,
    OPCODE_RAFT_PROTOCOL_HANDSHAKE, OPCODE_REFLECT, OPCODE_SPACE_END, ORGANIZATION_SERVICE_BASE,
    RAFT_CONSENSUS_SERVICE_BASE, READ_SERVICE_BASE, RESERVED_RANGE_END, SCHEMA_SERVICE_BASE,
    SERVICE_RANGE_SIZE, SYSTEM_DISCOVERY_SERVICE_BASE, TOKEN_SERVICE_BASE, USER_SERVICE_BASE,
    VAULT_SERVICE_BASE, WRITE_SERVICE_BASE, is_valid_opcode,
};
pub use version::{
    ALPN_LEDGER_WIRE_V1, CURRENT_PROTOCOL_VERSION, RAFT_PROTOCOL_VERSION, SUPPORTED_ALPN_PROTOCOLS,
    supported_alpn_protocols_owned,
};
