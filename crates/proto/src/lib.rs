//! Protobuf types and conversions for InferaDB Ledger.
//!
//! This crate provides:
//! - Generated protobuf types and gRPC service traits ([`proto`])
//! - Bidirectional conversions between domain types and proto types ([`convert`])
//!
//! # Architecture
//!
//! Extracted from the `raft` crate so that consumers needing only wire-format
//! types (e.g., the SDK) can avoid pulling in Raft consensus internals.

#![deny(unsafe_code)]
// gRPC services return tonic::Status (176 bytes) - standard practice for gRPC error handling
#![allow(clippy::result_large_err)]

/// Generated protobuf types and service traits.
pub mod proto {
    #![allow(clippy::all)]
    #![allow(missing_docs)]

    // Use pre-generated code when proto files aren't available (crates.io)
    #[cfg(use_pregenerated_proto)]
    include!("generated/ledger.v1.rs");

    // Use build-time generated code in development
    #[cfg(not(use_pregenerated_proto))]
    tonic::include_proto!("ledger.v1");
}

/// Serialized `FileDescriptorSet` for gRPC reflection.
///
/// Embedded at compile time from the prost-generated descriptor binary.
/// Only available when building from source (not pre-generated code).
#[cfg(not(use_pregenerated_proto))]
pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("ledger_v1_descriptor");

/// Bidirectional conversions between domain and protobuf types.
pub mod convert;
