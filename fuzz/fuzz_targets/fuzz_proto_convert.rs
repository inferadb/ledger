//! Fuzz target for protobuf deserialization.
//!
//! Tests that arbitrary bytes fed to prost's `Message::decode` never panic.
//! This covers the primary attack surface for untrusted input entering the
//! system via gRPC: `WriteRequest`, `ReadRequest`, and admin requests.

#![no_main]

use libfuzzer_sys::fuzz_target;
use prost::Message;

use inferadb_ledger_proto::proto;

fuzz_target!(|data: &[u8]| {
    // Vary which message type we decode based on the first byte.
    // This lets the fuzzer explore multiple request types with a single target.
    if data.is_empty() {
        return;
    }

    let selector = data[0] % 10;
    let payload = &data[1..];

    match selector {
        // gRPC request types (primary attack surface)
        0 => {
            let _ = proto::WriteRequest::decode(payload);
        },
        1 => {
            let _ = proto::ReadRequest::decode(payload);
        },
        2 => {
            let _ = proto::CreateNamespaceRequest::decode(payload);
        },
        3 => {
            let _ = proto::CreateVaultRequest::decode(payload);
        },
        4 => {
            let _ = proto::BatchWriteRequest::decode(payload);
        },
        // Individual proto message types
        5 => {
            let _ = proto::Transaction::decode(payload);
        },
        6 => {
            let _ = proto::Operation::decode(payload);
        },
        7 => {
            let _ = proto::Block::decode(payload);
        },
        8 => {
            let _ = proto::MerkleProof::decode(payload);
        },
        _ => {
            let _ = proto::SetCondition::decode(payload);
        },
    }
});
