//! Compile-pass: a minimal two-service unary protocol.
//!
//! Validates that the macro's emitted server traits, dispatch fns, client
//! structs, `OpCode` enum, registry helpers, and reflection table all
//! type-check against the production crates. Resolves the unqualified
//! absolute paths (`::bytes::Bytes`, `::postcard::*`,
//! `::inferadb_ledger_wire::*`, `::inferadb_ledger_wire_transport::*`,
//! `::futures::Stream`, `::serde::Deserialize`).

fn main() {}

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct ReadRequest;

#[derive(Serialize, Deserialize)]
pub struct ReadResponse;

#[derive(Serialize, Deserialize)]
pub struct WriteRequest;

#[derive(Serialize, Deserialize)]
pub struct WriteResponse;

inferadb_ledger_wire_macro::define_protocol! {
    service ReadService {
        base: 0x0100,
        rpc 0x0100 read(ReadRequest) -> ReadResponse;
    }
    service WriteService {
        base: 0x0200,
        rpc 0x0200 write(WriteRequest) -> WriteResponse;
    }
}
