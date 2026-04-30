//! Compile-pass: streaming RPC.
//!
//! Validates the streaming code paths — the trait method that takes a
//! `StreamSink`, the `dispatch_*_stream` fn, and the client method that
//! returns a `TypedResponseStream<T>`.

fn main() {}

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct WatchRequest;

#[derive(Serialize, Deserialize)]
pub struct Event;

inferadb_ledger_wire_macro::define_protocol! {
    service WatcherService {
        base: 0x0100,
        rpc 0x0100 watch(WatchRequest) -> stream Event;
    }
}
