//! Compile-pass: 4 services with mixed unary + streaming RPCs.
//!
//! Confirms the macro scales beyond the trivial cases — multiple
//! non-overlapping ranges, multiple RPCs per service, both unary and
//! streaming opcodes interleaved across services. The single
//! `define_protocol!` invocation pattern from the production protocol.

fn main() {}

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct ReadReq;
#[derive(Serialize, Deserialize)]
pub struct ReadResp;
#[derive(Serialize, Deserialize)]
pub struct WriteReq;
#[derive(Serialize, Deserialize)]
pub struct WriteResp;
#[derive(Serialize, Deserialize)]
pub struct WatchReq;
#[derive(Serialize, Deserialize)]
pub struct WatchEvt;
#[derive(Serialize, Deserialize)]
pub struct AdminReq;
#[derive(Serialize, Deserialize)]
pub struct AdminResp;

inferadb_ledger_wire_macro::define_protocol! {
    service ReadService {
        base: 0x0100,
        rpc 0x0100 read(ReadReq) -> ReadResp;
        rpc 0x0101 read_v2(ReadReq) -> ReadResp;
    }

    service WriteService {
        base: 0x0200,
        rpc 0x0200 write(WriteReq) -> WriteResp;
    }

    service WatcherService {
        base: 0x0300,
        rpc 0x0300 watch(WatchReq) -> stream WatchEvt;
        rpc 0x0301 watch_filtered(WatchReq) -> stream WatchEvt;
    }

    service AdminService {
        base: 0x0400,
        rpc 0x0400 admin(AdminReq) -> AdminResp;
    }
}
