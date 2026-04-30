fn main() {}

// Two services declared with the same base `0x0100`: their effective
// ranges `[0x0100, 0x01FF]` are identical and obviously overlap. The
// per-RPC opcodes don't collide today (`0x0100` vs `0x0101`), but future
// additions in either service would.
inferadb_ledger_wire_macro::define_protocol! {
    service A {
        base: 0x0100,
        rpc 0x0100 a(R) -> S;
    }
    service B {
        base: 0x0100,
        rpc 0x0101 b(R) -> S;
    }
}
