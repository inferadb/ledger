fn main() {}

inferadb_ledger_wire_macro::define_protocol! {
    service Foo {
        base: 0x0100,
        rpc 0x0100 a(R) -> S;
        rpc 0x0100 b(R) -> S;
    }
}
