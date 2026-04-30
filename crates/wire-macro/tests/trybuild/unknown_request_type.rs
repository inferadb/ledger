fn main() {}

mod proto {
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize)]
    pub struct Resp;
}

inferadb_ledger_wire_macro::define_protocol! {
    service Foo {
        base: 0x0100,
        rpc 0x0100 hello(NotARealType) -> proto::Resp;
    }
}
