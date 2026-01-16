//! Build script for inferadb-ledger-raft.
//!
//! Compiles protobuf definitions into Rust code using tonic-prost-build.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo::rerun-if-changed=../../proto/ledger/v1/ledger.proto");

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .emit_rerun_if_changed(true)
        .compile_protos(&["../../proto/ledger/v1/ledger.proto"], &["../../proto"])?;

    Ok(())
}
