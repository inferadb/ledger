//! Build script for ledger-raft.
//!
//! Compiles protobuf definitions into Rust code using tonic-build.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Tell cargo to rerun if the proto file changes
    println!("cargo::rerun-if-changed=../../proto/ledger/v1/ledger.proto");

    // Configure tonic-build
    tonic_build::configure()
        // Generate server code (for implementing services)
        .build_server(true)
        // Generate client code (for inter-node RaftNetwork)
        .build_client(true)
        // Don't emit docs for generated code
        .emit_rerun_if_changed(true)
        // Compile the proto file
        .compile_protos(&["../../proto/ledger/v1/ledger.proto"], &["../../proto"])?;

    Ok(())
}
