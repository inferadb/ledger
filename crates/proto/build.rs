//! Build script for inferadb-ledger-proto.
//!
//! Compiles protobuf definitions into Rust code using tonic-prost-build.
//! When publishing to crates.io, proto files aren't available, so we use
//! pre-generated code from src/generated/ instead.

use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Declare custom cfg for conditional compilation
    println!("cargo::rustc-check-cfg=cfg(use_pregenerated_proto)");

    let manifest_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?);
    let workspace_root = manifest_dir.join("../.."); // crates/proto -> workspace root
    let proto_file = workspace_root.join("proto/ledger/v1/ledger.proto");
    let proto_include = workspace_root.join("proto");

    // Only generate if proto files exist (development environment)
    // Published crates use pre-generated code in src/generated/
    if proto_file.exists() {
        println!("cargo::rerun-if-changed={}", proto_file.display());

        let out_dir = PathBuf::from(std::env::var("OUT_DIR")?);

        let proto_str = proto_file.to_str().ok_or("proto file path is not valid UTF-8")?;
        let include_str = proto_include.to_str().ok_or("proto include path is not valid UTF-8")?;

        tonic_prost_build::configure()
            .file_descriptor_set_path(out_dir.join("ledger_v1_descriptor.bin"))
            .build_server(true)
            .build_client(true)
            .emit_rerun_if_changed(true)
            .compile_protos(&[proto_str], &[include_str])?;
    } else {
        // Signal that we're using pre-generated code
        println!("cargo::rustc-cfg=use_pregenerated_proto");
        // Re-run if pre-generated code changes (e.g. after `just proto`)
        let generated = manifest_dir.join("src/generated/ledger.v1.rs");
        println!("cargo::rerun-if-changed={}", generated.display());
    }

    Ok(())
}
