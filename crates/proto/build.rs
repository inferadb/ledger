//! Build script for inferadb-ledger-proto.
//!
//! Compiles protobuf definitions into Rust code using tonic-prost-build.
//! When publishing to crates.io, proto files aren't available, so we use
//! pre-generated code from src/generated/ instead.

use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Declare custom cfg for conditional compilation
    println!("cargo::rustc-check-cfg=cfg(use_pregenerated_proto)");

    let proto_path = Path::new("../../proto/ledger/v1/ledger.proto");

    // Only generate if proto files exist (development environment)
    // Published crates use pre-generated code in src/generated/
    if proto_path.exists() {
        println!("cargo::rerun-if-changed=../../proto/ledger/v1/ledger.proto");

        let out_dir = std::path::PathBuf::from(std::env::var("OUT_DIR")?);

        tonic_prost_build::configure()
            .file_descriptor_set_path(out_dir.join("ledger_v1_descriptor.bin"))
            .build_server(true)
            .build_client(true)
            .emit_rerun_if_changed(true)
            .compile_protos(&["../../proto/ledger/v1/ledger.proto"], &["../../proto"])?;
    } else {
        // Signal that we're using pre-generated code
        println!("cargo::rustc-cfg=use_pregenerated_proto");
    }

    Ok(())
}
