//! `trybuild` compile-fail / compile-pass fixtures for `define_protocol!`.
//!
//! Compile-fail fixtures assert that specific parse-time and type-check
//! errors fire with sensible messages. Compile-pass fixtures validate
//! that the emitted code is syntactically valid Rust and type-checks
//! against the real `inferadb-ledger-wire`,
//! `inferadb-ledger-wire-transport`, `bytes`, `postcard`, `serde`, and
//! `futures` crates — not just stringified token streams.
//!
//! Note on a deliberately omitted fixture: a "duplicate opcode across
//! services" case is unreachable in isolation. The cross-service collision
//! check in `parse::validate_cross_service` is a defensive backstop, but
//! the per-service range bound (each opcode must lie in
//! `[base, base + 0xFF]`) and the cross-service range-overlap check fire
//! first for any colliding pair. The
//! `overlapping_service_ranges.rs` fixture covers the single observable
//! shape of that failure.

#[test]
fn trybuild() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/trybuild/duplicate_opcode_within_service.rs");
    t.compile_fail("tests/trybuild/opcode_out_of_service_range.rs");
    t.compile_fail("tests/trybuild/overlapping_service_ranges.rs");
    t.compile_fail("tests/trybuild/empty_protocol.rs");
    t.compile_fail("tests/trybuild/unknown_request_type.rs");
    t.pass("tests/trybuild/valid_protocol.rs");
    t.pass("tests/trybuild/streaming_rpc.rs");
    t.pass("tests/trybuild/multiple_services.rs");
}
