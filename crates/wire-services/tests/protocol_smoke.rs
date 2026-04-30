//! Smoke tests for the generated protocol surface.
//!
//! Detailed dispatch + reflection tests live in
//! `crates/wire-macro/tests/integration.rs` against the macro itself.
//! These tests confirm the production protocol invocation produces a
//! working surface — total RPC count, opcode round-trips, streaming
//! flag, and the registry predicate.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]

use inferadb_ledger_wire_services::{OpCode, is_registered_opcode, op_reflect_registry};

/// Total RPCs across all 14 services per `docs/migrations/from-grpc.md`.
/// Includes the two streaming RPCs the wire-macro DSL supports today
/// (`ReadService.watch_blocks`, `SystemDiscoveryService.watch_leader`)
/// plus the two unary placeholders for `RaftService.replicate` (E.6b)
/// and `RaftService.install_snapshot_stream` (E.6a).
const EXPECTED_RPC_COUNT: usize = 145;

#[test]
fn opcode_enum_is_populated() {
    let read = OpCode::from_opcode(0x0010).expect("ReadService.read at 0x0010");
    assert_eq!(read.opcode(), 0x0010);
    assert_eq!(read.name(), "ReadService::read");

    let write = OpCode::from_opcode(0x0100).expect("WriteService.write at 0x0100");
    assert_eq!(write.opcode(), 0x0100);
    assert_eq!(write.name(), "WriteService::write");

    let raft_committed =
        OpCode::from_opcode(0x0D00).expect("RaftService.committed_index at 0x0D00");
    assert_eq!(raft_committed.name(), "RaftService::committed_index");

    let admin_repair = OpCode::from_opcode(0x0A1B).expect("AdminService.repair_vault at 0x0A1B");
    assert_eq!(admin_repair.name(), "AdminService::repair_vault");
}

#[test]
fn op_reflect_registry_lists_all_145_rpcs() {
    let entries = op_reflect_registry();
    assert_eq!(
        entries.len(),
        EXPECTED_RPC_COUNT,
        "expected {EXPECTED_RPC_COUNT} RPCs across 14 services"
    );
}

#[test]
fn streaming_rpcs_flagged() {
    let entries = op_reflect_registry();
    let streaming: Vec<_> = entries.iter().filter(|e| e.streaming).collect();
    // ReadService.watch_blocks + SystemDiscoveryService.watch_leader.
    // Replicate (bidi) and InstallSnapshotStream (client-stream) are
    // declared unary placeholders today — see Tasks E.6a/E.6b.
    assert_eq!(streaming.len(), 2, "expected 2 server-streaming RPCs");

    let names: Vec<_> = streaming.iter().map(|e| e.name).collect();
    assert!(
        names.contains(&"ReadService::watch_blocks"),
        "missing ReadService::watch_blocks; got: {names:?}"
    );
    assert!(
        names.contains(&"SystemDiscoveryService::watch_leader"),
        "missing SystemDiscoveryService::watch_leader; got: {names:?}"
    );
}

#[test]
fn is_registered_opcode_covers_service_bases() {
    // Each of the 14 service bases must be registered.
    let bases = [
        0x0010_u16, // ReadService
        0x0100,     // WriteService
        0x0200,     // OrganizationService
        0x0300,     // VaultService
        0x0400,     // SchemaService
        0x0500,     // UserService
        0x0600,     // AppService
        0x0700,     // InvitationService
        0x0800,     // TokenService
        0x0900,     // EventsService
        0x0A00,     // AdminService
        0x0B00,     // HealthService
        0x0C00,     // SystemDiscoveryService
        0x0D00,     // RaftService
    ];
    for base in bases {
        assert!(is_registered_opcode(base), "base {base:#06x} should be registered");
    }
}

#[test]
fn is_registered_opcode_rejects_unknown() {
    // 0x0000 (null), reserved-range slots, and post-allocation are unset.
    assert!(!is_registered_opcode(0x0000));
    assert!(!is_registered_opcode(0x0005)); // OPCODE_REFLECT, not an RPC
    assert!(!is_registered_opcode(0xFFFF));
    // Sparse holes inside service ranges that no RPC claims.
    assert!(!is_registered_opcode(0x001D)); // ReadService has 0x0010..=0x001C
    assert!(!is_registered_opcode(0x0211)); // OrganizationService stops at 0x0210
}

#[test]
fn opcode_count_matches_registry_count() {
    let entries = op_reflect_registry();
    let mut by_service: std::collections::BTreeMap<&str, usize> = std::collections::BTreeMap::new();
    for entry in &entries {
        let service = entry.name.split("::").next().unwrap_or(entry.name);
        *by_service.entry(service).or_insert(0) += 1;
    }

    // Inventory totals from `docs/migrations/from-grpc.md`.
    let expected: &[(&str, usize)] = &[
        ("ReadService", 13),
        ("WriteService", 1),
        ("OrganizationService", 17),
        ("VaultService", 5),
        ("SchemaService", 7),
        ("UserService", 22),
        ("AppService", 18),
        ("InvitationService", 8),
        ("TokenService", 12),
        ("EventsService", 4),
        ("AdminService", 28),
        ("HealthService", 1),
        ("SystemDiscoveryService", 5),
        ("RaftService", 4),
    ];

    for (svc, want) in expected {
        let got = by_service.get(svc).copied().unwrap_or(0);
        assert_eq!(got, *want, "{svc} RPC count mismatch: want {want}, got {got}");
    }
    assert_eq!(by_service.len(), 14, "expected exactly 14 services");
}
