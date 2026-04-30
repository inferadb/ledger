//! Smoke test for the F.1.f.2.Stage5b wire-side snapshot install path.
//!
//! Verifies that the leader-side
//! [`WireSnapshotSender`](inferadb_ledger_raft::wire_consensus_transport::snapshot::WireSnapshotSender)
//! and the receiver-side `run_snapshot_receive_loop` cooperate end-to-end
//! over the production wire transport — the leader persists a snapshot,
//! `send_snapshot_for_test` dispatches it via QUIC, and the follower's
//! staging directory ends up with a `.snap.staged` file.
//!
//! Stage 5b deleted the tonic-using `snapshot_streamer.rs`; this test pins
//! the wire replacement against regression. The full reactor-driven path
//! (Action::SendSnapshot → WireSnapshotSender → wire sub-protocol →
//! receiver) is exercised by exposing the same trait implementation
//! through the public test helper `RaftManager::send_snapshot_for_test`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]

use std::time::Duration;

use inferadb_ledger_consensus::types::NodeId;
use inferadb_ledger_raft::snapshot::SnapshotScope;
use inferadb_ledger_types::{OrganizationId, Region};

use crate::common::TestCluster;

/// End-to-end: leader persists a system-group snapshot, dispatches the
/// wire snapshot streamer at one of the followers, and the staged file
/// lands on the follower's per-scope staging directory.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wire_snapshot_install_streams_to_follower() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_test_writer()
        .try_init();

    let cluster = TestCluster::with_wire_transport_and_size(0, 3).await;
    let nodes = cluster.nodes();
    assert_eq!(nodes.len(), 3, "wire snapshot smoke needs a 3-node cluster");

    // Wait for cluster convergence so leader-only operations
    // (`persist_snapshot`, `send_snapshot_for_test`) target a node that
    // owns the system shard's leadership.
    let leader_id = cluster
        .wait_for_leader_agreement(Duration::from_secs(15))
        .await
        .expect("3-node wire cluster must converge on a leader before snapshot smoke runs");

    let leader_idx = nodes
        .iter()
        .position(|n| n.id == leader_id)
        .expect("elected leader must be a member of the cluster");
    let follower_idx = (0..nodes.len()).find(|&i| nodes[i].id != leader_id).expect("follower");
    let leader = &nodes[leader_idx];
    let follower = &nodes[follower_idx];

    // Resolve the system shard via the leader's manager.
    let system_group =
        leader.manager.system_region().expect("system region present on leader after convergence");
    let shard_id = system_group.handle().shard_id();

    // Persist a snapshot on the leader. The system group lives at
    // `(GLOBAL, OrganizationId(0))` — same scope the snapshot persister
    // organizes files under.
    let scope = SnapshotScope::Org { organization_id: OrganizationId::new(0) };
    // last_included_term = 1: persist_snapshot stamps this into the
    // produced snapshot's header. Term 1 is fine for a smoke test —
    // receiver-side validation does not gate on it.
    let last_included_term = 1u64;
    let persisted = leader
        .manager
        .persist_snapshot(
            Region::GLOBAL,
            OrganizationId::new(0),
            None,
            shard_id,
            last_included_term,
        )
        .await
        .expect("persist system-group snapshot on leader");

    // Dispatch the snapshot send the same way the consensus reactor would
    // when it processes Action::SendSnapshot. The helper spawns a task
    // and returns immediately — same fire-and-forget contract as the
    // production reactor path.
    leader.manager.send_snapshot_for_test(shard_id, NodeId(follower.id), persisted.index);

    // Poll the follower's staging directory until the `.snap.staged`
    // file lands. The wire sub-protocol writes to `.partial` mid-stream
    // and atomic-renames on footer + CRC validation; we only treat the
    // final `.staged` filename as success. Generous timeout so a slow
    // QUIC handshake / first-message connect doesn't trip the test.
    let persister = follower.manager.snapshot_persister();
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    let staged_meta = loop {
        if std::time::Instant::now() >= deadline {
            panic!(
                "follower never observed staged snapshot at index {} for scope {:?} within timeout",
                persisted.index, scope,
            );
        }
        match persister.list_staged(Region::GLOBAL, scope).await {
            Ok(staged) if !staged.is_empty() => {
                let matching = staged.iter().find(|m| m.index == persisted.index).cloned();
                if let Some(m) = matching {
                    break m;
                }
            },
            Ok(_) => {},
            Err(err) => panic!("follower list_staged failed: {err}"),
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };

    // The staged file's bytes-on-disk should match the size of the file
    // the leader streamed — the wire sub-protocol carries the verbatim
    // at-rest envelope (no decrypt-then-re-encrypt at the leader).
    assert_eq!(
        staged_meta.index, persisted.index,
        "staged snapshot index must match the index the leader streamed",
    );
    assert_eq!(
        staged_meta.region,
        Region::GLOBAL,
        "staged snapshot must inherit the leader's region",
    );
    assert!(
        staged_meta.size_bytes > 0,
        "staged snapshot must contain bytes (size = {})",
        staged_meta.size_bytes,
    );
    assert_eq!(
        staged_meta.size_bytes, persisted.size_bytes,
        "staged snapshot size must match the leader-persisted size; \
         the wire stream carries the at-rest envelope verbatim",
    );
}
