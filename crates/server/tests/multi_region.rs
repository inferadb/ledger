//! Multi-region integration tests.
//!
//! Tests write forwarding, read consistency, batch writes, and cross-region
//! operations using the `RegionTestCluster` infrastructure.
//!
//! These tests exercise the full gRPC path through `WriteService`
//! and `ReadService` with multi-region routing, validating that
//! organization→region routing, idempotency, and error handling work
//! correctly across region boundaries.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]

use std::time::Duration;

use inferadb_ledger_types::{OrganizationSlug, VaultSlug};

use crate::common::{TestCluster, TestNode, create_read_client, create_write_client};

// ============================================================================
// Test Helpers
// ============================================================================

/// Creates an organization on a multi-region cluster and returns its slug.
async fn create_organization(
    addr: &str,
    name: &str,
    node: &TestNode,
) -> Result<OrganizationSlug, Box<dyn std::error::Error>> {
    let (slug, _admin) = crate::common::create_test_organization(addr, name, node).await?;
    Ok(slug)
}

/// Creates a vault in an organization and returns its slug.
async fn create_vault(
    addr: &str,
    organization: OrganizationSlug,
) -> Result<VaultSlug, Box<dyn std::error::Error>> {
    crate::common::create_test_vault(addr, organization).await
}

/// Writes an entity and return the block height.
async fn write_entity(
    addr: &str,
    organization: OrganizationSlug,
    vault: VaultSlug,
    key: &str,
    value: &[u8],
) -> Result<u64, Box<dyn std::error::Error>> {
    let mut client = create_write_client(addr).await?;

    let request = inferadb_ledger_proto::proto::WriteRequest {
        organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
            slug: organization.value(),
        }),
        vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault.value() }),
        client_id: Some(inferadb_ledger_proto::proto::ClientId {
            id: "multi-region-test".to_string(),
        }),
        idempotency_key: uuid::Uuid::new_v4().as_bytes().to_vec(),
        operations: vec![inferadb_ledger_proto::proto::Operation {
            op: Some(inferadb_ledger_proto::proto::operation::Op::SetEntity(
                inferadb_ledger_proto::proto::SetEntity {
                    key: key.to_string(),
                    value: value.to_vec(),
                    expires_at: None,
                    condition: None,
                },
            )),
        }],
        include_tx_proof: false,
        caller: None,
    };

    let response = client.write(request).await?.into_inner();
    match response.result {
        Some(inferadb_ledger_proto::proto::write_response::Result::Success(s)) => {
            Ok(s.block_height)
        },
        Some(inferadb_ledger_proto::proto::write_response::Result::Error(e)) => {
            Err(format!("Write error: {:?}", e).into())
        },
        None => Err("No result in write response".into()),
    }
}

/// Reads an entity from a vault.
async fn read_entity(
    addr: &str,
    organization: OrganizationSlug,
    vault: VaultSlug,
    key: &str,
) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
    let mut client = create_read_client(addr).await?;

    let request = inferadb_ledger_proto::proto::ReadRequest {
        organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
            slug: organization.value(),
        }),
        vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault.value() }),
        key: key.to_string(),
        consistency: 0, // EVENTUAL
        caller: None,
    };

    let response = client.read(request).await?.into_inner();
    Ok(response.value)
}

// ============================================================================
// Multi-Region Integration Tests
// ============================================================================

/// Tests that writes to an organization are routed to the correct region and readable.
///
/// Exercises the full write→route→Raft→apply→read path through gRPC.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_multi_region_write_and_read() {
    let cluster = TestCluster::with_data_regions(1, 2).await;
    assert!(
        cluster.wait_for_leaders(Duration::from_secs(10)).await,
        "all regions should elect leaders"
    );

    let node = cluster.any_node();

    // Create organization (gets assigned to a data region)
    let ns_id =
        create_organization(&node.addr, "ms-write-read", node).await.expect("create organization");
    let vault = create_vault(&node.addr, ns_id).await.expect("create vault");

    // Write entity
    let height =
        write_entity(&node.addr, ns_id, vault, "key1", b"value1").await.expect("write entity");
    assert!(height > 0, "block height should be positive");

    // Read it back
    let value = read_entity(&node.addr, ns_id, vault, "key1").await.expect("read entity");
    assert_eq!(value, Some(b"value1".to_vec()), "should read back written value");
}

/// Tests that multiple organizations in different regions are isolated.
///
/// Writes to organization A should not be visible in organization B, even if both
/// are served by the same node.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_multi_region_organization_isolation() {
    let cluster = TestCluster::with_data_regions(1, 2).await;
    assert!(
        cluster.wait_for_leaders(Duration::from_secs(10)).await,
        "all regions should elect leaders"
    );

    let node = cluster.any_node();

    // Create two organizations (may land in different regions)
    let ns_a = create_organization(&node.addr, "isolated-a", node).await.expect("create ns A");
    let ns_b = create_organization(&node.addr, "isolated-b", node).await.expect("create ns B");

    let vault_a = create_vault(&node.addr, ns_a).await.expect("create vault A");
    let vault_b = create_vault(&node.addr, ns_b).await.expect("create vault B");

    // Write to organization A
    write_entity(&node.addr, ns_a, vault_a, "shared-key", b"value-a").await.expect("write to ns A");

    // Write different value to organization B with same key
    write_entity(&node.addr, ns_b, vault_b, "shared-key", b"value-b").await.expect("write to ns B");

    // Read from both — values should be independent
    let val_a = read_entity(&node.addr, ns_a, vault_a, "shared-key").await.expect("read from ns A");
    let val_b = read_entity(&node.addr, ns_b, vault_b, "shared-key").await.expect("read from ns B");

    assert_eq!(val_a, Some(b"value-a".to_vec()), "ns A should have its own value");
    assert_eq!(val_b, Some(b"value-b".to_vec()), "ns B should have its own value");
}

/// Tests sequential per-vault writes through the multi-region service.
///
/// Originally exercised `BatchWrite`; A5 migrated this to per-vault `Write`
/// after Phase 6 of the per-vault consensus migration deprecated cross-vault
/// batches. The atomicity invariant is sacrificed (each `Write` is its own
/// Raft proposal) but the read-after-write contract still holds — both keys
/// must be readable after the loop completes, and block heights must be
/// strictly monotonic in commit order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_multi_region_batch_write() {
    let cluster = TestCluster::with_data_regions(1, 2).await;
    assert!(
        cluster.wait_for_leaders(Duration::from_secs(10)).await,
        "all regions should elect leaders"
    );

    let node = cluster.any_node();

    let ns_id =
        create_organization(&node.addr, "ms-batch", node).await.expect("create organization");
    let vault = create_vault(&node.addr, ns_id).await.expect("create vault");

    // Submit per-vault writes sequentially. The block height must strictly
    // increase across calls because each `Write` is its own Raft proposal.
    let entries: &[(&str, &[u8])] =
        &[("batch-key-1", b"batch-val-1"), ("batch-key-2", b"batch-val-2")];

    let mut last_block_height: u64 = 0;
    for (key, value) in entries {
        let height = write_entity(&node.addr, ns_id, vault, key, value).await.expect("write");
        assert!(
            height > last_block_height,
            "block height should be monotonically increasing: got {height} after {last_block_height}"
        );
        last_block_height = height;
    }

    // Verify both keys are readable
    let val1 =
        read_entity(&node.addr, ns_id, vault, "batch-key-1").await.expect("read batch key 1");
    let val2 =
        read_entity(&node.addr, ns_id, vault, "batch-key-2").await.expect("read batch key 2");

    assert_eq!(val1, Some(b"batch-val-1".to_vec()), "first batch key should be readable");
    assert_eq!(val2, Some(b"batch-val-2".to_vec()), "second batch key should be readable");
}

/// Asserts that `BatchWrite` is rejected with a structured deprecation error.
///
/// Phase 6 of the per-vault consensus migration deprecated cross-vault
/// `BatchWrite`. The server must reject every request, regardless of
/// content, with `FAILED_PRECONDITION` and an `ErrorDetails` payload
/// carrying:
/// - `error_code` = `DiagnosticCode::AppDeprecated` (3209) as a string
/// - `is_retryable` = false
/// - `suggested_action` = "Use per-vault Write calls instead"
/// - `context.docs` pointing at the migration doc
///
/// Validates the structural contract the SDK relies on to surface a
/// non-retryable, actionable error rather than blindly retrying. The SDK
/// migration to per-vault `Write` is tracked separately (task A5).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_batch_write_returns_deprecation_error() {
    use inferadb_ledger_types::DiagnosticCode;
    use prost::Message as _;

    let cluster = TestCluster::with_data_regions(1, 2).await;
    assert!(
        cluster.wait_for_leaders(Duration::from_secs(10)).await,
        "all regions should elect leaders"
    );

    let node = cluster.any_node();
    let ns_id =
        create_organization(&node.addr, "ms-deprecated", node).await.expect("create organization");
    let vault = create_vault(&node.addr, ns_id).await.expect("create vault");

    let mut client = create_write_client(&node.addr).await.expect("connect");

    let request = inferadb_ledger_proto::proto::BatchWriteRequest {
        organization: Some(inferadb_ledger_proto::proto::OrganizationSlug { slug: ns_id.value() }),
        vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault.value() }),
        client_id: Some(inferadb_ledger_proto::proto::ClientId {
            id: "deprecation-client".to_string(),
        }),
        idempotency_key: uuid::Uuid::new_v4().as_bytes().to_vec(),
        operations: vec![inferadb_ledger_proto::proto::BatchWriteOperation {
            operations: vec![inferadb_ledger_proto::proto::Operation {
                op: Some(inferadb_ledger_proto::proto::operation::Op::SetEntity(
                    inferadb_ledger_proto::proto::SetEntity {
                        key: "should-be-rejected".to_string(),
                        value: b"unused".to_vec(),
                        expires_at: None,
                        condition: None,
                    },
                )),
            }],
        }],
        include_tx_proofs: false,
        caller: None,
    };

    let status =
        client.batch_write(request).await.expect_err("BatchWrite must be rejected as deprecated");
    assert_eq!(
        status.code(),
        tonic::Code::FailedPrecondition,
        "deprecation must surface as FAILED_PRECONDITION, got: {status:?}"
    );
    assert!(
        status.message().contains("deprecated"),
        "status message should call out deprecation, got: {:?}",
        status.message()
    );

    // ErrorDetails must be present and structurally correct.
    let details_bytes = status.details();
    assert!(
        !details_bytes.is_empty(),
        "deprecation status must carry ErrorDetails for SDK consumption"
    );
    let details = inferadb_ledger_proto::proto::ErrorDetails::decode(details_bytes)
        .expect("decode ErrorDetails");
    assert_eq!(
        details.error_code,
        DiagnosticCode::AppDeprecated.as_u16().to_string(),
        "ErrorDetails.error_code must be AppDeprecated (3209)"
    );
    assert!(!details.is_retryable, "deprecation is not retryable");
    assert_eq!(
        details.suggested_action.as_deref(),
        Some("Use per-vault Write calls instead"),
        "ErrorDetails must guide clients to per-vault Write"
    );
    assert_eq!(
        details.context.get("docs").map(String::as_str),
        Some("docs/architecture/per-vault-consensus.md"),
        "ErrorDetails.context must include docs pointer"
    );
}

/// Tests idempotency across multi-region writes.
///
/// Same client_id + idempotency_key should return cached result on retry.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_multi_region_write_idempotency() {
    let cluster = TestCluster::with_data_regions(1, 2).await;
    assert!(
        cluster.wait_for_leaders(Duration::from_secs(10)).await,
        "all regions should elect leaders"
    );

    let node = cluster.any_node();

    let ns_id =
        create_organization(&node.addr, "ms-idempotent", node).await.expect("create organization");
    let vault = create_vault(&node.addr, ns_id).await.expect("create vault");

    let mut client = create_write_client(&node.addr).await.expect("connect");
    let idempotency_key = uuid::Uuid::new_v4().as_bytes().to_vec();

    let request = inferadb_ledger_proto::proto::WriteRequest {
        organization: Some(inferadb_ledger_proto::proto::OrganizationSlug { slug: ns_id.value() }),
        vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault.value() }),
        client_id: Some(inferadb_ledger_proto::proto::ClientId { id: "idempotent-ms".to_string() }),
        idempotency_key: idempotency_key.clone(),
        operations: vec![inferadb_ledger_proto::proto::Operation {
            op: Some(inferadb_ledger_proto::proto::operation::Op::SetEntity(
                inferadb_ledger_proto::proto::SetEntity {
                    key: "idem-key".to_string(),
                    value: b"idem-value".to_vec(),
                    expires_at: None,
                    condition: None,
                },
            )),
        }],
        include_tx_proof: false,
        caller: None,
    };

    // First write
    let resp1 = client.write(request.clone()).await.expect("first write").into_inner();

    // Second write with same idempotency key
    let resp2 = client.write(request).await.expect("second write").into_inner();

    // Both should return identical results
    match (resp1.result, resp2.result) {
        (
            Some(inferadb_ledger_proto::proto::write_response::Result::Success(s1)),
            Some(inferadb_ledger_proto::proto::write_response::Result::Success(s2)),
        ) => {
            assert_eq!(s1.tx_id, s2.tx_id, "idempotent writes should return same tx_id");
            assert_eq!(
                s1.block_height, s2.block_height,
                "idempotent writes should return same block_height"
            );
        },
        _ => panic!("both writes should succeed"),
    }
}

/// Tests that writes to a non-existent organization return an appropriate error.
///
/// The multi-region service should reject writes for organizations that haven't
/// been created, rather than silently dropping them or panicking.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_multi_region_write_nonexistent_organization() {
    let cluster = TestCluster::with_data_regions(1, 2).await;
    assert!(
        cluster.wait_for_leaders(Duration::from_secs(10)).await,
        "all regions should elect leaders"
    );

    let node = cluster.any_node();
    let mut client = create_write_client(&node.addr).await.expect("connect");

    // Write to organization 99999 which doesn't exist
    let request = inferadb_ledger_proto::proto::WriteRequest {
        organization: Some(inferadb_ledger_proto::proto::OrganizationSlug { slug: 99999 }),
        vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: 1 }),
        client_id: Some(inferadb_ledger_proto::proto::ClientId {
            id: "nonexistent-ns".to_string(),
        }),
        idempotency_key: uuid::Uuid::new_v4().as_bytes().to_vec(),
        operations: vec![inferadb_ledger_proto::proto::Operation {
            op: Some(inferadb_ledger_proto::proto::operation::Op::SetEntity(
                inferadb_ledger_proto::proto::SetEntity {
                    key: "test".to_string(),
                    value: b"test".to_vec(),
                    expires_at: None,
                    condition: None,
                },
            )),
        }],
        include_tx_proof: false,
        caller: None,
    };

    let result = client.write(request).await;

    // Should get an error (either gRPC status error or WriteResponse error)
    match result {
        Ok(response) => {
            let inner = response.into_inner();
            match inner.result {
                Some(inferadb_ledger_proto::proto::write_response::Result::Error(_)) => {
                    // Expected: write error for nonexistent organization
                },
                Some(inferadb_ledger_proto::proto::write_response::Result::Success(_)) => {
                    panic!("write to nonexistent organization should not succeed");
                },
                None => {
                    panic!("expected error result for nonexistent organization");
                },
            }
        },
        Err(status) => {
            // gRPC-level error is also acceptable
            assert!(
                status.code() == tonic::Code::NotFound
                    || status.code() == tonic::Code::Internal
                    || status.code() == tonic::Code::FailedPrecondition
                    || status.code() == tonic::Code::InvalidArgument,
                "expected NOT_FOUND, INTERNAL, FAILED_PRECONDITION, or INVALID_ARGUMENT for nonexistent organization, got: {:?}",
                status.code()
            );
        },
    }
}

/// Tests concurrent writes to multiple organizations across regions.
///
/// Verifies that writes to different organizations can proceed in parallel
/// without interfering with each other.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_multi_region_concurrent_writes() {
    let cluster = TestCluster::with_data_regions(1, 2).await;
    assert!(
        cluster.wait_for_leaders(Duration::from_secs(10)).await,
        "all regions should elect leaders"
    );

    let node = cluster.any_node();

    // Create 3 organizations
    let mut organizations = Vec::new();
    for i in 0..3 {
        let ns_id = create_organization(&node.addr, &format!("concurrent-{}", i), node)
            .await
            .expect("create organization");
        let vault = create_vault(&node.addr, ns_id).await.expect("create vault");
        organizations.push((ns_id, vault));
    }

    // Spawn concurrent writes to all organizations
    let addr = node.addr.clone();
    let mut handles = Vec::new();

    for (i, &(ns_id, vault)) in organizations.iter().enumerate() {
        let addr = addr.clone();
        let handle = tokio::spawn(async move {
            for j in 0..5 {
                let key = format!("concurrent-{}-{}", i, j);
                let value = format!("value-{}-{}", i, j);
                write_entity(&addr, ns_id, vault, &key, value.as_bytes())
                    .await
                    .expect("concurrent write");
            }
        });
        handles.push(handle);
    }

    // Wait for all writes to complete
    for handle in handles {
        handle.await.expect("task should not panic");
    }

    // Verify all writes are readable
    for (i, &(ns_id, vault)) in organizations.iter().enumerate() {
        for j in 0..5 {
            let key = format!("concurrent-{}-{}", i, j);
            let expected = format!("value-{}-{}", i, j);
            let value =
                read_entity(&addr, ns_id, vault, &key).await.expect("read after concurrent writes");
            assert_eq!(
                value,
                Some(expected.into_bytes()),
                "concurrent write {}-{} should be readable",
                i,
                j
            );
        }
    }
}

// ============================================================================
// Write Forwarding Integration Tests
// ============================================================================

/// Tests that writes to a local-region follower node succeed.
///
/// In a 3-node cluster where all nodes host all regions, writing to any node
/// should succeed — the `resolve_with_redirect()` returns `Local` and the
/// Raft layer handles leader election internally. This verifies the new
/// forwarding code path is a transparent no-op for local organizations.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_write_forwarding_local_region_all_nodes() {
    // Single-node cluster with 2 data regions still uses RegionResolverService
    // (supports_forwarding=true) so the forwarding code path is exercised.
    let cluster = TestCluster::with_data_regions(1, 2).await;
    assert!(
        cluster.wait_for_leaders(Duration::from_secs(15)).await,
        "all regions should elect leaders"
    );

    let node = cluster.any_node();
    let ns_id =
        create_organization(&node.addr, "fwd-local-all", node).await.expect("create organization");
    let vault = create_vault(&node.addr, ns_id).await.expect("create vault");

    // Write through the forwarding-enabled resolver — resolve_with_redirect
    // returns Local because the single node hosts every region.
    let height = write_entity(&node.addr, ns_id, vault, "fwd-key-0", b"fwd-val-0")
        .await
        .expect("write should succeed through forwarding path");
    assert!(height > 0, "block height should be positive");

    // Verify readable
    tokio::time::sleep(Duration::from_millis(200)).await;
    let value = read_entity(&node.addr, ns_id, vault, "fwd-key-0").await.expect("read after write");
    assert_eq!(value, Some(b"fwd-val-0".to_vec()));
}

/// Tests sequential per-vault writes through the forwarding-enabled write service.
///
/// Originally exercised `BatchWrite` against a non-leader; A5 migrated this
/// to per-vault `Write` after Phase 6 of the per-vault consensus migration
/// deprecated cross-vault batches. Each `Write` may itself be forwarded /
/// redirected to the per-vault leader; the test asserts the entries are
/// readable after the writes commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_batch_write_forwarding_local_region() {
    let cluster = TestCluster::with_data_regions(1, 2).await;
    assert!(
        cluster.wait_for_leaders(Duration::from_secs(15)).await,
        "all regions should elect leaders"
    );

    let node = cluster.any_node();
    let ns_id = create_organization(&node.addr, "fwd-batch-local", node)
        .await
        .expect("create organization");
    let vault = create_vault(&node.addr, ns_id).await.expect("create vault");

    let entries: &[(&str, &[u8])] =
        &[("batch-fwd-1", b"batch-fwd-val-1"), ("batch-fwd-2", b"batch-fwd-val-2")];

    for (key, value) in entries {
        let height =
            write_entity(&node.addr, ns_id, vault, key, value).await.expect("write to non-leader");
        assert!(height > 0, "write should produce a block");
    }

    // Verify readable
    tokio::time::sleep(Duration::from_millis(200)).await;
    let val1 =
        read_entity(&node.addr, ns_id, vault, "batch-fwd-1").await.expect("read batch key 1");
    let val2 =
        read_entity(&node.addr, ns_id, vault, "batch-fwd-2").await.expect("read batch key 2");

    assert_eq!(val1, Some(b"batch-fwd-val-1".to_vec()));
    assert_eq!(val2, Some(b"batch-fwd-val-2".to_vec()));
}
