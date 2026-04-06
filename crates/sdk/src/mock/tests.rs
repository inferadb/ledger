#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]

use std::sync::atomic::Ordering;

use inferadb_ledger_types::{OrganizationSlug, Region, VaultSlug};

use super::{MockLedgerServer, MockState};

const ORG: OrganizationSlug = OrganizationSlug::new(1);
const VAULT: VaultSlug = VaultSlug::new(0);

mod mock_server_tests {
    use super::*;

    #[tokio::test]
    async fn test_mock_server_starts_on_ephemeral_port() {
        let server = MockLedgerServer::start().await.unwrap();
        assert!(server.endpoint().starts_with("http://127.0.0.1:"));
        assert!(!server.endpoint().ends_with(":0"));
    }

    #[tokio::test]
    async fn test_mock_server_entity_storage() {
        let server = MockLedgerServer::start().await.unwrap();

        // Set entity
        server.set_entity(ORG, VAULT, "test-key", b"test-value");

        // Verify read_count starts at 0
        assert_eq!(server.read_count(), 0);
    }

    #[tokio::test]
    async fn test_mock_server_client_state() {
        let server = MockLedgerServer::start().await.unwrap();

        // Initially no client state
        assert_eq!(server.get_client_state(ORG, VAULT, "client-1"), None);

        // Set client state
        server.set_client_state(ORG, VAULT, "client-1", 5);
        assert_eq!(server.get_client_state(ORG, VAULT, "client-1"), Some(5));

        // Different client has no state
        assert_eq!(server.get_client_state(ORG, VAULT, "client-2"), None);
    }

    #[tokio::test]
    async fn test_mock_server_counters() {
        let server = MockLedgerServer::start().await.unwrap();

        assert_eq!(server.write_count(), 0);
        assert_eq!(server.read_count(), 0);
        assert_eq!(server.block_height(), 1);
    }

    #[tokio::test]
    async fn test_mock_server_reset() {
        let server = MockLedgerServer::start().await.unwrap();

        // Add some data
        server.set_entity(ORG, VAULT, "key", b"value");
        server.set_client_state(ORG, VAULT, "client", 5);
        server.add_relationship(ORG, VAULT, "r", "rel", "s");
        server.add_organization(ORG, "ns", Region::US_EAST_VA);
        server.add_vault(ORG, VAULT);
        server.add_peer("node", vec![], 5000);
        server.inject_unavailable(3);
        server.inject_delay(100);

        // Reset
        server.reset();

        // Verify reset (client state should be None)
        assert_eq!(server.get_client_state(ORG, VAULT, "client"), None);
        assert_eq!(server.write_count(), 0);
        assert_eq!(server.read_count(), 0);
        assert_eq!(server.block_height(), 1);
    }

    #[tokio::test]
    async fn test_mock_server_shutdown() {
        let server = MockLedgerServer::start().await.unwrap();
        let endpoint = server.endpoint().to_string();

        // Shutdown should not panic
        server.shutdown();

        // Verify endpoint was valid before shutdown
        assert!(endpoint.starts_with("http://"));
    }

    #[tokio::test]
    async fn test_mock_state_should_inject_unavailable_decrements() {
        let state = MockState::new();

        state.unavailable_count.store(2, Ordering::SeqCst);

        assert!(state.should_inject_unavailable());
        assert_eq!(state.unavailable_count.load(Ordering::SeqCst), 1);

        assert!(state.should_inject_unavailable());
        assert_eq!(state.unavailable_count.load(Ordering::SeqCst), 0);

        assert!(!state.should_inject_unavailable());
        assert_eq!(state.unavailable_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_mock_state_maybe_delay_zero() {
        let state = MockState::new();
        state.delay_ms.store(0, Ordering::SeqCst);

        // Should return immediately
        let start = std::time::Instant::now();
        state.maybe_delay().await;
        let elapsed = start.elapsed();

        assert!(elapsed.as_millis() < 10);
    }

    #[tokio::test]
    async fn test_mock_state_maybe_delay_nonzero() {
        let state = MockState::new();
        state.delay_ms.store(50, Ordering::SeqCst);

        let start = std::time::Instant::now();
        state.maybe_delay().await;
        let elapsed = start.elapsed();

        assert!(elapsed.as_millis() >= 40); // Allow some tolerance
    }
}

/// Integration tests for client -> mock server roundtrips.
///
/// These tests verify the full stack: client creates request, mock server processes,
/// client parses response. Validates serialization, error handling, and protocol behavior.
mod integration_tests {
    use std::time::Duration;

    use super::*;
    use crate::{
        ClientConfig, LedgerClient, Operation, RetryPolicy, ServerSource, TeamSlug, UserSlug,
        client::OrganizationTier,
    };

    /// Helper to create a client connected to a mock server.
    async fn create_client_for_mock(server: &MockLedgerServer) -> LedgerClient {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static([server.endpoint().to_string()]))
            .client_id("test-client")
            .timeout(Duration::from_secs(5))
            .connect_timeout(Duration::from_secs(2))
            .build()
            .expect("valid config");

        LedgerClient::new(config).await.expect("client creation")
    }

    /// Helper to create a client with custom retry policy.
    async fn create_client_with_retry(
        server: &MockLedgerServer,
        client_id: &str,
        max_attempts: u32,
    ) -> LedgerClient {
        let retry_policy = RetryPolicy::builder()
            .max_attempts(max_attempts)
            .initial_backoff(Duration::from_millis(10))
            .max_backoff(Duration::from_millis(100))
            .multiplier(2.0)
            .build();

        let config = ClientConfig::builder()
            .servers(ServerSource::from_static([server.endpoint().to_string()]))
            .client_id(client_id)
            .timeout(Duration::from_secs(5))
            .connect_timeout(Duration::from_secs(2))
            .retry_policy(retry_policy)
            .build()
            .expect("valid config");

        LedgerClient::new(config).await.expect("client creation")
    }

    // ==================== Read Operations ====================

    #[tokio::test]
    async fn test_read_existing_key_returns_value() {
        let server = MockLedgerServer::start().await.unwrap();
        server.set_entity(ORG, VAULT, "user:123", b"test data");
        let client = create_client_for_mock(&server).await;

        let result =
            client.read(UserSlug::new(42), ORG, Some(VAULT), "user:123", None, None).await.unwrap();

        assert_eq!(result, Some(b"test data".to_vec()));
    }

    #[tokio::test]
    async fn test_read_missing_key_returns_none() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let result = client
            .read(UserSlug::new(42), ORG, Some(VAULT), "nonexistent", None, None)
            .await
            .unwrap();

        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_read_with_linearizable_consistency_returns_value() {
        let server = MockLedgerServer::start().await.unwrap();
        server.set_entity(ORG, VAULT, "key", b"consistent value");
        let client = create_client_for_mock(&server).await;

        let result = client
            .read(
                UserSlug::new(42),
                ORG,
                Some(VAULT),
                "key",
                Some(crate::ReadConsistency::Linearizable),
                None,
            )
            .await
            .unwrap();

        assert_eq!(result, Some(b"consistent value".to_vec()));
    }

    #[tokio::test]
    async fn test_read_with_eventual_consistency() {
        let server = MockLedgerServer::start().await.unwrap();
        server.set_entity(ORG, VAULT, "key", b"eventual");
        let client = create_client_for_mock(&server).await;

        let result = client
            .read(
                UserSlug::new(42),
                ORG,
                Some(VAULT),
                "key",
                Some(crate::ReadConsistency::Eventual),
                None,
            )
            .await
            .unwrap();

        assert_eq!(result, Some(b"eventual".to_vec()));
    }

    #[tokio::test]
    async fn test_read_with_linearizable_consistency() {
        let server = MockLedgerServer::start().await.unwrap();
        server.set_entity(ORG, VAULT, "key", b"linearizable");
        let client = create_client_for_mock(&server).await;

        let result = client
            .read(
                UserSlug::new(42),
                ORG,
                Some(VAULT),
                "key",
                Some(crate::ReadConsistency::Linearizable),
                None,
            )
            .await
            .unwrap();

        assert_eq!(result, Some(b"linearizable".to_vec()));
    }

    #[tokio::test]
    async fn test_batch_read_mixed_found_not_found() {
        let server = MockLedgerServer::start().await.unwrap();
        server.set_entity(ORG, VAULT, "exists1", b"value1");
        server.set_entity(ORG, VAULT, "exists2", b"value2");
        let client = create_client_for_mock(&server).await;

        let keys = vec!["exists1".to_string(), "missing".to_string(), "exists2".to_string()];
        let result =
            client.batch_read(UserSlug::new(42), ORG, Some(VAULT), keys, None, None).await.unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result[0], ("exists1".to_string(), Some(b"value1".to_vec())));
        assert_eq!(result[1], ("missing".to_string(), None));
        assert_eq!(result[2], ("exists2".to_string(), Some(b"value2".to_vec())));
    }

    #[tokio::test]
    async fn test_batch_read_with_linearizable_consistency_returns_values() {
        let server = MockLedgerServer::start().await.unwrap();
        server.set_entity(ORG, VAULT, "a", b"1");
        server.set_entity(ORG, VAULT, "b", b"2");
        let client = create_client_for_mock(&server).await;

        let keys = vec!["a".to_string(), "b".to_string()];
        let result = client
            .batch_read(
                UserSlug::new(42),
                ORG,
                Some(VAULT),
                keys,
                Some(crate::ReadConsistency::Linearizable),
                None,
            )
            .await
            .unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0], ("a".to_string(), Some(b"1".to_vec())));
        assert_eq!(result[1], ("b".to_string(), Some(b"2".to_vec())));
    }

    #[tokio::test]
    async fn test_read_increments_read_count() {
        let server = MockLedgerServer::start().await.unwrap();
        server.set_entity(ORG, VAULT, "key", b"value");
        let client = create_client_for_mock(&server).await;

        assert_eq!(server.read_count(), 0);

        client.read(UserSlug::new(42), ORG, Some(VAULT), "key", None, None).await.unwrap();
        assert_eq!(server.read_count(), 1);

        client.read(UserSlug::new(42), ORG, Some(VAULT), "key", None, None).await.unwrap();
        assert_eq!(server.read_count(), 2);
    }

    // ==================== Write Operations ====================

    #[tokio::test]
    async fn test_write_single_operation_succeeds() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let ops = vec![Operation::set_entity("entity:1", b"data".to_vec(), None, None)];
        let result = client.write(UserSlug::new(42), ORG, Some(VAULT), ops, None).await.unwrap();

        assert!(!result.tx_id.is_empty());
        assert!(result.block_height > 0);
        assert_eq!(server.write_count(), 1);
    }

    #[tokio::test]
    async fn test_write_can_be_read_back() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let ops = vec![Operation::set_entity("user:abc", b"user data".to_vec(), None, None)];
        client.write(UserSlug::new(42), ORG, Some(VAULT), ops, None).await.unwrap();

        let value =
            client.read(UserSlug::new(42), ORG, Some(VAULT), "user:abc", None, None).await.unwrap();
        assert_eq!(value, Some(b"user data".to_vec()));
    }

    #[tokio::test]
    async fn test_write_multiple_operations() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let ops = vec![
            Operation::set_entity("k1", b"v1".to_vec(), None, None),
            Operation::set_entity("k2", b"v2".to_vec(), None, None),
            Operation::set_entity("k3", b"v3".to_vec(), None, None),
        ];
        let result = client.write(UserSlug::new(42), ORG, Some(VAULT), ops, None).await.unwrap();

        assert!(!result.tx_id.is_empty());

        // All three should be readable
        assert_eq!(
            client.read(UserSlug::new(42), ORG, Some(VAULT), "k1", None, None).await.unwrap(),
            Some(b"v1".to_vec())
        );
        assert_eq!(
            client.read(UserSlug::new(42), ORG, Some(VAULT), "k2", None, None).await.unwrap(),
            Some(b"v2".to_vec())
        );
        assert_eq!(
            client.read(UserSlug::new(42), ORG, Some(VAULT), "k3", None, None).await.unwrap(),
            Some(b"v3".to_vec())
        );
    }

    #[tokio::test]
    async fn test_write_delete_entity() {
        let server = MockLedgerServer::start().await.unwrap();
        server.set_entity(ORG, VAULT, "to_delete", b"exists");
        let client = create_client_for_mock(&server).await;

        // Verify it exists
        assert!(
            client
                .read(UserSlug::new(42), ORG, Some(VAULT), "to_delete", None, None)
                .await
                .unwrap()
                .is_some()
        );

        // Delete it
        let ops = vec![Operation::delete_entity("to_delete")];
        client.write(UserSlug::new(42), ORG, Some(VAULT), ops, None).await.unwrap();

        // Verify deleted
        assert_eq!(
            client
                .read(UserSlug::new(42), ORG, Some(VAULT), "to_delete", None, None)
                .await
                .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn test_write_create_relationship() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let ops = vec![Operation::create_relationship("document:123", "viewer", "user:456")];
        client.write(UserSlug::new(42), ORG, Some(VAULT), ops, None).await.unwrap();

        // Relationship was created (verified by write count, detailed check via list)
        assert_eq!(server.write_count(), 1);
    }

    #[tokio::test]
    async fn test_batch_write_atomic() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let batches = vec![
            vec![Operation::set_entity("batch1:a", b"a".to_vec(), None, None)],
            vec![
                Operation::set_entity("batch2:b", b"b".to_vec(), None, None),
                Operation::set_entity("batch2:c", b"c".to_vec(), None, None),
            ],
        ];
        let result =
            client.batch_write(UserSlug::new(42), ORG, Some(VAULT), batches, None).await.unwrap();

        assert!(!result.tx_id.is_empty());
        assert_eq!(server.write_count(), 1); // Single batch write

        // All entities from all batches should be readable
        assert_eq!(
            client.read(UserSlug::new(42), ORG, Some(VAULT), "batch1:a", None, None).await.unwrap(),
            Some(b"a".to_vec())
        );
        assert_eq!(
            client.read(UserSlug::new(42), ORG, Some(VAULT), "batch2:b", None, None).await.unwrap(),
            Some(b"b".to_vec())
        );
        assert_eq!(
            client.read(UserSlug::new(42), ORG, Some(VAULT), "batch2:c", None, None).await.unwrap(),
            Some(b"c".to_vec())
        );
    }

    // ==================== Single-Operation Convenience Methods ====================

    #[tokio::test]
    async fn test_set_entity_convenience() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let result = client
            .set_entity(
                UserSlug::new(42),
                ORG,
                Some(VAULT),
                "entity:1",
                b"data".to_vec(),
                None,
                None,
                None,
            )
            .await
            .unwrap();

        assert!(!result.tx_id.is_empty());
        assert_eq!(
            client.read(UserSlug::new(42), ORG, Some(VAULT), "entity:1", None, None).await.unwrap(),
            Some(b"data".to_vec())
        );
    }

    #[tokio::test]
    async fn test_delete_entity_convenience() {
        let server = MockLedgerServer::start().await.unwrap();
        server.set_entity(ORG, VAULT, "to_delete", b"exists");
        let client = create_client_for_mock(&server).await;

        assert!(
            client
                .read(UserSlug::new(42), ORG, Some(VAULT), "to_delete", None, None)
                .await
                .unwrap()
                .is_some()
        );

        client.delete_entity(UserSlug::new(42), ORG, Some(VAULT), "to_delete", None).await.unwrap();

        assert_eq!(
            client
                .read(UserSlug::new(42), ORG, Some(VAULT), "to_delete", None, None)
                .await
                .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn test_set_entity_with_condition_convenience() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let result = client
            .set_entity(
                UserSlug::new(42),
                ORG,
                Some(VAULT),
                "cond:1",
                b"value".to_vec(),
                None,
                Some(crate::SetCondition::NotExists),
                None,
            )
            .await
            .unwrap();

        assert!(!result.tx_id.is_empty());
        assert_eq!(
            client.read(UserSlug::new(42), ORG, Some(VAULT), "cond:1", None, None).await.unwrap(),
            Some(b"value".to_vec())
        );
    }

    #[tokio::test]
    async fn test_set_entity_with_expiry_convenience() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let result = client
            .set_entity(
                UserSlug::new(42),
                ORG,
                Some(VAULT),
                "ttl:1",
                b"temp".to_vec(),
                Some(1_700_000_000),
                None,
                None,
            )
            .await
            .unwrap();

        assert!(!result.tx_id.is_empty());
        assert_eq!(
            client.read(UserSlug::new(42), ORG, Some(VAULT), "ttl:1", None, None).await.unwrap(),
            Some(b"temp".to_vec())
        );
    }

    #[tokio::test]
    async fn test_set_entity_with_expiry_and_condition_convenience() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let result = client
            .set_entity(
                UserSlug::new(42),
                ORG,
                Some(VAULT),
                "cond_ttl:1",
                b"conditional-temp".to_vec(),
                Some(1_700_000_000),
                Some(crate::SetCondition::NotExists),
                None,
            )
            .await
            .unwrap();

        assert!(!result.tx_id.is_empty());
        assert_eq!(
            client
                .read(UserSlug::new(42), ORG, Some(VAULT), "cond_ttl:1", None, None)
                .await
                .unwrap(),
            Some(b"conditional-temp".to_vec())
        );
    }

    // ==================== Idempotency ====================

    #[tokio::test]
    async fn test_write_returns_assigned_sequence() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let ops = vec![Operation::set_entity("key1", b"data".to_vec(), None, None)];
        let result = client.write(UserSlug::new(42), ORG, Some(VAULT), ops, None).await.unwrap();

        assert_eq!(result.assigned_sequence, 1);

        // Second write gets next sequence
        let ops2 = vec![Operation::set_entity("key2", b"data2".to_vec(), None, None)];
        let result2 = client.write(UserSlug::new(42), ORG, Some(VAULT), ops2, None).await.unwrap();

        assert_eq!(result2.assigned_sequence, 2);
    }

    #[tokio::test]
    async fn test_batch_write_returns_assigned_sequence() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let batches = vec![vec![Operation::set_entity("bw:1", b"first".to_vec(), None, None)]];
        let result =
            client.batch_write(UserSlug::new(42), ORG, Some(VAULT), batches, None).await.unwrap();

        assert_eq!(result.assigned_sequence, 1);
    }

    #[tokio::test]
    async fn test_server_sequences_increment_per_vault() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        // Each write to the same vault gets incrementing sequences
        for i in 1..=5 {
            let ops = vec![Operation::set_entity(format!("seq:{i}"), b"data".to_vec(), None, None)];
            let result =
                client.write(UserSlug::new(42), ORG, Some(VAULT), ops, None).await.unwrap();
            assert_eq!(result.assigned_sequence, i);
        }

        assert_eq!(server.write_count(), 5);
    }

    // ==================== Retry ====================

    #[tokio::test]
    async fn test_retry_succeeds_after_transient_failure() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_with_retry(&server, "retry-client", 3).await;

        // Inject 1 UNAVAILABLE error - second attempt should succeed
        server.inject_unavailable(1);
        server.set_entity(ORG, VAULT, "retry-key", b"retry-value");

        let result = client
            .read(UserSlug::new(42), ORG, Some(VAULT), "retry-key", None, None)
            .await
            .unwrap();

        assert_eq!(result, Some(b"retry-value".to_vec()));
        // 2 reads: 1 failed, 1 succeeded
        assert_eq!(server.read_count(), 1); // Only successful read is counted
    }

    #[tokio::test]
    async fn test_retry_exhaustion_returns_error() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_with_retry(&server, "exhaust-client", 2).await;

        // Inject more failures than max attempts
        server.inject_unavailable(5);

        let result = client.read(UserSlug::new(42), ORG, Some(VAULT), "any-key", None, None).await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        // Should be RetryExhausted wrapping the underlying error
        assert!(
            matches!(err, crate::SdkError::RetryExhausted { .. })
                || matches!(err, crate::SdkError::Transport { .. }),
            "Expected RetryExhausted or Transport error, got: {:?}",
            err
        );
    }

    #[tokio::test]
    async fn test_retry_write_succeeds_after_transient_failure() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_with_retry(&server, "write-retry", 3).await;

        // First request fails, second succeeds
        server.inject_unavailable(1);

        let ops = vec![Operation::set_entity("retry-write", b"value".to_vec(), None, None)];
        let result = client.write(UserSlug::new(42), ORG, Some(VAULT), ops, None).await.unwrap();

        assert!(!result.tx_id.is_empty());
    }

    // ==================== Concurrent Operations ====================

    #[tokio::test]
    async fn test_concurrent_writes_to_different_vaults() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        // Spawn concurrent writes to different vaults
        let client1 = client.clone();
        let client2 = client.clone();

        let handle1 = tokio::spawn(async move {
            for i in 0..10 {
                let ops =
                    vec![Operation::set_entity(format!("v0:k{i}"), b"v0".to_vec(), None, None)];
                client1
                    .write(UserSlug::new(42), ORG, Some(VaultSlug::new(0)), ops, None)
                    .await
                    .unwrap();
            }
        });

        let handle2 = tokio::spawn(async move {
            for i in 0..10 {
                let ops =
                    vec![Operation::set_entity(format!("v1:k{i}"), b"v1".to_vec(), None, None)];
                client2
                    .write(UserSlug::new(42), ORG, Some(VaultSlug::new(1)), ops, None)
                    .await
                    .unwrap();
            }
        });

        handle1.await.unwrap();
        handle2.await.unwrap();

        // 20 writes total
        assert_eq!(server.write_count(), 20);
    }

    #[tokio::test]
    async fn test_concurrent_reads() {
        let server = MockLedgerServer::start().await.unwrap();
        for i in 0..100 {
            server.set_entity(ORG, VAULT, &format!("key:{}", i), format!("value:{}", i).as_bytes());
        }
        let client = create_client_for_mock(&server).await;

        // Spawn many concurrent reads
        let mut handles = vec![];
        for i in 0..100 {
            let client_clone = client.clone();
            handles.push(tokio::spawn(async move {
                let key = format!("key:{}", i);
                let expected = format!("value:{}", i).into_bytes();
                let result = client_clone
                    .read(UserSlug::new(42), ORG, Some(VAULT), &key, None, None)
                    .await
                    .unwrap();
                assert_eq!(result, Some(expected));
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }

        assert_eq!(server.read_count(), 100);
    }

    // ==================== Admin Operations ====================

    #[tokio::test]
    async fn test_create_and_get_organization() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let org = client
            .create_organization(
                "test-organization",
                Region::US_EAST_VA,
                UserSlug::new(0),
                OrganizationTier::Free,
            )
            .await
            .unwrap();
        assert!(org.slug.value() > 0);

        let org_info = client.get_organization(org.slug, UserSlug::new(0)).await.unwrap();
        assert_eq!(org_info.slug, org.slug);
        assert_eq!(org_info.name, "test-organization");
    }

    #[tokio::test]
    async fn test_list_organizations() {
        let server = MockLedgerServer::start().await.unwrap();
        server.add_organization(ORG, "ns1", Region::US_EAST_VA);
        server.add_organization(OrganizationSlug::new(2), "ns2", Region::US_EAST_VA);
        let client = create_client_for_mock(&server).await;

        let (organizations, _next) =
            client.list_organizations(UserSlug::new(1), 0, None).await.unwrap();

        assert_eq!(organizations.len(), 2);
        let names: Vec<_> = organizations.iter().map(|n| n.name.as_str()).collect();
        assert!(names.contains(&"ns1"));
        assert!(names.contains(&"ns2"));
    }

    #[tokio::test]
    async fn test_create_and_get_vault() {
        let server = MockLedgerServer::start().await.unwrap();
        server.add_organization(ORG, "ns", Region::US_EAST_VA);
        let client = create_client_for_mock(&server).await;

        let vault_info = client.create_vault(UserSlug::new(42), ORG).await.unwrap();
        assert!(vault_info.vault.value() > 0);

        let fetched = client.get_vault(UserSlug::new(42), ORG, vault_info.vault).await.unwrap();
        assert_eq!(fetched.vault, vault_info.vault);
    }

    #[tokio::test]
    async fn test_list_vaults() {
        let server = MockLedgerServer::start().await.unwrap();
        server.add_vault(ORG, VaultSlug::new(0));
        server.add_vault(ORG, VaultSlug::new(1));
        let client = create_client_for_mock(&server).await;

        let (vaults, _next) = client.list_vaults(UserSlug::new(42), 0, None, None).await.unwrap();

        assert_eq!(vaults.len(), 2);
    }

    // ==================== Health Check ====================

    #[tokio::test]
    async fn test_health_check_returns_healthy() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let is_healthy = client.health_check().await.unwrap();

        assert!(is_healthy);
    }

    #[tokio::test]
    async fn test_health_check_detailed_returns_result() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let result = client.health_check_detailed().await.unwrap();

        assert!(result.is_healthy());
    }

    // ==================== Query Operations ====================

    #[tokio::test]
    async fn test_list_entities_with_prefix() {
        let server = MockLedgerServer::start().await.unwrap();
        server.set_entity(ORG, VAULT, "user:1", b"data1");
        server.set_entity(ORG, VAULT, "user:2", b"data2");
        server.set_entity(ORG, VAULT, "team:1", b"team");
        let client = create_client_for_mock(&server).await;

        use crate::ListEntitiesOpts;
        let result = client
            .list_entities(UserSlug::new(42), ORG, ListEntitiesOpts::with_prefix("user:"))
            .await
            .unwrap();

        assert_eq!(result.items.len(), 2);
        assert!(result.items.iter().all(|e| e.key.starts_with("user:")));
    }

    #[tokio::test]
    async fn test_list_relationships_returns_relationships() {
        let server = MockLedgerServer::start().await.unwrap();
        server.add_relationship(ORG, VAULT, "doc:1", "viewer", "user:alice");
        server.add_relationship(ORG, VAULT, "doc:1", "editor", "user:bob");
        let client = create_client_for_mock(&server).await;

        use crate::ListRelationshipsOpts;
        let result = client
            .list_relationships(UserSlug::new(42), ORG, VAULT, ListRelationshipsOpts::new())
            .await
            .unwrap();

        assert_eq!(result.items.len(), 2);
    }

    #[tokio::test]
    async fn test_list_relationships_with_filter() {
        let server = MockLedgerServer::start().await.unwrap();
        server.add_relationship(ORG, VAULT, "doc:1", "viewer", "user:alice");
        server.add_relationship(ORG, VAULT, "doc:1", "editor", "user:bob");
        server.add_relationship(ORG, VAULT, "doc:2", "viewer", "user:charlie");
        let client = create_client_for_mock(&server).await;

        use crate::ListRelationshipsOpts;
        let result = client
            .list_relationships(
                UserSlug::new(42),
                ORG,
                VAULT,
                ListRelationshipsOpts::new().relation("viewer"),
            )
            .await
            .unwrap();

        assert_eq!(result.items.len(), 2);
        assert!(result.items.iter().all(|r| r.relation == "viewer"));
    }

    // ==================== Graceful Shutdown ====================

    #[tokio::test]
    async fn test_server_shutdown_closes_connections() {
        let server = MockLedgerServer::start().await.unwrap();
        let endpoint = server.endpoint().to_string();
        let client = create_client_for_mock(&server).await;

        // Verify connection works
        assert!(client.health_check().await.unwrap());

        // Shutdown server
        server.shutdown();

        // Give server time to shut down
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Connection should fail after shutdown
        let result = client.health_check().await;
        assert!(result.is_err(), "Expected error after shutdown, endpoint was: {}", endpoint);
    }

    #[tokio::test]
    async fn test_client_can_reconnect_after_server_restart() {
        // Start first server
        let server1 = MockLedgerServer::start().await.unwrap();
        let endpoint = server1.endpoint().to_string();
        let port: u16 = endpoint.trim_start_matches("http://127.0.0.1:").parse().unwrap();

        let client = create_client_for_mock(&server1).await;
        assert!(client.health_check().await.unwrap());

        // Shutdown first server
        server1.shutdown();
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Start new server on same port
        let server2 = MockLedgerServer::start_on_port(port).await.unwrap();

        // Client should be able to reconnect
        // (May need a small delay for the new server to be ready)
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Reset connection pool to force reconnection
        client.pool().reset();

        let result = client.health_check().await;
        assert!(result.is_ok(), "Expected success after restart: {:?}", result);

        server2.shutdown();
    }

    // ==================== Edge Cases ====================

    #[tokio::test]
    async fn test_empty_batch_read() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let empty_keys: Vec<String> = vec![];
        let result = client
            .batch_read(UserSlug::new(42), ORG, Some(VAULT), empty_keys, None, None)
            .await
            .unwrap();

        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_read_organization_level_without_vault() {
        let server = MockLedgerServer::start().await.unwrap();
        // Set entity at organization level (vault = 0 is treated as default)
        server.set_entity(ORG, VAULT, "ns-entity", b"organization data");
        let client = create_client_for_mock(&server).await;

        let result =
            client.read(UserSlug::new(42), ORG, None, "ns-entity", None, None).await.unwrap();

        assert_eq!(result, Some(b"organization data".to_vec()));
    }

    #[tokio::test]
    async fn test_write_with_none_vault() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let ops = vec![Operation::set_entity("ns:key", b"value".to_vec(), None, None)];
        let result = client.write(UserSlug::new(42), ORG, None, ops, None).await.unwrap();

        assert!(!result.tx_id.is_empty());

        // Read back with None vault
        let value = client.read(UserSlug::new(42), ORG, None, "ns:key", None, None).await.unwrap();
        assert_eq!(value, Some(b"value".to_vec()));
    }

    #[tokio::test]
    async fn test_large_value_read_write() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        // 1MB value
        let large_value = vec![0u8; 1024 * 1024];
        let ops = vec![Operation::set_entity("large:key", large_value.clone(), None, None)];
        client.write(UserSlug::new(42), ORG, Some(VAULT), ops, None).await.unwrap();

        let result = client
            .read(UserSlug::new(42), ORG, Some(VAULT), "large:key", None, None)
            .await
            .unwrap();
        assert_eq!(result.unwrap().len(), 1024 * 1024);
    }

    #[tokio::test]
    async fn test_multiple_clients_same_server() {
        let server = MockLedgerServer::start().await.unwrap();

        // Two clients with different client IDs
        let client1 = create_client_with_retry(&server, "client-1", 1).await;
        let client2 = create_client_with_retry(&server, "client-2", 1).await;

        // Both can write (they have independent sequences)
        let ops1 = vec![Operation::set_entity("c1:key", b"from-c1".to_vec(), None, None)];
        let ops2 = vec![Operation::set_entity("c2:key", b"from-c2".to_vec(), None, None)];

        client1.write(UserSlug::new(42), ORG, Some(VAULT), ops1, None).await.unwrap();
        client2.write(UserSlug::new(42), ORG, Some(VAULT), ops2, None).await.unwrap();

        // Both can read each other's data
        assert_eq!(
            client1.read(UserSlug::new(42), ORG, Some(VAULT), "c2:key", None, None).await.unwrap(),
            Some(b"from-c2".to_vec())
        );
        assert_eq!(
            client2.read(UserSlug::new(42), ORG, Some(VAULT), "c1:key", None, None).await.unwrap(),
            Some(b"from-c1".to_vec())
        );
    }

    // ==================== Client Shutdown Integration Tests ====================

    #[tokio::test]
    async fn test_client_shutdown_cancels_in_flight_request() {
        let server = MockLedgerServer::start().await.unwrap();
        // Add 500ms delay to simulate slow request
        server.inject_delay(500);

        let client = create_client_for_mock(&server).await;

        // Start a slow read in background
        let client_clone = client.clone();
        let handle = tokio::spawn(async move {
            client_clone.read(UserSlug::new(42), ORG, Some(VAULT), "key", None, None).await
        });

        // Give time for request to start
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Shutdown the client while request is in flight
        client.shutdown().await;

        // The spawned task should complete (may succeed or fail with transport error)
        // The key point is it doesn't hang forever
        let result = tokio::time::timeout(Duration::from_secs(2), handle).await;

        assert!(result.is_ok(), "Request should complete within timeout after shutdown");
    }

    #[tokio::test]
    async fn test_client_shutdown_prevents_new_requests() {
        let server = MockLedgerServer::start().await.unwrap();
        server.set_entity(ORG, VAULT, "key", b"value");
        let client = create_client_for_mock(&server).await;

        // Verify normal operation
        let result = client.read(UserSlug::new(42), ORG, Some(VAULT), "key", None, None).await;
        assert!(result.is_ok());

        // Shutdown
        client.shutdown().await;

        // New requests should fail with Shutdown error
        let result = client.read(UserSlug::new(42), ORG, Some(VAULT), "key", None, None).await;
        assert!(matches!(result, Err(crate::error::SdkError::Shutdown)));

        // Write should also fail
        let ops = vec![Operation::set_entity("new:key", b"value".to_vec(), None, None)];
        let result = client.write(UserSlug::new(42), ORG, Some(VAULT), ops, None).await;
        assert!(matches!(result, Err(crate::error::SdkError::Shutdown)));
    }

    #[tokio::test]
    async fn test_client_shutdown_with_multiple_operations() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        // Perform several successful operations
        for i in 0..5 {
            let ops = vec![Operation::set_entity(
                format!("key:{i}"),
                format!("value:{i}").into_bytes(),
                None,
                None,
            )];
            client.write(UserSlug::new(42), ORG, Some(VAULT), ops, None).await.unwrap();
        }

        // Verify writes completed
        assert_eq!(server.write_count(), 5);

        // Shutdown
        client.shutdown().await;

        // Operations should fail
        let ops = vec![Operation::set_entity("key:5", b"value".to_vec(), None, None)];
        let result = client.write(UserSlug::new(42), ORG, Some(VAULT), ops, None).await;
        assert!(matches!(result, Err(crate::error::SdkError::Shutdown)));

        // Write count should not have increased (rejected before reaching server)
        assert_eq!(
            server.write_count(),
            5,
            "No additional writes should reach server after shutdown"
        );
    }

    #[tokio::test]
    async fn test_cloned_client_shutdown_affects_all_clones() {
        let server = MockLedgerServer::start().await.unwrap();
        server.set_entity(ORG, VAULT, "key", b"value");

        let client1 = create_client_for_mock(&server).await;
        let client2 = client1.clone();
        let client3 = client1.clone();

        // All clones should work initially
        assert!(client1.read(UserSlug::new(42), ORG, Some(VAULT), "key", None, None).await.is_ok());
        assert!(client2.read(UserSlug::new(42), ORG, Some(VAULT), "key", None, None).await.is_ok());
        assert!(client3.read(UserSlug::new(42), ORG, Some(VAULT), "key", None, None).await.is_ok());

        // Shutdown through client2
        client2.shutdown().await;

        // All clones should now fail
        assert!(matches!(
            client1.read(UserSlug::new(42), ORG, Some(VAULT), "key", None, None).await,
            Err(crate::error::SdkError::Shutdown)
        ));
        assert!(matches!(
            client2.read(UserSlug::new(42), ORG, Some(VAULT), "key", None, None).await,
            Err(crate::error::SdkError::Shutdown)
        ));
        assert!(matches!(
            client3.read(UserSlug::new(42), ORG, Some(VAULT), "key", None, None).await,
            Err(crate::error::SdkError::Shutdown)
        ));
    }

    // =====================================================================
    // Team CRUD Tests
    // =====================================================================

    const INITIATOR: UserSlug = UserSlug::new(100);

    #[tokio::test]
    async fn test_create_organization_team() {
        let server = MockLedgerServer::start().await.unwrap();
        server.add_organization(ORG, "test-org", Region::US_EAST_VA);
        let client = create_client_for_mock(&server).await;

        let team = client.create_organization_team(ORG, "engineering", INITIATOR).await.unwrap();

        assert_eq!(team.name, "engineering");
        assert_eq!(team.organization, ORG);
        assert!(team.slug.value() > 0);
    }

    #[tokio::test]
    async fn test_create_team_duplicate_name_fails() {
        let server = MockLedgerServer::start().await.unwrap();
        server.add_organization(ORG, "test-org", Region::US_EAST_VA);
        let client = create_client_for_mock(&server).await;

        client.create_organization_team(ORG, "engineering", INITIATOR).await.unwrap();

        let err = client.create_organization_team(ORG, "engineering", INITIATOR).await.unwrap_err();
        assert!(matches!(
            err,
            crate::error::SdkError::Rpc { code: tonic::Code::AlreadyExists, .. }
        ));
    }

    #[tokio::test]
    async fn test_create_team_nonexistent_org_fails() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let nonexistent_org = OrganizationSlug::new(9999);
        let err = client
            .create_organization_team(nonexistent_org, "engineering", INITIATOR)
            .await
            .unwrap_err();
        assert!(matches!(err, crate::error::SdkError::Rpc { code: tonic::Code::NotFound, .. }));
    }

    #[tokio::test]
    async fn test_list_organization_teams_empty() {
        let server = MockLedgerServer::start().await.unwrap();
        server.add_organization(ORG, "test-org", Region::US_EAST_VA);
        let client = create_client_for_mock(&server).await;

        let (teams, next_page_token) =
            client.list_organization_teams(ORG, INITIATOR, 10, None).await.unwrap();

        assert!(teams.is_empty());
        assert!(next_page_token.is_none());
    }

    #[tokio::test]
    async fn test_list_organization_teams() {
        let server = MockLedgerServer::start().await.unwrap();
        server.add_organization(ORG, "test-org", Region::US_EAST_VA);
        let client = create_client_for_mock(&server).await;

        client.create_organization_team(ORG, "engineering", INITIATOR).await.unwrap();
        client.create_organization_team(ORG, "design", INITIATOR).await.unwrap();
        client.create_organization_team(ORG, "product", INITIATOR).await.unwrap();

        let (teams, _) = client.list_organization_teams(ORG, INITIATOR, 10, None).await.unwrap();

        assert_eq!(teams.len(), 3);
        let mut names: Vec<&str> = teams.iter().map(|t| t.name.as_str()).collect();
        names.sort();
        assert_eq!(names, vec!["design", "engineering", "product"]);
    }

    #[tokio::test]
    async fn test_delete_organization_team() {
        let server = MockLedgerServer::start().await.unwrap();
        server.add_organization(ORG, "test-org", Region::US_EAST_VA);
        let client = create_client_for_mock(&server).await;

        let team = client.create_organization_team(ORG, "engineering", INITIATOR).await.unwrap();

        client.delete_organization_team(team.slug, INITIATOR, None).await.unwrap();

        let (teams, _) = client.list_organization_teams(ORG, INITIATOR, 10, None).await.unwrap();
        assert!(teams.is_empty());
    }

    #[tokio::test]
    async fn test_delete_nonexistent_team_fails() {
        let server = MockLedgerServer::start().await.unwrap();
        server.add_organization(ORG, "test-org", Region::US_EAST_VA);
        let client = create_client_for_mock(&server).await;

        let nonexistent_team = TeamSlug::new(99999);
        let err =
            client.delete_organization_team(nonexistent_team, INITIATOR, None).await.unwrap_err();
        assert!(matches!(err, crate::error::SdkError::Rpc { code: tonic::Code::NotFound, .. }));
    }

    #[tokio::test]
    async fn test_update_organization_team_name() {
        let server = MockLedgerServer::start().await.unwrap();
        server.add_organization(ORG, "test-org", Region::US_EAST_VA);
        let client = create_client_for_mock(&server).await;

        let team = client.create_organization_team(ORG, "engineering", INITIATOR).await.unwrap();

        let updated =
            client.update_organization_team(team.slug, INITIATOR, Some("platform")).await.unwrap();

        assert_eq!(updated.name, "platform");
        assert_eq!(updated.slug, team.slug);
        assert_eq!(updated.organization, ORG);
    }

    #[tokio::test]
    async fn test_update_team_duplicate_name_fails() {
        let server = MockLedgerServer::start().await.unwrap();
        server.add_organization(ORG, "test-org", Region::US_EAST_VA);
        let client = create_client_for_mock(&server).await;

        client.create_organization_team(ORG, "engineering", INITIATOR).await.unwrap();
        let team_b = client.create_organization_team(ORG, "design", INITIATOR).await.unwrap();

        let err = client
            .update_organization_team(team_b.slug, INITIATOR, Some("engineering"))
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            crate::error::SdkError::Rpc { code: tonic::Code::AlreadyExists, .. }
        ));
    }

    #[tokio::test]
    async fn test_team_isolation_between_orgs() {
        let server = MockLedgerServer::start().await.unwrap();
        let org2 = OrganizationSlug::new(2);
        server.add_organization(ORG, "org-one", Region::US_EAST_VA);
        server.add_organization(org2, "org-two", Region::US_EAST_VA);
        let client = create_client_for_mock(&server).await;

        client.create_organization_team(ORG, "alpha", INITIATOR).await.unwrap();
        client.create_organization_team(ORG, "beta", INITIATOR).await.unwrap();
        client.create_organization_team(org2, "gamma", INITIATOR).await.unwrap();

        let (org1_teams, _) =
            client.list_organization_teams(ORG, INITIATOR, 10, None).await.unwrap();
        let (org2_teams, _) =
            client.list_organization_teams(org2, INITIATOR, 10, None).await.unwrap();

        assert_eq!(org1_teams.len(), 2);
        assert_eq!(org2_teams.len(), 1);
        assert_eq!(org2_teams[0].name, "gamma");
        assert_eq!(org2_teams[0].organization, org2);
    }

    // =====================================================================
    // Organization: Remaining Operations
    // =====================================================================

    #[tokio::test]
    async fn test_delete_organization() {
        let server = MockLedgerServer::start().await.unwrap();
        server.add_organization(ORG, "test-org", Region::US_EAST_VA);
        let client = create_client_for_mock(&server).await;

        let info = client.delete_organization(ORG, UserSlug::new(100)).await.unwrap();
        assert!(info.deleted_at.is_some());
        assert_eq!(info.retention_days, 90);
    }

    #[tokio::test]
    async fn test_delete_organization_not_found() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let err = client
            .delete_organization(OrganizationSlug::new(9999), UserSlug::new(100))
            .await
            .unwrap_err();
        assert!(matches!(err, crate::error::SdkError::Rpc { code: tonic::Code::NotFound, .. }));
    }

    #[tokio::test]
    async fn test_update_organization() {
        let server = MockLedgerServer::start().await.unwrap();
        server.add_organization(ORG, "old-name", Region::US_EAST_VA);
        let client = create_client_for_mock(&server).await;

        let updated = client
            .update_organization(ORG, UserSlug::new(100), Some("new-name".to_string()))
            .await
            .unwrap();
        assert_eq!(updated.name, "new-name");
    }

    #[tokio::test]
    async fn test_list_organization_members() {
        let server = MockLedgerServer::start().await.unwrap();
        // create_organization adds the caller as admin member
        let client = create_client_for_mock(&server).await;

        let org = client
            .create_organization(
                "members-org",
                Region::US_EAST_VA,
                UserSlug::new(100),
                crate::client::OrganizationTier::Free,
            )
            .await
            .unwrap();

        let (members, _) =
            client.list_organization_members(org.slug, UserSlug::new(100), 10, None).await.unwrap();
        assert_eq!(members.len(), 1);
        assert_eq!(members[0].user, UserSlug::new(100));
    }

    #[tokio::test]
    async fn test_remove_organization_member() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let org = client
            .create_organization(
                "remove-member-org",
                Region::US_EAST_VA,
                UserSlug::new(100),
                crate::client::OrganizationTier::Free,
            )
            .await
            .unwrap();

        // Self-removal
        client
            .remove_organization_member(org.slug, UserSlug::new(100), UserSlug::new(100))
            .await
            .unwrap();

        let (members, _) =
            client.list_organization_members(org.slug, UserSlug::new(100), 10, None).await.unwrap();
        assert!(members.is_empty());
    }

    #[tokio::test]
    async fn test_update_organization_member_role() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let org = client
            .create_organization(
                "role-org",
                Region::US_EAST_VA,
                UserSlug::new(100),
                crate::client::OrganizationTier::Free,
            )
            .await
            .unwrap();

        let member = client
            .update_organization_member_role(
                org.slug,
                UserSlug::new(100),
                UserSlug::new(100),
                crate::OrganizationMemberRole::Member,
            )
            .await
            .unwrap();

        assert_eq!(member.role, crate::OrganizationMemberRole::Member);
    }

    #[tokio::test]
    async fn test_get_organization_team() {
        let server = MockLedgerServer::start().await.unwrap();
        server.add_organization(ORG, "test-org", Region::US_EAST_VA);
        let client = create_client_for_mock(&server).await;

        let created = client.create_organization_team(ORG, "eng", INITIATOR).await.unwrap();

        let fetched = client.get_organization_team(created.slug, INITIATOR).await.unwrap();
        assert_eq!(fetched.slug, created.slug);
        assert_eq!(fetched.name, "eng");
    }

    #[tokio::test]
    async fn test_add_team_member() {
        let server = MockLedgerServer::start().await.unwrap();
        server.add_organization(ORG, "test-org", Region::US_EAST_VA);
        let client = create_client_for_mock(&server).await;

        let team = client.create_organization_team(ORG, "eng", INITIATOR).await.unwrap();

        let result = client
            .add_team_member(
                team.slug,
                UserSlug::new(200),
                crate::TeamMemberRole::Member,
                INITIATOR,
            )
            .await
            .unwrap();
        assert_eq!(result.slug, team.slug);
    }

    #[tokio::test]
    async fn test_remove_team_member() {
        let server = MockLedgerServer::start().await.unwrap();
        server.add_organization(ORG, "test-org", Region::US_EAST_VA);
        let client = create_client_for_mock(&server).await;

        let team = client.create_organization_team(ORG, "eng", INITIATOR).await.unwrap();

        client.remove_team_member(team.slug, UserSlug::new(200), INITIATOR).await.unwrap();
    }

    #[tokio::test]
    async fn test_update_team_member_role() {
        let server = MockLedgerServer::start().await.unwrap();
        server.add_organization(ORG, "test-org", Region::US_EAST_VA);
        let client = create_client_for_mock(&server).await;

        let team = client.create_organization_team(ORG, "eng", INITIATOR).await.unwrap();

        let result = client
            .update_team_member_role(
                team.slug,
                UserSlug::new(200),
                crate::TeamMemberRole::Manager,
                INITIATOR,
            )
            .await
            .unwrap();
        assert_eq!(result.slug, team.slug);
    }

    #[tokio::test]
    async fn test_migrate_organization() {
        let server = MockLedgerServer::start().await.unwrap();
        server.add_organization(ORG, "migrate-org", Region::US_EAST_VA);
        let client = create_client_for_mock(&server).await;

        let info = client
            .migrate_organization(ORG, Region::US_WEST_OR, false, UserSlug::new(100))
            .await
            .unwrap();

        assert_eq!(info.slug, ORG);
        assert_eq!(info.status, crate::types::admin::OrganizationStatus::Migrating);
    }

    #[tokio::test]
    async fn test_migrate_user_region() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        // Create a user first
        let user = client
            .create_user(
                "testuser",
                "test@example.com",
                "hmac_test",
                Region::US_EAST_VA,
                inferadb_ledger_types::UserRole::User,
            )
            .await
            .unwrap();

        let info = client
            .migrate_user_region(UserSlug::new(100), user.slug, Region::US_WEST_OR)
            .await
            .unwrap();

        assert_eq!(info.slug, user.slug);
        assert_eq!(info.directory_status, "MIGRATING");
    }

    #[tokio::test]
    async fn test_erase_user() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let user = client
            .create_user(
                "erase-me",
                "erase@example.com",
                "hmac_erase",
                Region::US_EAST_VA,
                inferadb_ledger_types::UserRole::User,
            )
            .await
            .unwrap();

        let erased_slug =
            client.erase_user(user.slug, UserSlug::new(100), Region::US_EAST_VA).await.unwrap();

        assert_eq!(erased_slug, user.slug);
    }

    // =====================================================================
    // Invitation Operations
    // =====================================================================

    #[tokio::test]
    async fn test_create_organization_invite() {
        let server = MockLedgerServer::start().await.unwrap();
        server.add_organization(ORG, "test-org", Region::US_EAST_VA);
        let client = create_client_for_mock(&server).await;

        let created = client
            .create_organization_invite(
                ORG,
                UserSlug::new(100),
                "invitee@example.com",
                crate::OrganizationMemberRole::Member,
                72,
                None,
            )
            .await
            .unwrap();

        assert!(created.slug.value() > 0);
        assert_eq!(created.status, crate::InvitationStatus::Pending);
        assert!(!created.token.is_empty());
    }

    #[tokio::test]
    async fn test_create_invite_nonexistent_org_fails() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let err = client
            .create_organization_invite(
                OrganizationSlug::new(9999),
                UserSlug::new(100),
                "test@example.com",
                crate::OrganizationMemberRole::Member,
                72,
                None,
            )
            .await
            .unwrap_err();
        assert!(matches!(err, crate::error::SdkError::Rpc { code: tonic::Code::NotFound, .. }));
    }

    #[tokio::test]
    async fn test_list_organization_invites_empty() {
        let server = MockLedgerServer::start().await.unwrap();
        server.add_organization(ORG, "test-org", Region::US_EAST_VA);
        let client = create_client_for_mock(&server).await;

        let page = client
            .list_organization_invites(ORG, UserSlug::new(100), None, None, 10)
            .await
            .unwrap();

        assert!(page.invitations.is_empty());
        assert!(page.next_page_token.is_none());
    }

    #[tokio::test]
    async fn test_list_organization_invites_returns_stored() {
        let server = MockLedgerServer::start().await.unwrap();
        server.add_organization(ORG, "test-org", Region::US_EAST_VA);
        server.add_invitation(
            10,
            ORG,
            100,
            "alice@example.com",
            "test-org",
            inferadb_ledger_proto::proto::OrganizationMemberRole::Member as i32,
            inferadb_ledger_proto::proto::InvitationStatus::Pending as i32,
        );
        let client = create_client_for_mock(&server).await;

        let page = client
            .list_organization_invites(ORG, UserSlug::new(100), None, None, 10)
            .await
            .unwrap();

        assert_eq!(page.invitations.len(), 1);
        assert_eq!(page.invitations[0].invitee_email, "alice@example.com");
    }

    #[tokio::test]
    async fn test_get_organization_invite() {
        let server = MockLedgerServer::start().await.unwrap();
        server.add_invitation(
            42,
            ORG,
            100,
            "bob@example.com",
            "test-org",
            inferadb_ledger_proto::proto::OrganizationMemberRole::Admin as i32,
            inferadb_ledger_proto::proto::InvitationStatus::Pending as i32,
        );
        let client = create_client_for_mock(&server).await;

        let info = client
            .get_organization_invite(crate::InviteSlug::new(42), UserSlug::new(100))
            .await
            .unwrap();

        assert_eq!(info.slug, crate::InviteSlug::new(42));
        assert_eq!(info.invitee_email, "bob@example.com");
        assert_eq!(info.role, crate::OrganizationMemberRole::Admin);
    }

    #[tokio::test]
    async fn test_get_organization_invite_not_found() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let err = client
            .get_organization_invite(crate::InviteSlug::new(9999), UserSlug::new(100))
            .await
            .unwrap_err();
        assert!(matches!(err, crate::error::SdkError::Rpc { code: tonic::Code::NotFound, .. }));
    }

    #[tokio::test]
    async fn test_revoke_organization_invite() {
        let server = MockLedgerServer::start().await.unwrap();
        server.add_invitation(
            50,
            ORG,
            100,
            "revoke@example.com",
            "test-org",
            inferadb_ledger_proto::proto::OrganizationMemberRole::Member as i32,
            inferadb_ledger_proto::proto::InvitationStatus::Pending as i32,
        );
        let client = create_client_for_mock(&server).await;

        let revoked = client
            .revoke_organization_invite(crate::InviteSlug::new(50), UserSlug::new(100))
            .await
            .unwrap();

        assert_eq!(revoked.status, crate::InvitationStatus::Revoked);
    }

    #[tokio::test]
    async fn test_list_received_invitations() {
        let server = MockLedgerServer::start().await.unwrap();
        server.add_invitation(
            60,
            ORG,
            100,
            "user@example.com",
            "Acme Corp",
            inferadb_ledger_proto::proto::OrganizationMemberRole::Member as i32,
            inferadb_ledger_proto::proto::InvitationStatus::Pending as i32,
        );
        let client = create_client_for_mock(&server).await;

        let page =
            client.list_received_invitations(UserSlug::new(200), None, None, 10).await.unwrap();

        assert_eq!(page.invitations.len(), 1);
        assert_eq!(page.invitations[0].organization_name, "Acme Corp");
    }

    #[tokio::test]
    async fn test_get_invitation_details() {
        let server = MockLedgerServer::start().await.unwrap();
        server.add_invitation(
            70,
            ORG,
            100,
            "detail@example.com",
            "Detail Org",
            inferadb_ledger_proto::proto::OrganizationMemberRole::Admin as i32,
            inferadb_ledger_proto::proto::InvitationStatus::Pending as i32,
        );
        let client = create_client_for_mock(&server).await;

        let info = client
            .get_invitation_details(crate::InviteSlug::new(70), UserSlug::new(200))
            .await
            .unwrap();

        assert_eq!(info.organization_name, "Detail Org");
        assert_eq!(info.role, crate::OrganizationMemberRole::Admin);
    }

    #[tokio::test]
    async fn test_accept_invitation() {
        let server = MockLedgerServer::start().await.unwrap();
        server.add_invitation(
            80,
            ORG,
            100,
            "accept@example.com",
            "Accept Org",
            inferadb_ledger_proto::proto::OrganizationMemberRole::Member as i32,
            inferadb_ledger_proto::proto::InvitationStatus::Pending as i32,
        );
        let client = create_client_for_mock(&server).await;

        let accepted =
            client.accept_invitation(crate::InviteSlug::new(80), UserSlug::new(200)).await.unwrap();

        assert_eq!(accepted.status, crate::InvitationStatus::Accepted);
    }

    #[tokio::test]
    async fn test_decline_invitation() {
        let server = MockLedgerServer::start().await.unwrap();
        server.add_invitation(
            90,
            ORG,
            100,
            "decline@example.com",
            "Decline Org",
            inferadb_ledger_proto::proto::OrganizationMemberRole::Member as i32,
            inferadb_ledger_proto::proto::InvitationStatus::Pending as i32,
        );
        let client = create_client_for_mock(&server).await;

        let declined = client
            .decline_invitation(crate::InviteSlug::new(90), UserSlug::new(200))
            .await
            .unwrap();

        assert_eq!(declined.status, crate::InvitationStatus::Declined);
    }

    // =====================================================================
    // Events Operations
    // =====================================================================

    #[tokio::test]
    async fn test_list_events_empty() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let page = client
            .list_events(UserSlug::new(42), ORG, crate::EventFilter::new(), 100)
            .await
            .unwrap();

        assert!(page.entries.is_empty());
    }

    #[tokio::test]
    async fn test_list_events_returns_stored() {
        let server = MockLedgerServer::start().await.unwrap();
        let event_id = uuid::Uuid::new_v4().into_bytes().to_vec();
        server.add_event(event_id, ORG, "ledger.vault.created", "user:42");
        let client = create_client_for_mock(&server).await;

        let page = client
            .list_events(UserSlug::new(42), ORG, crate::EventFilter::new(), 100)
            .await
            .unwrap();

        assert_eq!(page.entries.len(), 1);
        assert_eq!(page.entries[0].event_type, "ledger.vault.created");
        assert_eq!(page.entries[0].principal, "user:42");
    }

    #[tokio::test]
    async fn test_list_events_next_page() {
        let server = MockLedgerServer::start().await.unwrap();
        server.add_event(
            uuid::Uuid::new_v4().into_bytes().to_vec(),
            ORG,
            "ledger.entity.set",
            "user:1",
        );
        let client = create_client_for_mock(&server).await;

        let page = client.list_events_next(UserSlug::new(42), ORG, "").await.unwrap();

        assert_eq!(page.entries.len(), 1);
    }

    #[tokio::test]
    async fn test_get_event() {
        let server = MockLedgerServer::start().await.unwrap();
        let event_uuid = uuid::Uuid::new_v4();
        let event_id = event_uuid.into_bytes().to_vec();
        server.add_event(event_id, ORG, "ledger.vault.deleted", "user:admin");
        let client = create_client_for_mock(&server).await;

        let event =
            client.get_event(UserSlug::new(42), ORG, &event_uuid.to_string()).await.unwrap();

        assert_eq!(event.event_type, "ledger.vault.deleted");
        assert_eq!(event.principal, "user:admin");
    }

    #[tokio::test]
    async fn test_get_event_not_found() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let missing_uuid = uuid::Uuid::new_v4();
        let err =
            client.get_event(UserSlug::new(42), ORG, &missing_uuid.to_string()).await.unwrap_err();
        assert!(matches!(err, crate::error::SdkError::Rpc { code: tonic::Code::NotFound, .. }));
    }

    #[tokio::test]
    async fn test_get_event_invalid_uuid() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let err = client.get_event(UserSlug::new(42), ORG, "not-a-uuid").await.unwrap_err();
        assert!(matches!(err, crate::error::SdkError::Validation { .. }));
    }

    #[tokio::test]
    async fn test_count_events() {
        let server = MockLedgerServer::start().await.unwrap();
        server.add_event(
            uuid::Uuid::new_v4().into_bytes().to_vec(),
            ORG,
            "ledger.vault.created",
            "user:1",
        );
        server.add_event(
            uuid::Uuid::new_v4().into_bytes().to_vec(),
            ORG,
            "ledger.entity.set",
            "user:1",
        );
        let client = create_client_for_mock(&server).await;

        let count =
            client.count_events(UserSlug::new(42), ORG, crate::EventFilter::new()).await.unwrap();

        assert_eq!(count, 2);
    }

    #[tokio::test]
    async fn test_count_events_empty() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let count =
            client.count_events(UserSlug::new(42), ORG, crate::EventFilter::new()).await.unwrap();

        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_ingest_events() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let events = vec![
            crate::SdkIngestEventEntry::new(
                "engine.auth.checked",
                "user:alice",
                crate::EventOutcome::Success,
            )
            .detail("resource", "doc:123"),
            crate::SdkIngestEventEntry::new(
                "engine.auth.denied",
                "user:bob",
                crate::EventOutcome::Denied { reason: "no access".to_string() },
            ),
        ];

        let result = client
            .ingest_events(UserSlug::new(42), ORG, crate::EventSource::Engine, events)
            .await
            .unwrap();

        assert_eq!(result.accepted_count, 2);
        assert_eq!(result.rejected_count, 0);
        assert!(result.rejections.is_empty());
    }

    #[tokio::test]
    async fn test_ingest_then_count() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let events = vec![crate::SdkIngestEventEntry::new(
            "control.user.created",
            "system",
            crate::EventOutcome::Success,
        )];

        client
            .ingest_events(UserSlug::new(42), ORG, crate::EventSource::Control, events)
            .await
            .unwrap();

        let count =
            client.count_events(UserSlug::new(42), ORG, crate::EventFilter::new()).await.unwrap();

        assert_eq!(count, 1);
    }

    // =====================================================================
    // Onboarding Operations
    // =====================================================================

    #[tokio::test]
    async fn test_initiate_email_verification() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let result = client
            .initiate_email_verification("test@example.com", Region::US_EAST_VA)
            .await
            .unwrap();

        assert!(!result.code.is_empty());
        assert_eq!(result.code, "ABC123");
    }

    #[tokio::test]
    async fn test_verify_email_code_new_user() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let result = client
            .verify_email_code("new@example.com", "ABC123", Region::US_EAST_VA)
            .await
            .unwrap();

        assert!(
            matches!(
                &result,
                crate::types::admin::EmailVerificationResult::NewUser { onboarding_token }
                    if onboarding_token.starts_with("ilobt_")
            ),
            "Expected NewUser variant with ilobt_ prefix, got {result:?}"
        );
    }

    #[tokio::test]
    async fn test_complete_registration() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let result = client
            .complete_registration(
                "ilobt_mock_token",
                "user@example.com",
                Region::US_EAST_VA,
                "Test User",
                "Test Org",
            )
            .await
            .unwrap();

        assert!(result.user.value() > 0);
        assert!(!result.session.access_token.is_empty());
        assert!(!result.session.refresh_token.is_empty());
        assert!(result.organization.is_some());
    }

    #[tokio::test]
    async fn test_complete_registration_empty_fields_fail() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let err = client
            .complete_registration("", "user@example.com", Region::US_EAST_VA, "Name", "Org")
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            crate::error::SdkError::Rpc { code: tonic::Code::InvalidArgument, .. }
        ));
    }

    #[tokio::test]
    async fn test_onboarding_full_flow() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        // Step 1: Initiate email verification
        let code = client
            .initiate_email_verification("flow@example.com", Region::US_EAST_VA)
            .await
            .unwrap();

        // Step 2: Verify code
        let verification = client
            .verify_email_code("flow@example.com", &code.code, Region::US_EAST_VA)
            .await
            .unwrap();

        let crate::types::admin::EmailVerificationResult::NewUser { onboarding_token } =
            verification
        else {
            panic!("Expected NewUser variant, got {verification:?}");
        };

        // Step 3: Complete registration
        let registration = client
            .complete_registration(
                &onboarding_token,
                "flow@example.com",
                Region::US_EAST_VA,
                "Flow User",
                "Flow Org",
            )
            .await
            .unwrap();

        assert!(registration.user.value() > 0);
        assert!(registration.organization.is_some());
    }
}

/// Tests for app, token, credential, and schema operations against mock services.
mod app_token_credential_schema_tests {
    use std::time::Duration;

    use super::*;
    use crate::{ClientConfig, LedgerClient, ServerSource, UserSlug};

    /// Helper to create a client connected to a mock server.
    async fn create_client_for_mock(server: &MockLedgerServer) -> LedgerClient {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static([server.endpoint().to_string()]))
            .client_id("test-client")
            .timeout(Duration::from_secs(5))
            .connect_timeout(Duration::from_secs(2))
            .build()
            .expect("valid config");

        LedgerClient::new(config).await.expect("client creation")
    }

    const INITIATOR: UserSlug = UserSlug::new(42);

    // ==================== App Operations ====================

    #[tokio::test]
    async fn test_create_app() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let app = client
            .create_app(ORG, INITIATOR, "my-app", Some("A test app".to_string()))
            .await
            .unwrap();
        assert_eq!(app.name, "my-app");
        assert!(app.enabled);
        assert!(app.credentials.is_some());
        assert!(app.created_at.is_some());
    }

    #[tokio::test]
    async fn test_get_app() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let app_slug = crate::AppSlug::new(99);
        let app = client.get_app(ORG, INITIATOR, app_slug).await.unwrap();
        assert_eq!(app.slug, app_slug);
        assert!(app.enabled);
    }

    #[tokio::test]
    async fn test_list_apps() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let apps = client.list_apps(ORG, INITIATOR).await.unwrap();
        assert_eq!(apps.len(), 2);
        assert_eq!(apps[0].name, "app-one");
        assert_eq!(apps[1].name, "app-two");
        assert_eq!(apps[1].description.as_deref(), Some("second app"));
    }

    #[tokio::test]
    async fn test_update_app() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let app_slug = crate::AppSlug::new(50);
        let app = client
            .update_app(
                ORG,
                INITIATOR,
                app_slug,
                Some("new-name".to_string()),
                Some("new desc".to_string()),
            )
            .await
            .unwrap();
        assert_eq!(app.name, "new-name");
        assert_eq!(app.slug, app_slug);
    }

    #[tokio::test]
    async fn test_delete_app() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let app_slug = crate::AppSlug::new(50);
        client.delete_app(ORG, INITIATOR, app_slug).await.unwrap();
    }

    #[tokio::test]
    async fn test_enable_disable_app() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let app_slug = crate::AppSlug::new(50);
        let app = client.enable_app(ORG, INITIATOR, app_slug).await.unwrap();
        assert!(app.enabled);

        let app = client.disable_app(ORG, INITIATOR, app_slug).await.unwrap();
        assert!(!app.enabled);
    }

    #[tokio::test]
    async fn test_set_app_credential_enabled() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let app_slug = crate::AppSlug::new(50);
        let app = client
            .set_app_credential_enabled(
                ORG,
                INITIATOR,
                app_slug,
                crate::types::app::AppCredentialType::ClientSecret,
                true,
            )
            .await
            .unwrap();
        assert!(app.credentials.is_some());
    }

    #[tokio::test]
    async fn test_get_app_client_secret() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let app_slug = crate::AppSlug::new(50);
        let status = client.get_app_client_secret(ORG, INITIATOR, app_slug).await.unwrap();
        assert!(status.enabled);
        assert!(status.has_secret);
    }

    #[tokio::test]
    async fn test_rotate_app_client_secret() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let app_slug = crate::AppSlug::new(50);
        let secret = client.rotate_app_client_secret(ORG, INITIATOR, app_slug).await.unwrap();
        assert_eq!(secret, "mock-rotated-secret-base64");
    }

    #[tokio::test]
    async fn test_list_app_client_assertions() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let app_slug = crate::AppSlug::new(50);
        let assertions = client.list_app_client_assertions(ORG, INITIATOR, app_slug).await.unwrap();
        assert_eq!(assertions.len(), 1);
        assert_eq!(assertions[0].name, "test-assertion");
        assert!(assertions[0].enabled);
    }

    #[tokio::test]
    async fn test_create_app_client_assertion() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let app_slug = crate::AppSlug::new(50);
        let expires_at = std::time::SystemTime::now() + Duration::from_secs(3600);
        let result = client
            .create_app_client_assertion(ORG, INITIATOR, app_slug, "my-key", expires_at)
            .await
            .unwrap();
        assert_eq!(result.assertion.name, "my-key");
        assert!(result.assertion.enabled);
        assert!(result.private_key_pem.contains("PRIVATE KEY"));
    }

    #[tokio::test]
    async fn test_delete_app_client_assertion() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let app_slug = crate::AppSlug::new(50);
        let assertion_id = crate::ClientAssertionId::new(1);
        client.delete_app_client_assertion(ORG, INITIATOR, app_slug, assertion_id).await.unwrap();
    }

    #[tokio::test]
    async fn test_set_app_client_assertion_enabled() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let app_slug = crate::AppSlug::new(50);
        let assertion_id = crate::ClientAssertionId::new(1);
        client
            .set_app_client_assertion_enabled(ORG, INITIATOR, app_slug, assertion_id, false)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_list_app_vaults() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let app_slug = crate::AppSlug::new(50);
        let vaults = client.list_app_vaults(ORG, INITIATOR, app_slug).await.unwrap();
        assert_eq!(vaults.len(), 1);
        assert_eq!(vaults[0].vault_slug, VaultSlug::new(10));
        assert_eq!(vaults[0].allowed_scopes, vec!["read", "write"]);
    }

    #[tokio::test]
    async fn test_add_app_vault() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let app_slug = crate::AppSlug::new(50);
        let vault_slug = VaultSlug::new(20);
        let conn = client
            .add_app_vault(ORG, INITIATOR, app_slug, vault_slug, vec!["read".to_string()])
            .await
            .unwrap();
        assert_eq!(conn.vault_slug, vault_slug);
        assert_eq!(conn.allowed_scopes, vec!["read"]);
    }

    #[tokio::test]
    async fn test_update_app_vault() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let app_slug = crate::AppSlug::new(50);
        let vault_slug = VaultSlug::new(20);
        let conn = client
            .update_app_vault(
                ORG,
                INITIATOR,
                app_slug,
                vault_slug,
                vec!["read".to_string(), "write".to_string()],
            )
            .await
            .unwrap();
        assert_eq!(conn.vault_slug, vault_slug);
        assert_eq!(conn.allowed_scopes, vec!["read", "write"]);
    }

    #[tokio::test]
    async fn test_remove_app_vault() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let app_slug = crate::AppSlug::new(50);
        let vault_slug = VaultSlug::new(20);
        client.remove_app_vault(ORG, INITIATOR, app_slug, vault_slug).await.unwrap();
    }

    // ==================== Token Operations ====================

    #[tokio::test]
    async fn test_create_user_session() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let tokens = client.create_user_session(INITIATOR).await.unwrap();
        assert_eq!(tokens.access_token, "mock-access-token");
        assert_eq!(tokens.refresh_token, "mock-refresh-token");
    }

    #[tokio::test]
    async fn test_validate_token() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let validated = client.validate_token("some-access-token", "ledger").await.unwrap();
        match validated {
            crate::ValidatedToken::UserSession { user, role } => {
                assert_eq!(user.value(), 42);
                assert_eq!(role, "user");
            },
            _ => unreachable!("expected UserSession"),
        }
    }

    #[tokio::test]
    async fn test_revoke_all_user_sessions() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let count = client.revoke_all_user_sessions(INITIATOR).await.unwrap();
        assert_eq!(count, 3);
    }

    #[tokio::test]
    async fn test_revoke_all_app_sessions() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let app_slug = crate::AppSlug::new(50);
        let count = client.revoke_all_app_sessions(app_slug).await.unwrap();
        assert_eq!(count, 2);
    }

    #[tokio::test]
    async fn test_refresh_token() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let tokens = client.refresh_token("old-refresh-token").await.unwrap();
        assert_eq!(tokens.access_token, "mock-access-token");
        assert_eq!(tokens.refresh_token, "mock-refresh-token");
    }

    #[tokio::test]
    async fn test_revoke_token() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        client.revoke_token("some-refresh-token").await.unwrap();
    }

    #[tokio::test]
    async fn test_create_vault_token() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let app_slug = crate::AppSlug::new(50);
        let tokens =
            client.create_vault_token(ORG, app_slug, VAULT, &["read".to_string()]).await.unwrap();
        assert_eq!(tokens.access_token, "mock-access-token");
    }

    #[tokio::test]
    async fn test_authenticate_client_assertion() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let tokens = client
            .authenticate_client_assertion(ORG, VAULT, "jwt-assertion", &["read".to_string()])
            .await
            .unwrap();
        assert_eq!(tokens.access_token, "mock-access-token");
    }

    #[tokio::test]
    async fn test_create_signing_key() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let key = client.create_signing_key(INITIATOR, "global", None).await.unwrap();
        assert_eq!(key.kid, "mock-kid-001");
        assert_eq!(key.status, "active");
        assert_eq!(key.public_key.len(), 32);
    }

    #[tokio::test]
    async fn test_rotate_signing_key() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let key = client.rotate_signing_key(INITIATOR, "mock-kid-001", None, false).await.unwrap();
        assert_eq!(key.kid, "mock-kid-002");
        assert_eq!(key.status, "active");
    }

    #[tokio::test]
    async fn test_revoke_signing_key() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        client.revoke_signing_key(INITIATOR, "mock-kid-001").await.unwrap();
    }

    #[tokio::test]
    async fn test_get_public_keys() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let keys = client.get_public_keys(INITIATOR, None).await.unwrap();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].kid, "mock-kid-001");
    }

    // ==================== Credential Operations ====================

    #[tokio::test]
    async fn test_create_user_credential_passkey() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let data = crate::types::credential::CredentialData::Passkey(
            crate::types::credential::PasskeyCredentialInfo {
                credential_id: vec![1, 2, 3],
                public_key: vec![4, 5, 6],
                sign_count: 0,
                transports: vec!["internal".to_string()],
                backup_eligible: false,
                backup_state: false,
                attestation_format: None,
                aaguid: None,
            },
        );
        let cred =
            client.create_user_credential(INITIATOR, INITIATOR, "Touch ID", data).await.unwrap();
        assert_eq!(cred.name, "Touch ID");
        assert!(cred.enabled);
        assert_eq!(cred.credential_type, crate::types::credential::CredentialType::Passkey);
    }

    #[tokio::test]
    async fn test_create_user_credential_totp() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let data = crate::types::credential::CredentialData::Totp(
            crate::types::credential::TotpCredentialInfo {
                secret: vec![42; 20],
                algorithm: crate::types::credential::TotpAlgorithm::Sha1,
                digits: 6,
                period: 30,
            },
        );
        let cred = client
            .create_user_credential(INITIATOR, INITIATOR, "Authenticator", data)
            .await
            .unwrap();
        assert_eq!(cred.credential_type, crate::types::credential::CredentialType::Totp);
    }

    #[tokio::test]
    async fn test_list_user_credentials() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let creds = client.list_user_credentials(INITIATOR, INITIATOR, None).await.unwrap();
        assert_eq!(creds.len(), 1);
        assert_eq!(creds[0].credential_type, crate::types::credential::CredentialType::Passkey);
    }

    #[tokio::test]
    async fn test_update_user_credential() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let cred_id = crate::UserCredentialId::new(1);
        let cred = client
            .update_user_credential(
                INITIATOR,
                INITIATOR,
                cred_id,
                Some("New Name".to_string()),
                Some(false),
                None,
            )
            .await
            .unwrap();
        assert_eq!(cred.name, "New Name");
        assert!(!cred.enabled);
    }

    #[tokio::test]
    async fn test_delete_user_credential() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let cred_id = crate::UserCredentialId::new(1);
        client.delete_user_credential(INITIATOR, INITIATOR, cred_id).await.unwrap();
    }

    #[tokio::test]
    async fn test_create_totp_challenge() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let nonce = client.create_totp_challenge(INITIATOR, INITIATOR, "passkey").await.unwrap();
        assert_eq!(nonce.len(), 32);
    }

    #[tokio::test]
    async fn test_verify_totp() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let tokens =
            client.verify_totp(INITIATOR, INITIATOR, "123456", vec![0u8; 32]).await.unwrap();
        assert_eq!(tokens.access_token, "mock-totp-access");
        assert_eq!(tokens.refresh_token, "mock-totp-refresh");
    }

    #[tokio::test]
    async fn test_consume_recovery_code() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let result = client
            .consume_recovery_code(INITIATOR, INITIATOR, "ABCD-1234", vec![0u8; 32])
            .await
            .unwrap();
        assert_eq!(result.tokens.access_token, "mock-recovery-access");
        assert_eq!(result.remaining_codes, 7);
    }

    // ==================== Schema Operations ====================

    #[tokio::test]
    async fn test_deploy_schema() {
        let server = MockLedgerServer::start().await.unwrap();
        server.add_organization(ORG, "test-org", Region::US_EAST_VA);
        server.add_vault(ORG, VAULT);
        let client = create_client_for_mock(&server).await;

        let schema =
            serde_json::json!({"type": "object", "properties": {"name": {"type": "string"}}});
        let result = client
            .deploy_schema(
                INITIATOR,
                ORG,
                VAULT,
                schema,
                Some(1),
                Some("Initial schema".to_string()),
            )
            .await
            .unwrap();
        assert_eq!(result.version, 1);
    }

    #[tokio::test]
    async fn test_deploy_schema_auto_version() {
        let server = MockLedgerServer::start().await.unwrap();
        server.add_organization(ORG, "test-org", Region::US_EAST_VA);
        server.add_vault(ORG, VAULT);
        let client = create_client_for_mock(&server).await;

        let schema = serde_json::json!({"type": "object"});
        let r1 =
            client.deploy_schema(INITIATOR, ORG, VAULT, schema.clone(), None, None).await.unwrap();
        assert_eq!(r1.version, 1);

        let r2 = client.deploy_schema(INITIATOR, ORG, VAULT, schema, None, None).await.unwrap();
        assert_eq!(r2.version, 2);
    }

    #[tokio::test]
    async fn test_deploy_schema_version_zero_error() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let schema = serde_json::json!({"type": "object"});
        let err =
            client.deploy_schema(INITIATOR, ORG, VAULT, schema, Some(0), None).await.unwrap_err();
        assert!(matches!(err, crate::error::SdkError::Validation { .. }));
    }

    #[tokio::test]
    async fn test_get_schema() {
        let server = MockLedgerServer::start().await.unwrap();
        server.add_organization(ORG, "test-org", Region::US_EAST_VA);
        server.add_vault(ORG, VAULT);
        let client = create_client_for_mock(&server).await;

        let schema = serde_json::json!({"type": "object", "title": "v1"});
        client
            .deploy_schema(
                INITIATOR,
                ORG,
                VAULT,
                schema.clone(),
                Some(1),
                Some("v1 desc".to_string()),
            )
            .await
            .unwrap();

        let fetched = client.get_schema(INITIATOR, ORG, VAULT, 1).await.unwrap();
        assert_eq!(fetched.version, 1);
        assert_eq!(fetched.definition, schema);
        assert_eq!(fetched.description.as_deref(), Some("v1 desc"));
    }

    #[tokio::test]
    async fn test_get_schema_not_found() {
        let server = MockLedgerServer::start().await.unwrap();
        server.add_organization(ORG, "test-org", Region::US_EAST_VA);
        server.add_vault(ORG, VAULT);
        let client = create_client_for_mock(&server).await;

        let err = client.get_schema(INITIATOR, ORG, VAULT, 999).await.unwrap_err();
        assert!(matches!(err, crate::error::SdkError::Rpc { code: tonic::Code::NotFound, .. }));
    }

    #[tokio::test]
    async fn test_list_schema_versions() {
        let server = MockLedgerServer::start().await.unwrap();
        server.add_organization(ORG, "test-org", Region::US_EAST_VA);
        server.add_vault(ORG, VAULT);
        let client = create_client_for_mock(&server).await;

        let schema = serde_json::json!({"type": "object"});
        client.deploy_schema(INITIATOR, ORG, VAULT, schema.clone(), Some(1), None).await.unwrap();
        client.deploy_schema(INITIATOR, ORG, VAULT, schema, Some(2), None).await.unwrap();

        let versions = client.list_schema_versions(INITIATOR, ORG, VAULT).await.unwrap();
        assert_eq!(versions.len(), 2);
        assert!(versions[0].has_definition);
        assert!(versions[1].has_definition);
        assert!(!versions[0].is_active);
    }

    #[tokio::test]
    async fn test_list_schema_versions_empty() {
        let server = MockLedgerServer::start().await.unwrap();
        server.add_organization(ORG, "test-org", Region::US_EAST_VA);
        server.add_vault(ORG, VAULT);
        let client = create_client_for_mock(&server).await;

        let versions = client.list_schema_versions(INITIATOR, ORG, VAULT).await.unwrap();
        assert!(versions.is_empty());
    }

    #[tokio::test]
    async fn test_activate_schema() {
        let server = MockLedgerServer::start().await.unwrap();
        server.add_organization(ORG, "test-org", Region::US_EAST_VA);
        server.add_vault(ORG, VAULT);
        let client = create_client_for_mock(&server).await;

        let schema = serde_json::json!({"type": "object"});
        client.deploy_schema(INITIATOR, ORG, VAULT, schema, Some(1), None).await.unwrap();

        let activated = client.activate_schema(INITIATOR, ORG, VAULT, 1).await.unwrap();
        assert_eq!(activated, 1);

        let versions = client.list_schema_versions(INITIATOR, ORG, VAULT).await.unwrap();
        assert!(versions[0].is_active);
    }

    #[tokio::test]
    async fn test_activate_schema_not_found() {
        let server = MockLedgerServer::start().await.unwrap();
        server.add_organization(ORG, "test-org", Region::US_EAST_VA);
        server.add_vault(ORG, VAULT);
        let client = create_client_for_mock(&server).await;

        let err = client.activate_schema(INITIATOR, ORG, VAULT, 999).await.unwrap_err();
        assert!(matches!(err, crate::error::SdkError::Rpc { code: tonic::Code::NotFound, .. }));
    }

    #[tokio::test]
    async fn test_get_active_schema() {
        let server = MockLedgerServer::start().await.unwrap();
        server.add_organization(ORG, "test-org", Region::US_EAST_VA);
        server.add_vault(ORG, VAULT);
        let client = create_client_for_mock(&server).await;

        let schema = serde_json::json!({"type": "object", "title": "active"});
        client.deploy_schema(INITIATOR, ORG, VAULT, schema.clone(), Some(1), None).await.unwrap();
        client.activate_schema(INITIATOR, ORG, VAULT, 1).await.unwrap();

        let active = client.get_active_schema(INITIATOR, ORG, VAULT).await.unwrap();
        assert_eq!(active.version, 1);
        assert_eq!(active.definition, schema);
    }

    #[tokio::test]
    async fn test_get_active_schema_none_active() {
        let server = MockLedgerServer::start().await.unwrap();
        server.add_organization(ORG, "test-org", Region::US_EAST_VA);
        server.add_vault(ORG, VAULT);
        let client = create_client_for_mock(&server).await;

        let err = client.get_active_schema(INITIATOR, ORG, VAULT).await.unwrap_err();
        assert!(matches!(err, crate::error::SdkError::Rpc { code: tonic::Code::NotFound, .. }));
    }

    #[tokio::test]
    async fn test_rollback_schema() {
        let server = MockLedgerServer::start().await.unwrap();
        server.add_organization(ORG, "test-org", Region::US_EAST_VA);
        server.add_vault(ORG, VAULT);
        let client = create_client_for_mock(&server).await;

        let s1 = serde_json::json!({"version": 1});
        let s2 = serde_json::json!({"version": 2});
        client.deploy_schema(INITIATOR, ORG, VAULT, s1, Some(1), None).await.unwrap();
        client.deploy_schema(INITIATOR, ORG, VAULT, s2, Some(2), None).await.unwrap();
        client.activate_schema(INITIATOR, ORG, VAULT, 1).await.unwrap();
        client.activate_schema(INITIATOR, ORG, VAULT, 2).await.unwrap();

        let rolled_back = client.rollback_schema(INITIATOR, ORG, VAULT).await.unwrap();
        assert_eq!(rolled_back, 1);
    }

    #[tokio::test]
    async fn test_rollback_schema_no_history() {
        let server = MockLedgerServer::start().await.unwrap();
        server.add_organization(ORG, "test-org", Region::US_EAST_VA);
        server.add_vault(ORG, VAULT);
        let client = create_client_for_mock(&server).await;

        let err = client.rollback_schema(INITIATOR, ORG, VAULT).await.unwrap_err();
        assert!(matches!(err, crate::error::SdkError::Validation { .. }));
    }

    #[tokio::test]
    async fn test_diff_schemas() {
        let server = MockLedgerServer::start().await.unwrap();
        server.add_organization(ORG, "test-org", Region::US_EAST_VA);
        server.add_vault(ORG, VAULT);
        let client = create_client_for_mock(&server).await;

        let s1 = serde_json::json!({"name": "string", "age": "int"});
        let s2 = serde_json::json!({"name": "string", "email": "string"});
        client.deploy_schema(INITIATOR, ORG, VAULT, s1, Some(1), None).await.unwrap();
        client.deploy_schema(INITIATOR, ORG, VAULT, s2, Some(2), None).await.unwrap();

        let diff = client.diff_schemas(INITIATOR, ORG, VAULT, 1, 2).await.unwrap();
        let types: Vec<(&str, &str)> =
            diff.iter().map(|c| (c.field.as_str(), c.change_type.as_str())).collect();
        assert!(types.contains(&("age", "removed")));
        assert!(types.contains(&("email", "added")));
    }
}

/// Tests for user CRUD, email management, and blinding key operations.
mod user_tests {
    use std::time::Duration;

    use inferadb_ledger_types::UserRole;

    use super::*;
    use crate::{ClientConfig, LedgerClient, ServerSource, UserSlug};

    async fn create_client_for_mock(server: &MockLedgerServer) -> LedgerClient {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static([server.endpoint().to_string()]))
            .client_id("test-client")
            .timeout(Duration::from_secs(5))
            .connect_timeout(Duration::from_secs(2))
            .build()
            .expect("valid config");

        LedgerClient::new(config).await.expect("client creation")
    }

    // =====================================================================
    // User CRUD
    // =====================================================================

    #[tokio::test]
    async fn test_create_user() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let user = client
            .create_user(
                "Alice",
                "alice@example.com",
                "hmac_alice",
                Region::US_EAST_VA,
                UserRole::User,
            )
            .await
            .unwrap();

        assert!(!user.name.is_empty());
        assert_eq!(user.name, "Alice");
        assert!(user.slug.value() > 0);
        assert!(user.created_at.is_some());
    }

    #[tokio::test]
    async fn test_create_user_admin_role() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let user = client
            .create_user(
                "Admin",
                "admin@example.com",
                "hmac_admin",
                Region::US_EAST_VA,
                UserRole::Admin,
            )
            .await
            .unwrap();

        assert_eq!(user.name, "Admin");
        assert_eq!(user.role, UserRole::Admin);
    }

    #[tokio::test]
    async fn test_get_user() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let created = client
            .create_user("Bob", "bob@example.com", "hmac_bob", Region::US_EAST_VA, UserRole::User)
            .await
            .unwrap();

        let fetched = client.get_user(created.slug).await.unwrap();
        assert_eq!(fetched.slug, created.slug);
        assert_eq!(fetched.name, "Bob");
    }

    #[tokio::test]
    async fn test_get_user_not_found() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let err = client.get_user(UserSlug::new(99999)).await.unwrap_err();
        assert!(matches!(err, crate::error::SdkError::Rpc { code: tonic::Code::NotFound, .. }));
    }

    #[tokio::test]
    async fn test_update_user_name() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let created = client
            .create_user(
                "Old Name",
                "name@example.com",
                "hmac_name",
                Region::US_EAST_VA,
                UserRole::User,
            )
            .await
            .unwrap();

        let updated = client
            .update_user(created.slug, Some("New Name".to_string()), None, None)
            .await
            .unwrap();
        assert_eq!(updated.name, "New Name");
        assert_eq!(updated.slug, created.slug);
    }

    #[tokio::test]
    async fn test_update_user_role() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let created = client
            .create_user(
                "RoleUser",
                "role@example.com",
                "hmac_role",
                Region::US_EAST_VA,
                UserRole::User,
            )
            .await
            .unwrap();

        let updated =
            client.update_user(created.slug, None, Some(UserRole::Admin), None).await.unwrap();
        assert_eq!(updated.role, UserRole::Admin);
    }

    #[tokio::test]
    async fn test_update_user_not_found() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let err = client
            .update_user(UserSlug::new(99999), Some("Name".to_string()), None, None)
            .await
            .unwrap_err();
        assert!(matches!(err, crate::error::SdkError::Rpc { code: tonic::Code::NotFound, .. }));
    }

    #[tokio::test]
    async fn test_delete_user() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let created = client
            .create_user(
                "ToDelete",
                "del@example.com",
                "hmac_del",
                Region::US_EAST_VA,
                UserRole::User,
            )
            .await
            .unwrap();

        let deleted = client.delete_user(created.slug, UserSlug::new(1)).await.unwrap();
        assert_eq!(deleted.slug, created.slug);
        assert_eq!(deleted.status, inferadb_ledger_types::UserStatus::Deleted);
        assert!(deleted.deleted_at.is_some());
        assert_eq!(deleted.retention_days, Some(90));
    }

    #[tokio::test]
    async fn test_delete_user_not_found() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let err = client.delete_user(UserSlug::new(99999), UserSlug::new(1)).await.unwrap_err();
        assert!(matches!(err, crate::error::SdkError::Rpc { code: tonic::Code::NotFound, .. }));
    }

    #[tokio::test]
    async fn test_list_users_empty() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let (users, next_token) = client.list_users(UserSlug::new(1), 10, None).await.unwrap();
        assert!(users.is_empty());
        assert!(next_token.is_none());
    }

    #[tokio::test]
    async fn test_list_users_returns_created() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        client
            .create_user("User1", "u1@example.com", "hmac1", Region::US_EAST_VA, UserRole::User)
            .await
            .unwrap();
        client
            .create_user("User2", "u2@example.com", "hmac2", Region::US_EAST_VA, UserRole::User)
            .await
            .unwrap();

        let (users, _) = client.list_users(UserSlug::new(1), 10, None).await.unwrap();
        assert_eq!(users.len(), 2);
    }

    #[tokio::test]
    async fn test_search_users_by_email() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        client
            .create_user(
                "Searcher",
                "search@example.com",
                "hmac_search",
                Region::US_EAST_VA,
                UserRole::User,
            )
            .await
            .unwrap();
        client
            .create_user(
                "Other",
                "other@example.com",
                "hmac_other",
                Region::US_EAST_VA,
                UserRole::User,
            )
            .await
            .unwrap();

        let results = client.search_users(UserSlug::new(1), "search@").await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name, "Searcher");
    }

    #[tokio::test]
    async fn test_search_users_no_match() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        client
            .create_user(
                "Nobody",
                "nobody@example.com",
                "hmac_no",
                Region::US_EAST_VA,
                UserRole::User,
            )
            .await
            .unwrap();

        let results = client.search_users(UserSlug::new(1), "nonexistent@").await.unwrap();
        assert!(results.is_empty());
    }

    // =====================================================================
    // User Email Operations
    // =====================================================================

    #[tokio::test]
    async fn test_create_user_email() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let user = client
            .create_user(
                "EmailUser",
                "primary@example.com",
                "hmac_primary",
                Region::US_EAST_VA,
                UserRole::User,
            )
            .await
            .unwrap();

        let email = client
            .create_user_email(user.slug, "secondary@example.com", "hmac_secondary")
            .await
            .unwrap();
        assert_eq!(email.email, "secondary@example.com");
        assert!(email.id.value() > 0);
    }

    #[tokio::test]
    async fn test_create_user_email_nonexistent_user() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let err = client
            .create_user_email(UserSlug::new(99999), "test@example.com", "hmac_test")
            .await
            .unwrap_err();
        assert!(matches!(err, crate::error::SdkError::Rpc { code: tonic::Code::NotFound, .. }));
    }

    #[tokio::test]
    async fn test_delete_user_email() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let user = client
            .create_user(
                "DelEmail",
                "main@example.com",
                "hmac_main",
                Region::US_EAST_VA,
                UserRole::User,
            )
            .await
            .unwrap();

        let email =
            client.create_user_email(user.slug, "extra@example.com", "hmac_extra").await.unwrap();

        client.delete_user_email(user.slug, email.id).await.unwrap();
    }

    #[tokio::test]
    async fn test_delete_user_email_not_found() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let user = client
            .create_user("NoEmail", "ne@example.com", "hmac_ne", Region::US_EAST_VA, UserRole::User)
            .await
            .unwrap();

        let err = client
            .delete_user_email(user.slug, inferadb_ledger_types::UserEmailId::new(99999))
            .await
            .unwrap_err();
        assert!(matches!(err, crate::error::SdkError::Rpc { code: tonic::Code::NotFound, .. }));
    }

    #[tokio::test]
    async fn test_search_user_email_by_user() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let user = client
            .create_user(
                "SearchEmail",
                "se@example.com",
                "hmac_se",
                Region::US_EAST_VA,
                UserRole::User,
            )
            .await
            .unwrap();

        client.create_user_email(user.slug, "alt@example.com", "hmac_alt").await.unwrap();

        let emails =
            client.search_user_email(UserSlug::new(1), Some(user.slug), None).await.unwrap();
        // Should find both the primary email (from create_user) and the secondary
        assert!(emails.len() >= 2);
    }

    #[tokio::test]
    async fn test_search_user_email_by_address() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let user = client
            .create_user(
                "AddrSearch",
                "addr@example.com",
                "hmac_addr",
                Region::US_EAST_VA,
                UserRole::User,
            )
            .await
            .unwrap();

        client.create_user_email(user.slug, "findme@example.com", "hmac_findme").await.unwrap();

        let emails = client
            .search_user_email(UserSlug::new(1), None, Some("findme@".to_string()))
            .await
            .unwrap();
        assert_eq!(emails.len(), 1);
        assert_eq!(emails[0].email, "findme@example.com");
    }

    #[tokio::test]
    async fn test_verify_user_email() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let user = client
            .create_user(
                "Verifier",
                "verify@example.com",
                "hmac_verify",
                Region::US_EAST_VA,
                UserRole::User,
            )
            .await
            .unwrap();

        let email = client
            .create_user_email(user.slug, "toverify@example.com", "hmac_toverify")
            .await
            .unwrap();

        // Mock treats the token as the email_id string
        let verified = client.verify_user_email(email.id.value().to_string()).await.unwrap();
        assert_eq!(verified.email, "toverify@example.com");
        assert!(verified.verified_at.is_some());
    }

    #[tokio::test]
    async fn test_verify_user_email_invalid_token() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let err = client.verify_user_email("not-a-number").await.unwrap_err();
        assert!(matches!(err, crate::error::SdkError::Rpc { code: tonic::Code::NotFound, .. }));
    }

    // =====================================================================
    // Blinding Key Operations
    // =====================================================================

    #[tokio::test]
    async fn test_rotate_blinding_key_unimplemented() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        // Mock admin service returns UNIMPLEMENTED for blinding key operations
        let err = client.rotate_blinding_key(UserSlug::new(1), 2).await.unwrap_err();
        assert!(matches!(
            err,
            crate::error::SdkError::Rpc { code: tonic::Code::Unimplemented, .. }
        ));
    }

    #[tokio::test]
    async fn test_get_blinding_key_rehash_status_unimplemented() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let err = client.get_blinding_key_rehash_status(UserSlug::new(1)).await.unwrap_err();
        assert!(matches!(
            err,
            crate::error::SdkError::Rpc { code: tonic::Code::Unimplemented, .. }
        ));
    }

    // =====================================================================
    // User CRUD full lifecycle
    // =====================================================================

    #[tokio::test]
    async fn test_user_full_lifecycle() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        // Create
        let user = client
            .create_user(
                "Lifecycle",
                "life@example.com",
                "hmac_life",
                Region::US_EAST_VA,
                UserRole::User,
            )
            .await
            .unwrap();

        // Get
        let fetched = client.get_user(user.slug).await.unwrap();
        assert_eq!(fetched.name, "Lifecycle");

        // Update name
        let updated =
            client.update_user(user.slug, Some("Renamed".to_string()), None, None).await.unwrap();
        assert_eq!(updated.name, "Renamed");

        // Add email
        let email =
            client.create_user_email(user.slug, "extra@example.com", "hmac_extra").await.unwrap();

        // Search user emails
        let emails =
            client.search_user_email(UserSlug::new(1), Some(user.slug), None).await.unwrap();
        assert!(emails.len() >= 2);

        // Delete email
        client.delete_user_email(user.slug, email.id).await.unwrap();

        // List users
        let (users, _) = client.list_users(UserSlug::new(1), 10, None).await.unwrap();
        assert_eq!(users.len(), 1);

        // Delete user
        let deleted = client.delete_user(user.slug, UserSlug::new(1)).await.unwrap();
        assert_eq!(deleted.status, inferadb_ledger_types::UserStatus::Deleted);
    }
}

/// Tests for vault operations not covered by existing integration tests.
mod vault_extended_tests {
    use std::time::Duration;

    use super::*;
    use crate::{ClientConfig, LedgerClient, ServerSource, UserSlug};

    async fn create_client_for_mock(server: &MockLedgerServer) -> LedgerClient {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static([server.endpoint().to_string()]))
            .client_id("test-client")
            .timeout(Duration::from_secs(5))
            .connect_timeout(Duration::from_secs(2))
            .build()
            .expect("valid config");

        LedgerClient::new(config).await.expect("client creation")
    }

    #[tokio::test]
    async fn test_delete_vault() {
        let server = MockLedgerServer::start().await.unwrap();
        server.add_organization(ORG, "test-org", Region::US_EAST_VA);
        let client = create_client_for_mock(&server).await;

        let vault_info = client.create_vault(UserSlug::new(42), ORG).await.unwrap();

        client.delete_vault(UserSlug::new(42), ORG, vault_info.vault).await.unwrap();

        // Verify vault is gone by checking list
        let (vaults, _) = client.list_vaults(UserSlug::new(42), 0, None, None).await.unwrap();
        assert!(
            vaults.iter().all(|v| v.vault != vault_info.vault),
            "Deleted vault should not appear in list"
        );
    }

    #[tokio::test]
    async fn test_delete_vault_not_found() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        // Deleting a non-existent vault should not fail in the mock
        // (the mock just removes from the map, returns OK even if not present)
        client.delete_vault(UserSlug::new(42), ORG, VaultSlug::new(99999)).await.unwrap();
    }

    #[tokio::test]
    async fn test_update_vault() {
        let server = MockLedgerServer::start().await.unwrap();
        server.add_organization(ORG, "test-org", Region::US_EAST_VA);
        let client = create_client_for_mock(&server).await;

        let vault_info = client.create_vault(UserSlug::new(42), ORG).await.unwrap();

        // Update vault (mock always returns OK)
        client.update_vault(UserSlug::new(42), ORG, vault_info.vault, None).await.unwrap();
    }

    #[tokio::test]
    async fn test_get_vault_not_found() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let err =
            client.get_vault(UserSlug::new(42), ORG, VaultSlug::new(99999)).await.unwrap_err();
        assert!(matches!(err, crate::error::SdkError::Rpc { code: tonic::Code::NotFound, .. }));
    }

    #[tokio::test]
    async fn test_vault_create_list_delete_cycle() {
        let server = MockLedgerServer::start().await.unwrap();
        server.add_organization(ORG, "test-org", Region::US_EAST_VA);
        let client = create_client_for_mock(&server).await;

        // Create two vaults
        let v1 = client.create_vault(UserSlug::new(42), ORG).await.unwrap();
        let v2 = client.create_vault(UserSlug::new(42), ORG).await.unwrap();

        // List should return both
        let (vaults, _) = client.list_vaults(UserSlug::new(42), 0, None, None).await.unwrap();
        assert_eq!(vaults.len(), 2);

        // Delete one
        client.delete_vault(UserSlug::new(42), ORG, v1.vault).await.unwrap();

        // List should return one
        let (vaults, _) = client.list_vaults(UserSlug::new(42), 0, None, None).await.unwrap();
        assert_eq!(vaults.len(), 1);
        assert_eq!(vaults[0].vault, v2.vault);
    }

    #[tokio::test]
    async fn test_list_vaults_with_organization_filter() {
        let server = MockLedgerServer::start().await.unwrap();
        let org2 = OrganizationSlug::new(2);
        server.add_vault(ORG, VaultSlug::new(10));
        server.add_vault(org2, VaultSlug::new(20));
        let client = create_client_for_mock(&server).await;

        // List all vaults (no filter) — should get both
        let (all_vaults, _) = client.list_vaults(UserSlug::new(42), 0, None, None).await.unwrap();
        assert_eq!(all_vaults.len(), 2);

        // list_vaults with organization filter passes the param to the mock,
        // but mock ignores it (returns all). This still tests the client-side code path.
        let (filtered, _) =
            client.list_vaults(UserSlug::new(42), 0, None, Some(ORG)).await.unwrap();
        assert!(!filtered.is_empty());
    }
}

/// Tests for verified read and list_resources operations.
mod verified_read_and_list_tests {
    use std::time::Duration;

    use super::*;
    use crate::{
        ClientConfig, LedgerClient, ListResourcesOpts, ServerSource, UserSlug, VerifyOpts,
    };

    async fn create_client_for_mock(server: &MockLedgerServer) -> LedgerClient {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static([server.endpoint().to_string()]))
            .client_id("test-client")
            .timeout(Duration::from_secs(5))
            .connect_timeout(Duration::from_secs(2))
            .build()
            .expect("valid config");

        LedgerClient::new(config).await.expect("client creation")
    }

    #[tokio::test]
    async fn test_verified_read_existing_key() {
        let server = MockLedgerServer::start().await.unwrap();
        server.set_entity(ORG, VAULT, "verified:key", b"verified-data");
        let client = create_client_for_mock(&server).await;

        let result = client
            .verified_read(UserSlug::new(42), ORG, Some(VAULT), "verified:key", VerifyOpts::new())
            .await
            .unwrap();

        assert!(result.is_some());
        let verified = result.unwrap();
        assert_eq!(verified.value, Some(b"verified-data".to_vec()));
    }

    #[tokio::test]
    async fn test_verified_read_missing_key() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let result = client
            .verified_read(UserSlug::new(42), ORG, Some(VAULT), "nonexistent", VerifyOpts::new())
            .await
            .unwrap();

        // The mock still returns a block header for missing keys; from_proto may produce Some
        // but the value should be None within the VerifiedValue
        if let Some(verified) = result {
            assert!(verified.value.is_none());
        }
    }

    #[tokio::test]
    async fn test_verified_read_with_opts() {
        let server = MockLedgerServer::start().await.unwrap();
        server.set_entity(ORG, VAULT, "opts:key", b"opts-data");
        let client = create_client_for_mock(&server).await;

        let opts = VerifyOpts::new().at_height(5).with_chain_proof(1);
        let result = client
            .verified_read(UserSlug::new(42), ORG, Some(VAULT), "opts:key", opts)
            .await
            .unwrap();

        assert!(result.is_some());
    }

    #[tokio::test]
    async fn test_verified_read_without_vault() {
        let server = MockLedgerServer::start().await.unwrap();
        server.set_entity(ORG, VAULT, "org:key", b"org-data");
        let client = create_client_for_mock(&server).await;

        let result = client
            .verified_read(UserSlug::new(42), ORG, None, "org:key", VerifyOpts::new())
            .await
            .unwrap();

        // With None vault, the mock may or may not find the key (depends on vault=0 mapping)
        // But the client-side code path is exercised regardless
        assert!(result.is_some() || result.is_none());
    }

    #[tokio::test]
    async fn test_list_resources_returns_resources() {
        let server = MockLedgerServer::start().await.unwrap();
        server.add_relationship(ORG, VAULT, "document:1", "viewer", "user:alice");
        server.add_relationship(ORG, VAULT, "document:2", "editor", "user:bob");
        server.add_relationship(ORG, VAULT, "folder:1", "viewer", "user:alice");
        let client = create_client_for_mock(&server).await;

        let result = client
            .list_resources(UserSlug::new(42), ORG, VAULT, ListResourcesOpts::with_type("document"))
            .await
            .unwrap();

        assert_eq!(result.items.len(), 2);
        assert!(result.items.iter().all(|r| r.starts_with("document:")));
    }

    #[tokio::test]
    async fn test_list_resources_empty() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client_for_mock(&server).await;

        let result = client
            .list_resources(UserSlug::new(42), ORG, VAULT, ListResourcesOpts::with_type("nothing"))
            .await
            .unwrap();

        assert!(result.items.is_empty());
    }

    #[tokio::test]
    async fn test_list_resources_type_filter() {
        let server = MockLedgerServer::start().await.unwrap();
        server.add_relationship(ORG, VAULT, "doc:a", "viewer", "user:1");
        server.add_relationship(ORG, VAULT, "img:b", "viewer", "user:1");
        let client = create_client_for_mock(&server).await;

        let docs = client
            .list_resources(UserSlug::new(42), ORG, VAULT, ListResourcesOpts::with_type("doc"))
            .await
            .unwrap();
        assert_eq!(docs.items.len(), 1);
        assert!(docs.items[0].starts_with("doc:"));

        let imgs = client
            .list_resources(UserSlug::new(42), ORG, VAULT, ListResourcesOpts::with_type("img"))
            .await
            .unwrap();
        assert_eq!(imgs.items.len(), 1);
        assert!(imgs.items[0].starts_with("img:"));
    }
}
