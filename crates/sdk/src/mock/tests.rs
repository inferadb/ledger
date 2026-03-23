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
}
