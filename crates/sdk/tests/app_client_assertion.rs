//! Integration tests for `LedgerClient::get_app_client_assertion`.
//!
//! Exercises the single-fetch RPC against the mock server: a seeded
//! assertion is returned by ID, and an unknown ID surfaces as
//! `SdkError::Rpc { code: NotFound, .. }`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]

use inferadb_ledger_sdk::{
    AppSlug, ClientAssertionId, ClientConfig, LedgerClient, OrganizationSlug, SdkError,
    ServerSource, UserSlug, mock::MockLedgerServer,
};

const SEEDED_ASSERTION_ID: i64 = 42;
const UNKNOWN_ASSERTION_ID: i64 = 99_999;

fn build_client_config(endpoint: &str) -> ClientConfig {
    ClientConfig::builder()
        .servers(ServerSource::from_static([endpoint.to_string()]))
        .client_id("test")
        .build()
        .expect("client config")
}

#[tokio::test]
async fn get_app_client_assertion_returns_assertion_by_id() {
    let server = MockLedgerServer::start().await.expect("mock server");
    let organization = OrganizationSlug::new(1);
    let app = AppSlug::new(1);
    let assertion = ClientAssertionId::new(SEEDED_ASSERTION_ID);

    server.add_app_client_assertion(organization, app, assertion, "primary", true);

    let config = build_client_config(server.endpoint());
    let client = LedgerClient::new(config).await.expect("client");

    let info = client
        .get_app_client_assertion(UserSlug::new(1), organization, app, assertion)
        .await
        .expect("get must succeed");

    assert_eq!(info.id.value(), SEEDED_ASSERTION_ID);
    assert_eq!(info.name, "primary");
    assert!(info.enabled);
}

#[tokio::test]
async fn get_app_client_assertion_unknown_id_returns_not_found() {
    let server = MockLedgerServer::start().await.expect("mock server");
    let config = build_client_config(server.endpoint());
    let client = LedgerClient::new(config).await.expect("client");

    let err = client
        .get_app_client_assertion(
            UserSlug::new(1),
            OrganizationSlug::new(1),
            AppSlug::new(1),
            ClientAssertionId::new(UNKNOWN_ASSERTION_ID),
        )
        .await
        .expect_err("unknown id must error");

    assert!(
        matches!(err, SdkError::Rpc { code: tonic::Code::NotFound, .. }),
        "expected NotFound RPC error, got: {err:?}"
    );
}
