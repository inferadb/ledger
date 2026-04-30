//! SchemaService wire-trait stub (Phase 0.0.E.4.5).
//!
//! Per the migration inventory at `docs/migrations/from-grpc.md`,
//! `SchemaService` has no server-side implementation today — the SDK
//! implements schema management as a pure client-side convention layered
//! over `ReadService` / `WriteService` (JSON blobs at well-known key
//! prefixes). The proto definition exists so the wire surface can declare
//! the trait, but no real handlers run on the server.
//!
//! This module provides [`SchemaServiceStub`], an empty struct that
//! implements the wire-generated `SchemaService` trait by returning
//! `WireError { code: Internal }` for every method. The dispatcher
//! (E.4.16) wires this stub in alongside the other 13 service impls so
//! the typed `Dispatcher` impl compiles. F.1 deletes this stub once
//! schema management is either implemented as a real server-side
//! feature or the proto declaration is removed.

use std::collections::BTreeMap;

use inferadb_ledger_wire::{ErrorCode, RequestContext, WireError, services::schema as w};

/// Empty stub for [`inferadb_ledger_wire_services::SchemaService`].
///
/// All methods return `WireError::Internal` with a method-specific
/// message identifying the unimplemented RPC. Real schema management
/// lives in the SDK; this server-side stub exists only to satisfy the
/// typed dispatcher's trait-coverage requirement so the `WireServer`
/// can be wired up before Phase F.
#[derive(Debug, Clone, Copy, Default)]
pub struct SchemaServiceStub;

impl SchemaServiceStub {
    /// Build the canonical "not implemented server-side" error for a
    /// SchemaService method.
    ///
    /// `code` is `Internal` (not `FailedPrecondition` or `Deprecated`)
    /// to signal a programming error — the SDK is expected to never
    /// route schema RPCs to the server, so any frame reaching this stub
    /// indicates a misrouted client. `retryable` is false: retrying the
    /// same RPC will hit the same stub.
    fn unimplemented(method: &str) -> WireError {
        WireError {
            code: ErrorCode::Internal,
            message: format!(
                "SchemaService::{method} is not implemented server-side; use the SDK schema helpers (which dispatch over ReadService/WriteService)"
            ),
            retryable: false,
            retry_after_ms: 0,
            context: BTreeMap::new(),
            suggested_action:
                "use the SDK's schema helpers, which dispatch over ReadService and WriteService"
                    .to_string(),
        }
    }
}

impl inferadb_ledger_wire_services::SchemaService for SchemaServiceStub {
    async fn deploy_schema(
        &self,
        _request: w::DeploySchemaRequest,
        _ctx: RequestContext,
    ) -> Result<w::DeploySchemaResponse, WireError> {
        Err(Self::unimplemented("deploy_schema"))
    }

    async fn list_schema_versions(
        &self,
        _request: w::ListSchemaVersionsRequest,
        _ctx: RequestContext,
    ) -> Result<w::ListSchemaVersionsResponse, WireError> {
        Err(Self::unimplemented("list_schema_versions"))
    }

    async fn get_schema(
        &self,
        _request: w::GetSchemaRequest,
        _ctx: RequestContext,
    ) -> Result<w::GetSchemaResponse, WireError> {
        Err(Self::unimplemented("get_schema"))
    }

    async fn activate_schema(
        &self,
        _request: w::ActivateSchemaRequest,
        _ctx: RequestContext,
    ) -> Result<w::ActivateSchemaResponse, WireError> {
        Err(Self::unimplemented("activate_schema"))
    }

    async fn rollback_schema(
        &self,
        _request: w::RollbackSchemaRequest,
        _ctx: RequestContext,
    ) -> Result<w::RollbackSchemaResponse, WireError> {
        Err(Self::unimplemented("rollback_schema"))
    }

    async fn get_active_schema(
        &self,
        _request: w::GetActiveSchemaRequest,
        _ctx: RequestContext,
    ) -> Result<w::GetActiveSchemaResponse, WireError> {
        Err(Self::unimplemented("get_active_schema"))
    }

    async fn diff_schemas(
        &self,
        _request: w::DiffSchemasRequest,
        _ctx: RequestContext,
    ) -> Result<w::DiffSchemasResponse, WireError> {
        Err(Self::unimplemented("diff_schemas"))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use std::{net::SocketAddr, sync::Arc, time::Instant};

    use bytes::Bytes;
    use inferadb_ledger_wire::{
        AuthMethod, CallerIdentity, ConnectionMetrics,
        services::shared::{OrganizationSlug, UserSlug, VaultSlug},
    };
    use inferadb_ledger_wire_services::SchemaService as _;
    use tokio_util::sync::CancellationToken;

    use super::*;

    /// Minimal `RequestContext` for stub-method tests. Field values are
    /// intentionally placeholder — the stub never reads them.
    fn dummy_ctx() -> RequestContext {
        let peer_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        RequestContext {
            request_id: 0,
            idempotency_token: [0u8; 16],
            trace_id: [0u8; 16],
            span_id: [0u8; 8],
            trace_flags: 0,
            causality_commit_index: 0,
            deadline: None,
            caller_identity: CallerIdentity {
                user_id: None,
                app_id: None,
                organization_id: None,
                auth_method: AuthMethod::Anonymous,
                token_expiry: Instant::now(),
                revocation_epoch: 0,
            },
            peer_addr,
            sdk_version: None,
            forwarded_for: None,
            correlation_id: 0,
            cancellation: CancellationToken::new(),
            conn_metrics: Arc::new(ConnectionMetrics),
        }
    }

    #[test]
    fn unimplemented_helper_carries_internal_code_and_action() {
        let err = SchemaServiceStub::unimplemented("foo");
        assert_eq!(err.code, ErrorCode::Internal);
        assert!(err.message.contains("SchemaService::foo"));
        assert!(err.message.contains("not implemented"));
        assert!(!err.retryable);
        assert_eq!(err.retry_after_ms, 0);
        assert!(err.context.is_empty());
        assert!(!err.suggested_action.is_empty());
        assert!(err.suggested_action.contains("ReadService"));
    }

    #[tokio::test]
    async fn deploy_schema_returns_unimplemented() {
        let stub = SchemaServiceStub;
        let req = w::DeploySchemaRequest {
            organization: Some(OrganizationSlug::new(1)),
            vault: Some(VaultSlug::new(2)),
            definition: Bytes::from_static(b"{}"),
            version: Some(1),
            description: None,
            caller: Some(UserSlug::new(99)),
        };
        let err = stub.deploy_schema(req, dummy_ctx()).await.unwrap_err();
        assert_eq!(err.code, ErrorCode::Internal);
        assert!(err.message.contains("deploy_schema"));
    }

    #[tokio::test]
    async fn list_schema_versions_returns_unimplemented() {
        let stub = SchemaServiceStub;
        let req = w::ListSchemaVersionsRequest {
            organization: Some(OrganizationSlug::new(1)),
            vault: Some(VaultSlug::new(2)),
            caller: None,
        };
        let err = stub.list_schema_versions(req, dummy_ctx()).await.unwrap_err();
        assert_eq!(err.code, ErrorCode::Internal);
        assert!(err.message.contains("list_schema_versions"));
    }

    #[tokio::test]
    async fn get_schema_returns_unimplemented() {
        let stub = SchemaServiceStub;
        let req = w::GetSchemaRequest {
            organization: Some(OrganizationSlug::new(1)),
            vault: Some(VaultSlug::new(2)),
            version: 7,
            caller: None,
        };
        let err = stub.get_schema(req, dummy_ctx()).await.unwrap_err();
        assert_eq!(err.code, ErrorCode::Internal);
        assert!(err.message.contains("get_schema"));
    }

    #[tokio::test]
    async fn activate_schema_returns_unimplemented() {
        let stub = SchemaServiceStub;
        let req = w::ActivateSchemaRequest {
            organization: Some(OrganizationSlug::new(1)),
            vault: Some(VaultSlug::new(2)),
            version: 7,
            caller: None,
        };
        let err = stub.activate_schema(req, dummy_ctx()).await.unwrap_err();
        assert_eq!(err.code, ErrorCode::Internal);
        assert!(err.message.contains("activate_schema"));
    }

    #[tokio::test]
    async fn rollback_schema_returns_unimplemented() {
        let stub = SchemaServiceStub;
        let req = w::RollbackSchemaRequest {
            organization: Some(OrganizationSlug::new(1)),
            vault: Some(VaultSlug::new(2)),
            caller: None,
        };
        let err = stub.rollback_schema(req, dummy_ctx()).await.unwrap_err();
        assert_eq!(err.code, ErrorCode::Internal);
        assert!(err.message.contains("rollback_schema"));
    }

    #[tokio::test]
    async fn get_active_schema_returns_unimplemented() {
        let stub = SchemaServiceStub;
        let req = w::GetActiveSchemaRequest {
            organization: Some(OrganizationSlug::new(1)),
            vault: Some(VaultSlug::new(2)),
            caller: None,
        };
        let err = stub.get_active_schema(req, dummy_ctx()).await.unwrap_err();
        assert_eq!(err.code, ErrorCode::Internal);
        assert!(err.message.contains("get_active_schema"));
    }

    #[tokio::test]
    async fn diff_schemas_returns_unimplemented() {
        let stub = SchemaServiceStub;
        let req = w::DiffSchemasRequest {
            organization: Some(OrganizationSlug::new(1)),
            vault: Some(VaultSlug::new(2)),
            from_version: 1,
            to_version: 2,
            caller: None,
        };
        let err = stub.diff_schemas(req, dummy_ctx()).await.unwrap_err();
        assert_eq!(err.code, ErrorCode::Internal);
        assert!(err.message.contains("diff_schemas"));
    }
}
