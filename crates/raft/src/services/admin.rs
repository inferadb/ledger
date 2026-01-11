//! Admin service implementation.
//!
//! Handles namespace and vault management, snapshots, and integrity checks.

use std::sync::Arc;

use openraft::Raft;
use parking_lot::RwLock;
use tonic::{Request, Response, Status};

use crate::proto::admin_service_server::AdminService;
use crate::proto::{
    CheckIntegrityRequest, CheckIntegrityResponse, CreateNamespaceRequest, CreateNamespaceResponse,
    CreateSnapshotRequest, CreateSnapshotResponse, CreateVaultRequest, CreateVaultResponse,
    DeleteNamespaceRequest, DeleteNamespaceResponse, DeleteVaultRequest, DeleteVaultResponse,
    GetNamespaceRequest, GetNamespaceResponse, GetVaultRequest, GetVaultResponse,
    ListNamespacesRequest, ListNamespacesResponse, ListVaultsRequest, ListVaultsResponse,
    NamespaceId, ShardId, VaultId,
};
use crate::types::{LedgerRequest, LedgerResponse, LedgerTypeConfig};

use ledger_storage::StateLayer;

/// Admin service implementation.
pub struct AdminServiceImpl {
    /// The Raft instance.
    raft: Arc<Raft<LedgerTypeConfig>>,
    /// The state layer.
    #[allow(dead_code)]
    state: Arc<RwLock<StateLayer>>,
}

impl AdminServiceImpl {
    /// Create a new admin service.
    pub fn new(raft: Arc<Raft<LedgerTypeConfig>>, state: Arc<RwLock<StateLayer>>) -> Self {
        Self { raft, state }
    }
}

#[tonic::async_trait]
impl AdminService for AdminServiceImpl {
    async fn create_namespace(
        &self,
        request: Request<CreateNamespaceRequest>,
    ) -> Result<Response<CreateNamespaceResponse>, Status> {
        let req = request.into_inner();

        // Submit create namespace through Raft
        let ledger_request = LedgerRequest::CreateNamespace { name: req.name };

        let result = self
            .raft
            .client_write(ledger_request)
            .await
            .map_err(|e| Status::internal(format!("Raft error: {}", e)))?;

        match result.data {
            LedgerResponse::NamespaceCreated { namespace_id } => {
                Ok(Response::new(CreateNamespaceResponse {
                    namespace_id: Some(NamespaceId {
                        id: namespace_id as i64,
                    }),
                    shard_id: Some(ShardId { id: 0 }), // Default shard
                }))
            }
            LedgerResponse::Error { message } => Err(Status::internal(message)),
            _ => Err(Status::internal("Unexpected response type")),
        }
    }

    async fn delete_namespace(
        &self,
        request: Request<DeleteNamespaceRequest>,
    ) -> Result<Response<DeleteNamespaceResponse>, Status> {
        let req = request.into_inner();

        let namespace_id = req
            .namespace_id
            .as_ref()
            .map(|n| n.id)
            .ok_or_else(|| Status::invalid_argument("Missing namespace_id"))?;

        // Submit delete namespace through Raft
        let ledger_request = LedgerRequest::DeleteNamespace {
            namespace_id: namespace_id,
        };

        let result = self
            .raft
            .client_write(ledger_request)
            .await
            .map_err(|e| Status::internal(format!("Raft error: {}", e)))?;

        match result.data {
            LedgerResponse::NamespaceDeleted { success } => {
                if success {
                    Ok(Response::new(DeleteNamespaceResponse {
                        deleted_at: Some(prost_types::Timestamp::from(std::time::SystemTime::now())),
                    }))
                } else {
                    Err(Status::failed_precondition(
                        "Namespace has vaults, cannot delete",
                    ))
                }
            }
            LedgerResponse::Error { message } => Err(Status::internal(message)),
            _ => Err(Status::internal("Unexpected response type")),
        }
    }

    async fn get_namespace(
        &self,
        request: Request<GetNamespaceRequest>,
    ) -> Result<Response<GetNamespaceResponse>, Status> {
        let req = request.into_inner();

        // Extract namespace ID from lookup oneof
        let namespace_id = match req.lookup {
            Some(crate::proto::get_namespace_request::Lookup::NamespaceId(n)) => n.id,
            Some(crate::proto::get_namespace_request::Lookup::Name(_name)) => {
                // TODO: Lookup by name from _system namespace
                return Err(Status::unimplemented("Lookup by name not yet supported"));
            }
            None => return Err(Status::invalid_argument("Missing namespace lookup")),
        };

        // TODO: Implement namespace lookup from _system namespace
        // For now, return placeholder response
        Ok(Response::new(GetNamespaceResponse {
            namespace_id: Some(NamespaceId { id: namespace_id }),
            name: format!("namespace_{}", namespace_id),
            shard_id: Some(ShardId { id: 0 }),
            member_nodes: vec![],
            leader_hint: None,
            config_version: 0,
            status: crate::proto::NamespaceStatus::Active.into(),
        }))
    }

    async fn list_namespaces(
        &self,
        _request: Request<ListNamespacesRequest>,
    ) -> Result<Response<ListNamespacesResponse>, Status> {
        // TODO: Implement namespace listing from _system namespace
        Ok(Response::new(ListNamespacesResponse {
            namespaces: vec![],
            next_page_token: None,
        }))
    }

    async fn create_vault(
        &self,
        request: Request<CreateVaultRequest>,
    ) -> Result<Response<CreateVaultResponse>, Status> {
        let req = request.into_inner();

        let namespace_id = req
            .namespace_id
            .as_ref()
            .map(|n| n.id)
            .ok_or_else(|| Status::invalid_argument("Missing namespace_id"))?;

        // Submit create vault through Raft
        let ledger_request = LedgerRequest::CreateVault {
            namespace_id: namespace_id,
            name: None, // CreateVaultRequest doesn't have name field
        };

        let result = self
            .raft
            .client_write(ledger_request)
            .await
            .map_err(|e| Status::internal(format!("Raft error: {}", e)))?;

        match result.data {
            LedgerResponse::VaultCreated { vault_id } => {
                Ok(Response::new(CreateVaultResponse {
                    vault_id: Some(VaultId {
                        id: vault_id as i64,
                    }),
                    genesis: None, // TODO: Include genesis block header
                }))
            }
            LedgerResponse::Error { message } => Err(Status::internal(message)),
            _ => Err(Status::internal("Unexpected response type")),
        }
    }

    async fn delete_vault(
        &self,
        request: Request<DeleteVaultRequest>,
    ) -> Result<Response<DeleteVaultResponse>, Status> {
        let req = request.into_inner();

        let namespace_id = req
            .namespace_id
            .as_ref()
            .map(|n| n.id)
            .ok_or_else(|| Status::invalid_argument("Missing namespace_id"))?;

        let vault_id = req
            .vault_id
            .as_ref()
            .map(|v| v.id)
            .ok_or_else(|| Status::invalid_argument("Missing vault_id"))?;

        // Submit delete vault through Raft
        let ledger_request = LedgerRequest::DeleteVault {
            namespace_id: namespace_id,
            vault_id: vault_id,
        };

        let result = self
            .raft
            .client_write(ledger_request)
            .await
            .map_err(|e| Status::internal(format!("Raft error: {}", e)))?;

        match result.data {
            LedgerResponse::VaultDeleted { success } => {
                if success {
                    Ok(Response::new(DeleteVaultResponse {
                        deleted_at: Some(prost_types::Timestamp::from(std::time::SystemTime::now())),
                    }))
                } else {
                    Err(Status::internal("Failed to delete vault"))
                }
            }
            LedgerResponse::Error { message } => Err(Status::internal(message)),
            _ => Err(Status::internal("Unexpected response type")),
        }
    }

    async fn get_vault(
        &self,
        request: Request<GetVaultRequest>,
    ) -> Result<Response<GetVaultResponse>, Status> {
        let req = request.into_inner();

        let namespace_id = req
            .namespace_id
            .as_ref()
            .map(|n| n.id)
            .ok_or_else(|| Status::invalid_argument("Missing namespace_id"))?;

        let vault_id = req
            .vault_id
            .as_ref()
            .map(|v| v.id)
            .ok_or_else(|| Status::invalid_argument("Missing vault_id"))?;

        // TODO: Implement vault lookup from state layer
        Ok(Response::new(GetVaultResponse {
            namespace_id: Some(NamespaceId { id: namespace_id }),
            vault_id: Some(VaultId { id: vault_id }),
            height: 0, // TODO: Get from state
            state_root: None,
            nodes: vec![],
            leader: None,
            status: crate::proto::VaultStatus::Active.into(),
            retention_policy: None,
        }))
    }

    async fn list_vaults(
        &self,
        _request: Request<ListVaultsRequest>,
    ) -> Result<Response<ListVaultsResponse>, Status> {
        // TODO: Implement vault listing from state layer
        Ok(Response::new(ListVaultsResponse { vaults: vec![] }))
    }

    async fn create_snapshot(
        &self,
        request: Request<CreateSnapshotRequest>,
    ) -> Result<Response<CreateSnapshotResponse>, Status> {
        let _req = request.into_inner();

        // Trigger Raft snapshot
        let _ = self
            .raft
            .trigger()
            .snapshot()
            .await
            .map_err(|e| Status::internal(format!("Snapshot error: {}", e)))?;

        Ok(Response::new(CreateSnapshotResponse {
            block_height: 0, // TODO: Get actual height
            state_root: None,
            snapshot_path: format!("/snapshots/snapshot_{}", chrono::Utc::now().timestamp()),
        }))
    }

    async fn check_integrity(
        &self,
        _request: Request<CheckIntegrityRequest>,
    ) -> Result<Response<CheckIntegrityResponse>, Status> {
        // TODO: Implement integrity check
        Ok(Response::new(CheckIntegrityResponse {
            healthy: true,
            issues: vec![],
        }))
    }
}
