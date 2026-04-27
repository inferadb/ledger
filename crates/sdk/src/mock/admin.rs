//! Mock implementation of the admin gRPC service.

use std::sync::{Arc, atomic::Ordering};

use inferadb_ledger_proto::proto;
use tonic::{Request, Response, Status};

use super::MockState;

pub(super) struct MockAdminService {
    pub(super) state: Arc<MockState>,
}

impl MockAdminService {
    pub(super) fn new(state: Arc<MockState>) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl inferadb_ledger_proto::proto::admin_service_server::AdminService for MockAdminService {
    async fn join_cluster(
        &self,
        _request: Request<proto::JoinClusterRequest>,
    ) -> Result<Response<proto::JoinClusterResponse>, Status> {
        self.state.check_injection().await?;

        Ok(Response::new(proto::JoinClusterResponse {
            success: true,
            message: "Joined".to_string(),
            leader_id: 1,
            leader_address: "127.0.0.1:50051".to_string(),
        }))
    }

    async fn leave_cluster(
        &self,
        _request: Request<proto::LeaveClusterRequest>,
    ) -> Result<Response<proto::LeaveClusterResponse>, Status> {
        self.state.check_injection().await?;

        Ok(Response::new(proto::LeaveClusterResponse {
            success: true,
            message: "Left".to_string(),
        }))
    }

    async fn get_decommission_status(
        &self,
        _request: Request<proto::GetDecommissionStatusRequest>,
    ) -> Result<Response<proto::GetDecommissionStatusResponse>, Status> {
        Err(Status::unimplemented("GetDecommissionStatus not supported in mock"))
    }

    async fn check_peer_liveness(
        &self,
        _request: Request<proto::CheckPeerLivenessRequest>,
    ) -> Result<Response<proto::CheckPeerLivenessResponse>, Status> {
        Ok(Response::new(proto::CheckPeerLivenessResponse {
            reachable: false,
            last_seen_ago_ms: u64::MAX,
        }))
    }

    async fn get_cluster_info(
        &self,
        _request: Request<proto::GetClusterInfoRequest>,
    ) -> Result<Response<proto::GetClusterInfoResponse>, Status> {
        self.state.check_injection().await?;

        Ok(Response::new(proto::GetClusterInfoResponse {
            members: vec![proto::ClusterMember {
                node_id: 1,
                address: "127.0.0.1:50051".to_string(),
                role: proto::ClusterMemberRole::Voter as i32,
                is_leader: true,
            }],
            leader_id: 1,
            term: 1,
        }))
    }

    async fn get_node_info(
        &self,
        _request: Request<proto::GetNodeInfoRequest>,
    ) -> Result<Response<proto::GetNodeInfoResponse>, Status> {
        self.state.check_injection().await?;

        Ok(Response::new(proto::GetNodeInfoResponse {
            node_id: 1,
            address: "127.0.0.1:50051".to_string(),
            is_cluster_member: true,
            term: 1,
            cluster_id: 42,
            state: "running".to_string(),
        }))
    }

    async fn init_cluster(
        &self,
        _request: Request<proto::InitClusterRequest>,
    ) -> Result<Response<proto::InitClusterResponse>, Status> {
        self.state.check_injection().await?;

        Ok(Response::new(proto::InitClusterResponse {
            initialized: false,
            cluster_id: 42,
            already_initialized: true,
        }))
    }

    async fn create_snapshot(
        &self,
        request: Request<proto::CreateSnapshotRequest>,
    ) -> Result<Response<proto::CreateSnapshotResponse>, Status> {
        self.state.check_injection().await?;

        let _req = request.into_inner();
        Ok(Response::new(proto::CreateSnapshotResponse {
            block_height: self.state.block_height.load(Ordering::SeqCst),
            state_root: Some(proto::Hash { value: vec![0u8; 32] }),
            snapshot_path: "/tmp/mock-snapshot".to_string(),
        }))
    }

    async fn check_integrity(
        &self,
        _request: Request<proto::CheckIntegrityRequest>,
    ) -> Result<Response<proto::CheckIntegrityResponse>, Status> {
        self.state.check_injection().await?;

        Ok(Response::new(proto::CheckIntegrityResponse { healthy: true, issues: vec![] }))
    }

    async fn recover_vault(
        &self,
        _request: Request<proto::RecoverVaultRequest>,
    ) -> Result<Response<proto::RecoverVaultResponse>, Status> {
        self.state.check_injection().await?;

        Ok(Response::new(proto::RecoverVaultResponse {
            success: true,
            message: "Recovered".to_string(),
            health_status: proto::VaultHealthProto::Healthy as i32,
            final_height: self.state.block_height.load(Ordering::SeqCst),
            final_state_root: Some(proto::Hash { value: vec![0u8; 32] }),
        }))
    }

    async fn simulate_divergence(
        &self,
        _request: Request<proto::SimulateDivergenceRequest>,
    ) -> Result<Response<proto::SimulateDivergenceResponse>, Status> {
        self.state.check_injection().await?;

        Ok(Response::new(proto::SimulateDivergenceResponse {
            success: true,
            message: "Divergence simulated".to_string(),
            health_status: proto::VaultHealthProto::Diverged as i32,
        }))
    }

    async fn force_gc(
        &self,
        _request: Request<proto::ForceGcRequest>,
    ) -> Result<Response<proto::ForceGcResponse>, Status> {
        self.state.check_injection().await?;

        Ok(Response::new(proto::ForceGcResponse {
            success: true,
            message: "GC completed".to_string(),
            expired_count: 0,
            vaults_scanned: 1,
        }))
    }

    async fn update_config(
        &self,
        _request: Request<proto::UpdateConfigRequest>,
    ) -> Result<Response<proto::UpdateConfigResponse>, Status> {
        Ok(Response::new(proto::UpdateConfigResponse {
            applied: false,
            message: "Not supported in mock".to_string(),
            current_config_json: "{}".to_string(),
            changed_fields: Vec::new(),
        }))
    }

    async fn get_config(
        &self,
        _request: Request<proto::GetConfigRequest>,
    ) -> Result<Response<proto::GetConfigResponse>, Status> {
        Ok(Response::new(proto::GetConfigResponse { config_json: "{}".to_string() }))
    }

    async fn create_backup(
        &self,
        _request: Request<proto::CreateBackupRequest>,
    ) -> Result<Response<proto::CreateBackupResponse>, Status> {
        Err(Status::unimplemented("Backup not supported in mock"))
    }

    async fn list_backups(
        &self,
        _request: Request<proto::ListBackupsRequest>,
    ) -> Result<Response<proto::ListBackupsResponse>, Status> {
        Ok(Response::new(proto::ListBackupsResponse { backups: Vec::new() }))
    }

    async fn restore_backup(
        &self,
        _request: Request<proto::RestoreBackupRequest>,
    ) -> Result<Response<proto::RestoreBackupResponse>, Status> {
        Err(Status::unimplemented("Restore not supported in mock"))
    }

    async fn transfer_leadership(
        &self,
        _request: Request<proto::TransferLeadershipRequest>,
    ) -> Result<Response<proto::TransferLeadershipResponse>, Status> {
        Err(Status::unimplemented("Leader transfer not supported in mock"))
    }

    async fn rotate_blinding_key(
        &self,
        _request: Request<proto::RotateBlindingKeyRequest>,
    ) -> Result<Response<proto::RotateBlindingKeyResponse>, Status> {
        Err(Status::unimplemented("Blinding key rotation not supported in mock"))
    }

    async fn get_blinding_key_rehash_status(
        &self,
        _request: Request<proto::GetBlindingKeyRehashStatusRequest>,
    ) -> Result<Response<proto::GetBlindingKeyRehashStatusResponse>, Status> {
        Err(Status::unimplemented("Blinding key rehash status not supported in mock"))
    }

    async fn rotate_region_key(
        &self,
        _request: Request<proto::RotateRegionKeyRequest>,
    ) -> Result<Response<proto::RotateRegionKeyResponse>, Status> {
        Err(Status::unimplemented("Region key rotation not supported in mock"))
    }

    async fn get_rewrap_status(
        &self,
        _request: Request<proto::GetRewrapStatusRequest>,
    ) -> Result<Response<proto::GetRewrapStatusResponse>, Status> {
        Err(Status::unimplemented("Rewrap status not supported in mock"))
    }

    async fn migrate_existing_users(
        &self,
        _request: Request<proto::MigrateExistingUsersRequest>,
    ) -> Result<Response<proto::MigrateExistingUsersResponse>, Status> {
        self.state.check_injection().await?;

        Ok(Response::new(proto::MigrateExistingUsersResponse {
            users: 0,
            migrated: 0,
            skipped: 0,
            errors: 0,
            elapsed_secs: 0.0,
        }))
    }

    async fn provision_region(
        &self,
        request: Request<proto::ProvisionRegionRequest>,
    ) -> Result<Response<proto::ProvisionRegionResponse>, Status> {
        self.state.check_injection().await?;

        let req = request.into_inner();
        Ok(Response::new(proto::ProvisionRegionResponse { created: true, name: req.name }))
    }
}
