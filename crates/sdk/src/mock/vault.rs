//! Mock implementation of the vault gRPC service.

use std::sync::{Arc, atomic::Ordering};

use inferadb_ledger_proto::proto;
use inferadb_ledger_types::{OrganizationSlug, VaultSlug};
use tonic::{Request, Response, Status};

use super::{MockState, VaultData};

pub(super) struct MockVaultService {
    pub(super) state: Arc<MockState>,
}

impl MockVaultService {
    pub(super) fn new(state: Arc<MockState>) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl inferadb_ledger_proto::proto::vault_service_server::VaultService for MockVaultService {
    async fn create_vault(
        &self,
        request: Request<proto::CreateVaultRequest>,
    ) -> Result<Response<proto::CreateVaultResponse>, Status> {
        self.state.check_injection().await?;

        let req = request.into_inner();
        let organization = OrganizationSlug::new(req.organization.map_or(0, |n| n.slug));
        let vault = VaultSlug::new(self.state.next_vault.fetch_add(1, Ordering::SeqCst));

        {
            let mut vaults = self.state.vaults.write();
            vaults.insert(
                (organization, vault),
                VaultData {
                    height: 0,
                    state_root: vec![0u8; 32],
                    status: proto::VaultStatus::Active as i32,
                },
            );
        }

        Ok(Response::new(proto::CreateVaultResponse {
            vault: Some(proto::VaultSlug { slug: vault.value() }),
            genesis: Some(proto::BlockHeader {
                height: 0,
                organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                vault: Some(proto::VaultSlug { slug: vault.value() }),
                previous_hash: None,
                tx_merkle_root: Some(proto::Hash { value: vec![0u8; 32] }),
                state_root: Some(proto::Hash { value: vec![0u8; 32] }),
                timestamp: None,
                leader_id: Some(proto::NodeId { id: "mock-node".to_string() }),
                term: 1,
                committed_index: 0,
                block_hash: Some(proto::Hash { value: vec![0u8; 32] }),
            }),
        }))
    }

    async fn delete_vault(
        &self,
        request: Request<proto::DeleteVaultRequest>,
    ) -> Result<Response<proto::DeleteVaultResponse>, Status> {
        self.state.check_injection().await?;

        let req = request.into_inner();
        let organization = OrganizationSlug::new(req.organization.map_or(0, |n| n.slug));
        let vault = VaultSlug::new(req.vault.map_or(0, |v| v.slug));
        self.state.vaults.write().remove(&(organization, vault));

        let now = chrono::Utc::now();
        Ok(Response::new(proto::DeleteVaultResponse {
            deleted_at: Some(prost_types::Timestamp {
                seconds: now.timestamp(),
                nanos: now.timestamp_subsec_nanos() as i32,
            }),
        }))
    }

    async fn get_vault(
        &self,
        request: Request<proto::GetVaultRequest>,
    ) -> Result<Response<proto::GetVaultResponse>, Status> {
        self.state.check_injection().await?;

        let req = request.into_inner();
        let organization = OrganizationSlug::new(req.organization.map_or(0, |n| n.slug));
        let vault = VaultSlug::new(req.vault.map_or(0, |v| v.slug));

        let vaults = self.state.vaults.read();
        let data = vaults
            .get(&(organization, vault))
            .ok_or_else(|| Status::not_found("Vault not found"))?;

        Ok(Response::new(proto::GetVaultResponse {
            organization: Some(proto::OrganizationSlug { slug: organization.value() }),
            vault: Some(proto::VaultSlug { slug: vault.value() }),
            height: data.height,
            state_root: Some(proto::Hash { value: data.state_root.clone() }),
            nodes: vec![],
            leader: Some(proto::NodeId { id: "mock-node".to_string() }),
            status: data.status,
            retention_policy: None,
        }))
    }

    async fn list_vaults(
        &self,
        _request: Request<proto::ListVaultsRequest>,
    ) -> Result<Response<proto::ListVaultsResponse>, Status> {
        self.state.check_injection().await?;

        let vaults = self.state.vaults.read();
        let responses: Vec<proto::GetVaultResponse> = vaults
            .iter()
            .map(|((organization, vault), data)| proto::GetVaultResponse {
                organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                vault: Some(proto::VaultSlug { slug: vault.value() }),
                height: data.height,
                state_root: Some(proto::Hash { value: data.state_root.clone() }),
                nodes: vec![],
                leader: Some(proto::NodeId { id: "mock-node".to_string() }),
                status: data.status,
                retention_policy: None,
            })
            .collect();

        Ok(Response::new(proto::ListVaultsResponse { vaults: responses, next_page_token: None }))
    }

    async fn update_vault(
        &self,
        _request: Request<proto::UpdateVaultRequest>,
    ) -> Result<Response<proto::UpdateVaultResponse>, Status> {
        self.state.check_injection().await?;
        Ok(Response::new(proto::UpdateVaultResponse {}))
    }
}
