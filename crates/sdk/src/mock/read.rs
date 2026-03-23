//! Mock implementation of the read gRPC service.

use std::sync::{Arc, atomic::Ordering};

use inferadb_ledger_proto::proto;
use inferadb_ledger_types::{OrganizationSlug, VaultSlug};
use tonic::{Request, Response, Status};

use super::MockState;

pub(super) struct MockReadService {
    pub(super) state: Arc<MockState>,
}

impl MockReadService {
    pub(super) fn new(state: Arc<MockState>) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl inferadb_ledger_proto::proto::read_service_server::ReadService for MockReadService {
    async fn read(
        &self,
        request: Request<proto::ReadRequest>,
    ) -> Result<Response<proto::ReadResponse>, Status> {
        self.state.check_injection().await?;

        self.state.read_count.fetch_add(1, Ordering::SeqCst);

        let req = request.into_inner();
        let organization = OrganizationSlug::new(req.organization.map_or(0, |n| n.slug));
        let vault = VaultSlug::new(req.vault.map_or(0, |v| v.slug));

        let entities = self.state.entities.read();
        let key = (organization, vault, req.key);

        let (value, block_height) = match entities.get(&key) {
            Some((v, ..)) => (Some(v.clone()), self.state.block_height.load(Ordering::SeqCst)),
            None => (None, self.state.block_height.load(Ordering::SeqCst)),
        };

        Ok(Response::new(proto::ReadResponse { value, block_height }))
    }

    async fn batch_read(
        &self,
        request: Request<proto::BatchReadRequest>,
    ) -> Result<Response<proto::BatchReadResponse>, Status> {
        self.state.check_injection().await?;

        self.state.read_count.fetch_add(1, Ordering::SeqCst);

        let req = request.into_inner();
        let organization = OrganizationSlug::new(req.organization.map_or(0, |n| n.slug));
        let vault = VaultSlug::new(req.vault.map_or(0, |v| v.slug));

        let entities = self.state.entities.read();
        let results: Vec<proto::BatchReadResult> = req
            .keys
            .iter()
            .map(|key| {
                let entity_key = (organization, vault, key.clone());
                match entities.get(&entity_key) {
                    Some((v, ..)) => proto::BatchReadResult {
                        key: key.clone(),
                        value: Some(v.clone()),
                        found: true,
                    },
                    None => proto::BatchReadResult { key: key.clone(), value: None, found: false },
                }
            })
            .collect();

        Ok(Response::new(proto::BatchReadResponse {
            results,
            block_height: self.state.block_height.load(Ordering::SeqCst),
        }))
    }

    async fn verified_read(
        &self,
        request: Request<proto::VerifiedReadRequest>,
    ) -> Result<Response<proto::VerifiedReadResponse>, Status> {
        self.state.check_injection().await?;

        self.state.read_count.fetch_add(1, Ordering::SeqCst);

        let req = request.into_inner();
        let organization = OrganizationSlug::new(req.organization.map_or(0, |n| n.slug));
        let vault = VaultSlug::new(req.vault.map_or(0, |v| v.slug));

        let entities = self.state.entities.read();
        let key = (organization, vault, req.key);
        let block_height = self.state.block_height.load(Ordering::SeqCst);

        let value = entities.get(&key).map(|(v, ..)| v.clone());

        // Create minimal valid proofs for testing
        let state_root = vec![0u8; 32];
        let block_header = proto::BlockHeader {
            height: block_height,
            organization: Some(proto::OrganizationSlug { slug: organization.value() }),
            vault: Some(proto::VaultSlug { slug: vault.value() }),
            previous_hash: Some(proto::Hash { value: vec![0u8; 32] }),
            tx_merkle_root: Some(proto::Hash { value: vec![0u8; 32] }),
            state_root: Some(proto::Hash { value: state_root.clone() }),
            timestamp: None,
            leader_id: Some(proto::NodeId { id: "mock-node".to_string() }),
            term: 1,
            committed_index: block_height,
            block_hash: Some(proto::Hash { value: vec![0u8; 32] }),
        };

        // Create a simple merkle proof (single element tree)
        let merkle_proof = proto::MerkleProof {
            leaf_hash: Some(proto::Hash { value: state_root }),
            siblings: vec![],
        };

        Ok(Response::new(proto::VerifiedReadResponse {
            value,
            block_height,
            block_header: Some(block_header),
            merkle_proof: Some(merkle_proof),
            chain_proof: None,
        }))
    }

    async fn historical_read(
        &self,
        request: Request<proto::HistoricalReadRequest>,
    ) -> Result<Response<proto::HistoricalReadResponse>, Status> {
        self.state.check_injection().await?;

        self.state.read_count.fetch_add(1, Ordering::SeqCst);

        let req = request.into_inner();
        let organization = OrganizationSlug::new(req.organization.map_or(0, |n| n.slug));
        let vault = VaultSlug::new(req.vault.map_or(0, |v| v.slug));

        let entities = self.state.entities.read();
        let key = (organization, vault, req.key);
        let value = entities.get(&key).map(|(v, ..)| v.clone());

        Ok(Response::new(proto::HistoricalReadResponse {
            value,
            block_height: req.at_height,
            block_header: None,
            merkle_proof: None,
            chain_proof: None,
        }))
    }

    type WatchBlocksStream = futures::stream::Pending<Result<proto::BlockAnnouncement, Status>>;

    async fn watch_blocks(
        &self,
        _request: Request<proto::WatchBlocksRequest>,
    ) -> Result<Response<Self::WatchBlocksStream>, Status> {
        self.state.check_injection().await?;

        // Return a pending stream for now - integration tests will need a more sophisticated mock
        Ok(Response::new(futures::stream::pending()))
    }

    async fn get_block(
        &self,
        request: Request<proto::GetBlockRequest>,
    ) -> Result<Response<proto::GetBlockResponse>, Status> {
        self.state.check_injection().await?;

        let req = request.into_inner();
        let organization = OrganizationSlug::new(req.organization.map_or(0, |n| n.slug));
        let vault = VaultSlug::new(req.vault.map_or(0, |v| v.slug));

        let block = proto::Block {
            header: Some(proto::BlockHeader {
                height: req.height,
                organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                vault: Some(proto::VaultSlug { slug: vault.value() }),
                previous_hash: Some(proto::Hash { value: vec![0u8; 32] }),
                tx_merkle_root: Some(proto::Hash { value: vec![0u8; 32] }),
                state_root: Some(proto::Hash { value: vec![0u8; 32] }),
                timestamp: None,
                leader_id: Some(proto::NodeId { id: "mock-node".to_string() }),
                term: 1,
                committed_index: req.height,
                block_hash: Some(proto::Hash { value: vec![0u8; 32] }),
            }),
            transactions: vec![],
        };

        Ok(Response::new(proto::GetBlockResponse { block: Some(block) }))
    }

    async fn get_block_range(
        &self,
        request: Request<proto::GetBlockRangeRequest>,
    ) -> Result<Response<proto::GetBlockRangeResponse>, Status> {
        self.state.check_injection().await?;

        let req = request.into_inner();
        let organization = OrganizationSlug::new(req.organization.map_or(0, |n| n.slug));
        let vault = VaultSlug::new(req.vault.map_or(0, |v| v.slug));

        let blocks: Vec<proto::Block> = (req.start_height..=req.end_height)
            .map(|height| proto::Block {
                header: Some(proto::BlockHeader {
                    height,
                    organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                    vault: Some(proto::VaultSlug { slug: vault.value() }),
                    previous_hash: Some(proto::Hash { value: vec![0u8; 32] }),
                    tx_merkle_root: Some(proto::Hash { value: vec![0u8; 32] }),
                    state_root: Some(proto::Hash { value: vec![0u8; 32] }),
                    timestamp: None,
                    leader_id: Some(proto::NodeId { id: "mock-node".to_string() }),
                    term: 1,
                    committed_index: height,
                    block_hash: Some(proto::Hash { value: vec![0u8; 32] }),
                }),
                transactions: vec![],
            })
            .collect();

        Ok(Response::new(proto::GetBlockRangeResponse {
            blocks,
            current_tip: self.state.block_height.load(Ordering::SeqCst),
        }))
    }

    async fn get_tip(
        &self,
        request: Request<proto::GetTipRequest>,
    ) -> Result<Response<proto::GetTipResponse>, Status> {
        self.state.check_injection().await?;

        let _req = request.into_inner();
        let height = self.state.block_height.load(Ordering::SeqCst);

        Ok(Response::new(proto::GetTipResponse {
            height,
            block_hash: Some(proto::Hash { value: vec![0u8; 32] }),
            state_root: Some(proto::Hash { value: vec![0u8; 32] }),
        }))
    }

    async fn get_client_state(
        &self,
        request: Request<proto::GetClientStateRequest>,
    ) -> Result<Response<proto::GetClientStateResponse>, Status> {
        self.state.check_injection().await?;

        let req = request.into_inner();
        let organization = OrganizationSlug::new(req.organization.map_or(0, |n| n.slug));
        let vault = VaultSlug::new(req.vault.map_or(0, |v| v.slug));
        let client_id = req.client_id.map(|c| c.id).unwrap_or_default();

        let sequences = self.state.client_sequences.read();
        let last_seq = sequences.get(&(organization, vault, client_id)).copied().unwrap_or(0);

        Ok(Response::new(proto::GetClientStateResponse { last_committed_sequence: last_seq }))
    }

    async fn list_relationships(
        &self,
        request: Request<proto::ListRelationshipsRequest>,
    ) -> Result<Response<proto::ListRelationshipsResponse>, Status> {
        self.state.check_injection().await?;

        self.state.read_count.fetch_add(1, Ordering::SeqCst);

        let req = request.into_inner();
        let organization = OrganizationSlug::new(req.organization.map_or(0, |n| n.slug));
        let vault = VaultSlug::new(req.vault.map_or(0, |v| v.slug));

        let relationships = self.state.relationships.read();
        let rels = relationships.get(&(organization, vault)).cloned().unwrap_or_default();

        // Apply filters
        let filtered: Vec<proto::Relationship> = rels
            .into_iter()
            .filter(|r| req.resource.as_ref().is_none_or(|f| &r.resource == f))
            .filter(|r| req.relation.as_ref().is_none_or(|f| &r.relation == f))
            .filter(|r| req.subject.as_ref().is_none_or(|f| &r.subject == f))
            .take(req.limit.max(100) as usize)
            .collect();

        Ok(Response::new(proto::ListRelationshipsResponse {
            relationships: filtered,
            block_height: self.state.block_height.load(Ordering::SeqCst),
            next_page_token: String::new(),
        }))
    }

    async fn list_resources(
        &self,
        request: Request<proto::ListResourcesRequest>,
    ) -> Result<Response<proto::ListResourcesResponse>, Status> {
        self.state.check_injection().await?;

        self.state.read_count.fetch_add(1, Ordering::SeqCst);

        let req = request.into_inner();
        let organization = OrganizationSlug::new(req.organization.map_or(0, |n| n.slug));
        let vault = VaultSlug::new(req.vault.map_or(0, |v| v.slug));

        let relationships = self.state.relationships.read();
        let resources: Vec<String> = relationships
            .get(&(organization, vault))
            .map(|rels| {
                rels.iter()
                    .filter(|r| r.resource.starts_with(&format!("{}:", req.resource_type)))
                    .map(|r| r.resource.clone())
                    .collect::<std::collections::HashSet<_>>()
                    .into_iter()
                    .take(req.limit.max(100) as usize)
                    .collect()
            })
            .unwrap_or_default();

        Ok(Response::new(proto::ListResourcesResponse {
            resources,
            block_height: self.state.block_height.load(Ordering::SeqCst),
            next_page_token: String::new(),
        }))
    }

    async fn list_entities(
        &self,
        request: Request<proto::ListEntitiesRequest>,
    ) -> Result<Response<proto::ListEntitiesResponse>, Status> {
        self.state.check_injection().await?;

        self.state.read_count.fetch_add(1, Ordering::SeqCst);

        let req = request.into_inner();
        let organization = OrganizationSlug::new(req.organization.map_or(0, |n| n.slug));

        let entities = self.state.entities.read();
        let matching: Vec<proto::Entity> = entities
            .iter()
            .filter(|((ns, _vault, key), _)| {
                *ns == organization && key.starts_with(&req.key_prefix)
            })
            .map(|((_, _, key), (value, version, expires_at))| proto::Entity {
                key: key.clone(),
                value: value.clone(),
                expires_at: *expires_at,
                version: *version,
            })
            .take(req.limit.max(100) as usize)
            .collect();

        Ok(Response::new(proto::ListEntitiesResponse {
            entities: matching,
            block_height: self.state.block_height.load(Ordering::SeqCst),
            next_page_token: String::new(),
        }))
    }
}
