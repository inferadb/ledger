//! Mock implementation of the token gRPC service.

use std::sync::Arc;

use inferadb_ledger_proto::proto;
use tonic::{Request, Response, Status};

use super::MockState;

pub(super) struct MockTokenService {
    pub(super) state: Arc<MockState>,
}

impl MockTokenService {
    pub(super) fn new(state: Arc<MockState>) -> Self {
        Self { state }
    }

    fn mock_token_pair() -> proto::TokenPair {
        proto::TokenPair {
            access_token: "mock-access-token".to_string(),
            refresh_token: "mock-refresh-token".to_string(),
            access_expires_at: None,
            refresh_expires_at: None,
        }
    }

    fn now_timestamp() -> prost_types::Timestamp {
        let now =
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
        prost_types::Timestamp { seconds: now.as_secs() as i64, nanos: now.subsec_nanos() as i32 }
    }
}

#[tonic::async_trait]
impl proto::token_service_server::TokenService for MockTokenService {
    async fn create_user_session(
        &self,
        _request: Request<proto::CreateUserSessionRequest>,
    ) -> Result<Response<proto::CreateUserSessionResponse>, Status> {
        self.state.check_injection().await?;

        Ok(Response::new(proto::CreateUserSessionResponse {
            tokens: Some(Self::mock_token_pair()),
        }))
    }

    async fn validate_token(
        &self,
        request: Request<proto::ValidateTokenRequest>,
    ) -> Result<Response<proto::ValidateTokenResponse>, Status> {
        self.state.check_injection().await?;

        let req = request.into_inner();
        if req.token.is_empty() {
            return Err(Status::unauthenticated("empty token"));
        }

        Ok(Response::new(proto::ValidateTokenResponse {
            subject: "user:42".to_string(),
            token_type: "user_session".to_string(),
            expires_at: None,
            claims: Some(proto::validate_token_response::Claims::UserSession(
                proto::UserSessionClaims { user_slug: 42, role: "user".to_string() },
            )),
        }))
    }

    async fn create_vault_token(
        &self,
        _request: Request<proto::CreateVaultTokenRequest>,
    ) -> Result<Response<proto::CreateVaultTokenResponse>, Status> {
        self.state.check_injection().await?;

        Ok(Response::new(proto::CreateVaultTokenResponse { tokens: Some(Self::mock_token_pair()) }))
    }

    async fn refresh_token(
        &self,
        _request: Request<proto::RefreshTokenRequest>,
    ) -> Result<Response<proto::RefreshTokenResponse>, Status> {
        self.state.check_injection().await?;

        Ok(Response::new(proto::RefreshTokenResponse { tokens: Some(Self::mock_token_pair()) }))
    }

    async fn revoke_token(
        &self,
        _request: Request<proto::RevokeTokenRequest>,
    ) -> Result<Response<proto::RevokeTokenResponse>, Status> {
        self.state.check_injection().await?;
        Ok(Response::new(proto::RevokeTokenResponse {}))
    }

    async fn revoke_all_user_sessions(
        &self,
        _request: Request<proto::RevokeAllUserSessionsRequest>,
    ) -> Result<Response<proto::RevokeAllUserSessionsResponse>, Status> {
        self.state.check_injection().await?;

        Ok(Response::new(proto::RevokeAllUserSessionsResponse { revoked_count: 3 }))
    }

    async fn revoke_all_app_sessions(
        &self,
        _request: Request<proto::RevokeAllAppSessionsRequest>,
    ) -> Result<Response<proto::RevokeAllAppSessionsResponse>, Status> {
        self.state.check_injection().await?;

        Ok(Response::new(proto::RevokeAllAppSessionsResponse { revoked_count: 2 }))
    }

    async fn create_signing_key(
        &self,
        _request: Request<proto::CreateSigningKeyRequest>,
    ) -> Result<Response<proto::CreateSigningKeyResponse>, Status> {
        self.state.check_injection().await?;

        let now = Self::now_timestamp();
        let key = proto::PublicKeyInfo {
            kid: "mock-kid-001".to_string(),
            public_key: vec![0u8; 32],
            status: "active".to_string(),
            valid_from: Some(now),
            valid_until: None,
            created_at: Some(now),
        };

        Ok(Response::new(proto::CreateSigningKeyResponse { key: Some(key) }))
    }

    async fn rotate_signing_key(
        &self,
        _request: Request<proto::RotateSigningKeyRequest>,
    ) -> Result<Response<proto::RotateSigningKeyResponse>, Status> {
        self.state.check_injection().await?;

        let now = Self::now_timestamp();
        let new_key = proto::PublicKeyInfo {
            kid: "mock-kid-002".to_string(),
            public_key: vec![1u8; 32],
            status: "active".to_string(),
            valid_from: Some(now),
            valid_until: None,
            created_at: Some(now),
        };

        Ok(Response::new(proto::RotateSigningKeyResponse { new_key: Some(new_key) }))
    }

    async fn revoke_signing_key(
        &self,
        _request: Request<proto::RevokeSigningKeyRequest>,
    ) -> Result<Response<proto::RevokeSigningKeyResponse>, Status> {
        self.state.check_injection().await?;
        Ok(Response::new(proto::RevokeSigningKeyResponse {}))
    }

    async fn get_public_keys(
        &self,
        _request: Request<proto::GetPublicKeysRequest>,
    ) -> Result<Response<proto::GetPublicKeysResponse>, Status> {
        self.state.check_injection().await?;

        let now = Self::now_timestamp();
        let key = proto::PublicKeyInfo {
            kid: "mock-kid-001".to_string(),
            public_key: vec![0u8; 32],
            status: "active".to_string(),
            valid_from: Some(now),
            valid_until: None,
            created_at: Some(now),
        };

        Ok(Response::new(proto::GetPublicKeysResponse { keys: vec![key] }))
    }

    async fn authenticate_client_assertion(
        &self,
        _request: Request<proto::AuthenticateClientAssertionRequest>,
    ) -> Result<Response<proto::AuthenticateClientAssertionResponse>, Status> {
        self.state.check_injection().await?;

        Ok(Response::new(proto::AuthenticateClientAssertionResponse {
            tokens: Some(Self::mock_token_pair()),
        }))
    }
}
