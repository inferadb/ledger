//! Mock implementation of the app gRPC service.

use std::sync::Arc;

use inferadb_ledger_proto::proto;
use tonic::{Request, Response, Status};

use super::MockState;

pub(super) struct MockAppService {
    pub(super) state: Arc<MockState>,
}

impl MockAppService {
    pub(super) fn new(state: Arc<MockState>) -> Self {
        Self { state }
    }

    fn now_timestamp() -> prost_types::Timestamp {
        let now =
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
        prost_types::Timestamp { seconds: now.as_secs() as i64, nanos: now.subsec_nanos() as i32 }
    }

    fn mock_app_info(slug: u64, name: &str, description: &Option<String>) -> proto::AppInfo {
        let now = Self::now_timestamp();
        proto::AppInfo {
            slug: Some(proto::AppSlug { slug }),
            name: name.to_string(),
            description: description.clone(),
            enabled: true,
            credentials: Some(proto::AppCredentialsInfo {
                client_secret_enabled: false,
                mtls_ca_enabled: false,
                mtls_self_signed_enabled: false,
                client_assertion_enabled: false,
            }),
            created_at: Some(now),
            updated_at: Some(now),
        }
    }
}

#[tonic::async_trait]
impl proto::app_service_server::AppService for MockAppService {
    async fn create_app(
        &self,
        request: Request<proto::CreateAppRequest>,
    ) -> Result<Response<proto::CreateAppResponse>, Status> {
        self.state.check_injection().await?;

        let req = request.into_inner();
        let slug = self.state.next_vault.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let app = Self::mock_app_info(slug, &req.name, &req.description);

        Ok(Response::new(proto::CreateAppResponse { app: Some(app) }))
    }

    async fn get_app(
        &self,
        request: Request<proto::GetAppRequest>,
    ) -> Result<Response<proto::GetAppResponse>, Status> {
        self.state.check_injection().await?;

        let req = request.into_inner();
        let slug = req.app.map_or(0, |a| a.slug);
        let app = Self::mock_app_info(slug, "test-app", &None);

        Ok(Response::new(proto::GetAppResponse { app: Some(app) }))
    }

    async fn list_apps(
        &self,
        _request: Request<proto::ListAppsRequest>,
    ) -> Result<Response<proto::ListAppsResponse>, Status> {
        self.state.check_injection().await?;

        let app1 = Self::mock_app_info(100, "app-one", &None);
        let app2 = Self::mock_app_info(200, "app-two", &Some("second app".to_string()));

        Ok(Response::new(proto::ListAppsResponse { apps: vec![app1, app2] }))
    }

    async fn update_app(
        &self,
        request: Request<proto::UpdateAppRequest>,
    ) -> Result<Response<proto::UpdateAppResponse>, Status> {
        self.state.check_injection().await?;

        let req = request.into_inner();
        let slug = req.app.map_or(0, |a| a.slug);
        let name = req.name.unwrap_or_else(|| "test-app".to_string());
        let app = Self::mock_app_info(slug, &name, &req.description);

        Ok(Response::new(proto::UpdateAppResponse { app: Some(app) }))
    }

    async fn delete_app(
        &self,
        _request: Request<proto::DeleteAppRequest>,
    ) -> Result<Response<proto::DeleteAppResponse>, Status> {
        self.state.check_injection().await?;
        Ok(Response::new(proto::DeleteAppResponse {}))
    }

    async fn set_app_enabled(
        &self,
        request: Request<proto::SetAppEnabledRequest>,
    ) -> Result<Response<proto::SetAppEnabledResponse>, Status> {
        self.state.check_injection().await?;

        let req = request.into_inner();
        let slug = req.app.map_or(0, |a| a.slug);
        let mut app = Self::mock_app_info(slug, "test-app", &None);
        app.enabled = req.enabled;

        Ok(Response::new(proto::SetAppEnabledResponse { app: Some(app) }))
    }

    async fn set_app_credential_enabled(
        &self,
        request: Request<proto::SetAppCredentialEnabledRequest>,
    ) -> Result<Response<proto::SetAppCredentialEnabledResponse>, Status> {
        self.state.check_injection().await?;

        let req = request.into_inner();
        let slug = req.app.map_or(0, |a| a.slug);
        let app = Self::mock_app_info(slug, "test-app", &None);

        Ok(Response::new(proto::SetAppCredentialEnabledResponse { app: Some(app) }))
    }

    async fn get_app_client_secret(
        &self,
        _request: Request<proto::GetAppClientSecretRequest>,
    ) -> Result<Response<proto::GetAppClientSecretResponse>, Status> {
        self.state.check_injection().await?;

        Ok(Response::new(proto::GetAppClientSecretResponse { enabled: true, has_secret: true }))
    }

    async fn rotate_app_client_secret(
        &self,
        _request: Request<proto::RotateAppClientSecretRequest>,
    ) -> Result<Response<proto::RotateAppClientSecretResponse>, Status> {
        self.state.check_injection().await?;

        Ok(Response::new(proto::RotateAppClientSecretResponse {
            secret: "mock-rotated-secret-base64".to_string(),
        }))
    }

    async fn list_app_client_assertions(
        &self,
        _request: Request<proto::ListAppClientAssertionsRequest>,
    ) -> Result<Response<proto::ListAppClientAssertionsResponse>, Status> {
        self.state.check_injection().await?;

        let now = Self::now_timestamp();
        let assertion = proto::AppClientAssertionInfo {
            id: Some(proto::ClientAssertionId { id: 1 }),
            name: "test-assertion".to_string(),
            enabled: true,
            expires_at: Some(now),
            created_at: Some(now),
        };

        Ok(Response::new(proto::ListAppClientAssertionsResponse { assertions: vec![assertion] }))
    }

    async fn create_app_client_assertion(
        &self,
        request: Request<proto::CreateAppClientAssertionRequest>,
    ) -> Result<Response<proto::CreateAppClientAssertionResponse>, Status> {
        self.state.check_injection().await?;

        let req = request.into_inner();
        let now = Self::now_timestamp();
        let assertion = proto::AppClientAssertionInfo {
            id: Some(proto::ClientAssertionId { id: 42 }),
            name: req.name,
            enabled: true,
            expires_at: req.expires_at,
            created_at: Some(now),
        };

        Ok(Response::new(proto::CreateAppClientAssertionResponse {
            assertion: Some(assertion),
            private_key_pem: "-----BEGIN PRIVATE KEY-----\nmock-key\n-----END PRIVATE KEY-----\n"
                .to_string(),
        }))
    }

    async fn delete_app_client_assertion(
        &self,
        _request: Request<proto::DeleteAppClientAssertionRequest>,
    ) -> Result<Response<proto::DeleteAppClientAssertionResponse>, Status> {
        self.state.check_injection().await?;
        Ok(Response::new(proto::DeleteAppClientAssertionResponse {}))
    }

    async fn set_app_client_assertion_enabled(
        &self,
        _request: Request<proto::SetAppClientAssertionEnabledRequest>,
    ) -> Result<Response<proto::SetAppClientAssertionEnabledResponse>, Status> {
        self.state.check_injection().await?;
        Ok(Response::new(proto::SetAppClientAssertionEnabledResponse {}))
    }

    async fn list_app_vaults(
        &self,
        _request: Request<proto::ListAppVaultsRequest>,
    ) -> Result<Response<proto::ListAppVaultsResponse>, Status> {
        self.state.check_injection().await?;

        let now = Self::now_timestamp();
        let vault = proto::AppVaultConnectionInfo {
            vault: Some(proto::VaultSlug { slug: 10 }),
            allowed_scopes: vec!["read".to_string(), "write".to_string()],
            created_at: Some(now),
            updated_at: None,
        };

        Ok(Response::new(proto::ListAppVaultsResponse { vaults: vec![vault] }))
    }

    async fn add_app_vault(
        &self,
        request: Request<proto::AddAppVaultRequest>,
    ) -> Result<Response<proto::AddAppVaultResponse>, Status> {
        self.state.check_injection().await?;

        let req = request.into_inner();
        let now = Self::now_timestamp();
        let vault = proto::AppVaultConnectionInfo {
            vault: req.vault,
            allowed_scopes: req.allowed_scopes,
            created_at: Some(now),
            updated_at: None,
        };

        Ok(Response::new(proto::AddAppVaultResponse { vault: Some(vault) }))
    }

    async fn update_app_vault(
        &self,
        request: Request<proto::UpdateAppVaultRequest>,
    ) -> Result<Response<proto::UpdateAppVaultResponse>, Status> {
        self.state.check_injection().await?;

        let req = request.into_inner();
        let now = Self::now_timestamp();
        let vault = proto::AppVaultConnectionInfo {
            vault: req.vault,
            allowed_scopes: req.allowed_scopes,
            created_at: Some(now),
            updated_at: None,
        };

        Ok(Response::new(proto::UpdateAppVaultResponse { vault: Some(vault) }))
    }

    async fn remove_app_vault(
        &self,
        _request: Request<proto::RemoveAppVaultRequest>,
    ) -> Result<Response<proto::RemoveAppVaultResponse>, Status> {
        self.state.check_injection().await?;
        Ok(Response::new(proto::RemoveAppVaultResponse {}))
    }
}
