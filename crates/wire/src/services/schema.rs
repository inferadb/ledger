//! Wire types for `SchemaService`.

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::services::shared::{OrganizationSlug, UserSlug, VaultSlug};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeploySchemaRequest {
    pub organization: Option<OrganizationSlug>,
    pub vault: Option<VaultSlug>,
    pub definition: Bytes,
    pub version: Option<u32>,
    pub description: Option<String>,
    pub caller: Option<UserSlug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DeploySchemaResponse {
    pub version: u32,
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ListSchemaVersionsRequest {
    pub organization: Option<OrganizationSlug>,
    pub vault: Option<VaultSlug>,
    pub caller: Option<UserSlug>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SchemaVersionEntry {
    pub version: u32,
    pub has_definition: bool,
    pub is_active: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ListSchemaVersionsResponse {
    pub versions: Vec<SchemaVersionEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GetSchemaRequest {
    pub organization: Option<OrganizationSlug>,
    pub vault: Option<VaultSlug>,
    pub version: u32,
    pub caller: Option<UserSlug>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GetSchemaResponse {
    pub version: u32,
    pub definition: Bytes,
    pub description: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ActivateSchemaRequest {
    pub organization: Option<OrganizationSlug>,
    pub vault: Option<VaultSlug>,
    pub version: u32,
    pub caller: Option<UserSlug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ActivateSchemaResponse {
    pub version: u32,
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RollbackSchemaRequest {
    pub organization: Option<OrganizationSlug>,
    pub vault: Option<VaultSlug>,
    pub caller: Option<UserSlug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RollbackSchemaResponse {
    pub version: u32,
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GetActiveSchemaRequest {
    pub organization: Option<OrganizationSlug>,
    pub vault: Option<VaultSlug>,
    pub caller: Option<UserSlug>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GetActiveSchemaResponse {
    pub version: u32,
    pub definition: Bytes,
    pub description: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DiffSchemasRequest {
    pub organization: Option<OrganizationSlug>,
    pub vault: Option<VaultSlug>,
    pub from_version: u32,
    pub to_version: u32,
    pub caller: Option<UserSlug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DiffFieldChange {
    pub field: String,
    pub change_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DiffSchemasResponse {
    pub from_version: u32,
    pub to_version: u32,
    pub changes: Vec<DiffFieldChange>,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    #[test]
    fn deploy_schema_request_roundtrips_via_postcard() {
        let req = DeploySchemaRequest {
            organization: Some(OrganizationSlug::new(1)),
            vault: Some(VaultSlug::new(2)),
            definition: Bytes::from_static(br#"{"type":"object"}"#),
            version: Some(7),
            description: Some("v7 release".to_owned()),
            caller: Some(UserSlug::new(99)),
        };
        let bytes = postcard::to_allocvec(&req).unwrap();
        let decoded: DeploySchemaRequest = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(req, decoded);
    }
}
