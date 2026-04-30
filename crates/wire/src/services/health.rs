//! Wire types for `HealthService`.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::services::shared::{HealthStatus, OrganizationSlug, VaultSlug};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct HealthCheckRequest {
    pub organization: Option<OrganizationSlug>,
    pub vault: Option<VaultSlug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct HealthCheckResponse {
    pub status: HealthStatus,
    pub message: String,
    pub details: BTreeMap<String, String>,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    #[test]
    fn health_check_response_roundtrips() {
        let mut details = BTreeMap::new();
        details.insert("raft_lag".to_owned(), "12".to_owned());
        details.insert("disk_writable".to_owned(), "true".to_owned());

        let resp = HealthCheckResponse {
            status: HealthStatus::Degraded,
            message: "raft lag elevated".to_owned(),
            details,
        };
        let bytes = postcard::to_allocvec(&resp).unwrap();
        let decoded: HealthCheckResponse = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(resp, decoded);
    }
}
