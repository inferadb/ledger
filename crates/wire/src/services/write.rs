//! Wire types for `WriteService`.

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::services::shared::{
    BlockHeader, ClientIdMessage, MerkleProof, Operation, OrganizationSlug, TxIdMessage, UserSlug,
    VaultSlug,
};

// ---------------------------------------------------------------------------
// Write request / response
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WriteRequest {
    pub organization: Option<OrganizationSlug>,
    pub vault: Option<VaultSlug>,
    pub client_id: Option<ClientIdMessage>,
    /// 16-byte UUID for idempotent retries.
    pub idempotency_key: Bytes,
    pub operations: Vec<Operation>,
    pub include_tx_proof: bool,
    pub caller: Option<UserSlug>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WriteResponse {
    pub result: Option<WriteResponseResult>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum WriteResponseResult {
    Success(WriteSuccess),
    Error(WriteError),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WriteSuccess {
    pub tx_id: Option<TxIdMessage>,
    pub block_height: u64,
    pub block_header: Option<BlockHeader>,
    pub tx_proof: Option<MerkleProof>,
    pub assigned_sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WriteError {
    pub code: WriteErrorCode,
    pub key: String,
    pub current_version: Option<u64>,
    pub current_value: Option<Bytes>,
    pub message: String,
    pub committed_tx_id: Option<TxIdMessage>,
    pub committed_block_height: Option<u64>,
    pub assigned_sequence: Option<u64>,
}

#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum WriteErrorCode {
    Unspecified = 0,
    KeyExists = 1,
    KeyNotFound = 2,
    VersionMismatch = 3,
    ValueMismatch = 4,
    AlreadyCommitted = 5,
    IdempotencyKeyReused = 6,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;
    use crate::services::shared::{OperationKind, SetEntity};

    #[test]
    fn write_request_roundtrips_via_postcard() {
        let req = WriteRequest {
            organization: Some(OrganizationSlug::new(1)),
            vault: Some(VaultSlug::new(2)),
            client_id: Some(ClientIdMessage { id: "client-x".to_owned() }),
            idempotency_key: Bytes::from_static(&[0xAA; 16]),
            operations: vec![Operation {
                op: Some(OperationKind::SetEntity(SetEntity {
                    key: "user:42".to_owned(),
                    value: Bytes::from_static(b"payload"),
                    expires_at: None,
                    condition: None,
                })),
            }],
            include_tx_proof: true,
            caller: Some(UserSlug::new(99)),
        };
        let bytes = postcard::to_allocvec(&req).unwrap();
        let decoded: WriteRequest = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(req, decoded);
    }

    #[test]
    fn write_error_roundtrips() {
        let err = WriteError {
            code: WriteErrorCode::VersionMismatch,
            key: "user:42".to_owned(),
            current_version: Some(7),
            current_value: Some(Bytes::from_static(b"old")),
            message: "version mismatch".to_owned(),
            committed_tx_id: None,
            committed_block_height: None,
            assigned_sequence: None,
        };
        let bytes = postcard::to_allocvec(&err).unwrap();
        let decoded: WriteError = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(err, decoded);
    }
}
