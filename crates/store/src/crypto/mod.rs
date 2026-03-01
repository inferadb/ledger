//! Envelope encryption for data at rest.
//!
//! Each artifact (B+ tree page, snapshot chunk, Raft log entry) gets a
//! unique random Data Encryption Key (DEK). The DEK encrypts the data
//! with AES-256-GCM. The DEK is wrapped by the Region Master Key (RMK)
//! using AES-KWP (RFC 5649) and stored alongside the artifact.
//!
//! ```text
//! Write path:
//!   generate DEK → encrypt body (AES-256-GCM, AAD=header) → wrap DEK (AES-KWP)
//!     → store ciphertext + metadata → zeroize DEK (or cache)
//!
//! Read path:
//!   PageCache hit? → return plaintext (zero crypto)
//!   DekCache hit?  → decrypt only (skip unwrap)
//!   RmkCache hit?  → unwrap DEK + decrypt
//!   miss           → load RMK, unwrap, decrypt
//! ```

mod backend;
mod cache;
mod key_manager;
mod operations;
mod sidecar;
mod types;

pub use backend::EncryptedBackend;
pub use cache::{DekCache, RmkCache};
pub use key_manager::{
    EnvKeyManager, FileKeyManager, RegionKeyManager, SecretsClient, SecretsManagerKeyManager,
    VersionSidecarEntry, required_regions, rmk_versions_for_health, validate_rmk_provisioning,
};
pub use operations::{decrypt_page_body, encrypt_page_body, generate_dek, unwrap_dek, wrap_dek};
pub use sidecar::CryptoSidecar;
pub use types::{
    CRYPTO_METADATA_SIZE, CryptoMetadata, DataEncryptionKey, RegionMasterKey, WrappedDek,
};
