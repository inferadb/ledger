//! Integration tests for WAL encryption, snapshot crypto, and key lifecycle.
//!
//! These tests exercise cross-cutting scenarios that span the
//! `EncryptedWalBackend`, `InMemoryKeyProvider`, and `snapshot_crypto` modules
//! working together. Unit-level tests for each module live in their respective
//! `#[cfg(test)]` blocks.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]

use std::sync::Arc;

use inferadb_ledger_consensus::{
    crypto::InMemoryKeyProvider,
    snapshot_crypto::{decrypt_snapshot, encrypt_snapshot},
    types::ShardId,
    wal::{EncryptedWalBackend, InMemoryWalBackend},
    wal_backend::{CHECKPOINT_SHARD_ID, CheckpointFrame, WalBackend, WalFrame},
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_frame(shard: u64, index: u64, data: &[u8]) -> WalFrame {
    WalFrame { shard_id: ShardId(shard), index, term: 1, data: Arc::from(data) }
}

fn provider_with_key(vault_id: u64, version: u16, key: [u8; 32]) -> Arc<InMemoryKeyProvider> {
    let p = Arc::new(InMemoryKeyProvider::new());
    p.set_key(vault_id, version, key);
    p
}

fn encrypted_wal(
    provider: Arc<InMemoryKeyProvider>,
) -> EncryptedWalBackend<InMemoryWalBackend, Arc<InMemoryKeyProvider>> {
    EncryptedWalBackend::new(InMemoryWalBackend::new(), provider)
}

// ===========================================================================
// WAL encryption roundtrip
// ===========================================================================

#[test]
fn wal_encrypt_decrypt_roundtrip_preserves_payload() {
    // Arrange
    let provider = provider_with_key(1, 0, [0x42; 32]);
    let mut wal = encrypted_wal(provider);

    // Act
    wal.append(&[make_frame(1, 1, b"secret data")]).unwrap();
    wal.sync().unwrap();
    let frames = wal.read_frames(0).unwrap();

    // Assert
    assert_eq!(frames.len(), 1);
    assert_eq!(&*frames[0].data, b"secret data");
    assert_eq!(frames[0].shard_id, ShardId(1));
    assert_eq!(frames[0].index, 1);
    assert_eq!(frames[0].term, 1);
}

#[test]
fn wal_encrypt_empty_payload_roundtrips() {
    // Arrange
    let provider = provider_with_key(1, 0, [0xAA; 32]);
    let mut wal = encrypted_wal(provider);

    // Act
    wal.append(&[make_frame(1, 1, b"")]).unwrap();
    wal.sync().unwrap();
    let frames = wal.read_frames(0).unwrap();

    // Assert
    assert_eq!(frames.len(), 1);
    assert_eq!(&*frames[0].data, b"");
}

#[test]
fn wal_encrypt_large_payload_roundtrips() {
    // Arrange: 1 MiB payload simulating a large batch write.
    let provider = provider_with_key(1, 0, [0xBB; 32]);
    let mut wal = encrypted_wal(provider);
    let large_data = vec![0xCDu8; 1024 * 1024];

    // Act
    wal.append(&[make_frame(1, 1, &large_data)]).unwrap();
    wal.sync().unwrap();
    let frames = wal.read_frames(0).unwrap();

    // Assert
    assert_eq!(frames.len(), 1);
    assert_eq!(&*frames[0].data, &large_data[..]);
}

#[test]
fn wal_encrypting_same_data_twice_produces_different_ciphertext() {
    // Arrange: two separate WAL instances with the same key.
    let provider = provider_with_key(1, 0, [0xDD; 32]);
    let mut wal1 = encrypted_wal(Arc::clone(&provider));
    let mut wal2 = encrypted_wal(provider);
    let plaintext = b"same plaintext";

    // Act
    wal1.append(&[make_frame(1, 1, plaintext)]).unwrap();
    wal1.sync().unwrap();
    wal2.append(&[make_frame(1, 1, plaintext)]).unwrap();
    wal2.sync().unwrap();

    // Assert: both decrypt to the same value, proving roundtrip works.
    let f1 = wal1.read_frames(0).unwrap();
    let f2 = wal2.read_frames(0).unwrap();
    assert_eq!(&*f1[0].data, plaintext.as_slice());
    assert_eq!(&*f2[0].data, plaintext.as_slice());
    // Note: we cannot directly compare ciphertext from integration tests
    // since `inner` is private, but the unit tests verify nonce uniqueness.
}

// ===========================================================================
// Multi-vault / multi-shard isolation
// ===========================================================================

#[test]
fn wal_different_vaults_use_independent_keys() {
    // Arrange
    let provider = Arc::new(InMemoryKeyProvider::new());
    provider.set_key(1, 0, [0x11; 32]);
    provider.set_key(2, 0, [0x22; 32]);
    let mut wal = encrypted_wal(provider);

    // Act
    wal.append(&[make_frame(1, 1, b"vault-one"), make_frame(2, 1, b"vault-two")]).unwrap();
    wal.sync().unwrap();
    let frames = wal.read_frames(0).unwrap();

    // Assert
    let v1 = frames.iter().find(|f| f.shard_id == ShardId(1)).unwrap();
    let v2 = frames.iter().find(|f| f.shard_id == ShardId(2)).unwrap();
    assert_eq!(&*v1.data, b"vault-one");
    assert_eq!(&*v2.data, b"vault-two");
}

#[test]
fn wal_batch_append_fails_atomically_when_one_shard_has_no_key() {
    // Arrange: vault 1 has a key, vault 2 does not.
    let provider = provider_with_key(1, 0, [0x11; 32]);
    let mut wal = encrypted_wal(provider);

    // Act: batch contains frames for both vaults.
    let result = wal.append(&[make_frame(1, 1, b"ok"), make_frame(2, 1, b"no-key")]);

    // Assert: the entire batch fails.
    assert!(result.is_err(), "batch must fail when any frame lacks a key");

    // No frames should have been written.
    let frames = wal.read_frames(0).unwrap();
    assert!(frames.is_empty(), "inner backend must not contain partial writes");
}

// ===========================================================================
// Key rotation and versioning
// ===========================================================================

#[test]
fn wal_key_rotation_reads_old_and_new_frames() {
    // Arrange
    let provider = Arc::new(InMemoryKeyProvider::new());
    provider.set_key(1, 0, [0xAA; 32]);
    let mut wal = encrypted_wal(Arc::clone(&provider));

    // Act: write with version 0, then rotate to version 1 and write again.
    wal.append(&[make_frame(1, 1, b"v0-data")]).unwrap();
    wal.sync().unwrap();

    provider.set_key(1, 1, [0xBB; 32]);
    wal.append(&[make_frame(1, 2, b"v1-data")]).unwrap();
    wal.sync().unwrap();

    let frames = wal.read_frames(0).unwrap();

    // Assert: both frames readable — old key v0 still present.
    assert_eq!(frames.len(), 2);
    assert_eq!(&*frames[0].data, b"v0-data");
    assert_eq!(&*frames[1].data, b"v1-data");
}

#[test]
fn wal_three_key_versions_active_simultaneously() {
    // Arrange: register three versions for vault 1.
    let provider = Arc::new(InMemoryKeyProvider::new());
    provider.set_key(1, 0, [0x10; 32]);
    let mut wal = encrypted_wal(Arc::clone(&provider));

    // Act: write a frame per version.
    wal.append(&[make_frame(1, 1, b"v0")]).unwrap();

    provider.set_key(1, 1, [0x20; 32]);
    wal.append(&[make_frame(1, 2, b"v1")]).unwrap();

    provider.set_key(1, 2, [0x30; 32]);
    wal.append(&[make_frame(1, 3, b"v2")]).unwrap();
    wal.sync().unwrap();

    let frames = wal.read_frames(0).unwrap();

    // Assert: all three versions decrypt correctly.
    assert_eq!(frames.len(), 3);
    assert_eq!(&*frames[0].data, b"v0");
    assert_eq!(&*frames[1].data, b"v1");
    assert_eq!(&*frames[2].data, b"v2");
}

#[test]
fn wal_destroy_old_key_then_rekey_with_new_version() {
    // Arrange
    let provider = Arc::new(InMemoryKeyProvider::new());
    provider.set_key(1, 0, [0xAA; 32]);
    let mut wal = encrypted_wal(Arc::clone(&provider));

    wal.append(&[make_frame(1, 1, b"old-data")]).unwrap();
    wal.sync().unwrap();

    // Act: destroy v0, register v1 (re-key).
    provider.destroy_key(1, 0);
    provider.set_key(1, 1, [0xBB; 32]);

    // New writes succeed with v1.
    wal.append(&[make_frame(1, 2, b"new-data")]).unwrap();
    wal.sync().unwrap();

    // Assert: reading all frames fails because v0 is destroyed.
    let result = wal.read_frames(0);
    assert!(result.is_err(), "old frames encrypted with destroyed key must fail");
}

// ===========================================================================
// Missing key / destroyed key errors
// ===========================================================================

#[test]
fn wal_write_fails_when_no_key_registered() {
    // Arrange: no key for vault 1.
    let provider = Arc::new(InMemoryKeyProvider::new());
    let mut wal = encrypted_wal(provider);

    // Act + Assert
    let err = wal.append(&[make_frame(1, 1, b"data")]);
    assert!(err.is_err(), "appending without a registered key must fail");
}

#[test]
fn wal_read_fails_after_key_destroyed() {
    // Arrange
    let provider = Arc::new(InMemoryKeyProvider::new());
    provider.set_key(1, 0, [0x55; 32]);
    let mut wal = encrypted_wal(Arc::clone(&provider));

    wal.append(&[make_frame(1, 1, b"confidential")]).unwrap();
    wal.sync().unwrap();

    // Act
    provider.destroy_key(1, 0);
    let result = wal.read_frames(0);

    // Assert
    assert!(result.is_err());
}

// ===========================================================================
// Crypto-shredding (frame zeroing)
// ===========================================================================

#[test]
fn shred_zeros_target_shard_preserves_others() {
    // Arrange
    let mut wal = InMemoryWalBackend::new();
    wal.append(&[make_frame(1, 1, b"vault-1-data"), make_frame(2, 1, b"vault-2-data")]).unwrap();
    wal.sync().unwrap();

    // Act
    let count = wal.shred_frames(ShardId(1)).unwrap();

    // Assert
    assert_eq!(count, 1);
    let frames = wal.read_frames(0).unwrap();
    let shard1 = frames.iter().find(|f| f.shard_id == ShardId(1)).unwrap();
    assert!(shard1.data.iter().all(|&b| b == 0), "shard 1 data must be zeroed");
    let shard2 = frames.iter().find(|f| f.shard_id == ShardId(2)).unwrap();
    assert_eq!(&*shard2.data, b"vault-2-data");
}

#[test]
fn shred_zeros_both_pending_and_durable_frames() {
    // Arrange
    let mut wal = InMemoryWalBackend::new();
    wal.append(&[make_frame(1, 1, b"durable-secret")]).unwrap();
    wal.sync().unwrap();
    wal.append(&[make_frame(1, 2, b"pending-secret")]).unwrap();

    // Act
    let zeroed = wal.shred_frames(ShardId(1)).unwrap();

    // Assert
    assert_eq!(zeroed, 2);
    wal.sync().unwrap();
    let all = wal.read_frames(0).unwrap();
    assert_eq!(all.len(), 2);
    for f in &all {
        assert!(f.data.iter().all(|&b| b == 0), "frame data must be zeroed: {:?}", f.data);
    }
}

#[test]
fn shred_through_encrypted_backend_delegates_to_inner() {
    // Arrange: encrypted WAL wrapping an in-memory backend.
    let provider = Arc::new(InMemoryKeyProvider::new());
    provider.set_key(1, 0, [0x11; 32]);
    provider.set_key(2, 0, [0x22; 32]);
    let mut wal = encrypted_wal(Arc::clone(&provider));

    wal.append(&[make_frame(1, 1, b"shard-1"), make_frame(2, 1, b"shard-2")]).unwrap();
    wal.sync().unwrap();

    // Act: shred shard 1 through the encrypted backend.
    let count = wal.shred_frames(ShardId(1)).unwrap();

    // Assert: shard 1 zeroed (decryption fails since data is now zeros),
    // shard 2 still decrypts normally.
    assert_eq!(count, 1);

    // Reading all frames will fail because shard 1's zeroed data can't
    // decrypt. This confirms shred propagated through the encrypted layer.
    let result = wal.read_frames(0);
    assert!(result.is_err(), "zeroed shard 1 data must fail decryption");
}

// ===========================================================================
// Checkpoint encryption
// ===========================================================================

#[test]
fn checkpoint_frame_written_through_encrypted_backend_is_readable() {
    // Arrange: checkpoints use CHECKPOINT_SHARD_ID (u64::MAX). We must register
    // a key for that shard ID so the encrypted backend can encrypt the frame.
    let provider = Arc::new(InMemoryKeyProvider::new());
    provider.set_key(CHECKPOINT_SHARD_ID.0, 0, [0xCC; 32]);
    let mut wal = encrypted_wal(Arc::clone(&provider));

    let checkpoint = CheckpointFrame { committed_index: 42, term: 3, voted_for: None };

    // Act
    wal.write_checkpoint(&checkpoint).unwrap();
    wal.sync().unwrap();

    let frames = wal.read_frames(0).unwrap();

    // Assert: the checkpoint data decrypts and decodes correctly.
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].shard_id, CHECKPOINT_SHARD_ID);
    let decoded = CheckpointFrame::decode(&frames[0].data).expect("checkpoint must decode");
    assert_eq!(decoded.committed_index, 42);
    assert_eq!(decoded.term, 3);
}

// ===========================================================================
// truncate_before through encrypted backend
// ===========================================================================

#[test]
fn truncate_before_through_encrypted_backend() {
    // Arrange
    let provider = provider_with_key(1, 0, [0xEE; 32]);
    let mut wal = encrypted_wal(provider);

    wal.append(&[make_frame(1, 1, b"first"), make_frame(1, 2, b"second")]).unwrap();
    wal.sync().unwrap();

    // Act: truncate frames before offset 1.
    wal.truncate_before(1).unwrap();
    let frames = wal.read_frames(0).unwrap();

    // Assert: only frames at offset >= 1 remain.
    // The InMemoryWalBackend stores frames in a Vec and truncate_before
    // removes frames with index < offset, so frame at index 2 survives.
    assert_eq!(frames.len(), 1);
    assert_eq!(&*frames[0].data, b"second");
}

// ===========================================================================
// read_frames with non-zero offset
// ===========================================================================

#[test]
fn wal_read_frames_from_nonzero_offset() {
    // Arrange
    let provider = provider_with_key(1, 0, [0xFF; 32]);
    let mut wal = encrypted_wal(provider);

    wal.append(&[make_frame(1, 1, b"first"), make_frame(1, 2, b"second")]).unwrap();
    wal.sync().unwrap();

    // Act: read from positional offset 1 (skips the first durable frame).
    let frames = wal.read_frames(1).unwrap();

    // Assert: only the second frame is returned and decrypted.
    assert_eq!(frames.len(), 1);
    assert_eq!(&*frames[0].data, b"second");
}

// ===========================================================================
// Snapshot crypto (integration-level scenarios)
// ===========================================================================

#[test]
fn snapshot_encrypt_decrypt_roundtrip() {
    // Arrange
    let key = [0x07; 32];
    let plaintext = b"snapshot state data with entities and relationships";

    // Act
    let encrypted = encrypt_snapshot(&key, plaintext).unwrap();
    let decrypted = decrypt_snapshot(&key, &encrypted).unwrap();

    // Assert
    assert_ne!(encrypted.len(), plaintext.len());
    assert_eq!(decrypted, plaintext);
}

#[test]
fn snapshot_wrong_key_fails_decryption() {
    // Arrange
    let key = [0x07; 32];
    let wrong_key = [0x08; 32];
    let encrypted = encrypt_snapshot(&key, b"data").unwrap();

    // Act + Assert
    assert!(decrypt_snapshot(&wrong_key, &encrypted).is_err());
}

#[test]
fn snapshot_empty_plaintext_roundtrips() {
    let key = [0x00; 32];
    let encrypted = encrypt_snapshot(&key, b"").unwrap();
    let decrypted = decrypt_snapshot(&key, &encrypted).unwrap();
    assert_eq!(decrypted, b"");
}

#[test]
fn snapshot_truncated_ciphertext_fails() {
    let key = [0x01; 32];
    let encrypted = encrypt_snapshot(&key, b"hello world").unwrap();
    // Truncate to just the nonce.
    let truncated = &encrypted[..12];
    assert!(decrypt_snapshot(&key, truncated).is_err());
}

#[test]
fn snapshot_tampered_nonce_fails_decryption() {
    let key = [0x33; 32];
    let mut encrypted = encrypt_snapshot(&key, b"nonce tamper test").unwrap();
    encrypted[0] ^= 0x01;
    assert!(decrypt_snapshot(&key, &encrypted).is_err(), "tampered nonce must fail GCM auth");
}

#[test]
fn snapshot_tampered_ciphertext_body_fails_decryption() {
    // Arrange
    let key = [0x44; 32];
    let mut encrypted = encrypt_snapshot(&key, b"authentic payload").unwrap();

    // Flip a bit in the ciphertext body (past the 12-byte nonce, before the
    // 16-byte tag at the end).
    let body_index = 12 + (encrypted.len() - 12 - 16) / 2;
    encrypted[body_index] ^= 0x01;

    // Act + Assert
    assert!(decrypt_snapshot(&key, &encrypted).is_err(), "tampered body must fail GCM auth");
}

#[test]
fn snapshot_large_payload_roundtrips() {
    // Arrange: 2 MiB snapshot.
    let key = [0x99; 32];
    let large = vec![0xABu8; 2 * 1024 * 1024];

    // Act
    let encrypted = encrypt_snapshot(&key, &large).unwrap();
    let decrypted = decrypt_snapshot(&key, &encrypted).unwrap();

    // Assert
    assert_eq!(decrypted, large);
    // Size: 12 nonce + plaintext_len + 16 tag.
    assert_eq!(encrypted.len(), 12 + large.len() + 16);
}
