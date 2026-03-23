//! Create Signing Key saga state machine and step definitions.

use std::time::Duration;

use chrono::{DateTime, Utc};
use inferadb_ledger_types::SigningKeyScope;
use serde::{Deserialize, Serialize};

use super::{MAX_BACKOFF, MAX_RETRIES, SagaId};

/// Input parameters for Create Signing Key saga.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateSigningKeyInput {
    /// Scope of the signing key to create.
    pub scope: SigningKeyScope,
}

/// State machine for the Create Signing Key saga.
///
/// Coordinates signing key creation on the leader:
/// 1. Generate Ed25519 keypair and encrypt private key with RMK
/// 2. Propose `CreateSigningKey` through Raft
///
/// Idempotent: if an active key already exists for the scope, the saga
/// completes immediately without creating a new key.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum CreateSigningKeySagaState {
    /// Initial state: about to generate keypair and encrypt.
    Pending,
    /// Step 1 complete: keypair generated, encrypted, ready to propose through Raft.
    /// Stores the encrypted material for retry idempotency.
    KeyGenerated {
        /// UUID-format key identifier.
        kid: String,
        /// 32-byte Ed25519 public key.
        public_key_bytes: Vec<u8>,
        /// `SigningKeyEnvelope` serialized bytes.
        encrypted_private_key: Vec<u8>,
        /// RMK version used to wrap the DEK.
        rmk_version: u32,
    },
    /// Step 2 complete: signing key committed through Raft.
    Completed {
        /// The created key's identifier.
        kid: String,
    },
    /// Saga failed after exhausting retries.
    Failed {
        /// The step that failed (0-indexed).
        step: u8,
        /// Error description.
        error: String,
    },
    /// Saga exceeded timeout and was auto-cancelled.
    TimedOut,
}

/// Record for the Create Signing Key saga, tracking state and retry progress.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateSigningKeySaga {
    /// Unique saga identifier.
    pub id: SagaId,
    /// Current state.
    pub state: CreateSigningKeySagaState,
    /// Input parameters.
    pub input: CreateSigningKeyInput,
    /// When the saga was created.
    pub created_at: DateTime<Utc>,
    /// When the saga was last updated.
    pub updated_at: DateTime<Utc>,
    /// Number of retry attempts.
    pub retries: u8,
    /// Next retry time (for exponential backoff).
    pub next_retry_at: Option<DateTime<Utc>>,
}

impl CreateSigningKeySaga {
    /// Creates a new saga in Pending state.
    pub fn new(id: SagaId, input: CreateSigningKeyInput) -> Self {
        let now = Utc::now();
        Self {
            id,
            state: CreateSigningKeySagaState::Pending,
            input,
            created_at: now,
            updated_at: now,
            retries: 0,
            next_retry_at: None,
        }
    }

    /// Checks if the saga is complete (success or permanently failed).
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.state,
            CreateSigningKeySagaState::Completed { .. }
                | CreateSigningKeySagaState::Failed { .. }
                | CreateSigningKeySagaState::TimedOut
        )
    }

    /// Checks if the saga is ready for retry.
    pub fn is_ready_for_retry(&self) -> bool {
        if self.is_terminal() {
            return false;
        }
        match self.next_retry_at {
            Some(retry_at) => Utc::now() >= retry_at,
            None => true,
        }
    }

    /// Checks if the saga has exceeded the given timeout.
    pub fn is_timed_out(&self, timeout: Duration) -> bool {
        let elapsed = Utc::now() - self.created_at;
        elapsed > chrono::Duration::from_std(timeout).unwrap_or(chrono::Duration::MAX)
    }

    /// Calculates next backoff duration using exponential backoff.
    pub fn next_backoff(&self) -> Duration {
        let base = Duration::from_secs(1);
        let backoff = base * 2u32.saturating_pow(self.retries as u32);
        std::cmp::min(backoff, MAX_BACKOFF)
    }

    /// Transitions to a new state.
    pub fn transition(&mut self, new_state: CreateSigningKeySagaState) {
        self.state = new_state;
        self.updated_at = Utc::now();
        self.next_retry_at = None;
    }

    /// Marks as failed with error.
    ///
    /// After `MAX_RETRIES` failures, transitions to terminal Failed state.
    pub fn fail(&mut self, step: u8, error: String) {
        self.retries = self.retries.saturating_add(1);
        if self.retries >= MAX_RETRIES {
            self.state = CreateSigningKeySagaState::Failed { step, error };
        } else {
            let backoff = self.next_backoff();
            self.next_retry_at =
                Some(Utc::now() + chrono::Duration::from_std(backoff).unwrap_or_default());
        }
        self.updated_at = Utc::now();
    }

    /// Returns the step number for the current state.
    pub fn current_step(&self) -> u8 {
        match &self.state {
            CreateSigningKeySagaState::Pending => 0,
            CreateSigningKeySagaState::KeyGenerated { .. } => 1,
            CreateSigningKeySagaState::Completed { .. }
            | CreateSigningKeySagaState::Failed { .. }
            | CreateSigningKeySagaState::TimedOut => 0,
        }
    }
}
