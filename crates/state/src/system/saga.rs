//! Cross-namespace saga support.
//!
//! Sagas coordinate operations spanning multiple namespaces using eventual
//! consistency. Each saga step is idempotent for crash recovery.
//!
//! ## Saga Patterns
//!
//! - **CreateOrg**: Creates user in `_system`, then namespace with membership
//! - **DeleteUser**: Marks user as deleting, removes memberships, then deletes user
//!
//! ## Storage
//!
//! Sagas are stored in `_system` namespace under `saga:{saga_id}` keys.
//! The leader polls for incomplete sagas every 30 seconds.

use std::time::Duration;

use chrono::{DateTime, Utc};
use inferadb_ledger_types::{NamespaceId, UserId};
use serde::{Deserialize, Serialize};

/// Unique identifier for a saga.
pub type SagaId = String;

/// Maximum number of retry attempts before marking a saga as failed.
pub const MAX_RETRIES: u8 = 10;

/// Interval between saga poll cycles (30 seconds).
pub const SAGA_POLL_INTERVAL: Duration = Duration::from_secs(30);

/// Maximum backoff duration for retries (5 minutes).
pub const MAX_BACKOFF: Duration = Duration::from_secs(5 * 60);

// =============================================================================
// Create Organization Saga
// =============================================================================

/// State machine for the Create Organization saga.
///
/// Creates a new user (if needed) and a new organization namespace.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum CreateOrgSagaState {
    /// Initial state: about to create user.
    Pending,
    /// User created in `_system`, waiting for namespace creation.
    UserCreated {
        /// The created user's ID.
        user_id: UserId,
    },
    /// Namespace created, waiting for finalization.
    NamespaceCreated {
        /// The user's ID.
        user_id: UserId,
        /// The created namespace's ID.
        namespace_id: NamespaceId,
    },
    /// Saga completed successfully.
    Completed {
        /// The user's ID.
        user_id: UserId,
        /// The created namespace's ID.
        namespace_id: NamespaceId,
    },
    /// Saga failed and compensation was attempted.
    Failed {
        /// The step that failed (0-indexed).
        step: u8,
        /// Error description.
        error: String,
    },
    /// Saga failed and compensation completed.
    Compensated {
        /// The step that failed.
        step: u8,
        /// What was cleaned up.
        cleanup_summary: String,
    },
}

/// Input parameters for Create Organization saga.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateOrgInput {
    /// User's display name.
    pub user_name: String,
    /// User's email address.
    pub user_email: String,
    /// Organization name.
    pub org_name: String,
    /// Optional existing user ID (if user already exists).
    pub existing_user_id: Option<UserId>,
}

/// Record for the Create Organization saga, tracking state and retry progress.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateOrgSaga {
    /// Unique saga identifier.
    pub id: SagaId,
    /// Current state.
    pub state: CreateOrgSagaState,
    /// Input parameters.
    pub input: CreateOrgInput,
    /// When the saga was created.
    pub created_at: DateTime<Utc>,
    /// When the saga was last updated.
    pub updated_at: DateTime<Utc>,
    /// Number of retry attempts.
    pub retries: u8,
    /// Next retry time (for exponential backoff).
    pub next_retry_at: Option<DateTime<Utc>>,
}

impl CreateOrgSaga {
    /// Creates a new saga in Pending state.
    pub fn new(id: SagaId, input: CreateOrgInput) -> Self {
        let now = Utc::now();
        Self {
            id,
            state: CreateOrgSagaState::Pending,
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
            CreateOrgSagaState::Completed { .. }
                | CreateOrgSagaState::Failed { .. }
                | CreateOrgSagaState::Compensated { .. }
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

    /// Calculates next backoff duration using exponential backoff.
    pub fn next_backoff(&self) -> Duration {
        let base = Duration::from_secs(1);
        let backoff = base * 2u32.saturating_pow(self.retries as u32);
        std::cmp::min(backoff, MAX_BACKOFF)
    }

    /// Increments the retry count and sets the next retry time.
    pub fn schedule_retry(&mut self) {
        self.retries = self.retries.saturating_add(1);
        let backoff = self.next_backoff();
        self.next_retry_at =
            Some(Utc::now() + chrono::Duration::from_std(backoff).unwrap_or_default());
        self.updated_at = Utc::now();
    }

    /// Transitions to a new state.
    pub fn transition(&mut self, new_state: CreateOrgSagaState) {
        self.state = new_state;
        self.updated_at = Utc::now();
        self.next_retry_at = None; // Clear retry on successful transition
    }

    /// Marks as failed with error.
    ///
    /// After MAX_RETRIES failures, transitions to terminal Failed state.
    pub fn fail(&mut self, step: u8, error: String) {
        self.retries = self.retries.saturating_add(1);
        if self.retries >= MAX_RETRIES {
            self.state = CreateOrgSagaState::Failed { step, error };
        } else {
            // Schedule next retry with exponential backoff
            let backoff = self.next_backoff();
            self.next_retry_at =
                Some(Utc::now() + chrono::Duration::from_std(backoff).unwrap_or_default());
        }
        self.updated_at = Utc::now();
    }
}

// =============================================================================
// Delete User Saga
// =============================================================================

/// State machine for the Delete User saga.
///
/// Removes a user and all their memberships across namespaces.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum DeleteUserSagaState {
    /// Initial state: about to mark user as deleting.
    Pending,
    /// User marked as DELETING, removing memberships.
    MarkingDeleted {
        /// The user being deleted.
        user_id: UserId,
        /// Namespaces with memberships to remove.
        remaining_namespaces: Vec<NamespaceId>,
    },
    /// All memberships removed, ready to delete user record.
    MembershipsRemoved {
        /// The user being deleted.
        user_id: UserId,
    },
    /// Saga completed successfully.
    Completed {
        /// The deleted user's ID.
        user_id: UserId,
    },
    /// Saga failed.
    Failed {
        /// The step that failed.
        step: u8,
        /// Error description.
        error: String,
    },
}

/// Input parameters for Delete User saga.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteUserInput {
    /// User to delete.
    pub user_id: UserId,
    /// Lists of namespace IDs where user has memberships.
    pub namespace_ids: Vec<NamespaceId>,
}

/// Record for the Delete User saga, tracking state and retry progress.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteUserSaga {
    /// Unique saga identifier.
    pub id: SagaId,
    /// Current state.
    pub state: DeleteUserSagaState,
    /// Input parameters.
    pub input: DeleteUserInput,
    /// When the saga was created.
    pub created_at: DateTime<Utc>,
    /// When the saga was last updated.
    pub updated_at: DateTime<Utc>,
    /// Number of retry attempts.
    pub retries: u8,
    /// Next retry time.
    pub next_retry_at: Option<DateTime<Utc>>,
}

impl DeleteUserSaga {
    /// Creates a new saga in Pending state.
    pub fn new(id: SagaId, input: DeleteUserInput) -> Self {
        let now = Utc::now();
        Self {
            id,
            state: DeleteUserSagaState::Pending,
            input,
            created_at: now,
            updated_at: now,
            retries: 0,
            next_retry_at: None,
        }
    }

    /// Checks if the saga is complete.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.state,
            DeleteUserSagaState::Completed { .. } | DeleteUserSagaState::Failed { .. }
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

    /// Calculates the next backoff duration using exponential backoff.
    pub fn next_backoff(&self) -> Duration {
        let base = Duration::from_secs(1);
        let backoff = base * 2u32.saturating_pow(self.retries as u32);
        std::cmp::min(backoff, MAX_BACKOFF)
    }

    /// Schedules the next retry with exponential backoff.
    pub fn schedule_retry(&mut self) {
        self.retries = self.retries.saturating_add(1);
        let backoff = self.next_backoff();
        self.next_retry_at =
            Some(Utc::now() + chrono::Duration::from_std(backoff).unwrap_or_default());
        self.updated_at = Utc::now();
    }

    /// Transitions to a new state.
    pub fn transition(&mut self, new_state: DeleteUserSagaState) {
        self.state = new_state;
        self.updated_at = Utc::now();
        self.next_retry_at = None;
    }

    /// Marks as failed with error.
    ///
    /// After MAX_RETRIES failures, transitions to terminal Failed state.
    pub fn fail(&mut self, step: u8, error: String) {
        self.retries = self.retries.saturating_add(1);
        if self.retries >= MAX_RETRIES {
            self.state = DeleteUserSagaState::Failed { step, error };
        } else {
            // Schedule next retry with exponential backoff
            let backoff = self.next_backoff();
            self.next_retry_at =
                Some(Utc::now() + chrono::Duration::from_std(backoff).unwrap_or_default());
        }
        self.updated_at = Utc::now();
    }
}

// =============================================================================
// Generic Saga Wrapper
// =============================================================================

/// Type of saga.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum SagaType {
    /// Create Organization saga type.
    CreateOrg,
    /// Delete User saga type.
    DeleteUser,
}

/// Generic saga record that wraps specific saga types.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Saga {
    /// Create Organization saga.
    CreateOrg(CreateOrgSaga),
    /// Delete User saga.
    DeleteUser(DeleteUserSaga),
}

impl Saga {
    /// Returns the saga ID.
    pub fn id(&self) -> &str {
        match self {
            Saga::CreateOrg(s) => &s.id,
            Saga::DeleteUser(s) => &s.id,
        }
    }

    /// Returns the saga type.
    pub fn saga_type(&self) -> SagaType {
        match self {
            Saga::CreateOrg(_) => SagaType::CreateOrg,
            Saga::DeleteUser(_) => SagaType::DeleteUser,
        }
    }

    /// Checks if the saga is in a terminal state.
    pub fn is_terminal(&self) -> bool {
        match self {
            Saga::CreateOrg(s) => s.is_terminal(),
            Saga::DeleteUser(s) => s.is_terminal(),
        }
    }

    /// Checks if the saga is ready for retry.
    pub fn is_ready_for_retry(&self) -> bool {
        match self {
            Saga::CreateOrg(s) => s.is_ready_for_retry(),
            Saga::DeleteUser(s) => s.is_ready_for_retry(),
        }
    }

    /// Returns the creation timestamp.
    pub fn created_at(&self) -> DateTime<Utc> {
        match self {
            Saga::CreateOrg(s) => s.created_at,
            Saga::DeleteUser(s) => s.created_at,
        }
    }

    /// Returns the last-updated timestamp.
    pub fn updated_at(&self) -> DateTime<Utc> {
        match self {
            Saga::CreateOrg(s) => s.updated_at,
            Saga::DeleteUser(s) => s.updated_at,
        }
    }

    /// Returns the retry count.
    pub fn retries(&self) -> u8 {
        match self {
            Saga::CreateOrg(s) => s.retries,
            Saga::DeleteUser(s) => s.retries,
        }
    }

    /// Serializes to JSON bytes.
    pub fn to_bytes(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    /// Deserializes from JSON bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn test_create_org_saga_new() {
        let input = CreateOrgInput {
            user_name: "Alice".to_string(),
            user_email: "alice@example.com".to_string(),
            org_name: "Acme Corp".to_string(),
            existing_user_id: None,
        };
        let saga = CreateOrgSaga::new("saga-123".to_string(), input);

        assert_eq!(saga.id, "saga-123");
        assert_eq!(saga.state, CreateOrgSagaState::Pending);
        assert_eq!(saga.retries, 0);
        assert!(!saga.is_terminal());
        assert!(saga.is_ready_for_retry());
    }

    #[test]
    fn test_create_org_saga_transitions() {
        let input = CreateOrgInput {
            user_name: "Alice".to_string(),
            user_email: "alice@example.com".to_string(),
            org_name: "Acme Corp".to_string(),
            existing_user_id: None,
        };
        let mut saga = CreateOrgSaga::new("saga-123".to_string(), input);

        // Transition to UserCreated
        saga.transition(CreateOrgSagaState::UserCreated { user_id: UserId::new(1) });
        assert!(matches!(
            saga.state,
            CreateOrgSagaState::UserCreated { user_id } if user_id == UserId::new(1)
        ));
        assert!(!saga.is_terminal());

        // Transition to NamespaceCreated
        saga.transition(CreateOrgSagaState::NamespaceCreated {
            user_id: UserId::new(1),
            namespace_id: NamespaceId::new(100),
        });
        assert!(!saga.is_terminal());

        // Transition to Completed
        saga.transition(CreateOrgSagaState::Completed {
            user_id: UserId::new(1),
            namespace_id: NamespaceId::new(100),
        });
        assert!(saga.is_terminal());
    }

    #[test]
    fn test_exponential_backoff() {
        let input = CreateOrgInput {
            user_name: "Alice".to_string(),
            user_email: "alice@example.com".to_string(),
            org_name: "Acme Corp".to_string(),
            existing_user_id: None,
        };
        let mut saga = CreateOrgSaga::new("saga-123".to_string(), input);

        // First backoff: 1s
        assert_eq!(saga.next_backoff(), Duration::from_secs(1));

        saga.schedule_retry();
        // Second backoff: 2s
        assert_eq!(saga.next_backoff(), Duration::from_secs(2));

        saga.schedule_retry();
        // Third backoff: 4s
        assert_eq!(saga.next_backoff(), Duration::from_secs(4));
    }

    #[test]
    fn test_max_backoff() {
        let input = CreateOrgInput {
            user_name: "Alice".to_string(),
            user_email: "alice@example.com".to_string(),
            org_name: "Acme Corp".to_string(),
            existing_user_id: None,
        };
        let mut saga = CreateOrgSaga::new("saga-123".to_string(), input);

        // Simulate many retries
        for _ in 0..20 {
            saga.schedule_retry();
        }

        // Should cap at MAX_BACKOFF (5 minutes)
        assert_eq!(saga.next_backoff(), MAX_BACKOFF);
    }

    #[test]
    fn test_fail_after_max_retries() {
        let input = CreateOrgInput {
            user_name: "Alice".to_string(),
            user_email: "alice@example.com".to_string(),
            org_name: "Acme Corp".to_string(),
            existing_user_id: None,
        };
        let mut saga = CreateOrgSaga::new("saga-123".to_string(), input);

        // Fail MAX_RETRIES times
        for _ in 0..MAX_RETRIES {
            saga.fail(1, "test error".to_string());
        }

        // Should now be in Failed state
        assert!(saga.is_terminal());
        assert!(matches!(saga.state, CreateOrgSagaState::Failed { step: 1, .. }));
    }

    #[test]
    fn test_delete_user_saga() {
        let input = DeleteUserInput {
            user_id: UserId::new(1),
            namespace_ids: vec![NamespaceId::new(100), NamespaceId::new(101)],
        };
        let mut saga = DeleteUserSaga::new("delete-123".to_string(), input);

        assert!(!saga.is_terminal());

        saga.transition(DeleteUserSagaState::MarkingDeleted {
            user_id: UserId::new(1),
            remaining_namespaces: vec![NamespaceId::new(100), NamespaceId::new(101)],
        });
        assert!(!saga.is_terminal());

        saga.transition(DeleteUserSagaState::MembershipsRemoved { user_id: UserId::new(1) });
        assert!(!saga.is_terminal());

        saga.transition(DeleteUserSagaState::Completed { user_id: UserId::new(1) });
        assert!(saga.is_terminal());
    }

    #[test]
    fn test_saga_serialization() {
        let input = CreateOrgInput {
            user_name: "Alice".to_string(),
            user_email: "alice@example.com".to_string(),
            org_name: "Acme Corp".to_string(),
            existing_user_id: None,
        };
        let saga = Saga::CreateOrg(CreateOrgSaga::new("saga-123".to_string(), input));

        let bytes = saga.to_bytes().unwrap();
        let restored = Saga::from_bytes(&bytes).unwrap();

        assert_eq!(saga.id(), restored.id());
        assert_eq!(saga.saga_type(), restored.saga_type());
    }

    #[test]
    fn test_saga_wrapper() {
        let input = CreateOrgInput {
            user_name: "Alice".to_string(),
            user_email: "alice@example.com".to_string(),
            org_name: "Acme Corp".to_string(),
            existing_user_id: None,
        };
        let saga = Saga::CreateOrg(CreateOrgSaga::new("saga-123".to_string(), input));

        assert_eq!(saga.id(), "saga-123");
        assert_eq!(saga.saga_type(), SagaType::CreateOrg);
        assert!(!saga.is_terminal());
        assert!(saga.is_ready_for_retry());
        assert_eq!(saga.retries(), 0);
    }
}
