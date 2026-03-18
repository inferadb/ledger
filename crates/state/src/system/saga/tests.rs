use std::time::Duration;

use chrono::Utc;
use inferadb_ledger_types::{
    OrganizationId, OrganizationSlug, Region, SigningKeyScope, UserId, UserSlug,
};

use super::*;

#[test]
fn test_exponential_backoff() {
    let input =
        DeleteUserInput { user: UserId::new(1), organization_ids: vec![OrganizationId::new(100)] };
    let mut saga = DeleteUserSaga::new(SagaId::new("saga-123"), input);

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
    let input =
        DeleteUserInput { user: UserId::new(1), organization_ids: vec![OrganizationId::new(100)] };
    let mut saga = DeleteUserSaga::new(SagaId::new("saga-123"), input);

    // Simulate many retries
    for _ in 0..20 {
        saga.schedule_retry();
    }

    // Should cap at MAX_BACKOFF (5 minutes)
    assert_eq!(saga.next_backoff(), MAX_BACKOFF);
}

#[test]
fn test_fail_after_max_retries() {
    let input =
        DeleteUserInput { user: UserId::new(1), organization_ids: vec![OrganizationId::new(100)] };
    let mut saga = DeleteUserSaga::new(SagaId::new("saga-123"), input);

    // Fail MAX_RETRIES times
    for _ in 0..MAX_RETRIES {
        saga.fail(1, "test error".to_string());
    }

    // Should now be in Failed state
    assert!(saga.is_terminal());
    assert!(matches!(saga.state, DeleteUserSagaState::Failed { step: 1, .. }));
}

#[test]
fn test_delete_user_saga() {
    let input = DeleteUserInput {
        user: UserId::new(1),
        organization_ids: vec![OrganizationId::new(100), OrganizationId::new(101)],
    };
    let mut saga = DeleteUserSaga::new(SagaId::new("delete-123"), input);

    assert!(!saga.is_terminal());

    saga.transition(DeleteUserSagaState::MarkingDeleted {
        user_id: UserId::new(1),
        remaining_organizations: vec![OrganizationId::new(100), OrganizationId::new(101)],
    });
    assert!(!saga.is_terminal());

    saga.transition(DeleteUserSagaState::MembershipsRemoved { user_id: UserId::new(1) });
    assert!(!saga.is_terminal());

    saga.transition(DeleteUserSagaState::Completed { user_id: UserId::new(1) });
    assert!(saga.is_terminal());
}

#[test]
fn test_saga_serialization() {
    let input =
        DeleteUserInput { user: UserId::new(42), organization_ids: vec![OrganizationId::new(1)] };
    let saga = Saga::DeleteUser(DeleteUserSaga::new(SagaId::new("saga-123"), input));

    let bytes = saga.to_bytes().unwrap();
    let restored = Saga::from_bytes(&bytes).unwrap();

    assert_eq!(saga.id(), restored.id());
    assert_eq!(saga.saga_type(), restored.saga_type());
}

#[test]
fn test_saga_wrapper() {
    let input =
        DeleteUserInput { user: UserId::new(42), organization_ids: vec![OrganizationId::new(1)] };
    let saga = Saga::DeleteUser(DeleteUserSaga::new(SagaId::new("saga-123"), input));

    assert_eq!(saga.id(), "saga-123");
    assert_eq!(saga.saga_type(), SagaType::DeleteUser);
    assert!(!saga.is_terminal());
    assert!(saga.is_ready_for_retry());
    assert_eq!(saga.retries(), 0);
}

fn make_migrate_org_input() -> MigrateOrgInput {
    MigrateOrgInput {
        organization_id: OrganizationId::new(42),
        organization_slug: OrganizationSlug::new(9_001_000_000_000_000_000),
        source_region: Region::US_EAST_VA,
        target_region: Region::IE_EAST_DUBLIN,
        acknowledge_residency_downgrade: false,
        metadata_only: false,
    }
}

#[test]
fn test_migrate_org_saga_new() {
    let saga = MigrateOrgSaga::new(SagaId::new("migrate-1"), make_migrate_org_input());

    assert_eq!(saga.id, "migrate-1");
    assert_eq!(saga.state, MigrateOrgSagaState::Pending);
    assert_eq!(saga.retries, 0);
    assert!(saga.next_retry_at.is_none());
    assert!(!saga.is_terminal());
    assert!(saga.is_ready_for_retry());
}

#[test]
fn test_migrate_org_saga_transitions() {
    let mut saga = MigrateOrgSaga::new(SagaId::new("migrate-2"), make_migrate_org_input());

    // Pending → MigrationStarted
    saga.transition(MigrateOrgSagaState::MigrationStarted);
    assert_eq!(saga.state, MigrateOrgSagaState::MigrationStarted);
    assert!(!saga.is_terminal());

    // MigrationStarted → DataSnapshotTaken
    saga.transition(MigrateOrgSagaState::DataSnapshotTaken {
        source_state_root: vec![0xde, 0xad, 0xbe, 0xef],
    });
    assert!(matches!(
        saga.state,
        MigrateOrgSagaState::DataSnapshotTaken { ref source_state_root }
            if source_state_root == &[0xde, 0xad, 0xbe, 0xef]
    ));
    assert!(!saga.is_terminal());

    // DataSnapshotTaken → DataWritten
    saga.transition(MigrateOrgSagaState::DataWritten);
    assert_eq!(saga.state, MigrateOrgSagaState::DataWritten);
    assert!(!saga.is_terminal());

    // DataWritten → IntegrityVerified
    saga.transition(MigrateOrgSagaState::IntegrityVerified);
    assert_eq!(saga.state, MigrateOrgSagaState::IntegrityVerified);
    assert!(!saga.is_terminal());

    // IntegrityVerified → RoutingUpdated
    saga.transition(MigrateOrgSagaState::RoutingUpdated);
    assert_eq!(saga.state, MigrateOrgSagaState::RoutingUpdated);
    assert!(!saga.is_terminal());

    // RoutingUpdated → SourceDeleted
    saga.transition(MigrateOrgSagaState::SourceDeleted);
    assert_eq!(saga.state, MigrateOrgSagaState::SourceDeleted);
    assert!(!saga.is_terminal());

    // SourceDeleted → Completed
    saga.transition(MigrateOrgSagaState::Completed);
    assert_eq!(saga.state, MigrateOrgSagaState::Completed);
    assert!(saga.is_terminal());
    assert!(!saga.is_ready_for_retry());
}

#[test]
fn test_migrate_org_saga_is_terminal() {
    let base = MigrateOrgSaga::new(SagaId::new("migrate-3"), make_migrate_org_input());

    let completed = {
        let mut s = base.clone();
        s.state = MigrateOrgSagaState::Completed;
        s
    };
    assert!(completed.is_terminal());

    let failed = {
        let mut s = base.clone();
        s.state = MigrateOrgSagaState::Failed { step: 2, error: "disk full".to_string() };
        s
    };
    assert!(failed.is_terminal());

    let rolled_back = {
        let mut s = base.clone();
        s.state = MigrateOrgSagaState::RolledBack { reason: "operator abort".to_string() };
        s
    };
    assert!(rolled_back.is_terminal());

    let timed_out = {
        let mut s = base.clone();
        s.state = MigrateOrgSagaState::TimedOut;
        s
    };
    assert!(timed_out.is_terminal());

    // Non-terminal states
    let mut in_progress = base.clone();
    in_progress.state = MigrateOrgSagaState::MigrationStarted;
    assert!(!in_progress.is_terminal());
}

#[test]
fn test_migrate_org_saga_fail() {
    let mut saga = MigrateOrgSaga::new(SagaId::new("migrate-4"), make_migrate_org_input());

    // First MAX_RETRIES - 1 failures schedule retries, not terminal.
    for _ in 0..(MAX_RETRIES - 1) {
        saga.fail(1, "transient error".to_string());
        assert!(!saga.is_terminal(), "should not be terminal before exhausting retries");
        assert!(saga.next_retry_at.is_some(), "should schedule a retry");
    }

    // Final failure tips over into terminal Failed state.
    saga.fail(1, "permanent error".to_string());
    assert!(saga.is_terminal());
    assert!(matches!(
        saga.state,
        MigrateOrgSagaState::Failed { step: 1, ref error } if error == "permanent error"
    ));
    assert_eq!(saga.retries, MAX_RETRIES);
    assert!(!saga.is_ready_for_retry());
}

#[test]
fn test_migrate_org_saga_is_timed_out() {
    let mut saga = MigrateOrgSaga::new(SagaId::new("migrate-5"), make_migrate_org_input());

    // Back-date creation so the saga appears old.
    saga.created_at = Utc::now() - chrono::Duration::hours(2);

    assert!(saga.is_timed_out(Duration::from_secs(3600))); // 1-hour timeout exceeded
    assert!(!saga.is_timed_out(Duration::from_secs(7200 + 60))); // 2-hour+ timeout not yet exceeded
}

#[test]
fn test_migrate_org_saga_current_step() {
    let mut saga = MigrateOrgSaga::new(SagaId::new("migrate-6"), make_migrate_org_input());

    assert_eq!(saga.current_step(), 0); // Pending

    saga.state = MigrateOrgSagaState::MigrationStarted;
    assert_eq!(saga.current_step(), 1);

    saga.state = MigrateOrgSagaState::DataSnapshotTaken { source_state_root: vec![0x01] };
    assert_eq!(saga.current_step(), 2);

    saga.state = MigrateOrgSagaState::DataWritten;
    assert_eq!(saga.current_step(), 3);

    saga.state = MigrateOrgSagaState::IntegrityVerified;
    assert_eq!(saga.current_step(), 4);

    saga.state = MigrateOrgSagaState::RoutingUpdated;
    assert_eq!(saga.current_step(), 5);

    saga.state = MigrateOrgSagaState::SourceDeleted;
    assert_eq!(saga.current_step(), 6);

    // Terminal states return 0
    saga.state = MigrateOrgSagaState::Completed;
    assert_eq!(saga.current_step(), 0);

    saga.state = MigrateOrgSagaState::Failed { step: 3, error: "oops".to_string() };
    assert_eq!(saga.current_step(), 0);

    saga.state = MigrateOrgSagaState::RolledBack { reason: "timeout".to_string() };
    assert_eq!(saga.current_step(), 0);

    saga.state = MigrateOrgSagaState::TimedOut;
    assert_eq!(saga.current_step(), 0);
}

#[test]
fn test_migrate_org_saga_serialization() {
    let inner = MigrateOrgSaga::new(SagaId::new("migrate-7"), make_migrate_org_input());
    let saga = Saga::MigrateOrg(inner);

    let bytes = saga.to_bytes().unwrap();
    let restored = Saga::from_bytes(&bytes).unwrap();

    assert_eq!(saga.id(), restored.id());
    assert_eq!(saga.saga_type(), SagaType::MigrateOrg);
    assert_eq!(restored.saga_type(), SagaType::MigrateOrg);
    assert_eq!(saga.retries(), restored.retries());
    assert_eq!(saga.created_at(), restored.created_at());

    // Verify inner fields survive the round-trip.
    let Saga::MigrateOrg(ref original) = saga else { panic!("expected MigrateOrg") };
    let Saga::MigrateOrg(ref deserialized) = restored else { panic!("expected MigrateOrg") };
    assert_eq!(original.input.organization_id, deserialized.input.organization_id);
    assert_eq!(original.input.organization_slug, deserialized.input.organization_slug);
    assert_eq!(original.input.source_region, deserialized.input.source_region);
    assert_eq!(original.input.target_region, deserialized.input.target_region);
    assert_eq!(
        original.input.acknowledge_residency_downgrade,
        deserialized.input.acknowledge_residency_downgrade
    );
    assert_eq!(original.input.metadata_only, deserialized.input.metadata_only);
    assert_eq!(original.state, deserialized.state);
}

// =========================================================================
// MigrateUserSaga tests
// =========================================================================

fn make_migrate_user_input() -> MigrateUserInput {
    MigrateUserInput {
        user: UserId::new(7),
        source_region: Region::US_EAST_VA,
        target_region: Region::IE_EAST_DUBLIN,
    }
}

#[test]
fn test_migrate_user_saga_new() {
    let saga = MigrateUserSaga::new(SagaId::new("user-migrate-1"), make_migrate_user_input());

    assert_eq!(saga.id, "user-migrate-1");
    assert_eq!(saga.state, MigrateUserSagaState::Pending);
    assert_eq!(saga.retries, 0);
    assert!(saga.next_retry_at.is_none());
    assert!(!saga.is_terminal());
    assert!(saga.is_ready_for_retry());
}

#[test]
fn test_migrate_user_saga_transitions() {
    let mut saga = MigrateUserSaga::new(SagaId::new("user-migrate-2"), make_migrate_user_input());

    // Step 1: Mark directory as migrating
    saga.transition(MigrateUserSagaState::DirectoryMarkedMigrating);
    assert_eq!(saga.state, MigrateUserSagaState::DirectoryMarkedMigrating);
    assert!(!saga.is_terminal());

    // Step 2: User data read from source region
    saga.transition(MigrateUserSagaState::UserDataRead);
    assert_eq!(saga.state, MigrateUserSagaState::UserDataRead);
    assert!(!saga.is_terminal());

    // Step 3: User data written to target region
    saga.transition(MigrateUserSagaState::UserDataWritten);
    assert_eq!(saga.state, MigrateUserSagaState::UserDataWritten);
    assert!(!saga.is_terminal());

    // Step 4: Directory updated to Active with new region
    saga.transition(MigrateUserSagaState::DirectoryUpdated);
    assert_eq!(saga.state, MigrateUserSagaState::DirectoryUpdated);
    assert!(!saga.is_terminal());

    // Step 5: Source data deleted
    saga.transition(MigrateUserSagaState::SourceDeleted);
    assert_eq!(saga.state, MigrateUserSagaState::SourceDeleted);
    assert!(!saga.is_terminal());

    // Step 6: Completed
    saga.transition(MigrateUserSagaState::Completed);
    assert_eq!(saga.state, MigrateUserSagaState::Completed);
    assert!(saga.is_terminal());
}

#[test]
fn test_migrate_user_saga_is_terminal() {
    let base = MigrateUserSaga::new(SagaId::new("user-migrate-3"), make_migrate_user_input());

    // Completed is terminal
    let mut s = base.clone();
    s.state = MigrateUserSagaState::Completed;
    assert!(s.is_terminal());

    // Failed is terminal
    let mut s = base.clone();
    s.state = MigrateUserSagaState::Failed { step: 2, error: "disk full".to_string() };
    assert!(s.is_terminal());

    // Compensated is terminal
    let mut s = base.clone();
    s.state = MigrateUserSagaState::Compensated { reason: "operator abort".to_string() };
    assert!(s.is_terminal());

    // TimedOut is terminal
    let mut s = base.clone();
    s.state = MigrateUserSagaState::TimedOut;
    assert!(s.is_terminal());

    // In-progress states are NOT terminal
    let mut in_progress = base;
    in_progress.state = MigrateUserSagaState::DirectoryMarkedMigrating;
    assert!(!in_progress.is_terminal());
}

#[test]
fn test_migrate_user_saga_fail() {
    let mut saga = MigrateUserSaga::new(SagaId::new("user-migrate-4"), make_migrate_user_input());

    // Fail multiple times with retries
    for i in 0..MAX_RETRIES.saturating_sub(1) {
        saga.fail(1, "transient error".to_string());
        assert!(!saga.is_terminal(), "should not be terminal after {} retries", i + 1);
        assert!(saga.next_retry_at.is_some());
    }

    // Final fail transitions to terminal Failed
    saga.fail(1, "permanent error".to_string());
    assert!(saga.is_terminal());
    assert!(matches!(
        saga.state,
        MigrateUserSagaState::Failed { step: 1, ref error } if error == "permanent error"
    ));
}

#[test]
fn test_migrate_user_saga_is_timed_out() {
    let mut saga = MigrateUserSaga::new(SagaId::new("user-migrate-5"), make_migrate_user_input());
    saga.created_at = Utc::now() - chrono::Duration::minutes(10);

    assert!(saga.is_timed_out(Duration::from_secs(300))); // 5 min timeout
    assert!(!saga.is_timed_out(Duration::from_secs(3600))); // 1 hour timeout
}

#[test]
fn test_migrate_user_saga_current_step() {
    let mut saga = MigrateUserSaga::new(SagaId::new("user-migrate-6"), make_migrate_user_input());

    assert_eq!(saga.current_step(), 0); // Pending

    saga.state = MigrateUserSagaState::DirectoryMarkedMigrating;
    assert_eq!(saga.current_step(), 1);

    saga.state = MigrateUserSagaState::UserDataRead;
    assert_eq!(saga.current_step(), 2);

    saga.state = MigrateUserSagaState::UserDataWritten;
    assert_eq!(saga.current_step(), 3);

    saga.state = MigrateUserSagaState::DirectoryUpdated;
    assert_eq!(saga.current_step(), 4);

    saga.state = MigrateUserSagaState::SourceDeleted;
    assert_eq!(saga.current_step(), 5);

    // Terminal states return 0
    saga.state = MigrateUserSagaState::Completed;
    assert_eq!(saga.current_step(), 0);

    saga.state = MigrateUserSagaState::Failed { step: 3, error: "oops".to_string() };
    assert_eq!(saga.current_step(), 0);

    saga.state = MigrateUserSagaState::Compensated { reason: "timeout".to_string() };
    assert_eq!(saga.current_step(), 0);

    saga.state = MigrateUserSagaState::TimedOut;
    assert_eq!(saga.current_step(), 0);
}

#[test]
fn test_migrate_user_saga_serialization() {
    let inner = MigrateUserSaga::new(SagaId::new("user-migrate-7"), make_migrate_user_input());
    let saga = Saga::MigrateUser(inner);

    let bytes = saga.to_bytes().unwrap();
    let restored = Saga::from_bytes(&bytes).unwrap();

    assert_eq!(saga.id(), restored.id());
    assert_eq!(saga.saga_type(), SagaType::MigrateUser);
    assert_eq!(restored.saga_type(), SagaType::MigrateUser);
    assert_eq!(saga.retries(), restored.retries());
    assert_eq!(saga.created_at(), restored.created_at());

    // Verify inner fields survive the round-trip.
    let Saga::MigrateUser(ref original) = saga else { panic!("expected MigrateUser") };
    let Saga::MigrateUser(ref deserialized) = restored else { panic!("expected MigrateUser") };
    assert_eq!(original.input.user, deserialized.input.user);
    assert_eq!(original.input.source_region, deserialized.input.source_region);
    assert_eq!(original.input.target_region, deserialized.input.target_region);
    assert_eq!(original.state, deserialized.state);
}

#[test]
fn test_migrate_user_saga_wrapper_integration() {
    let inner = MigrateUserSaga::new(SagaId::new("user-migrate-8"), make_migrate_user_input());
    let saga = Saga::MigrateUser(inner);

    assert_eq!(saga.id(), "user-migrate-8");
    assert_eq!(saga.saga_type(), SagaType::MigrateUser);
    assert!(!saga.is_terminal());
    assert!(saga.is_ready_for_retry());
    assert_eq!(saga.retries(), 0);
}

// =========================================================================
// SagaStep and StepStatus tests
// =========================================================================

#[test]
fn test_step_status_serialization() {
    let statuses = [
        (StepStatus::Pending, "\"pending\""),
        (StepStatus::Completed, "\"completed\""),
        (StepStatus::Failed, "\"failed\""),
        (StepStatus::Compensated, "\"compensated\""),
    ];

    for (status, expected_json) in &statuses {
        let json = serde_json::to_string(status).unwrap();
        assert_eq!(&json, expected_json);

        let deserialized: StepStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(*status, deserialized);
    }
}

#[test]
fn test_saga_step_construction_and_serialization() {
    let step = SagaStep {
        step_id: 0,
        target_region: Region::GLOBAL,
        action: "reserve email HMAC".to_string(),
        compensate: "delete email HMAC".to_string(),
        status: StepStatus::Pending,
    };

    assert_eq!(step.step_id, 0);
    assert_eq!(step.target_region, Region::GLOBAL);
    assert_eq!(step.status, StepStatus::Pending);

    let bytes = serde_json::to_vec(&step).unwrap();
    let restored: SagaStep = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(step, restored);
}

// =========================================================================
// SagaLockKey tests
// =========================================================================

#[test]
fn test_saga_lock_key_display() {
    assert_eq!(SagaLockKey::User(UserId::new(42)).to_string(), "user:42");
    assert_eq!(SagaLockKey::Email("abc123".to_string()).to_string(), "email:abc123");
    assert_eq!(SagaLockKey::Organization(OrganizationId::new(7)).to_string(), "org:7");
}

#[test]
fn test_saga_lock_key_equality_and_hash() {
    use std::collections::HashSet;

    let key1 = SagaLockKey::User(UserId::new(1));
    let key2 = SagaLockKey::User(UserId::new(1));
    let key3 = SagaLockKey::User(UserId::new(2));

    assert_eq!(key1, key2);
    assert_ne!(key1, key3);

    let mut set = HashSet::new();
    set.insert(key1.clone());
    assert!(set.contains(&key2));
    assert!(!set.contains(&key3));
}

#[test]
fn test_saga_lock_key_serialization() {
    let keys = vec![
        SagaLockKey::User(UserId::new(42)),
        SagaLockKey::Email("hmac_hex".to_string()),
        SagaLockKey::Organization(OrganizationId::new(7)),
    ];

    for key in &keys {
        let json = serde_json::to_vec(key).unwrap();
        let restored: SagaLockKey = serde_json::from_slice(&json).unwrap();
        assert_eq!(*key, restored);
    }
}

// =========================================================================
// CreateUserSaga tests
// =========================================================================

fn make_create_user_input() -> CreateUserInput {
    CreateUserInput {
        hmac: "deadbeef0123456789abcdef".to_string(),
        region: Region::IE_EAST_DUBLIN,
        admin: false,
        organization_name: "Test Organization".to_string(),
        default_org_tier: OrganizationTier::Free,
    }
}

#[test]
fn test_create_user_saga_new() {
    let saga = CreateUserSaga::new(SagaId::new("cu-1"), make_create_user_input());

    assert_eq!(saga.id, "cu-1");
    assert_eq!(saga.state, CreateUserSagaState::Pending);
    assert_eq!(saga.retries, 0);
    assert!(saga.next_retry_at.is_none());
    assert!(!saga.is_terminal());
    assert!(saga.is_ready_for_retry());
}

#[test]
fn test_create_user_saga_transitions() {
    let mut saga = CreateUserSaga::new(SagaId::new("cu-2"), make_create_user_input());

    // Step 0 → EmailReserved
    saga.transition(CreateUserSagaState::EmailReserved {
        user_id: UserId::new(10),
        user_slug: UserSlug::new(1_000_000),
        hmac_hex: "deadbeef".to_string(),
    });
    assert!(matches!(saga.state, CreateUserSagaState::EmailReserved { .. }));
    assert!(!saga.is_terminal());

    // Step 1 → RegionalDataWritten
    saga.transition(CreateUserSagaState::RegionalDataWritten {
        user_id: UserId::new(10),
        user_slug: UserSlug::new(1_000_000),
        hmac_hex: "deadbeef".to_string(),
    });
    assert!(matches!(saga.state, CreateUserSagaState::RegionalDataWritten { .. }));
    assert!(!saga.is_terminal());

    // Step 2 → Completed
    saga.transition(CreateUserSagaState::Completed {
        user_id: UserId::new(10),
        user_slug: UserSlug::new(1_000_000),
    });
    assert!(saga.is_terminal());
    assert!(!saga.is_ready_for_retry());
}

#[test]
fn test_create_user_saga_is_terminal() {
    let base = CreateUserSaga::new(SagaId::new("cu-3"), make_create_user_input());

    let mut s = base.clone();
    s.state =
        CreateUserSagaState::Completed { user_id: UserId::new(1), user_slug: UserSlug::new(1) };
    assert!(s.is_terminal());

    let mut s = base.clone();
    s.state = CreateUserSagaState::Failed { step: 1, error: "err".to_string() };
    assert!(s.is_terminal());

    let mut s = base.clone();
    s.state = CreateUserSagaState::Compensated { step: 0, cleanup_summary: "cleaned".to_string() };
    assert!(s.is_terminal());

    let mut s = base.clone();
    s.state = CreateUserSagaState::TimedOut;
    assert!(s.is_terminal());

    // Non-terminal
    let mut s = base;
    s.state = CreateUserSagaState::EmailReserved {
        user_id: UserId::new(1),
        user_slug: UserSlug::new(1),
        hmac_hex: "abc".to_string(),
    };
    assert!(!s.is_terminal());
}

#[test]
fn test_create_user_saga_fail_and_retry() {
    let mut saga = CreateUserSaga::new(SagaId::new("cu-4"), make_create_user_input());

    for _ in 0..MAX_RETRIES.saturating_sub(1) {
        saga.fail(0, "transient".to_string());
        assert!(!saga.is_terminal());
        assert!(saga.next_retry_at.is_some());
    }

    saga.fail(0, "permanent".to_string());
    assert!(saga.is_terminal());
    assert!(matches!(
        saga.state,
        CreateUserSagaState::Failed { step: 0, ref error } if error == "permanent"
    ));
}

#[test]
fn test_create_user_saga_is_timed_out() {
    let mut saga = CreateUserSaga::new(SagaId::new("cu-5"), make_create_user_input());
    saga.created_at = Utc::now() - chrono::Duration::minutes(5);

    assert!(saga.is_timed_out(Duration::from_secs(60))); // 1 min timeout
    assert!(!saga.is_timed_out(Duration::from_secs(600))); // 10 min timeout
}

#[test]
fn test_create_user_saga_current_step() {
    let mut saga = CreateUserSaga::new(SagaId::new("cu-6"), make_create_user_input());

    assert_eq!(saga.current_step(), 0); // Pending

    saga.state = CreateUserSagaState::EmailReserved {
        user_id: UserId::new(1),
        user_slug: UserSlug::new(1),
        hmac_hex: "abc".to_string(),
    };
    assert_eq!(saga.current_step(), 1);

    saga.state = CreateUserSagaState::RegionalDataWritten {
        user_id: UserId::new(1),
        user_slug: UserSlug::new(1),
        hmac_hex: "abc".to_string(),
    };
    assert_eq!(saga.current_step(), 2);

    // Terminal states return 0
    saga.state =
        CreateUserSagaState::Completed { user_id: UserId::new(1), user_slug: UserSlug::new(1) };
    assert_eq!(saga.current_step(), 0);
}

#[test]
fn test_create_user_saga_target_region() {
    let mut saga = CreateUserSaga::new(SagaId::new("cu-7"), make_create_user_input());
    assert_eq!(saga.input.region, Region::IE_EAST_DUBLIN);

    // Pending → GLOBAL (step 0 targets GLOBAL for ID allocation + email CAS)
    assert_eq!(saga.target_region(), Region::GLOBAL);

    // EmailReserved → user's region (step 1 targets regional store)
    saga.state = CreateUserSagaState::EmailReserved {
        user_id: UserId::new(1),
        user_slug: UserSlug::new(1),
        hmac_hex: "abc".to_string(),
    };
    assert_eq!(saga.target_region(), Region::IE_EAST_DUBLIN);

    // RegionalDataWritten → GLOBAL (step 2 targets GLOBAL for directory entry)
    saga.state = CreateUserSagaState::RegionalDataWritten {
        user_id: UserId::new(1),
        user_slug: UserSlug::new(1),
        hmac_hex: "abc".to_string(),
    };
    assert_eq!(saga.target_region(), Region::GLOBAL);
}

#[test]
fn test_create_user_saga_serialization() {
    let inner = CreateUserSaga::new(SagaId::new("cu-8"), make_create_user_input());
    let saga = Saga::CreateUser(inner);

    let bytes = saga.to_bytes().unwrap();
    let restored = Saga::from_bytes(&bytes).unwrap();

    assert_eq!(saga.id(), restored.id());
    assert_eq!(saga.saga_type(), SagaType::CreateUser);
    assert_eq!(restored.saga_type(), SagaType::CreateUser);
    assert_eq!(saga.retries(), restored.retries());
    assert_eq!(saga.created_at(), restored.created_at());

    let Saga::CreateUser(ref original) = saga else { panic!("expected CreateUser") };
    let Saga::CreateUser(ref deserialized) = restored else { panic!("expected CreateUser") };
    assert_eq!(original.input.hmac, deserialized.input.hmac);
    assert_eq!(original.input.region, deserialized.input.region);
    assert_eq!(original.input.admin, deserialized.input.admin);
    assert_eq!(original.state, deserialized.state);
}

#[test]
fn test_create_user_saga_wrapper_integration() {
    let inner = CreateUserSaga::new(SagaId::new("cu-9"), make_create_user_input());
    let saga = Saga::CreateUser(inner);

    assert_eq!(saga.id(), "cu-9");
    assert_eq!(saga.saga_type(), SagaType::CreateUser);
    assert!(!saga.is_terminal());
    assert!(saga.is_ready_for_retry());
    assert_eq!(saga.retries(), 0);
}

// =========================================================================
// Lock key integration tests
// =========================================================================

#[test]
fn test_saga_lock_keys_create_user() {
    let inner = CreateUserSaga::new(SagaId::new("cu-lock"), make_create_user_input());
    let saga = Saga::CreateUser(inner);

    let keys = saga.lock_keys();
    assert_eq!(keys.len(), 1);
    assert!(matches!(&keys[0], SagaLockKey::Email(h) if h == "deadbeef0123456789abcdef"));
}

#[test]
fn test_saga_lock_keys_migrate_org() {
    let inner = MigrateOrgSaga::new(SagaId::new("mo-lock"), make_migrate_org_input());
    let saga = Saga::MigrateOrg(inner);

    let keys = saga.lock_keys();
    assert_eq!(keys.len(), 1);
    assert!(matches!(keys[0], SagaLockKey::Organization(id) if id == OrganizationId::new(42)));
}

#[test]
fn test_saga_lock_keys_migrate_user() {
    let inner = MigrateUserSaga::new(SagaId::new("mu-lock"), make_migrate_user_input());
    let saga = Saga::MigrateUser(inner);

    let keys = saga.lock_keys();
    assert_eq!(keys.len(), 1);
    assert!(matches!(keys[0], SagaLockKey::User(id) if id == UserId::new(7)));
}

// =========================================================================
// CreateOrganizationSaga tests
// =========================================================================

fn make_create_organization_input() -> CreateOrganizationInput {
    CreateOrganizationInput {
        slug: OrganizationSlug::new(42),
        region: Region::US_EAST_VA,
        tier: OrganizationTier::Free,
        admin: UserId::new(1),
    }
}

#[test]
fn test_create_organization_saga_new() {
    let saga =
        CreateOrganizationSaga::new(SagaId::new("org-saga-1"), make_create_organization_input());
    assert_eq!(saga.id, "org-saga-1");
    assert_eq!(saga.state, CreateOrganizationSagaState::Pending);
    assert_eq!(saga.retries, 0);
    assert!(saga.next_retry_at.is_none());
    assert!(!saga.is_terminal());
    assert!(saga.is_ready_for_retry());
}

#[test]
fn test_create_organization_saga_transitions() {
    let input = CreateOrganizationInput {
        slug: OrganizationSlug::new(9999),
        region: Region::US_EAST_VA,
        tier: OrganizationTier::Pro,
        admin: UserId::new(1),
    };
    let mut saga = CreateOrganizationSaga::new(SagaId::new("org-saga-2"), input);

    let org_id = OrganizationId::new(42);
    let org_slug = OrganizationSlug::new(9999);

    // Pending → DirectoryCreated
    saga.transition(CreateOrganizationSagaState::DirectoryCreated {
        organization_id: org_id,
        organization_slug: org_slug,
    });
    assert_eq!(saga.current_step(), 1);
    assert!(!saga.is_terminal());

    // DirectoryCreated → ProfileWritten
    saga.transition(CreateOrganizationSagaState::ProfileWritten {
        organization_id: org_id,
        organization_slug: org_slug,
    });
    assert_eq!(saga.current_step(), 2);
    assert!(!saga.is_terminal());

    // ProfileWritten → Completed
    saga.transition(CreateOrganizationSagaState::Completed {
        organization_id: org_id,
        organization_slug: org_slug,
    });
    assert!(saga.is_terminal());
}

#[test]
fn test_create_organization_saga_target_region() {
    let input = CreateOrganizationInput {
        slug: OrganizationSlug::new(8888),
        region: Region::IE_EAST_DUBLIN,
        tier: OrganizationTier::Free,
        admin: UserId::new(1),
    };
    let mut saga = CreateOrganizationSaga::new(SagaId::new("org-saga-3"), input);

    // Step 0 targets GLOBAL
    assert_eq!(saga.target_region(), Region::GLOBAL);

    // Step 1 targets regional
    saga.transition(CreateOrganizationSagaState::DirectoryCreated {
        organization_id: OrganizationId::new(1),
        organization_slug: OrganizationSlug::new(1),
    });
    assert_eq!(saga.target_region(), Region::IE_EAST_DUBLIN);

    // Step 2 targets GLOBAL
    saga.transition(CreateOrganizationSagaState::ProfileWritten {
        organization_id: OrganizationId::new(1),
        organization_slug: OrganizationSlug::new(1),
    });
    assert_eq!(saga.target_region(), Region::GLOBAL);
}

#[test]
fn test_create_organization_saga_is_terminal() {
    let base =
        CreateOrganizationSaga::new(SagaId::new("org-saga-4"), make_create_organization_input());

    let mut s = base.clone();
    s.state = CreateOrganizationSagaState::Completed {
        organization_id: OrganizationId::new(1),
        organization_slug: OrganizationSlug::new(1),
    };
    assert!(s.is_terminal());

    let mut s = base.clone();
    s.state = CreateOrganizationSagaState::Failed { step: 1, error: "err".to_string() };
    assert!(s.is_terminal());

    let mut s = base.clone();
    s.state = CreateOrganizationSagaState::Compensated {
        step: 0,
        cleanup_summary: "cleaned".to_string(),
    };
    assert!(s.is_terminal());

    let mut s = base.clone();
    s.state = CreateOrganizationSagaState::TimedOut;
    assert!(s.is_terminal());

    // Non-terminal
    let mut s = base;
    s.state = CreateOrganizationSagaState::DirectoryCreated {
        organization_id: OrganizationId::new(1),
        organization_slug: OrganizationSlug::new(1),
    };
    assert!(!s.is_terminal());
}

#[test]
fn test_create_organization_saga_fail_and_retry() {
    let mut saga =
        CreateOrganizationSaga::new(SagaId::new("org-saga-5"), make_create_organization_input());

    for _ in 0..MAX_RETRIES.saturating_sub(1) {
        saga.fail(0, "transient".to_string());
        assert!(!saga.is_terminal());
        assert!(saga.next_retry_at.is_some());
    }

    saga.fail(0, "permanent".to_string());
    assert!(saga.is_terminal());
    assert!(matches!(
        saga.state,
        CreateOrganizationSagaState::Failed { step: 0, ref error } if error == "permanent"
    ));
}

#[test]
fn test_create_organization_saga_is_timed_out() {
    let mut saga =
        CreateOrganizationSaga::new(SagaId::new("org-saga-6"), make_create_organization_input());
    saga.created_at = Utc::now() - chrono::Duration::minutes(5);

    assert!(saga.is_timed_out(Duration::from_secs(60))); // 1 min timeout
    assert!(!saga.is_timed_out(Duration::from_secs(600))); // 10 min timeout
}

#[test]
fn test_create_organization_saga_current_step() {
    let mut saga =
        CreateOrganizationSaga::new(SagaId::new("org-saga-7"), make_create_organization_input());

    assert_eq!(saga.current_step(), 0); // Pending

    saga.state = CreateOrganizationSagaState::DirectoryCreated {
        organization_id: OrganizationId::new(1),
        organization_slug: OrganizationSlug::new(1),
    };
    assert_eq!(saga.current_step(), 1);

    saga.state = CreateOrganizationSagaState::ProfileWritten {
        organization_id: OrganizationId::new(1),
        organization_slug: OrganizationSlug::new(1),
    };
    assert_eq!(saga.current_step(), 2);

    // Terminal states return 0
    saga.state = CreateOrganizationSagaState::Completed {
        organization_id: OrganizationId::new(1),
        organization_slug: OrganizationSlug::new(1),
    };
    assert_eq!(saga.current_step(), 0);

    saga.state = CreateOrganizationSagaState::Failed { step: 1, error: "err".to_string() };
    assert_eq!(saga.current_step(), 0);

    saga.state = CreateOrganizationSagaState::TimedOut;
    assert_eq!(saga.current_step(), 0);
}

#[test]
fn test_create_organization_saga_serialization() {
    let input = CreateOrganizationInput {
        slug: OrganizationSlug::new(777),
        region: Region::US_EAST_VA,
        tier: OrganizationTier::Enterprise,
        admin: UserId::new(7),
    };
    let saga = CreateOrganizationSaga::new(SagaId::new("org-saga-ser"), input);
    let wrapped = Saga::CreateOrganization(saga);

    let json = serde_json::to_string(&wrapped).unwrap();
    let deserialized: Saga = serde_json::from_str(&json).unwrap();

    match deserialized {
        Saga::CreateOrganization(s) => {
            assert_eq!(s.id, "org-saga-ser");
            assert_eq!(s.input.region, Region::US_EAST_VA);
            assert_eq!(s.input.tier, OrganizationTier::Enterprise);
            assert_eq!(s.input.admin, UserId::new(7));
        },
        _ => panic!("wrong saga variant"),
    }
}

#[test]
fn test_create_organization_saga_wrapper() {
    let saga =
        CreateOrganizationSaga::new(SagaId::new("org-saga-wrap"), make_create_organization_input());
    let wrapped = Saga::CreateOrganization(saga);

    assert_eq!(wrapped.id(), "org-saga-wrap");
    assert_eq!(wrapped.saga_type(), SagaType::CreateOrganization);
    assert!(!wrapped.is_terminal());
    assert!(wrapped.is_ready_for_retry());
    assert_eq!(wrapped.retries(), 0);
    assert_eq!(wrapped.current_step(), 0);
}

#[test]
fn test_create_organization_saga_lock_keys() {
    // Pending state has no lock keys (no org ID allocated yet)
    let saga = CreateOrganizationSaga::new(
        SagaId::new("org-saga-lock-1"),
        make_create_organization_input(),
    );
    let wrapped = Saga::CreateOrganization(saga);
    assert!(wrapped.lock_keys().is_empty());

    // DirectoryCreated state locks the organization
    let mut saga = CreateOrganizationSaga::new(
        SagaId::new("org-saga-lock-2"),
        make_create_organization_input(),
    );
    saga.transition(CreateOrganizationSagaState::DirectoryCreated {
        organization_id: OrganizationId::new(42),
        organization_slug: OrganizationSlug::new(9999),
    });
    let wrapped = Saga::CreateOrganization(saga);
    let keys = wrapped.lock_keys();
    assert_eq!(keys.len(), 1);
    assert!(matches!(keys[0], SagaLockKey::Organization(id) if id == OrganizationId::new(42)));

    // ProfileWritten state also locks the organization
    let mut saga = CreateOrganizationSaga::new(
        SagaId::new("org-saga-lock-3"),
        make_create_organization_input(),
    );
    saga.state = CreateOrganizationSagaState::ProfileWritten {
        organization_id: OrganizationId::new(42),
        organization_slug: OrganizationSlug::new(9999),
    };
    let wrapped = Saga::CreateOrganization(saga);
    let keys = wrapped.lock_keys();
    assert_eq!(keys.len(), 1);
    assert!(matches!(keys[0], SagaLockKey::Organization(id) if id == OrganizationId::new(42)));

    // Completed state also locks the organization
    let mut saga = CreateOrganizationSaga::new(
        SagaId::new("org-saga-lock-4"),
        make_create_organization_input(),
    );
    saga.state = CreateOrganizationSagaState::Completed {
        organization_id: OrganizationId::new(42),
        organization_slug: OrganizationSlug::new(9999),
    };
    let wrapped = Saga::CreateOrganization(saga);
    let keys = wrapped.lock_keys();
    assert_eq!(keys.len(), 1);
    assert!(matches!(keys[0], SagaLockKey::Organization(id) if id == OrganizationId::new(42)));

    // Failed state has no lock keys
    let mut saga = CreateOrganizationSaga::new(
        SagaId::new("org-saga-lock-5"),
        make_create_organization_input(),
    );
    saga.state = CreateOrganizationSagaState::Failed { step: 0, error: "err".to_string() };
    let wrapped = Saga::CreateOrganization(saga);
    assert!(wrapped.lock_keys().is_empty());
}

// =========================================================================
// CreateSigningKeySaga tests
// =========================================================================

fn make_create_signing_key_input() -> CreateSigningKeyInput {
    CreateSigningKeyInput { scope: SigningKeyScope::Global }
}

#[test]
fn test_create_signing_key_saga_new() {
    let saga = CreateSigningKeySaga::new(SagaId::new("sk-1"), make_create_signing_key_input());
    assert_eq!(saga.id, "sk-1");
    assert_eq!(saga.state, CreateSigningKeySagaState::Pending);
    assert_eq!(saga.retries, 0);
    assert!(saga.next_retry_at.is_none());
    assert_eq!(saga.input.scope, SigningKeyScope::Global);
}

#[test]
fn test_create_signing_key_saga_full_lifecycle() {
    let mut saga = CreateSigningKeySaga::new(SagaId::new("sk-2"), make_create_signing_key_input());

    // Step 1: key generated
    saga.transition(CreateSigningKeySagaState::KeyGenerated {
        kid: "kid-uuid-1".to_string(),
        public_key_bytes: vec![0x01; 32],
        encrypted_private_key: vec![0x02; 100],
        rmk_version: 1,
    });
    assert!(matches!(saga.state, CreateSigningKeySagaState::KeyGenerated { .. }));
    assert_eq!(saga.current_step(), 1);

    // Step 2: completed
    saga.transition(CreateSigningKeySagaState::Completed { kid: "kid-uuid-1".to_string() });
    assert!(
        matches!(saga.state, CreateSigningKeySagaState::Completed { ref kid } if kid == "kid-uuid-1")
    );
    assert!(saga.is_terminal());
}

#[test]
fn test_create_signing_key_saga_terminal_states() {
    let base = CreateSigningKeySaga::new(SagaId::new("sk-3"), make_create_signing_key_input());

    let mut s = base.clone();
    s.state = CreateSigningKeySagaState::Completed { kid: "k1".to_string() };
    assert!(s.is_terminal());

    let mut s = base.clone();
    s.state = CreateSigningKeySagaState::Failed { step: 0, error: "err".to_string() };
    assert!(s.is_terminal());

    let mut s = base.clone();
    s.state = CreateSigningKeySagaState::TimedOut;
    assert!(s.is_terminal());

    // Non-terminal: Pending
    let s = base.clone();
    assert!(!s.is_terminal());

    // Non-terminal: KeyGenerated
    let mut s = base;
    s.state = CreateSigningKeySagaState::KeyGenerated {
        kid: "k2".to_string(),
        public_key_bytes: vec![],
        encrypted_private_key: vec![],
        rmk_version: 1,
    };
    assert!(!s.is_terminal());
}

#[test]
fn test_create_signing_key_saga_fail_and_retry() {
    let mut saga = CreateSigningKeySaga::new(SagaId::new("sk-4"), make_create_signing_key_input());

    // First failure: stays in Pending, gets retry backoff
    saga.fail(0, "transient error".to_string());
    assert_eq!(saga.retries, 1);
    assert!(!saga.is_terminal());
    assert!(saga.next_retry_at.is_some());

    // Exhaust retries
    for _ in 1..MAX_RETRIES {
        saga.fail(0, "permanent".to_string());
    }
    assert!(matches!(
        saga.state,
        CreateSigningKeySagaState::Failed { step: 0, ref error } if error == "permanent"
    ));
    assert!(saga.is_terminal());
}

#[test]
fn test_create_signing_key_saga_retry_readiness() {
    let mut saga = CreateSigningKeySaga::new(SagaId::new("sk-5"), make_create_signing_key_input());
    assert!(saga.is_ready_for_retry());

    // After failure, has a future retry_at
    saga.fail(0, "err".to_string());
    assert!(!saga.is_ready_for_retry());
}

#[test]
fn test_create_signing_key_saga_is_timed_out() {
    let mut saga =
        CreateSigningKeySaga::new(SagaId::new("sk-timeout"), make_create_signing_key_input());
    saga.created_at = Utc::now() - chrono::Duration::minutes(5);

    assert!(saga.is_timed_out(Duration::from_secs(60))); // 1 min timeout
    assert!(!saga.is_timed_out(Duration::from_secs(600))); // 10 min timeout
}

#[test]
fn test_create_signing_key_saga_current_step() {
    let mut saga = CreateSigningKeySaga::new(SagaId::new("sk-6"), make_create_signing_key_input());
    assert_eq!(saga.current_step(), 0);

    saga.state = CreateSigningKeySagaState::KeyGenerated {
        kid: "k".to_string(),
        public_key_bytes: vec![],
        encrypted_private_key: vec![],
        rmk_version: 1,
    };
    assert_eq!(saga.current_step(), 1);

    saga.state = CreateSigningKeySagaState::Completed { kid: "k".to_string() };
    assert_eq!(saga.current_step(), 0);

    saga.state = CreateSigningKeySagaState::Failed { step: 1, error: "err".to_string() };
    assert_eq!(saga.current_step(), 0);

    saga.state = CreateSigningKeySagaState::TimedOut;
    assert_eq!(saga.current_step(), 0);
}

#[test]
fn test_create_signing_key_saga_serialization() {
    let inner = CreateSigningKeySaga::new(SagaId::new("sk-ser-1"), make_create_signing_key_input());
    let saga = Saga::CreateSigningKey(inner);
    let bytes = saga.to_bytes().unwrap();
    let restored = Saga::from_bytes(&bytes).unwrap();
    assert_eq!(saga.id(), restored.id());
    assert_eq!(saga.saga_type(), SagaType::CreateSigningKey);
    assert_eq!(restored.saga_type(), SagaType::CreateSigningKey);

    let Saga::CreateSigningKey(ref original) = saga else { panic!("expected CreateSigningKey") };
    let Saga::CreateSigningKey(ref deserialized) = restored else {
        panic!("expected CreateSigningKey")
    };
    assert_eq!(original.id, deserialized.id);
    assert_eq!(original.input.scope, deserialized.input.scope);
}

#[test]
fn test_create_signing_key_saga_wrapper_delegation() {
    let inner =
        CreateSigningKeySaga::new(SagaId::new("sk-wrap-1"), make_create_signing_key_input());
    let saga = Saga::CreateSigningKey(inner);

    assert_eq!(saga.id(), "sk-wrap-1");
    assert_eq!(saga.saga_type(), SagaType::CreateSigningKey);
    assert!(!saga.is_terminal());
    assert_eq!(saga.retries(), 0);
    assert_eq!(saga.current_step(), 0);
}

#[test]
fn test_create_signing_key_saga_lock_keys() {
    // Global scope
    let inner = CreateSigningKeySaga::new(
        SagaId::new("sk-lock-1"),
        CreateSigningKeyInput { scope: SigningKeyScope::Global },
    );
    let saga = Saga::CreateSigningKey(inner);
    let keys = saga.lock_keys();
    assert_eq!(keys.len(), 1);
    assert!(matches!(keys[0], SagaLockKey::SigningKeyScope(SigningKeyScope::Global)));

    // Organization scope
    let inner = CreateSigningKeySaga::new(
        SagaId::new("sk-lock-2"),
        CreateSigningKeyInput { scope: SigningKeyScope::Organization(OrganizationId::new(42)) },
    );
    let saga = Saga::CreateSigningKey(inner);
    let keys = saga.lock_keys();
    assert_eq!(keys.len(), 1);
    assert!(matches!(
        keys[0],
        SagaLockKey::SigningKeyScope(SigningKeyScope::Organization(id)) if id == OrganizationId::new(42)
    ));
}

#[test]
fn test_signing_key_scope_lock_key_display() {
    assert_eq!(
        SagaLockKey::SigningKeyScope(SigningKeyScope::Global).to_string(),
        "signing_key:global"
    );
    assert_eq!(
        SagaLockKey::SigningKeyScope(SigningKeyScope::Organization(OrganizationId::new(7)))
            .to_string(),
        "signing_key:org:7"
    );
}

#[test]
fn test_signing_key_scope_lock_key_serde() {
    let keys = vec![
        SagaLockKey::SigningKeyScope(SigningKeyScope::Global),
        SagaLockKey::SigningKeyScope(SigningKeyScope::Organization(OrganizationId::new(42))),
    ];
    for key in keys {
        let json = serde_json::to_vec(&key).unwrap();
        let restored: SagaLockKey = serde_json::from_slice(&json).unwrap();
        assert_eq!(key, restored);
    }
}

#[test]
fn test_create_signing_key_saga_org_scope() {
    let saga = CreateSigningKeySaga::new(
        SagaId::new("sk-org-1"),
        CreateSigningKeyInput { scope: SigningKeyScope::Organization(OrganizationId::new(99)) },
    );
    assert_eq!(saga.input.scope, SigningKeyScope::Organization(OrganizationId::new(99)));

    // Roundtrip through Saga wrapper
    let wrapped = Saga::CreateSigningKey(saga);
    let bytes = wrapped.to_bytes().unwrap();
    let restored = Saga::from_bytes(&bytes).unwrap();
    let Saga::CreateSigningKey(ref s) = restored else { panic!("expected CreateSigningKey") };
    assert_eq!(s.input.scope, SigningKeyScope::Organization(OrganizationId::new(99)));
}
