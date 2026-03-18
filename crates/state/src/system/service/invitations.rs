//! Organization invitation operations (REGIONAL records).

use chrono::{DateTime, Utc};
use inferadb_ledger_store::StorageBackend;
use inferadb_ledger_types::{
    InvitationStatus, InviteId, Operation, OrganizationId, OrganizationInvitation, decode, encode,
};
use snafu::ResultExt;
use tracing::warn;

use super::{
    CodecSnafu, KeyTier, Result, SYSTEM_VAULT_ID, StateSnafu, SystemError, SystemKeys,
    SystemOrganizationService, require_tier,
};

/// Maximum number of invitations returned by `list_invitations_by_org`.
const MAX_LIST_INVITATIONS: usize = 10_000;

impl<B: StorageBackend> SystemOrganizationService<B> {
    // ========================================================================
    // Organization Invitations (REGIONAL records)
    // ========================================================================

    /// Writes a full invitation record to REGIONAL storage (upsert).
    ///
    /// Overwrites any existing record with the same key. The caller is
    /// responsible for generating all fields (ID, slug, token hash,
    /// timestamps). This method performs only the REGIONAL write — GLOBAL index
    /// writes are handled by the Raft apply handler for `CreateOrganizationInvite`.
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::Codec`] if serialization fails, or
    /// [`SystemError::State`] if the write fails.
    pub fn create_invitation(&self, invitation: &OrganizationInvitation) -> Result<()> {
        let key = SystemKeys::invite_key(invitation.organization, invitation.id);
        require_tier(&key, KeyTier::Regional)?;
        let value = encode(invitation).context(CodecSnafu)?;

        let ops = vec![Operation::SetEntity { key, value, condition: None, expires_at: None }];
        self.state.apply_operations(SYSTEM_VAULT_ID, &ops, 0).context(StateSnafu)?;
        Ok(())
    }

    /// Reads an invitation record by organization ID and invite ID.
    ///
    /// Returns `None` if no invitation exists for the given key.
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::Codec`] if deserialization fails, or
    /// [`SystemError::State`] if the read fails.
    pub fn read_invitation(
        &self,
        organization: OrganizationId,
        invite: InviteId,
    ) -> Result<Option<OrganizationInvitation>> {
        let key = SystemKeys::invite_key(organization, invite);
        let entity_opt =
            self.state.get_entity(SYSTEM_VAULT_ID, key.as_bytes()).context(StateSnafu)?;
        match entity_opt {
            Some(entity) => {
                let record: OrganizationInvitation = decode(&entity.value).context(CodecSnafu)?;
                Ok(Some(record))
            },
            None => Ok(None),
        }
    }

    /// Transitions an invitation from Pending to a terminal state.
    ///
    /// Performs a compare-and-swap: reads the current record, verifies it is
    /// still `Pending`, applies the new status and `resolved_at` timestamp,
    /// then writes back. Returns the updated record on success.
    ///
    /// Returns [`SystemError::NotFound`] if the invitation doesn't exist.
    /// Returns [`SystemError::FailedPrecondition`] if the invitation is no
    /// longer Pending (already resolved).
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::Codec`] on serialization failure, or
    /// [`SystemError::State`] on storage failure.
    pub fn update_invitation_status(
        &self,
        organization: OrganizationId,
        invite: InviteId,
        new_status: InvitationStatus,
        resolved_at: DateTime<Utc>,
    ) -> Result<OrganizationInvitation> {
        if !new_status.is_terminal() {
            return Err(SystemError::FailedPrecondition {
                message: format!("target status must be terminal, got {new_status}"),
            });
        }

        let key = SystemKeys::invite_key(organization, invite);
        require_tier(&key, KeyTier::Regional)?;

        let entity = self
            .state
            .get_entity(SYSTEM_VAULT_ID, key.as_bytes())
            .context(StateSnafu)?
            .ok_or_else(|| SystemError::NotFound {
                entity: format!("invite:{}:{}", organization.value(), invite.value()),
            })?;

        let mut record: OrganizationInvitation = decode(&entity.value).context(CodecSnafu)?;
        if record.status != InvitationStatus::Pending {
            return Err(SystemError::FailedPrecondition {
                message: format!(
                    "invitation {} is already {} (expected pending)",
                    invite.value(),
                    record.status,
                ),
            });
        }

        record.status = new_status;
        record.resolved_at = Some(resolved_at);

        let value = encode(&record).context(CodecSnafu)?;
        let ops = vec![Operation::SetEntity { key, value, condition: None, expires_at: None }];
        self.state.apply_operations(SYSTEM_VAULT_ID, &ops, 0).context(StateSnafu)?;

        Ok(record)
    }

    /// Updates the `invitee_email_hmac` field in an invitation record.
    ///
    /// Used during blinding key rotation to rehash REGIONAL invitation records
    /// so that acceptance HMAC comparison works with the new key. Only updates
    /// `Pending` invitations — terminal invitations are not rehashed.
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::NotFound`] if the invitation doesn't exist.
    /// Returns [`SystemError::FailedPrecondition`] if the invitation is
    /// no longer Pending.
    /// Returns [`SystemError::Codec`] or [`SystemError::State`] on failures.
    pub fn update_invitation_email_hmac(
        &self,
        organization: OrganizationId,
        invite: InviteId,
        new_hmac: String,
    ) -> Result<()> {
        let key = SystemKeys::invite_key(organization, invite);
        require_tier(&key, KeyTier::Regional)?;

        let entity = self
            .state
            .get_entity(SYSTEM_VAULT_ID, key.as_bytes())
            .context(StateSnafu)?
            .ok_or_else(|| SystemError::NotFound {
                entity: format!("invite:{}:{}", organization.value(), invite.value()),
            })?;

        let mut record: OrganizationInvitation = decode(&entity.value).context(CodecSnafu)?;
        if record.status != InvitationStatus::Pending {
            return Err(SystemError::FailedPrecondition {
                message: format!(
                    "invitation {} is {} (only pending invitations are rehashed)",
                    invite.value(),
                    record.status,
                ),
            });
        }

        record.invitee_email_hmac = new_hmac;

        let value = encode(&record).context(CodecSnafu)?;
        let ops = vec![Operation::SetEntity { key, value, condition: None, expires_at: None }];
        self.state.apply_operations(SYSTEM_VAULT_ID, &ops, 0).context(StateSnafu)?;

        Ok(())
    }

    /// Deletes an invitation record from REGIONAL storage.
    ///
    /// No-op if the record doesn't exist. Used by the retention reaper to
    /// clean up terminal invitations past the 90-day retention window.
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::State`] if the delete fails.
    pub fn delete_invitation(&self, organization: OrganizationId, invite: InviteId) -> Result<()> {
        let key = SystemKeys::invite_key(organization, invite);
        require_tier(&key, KeyTier::Regional)?;
        let ops = vec![Operation::DeleteEntity { key }];
        self.state.apply_operations(SYSTEM_VAULT_ID, &ops, 0).context(StateSnafu)?;
        Ok(())
    }

    /// Lists all invitations for an organization via prefix scan.
    ///
    /// Scans `invite:{org_id}:` in REGIONAL storage. Results are unordered
    /// (bucket-hashed key scattering) — callers must sort if needed.
    ///
    /// **Note**: `start_after` is unreliable for prefix-scan pagination
    /// because keys are scattered across buckets. Use `None` and paginate
    /// at the application level.
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::State`] if the scan fails. Corrupt records
    /// are logged and skipped rather than failing the entire scan.
    pub fn list_invitations_by_org(
        &self,
        organization: OrganizationId,
        start_after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<OrganizationInvitation>> {
        let prefix = SystemKeys::invite_prefix(organization);
        let scan_limit = limit.min(MAX_LIST_INVITATIONS);
        let entities = self
            .state
            .list_entities(SYSTEM_VAULT_ID, Some(&prefix), start_after, scan_limit)
            .context(StateSnafu)?;

        let mut invitations = Vec::new();
        for entity in &entities {
            let key_str = String::from_utf8_lossy(&entity.key);
            if SystemKeys::parse_invite_key(&key_str).is_none() {
                continue;
            }
            match decode::<OrganizationInvitation>(&entity.value) {
                Ok(inv) => invitations.push(inv),
                Err(e) => warn!(error = %e, key = %key_str, "Skipping corrupt invitation entry"),
            }
        }
        Ok(invitations)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use chrono::Utc;
    use inferadb_ledger_types::{
        InvitationStatus, InviteId, InviteSlug, OrganizationId, OrganizationInvitation,
        OrganizationMemberRole, TeamId, UserId,
    };

    use crate::system::service::{SystemError, SystemOrganizationService};

    fn create_test_service() -> SystemOrganizationService<inferadb_ledger_store::InMemoryBackend> {
        crate::system::create_test_service()
    }

    fn make_test_invitation(org: OrganizationId, invite: InviteId) -> OrganizationInvitation {
        OrganizationInvitation {
            id: invite,
            slug: InviteSlug::new(9999),
            organization: org,
            token_hash: [0xAB; 32],
            inviter: UserId::new(1),
            invitee_email_hmac: "hmac_hex".into(),
            invitee_email: "alice@example.com".into(),
            role: OrganizationMemberRole::Member,
            team: Some(TeamId::new(5)),
            status: InvitationStatus::Pending,
            created_at: Utc::now(),
            expires_at: Utc::now(),
            resolved_at: None,
        }
    }

    #[test]
    fn test_create_and_read_invitation() {
        let svc = create_test_service();
        let org = OrganizationId::new(1);
        let invite_id = InviteId::new(1);
        let inv = make_test_invitation(org, invite_id);

        svc.create_invitation(&inv).unwrap();

        let read = svc.read_invitation(org, invite_id).unwrap().unwrap();
        assert_eq!(read.id, invite_id);
        assert_eq!(read.organization, org);
        assert_eq!(read.invitee_email, "alice@example.com");
        assert_eq!(read.status, InvitationStatus::Pending);
        assert!(read.resolved_at.is_none());
    }

    #[test]
    fn test_read_invitation_not_found() {
        let svc = create_test_service();
        let result = svc.read_invitation(OrganizationId::new(1), InviteId::new(99)).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_update_invitation_status_accept() {
        let svc = create_test_service();
        let org = OrganizationId::new(1);
        let invite_id = InviteId::new(1);
        let inv = make_test_invitation(org, invite_id);
        svc.create_invitation(&inv).unwrap();

        let now = Utc::now();
        let updated =
            svc.update_invitation_status(org, invite_id, InvitationStatus::Accepted, now).unwrap();
        assert_eq!(updated.status, InvitationStatus::Accepted);
        assert_eq!(updated.resolved_at.unwrap(), now);

        // Verify persisted
        let read = svc.read_invitation(org, invite_id).unwrap().unwrap();
        assert_eq!(read.status, InvitationStatus::Accepted);
        assert_eq!(read.resolved_at.unwrap(), now);
    }

    #[test]
    fn test_update_invitation_status_decline() {
        let svc = create_test_service();
        let org = OrganizationId::new(1);
        let invite_id = InviteId::new(1);
        svc.create_invitation(&make_test_invitation(org, invite_id)).unwrap();

        let now = Utc::now();
        let updated =
            svc.update_invitation_status(org, invite_id, InvitationStatus::Declined, now).unwrap();
        assert_eq!(updated.status, InvitationStatus::Declined);
    }

    #[test]
    fn test_update_invitation_status_revoke() {
        let svc = create_test_service();
        let org = OrganizationId::new(1);
        let invite_id = InviteId::new(1);
        svc.create_invitation(&make_test_invitation(org, invite_id)).unwrap();

        let now = Utc::now();
        let updated =
            svc.update_invitation_status(org, invite_id, InvitationStatus::Revoked, now).unwrap();
        assert_eq!(updated.status, InvitationStatus::Revoked);
    }

    #[test]
    fn test_update_invitation_status_expire() {
        let svc = create_test_service();
        let org = OrganizationId::new(1);
        let invite_id = InviteId::new(1);
        svc.create_invitation(&make_test_invitation(org, invite_id)).unwrap();

        let now = Utc::now();
        let updated =
            svc.update_invitation_status(org, invite_id, InvitationStatus::Expired, now).unwrap();
        assert_eq!(updated.status, InvitationStatus::Expired);
    }

    #[test]
    fn test_update_invitation_status_cas_rejects_non_pending() {
        let svc = create_test_service();
        let org = OrganizationId::new(1);
        let invite_id = InviteId::new(1);
        svc.create_invitation(&make_test_invitation(org, invite_id)).unwrap();

        // First transition: accept
        let now = Utc::now();
        svc.update_invitation_status(org, invite_id, InvitationStatus::Accepted, now).unwrap();

        // Second transition: attempt to revoke — must fail
        let err = svc
            .update_invitation_status(org, invite_id, InvitationStatus::Revoked, now)
            .unwrap_err();
        assert!(matches!(err, SystemError::FailedPrecondition { .. }));
    }

    #[test]
    fn test_update_invitation_status_rejects_non_terminal() {
        let svc = create_test_service();
        let org = OrganizationId::new(1);
        let invite_id = InviteId::new(1);
        svc.create_invitation(&make_test_invitation(org, invite_id)).unwrap();

        let err = svc
            .update_invitation_status(org, invite_id, InvitationStatus::Pending, Utc::now())
            .unwrap_err();
        assert!(matches!(err, SystemError::FailedPrecondition { .. }));

        // Verify the invitation is still Pending and unmodified
        let read = svc.read_invitation(org, invite_id).unwrap().unwrap();
        assert_eq!(read.status, InvitationStatus::Pending);
        assert!(read.resolved_at.is_none());
    }

    #[test]
    fn test_update_invitation_status_not_found() {
        let svc = create_test_service();
        let now = Utc::now();
        let err = svc
            .update_invitation_status(
                OrganizationId::new(1),
                InviteId::new(99),
                InvitationStatus::Accepted,
                now,
            )
            .unwrap_err();
        assert!(matches!(err, SystemError::NotFound { .. }));
    }

    #[test]
    fn test_delete_invitation() {
        let svc = create_test_service();
        let org = OrganizationId::new(1);
        let invite_id = InviteId::new(1);
        svc.create_invitation(&make_test_invitation(org, invite_id)).unwrap();

        svc.delete_invitation(org, invite_id).unwrap();

        let result = svc.read_invitation(org, invite_id).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_delete_invitation_nonexistent_is_noop() {
        let svc = create_test_service();
        // Should not error when deleting a non-existent record
        svc.delete_invitation(OrganizationId::new(1), InviteId::new(99)).unwrap();
    }

    #[test]
    fn test_list_invitations_by_org() {
        let svc = create_test_service();
        let org = OrganizationId::new(1);

        // Create 3 invitations for org 1
        for i in 1..=3 {
            svc.create_invitation(&make_test_invitation(org, InviteId::new(i))).unwrap();
        }

        let results = svc.list_invitations_by_org(org, None, 100).unwrap();
        assert_eq!(results.len(), 3);

        // Verify all expected IDs are present (order is bucket-dependent, not InviteId-ordered)
        let mut ids: Vec<i64> = results.iter().map(|r| r.id.value()).collect();
        ids.sort();
        assert_eq!(ids, vec![1, 2, 3]);
    }

    #[test]
    fn test_list_invitations_by_org_empty() {
        let svc = create_test_service();
        let results = svc.list_invitations_by_org(OrganizationId::new(1), None, 100).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_list_invitations_by_org_isolation() {
        let svc = create_test_service();
        let org1 = OrganizationId::new(1);
        let org2 = OrganizationId::new(2);

        svc.create_invitation(&make_test_invitation(org1, InviteId::new(1))).unwrap();
        svc.create_invitation(&make_test_invitation(org2, InviteId::new(2))).unwrap();

        let results1 = svc.list_invitations_by_org(org1, None, 100).unwrap();
        let results2 = svc.list_invitations_by_org(org2, None, 100).unwrap();

        assert_eq!(results1.len(), 1);
        assert_eq!(results1[0].organization, org1);
        assert_eq!(results2.len(), 1);
        assert_eq!(results2[0].organization, org2);
    }

    #[test]
    fn test_list_invitations_by_org_limit() {
        let svc = create_test_service();
        let org = OrganizationId::new(1);

        for i in 1..=5 {
            svc.create_invitation(&make_test_invitation(org, InviteId::new(i))).unwrap();
        }

        // Limit restricts result count
        let results = svc.list_invitations_by_org(org, None, 3).unwrap();
        assert_eq!(results.len(), 3);

        // Unlimited returns all
        let all = svc.list_invitations_by_org(org, None, 100).unwrap();
        assert_eq!(all.len(), 5);
    }

    #[test]
    fn test_update_invitation_status_expire_after_accept_rejected() {
        let svc = create_test_service();
        let org = OrganizationId::new(1);
        let invite_id = InviteId::new(1);
        svc.create_invitation(&make_test_invitation(org, invite_id)).unwrap();

        // Accept first
        let now = Utc::now();
        svc.update_invitation_status(org, invite_id, InvitationStatus::Accepted, now).unwrap();

        // Try to expire after accept — must fail (CAS rejects non-Pending)
        let err = svc
            .update_invitation_status(org, invite_id, InvitationStatus::Expired, now)
            .unwrap_err();
        assert!(matches!(err, SystemError::FailedPrecondition { .. }));

        // Verify status is still Accepted
        let read = svc.read_invitation(org, invite_id).unwrap().unwrap();
        assert_eq!(read.status, InvitationStatus::Accepted);
    }

    #[test]
    fn test_update_invitation_status_all_terminal_transitions_rejected() {
        let svc = create_test_service();
        let org = OrganizationId::new(1);
        let now = Utc::now();

        // Test every terminal state rejects further transitions
        let terminal_states = [
            InvitationStatus::Accepted,
            InvitationStatus::Declined,
            InvitationStatus::Expired,
            InvitationStatus::Revoked,
        ];
        let attempt_states = [
            InvitationStatus::Accepted,
            InvitationStatus::Declined,
            InvitationStatus::Expired,
            InvitationStatus::Revoked,
        ];

        for (i, &initial) in terminal_states.iter().enumerate() {
            for &attempt in &attempt_states {
                let invite_id = InviteId::new((i * 4 + 100) as i64);
                svc.create_invitation(&make_test_invitation(org, invite_id)).unwrap();

                // Set initial terminal state
                svc.update_invitation_status(org, invite_id, initial, now).unwrap();

                // Attempt second transition — must fail
                let err = svc.update_invitation_status(org, invite_id, attempt, now).unwrap_err();
                assert!(
                    matches!(err, SystemError::FailedPrecondition { .. }),
                    "transition from {initial:?} to {attempt:?} should be rejected"
                );
            }
        }
    }

    #[test]
    fn test_update_invitation_status_sets_resolved_at() {
        let svc = create_test_service();
        let org = OrganizationId::new(1);
        let invite_id = InviteId::new(1);
        svc.create_invitation(&make_test_invitation(org, invite_id)).unwrap();

        let resolved_time = Utc::now();
        svc.update_invitation_status(org, invite_id, InvitationStatus::Declined, resolved_time)
            .unwrap();

        let read = svc.read_invitation(org, invite_id).unwrap().unwrap();
        assert_eq!(read.status, InvitationStatus::Declined);
        assert_eq!(read.resolved_at, Some(resolved_time));
    }

    #[test]
    fn test_create_invitation_overwrite() {
        let svc = create_test_service();
        let org = OrganizationId::new(1);
        let invite_id = InviteId::new(1);

        let mut inv = make_test_invitation(org, invite_id);
        svc.create_invitation(&inv).unwrap();

        // Overwrite with updated email
        inv.invitee_email = "bob@example.com".into();
        svc.create_invitation(&inv).unwrap();

        let read = svc.read_invitation(org, invite_id).unwrap().unwrap();
        assert_eq!(read.invitee_email, "bob@example.com");
    }
}
