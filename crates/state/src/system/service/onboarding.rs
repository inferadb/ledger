//! Onboarding CRUD operations (REGIONAL store, ephemeral).

use inferadb_ledger_store::StorageBackend;
use inferadb_ledger_types::{Operation, decode, encode};
use snafu::ResultExt;

use super::{
    CodecSnafu, KeyTier, Result, SYSTEM_VAULT_ID, StateSnafu, SystemKeys,
    SystemOrganizationService, require_tier,
};
use crate::system::types::{OnboardingAccount, PendingEmailVerification};

impl<B: StorageBackend> SystemOrganizationService<B> {
    /// Stores or overwrites a pending email verification record.
    pub fn store_email_verification(
        &self,
        email_hmac: &str,
        record: &PendingEmailVerification,
    ) -> Result<()> {
        let key = SystemKeys::onboard_verify_key(email_hmac);
        require_tier(&key, KeyTier::Regional)?;
        let value = encode(record).context(CodecSnafu)?;
        let ops = vec![Operation::SetEntity { key, value, condition: None, expires_at: None }];
        self.state.apply_operations_lazy(SYSTEM_VAULT_ID, &ops, 0).context(StateSnafu)?;
        Ok(())
    }

    /// Reads a pending email verification record by email HMAC.
    pub fn get_email_verification(
        &self,
        email_hmac: &str,
    ) -> Result<Option<PendingEmailVerification>> {
        let key = SystemKeys::onboard_verify_key(email_hmac);
        let entity_opt =
            self.state.get_entity(SYSTEM_VAULT_ID, key.as_bytes()).context(StateSnafu)?;
        match entity_opt {
            Some(entity) => {
                let record: PendingEmailVerification = decode(&entity.value).context(CodecSnafu)?;
                Ok(Some(record))
            },
            None => Ok(None),
        }
    }

    /// Deletes a pending email verification record.
    pub fn delete_email_verification(&self, email_hmac: &str) -> Result<()> {
        let key = SystemKeys::onboard_verify_key(email_hmac);
        let ops = vec![Operation::DeleteEntity { key }];
        self.state.apply_operations_lazy(SYSTEM_VAULT_ID, &ops, 0).context(StateSnafu)?;
        Ok(())
    }

    /// Stores or overwrites an onboarding account record.
    pub fn store_onboarding_account(
        &self,
        email_hmac: &str,
        account: &OnboardingAccount,
    ) -> Result<()> {
        let key = SystemKeys::onboard_account_key(email_hmac);
        require_tier(&key, KeyTier::Regional)?;
        let value = encode(account).context(CodecSnafu)?;
        let ops = vec![Operation::SetEntity { key, value, condition: None, expires_at: None }];
        self.state.apply_operations_lazy(SYSTEM_VAULT_ID, &ops, 0).context(StateSnafu)?;
        Ok(())
    }

    /// Reads an onboarding account record by email HMAC.
    pub fn get_onboarding_account_by_hmac(
        &self,
        email_hmac: &str,
    ) -> Result<Option<OnboardingAccount>> {
        let key = SystemKeys::onboard_account_key(email_hmac);
        let entity_opt =
            self.state.get_entity(SYSTEM_VAULT_ID, key.as_bytes()).context(StateSnafu)?;
        match entity_opt {
            Some(entity) => {
                let account: OnboardingAccount = decode(&entity.value).context(CodecSnafu)?;
                Ok(Some(account))
            },
            None => Ok(None),
        }
    }

    /// Deletes an onboarding account record.
    pub fn delete_onboarding_account(&self, email_hmac: &str) -> Result<()> {
        let key = SystemKeys::onboard_account_key(email_hmac);
        let ops = vec![Operation::DeleteEntity { key }];
        self.state.apply_operations_lazy(SYSTEM_VAULT_ID, &ops, 0).context(StateSnafu)?;
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use chrono::Utc;
    use inferadb_ledger_types::Region;

    use crate::system::{
        service::SystemOrganizationService,
        types::{OnboardingAccount, PendingEmailVerification},
    };

    fn create_test_service() -> SystemOrganizationService<inferadb_ledger_store::InMemoryBackend> {
        crate::system::create_test_service()
    }

    #[test]
    fn test_store_and_get_email_verification() {
        let svc = create_test_service();
        let hmac = "verify_hmac_abc123";
        let now = Utc::now();
        let record = PendingEmailVerification {
            code_hash: [0xAB; 32],
            region: Region::US_EAST_VA,
            expires_at: now,
            attempts: 0,
            rate_limit_count: 1,
            rate_limit_window_start: now,
        };
        svc.store_email_verification(hmac, &record).unwrap();
        assert_eq!(svc.get_email_verification(hmac).unwrap(), Some(record));
    }

    #[test]
    fn test_get_email_verification_not_found() {
        let svc = create_test_service();
        assert_eq!(svc.get_email_verification("nonexistent").unwrap(), None);
    }

    #[test]
    fn test_store_email_verification_overwrites() {
        let svc = create_test_service();
        let hmac = "overwrite_hmac";
        let now = Utc::now();
        let record1 = PendingEmailVerification {
            code_hash: [0x11; 32],
            region: Region::US_EAST_VA,
            expires_at: now,
            attempts: 0,
            rate_limit_count: 1,
            rate_limit_window_start: now,
        };
        svc.store_email_verification(hmac, &record1).unwrap();
        let record2 = PendingEmailVerification {
            code_hash: [0x22; 32],
            region: Region::US_EAST_VA,
            expires_at: now,
            attempts: 0,
            rate_limit_count: 2,
            rate_limit_window_start: now,
        };
        svc.store_email_verification(hmac, &record2).unwrap();
        let result = svc.get_email_verification(hmac).unwrap().unwrap();
        assert_eq!(result.code_hash, [0x22; 32]);
        assert_eq!(result.rate_limit_count, 2);
    }

    #[test]
    fn test_delete_email_verification() {
        let svc = create_test_service();
        let hmac = "delete_verify_hmac";
        let now = Utc::now();
        let record = PendingEmailVerification {
            code_hash: [0xCC; 32],
            region: Region::IE_EAST_DUBLIN,
            expires_at: now,
            attempts: 1,
            rate_limit_count: 1,
            rate_limit_window_start: now,
        };
        svc.store_email_verification(hmac, &record).unwrap();
        svc.delete_email_verification(hmac).unwrap();
        assert_eq!(svc.get_email_verification(hmac).unwrap(), None);
    }

    #[test]
    fn test_delete_email_verification_nonexistent_is_noop() {
        let svc = create_test_service();
        svc.delete_email_verification("does_not_exist").unwrap();
    }

    #[test]
    fn test_store_and_get_onboarding_account() {
        let svc = create_test_service();
        let hmac = "account_hmac_xyz";
        let now = Utc::now();
        let account = OnboardingAccount {
            token_hash: [0xDD; 32],
            region: Region::JP_EAST_TOKYO,
            expires_at: now,
            created_at: now,
        };
        svc.store_onboarding_account(hmac, &account).unwrap();
        assert_eq!(svc.get_onboarding_account_by_hmac(hmac).unwrap(), Some(account));
    }

    #[test]
    fn test_get_onboarding_account_not_found() {
        let svc = create_test_service();
        assert_eq!(svc.get_onboarding_account_by_hmac("nonexistent").unwrap(), None);
    }

    #[test]
    fn test_store_onboarding_account_overwrites() {
        let svc = create_test_service();
        let hmac = "overwrite_account";
        let now = Utc::now();
        let account1 = OnboardingAccount {
            token_hash: [0x11; 32],
            region: Region::US_EAST_VA,
            expires_at: now,
            created_at: now,
        };
        svc.store_onboarding_account(hmac, &account1).unwrap();
        let account2 = OnboardingAccount {
            token_hash: [0x22; 32],
            region: Region::US_EAST_VA,
            expires_at: now,
            created_at: now,
        };
        svc.store_onboarding_account(hmac, &account2).unwrap();
        assert_eq!(
            svc.get_onboarding_account_by_hmac(hmac).unwrap().unwrap().token_hash,
            [0x22; 32]
        );
    }

    #[test]
    fn test_delete_onboarding_account() {
        let svc = create_test_service();
        let hmac = "delete_account_hmac";
        let now = Utc::now();
        let account = OnboardingAccount {
            token_hash: [0xEE; 32],
            region: Region::IE_EAST_DUBLIN,
            expires_at: now,
            created_at: now,
        };
        svc.store_onboarding_account(hmac, &account).unwrap();
        svc.delete_onboarding_account(hmac).unwrap();
        assert_eq!(svc.get_onboarding_account_by_hmac(hmac).unwrap(), None);
    }

    #[test]
    fn test_delete_onboarding_account_nonexistent_is_noop() {
        let svc = create_test_service();
        svc.delete_onboarding_account("does_not_exist").unwrap();
    }

    #[test]
    fn test_verification_and_account_independent() {
        let svc = create_test_service();
        let hmac = "independent_hmac";
        let now = Utc::now();
        let verify = PendingEmailVerification {
            code_hash: [0xAA; 32],
            region: Region::US_EAST_VA,
            expires_at: now,
            attempts: 0,
            rate_limit_count: 1,
            rate_limit_window_start: now,
        };
        let account = OnboardingAccount {
            token_hash: [0xBB; 32],
            region: Region::US_EAST_VA,
            expires_at: now,
            created_at: now,
        };
        svc.store_email_verification(hmac, &verify).unwrap();
        svc.store_onboarding_account(hmac, &account).unwrap();
        assert!(svc.get_email_verification(hmac).unwrap().is_some());
        assert!(svc.get_onboarding_account_by_hmac(hmac).unwrap().is_some());
        svc.delete_email_verification(hmac).unwrap();
        assert_eq!(svc.get_email_verification(hmac).unwrap(), None);
        assert!(svc.get_onboarding_account_by_hmac(hmac).unwrap().is_some());
    }
}
