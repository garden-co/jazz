//! Durable ordered command journal for one application registry.
use super::{
    AccountCommand, AccountCommandResult, AccountError, AccountRegistry, Assignment, Principal,
    codec,
};
use crate::groove::storage::{OrderedKvStorage, ScanRequest};

const CF: &str = "default";
const PREFIX: &[u8] = b"account-command:v1:";

/// A single authority owns this journal. Conditional append additionally fails
/// closed if two owners accidentally attempt to advance the same revision.
pub struct StoredAccountRegistry<S> {
    storage: S,
    state: AccountRegistry,
    revision: u64,
    poisoned: bool,
}

/// Registry denial or an unavailable/corrupt durable authority.
#[derive(Debug, thiserror::Error)]
pub enum RegistryError {
    /// A valid request did not meet registry preconditions.
    #[error(transparent)]
    Decision(#[from] AccountError),
    /// Admission must stop until the authority is reopened successfully.
    #[error("account registry unavailable: {0}")]
    Unavailable(String),
}

impl<S: OrderedKvStorage> StoredAccountRegistry<S> {
    /// Recover committed commands in order, rejecting gaps and unknown codecs.
    /// The supplied root must be exclusive to this application registry.
    pub async fn open(storage: S) -> Result<Self, RegistryError> {
        let mut state = AccountRegistry::default();
        let mut revision = 0u64;
        {
            let mut scan = storage
                .scan(ScanRequest::prefix(CF.into(), b"account-command:".to_vec()))
                .await
                .map_err(unavailable)?;
            while let Some(batch) = scan.next_batch().await.map_err(unavailable)? {
                for (key, bytes) in batch {
                    if key != command_key(revision) {
                        return Err(unavailable("invalid account journal sequence"));
                    }
                    let command = codec::decode(&bytes).map_err(unavailable)?;
                    state.apply(&command).map_err(unavailable)?;
                    revision = revision
                        .checked_add(1)
                        .ok_or_else(|| unavailable("account revision exhausted"))?;
                }
            }
        }
        // A previous process may have stopped after append and before its
        // explicit durability barrier. Never serve replayed state before it.
        storage.flush_write_boundary().await.map_err(unavailable)?;
        Ok(Self {
            storage,
            state,
            revision,
            poisoned: false,
        })
    }

    /// Resolve current admission from the authoritative ordered state.
    pub async fn login(&mut self, principal: &Principal) -> Result<Assignment, RegistryError> {
        self.ensure_available()?;
        // Fence cached admission even if a second owner only performs reads.
        // The successful absence read is this decision's linearization point.
        self.poisoned = true;
        let advanced = self
            .storage
            .get(CF.into(), command_key(self.revision))
            .await
            .map_err(unavailable)?;
        if advanced.is_some() {
            return Err(unavailable("account authority advanced elsewhere"));
        }
        self.poisoned = false;
        Ok(self.state.login(principal)?.clone())
    }

    /// Persist before publishing. Any ambiguous storage error poisons this
    /// owner: continuing from its older in-memory state could reuse a nonce.
    pub async fn execute(
        &mut self,
        command: &AccountCommand,
    ) -> Result<AccountCommandResult, RegistryError> {
        self.ensure_available()?;
        let mut candidate = self.state.clone();
        let result = candidate.apply(command)?;
        let bytes = codec::encode(command).map_err(unavailable)?;
        let next = self
            .revision
            .checked_add(1)
            .ok_or_else(|| unavailable("account revision exhausted"))?;
        self.poisoned = true;
        let existing = self
            .storage
            .put_if_absent(CF.into(), command_key(self.revision), bytes)
            .await
            .map_err(unavailable)?;
        if existing.is_some() {
            return Err(unavailable("concurrent account authority detected"));
        }
        self.storage
            .flush_write_boundary()
            .await
            .map_err(unavailable)?;
        self.state = candidate;
        self.revision = next;
        self.poisoned = false;
        Ok(result)
    }

    /// Finish storage ownership before reopening its directory.
    pub async fn close(self) -> Result<(), RegistryError> {
        self.storage.close().await.map_err(unavailable)
    }

    fn ensure_available(&self) -> Result<(), RegistryError> {
        if self.poisoned {
            Err(unavailable("authority requires recovery"))
        } else {
            Ok(())
        }
    }
}

fn command_key(revision: u64) -> Vec<u8> {
    let mut key = PREFIX.to_vec();
    key.extend_from_slice(&revision.to_be_bytes());
    key
}
fn unavailable(error: impl std::fmt::Display) -> RegistryError {
    RegistryError::Unavailable(error.to_string())
}

// Journal recovery and conflicting-owner behavior are below the HTTP layer;
// exercise the real ordered memory adapter here, not a mocked persistence API.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::account_registry::AccountId;
    use crate::groove::storage::MemoryStorage;
    use uuid::Uuid;

    #[test]
    fn recovery_retains_revocation_and_competing_owners_fail_closed() {
        crate::db::block_on(async {
            let storage = MemoryStorage::new(&[CF]).unwrap();
            let mut first = StoredAccountRegistry::open(storage.clone()).await.unwrap();
            let mut stale = StoredAccountRegistry::open(storage.clone()).await.unwrap();
            let alice = Principal {
                issuer: "https://issuer.example".into(),
                subject: "alice".into(),
            };
            let register = AccountCommand::Register {
                principal: alice.clone(),
                account: AccountId(Uuid::from_u128(1)),
            };
            first.execute(&register).await.unwrap();
            assert!(matches!(
                stale.execute(&register).await,
                Err(RegistryError::Unavailable(_))
            ));
            assert!(matches!(
                stale.login(&alice).await,
                Err(RegistryError::Unavailable(_))
            ));
            first
                .execute(&AccountCommand::Revoke {
                    approver: alice.clone(),
                    target: alice.clone(),
                })
                .await
                .unwrap();
            let mut reopened = StoredAccountRegistry::open(storage).await.unwrap();
            assert!(matches!(
                reopened.login(&alice).await,
                Err(RegistryError::Decision(AccountError::NotAuthorized))
            ));
            assert!(matches!(
                reopened.execute(&register).await,
                Err(RegistryError::Decision(AccountError::AlreadyAssigned))
            ));
        });
    }
}

#[cfg(test)]
mod stale_admission_tests {
    use super::*;
    use crate::{account_registry::AccountId, groove::storage::MemoryStorage};
    use uuid::Uuid;

    // Unlike the conflicting-write test, this stale owner never mutates.
    #[test]
    fn read_only_stale_owner_cannot_admit_a_revoked_identity() {
        crate::db::block_on(async {
            let storage = MemoryStorage::new(&[CF]).unwrap();
            let mut writer = StoredAccountRegistry::open(storage.clone()).await.unwrap();
            let alice = Principal {
                issuer: "https://issuer.example".into(),
                subject: "alice".into(),
            };
            writer
                .execute(&AccountCommand::Register {
                    principal: alice.clone(),
                    account: AccountId(Uuid::from_u128(1)),
                })
                .await
                .unwrap();
            let mut stale = StoredAccountRegistry::open(storage).await.unwrap();
            assert!(stale.login(&alice).await.is_ok());
            writer
                .execute(&AccountCommand::Revoke {
                    approver: alice.clone(),
                    target: alice.clone(),
                })
                .await
                .unwrap();
            assert!(matches!(
                stale.login(&alice).await,
                Err(RegistryError::Unavailable(_))
            ));
        });
    }
}
