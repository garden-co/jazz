//! Durable ordered command journal for one application registry.
use super::{
    AccountCommand, AccountCommandResult, AccountError, AccountRegistry, Assignment, Principal,
    codec,
};
use crate::groove::storage::{OrderedKvStorage, RecordStore, ScanBounds};

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

/// Outcome of an atomic login-or-register decision; creation is reported only
/// after its durable boundary so owners can notify admission observers once.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoginOrRegisterResult {
    /// Current active assignment.
    pub assignment: Assignment,
    /// Whether this decision created the assignment.
    pub created: bool,
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
            let descriptor = codec::descriptor();
            let records = RecordStore::new(&storage, CF, &descriptor);
            let mut scan = records
                .scan(ScanBounds::Prefix(b"account-command:".to_vec()))
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
        let descriptor = codec::descriptor();
        let records = RecordStore::new(&self.storage, CF, &descriptor);
        let advanced = records
            .get(&command_key(self.revision))
            .await
            .map_err(unavailable)?;
        if advanced.is_some() {
            return Err(unavailable("account authority advanced elsewhere"));
        }
        self.poisoned = false;
        Ok(self.state.login(principal)?.clone())
    }

    /// Atomically resolve an external identity or durably create its first assignment.
    /// The exclusive mutable owner keeps lookup and append in one decision order.
    /// Only an unassigned identity may register; revocation is never undone.
    pub async fn login_or_register(
        &mut self,
        principal: &Principal,
    ) -> Result<LoginOrRegisterResult, RegistryError> {
        principal.validate(false)?;
        match self.login(principal).await {
            Ok(assignment) => Ok(LoginOrRegisterResult {
                assignment,
                created: false,
            }),
            Err(RegistryError::Decision(AccountError::NotAssigned)) => {
                let result = self
                    .execute(&AccountCommand::Register {
                        principal: principal.clone(),
                        account: super::AccountId(uuid::Uuid::new_v4()),
                    })
                    .await?;
                let AccountCommandResult::Assignment(assignment) = result else {
                    unreachable!("registration returns an assignment")
                };
                Ok(LoginOrRegisterResult {
                    assignment,
                    created: true,
                })
            }
            Err(error) => Err(error),
        }
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
        let descriptor = codec::descriptor();
        let records = RecordStore::new(&self.storage, CF, &descriptor);
        let existing = records
            .put_if_absent(&command_key(self.revision), &bytes)
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

    // Fault injection is internal because HTTP cannot force a durable boundary
    // error after a successful journal append. All decisions use the public API.
    #[derive(Clone)]
    struct FailFlush {
        inner: MemoryStorage,
        fail: std::rc::Rc<std::cell::Cell<bool>>,
    }
    impl OrderedKvStorage for FailFlush {
        fn get(
            &self,
            cf: String,
            key: Vec<u8>,
        ) -> crate::groove::storage::StorageFuture<
            '_,
            Result<Option<Vec<u8>>, crate::groove::storage::Error>,
        > {
            self.inner.get(cf, key)
        }
        fn put_if_absent(
            &self,
            cf: String,
            key: Vec<u8>,
            value: Vec<u8>,
        ) -> crate::groove::storage::StorageFuture<
            '_,
            Result<Option<Vec<u8>>, crate::groove::storage::Error>,
        > {
            self.inner.put_if_absent(cf, key, value)
        }
        fn compare_and_delete(
            &self,
            cf: String,
            key: Vec<u8>,
            expected: Vec<u8>,
        ) -> crate::groove::storage::StorageFuture<'_, Result<bool, crate::groove::storage::Error>>
        {
            self.inner.compare_and_delete(cf, key, expected)
        }
        fn set(
            &self,
            cf: String,
            key: Vec<u8>,
            value: Vec<u8>,
        ) -> crate::groove::storage::StorageFuture<'_, Result<(), crate::groove::storage::Error>>
        {
            self.inner.set(cf, key, value)
        }
        fn delete(
            &self,
            cf: String,
            key: Vec<u8>,
        ) -> crate::groove::storage::StorageFuture<'_, Result<(), crate::groove::storage::Error>>
        {
            self.inner.delete(cf, key)
        }
        fn scan(
            &self,
            request: crate::groove::storage::ScanRequest,
        ) -> crate::groove::storage::StorageFuture<
            '_,
            Result<crate::groove::storage::StorageScan<'_>, crate::groove::storage::Error>,
        > {
            self.inner.scan(request)
        }
        fn write_many(
            &self,
            operations: Vec<crate::groove::storage::OwnedWriteOperation>,
        ) -> crate::groove::storage::StorageFuture<'_, Result<(), crate::groove::storage::Error>>
        {
            self.inner.write_many(operations)
        }
        fn flush_write_boundary(
            &self,
        ) -> crate::groove::storage::StorageFuture<'_, Result<(), crate::groove::storage::Error>>
        {
            Box::pin(async move {
                if self.fail.replace(false) {
                    Err(crate::groove::storage::Error::InvalidStorageLayout(
                        "injected flush failure".into(),
                    ))
                } else {
                    Ok(())
                }
            })
        }
    }

    #[test]
    fn login_or_register_flush_failure_requires_recovery_before_admission() {
        crate::db::block_on(async {
            let storage = FailFlush {
                inner: MemoryStorage::new(&[CF]).unwrap(),
                fail: Default::default(),
            };
            let mut registry = StoredAccountRegistry::open(storage.clone()).await.unwrap();
            let principal = Principal {
                issuer: "https://issuer.example".into(),
                subject: "user".into(),
            };
            storage.fail.set(true);
            assert!(matches!(
                registry.login_or_register(&principal).await,
                Err(RegistryError::Unavailable(_))
            ));
            assert!(matches!(
                registry.login(&principal).await,
                Err(RegistryError::Unavailable(_))
            ));
            assert!(matches!(
                registry.login_or_register(&principal).await,
                Err(RegistryError::Unavailable(_))
            ));
            drop(registry);
            let mut recovered = StoredAccountRegistry::open(storage).await.unwrap();
            let committed = recovered.login(&principal).await.unwrap();
            assert_eq!(
                recovered
                    .login_or_register(&principal)
                    .await
                    .unwrap()
                    .assignment,
                committed
            );
        });
    }

    // Restart, revision fencing and reserved identities are registry-boundary
    // behavior: this uses the public durable API and real storage adapter.
    #[test]
    fn login_or_register_recovery_preserves_assignment_and_denials() {
        crate::db::block_on(async {
            let storage = MemoryStorage::new(&[CF]).unwrap();
            let mut registry = StoredAccountRegistry::open(storage.clone()).await.unwrap();
            let principal = Principal {
                issuer: "https://issuer.example".into(),
                subject: "user".into(),
            };
            for invalid in [
                Principal {
                    issuer: "".into(),
                    subject: "user".into(),
                },
                Principal {
                    issuer: "urn:jazz:system".into(),
                    subject: Uuid::new_v4().to_string(),
                },
                Principal {
                    issuer: crate::tools::identity::LOCAL_FIRST_ISSUER.into(),
                    subject: Uuid::new_v4().to_string(),
                },
            ] {
                assert!(matches!(
                    registry.login_or_register(&invalid).await,
                    Err(RegistryError::Decision(AccountError::InvalidPrincipal))
                ));
            }
            let created = registry.login_or_register(&principal).await.unwrap();
            assert!(created.created);
            let first = created.assignment;
            assert_eq!(
                registry
                    .login_or_register(&principal)
                    .await
                    .unwrap()
                    .assignment,
                first
            );
            drop(registry);
            let mut reopened = StoredAccountRegistry::open(storage.clone()).await.unwrap();
            assert_eq!(
                reopened
                    .login_or_register(&principal)
                    .await
                    .unwrap()
                    .assignment,
                first
            );
            // A distinct exact issuer is a different principal, even with the same subject.
            let other = Principal {
                issuer: "https://other.example".into(),
                ..principal.clone()
            };
            assert_ne!(
                reopened
                    .login_or_register(&other)
                    .await
                    .unwrap()
                    .assignment
                    .account,
                first.account
            );
            reopened
                .execute(&AccountCommand::Revoke {
                    approver: principal.clone(),
                    target: principal.clone(),
                })
                .await
                .unwrap();
            drop(reopened);
            let mut reopened = StoredAccountRegistry::open(storage.clone()).await.unwrap();
            assert!(matches!(
                reopened.login_or_register(&principal).await,
                Err(RegistryError::Decision(AccountError::NotAuthorized))
            ));
            assert!(matches!(
                reopened
                    .execute(&AccountCommand::Register {
                        principal: principal.clone(),
                        account: AccountId(Uuid::new_v4())
                    })
                    .await,
                Err(RegistryError::Decision(AccountError::AlreadyAssigned))
            ));
            let mut competing = StoredAccountRegistry::open(storage).await.unwrap();
            let next = Principal {
                subject: "next".into(),
                ..other.clone()
            };
            reopened.login_or_register(&next).await.unwrap();
            assert!(matches!(
                competing.login_or_register(&next).await,
                Err(RegistryError::Unavailable(_))
            ));
            // Once fenced, it must not expose any cached successful assignment.
            assert!(matches!(
                competing.login_or_register(&other).await,
                Err(RegistryError::Unavailable(_))
            ));
        });
    }

    // Journal corruption cannot be constructed through the public command API.
    #[test]
    fn recovery_rejects_old_versions_gaps_and_corrupt_records() {
        crate::db::block_on(async {
            let command = AccountCommand::Register {
                principal: Principal {
                    issuer: "i".into(),
                    subject: "s".into(),
                },
                account: AccountId(Uuid::from_u128(1)),
            };
            for (key, bytes) in [
                (
                    [b"account-command:v1:".as_slice(), &0u64.to_be_bytes()].concat(),
                    b"JACC\x01\x00".to_vec(),
                ),
                (
                    [b"account-command:v2:".as_slice(), &0u64.to_be_bytes()].concat(),
                    codec::encode(&command).unwrap(),
                ),
                (command_key(1), codec::encode(&command).unwrap()),
                (command_key(0), vec![0]),
                (
                    [b"account-command:v3:".as_slice(), &0u64.to_be_bytes()].concat(),
                    codec::encode(&command).unwrap(),
                ),
            ] {
                let storage = MemoryStorage::new(&[CF]).unwrap();
                storage.put_if_absent(CF.into(), key, bytes).await.unwrap();
                assert!(matches!(
                    StoredAccountRegistry::open(storage).await,
                    Err(RegistryError::Unavailable(_))
                ));
            }
        });
    }

    #[test]
    fn recovery_retains_link_nonce_consumption_after_revocation() {
        crate::db::block_on(async {
            let storage = MemoryStorage::new(&[CF]).unwrap();
            let mut registry = StoredAccountRegistry::open(storage.clone()).await.unwrap();
            let approver = Principal {
                issuer: "i".into(),
                subject: "a".into(),
            };
            let candidate = Principal {
                issuer: "i".into(),
                subject: "b".into(),
            };
            let nonce = Uuid::from_u128(2);
            registry
                .execute(&AccountCommand::Register {
                    principal: approver.clone(),
                    account: AccountId(Uuid::from_u128(1)),
                })
                .await
                .unwrap();
            registry
                .execute(&AccountCommand::RequestLink {
                    approver: approver.clone(),
                    candidate: candidate.clone(),
                    nonce,
                    now: 10,
                    expires_at: 20,
                })
                .await
                .unwrap();
            let accept = AccountCommand::AcceptLink {
                candidate: candidate.clone(),
                nonce,
                now: 11,
            };
            registry.execute(&accept).await.unwrap();
            registry
                .execute(&AccountCommand::Revoke {
                    approver,
                    target: candidate.clone(),
                })
                .await
                .unwrap();
            let mut reopened = StoredAccountRegistry::open(storage).await.unwrap();
            assert!(matches!(
                reopened.login(&candidate).await,
                Err(RegistryError::Decision(AccountError::NotAuthorized))
            ));
            assert!(reopened.execute(&accept).await.is_err());
            assert!(matches!(
                reopened.login(&candidate).await,
                Err(RegistryError::Decision(AccountError::NotAuthorized))
            ));
        });
    }

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
