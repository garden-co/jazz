//! Durable ordered command journal for one application's account registry.

use jazz::account_registry::{
    AccountCommand, AccountCommandResult, AccountError, AccountId, AccountRegistry, Assignment,
    Principal,
};

use crate::kv::HostKv;

const PREFIX: &[u8] = b"account-command:v1:";

/// A single authority owns this journal. Conditional append additionally fails
/// closed if two owners accidentally attempt to advance the same revision.
pub(crate) struct StoredAccountRegistry {
    storage: HostKv,
    state: AccountRegistry,
    revision: u64,
    poisoned: bool,
}

/// Outcome of an atomic login-or-register decision; creation is reported only
/// after its durable boundary so owners can notify admission observers once.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LoginOrRegisterResult {
    pub(crate) assignment: Assignment,
    pub(crate) created: bool,
}

/// Registry denial or an unavailable/corrupt durable authority.
#[derive(Debug, thiserror::Error)]
pub(crate) enum RegistryError {
    /// A valid request did not meet registry preconditions.
    #[error(transparent)]
    Decision(#[from] AccountError),
    /// Admission must stop until the authority is reopened successfully.
    #[error("account registry unavailable: {0}")]
    Unavailable(String),
}

impl StoredAccountRegistry {
    /// Recover committed commands in order, rejecting gaps and unknown codecs.
    /// The supplied store must be exclusive to this application registry.
    pub(crate) fn open(storage: HostKv) -> Result<Self, RegistryError> {
        let mut state = AccountRegistry::default();
        let mut revision = 0u64;
        for (key, bytes) in storage.prefix(b"account-command:").map_err(unavailable)? {
            if key != command_key(revision) {
                return Err(unavailable("invalid account journal sequence"));
            }
            let command: AccountCommand = postcard::from_bytes(&bytes).map_err(unavailable)?;
            state.apply(&command).map_err(unavailable)?;
            revision = revision
                .checked_add(1)
                .ok_or_else(|| unavailable("account revision exhausted"))?;
        }
        Ok(Self {
            storage,
            state,
            revision,
            poisoned: false,
        })
    }

    /// Resolve current admission from the authoritative ordered state.
    pub(crate) fn login(&mut self, principal: &Principal) -> Result<Assignment, RegistryError> {
        self.ensure_available()?;
        // Fence cached admission even if a second owner only performs reads.
        // The successful absence read is this decision's linearization point.
        // A failed read decides nothing and changes nothing: this lookup
        // fails, the next one reads again.
        if self
            .storage
            .get(&command_key(self.revision))
            .map_err(unavailable)?
            .is_some()
        {
            self.poisoned = true;
            return Err(unavailable("account authority advanced elsewhere"));
        }
        Ok(self.state.login(principal)?.clone())
    }

    /// Atomically resolve an external identity or durably create its first
    /// assignment. Only an unassigned identity may register; revocation is
    /// never undone.
    pub(crate) fn login_or_register(
        &mut self,
        principal: &Principal,
    ) -> Result<LoginOrRegisterResult, RegistryError> {
        principal.validate(false)?;
        match self.login(principal) {
            Ok(assignment) => Ok(LoginOrRegisterResult {
                assignment,
                created: false,
            }),
            Err(RegistryError::Decision(AccountError::NotAssigned)) => {
                let result = self.execute(&AccountCommand::Register {
                    principal: principal.clone(),
                    account: AccountId(uuid::Uuid::new_v4()),
                })?;
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
    pub(crate) fn execute(
        &mut self,
        command: &AccountCommand,
    ) -> Result<AccountCommandResult, RegistryError> {
        self.ensure_available()?;
        let mut candidate = self.state.clone();
        let result = candidate.apply(command)?;
        let bytes = postcard::to_allocvec(command).map_err(unavailable)?;
        let next = self
            .revision
            .checked_add(1)
            .ok_or_else(|| unavailable("account revision exhausted"))?;
        self.poisoned = true;
        if self
            .storage
            .put_if_absent(&command_key(self.revision), &bytes)
            .map_err(unavailable)?
            .is_some()
        {
            return Err(unavailable("concurrent account authority detected"));
        }
        self.state = candidate;
        self.revision = next;
        self.poisoned = false;
        Ok(result)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn principal(subject: &str) -> Principal {
        Principal {
            issuer: "https://issuer.example".into(),
            subject: subject.into(),
        }
    }

    #[test]
    fn a_second_owner_of_the_same_journal_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let storage = crate::runtime::StorageConfig::data_dir(dir.path());
        let journal = || {
            crate::runtime::AppStore::open(&storage)
                .unwrap()
                .kv(crate::kv::ACCOUNTS)
                .unwrap()
        };
        let mut first = StoredAccountRegistry::open(journal()).unwrap();
        let mut second = StoredAccountRegistry::open(journal()).unwrap();
        first.login_or_register(&principal("alice")).unwrap();
        assert!(matches!(
            second.login_or_register(&principal("bob")),
            Err(RegistryError::Unavailable(_))
        ));
        // Recovery replays the journal the first owner wrote.
        let mut reopened = StoredAccountRegistry::open(journal()).unwrap();
        assert!(reopened.login(&principal("alice")).is_ok());
    }

    #[test]
    fn a_failed_lookup_read_leaves_the_registry_available() {
        let store = crate::runtime::AppStore::memory().unwrap();
        let mut registry =
            StoredAccountRegistry::open(store.kv(crate::kv::ACCOUNTS).unwrap()).unwrap();
        let assigned = registry
            .login_or_register(&principal("alice"))
            .unwrap()
            .assignment;
        registry.storage.fail_next_read();
        assert!(matches!(
            registry.login(&principal("alice")),
            Err(RegistryError::Unavailable(_))
        ));
        // A read changes nothing: the next lookup reads again.
        assert_eq!(registry.login(&principal("alice")).unwrap(), assigned);
    }

    #[test]
    fn a_journal_with_a_gap_is_rejected() {
        let kv = crate::runtime::AppStore::memory()
            .unwrap()
            .kv(crate::kv::ACCOUNTS)
            .unwrap();
        let command = AccountCommand::Register {
            principal: principal("alice"),
            account: AccountId(uuid::Uuid::from_u128(1)),
        };
        kv.write_many(&[(command_key(1), postcard::to_allocvec(&command).unwrap())])
            .unwrap();
        assert!(StoredAccountRegistry::open(kv).is_err());
    }
}
