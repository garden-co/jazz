//! One application's account registry, kept as its current state in two
//! host tables of the app's store file.
//!
//! Nothing is cached: a login is one primary-key read of committed state,
//! and a command reads what it needs, decides and writes in one savepoint
//! that is durable before the command answers. Opening reads nothing, and a
//! second owner of the same file cannot decide from stale state.

use crate::kv::HostSql;
pub(crate) use crate::runtime::accounts::RegistryError;
use crate::runtime::accounts::sql::{CREATE, Sql, storage_error, unavailable};
use jazz::account_registry::{
    self, AccountCommand, AccountCommandResult, AccountError, AccountId, Assignment, Principal,
};
use uuid::Uuid;

pub(crate) struct StoredAccountRegistry {
    storage: HostSql,
}

/// Outcome of an atomic login-or-register decision; creation is reported only
/// after its durable boundary so owners can notify admission observers once.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LoginOrRegisterResult {
    pub(crate) assignment: Assignment,
    pub(crate) created: bool,
}

impl StoredAccountRegistry {
    /// Open the registry in `storage`, creating its tables in a new file.
    pub(crate) fn open(storage: HostSql) -> Result<Self, RegistryError> {
        storage
            .write(|conn| conn.execute_batch(CREATE).map_err(storage_error))
            .map_err(unavailable)?;
        Ok(Self { storage })
    }

    /// Resolve current admission from committed state.
    pub(crate) fn login(&mut self, principal: &Principal) -> Result<Assignment, RegistryError> {
        self.storage
            .read(|conn| Ok(account_registry::login(&mut Sql(conn), principal)))
            .map_err(unavailable)?
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
                    account: AccountId(Uuid::new_v4()),
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

    /// Decide and write in one savepoint: the result is durable when it
    /// returns, and a refused or failed command writes nothing.
    pub(crate) fn execute(
        &mut self,
        command: &AccountCommand,
    ) -> Result<AccountCommandResult, RegistryError> {
        let command = command.clone();
        self.storage
            .write(
                move |conn| match account_registry::apply(&mut Sql(conn), &command) {
                    Err(RegistryError::Unavailable(error)) => Err(storage_error(error)),
                    decided => Ok(decided),
                },
            )
            .map_err(unavailable)?
    }
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
    fn a_second_owner_of_the_same_file_decides_from_committed_state() {
        let dir = tempfile::tempdir().unwrap();
        let storage = crate::runtime::StorageConfig::data_dir(dir.path());
        let open = || {
            StoredAccountRegistry::open(
                crate::runtime::AppStore::open(&storage)
                    .unwrap()
                    .sql()
                    .unwrap(),
            )
            .unwrap()
        };
        let mut first = open();
        let mut second = open();
        let alice = first.login_or_register(&principal("alice")).unwrap();
        assert!(alice.created);
        // The second owner sees Alice's assignment instead of registering
        // her again, and its own write is visible to the first.
        assert_eq!(
            second.login_or_register(&principal("alice")).unwrap(),
            LoginOrRegisterResult {
                assignment: alice.assignment,
                created: false
            }
        );
        let bob = second.login_or_register(&principal("bob")).unwrap();
        assert_eq!(first.login(&principal("bob")).unwrap(), bob.assignment);
    }

    #[test]
    fn a_failed_lookup_read_leaves_the_registry_available() {
        let store = crate::runtime::AppStore::memory().unwrap();
        let mut registry = StoredAccountRegistry::open(store.sql().unwrap()).unwrap();
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
    fn a_refused_command_writes_nothing_and_expired_links_are_pruned() {
        let store = crate::runtime::AppStore::memory().unwrap();
        let mut registry = StoredAccountRegistry::open(store.sql().unwrap()).unwrap();
        let alice = principal("alice");
        registry.login_or_register(&alice).unwrap();
        let link = |nonce: u128, now: u64| AccountCommand::RequestLink {
            approver: alice.clone(),
            candidate: principal("bob"),
            nonce: Uuid::from_u128(nonce),
            now,
            expires_at: now + 10,
        };
        registry.execute(&link(1, 0)).unwrap();
        assert!(matches!(
            registry.execute(&link(1, 5)),
            Err(RegistryError::Decision(AccountError::IdentifierCollision))
        ));
        let links = || {
            store
                .sql()
                .unwrap()
                .read(|conn| {
                    conn.query_row("SELECT count(*) FROM _jazz_host_account_links", [], |row| {
                        row.get::<_, i64>(0)
                    })
                    .map_err(storage_error)
                })
                .unwrap()
        };
        assert_eq!(links(), 1);
        // A later request forgets the first, which expired unaccepted.
        registry.execute(&link(2, 20)).unwrap();
        assert_eq!(links(), 1);
        assert!(matches!(
            registry.execute(&AccountCommand::AcceptLink {
                candidate: principal("bob"),
                nonce: Uuid::from_u128(1),
                now: 21,
            }),
            Err(RegistryError::Decision(AccountError::InvalidIntent))
        ));
    }

    #[test]
    fn pruning_keeps_accepted_links_so_a_repeat_accept_answers() {
        let store = crate::runtime::AppStore::memory().unwrap();
        let mut registry = StoredAccountRegistry::open(store.sql().unwrap()).unwrap();
        let alice = principal("alice");
        registry.login_or_register(&alice).unwrap();
        let request = |nonce: u128, candidate: &str, now: u64| AccountCommand::RequestLink {
            approver: alice.clone(),
            candidate: principal(candidate),
            nonce: Uuid::from_u128(nonce),
            now,
            expires_at: now + 10,
        };
        let accept = AccountCommand::AcceptLink {
            candidate: principal("bob"),
            nonce: Uuid::from_u128(1),
            now: 5,
        };
        registry.execute(&request(1, "bob", 0)).unwrap();
        let linked = registry.execute(&accept).unwrap();
        // Long after the link expired, a request prunes expired links.
        registry.execute(&request(2, "carol", 100)).unwrap();
        assert_eq!(registry.execute(&accept).unwrap(), linked);
    }
}
