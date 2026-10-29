//! One application's account registry, kept as its current state in two
//! host tables of the app's store file.
//!
//! Nothing is cached: a login is one primary-key read of committed state,
//! and a command reads what it needs, decides and writes in one savepoint
//! that is durable before the command answers. Opening reads nothing, and a
//! second owner of the same file cannot decide from stale state.

use jazz::account_registry::{
    self, AccountCommand, AccountCommandResult, AccountError, AccountId, AccountState, Assignment,
    LinkIntent, Principal,
};
use rusqlite::{Connection, OptionalExtension, params};
use uuid::Uuid;

use crate::kv::HostSql;

/// The tables, created by the registry's first open. Principals keep their
/// exact issuer and subject text; ids are 16 uuid bytes.
const CREATE: &str = "
CREATE TABLE IF NOT EXISTS _jazz_host_account_identities (
    issuer TEXT NOT NULL,
    subject TEXT NOT NULL,
    account BLOB NOT NULL,
    active INTEGER NOT NULL,
    manage_identities INTEGER NOT NULL,
    generation INTEGER NOT NULL,
    PRIMARY KEY (issuer, subject)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS _jazz_host_account_identities_by_account
    ON _jazz_host_account_identities (account);
CREATE TABLE IF NOT EXISTS _jazz_host_account_links (
    nonce BLOB PRIMARY KEY,
    account BLOB NOT NULL,
    approver_issuer TEXT NOT NULL,
    approver_subject TEXT NOT NULL,
    approver_generation INTEGER NOT NULL,
    candidate_issuer TEXT NOT NULL,
    candidate_subject TEXT NOT NULL,
    expires_at INTEGER NOT NULL,
    completed INTEGER NOT NULL
) WITHOUT ROWID;
";

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

/// Registry denial or an unavailable/corrupt durable authority.
#[derive(Debug, thiserror::Error)]
pub(crate) enum RegistryError {
    /// A valid request did not meet registry preconditions.
    #[error(transparent)]
    Decision(#[from] AccountError),
    /// The store could not be read or written; the outcome of a write that
    /// fails this way is unknown until the next read.
    #[error("account registry unavailable: {0}")]
    Unavailable(String),
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

/// The registry state in the store, inside one read or write.
struct Sql<'a>(&'a Connection);

impl AccountState for Sql<'_> {
    type Error = RegistryError;

    fn assignment(&mut self, principal: &Principal) -> Result<Option<Assignment>, RegistryError> {
        self.0
            .prepare_cached(
                "SELECT account, active, manage_identities, generation \
                 FROM _jazz_host_account_identities WHERE issuer = ?1 AND subject = ?2",
            )
            .and_then(|mut statement| {
                statement
                    .query_row(params![principal.issuer, principal.subject], |row| {
                        Ok((
                            row.get::<_, Vec<u8>>(0)?,
                            row.get::<_, bool>(1)?,
                            row.get::<_, bool>(2)?,
                            row.get::<_, i64>(3)?,
                        ))
                    })
                    .optional()
            })
            .map_err(unavailable)?
            .map(|(account, active, manage_identities, generation)| {
                Ok(Assignment {
                    account: AccountId(uuid(&account)?),
                    active,
                    manage_identities,
                    generation: unsigned(generation)?,
                })
            })
            .transpose()
    }

    fn account_assigned(&mut self, account: AccountId) -> Result<bool, RegistryError> {
        self.0
            .prepare_cached(
                "SELECT EXISTS (SELECT 1 FROM _jazz_host_account_identities WHERE account = ?1)",
            )
            .and_then(|mut statement| statement.query_row([account.0.as_bytes()], |row| row.get(0)))
            .map_err(unavailable)
    }

    fn link(&mut self, nonce: Uuid) -> Result<Option<LinkIntent>, RegistryError> {
        self.0
            .prepare_cached(
                "SELECT account, approver_issuer, approver_subject, approver_generation, \
                 candidate_issuer, candidate_subject, expires_at, completed \
                 FROM _jazz_host_account_links WHERE nonce = ?1",
            )
            .and_then(|mut statement| {
                statement
                    .query_row([nonce.as_bytes()], |row| {
                        Ok((
                            row.get::<_, Vec<u8>>(0)?,
                            Principal {
                                issuer: row.get(1)?,
                                subject: row.get(2)?,
                            },
                            row.get::<_, i64>(3)?,
                            Principal {
                                issuer: row.get(4)?,
                                subject: row.get(5)?,
                            },
                            row.get::<_, i64>(6)?,
                            row.get::<_, bool>(7)?,
                        ))
                    })
                    .optional()
            })
            .map_err(unavailable)?
            .map(
                |(account, approver, approver_generation, candidate, expires_at, completed)| {
                    Ok(LinkIntent {
                        account: AccountId(uuid(&account)?),
                        approver,
                        approver_generation: unsigned(approver_generation)?,
                        candidate,
                        expires_at: unsigned(expires_at)?,
                        completed,
                    })
                },
            )
            .transpose()
    }

    fn put_assignment(
        &mut self,
        principal: &Principal,
        assignment: &Assignment,
    ) -> Result<(), RegistryError> {
        self.0
            .prepare_cached(
                "INSERT OR REPLACE INTO _jazz_host_account_identities \
                 (issuer, subject, account, active, manage_identities, generation) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            )
            .and_then(|mut statement| {
                statement.execute(params![
                    principal.issuer,
                    principal.subject,
                    assignment.account.0.as_bytes(),
                    assignment.active,
                    assignment.manage_identities,
                    signed(assignment.generation)?,
                ])
            })
            .map(|_| ())
            .map_err(unavailable)
    }

    fn put_link(&mut self, nonce: Uuid, link: &LinkIntent) -> Result<(), RegistryError> {
        self.0
            .prepare_cached(
                "INSERT OR REPLACE INTO _jazz_host_account_links \
                 (nonce, account, approver_issuer, approver_subject, approver_generation, \
                 candidate_issuer, candidate_subject, expires_at, completed) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            )
            .and_then(|mut statement| {
                statement.execute(params![
                    nonce.as_bytes(),
                    link.account.0.as_bytes(),
                    link.approver.issuer,
                    link.approver.subject,
                    signed(link.approver_generation)?,
                    link.candidate.issuer,
                    link.candidate.subject,
                    signed(link.expires_at)?,
                    link.completed,
                ])
            })
            .map(|_| ())
            .map_err(unavailable)
    }

    fn prune_links(&mut self, now: u64) -> Result<(), RegistryError> {
        self.0
            .prepare_cached(
                "DELETE FROM _jazz_host_account_links WHERE completed = 0 AND expires_at <= ?1",
            )
            .and_then(|mut statement| statement.execute([signed(now)?]))
            .map(|_| ())
            .map_err(unavailable)
    }
}

fn uuid(bytes: &[u8]) -> Result<Uuid, RegistryError> {
    Uuid::from_slice(bytes).map_err(unavailable)
}

/// Counters and Unix seconds are stored as SQLite's signed integers.
fn unsigned(value: i64) -> Result<u64, RegistryError> {
    u64::try_from(value).map_err(unavailable)
}

fn signed(value: u64) -> rusqlite::Result<i64> {
    i64::try_from(value).map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))
}

fn storage_error(error: impl std::fmt::Display) -> jazz::db::Error {
    jazz::db::Error::new(jazz::db::ErrorCode::Storage, error.to_string())
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
}
