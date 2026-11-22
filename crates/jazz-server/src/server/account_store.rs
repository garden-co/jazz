//! One application's account registry, kept as its current state in two
//! host tables of the app's store file ([`crate::portable::accounts`]),
//! over native's [`HostSql`]: lookups on the read connection, commands on
//! the store's thread, durable before they answer.
//!
//! Nothing is cached: a login is one primary-key read of committed state,
//! and a command reads what it needs, decides and writes in one savepoint
//! that is durable before the command answers. Opening reads nothing, and a
//! second owner of the same file cannot decide from stale state.

use jazz::account_registry::{
    AccountCommand, AccountCommandResult, AccountId, Assignment, Principal,
};
use uuid::Uuid;

use crate::kv::HostSql;
use crate::portable::accounts::{self as registry, unavailable};
pub(crate) use crate::portable::accounts::{LoginOrRegisterResult, RegistryError};

pub(crate) struct StoredAccountRegistry {
    storage: HostSql,
}

impl StoredAccountRegistry {
    /// Open the registry in `storage`, creating its tables in a new file.
    pub(crate) fn open(storage: HostSql) -> Result<Self, RegistryError> {
        storage
            .write(|conn| Ok(registry::open(conn)))
            .map_err(unavailable)??;
        Ok(Self { storage })
    }

    /// Resolve current admission from committed state.
    pub(crate) fn login(&mut self, principal: &Principal) -> Result<Assignment, RegistryError> {
        self.storage
            .read(|conn| Ok(registry::login(conn, principal)))
            .map_err(unavailable)?
    }

    /// Atomically resolve an external identity or durably create its first
    /// assignment. Only an unassigned identity may register; revocation is
    /// never undone.
    pub(crate) fn login_or_register(
        &mut self,
        principal: &Principal,
    ) -> Result<LoginOrRegisterResult, RegistryError> {
        let this = std::cell::RefCell::new(self);
        registry::login_or_register_by(
            principal,
            || AccountId(Uuid::new_v4()),
            |principal| this.borrow_mut().login(principal),
            |command| this.borrow_mut().execute(command),
        )
    }

    /// Decide and write in one savepoint: the result is durable when it
    /// returns, and a refused or failed command writes nothing.
    pub(crate) fn execute(
        &mut self,
        command: &AccountCommand,
    ) -> Result<AccountCommandResult, RegistryError> {
        let command = command.clone();
        self.storage
            .write(move |conn| match registry::execute(conn, &command) {
                Err(RegistryError::Unavailable(error)) => Err(registry::storage_error(error)),
                decided => Ok(decided),
            })
            .map_err(unavailable)?
    }

    /// Record that `principal`'s sessions authenticated before `before_ms`
    /// (Unix milliseconds, the app backend's clock) are revoked; see
    /// [`registry::revoke_sessions`]. Returns the mark now in force.
    pub(crate) fn revoke_sessions(
        &mut self,
        principal: &Principal,
        before_ms: u64,
    ) -> Result<u64, RegistryError> {
        let principal = principal.clone();
        let now_ms = unix_now_ms();
        self.storage
            .write(move |conn| {
                registry::revoke_sessions(conn, &principal, before_ms, now_ms)
                    .map_err(registry::storage_error)
            })
            .map_err(unavailable)
    }

    /// The app's backend signing key kept in the store, generated and kept
    /// on first use (see [`crate::portable::backend_signing_key::stored`]).
    pub(crate) fn backend_signing_key(
        &mut self,
    ) -> Result<crate::portable::backend_signing_key::Key, RegistryError> {
        use crate::portable::backend_signing_key as signing;
        self.storage
            .write(
                |conn| match signing::stored(conn, || signing::generate(Uuid::new_v4)) {
                    Err(RegistryError::Unavailable(error)) => Err(registry::storage_error(error)),
                    decided => Ok(decided),
                },
            )
            .map_err(unavailable)?
    }

    /// The latest session revocation mark recorded for `principal`.
    pub(crate) fn sessions_revoked_before(
        &mut self,
        principal: &Principal,
    ) -> Result<Option<u64>, RegistryError> {
        self.storage
            .read(|conn| Ok(registry::sessions_revoked_before(conn, principal)))
            .map_err(unavailable)?
    }
}

fn unix_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
        })
}

/// The registry as core's checks beside the account tables' policies read
/// it (E2EE protocol R5, R6). Core reads it on the store's own connection,
/// inside the write's transaction: a second connection to a shared
/// in-memory database, which has no write-ahead log, is locked out while
/// the store's connection holds a write. The reader kept beside it serves
/// callers that hold no connection.
pub(crate) struct RegistryFounders {
    reader: jazz::host::HostReader,
    app: Uuid,
}

/// The identities of `account` in the registry's tables on `conn`, or none
/// for a store whose registry never opened.
fn identities_of(
    conn: &jazz::storage::Storage,
    account: AccountId,
) -> Result<Vec<(String, String)>, jazz::db::Error> {
    let mut statement = conn
        .prepare_cached(
            "SELECT issuer, subject FROM _jazz_host_account_identities WHERE account = ?1",
        )
        .map_err(registry::storage_error)?;
    let rows = statement
        .query_map([account.0.as_bytes()], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(registry::storage_error)?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(registry::storage_error)
}

/// How many identities of `account` are not device-account identities.
fn external_identities_of(
    conn: &jazz::storage::Storage,
    account: AccountId,
) -> Result<i64, jazz::db::Error> {
    let mut statement = conn
        .prepare_cached(
            "SELECT COUNT(*) FROM _jazz_host_account_identities \
             WHERE account = ?1 AND issuer <> ?2",
        )
        .map_err(registry::storage_error)?;
    statement
        .query_row(
            rusqlite::params![account.0.as_bytes(), jazz::identity::DEVICE_ACCOUNT_ISSUER],
            |row| row.get::<_, i64>(0),
        )
        .map_err(registry::storage_error)
}

impl RegistryFounders {
    pub(crate) fn new(reader: jazz::host::HostReader, app: Uuid) -> Self {
        Self { reader, app }
    }

    /// An account was founded by a device-account identity when one of its
    /// device-account identities derives its id: linking a device-account
    /// identity into another account keeps that account's id.
    fn founded(
        &self,
        account: AccountId,
        identities: Result<Vec<(String, String)>, jazz::db::Error>,
    ) -> Result<Option<bool>, String> {
        // A registry that cannot be read, a missing table included, is an
        // error that refuses the write: never "an account nobody founded",
        // which would let the session's issuer decide.
        let identities =
            identities.map_err(|error| format!("the account registry cannot be read: {error}"))?;
        if identities.is_empty() {
            return Ok(None);
        }
        Ok(Some(identities.iter().any(|(issuer, subject)| {
            issuer == jazz::identity::DEVICE_ACCOUNT_ISSUER
                && jazz::account_registry::device_account_id(self.app, subject) == account
        })))
    }
}

impl jazz::db::AccountRegistryView for RegistryFounders {
    fn founded_by_device_account(&self, account: AccountId) -> Result<Option<bool>, String> {
        self.founded(
            account,
            self.reader.sql(|conn| identities_of(conn, account)),
        )
    }

    fn founded_by_device_account_on(
        &self,
        store: &jazz::storage::Storage,
        account: AccountId,
    ) -> Result<Option<bool>, String> {
        self.founded(account, identities_of(store, account))
    }

    /// An account whose identities are all device-account identities and
    /// none of which derives its id in this app's namespace was founded for
    /// another audience (`found-local-first` and a guest's first admission
    /// may name one). An encrypting app does not serve such accounts: this rule is
    /// the documented one (E2EE protocol R5, R6), and the founding routes
    /// refuse to create them. An account with an external identity is not
    /// told apart from one the app founded, so it reads as the app's.
    fn founded_for_another_audience(&self, account: AccountId) -> Result<bool, String> {
        let Some(founded) = self.founded_by_device_account(account)? else {
            return Ok(false);
        };
        if founded {
            return Ok(false);
        }
        self.reader
            .sql(|conn| external_identities_of(conn, account))
            .map(|external| external == 0)
            .map_err(|error| error.to_string())
    }

    fn founded_for_another_audience_on(
        &self,
        store: &jazz::storage::Storage,
        account: AccountId,
    ) -> Result<bool, String> {
        let Some(founded) = self.founded_by_device_account_on(store, account)? else {
            return Ok(false);
        };
        if founded {
            return Ok(false);
        }
        external_identities_of(store, account)
            .map(|external| external == 0)
            .map_err(|error| error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::portable::accounts::{REVOCATION_MARK_RETENTION_MS, storage_error};
    use jazz::account_registry::AccountError;

    fn principal(subject: &str) -> Principal {
        Principal {
            issuer: "https://issuer.example".into(),
            subject: subject.into(),
        }
    }

    // A session revocation mark never moves back, and recording one forgets
    // marks past their retention, so the table doesn't grow with every
    // sign-out ever made.
    #[test]
    fn session_revocation_marks_move_forward_and_old_ones_are_forgotten() {
        let store = crate::runtime::AppStore::memory().unwrap();
        let mut registry = StoredAccountRegistry::open(store.sql().unwrap()).unwrap();
        let (alice, bob) = (principal("alice"), principal("bob"));
        let now = unix_now_ms();
        assert_eq!(registry.sessions_revoked_before(&alice).unwrap(), None);
        assert_eq!(registry.revoke_sessions(&alice, now).unwrap(), now);
        assert_eq!(registry.revoke_sessions(&alice, now - 1).unwrap(), now);
        assert_eq!(registry.sessions_revoked_before(&alice).unwrap(), Some(now));

        let long_ago = now - REVOCATION_MARK_RETENTION_MS - 1;
        assert_eq!(registry.revoke_sessions(&bob, long_ago).unwrap(), long_ago);
        // The next mark recorded (anyone's) forgets Bob's old one, not Alice's.
        registry.revoke_sessions(&alice, now).unwrap();
        assert_eq!(registry.sessions_revoked_before(&bob).unwrap(), None);
        assert_eq!(registry.sessions_revoked_before(&alice).unwrap(), Some(now));
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

    // Core's account-record checks read who founded an account through
    // the registry's own read connection (E2EE protocol R5, R6). Internal:
    // the view has no public reader of its own to observe it through.
    #[test]
    fn the_founders_view_tells_device_account_founded_accounts() {
        use jazz::db::AccountRegistryView;
        let dir = tempfile::tempdir().unwrap();
        let storage = crate::runtime::StorageConfig::data_dir(dir.path());
        let store = crate::runtime::AppStore::open(&storage).unwrap();
        let mut registry = StoredAccountRegistry::open(store.sql().unwrap()).unwrap();
        let app = Uuid::from_bytes([7; 16]);
        let external = registry
            .login_or_register(&principal("alice"))
            .unwrap()
            .assignment
            .account;
        let founder = Principal {
            issuer: jazz::identity::DEVICE_ACCOUNT_ISSUER.into(),
            subject: Uuid::from_bytes([9; 16]).to_string(),
        };
        let AccountCommandResult::Assignment(founded) = registry
            .execute(&AccountCommand::FoundDeviceAccount {
                principal: founder,
                app,
            })
            .unwrap()
        else {
            panic!("an assignment");
        };
        let view = RegistryFounders::new(
            jazz::host::HostReader::open(
                &dir.path().join(crate::server::identity::STORE_FILE),
                None,
            )
            .unwrap(),
            app,
        );
        assert_eq!(
            view.founded_by_device_account(founded.account),
            Ok(Some(true))
        );
        assert_eq!(view.founded_by_device_account(external), Ok(Some(false)));
        assert_eq!(
            view.founded_by_device_account(AccountId(Uuid::from_bytes([3; 16]))),
            Ok(None)
        );
    }

    /// An account whose identities are all device-account identities and
    /// none of which derives its id in the app's namespace was founded for
    /// another audience; the app's own founders, external accounts and unknown
    /// accounts are not.
    #[test]
    fn an_account_founded_for_another_audience_is_told_apart() {
        use jazz::db::AccountRegistryView;
        let dir = tempfile::tempdir().unwrap();
        let app = Uuid::from_bytes([7; 16]);
        let store =
            crate::runtime::AppStore::open(&crate::runtime::StorageConfig::data_dir(dir.path()))
                .unwrap();
        let mut registry = StoredAccountRegistry::open(store.sql().unwrap()).unwrap();
        let device = |subject: &str| Principal {
            issuer: jazz::identity::DEVICE_ACCOUNT_ISSUER.into(),
            subject: subject.into(),
        };
        let found = |registry: &mut StoredAccountRegistry, subject: &str, app: Uuid| {
            let AccountCommandResult::Assignment(assignment) = registry
                .execute(&AccountCommand::FoundDeviceAccount {
                    principal: device(subject),
                    app,
                })
                .unwrap()
            else {
                panic!("an assignment");
            };
            assignment.account
        };
        let own = found(&mut registry, &Uuid::from_bytes([1; 16]).to_string(), app);
        let other = found(
            &mut registry,
            &Uuid::from_bytes([2; 16]).to_string(),
            Uuid::from_bytes([8; 16]),
        );
        let external = registry
            .login_or_register(&principal("alice"))
            .unwrap()
            .assignment
            .account;
        let view = RegistryFounders::new(
            jazz::host::HostReader::open(
                &dir.path().join(crate::server::identity::STORE_FILE),
                None,
            )
            .unwrap(),
            app,
        );
        assert_eq!(view.founded_for_another_audience(own), Ok(false));
        assert_eq!(view.founded_for_another_audience(other), Ok(true));
        assert_eq!(view.founded_for_another_audience(external), Ok(false));
        assert_eq!(
            view.founded_for_another_audience(AccountId(Uuid::from_bytes([3; 16]))),
            Ok(false)
        );
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
