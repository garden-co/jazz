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
            .write(|conn| Ok(jazz::db::block_on(registry::open(conn))))
            .map_err(unavailable)??;
        Ok(Self { storage })
    }

    /// Resolve current admission from committed state.
    pub(crate) fn login(&mut self, principal: &Principal) -> Result<Assignment, RegistryError> {
        self.storage
            .read(|conn| Ok(jazz::db::block_on(registry::login(conn, principal))))
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
        jazz::db::block_on(registry::login_or_register_by(
            principal,
            || AccountId(Uuid::new_v4()),
            async |principal| this.borrow_mut().login(principal),
            async |command| this.borrow_mut().execute(command),
        ))
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
                move |conn| match jazz::db::block_on(registry::execute(conn, &command)) {
                    Err(RegistryError::Unavailable(error)) => Err(registry::storage_error(error)),
                    decided => Ok(decided),
                },
            )
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
                jazz::db::block_on(registry::revoke_sessions(
                    conn, &principal, before_ms, now_ms,
                ))
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
            .write(|conn| {
                match jazz::db::block_on(signing::stored(conn, || signing::generate(Uuid::new_v4)))
                {
                    Err(RegistryError::Unavailable(error)) => Err(registry::storage_error(error)),
                    decided => Ok(decided),
                }
            })
            .map_err(unavailable)?
    }

    /// The latest session revocation mark recorded for `principal`.
    pub(crate) fn sessions_revoked_before(
        &mut self,
        principal: &Principal,
    ) -> Result<Option<u64>, RegistryError> {
        self.storage
            .read(|conn| {
                Ok(jazz::db::block_on(registry::sessions_revoked_before(
                    conn, principal,
                )))
            })
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::portable::accounts::RegistryFounders;
    use crate::portable::accounts::{REVOCATION_MARK_RETENTION_MS, storage_error};
    use jazz::account_registry::AccountError;
    use jazz::db::AccountRegistryView;
    use jazz::storage::{Backend, Cell, Cursor, SqlResult, Storage, TransactionBody, Value};
    use std::collections::VecDeque;
    use std::future::Future;
    use std::path::Path;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::task::Poll;

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

    // Read the same small SQLite fixture through the host SQL interface, where
    // starting a query and advancing its cursor can both suspend.
    struct SuspendingRegistrySql {
        conn: Mutex<rusqlite::Connection>,
        caller_resumed: Arc<AtomicBool>,
    }

    struct RegistryRows {
        columns: Vec<String>,
        rows: VecDeque<Vec<Value>>,
        current: Option<Vec<Value>>,
        caller_resumed: Arc<AtomicBool>,
    }

    async fn yield_to_caller(caller_resumed: &AtomicBool) {
        // Require the caller to drive the next poll. A nested blocking
        // executor reaches the assertion instead of hanging the test.
        let mut yielded = false;
        std::future::poll_fn(|cx| {
            if yielded {
                assert!(
                    caller_resumed.load(Ordering::Relaxed),
                    "the registry lookup must yield to its caller while waiting for SQL"
                );
                Poll::Ready(())
            } else {
                yielded = true;
                caller_resumed.store(false, Ordering::Relaxed);
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        })
        .await;
    }

    #[async_trait::async_trait(?Send)]
    impl Cursor for RegistryRows {
        async fn next(&mut self) -> SqlResult<Option<&[Value]>> {
            yield_to_caller(&self.caller_resumed).await;
            self.current = self.rows.pop_front();
            Ok(self.current.as_deref())
        }

        fn columns(&self) -> &[String] {
            &self.columns
        }
    }

    #[async_trait::async_trait(?Send)]
    impl Backend for SuspendingRegistrySql {
        async fn query(&self, sql: &str, params: &[Value]) -> SqlResult<Box<dyn Cursor + '_>> {
            yield_to_caller(&self.caller_resumed).await;
            let conn = self.conn.lock().unwrap();
            let mut statement = conn.prepare_cached(sql)?;
            let columns: Vec<_> = statement
                .column_names()
                .into_iter()
                .map(str::to_owned)
                .collect();
            let params = params.iter().map(jazz::storage::ToSql::as_cell);
            let mut rows = statement.query(rusqlite::params_from_iter(params))?;
            let mut result = VecDeque::new();
            while let Some(row) = rows.next()? {
                let cells = (0..columns.len())
                    .map(|index| row.get_ref(index).map(|cell| Value::from(Cell::from(cell))))
                    .collect::<Result<_, _>>()?;
                result.push_back(cells);
            }
            Ok(Box::new(RegistryRows {
                columns,
                rows: result,
                current: None,
                caller_resumed: Arc::clone(&self.caller_resumed),
            }))
        }

        async fn execute(&self, _: &str, _: &[Value]) -> SqlResult<usize> {
            unreachable!()
        }

        async fn execute_script(&self, _: &str) -> SqlResult<()> {
            unreachable!()
        }

        async fn transaction(&self, _: TransactionBody<'_>) -> SqlResult<()> {
            unreachable!()
        }

        async fn last_insert_rowid(&self) -> SqlResult<i64> {
            unreachable!()
        }

        async fn total_changes(&self) -> SqlResult<u64> {
            unreachable!()
        }

        fn max_parameters(&self) -> usize {
            2
        }
    }

    fn check_registry_on_both_backends(dir: &Path, check: impl AsyncFn(&Storage)) {
        let path = dir.join(crate::server::identity::STORE_FILE);
        let open = || {
            rusqlite::Connection::open_with_flags(&path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
                .unwrap()
        };
        let native = Storage::native(open());
        let caller_resumed = Arc::new(AtomicBool::new(false));
        let suspending = Storage::from_backend(SuspendingRegistrySql {
            conn: Mutex::new(open()),
            caller_resumed: Arc::clone(&caller_resumed),
        });
        futures::executor::block_on(async {
            check(&native).await;
            let mut check = std::pin::pin!(check(&suspending));
            std::future::poll_fn(|cx| {
                let result = check.as_mut().poll(cx);
                if result.is_pending() {
                    caller_resumed.store(true, Ordering::Relaxed);
                }
                result
            })
            .await;
            assert!(caller_resumed.load(Ordering::Relaxed));
        });
    }

    /// Core checks who founded Alice's and Bob's accounts through this view.
    /// A view-level test is needed to observe it yielding during SQL reads.
    #[test]
    fn the_founders_view_tells_device_account_founded_accounts() {
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
        let AccountCommandResult::Assignment(other) = registry
            .execute(&AccountCommand::FoundDeviceAccount {
                principal: Principal {
                    issuer: jazz::identity::DEVICE_ACCOUNT_ISSUER.into(),
                    subject: Uuid::from_bytes([8; 16]).to_string(),
                },
                app: Uuid::from_bytes([8; 16]),
            })
            .unwrap()
        else {
            panic!("an assignment");
        };
        let view = RegistryFounders::new(app);
        check_registry_on_both_backends(dir.path(), async |conn| {
            for (account, founded_here, another_audience) in [
                (founded.account, Some(true), false),
                (other.account, Some(false), true),
                (external, Some(false), false),
                (AccountId(Uuid::from_bytes([3; 16])), None, false),
            ] {
                assert_eq!(
                    view.founded_by_device_account(conn, account).await,
                    Ok(founded_here)
                );
                assert_eq!(
                    view.founded_for_another_audience(conn, account).await,
                    Ok(another_audience)
                );
            }
        });
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
                    jazz::db::block_on(conn.query_row(
                        "SELECT count(*) FROM _jazz_host_account_links",
                        [],
                        |row| row.get::<_, i64>(0),
                    ))
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
