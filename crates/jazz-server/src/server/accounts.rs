//! Dedicated owner for the Rust account registry and its durable adapter.
use super::account_store::{RegistryError, StoredAccountRegistry};
use crate::kv::HostSql;
use jazz::account_registry::{AccountCommand, AccountCommandResult, Assignment, Principal};
use std::sync::mpsc;
use tokio::sync::oneshot;

/// Tests can make the next registry write fail after it commits.
#[cfg(test)]
type Failpoint = std::sync::Arc<std::sync::atomic::AtomicBool>;
#[cfg(not(test))]
type Failpoint = ();

enum Request {
    Execute(
        AccountCommand,
        oneshot::Sender<Result<AccountCommandResult, RegistryError>>,
    ),
    Login(
        Principal,
        oneshot::Sender<Result<Assignment, RegistryError>>,
    ),
    LoginOrRegister(
        Principal,
        oneshot::Sender<Result<Assignment, RegistryError>>,
    ),
    Close(mpsc::Sender<Result<(), String>>),
}

pub(crate) struct AccountRegistryOwner {
    sender: mpsc::Sender<Request>,
    closed: std::sync::atomic::AtomicBool,
    /// Template receiver. The only sender lives on the owner thread, so the
    /// watch closes (and every subscriber re-checks and fails closed) if that
    /// thread exits for any reason, including a panic.
    changes: tokio::sync::watch::Receiver<u64>,
    #[cfg(test)]
    failpoint: Failpoint,
}

impl AccountRegistryOwner {
    /// Own the registry kept in `storage`.
    pub(crate) fn open(storage: HostSql) -> Result<Self, String> {
        let (sender, receiver) = mpsc::channel();
        let (notify, changes) = tokio::sync::watch::channel(0u64);
        let (ready, opened) = mpsc::sync_channel(1);
        std::thread::Builder::new()
            .name("jazz-account-registry".into())
            .spawn(move || {
                let registry = {
                    #[cfg(test)]
                    let failpoint = storage.failpoint();
                    #[cfg(not(test))]
                    let failpoint: Failpoint = ();
                    StoredAccountRegistry::open(storage)
                        .map(|registry| (registry, failpoint))
                        .map_err(|error| error.to_string())
                };
                let mut registry = match registry {
                    Ok((registry, failpoint)) => {
                        if ready.send(Ok(failpoint)).is_err() {
                            return;
                        }
                        registry
                    }
                    Err(error) => {
                        let _ = ready.send(Err(error));
                        return;
                    }
                };
                while let Ok(request) = receiver.recv() {
                    match request {
                        Request::Execute(command, response) => {
                            let result = registry.execute(&command);
                            // Announce a success, and any `Unavailable` failure:
                            // it may come after the write committed (for example
                            // a failed durable flush), so live sockets must
                            // re-check rather than trust their last lookup. `Decision` errors are
                            // rejected before any write and change nothing, so
                            // they stay silent (a spammed denied request must not
                            // force a lookup on every live socket).
                            if announces(&result) {
                                notify.send_modify(|revision| *revision = revision.wrapping_add(1));
                            }
                            let _ = response.send(result);
                        }
                        Request::LoginOrRegister(principal, response) => {
                            let result = registry.login_or_register(&principal);
                            let created = result.as_ref().is_ok_and(|result| result.created);
                            if created || matches!(result, Err(RegistryError::Unavailable(_))) {
                                notify.send_modify(|revision| *revision = revision.wrapping_add(1));
                            }
                            let _ = response.send(result.map(|result| result.assignment));
                        }
                        Request::Login(principal, response) => {
                            // A lookup never mutates: nothing to announce.
                            let _ = response.send(registry.login(&principal));
                        }
                        Request::Close(response) => {
                            drop(registry);
                            let _ = response.send(Ok(()));
                            return;
                        }
                    }
                }
            })
            .map_err(|error| error.to_string())?;
        #[cfg_attr(not(test), allow(clippy::let_unit_value))]
        let _failpoint = opened
            .recv()
            .map_err(|_| "account registry owner exited during startup".to_owned())??;
        Ok(Self {
            sender,
            changes,
            closed: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            failpoint: _failpoint,
        })
    }

    /// Tests: a registry whose store lives in memory.
    #[cfg(test)]
    pub(crate) fn in_memory_for_test() -> Self {
        let store = crate::runtime::AppStore::memory().expect("memory store");
        Self::open(store.sql().expect("store")).expect("registry")
    }

    /// Tests: the next registry write commits and then reports a failure.
    #[cfg(test)]
    pub(crate) fn fail_next_write_for_test(&self) {
        self.failpoint
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Tests: whether the injected failure is still waiting for a write.
    #[cfg(test)]
    pub(crate) fn write_failure_pending_for_test(&self) -> bool {
        self.failpoint.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub(crate) fn subscribe(&self) -> tokio::sync::watch::Receiver<u64> {
        let mut changes = self.changes.clone();
        changes.mark_unchanged();
        changes
    }

    pub(crate) fn close(&self) -> Result<(), String> {
        if self.closed.swap(true, std::sync::atomic::Ordering::AcqRel) {
            return Ok(());
        }
        let (reply, response) = mpsc::channel();
        self.sender
            .send(Request::Close(reply))
            .map_err(|_| "account owner stopped".to_owned())?;
        response
            .recv()
            .map_err(|_| "account owner exited during close".to_owned())?
    }

    pub(crate) async fn execute(
        &self,
        command: AccountCommand,
    ) -> Result<AccountCommandResult, RegistryError> {
        let (reply, response) = oneshot::channel();
        self.sender
            .send(Request::Execute(command, reply))
            .map_err(|_| unavailable())?;
        response.await.map_err(|_| unavailable())?
    }

    pub(crate) async fn login_or_register(
        &self,
        principal: Principal,
    ) -> Result<Assignment, RegistryError> {
        let (reply, response) = oneshot::channel();
        self.sender
            .send(Request::LoginOrRegister(principal, reply))
            .map_err(|_| unavailable())?;
        response.await.map_err(|_| unavailable())?
    }

    pub(crate) async fn login(&self, principal: Principal) -> Result<Assignment, RegistryError> {
        let (reply, response) = oneshot::channel();
        self.sender
            .send(Request::Login(principal, reply))
            .map_err(|_| unavailable())?;
        response.await.map_err(|_| unavailable())?
    }
}

impl Drop for AccountRegistryOwner {
    fn drop(&mut self) {
        let _ = self.close();
    }
}
/// Whether a registry command result may have changed what `login` answers.
fn announces<T>(result: &Result<T, RegistryError>) -> bool {
    !matches!(result, Err(RegistryError::Decision(_)))
}

fn unavailable() -> RegistryError {
    RegistryError::Unavailable("registry owner stopped".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use jazz::account_registry::{AccountError, AccountId};
    use uuid::Uuid;

    // Observer invalidation is an internal owner contract, not an HTTP result.
    #[tokio::test]
    async fn login_or_register_notifies_only_new_assignments() {
        let owner = AccountRegistryOwner::in_memory_for_test();
        let mut changes = owner.subscribe();
        let principal = Principal {
            issuer: "https://issuer.example".into(),
            subject: "user".into(),
        };
        let created = owner.login_or_register(principal.clone()).await.unwrap();
        assert!(changes.has_changed().unwrap());
        changes.borrow_and_update();
        assert_eq!(owner.login_or_register(principal).await.unwrap(), created);
        assert!(!changes.has_changed().unwrap());
        owner.close().unwrap();
    }

    // Live sockets skip their per-message registry lookup while this watch is
    // unchanged (#3387). A request the registry rejects before any write (a
    // `Decision` error) changes nothing and must stay silent, so denied
    // requests cannot force a lookup on every live socket; the watch must close
    // once the owner thread is gone. (`Unavailable` failures are announced;
    // the websocket failed-flush test covers them.)
    #[tokio::test]
    async fn changes_skip_rejected_requests_and_close_with_the_owner() {
        let owner = AccountRegistryOwner::in_memory_for_test();
        let mut changes = owner.subscribe();
        assert!(!changes.has_changed().unwrap());
        let unknown = Principal {
            issuer: "https://issuer.example".into(),
            subject: "nobody".into(),
        };
        let failed = owner
            .execute(AccountCommand::Revoke {
                approver: unknown.clone(),
                target: unknown.clone(),
            })
            .await;
        assert!(
            matches!(failed, Err(RegistryError::Decision(_))),
            "an unassigned approver cannot revoke: {failed:?}"
        );
        assert!(owner.login(unknown).await.is_err(), "no assignment exists");
        assert!(!changes.has_changed().unwrap(), "rejections change nothing");
        owner.close().unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while changes.changed().await.is_ok() {}
        })
        .await
        .expect("the watch closes when the owner thread exits");
    }

    // Internal durable-owner test: HTTP tests cover authentication, while this
    // test must close and reopen the exact registry root between protocol steps.
    #[tokio::test]
    async fn disk_recovery_preserves_pending_links_and_permanent_revoked_assignments() {
        let directory = tempfile::tempdir().unwrap();
        let storage = crate::runtime::StorageConfig::data_dir(directory.path());
        let open = || {
            let store = crate::runtime::AppStore::open(&storage).unwrap();
            AccountRegistryOwner::open(store.sql().unwrap()).unwrap()
        };
        let alice = Principal {
            issuer: "https://issuer.example".into(),
            subject: "alice".into(),
        };
        let bob = Principal {
            subject: "bob".into(),
            ..alice.clone()
        };
        let account = AccountId(Uuid::from_u128(1));
        let nonce = Uuid::from_u128(2);
        let first = open();
        first
            .execute(AccountCommand::Register {
                principal: alice.clone(),
                account,
            })
            .await
            .unwrap();
        first
            .execute(AccountCommand::RequestLink {
                approver: alice.clone(),
                candidate: bob.clone(),
                nonce,
                now: 100,
                expires_at: 200,
            })
            .await
            .unwrap();
        first.close().unwrap();

        let second = open();
        assert_eq!(second.login(alice.clone()).await.unwrap().account, account);
        assert!(matches!(
            second.login(bob.clone()).await,
            Err(RegistryError::Decision(AccountError::NotAssigned))
        ));
        let accept = AccountCommand::AcceptLink {
            candidate: bob.clone(),
            nonce,
            now: 150,
        };
        let linked = second.execute(accept.clone()).await.unwrap();
        assert!(
            matches!(linked, AccountCommandResult::Assignment(Assignment { account: id, .. }) if id == account)
        );
        second.close().unwrap();

        let third = open();
        assert_eq!(third.execute(accept.clone()).await.unwrap(), linked);
        third
            .execute(AccountCommand::Revoke {
                approver: alice.clone(),
                target: bob.clone(),
            })
            .await
            .unwrap();
        third.close().unwrap();

        let fourth = open();
        assert_eq!(fourth.login(alice).await.unwrap().account, account);
        assert!(matches!(
            fourth.login(bob.clone()).await,
            Err(RegistryError::Decision(AccountError::NotAuthorized))
        ));
        assert!(matches!(
            fourth.execute(accept).await,
            Err(RegistryError::Decision(AccountError::NotAuthorized))
        ));
        assert!(matches!(
            fourth
                .execute(AccountCommand::Register {
                    principal: bob,
                    account: AccountId(Uuid::from_u128(3)),
                })
                .await,
            Err(RegistryError::Decision(AccountError::AlreadyAssigned))
        ));
        fourth.close().unwrap();
    }
}
