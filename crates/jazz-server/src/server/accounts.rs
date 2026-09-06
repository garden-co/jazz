//! Dedicated owner for the Rust account registry and its durable adapter.
use jazz::account_registry::storage::{RegistryError, StoredAccountRegistry};
use jazz::account_registry::{AccountCommand, AccountCommandResult, Assignment, Principal};
use jazz::groove::storage::{BoxedStorage, MemoryStorage, StorageFactory};
use std::{
    path::PathBuf,
    sync::{Arc, mpsc},
};
use tokio::sync::oneshot;

enum Request {
    Execute(
        AccountCommand,
        oneshot::Sender<Result<AccountCommandResult, RegistryError>>,
    ),
    Login(
        Principal,
        oneshot::Sender<Result<Assignment, RegistryError>>,
    ),
    Close(mpsc::Sender<Result<(), String>>),
}

pub(crate) struct AccountRegistryOwner {
    sender: mpsc::Sender<Request>,
    closed: std::sync::atomic::AtomicBool,
    changes: tokio::sync::watch::Sender<u64>,
}

impl AccountRegistryOwner {
    pub(crate) fn open(
        durable: Option<(Arc<dyn StorageFactory>, PathBuf)>,
    ) -> Result<Self, String> {
        let (sender, receiver) = mpsc::channel();
        let (changes, _) = tokio::sync::watch::channel(0u64);
        let notify = changes.clone();
        let (ready, opened) = mpsc::sync_channel(1);
        std::thread::Builder::new()
            .name("jazz-account-registry".into())
            .spawn(move || {
                let storage: Result<BoxedStorage, String> = match durable {
                    Some((factory, path)) => {
                        let profile = jazz::storage_codec_profile::epoch_1_storage_codec_profile()
                            .and_then(|profile| {
                                profile.with_additional_codecs(["jazz.account-command.v1"])
                            });
                        profile
                            .map_err(|error| error.to_string())
                            .and_then(|profile| {
                                jazz::db::block_on(factory.open(
                                    path,
                                    vec!["default".into()],
                                    profile,
                                ))
                                .map_err(|error| error.to_string())
                            })
                    }
                    None => MemoryStorage::new(&["default"])
                        .map(BoxedStorage::new)
                        .map_err(|error| error.to_string()),
                };
                let registry = storage.and_then(|storage| {
                    jazz::db::block_on(StoredAccountRegistry::open(storage))
                        .map_err(|error| error.to_string())
                });
                let mut registry = match registry {
                    Ok(registry) => {
                        if ready.send(Ok(())).is_err() {
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
                            let result = jazz::db::block_on(registry.execute(&command));
                            if result.is_ok() {
                                notify.send_modify(|revision| *revision = revision.wrapping_add(1));
                            }
                            let _ = response.send(result);
                        }
                        Request::Login(principal, response) => {
                            let _ = response.send(jazz::db::block_on(registry.login(&principal)));
                        }
                        Request::Close(response) => {
                            let result = jazz::db::block_on(registry.close())
                                .map_err(|error| error.to_string());
                            let _ = response.send(result);
                            return;
                        }
                    }
                }
                let _ = jazz::db::block_on(registry.close());
            })
            .map_err(|error| error.to_string())?;
        opened
            .recv()
            .map_err(|_| "account registry owner exited during startup".to_owned())??;
        Ok(Self {
            sender,
            changes,
            closed: std::sync::atomic::AtomicBool::new(false),
        })
    }

    pub(crate) fn subscribe(&self) -> tokio::sync::watch::Receiver<u64> {
        self.changes.subscribe()
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
fn unavailable() -> RegistryError {
    RegistryError::Unavailable("registry owner stopped".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use jazz::account_registry::{AccountError, AccountId};
    use uuid::Uuid;

    // Internal durable-owner test: HTTP tests cover authentication, while this
    // test must close and reopen the exact registry root between protocol steps.
    #[tokio::test]
    async fn disk_recovery_preserves_pending_links_and_permanent_revoked_assignments() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("accounts.rocksdb");
        let open = || {
            AccountRegistryOwner::open(Some((
                Arc::new(jazz_storage_rocksdb::RocksDbStorageFactory),
                path.clone(),
            )))
            .unwrap()
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
