//! The app's link and store: disconnecting and reconnecting, deleting the
//! local store, and the writes the server refused that nothing waited on
//! (TS's `disconnect`, `reconnect`, `deleteClientStorage`, `onMutationError`).

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Mutex, MutexGuard};
use std::task::{Context, Poll};
use std::time::Duration;

use futures_core::Stream;
use jazz::tools::{JazzClient, MutationError, TransactionId};
use tokio::sync::mpsc;

use crate::error::{Error, Result};
use crate::handle::Jazz;

/// How often refusals are collected from the client.
const POLL: Duration = Duration::from_millis(50);
/// Refusals kept for a listener that hasn't subscribed yet.
const PENDING_MAX: usize = 1024;
/// Refusals a listener may leave unread before newer ones are dropped
/// for it.
const LISTENER_MAX: usize = 1024;

#[derive(Default)]
struct Reports {
    /// Commits a durable wait is waiting on: their refusals are its.
    claimed: HashSet<TransactionId>,
    listeners: Vec<mpsc::Sender<MutationError>>,
    /// Refusals no listener has taken yet, oldest first.
    pending: Vec<MutationError>,
}

thread_local! {
    // On the client's thread: jobs and the reporter share it.
    static REPORTS: RefCell<Reports> = RefCell::default();
}

pub(crate) fn claim(written: TransactionId) {
    REPORTS.with(|reports| reports.borrow_mut().claimed.insert(written));
}

pub(crate) fn unclaim(written: TransactionId) {
    REPORTS.with(|reports| reports.borrow_mut().claimed.remove(&written));
}

/// Runs on the client's thread for its lifetime: hands each refusal no
/// durable wait claimed to the listeners, or keeps it until one comes.
pub(crate) async fn report_mutation_errors(client: JazzClient) {
    loop {
        tokio::time::sleep(POLL).await;
        let errors = client.take_mutation_errors();
        if errors.is_empty() {
            continue;
        }
        REPORTS.with(|reports| {
            let mut reports = reports.borrow_mut();
            for error in errors {
                if reports.claimed.remove(&error.transaction) {
                    continue;
                }
                reports.listeners.retain(|listener| !listener.is_closed());
                if reports.listeners.is_empty() {
                    if reports.pending.len() >= PENDING_MAX {
                        let dropped = reports.pending.remove(0);
                        tracing::warn!(
                            transaction = %dropped.transaction,
                            message = %dropped.message,
                            "jazz: dropping the oldest unreported mutation error: \
                             {PENDING_MAX} wait for a mutation_errors() listener"
                        );
                    }
                    reports.pending.push(error);
                } else {
                    for listener in &reports.listeners {
                        if listener.try_send(error.clone()).is_err() && !listener.is_closed() {
                            tracing::warn!(
                                transaction = %error.transaction,
                                message = %error.message,
                                "jazz: dropping a mutation error: its listener left \
                                 {LISTENER_MAX} unread"
                            );
                        }
                    }
                }
            }
        });
    }
}

/// Refused writes nothing waited on, as they arrive: see
/// [`Jazz::mutation_errors`].
pub struct MutationErrors {
    receiver: mpsc::Receiver<MutationError>,
    _jazz: Jazz,
}

impl Stream for MutationErrors {
    type Item = MutationError;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.poll_recv(cx)
    }
}

impl Jazz {
    /// The writes the server refused that no [`Jazz::durable`] wait handled,
    /// as TS's `onMutationError` reports them: a plain write returns once
    /// it is committed locally, so its refusal comes here. Refusals that
    /// arrived before the first call are delivered to it first (the last
    /// 1024 of them). Without a server nothing is refused, and the stream
    /// stays empty.
    ///
    /// Each stream holds up to 1024 unread refusals; past that, newer ones
    /// are dropped for it, with a `tracing` warning. Read it, or drop it.
    pub async fn mutation_errors(&self) -> Result<MutationErrors> {
        let (sender, receiver) = mpsc::channel(LISTENER_MAX);
        self.with_client(move |_| async move {
            REPORTS.with(|reports| {
                let mut reports = reports.borrow_mut();
                // PENDING_MAX <= LISTENER_MAX: they all fit.
                for error in std::mem::take(&mut reports.pending) {
                    let _ = sender.try_send(error);
                }
                reports.listeners.push(sender);
            });
            Ok(())
        })
        .await?;
        Ok(MutationErrors {
            receiver,
            _jazz: self.clone(),
        })
    }

    /// Leaves the server: local reads and writes go on, and sync resumes
    /// with [`Jazz::reconnect`] (TS's `disconnect`). Without a server it
    /// does nothing.
    pub async fn disconnect(&self) -> Result<()> {
        self.with_client(|client| async move {
            client.disconnect();
            Ok(())
        })
        .await
    }

    /// Resumes sync after [`Jazz::disconnect`], once the link is attached
    /// (TS's `reconnect`). Without a server it does nothing, and returns at
    /// once.
    pub async fn reconnect(&self) -> Result<()> {
        self.with_client(|client| async move {
            client.reconnect().await?;
            Ok(())
        })
        .await
    }

    /// Stops the client and deletes its local store (TS's
    /// `deleteClientStorage`): the next open of the app starts empty and
    /// syncs again. Writes the server hasn't accepted yet are lost. Nothing
    /// to delete for [`crate::Storage::Memory`].
    ///
    /// Refused with [`Error::StoreHeld`] while another open app (in this
    /// process or another) has the same store directory, as TS refuses
    /// while another tab holds the store: that app is left running. Once
    /// a check passes, opening the directory fails until the files are
    /// gone. Another process is seen only after this client has stopped,
    /// so there the refusal comes with this client stopped.
    pub async fn delete_client_storage(&self) -> Result<()> {
        let Some(dir) = &self.info.storage_dir else {
            self.shutdown().await?;
            return Ok(());
        };
        let deleting = StoreDir::begin_delete(dir)?;
        self.shutdown().await?;
        deleting.finish()
    }
}

/// The store files a client keeps in its directory.
const STORE_FILES: [&str; 4] = [
    "jazz.sqlite",
    "jazz.sqlite-wal",
    "jazz.sqlite-shm",
    "client_id",
];
/// Held shared by every open app of a directory, and exclusively while its
/// store is deleted: the guard across processes.
const LOCK_FILE: &str = "jazz.lock";

#[derive(Default)]
struct DirState {
    /// Apps open on it in this process.
    open: usize,
    /// A delete is under way: nothing may open it.
    deleting: bool,
}

/// Store directories in use in this process, by canonical path.
static STORE_DIRS: Mutex<Option<HashMap<PathBuf, DirState>>> = Mutex::new(None);

fn store_dirs() -> MutexGuard<'static, Option<HashMap<PathBuf, DirState>>> {
    STORE_DIRS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn canonical(dir: &Path) -> Result<PathBuf> {
    std::fs::create_dir_all(dir)
        .and_then(|()| dir.canonicalize())
        .map_err(|error| jazz::tools::JazzError::Io(error).into())
}

fn held(dir: &Path, by: &str) -> Error {
    Error::StoreHeld(format!("{} is {by}", dir.display()))
}

/// An app's hold on its store directory, from open until its client has
/// stopped.
pub(crate) struct StoreDir {
    path: PathBuf,
    /// A shared lock on [`LOCK_FILE`].
    _lock: File,
}

impl StoreDir {
    pub(crate) fn hold(dir: &Path) -> Result<Self> {
        let path = canonical(dir)?;
        let mut dirs = store_dirs();
        let state = dirs
            .get_or_insert_with(HashMap::new)
            .entry(path.clone())
            .or_default();
        if state.deleting {
            return Err(held(&path, "being deleted"));
        }
        let lock = File::create(path.join(LOCK_FILE)).map_err(jazz::tools::JazzError::Io)?;
        match lock.try_lock_shared() {
            Ok(()) => {}
            Err(std::fs::TryLockError::WouldBlock) => {
                return Err(held(&path, "being deleted by another process"));
            }
            Err(std::fs::TryLockError::Error(error)) => {
                return Err(jazz::tools::JazzError::Io(error).into());
            }
        }
        state.open += 1;
        Ok(Self { path, _lock: lock })
    }

    /// Marks `dir` as being deleted, if only this app has it open here.
    fn begin_delete(dir: &Path) -> Result<Deleting> {
        let path = canonical(dir)?;
        let mut dirs = store_dirs();
        let state = dirs
            .get_or_insert_with(HashMap::new)
            .entry(path.clone())
            .or_default();
        if state.deleting {
            return Err(held(&path, "being deleted"));
        }
        // This app's own hold, if it is still running, is one of them.
        if state.open > 1 {
            return Err(held(&path, "open in another app of this process"));
        }
        state.deleting = true;
        Ok(Deleting { path })
    }
}

impl Drop for StoreDir {
    fn drop(&mut self) {
        if let Some(state) = store_dirs()
            .as_mut()
            .and_then(|dirs| dirs.get_mut(&self.path))
        {
            state.open = state.open.saturating_sub(1);
        }
    }
}

/// A delete under way: opening the directory in this process fails until
/// it ends.
struct Deleting {
    path: PathBuf,
}

impl Deleting {
    /// With this app stopped: deletes the store unless another process
    /// still has it open.
    fn finish(self) -> Result<()> {
        if store_dirs()
            .as_ref()
            .and_then(|dirs| dirs.get(&self.path))
            .is_some_and(|state| state.open > 0)
        {
            return Err(held(&self.path, "open in another app of this process"));
        }
        let io = |error| Error::from(jazz::tools::JazzError::Io(error));
        let lock = File::create(self.path.join(LOCK_FILE)).map_err(io)?;
        match lock.try_lock() {
            Ok(()) => {}
            Err(std::fs::TryLockError::WouldBlock) => {
                return Err(held(&self.path, "open in another process"));
            }
            Err(std::fs::TryLockError::Error(error)) => return Err(io(error)),
        }
        for file in STORE_FILES {
            match std::fs::remove_file(self.path.join(file)) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(io(error)),
            }
        }
        Ok(())
    }
}

impl Drop for Deleting {
    fn drop(&mut self) {
        if let Some(state) = store_dirs()
            .as_mut()
            .and_then(|dirs| dirs.get_mut(&self.path))
        {
            state.deleting = false;
        }
    }
}
