//! The app's link and store: disconnecting and reconnecting, deleting the
//! local store, and the writes the server refused that nothing waited on
//! (TS's `disconnect`, `reconnect`, `deleteClientStorage`, `onMutationError`).

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use futures_core::Stream;
use jazz::tools::{JazzClient, MutationError, TransactionId};
use tokio::sync::{mpsc, watch};

use crate::error::{Error, Result};
use crate::handle::{Command, Jazz};

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

/// What a binding cleans up on the client's thread before its client stops.
type BeforeStop = fn(JazzClient) -> Pin<Box<dyn std::future::Future<Output = ()>>>;

thread_local! {
    static BEFORE_STOP: std::cell::Cell<Option<BeforeStop>> = const { std::cell::Cell::new(None) };
}

/// For a binding, called from a [`Jazz::with_client`] job: `clean_up` runs
/// on this client's thread once its jobs are done and before it stops,
/// however it comes to stop (closed, signed out, replaced, deleted). One
/// per client; not public API.
#[doc(hidden)]
pub fn before_client_stops(clean_up: BeforeStop) {
    BEFORE_STOP.set(Some(clean_up));
}

/// Runs what [`before_client_stops`] left.
pub(crate) async fn run_before_stop(client: &JazzClient) {
    if let Some(clean_up) = BEFORE_STOP.take() {
        clean_up(client.clone()).await;
    }
}

impl Jazz {
    /// A handle acting for a user ([`Jazz::for_user`] on a trusted backend
    /// client, or `Endpoint::for_user` with the `unstable-endpoint` feature)
    /// shares its trusted client with every other user: its link, store and
    /// refusals are that client's, not this user's to manage or see, and so
    /// is its session. For bindings too; not public API.
    #[doc(hidden)]
    pub fn refuse_for_user(&self, what: &str) -> Result<()> {
        if self.for_user {
            return Err(Error::Unsupported(format!(
                "{what} is not supported on a handle acting for a user (for_user): it \
                 shares its client's session, link and store with every other user; call it on the \
                 handle for_user was called on"
            )));
        }
        Ok(())
    }

    /// The writes the server refused that no [`Jazz::durable`] wait handled,
    /// as TS's `onMutationError` reports them: a plain write returns once
    /// it is committed locally, so its refusal comes here. Refusals that
    /// arrived before the first call are delivered to it first (the last
    /// 1024 of them). Without a server nothing is refused, and the stream
    /// stays empty.
    ///
    /// Each stream holds up to 1024 unread refusals; past that, newer ones
    /// are dropped for it, with a `tracing` warning. Read it, or drop it.
    ///
    /// Refused with [`Error::Unsupported`] on a handle acting for a user
    /// ([`Jazz::for_user`]): the refusals are every user's.
    pub async fn mutation_errors(&self) -> Result<MutationErrors> {
        self.refuse_for_user("mutation_errors")?;
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
    ///
    /// Refused with [`Error::Unsupported`] on a handle acting for a user
    /// ([`Jazz::for_user`]), whose link every user shares.
    pub async fn disconnect(&self) -> Result<()> {
        self.refuse_for_user("disconnect")?;
        self.with_client(|client| async move {
            client.disconnect();
            Ok(())
        })
        .await
    }

    /// Resumes sync after [`Jazz::disconnect`], once the link is attached
    /// (TS's `reconnect`). Without a server it does nothing, and returns at
    /// once.
    ///
    /// Refused with [`Error::Unsupported`] on a handle acting for a user.
    pub async fn reconnect(&self) -> Result<()> {
        self.refuse_for_user("reconnect")?;
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
    /// One app at a time has a store open, so nothing else holds it while
    /// this one runs: the directory stays locked until the files are gone,
    /// and opening it fails meanwhile. Refused with [`Error::StoreHeld`],
    /// deleting nothing, when this app had already stopped and another has
    /// the store now.
    ///
    /// Refused with [`Error::Unsupported`] on a handle acting for a user
    /// ([`Jazz::for_user`]): it has no store of its own.
    pub async fn delete_client_storage(&self) -> Result<()> {
        self.refuse_for_user("delete_client_storage")?;
        let Some(dir) = &self.info.storage_dir else {
            self.shutdown().await?;
            return Ok(());
        };
        let deleting = StoreDir::begin_delete(dir, &self.info.life)?;
        self.shutdown().await?;
        deleting.finish()
    }
}

/// Refuses a store at `dir` that a signed-in user wrote, which an earlier
/// version kept at the dir itself, for an open with no user: every author
/// of a store such an app writes is its own anonymous one (or there is none
/// yet, and nothing in it is anyone's).
pub(crate) fn refuse_a_users_store(dir: &Path) -> Result<()> {
    let store = dir.join("jazz.sqlite");
    if !store.exists() {
        return Ok(());
    }
    let own = std::fs::read_to_string(dir.join("client_id")).unwrap_or_default();
    let unreadable = |error: rusqlite::Error| {
        Error::Config(format!(
            "cannot read the store at {}: {error}",
            dir.display()
        ))
    };
    let db = rusqlite::Connection::open_with_flags(
        &store,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(unreadable)?;
    let has_authors = db
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = '_jazz_authors'",
            [],
            |_| Ok(()),
        )
        .is_ok();
    if !has_authors {
        return Ok(());
    }
    let others: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM _jazz_authors WHERE NOT (issuer = ?1 AND subject = ?2)",
            [jazz::tools::identity::ANONYMOUS_ISSUER, own.trim()],
            |row| row.get(0),
        )
        .map_err(unreadable)?;
    if others > 0 {
        return Err(Error::Refused(format!(
            "the store at {} holds a signed-in user's data, kept there by an earlier version: \
             open it as that user, or delete it",
            dir.display()
        )));
    }
    Ok(())
}

/// The store files a client keeps in its directory.
const STORE_FILES: [&str; 4] = [
    "jazz.sqlite",
    "jazz.sqlite-wal",
    "jazz.sqlite-shm",
    "client_id",
];
/// Held exclusively by the one open app of a directory, through the delete
/// of its store: the guard across processes. On unix the delete removes it
/// last, so nothing is left behind; on Windows it stays.
const LOCK_FILE: &str = "jazz.lock";

/// How long an open waits for the store's holder in this process when that
/// is already stopping (its shutdown under way, its last handle dropped, its
/// open failed or abandoned): closing and reopening mustn't race.
const STOPPING_WAIT: Duration = Duration::from_secs(5);
/// How long an open retries a lock that no app of this process has. A child
/// process being spawned shares every open file until it execs, so the lock
/// of an app that just closed here can outlive it by a moment.
const SPAWN_WAIT: Duration = Duration::from_millis(100);
/// How often a waiting open looks again.
const HOLD_POLL: Duration = Duration::from_millis(5);

/// One client's run, as whoever opens or deletes its store needs to know it.
pub(crate) struct Life {
    stopping: AtomicBool,
    /// Not upgradable once the last handle is gone, or the open was given up.
    commands: mpsc::WeakUnboundedSender<Command>,
    /// Whether the client's thread ended, letting go of the store.
    ended: watch::Sender<bool>,
}

impl Life {
    pub(crate) fn new(commands: &mpsc::UnboundedSender<Command>) -> Arc<Self> {
        Arc::new(Self {
            stopping: AtomicBool::new(false),
            commands: commands.downgrade(),
            ended: watch::Sender::new(false),
        })
    }

    /// The client is going to stop: an open of its store waits for it.
    pub(crate) fn stop(&self) {
        self.stopping.store(true, Ordering::Release);
    }

    fn is_stopping(&self) -> bool {
        self.stopping.load(Ordering::Acquire)
            || self
                .commands
                .upgrade()
                .is_none_or(|commands| commands.is_closed())
    }

    pub(crate) fn end(&self) {
        self.ended.send_replace(true);
    }

    /// Returns once the client's thread ended, with the store let go of.
    pub(crate) async fn ended(&self) {
        // An error means no sender is left, which `self` rules out.
        let _ = self.ended.subscribe().wait_for(|ended| *ended).await;
    }
}

#[derive(Default)]
struct DirState {
    /// The app of this process that has it open.
    holder: Option<Arc<Life>>,
    /// A delete is under way: nothing may open it.
    deleting: bool,
    /// The holder's lock, kept from its stop to the end of its delete.
    kept: Option<File>,
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

fn held_open(dir: &Path, by: &str) -> Error {
    held(
        dir,
        &format!(
            "{by}: one app at a time can have a store open, so share that app's handle \
             or use another directory"
        ),
    )
}

fn io(error: std::io::Error) -> Error {
    jazz::tools::JazzError::Io(error).into()
}

fn remove(file: &Path) -> Result<()> {
    match std::fs::remove_file(file) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(io(error)),
    }
}

#[derive(Clone, Copy)]
enum Lock {
    /// What apps took up to 2.0.0-beta.0, and what an exclusive lock still
    /// has to see.
    #[cfg_attr(not(test), allow(dead_code))]
    Shared,
    Exclusive,
}

/// Takes `kind` of lock on `file` without waiting; `false` when another
/// holder's conflicts.
#[cfg(not(target_os = "android"))]
fn try_lock(file: &File, kind: Lock) -> std::io::Result<bool> {
    let locked = match kind {
        Lock::Shared => file.try_lock_shared(),
        Lock::Exclusive => file.try_lock(),
    };
    match locked {
        Ok(()) => Ok(true),
        Err(std::fs::TryLockError::WouldBlock) => Ok(false),
        Err(std::fs::TryLockError::Error(error)) => Err(error),
    }
}

#[cfg(target_os = "android")]
use flock::try_lock;

/// The same lock on Android. std implements `File::try_lock` with flock(2)
/// on Linux, but leaves Android out of its list of targets (as of Rust
/// 1.93): there it always fails "not supported", though bionic has flock.
/// Remove this once the pinned toolchain's std implements file locks for
/// Android.
#[cfg(any(target_os = "android", all(test, target_os = "linux")))]
pub(crate) mod flock {
    use std::io::{Error, ErrorKind, Result};
    use std::os::fd::AsRawFd;
    use std::time::{Duration, Instant};

    use super::{File, Lock};

    /// Locked, a conflicting holder (`false`) or an error: std's three
    /// outcomes. Closing the file releases the lock, as with std's.
    pub(super) fn try_lock(file: &File, kind: Lock) -> Result<bool> {
        let operation = match kind {
            Lock::Shared => libc::LOCK_SH,
            Lock::Exclusive => libc::LOCK_EX,
        } | libc::LOCK_NB;
        match flock(file, operation) {
            Ok(()) => Ok(true),
            Err(error) if error.kind() == ErrorKind::WouldBlock => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// How often a lock that is waited for is tried again.
    const RETRY: Duration = Duration::from_millis(5);

    /// Takes the exclusive lock, waiting up to `wait` for whoever holds it
    /// (`TimedOut` after): not `File::lock`'s endless wait, as the holder
    /// may be a process Android froze.
    pub(crate) fn lock(file: &File, wait: Duration) -> Result<()> {
        let deadline = Instant::now() + wait;
        while !try_lock(file, Lock::Exclusive)? {
            if Instant::now() >= deadline {
                return Err(Error::new(
                    ErrorKind::TimedOut,
                    "another process held its lock for too long",
                ));
            }
            std::thread::sleep(RETRY);
        }
        Ok(())
    }

    fn flock(file: &File, operation: libc::c_int) -> Result<()> {
        loop {
            // SAFETY: `file` is borrowed for the call, so its descriptor is
            // open, and flock takes no pointers.
            if unsafe { libc::flock(file.as_raw_fd(), operation) } == 0 {
                return Ok(());
            }
            let error = Error::last_os_error();
            if error.kind() != ErrorKind::Interrupted {
                return Err(error);
            }
        }
    }
}

/// Locks `dir`'s [`LOCK_FILE`], creating it if needed; `None` when
/// another holder's lock conflicts. A delete removes the file while it
/// holds it exclusively, so a lock taken on a file that was meanwhile
/// removed (or replaced) guards nothing: it is dropped and taken again on
/// the file the path names now.
fn lock_store(dir: &Path, kind: Lock) -> Result<Option<File>> {
    let path = dir.join(LOCK_FILE);
    loop {
        // Not truncated: changing a file another handle locked fails on Windows.
        let lock = File::options()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&path)
            .map_err(io)?;
        if !try_lock(&lock, kind).map_err(io)? {
            return Ok(None);
        }
        if still_named(&lock, &path)? {
            return Ok(Some(lock));
        }
    }
}

/// Deletes the store files in `dir` under `lock`, its exclusive lock,
/// running `locked` first and dropping what that returns once the store
/// files are gone.
fn delete_locked<G>(dir: &Path, lock: File, locked: impl FnOnce() -> G) -> Result<()> {
    let held = locked();
    for file in STORE_FILES {
        remove(&dir.join(file))?;
    }
    // The store is gone: an app may open the directory afresh from here.
    drop(held);
    // On unix the lock file goes too, last and while still held: an app
    // that opened it just before finds, once it has its lock, that the
    // path no longer names that file, and starts over (`lock_store`).
    // Released first, an app could lock it in between and keep a lock on
    // the removed file, which guards nothing.
    #[cfg(unix)]
    remove(&dir.join(LOCK_FILE))?;
    // Elsewhere (Windows) it stays: an open file there can be removed
    // (POSIX deletes), and a lock on it can't be checked against its path.
    drop(lock);
    Ok(())
}

/// [`delete_locked`] under a lock of its own; `false`, deleting nothing,
/// while another holder has it.
#[cfg(all(test, unix))]
fn delete_store<G>(dir: &Path, locked: impl FnOnce() -> G) -> Result<bool> {
    let Some(lock) = lock_store(dir, Lock::Exclusive)? else {
        return Ok(false);
    };
    delete_locked(dir, lock, locked)?;
    Ok(true)
}

/// Whether `path` still names the open file `lock`.
#[cfg(unix)]
fn still_named(lock: &File, path: &Path) -> Result<bool> {
    use std::os::unix::fs::MetadataExt;
    let open = lock.metadata().map_err(io)?;
    match std::fs::metadata(path) {
        Ok(named) => Ok(named.dev() == open.dev() && named.ino() == open.ino()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(io(error)),
    }
}

/// Off unix the delete never removes the lock file, so the open file is
/// the one its path names.
#[cfg(not(unix))]
fn still_named(_lock: &File, _path: &Path) -> Result<bool> {
    Ok(true)
}

/// An app's hold on its store directory, from before anything reads it
/// until its client has stopped. One app at a time has one: the engine is
/// its file's only writer, and two clients of one store would mint
/// transaction ids under the same node (INV-DATA-5).
pub(crate) struct StoreDir {
    path: PathBuf,
    life: Arc<Life>,
    /// The exclusive lock on [`LOCK_FILE`].
    lock: Option<File>,
}

impl StoreDir {
    /// Holds `dir` for `life`. Refused at once while another app has it
    /// open; waits up to [`STOPPING_WAIT`] for one of this process that is
    /// stopping. `None` when `abandoned` turns true while it waits.
    ///
    /// It blocks, so it runs on the client's own thread.
    pub(crate) fn hold(
        dir: &Path,
        life: &Arc<Life>,
        abandoned: impl Fn() -> bool,
    ) -> Result<Option<Self>> {
        let started = Instant::now();
        // When the lock was first found taken by no app of this process.
        let mut locked_elsewhere = None;
        loop {
            // Each time: a delete may have removed what it named.
            let path = canonical(dir)?;
            {
                let mut dirs = store_dirs();
                let state = dirs
                    .get_or_insert_with(HashMap::new)
                    .entry(path.clone())
                    .or_default();
                if state.deleting {
                    return Err(held(&path, "being deleted"));
                }
                match &state.holder {
                    Some(holder) => {
                        locked_elsewhere = None;
                        if !holder.is_stopping() {
                            return Err(held_open(&path, "open in another app of this process"));
                        }
                        if started.elapsed() >= STOPPING_WAIT {
                            return Err(held_open(
                                &path,
                                "still being closed by another app of this process",
                            ));
                        }
                    }
                    None => {
                        if let Some(lock) = lock_store(&path, Lock::Exclusive)? {
                            state.holder = Some(life.clone());
                            return Ok(Some(Self {
                                path,
                                life: life.clone(),
                                lock: Some(lock),
                            }));
                        }
                        let since = *locked_elsewhere.get_or_insert_with(Instant::now);
                        if since.elapsed() >= SPAWN_WAIT {
                            return Err(held_open(&path, "open in another process"));
                        }
                    }
                }
            }
            if abandoned() {
                return Ok(None);
            }
            std::thread::sleep(HOLD_POLL);
        }
    }

    /// Marks `dir` as being deleted by `life`'s app, with the lock: the one
    /// it holds, or, when it has already stopped, one taken now.
    fn begin_delete(dir: &Path, life: &Arc<Life>) -> Result<Deleting> {
        let path = canonical(dir)?;
        let mut dirs = store_dirs();
        let state = dirs
            .get_or_insert_with(HashMap::new)
            .entry(path.clone())
            .or_default();
        if state.deleting {
            return Err(held(&path, "being deleted"));
        }
        match &state.holder {
            Some(holder) if !Arc::ptr_eq(holder, life) => {
                return Err(held(&path, "open in another app of this process"));
            }
            // Handed over when the client stops.
            Some(_) => {}
            None => match lock_store(&path, Lock::Exclusive)? {
                Some(lock) => state.kept = Some(lock),
                None => return Err(held(&path, "open in another process")),
            },
        }
        state.deleting = true;
        life.stop();
        Ok(Deleting { path })
    }
}

impl Drop for StoreDir {
    fn drop(&mut self) {
        let mut dirs = store_dirs();
        let state = dirs.as_mut().and_then(|dirs| dirs.get_mut(&self.path));
        if let Some(state) = state
            && state
                .holder
                .as_ref()
                .is_some_and(|holder| Arc::ptr_eq(holder, &self.life))
        {
            state.holder = None;
            if state.deleting {
                state.kept = self.lock.take();
            }
        }
        // Closed with the registry still locked: whoever next sees no holder
        // here finds the file unlocked too.
        drop(self.lock.take());
    }
}

/// A delete under way: opening the directory in this process fails until
/// it ends, and in another one while it has the lock.
struct Deleting {
    path: PathBuf,
}

impl Deleting {
    /// With this app stopped: deletes the store.
    fn finish(self) -> Result<()> {
        let kept = store_dirs()
            .as_mut()
            .and_then(|dirs| dirs.get_mut(&self.path))
            .and_then(|state| state.kept.take());
        match kept {
            Some(lock) => delete_locked(&self.path, lock, || ()),
            None => Err(held(&self.path, "still open in this app")),
        }
    }
}

impl Drop for Deleting {
    fn drop(&mut self) {
        if let Some(state) = store_dirs()
            .as_mut()
            .and_then(|dirs| dirs.get_mut(&self.path))
        {
            state.deleting = false;
            state.kept = None;
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    /// A lock held on a lock file the delete removed, or replaced, isn't
    /// the directory's lock: the locker takes it again.
    #[test]
    fn a_removed_lock_file_is_not_still_named() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join(LOCK_FILE);
        let lock = lock_store(dir.path(), Lock::Shared).unwrap().unwrap();
        assert!(still_named(&lock, &path).unwrap());
        std::fs::remove_file(&path).unwrap();
        assert!(!still_named(&lock, &path).unwrap());
        // Another app's fresh lock file at the same path.
        let fresh = lock_store(dir.path(), Lock::Exclusive).unwrap().unwrap();
        assert!(!still_named(&lock, &path).unwrap());
        assert!(still_named(&fresh, &path).unwrap());
    }

    /// Android's lock is the one std takes on Linux: the two see each
    /// other, shared beside shared and exclusive alone.
    #[cfg(target_os = "linux")]
    #[test]
    fn androids_flock_is_stds_lock() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join(LOCK_FILE);
        let take = |by_flock: bool, kind| {
            let file = File::create(&path).unwrap();
            let locked = if by_flock {
                flock::try_lock(&file, kind).unwrap()
            } else {
                try_lock(&file, kind).unwrap()
            };
            locked.then_some(file)
        };
        for flock_first in [true, false] {
            let held = take(flock_first, Lock::Shared).expect("a shared lock");
            assert!(take(!flock_first, Lock::Shared).is_some());
            assert!(take(!flock_first, Lock::Exclusive).is_none());
            drop(held);

            let held = take(flock_first, Lock::Exclusive).expect("an exclusive lock");
            assert!(take(!flock_first, Lock::Shared).is_none());
            assert!(take(!flock_first, Lock::Exclusive).is_none());
            drop(held);
        }
    }

    /// Deletes and opens racing on one directory never hold its lock at
    /// once: the deletes remove the lock file under their lock, and an open
    /// that locked a removed file takes the lock again on the current one.
    #[test]
    fn deletes_and_opens_never_overlap() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};

        const ROUNDS: usize = 20_000;
        let dir = Arc::new(tempfile::TempDir::new().unwrap());
        let shared = Arc::new(AtomicUsize::new(0));
        let exclusive = Arc::new(AtomicUsize::new(0));
        let overlapped = Arc::new(AtomicBool::new(false));
        let (opened, deleted) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));

        struct Leave(Arc<AtomicUsize>);
        impl Drop for Leave {
            fn drop(&mut self) {
                self.0.fetch_sub(1, SeqCst);
            }
        }

        let mut threads = Vec::new();
        for _ in 0..4 {
            let (dir, shared, exclusive, overlapped, opened) = (
                dir.clone(),
                shared.clone(),
                exclusive.clone(),
                overlapped.clone(),
                opened.clone(),
            );
            threads.push(std::thread::spawn(move || {
                for _ in 0..ROUNDS {
                    let Some(lock) = lock_store(dir.path(), Lock::Shared).unwrap() else {
                        continue;
                    };
                    shared.fetch_add(1, SeqCst);
                    if exclusive.load(SeqCst) > 0 {
                        overlapped.store(true, SeqCst);
                    }
                    shared.fetch_sub(1, SeqCst);
                    drop(lock);
                    opened.fetch_add(1, SeqCst);
                    // Leave the lock free now and then, for the deletes.
                    std::thread::yield_now();
                }
            }));
        }
        for _ in 0..2 {
            let (dir, shared, exclusive, overlapped, deleted) = (
                dir.clone(),
                shared.clone(),
                exclusive.clone(),
                overlapped.clone(),
                deleted.clone(),
            );
            threads.push(std::thread::spawn(move || {
                for _ in 0..ROUNDS {
                    let done = delete_store(dir.path(), || {
                        if exclusive.fetch_add(1, SeqCst) > 0 || shared.load(SeqCst) > 0 {
                            overlapped.store(true, SeqCst);
                        }
                        Leave(exclusive.clone())
                    })
                    .unwrap();
                    if done {
                        deleted.fetch_add(1, SeqCst);
                    }
                    std::thread::yield_now();
                }
            }));
        }
        for thread in threads {
            thread.join().unwrap();
        }
        assert!(
            !overlapped.load(SeqCst),
            "a delete and an open held the lock at once"
        );
        // Both sides actually ran, and contended.
        let (opened, deleted) = (opened.load(SeqCst), deleted.load(SeqCst));
        assert!(
            opened > 1000 && deleted > 1000,
            "{opened} opens, {deleted} deletes"
        );
    }
}
