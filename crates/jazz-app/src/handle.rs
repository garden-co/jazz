//! The `Send + Clone` handle, and the thread its client runs on.
//!
//! A [`JazzClient`] owns its database on the thread that opened it
//! (`Rc`/`RefCell`), so it cannot be shared across a multi-threaded
//! runtime. [`Jazz`] runs it on a thread of its own, with a current-thread
//! runtime and a `LocalSet`, and sends it jobs: closures that get a clone of
//! the client and run as local tasks there, so a slow read or a long-lived
//! subscription never holds up the others.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use futures_util::FutureExt as _;
use jazz::account_registry::AccountId;
use jazz::db::ReadOpts;
use jazz::tools::{
    AppContext, ClientStorage, DurabilityTier, JazzClient, ObjectId, QueryResult, ReadTier,
    Session, TableName, TransactionId,
};
use tokio::sync::{mpsc, oneshot, watch};

use crate::app::AppFiles;
use crate::auth::{Auth, DEVICE_TOKEN_TTL_SECS, TokenReason};
use crate::backend;
use crate::config::{Config, Storage};
use crate::error::{Error, Result};
use crate::include::Row;
use crate::query::Query;
use crate::session::{
    self as user_session, Credential, Session as UserSession, SyncStatus, User, UserKind,
};
use crate::subscription::{self, Subscription};
use crate::table::{self, Implementations, Insert, Table, Update};

type Job = Box<dyn FnOnce(JazzClient) -> Pin<Box<dyn Future<Output = ()>>> + Send>;

pub(crate) enum Command {
    Run(Job),
    Shutdown(oneshot::Sender<Result<()>>),
}

/// How long [`Jazz::shutdown`] lets the calls already made run before it
/// cancels them.
const SHUTDOWN_DRAIN: std::time::Duration = std::time::Duration::from_secs(30);

/// The issuer `createJazz` names the users a backend acts as by.
const BACKEND_ISSUER: &str = "jazz:backend";

/// An open Jazz app: `Send + Clone`, so it can live in shared state such as
/// an Axum router's. Every clone talks to the same client.
///
/// The client stops when [`Jazz::shutdown`] is called or the last clone
/// (including those held by subscriptions) is dropped.
#[derive(Clone)]
pub struct Jazz {
    pub(crate) commands: mpsc::UnboundedSender<Command>,
    pub(crate) info: Arc<Info>,
    /// The user one of the endpoint's handles acts for
    /// (`Endpoint::for_user`): its reads and writes run as their
    /// session, under their permissions, on the endpoint's trusted link.
    pub(crate) scope: Option<Arc<Session>>,
    /// Whether this handle acts for one of the backend's users.
    pub(crate) for_user: bool,
    /// The tier this handle's writes wait for ([`Jazz::durable`]); `None`
    /// returns once a write is committed locally.
    pub(crate) durability: Option<DurabilityTier>,
}

#[cfg_attr(not(feature = "unstable-endpoint"), allow(dead_code))]
pub(crate) struct Info {
    bundle: Option<String>,
    account: Option<AccountId>,
    pub(crate) implementations: Implementations,
    /// No server: nothing confirms a write past the local store.
    pub(crate) serverless: bool,
    /// How reads that name no tier read.
    pub(crate) default_read: DefaultRead,
    /// Where the local store lives, for [`Jazz::delete_client_storage`].
    pub(crate) storage_dir: Option<std::path::PathBuf>,
    /// An end user's session ([`Config::user`]).
    pub(crate) session: Option<UserSession>,
    pub(crate) sync: watch::Receiver<SyncStatus>,
    /// What a backend holds to vouch for its users (`Endpoint`).
    pub(crate) backend: Option<BackendLink>,
    /// How this backend was opened, for the trusted client its endpoint
    /// acts for its users with.
    pub(crate) opened_with: Option<(AppFiles, Config)>,
    /// A backend client's way back to its main backend.
    pub(crate) backend_client: Option<crate::backend_client::BackendClientLink>,
    /// The store keeps nothing past the process.
    pub(crate) memory: bool,
    /// Its [`crate::JazzSession`] replaced it: calls fail with
    /// [`Error::Replaced`], not [`Error::Stopped`].
    pub(crate) replaced: std::sync::atomic::AtomicBool,
}

/// A backend's way to the core, for the endpoint it serves its users with.
#[derive(Clone)]
#[cfg_attr(not(feature = "unstable-endpoint"), allow(dead_code))]
pub(crate) struct BackendLink {
    pub server: String,
    pub app_id: jazz::tools::AppId,
    pub backend_secret: String,
    pub admin_secret: String,
}

impl std::fmt::Debug for Jazz {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Jazz")
            .field("bundle", &self.info.bundle)
            .finish_non_exhaustive()
    }
}

impl Jazz {
    /// Opens the app `files` define.
    ///
    /// On a backend ([`Config::backend`]) it first publishes the app's
    /// permissions bundle and resolves the account of the user it acts as;
    /// its session is then pinned to that bundle.
    pub async fn open(files: AppFiles, config: Config) -> Result<Self> {
        Self::open_with(files, config, Implementations::default()).await
    }

    /// [`Jazz::open`] for an app with computed columns or transforms: the
    /// generated `open` passes their implementations.
    pub async fn open_with(
        files: AppFiles,
        config: Config,
        implementations: Implementations,
    ) -> Result<Self> {
        Self::open_as(files, config, implementations, Link::Own).await
    }

    /// The client a backend's endpoint acts for its users with
    /// (`Endpoint::for_user`), as `createJazz`'s backend session:
    /// its link states only the backend's secret and the bundle it is pinned
    /// to, so the core trusts it to act for the sessions its calls name, and
    /// its schema carries the permissions, so their reads evaluate here too.
    #[cfg_attr(not(feature = "unstable-endpoint"), allow(dead_code))]
    pub(crate) async fn open_for_users(&self) -> Result<Self> {
        let Some((files, config)) = self.info.opened_with.clone() else {
            return Err(Error::Config(
                "an endpoint is served by the app's backend: open it with Config::backend".into(),
            ));
        };
        let config = Config {
            storage: Storage::Memory,
            ..config
        };
        Self::open_as(
            files,
            config,
            self.info.implementations.clone(),
            Link::TrustedForUsers,
        )
        .await
    }

    async fn open_as(
        files: AppFiles,
        config: Config,
        implementations: Implementations,
        link: Link,
    ) -> Result<Self> {
        let files = match config.backend.as_ref().and_then(|b| b.permissions.clone()) {
            Some(permissions) => files.with_permissions(permissions),
            None => files,
        };
        if config.backend.is_some() && config.server_url.is_none() {
            return Err(Error::Config(
                "a backend needs the sync server's URL (Config::server)".into(),
            ));
        }
        if matches!(config.user, Some(Auth::Credential(_))) && !files.has_permissions() {
            return Err(Error::App(
                "a backend client needs the app's jazz.permissions.json (AppFiles::read, or \
                 AppFiles::with_permissions), as createJazzClient takes its permissions: it \
                 checks the users it acts for by them"
                    .into(),
            ));
        }
        if config.backend.is_some() && !files.has_permissions() {
            return Err(Error::App(
                "a backend needs the app's jazz.permissions.json".into(),
            ));
        }
        if config.backend.is_some() && config.user.is_some() {
            return Err(Error::Config(
                "open either as the app's backend or as an end user, not both".into(),
            ));
        }
        if config.user.is_some() && config.server_url.is_none() {
            return Err(Error::Config(
                "an end user needs the app's Jazz endpoint (Config::server), \
                 e.g. https://app.example/jazz"
                    .into(),
            ));
        }
        if config.user.is_some()
            && !config.allow_insecure_http
            && config.sends_credentials_in_plain_text()
        {
            return Err(Error::Config(
                "an end user's credentials would cross the network in plain text: use \
                 https:// (or wss://), or Config::allow_insecure_http for a dev server"
                    .into(),
            ));
        }
        if config.server_url.is_some() && config.backend.is_none() && config.user.is_none() {
            return Err(Error::Config(
                "a server needs someone to connect as: the app's backend (Config::backend) \
                 or an end user (Config::user)"
                    .into(),
            ));
        }
        let serverless = config.server_url.is_none();
        let default_read = DefaultRead::of(&config);
        let storage_dir = match &config.storage {
            Storage::Dir(dir) => Some(dir.clone()),
            Storage::Memory => None,
        };
        // Held until the client has stopped: `delete_client_storage` won't
        // pull the store from under this app.
        let store = storage_dir
            .as_deref()
            .map(crate::lifecycle::StoreDir::hold)
            .transpose()?;
        let opened_with = config
            .backend
            .is_some()
            .then(|| (files.clone(), config.clone()));
        let (commands, receiver) = mpsc::unbounded_channel();
        let (ready, opened) = oneshot::channel();
        std::thread::Builder::new()
            .name("jazz-client".into())
            .spawn({
                // The session stops the client through this, without keeping
                // it alive: it stops with its last handle all the same.
                let stopper = commands.downgrade();
                move || run(files, config, link, store, receiver, ready, stopper)
            })
            .map_err(|error| Error::Config(format!("cannot start the client thread: {error}")))?;
        let Opened {
            bundle,
            account,
            session,
            sync,
            backend,
            backend_client,
            memory,
        } = opened.await.map_err(|_| Error::Stopped)??;
        Ok(Self {
            commands,
            info: Arc::new(Info {
                bundle,
                account,
                implementations,
                serverless,
                default_read,
                storage_dir,
                session,
                sync,
                backend,
                opened_with,
                backend_client,
                memory,
                replaced: std::sync::atomic::AtomicBool::new(false),
            }),
            scope: None,
            for_user: false,
            durability: None,
        })
    }

    /// The permissions bundle this backend published and is pinned to.
    pub fn bundle(&self) -> Option<&str> {
        self.info.bundle.as_deref()
    }

    /// The account of the user this backend acts as, or of the end user
    /// signed in.
    pub fn account(&self) -> Option<AccountId> {
        match &self.scope {
            Some(session) => session.account_id,
            None => self.info.account,
        }
    }

    /// This backend's handle, acting as `session`'s user: the same client,
    /// with every read and write run as them and their permissions.
    #[cfg_attr(not(feature = "unstable-endpoint"), allow(dead_code))]
    pub(crate) fn scoped(&self, session: Session) -> Self {
        Self {
            commands: self.commands.clone(),
            info: self.info.clone(),
            scope: Some(Arc::new(session)),
            for_user: true,
            durability: self.durability,
        }
    }

    /// Whether this handle acts for one of the backend's users.
    pub fn acts_for_user(&self) -> bool {
        self.for_user
    }

    /// Runs `job` with the untyped client on its thread: for what the typed
    /// API does not cover.
    pub async fn with_client<F, Fut, R>(&self, job: F) -> Result<R>
    where
        F: FnOnce(JazzClient) -> Fut + Send + 'static,
        Fut: Future<Output = Result<R>> + 'static,
        R: Send + 'static,
    {
        let (reply, answer) = oneshot::channel();
        let scope = self.scope.clone();
        let job: Job = Box::new(move |client| {
            let client = match scope {
                Some(session) => client.for_session((*session).clone()),
                None => client,
            };
            Box::pin(async move {
                // A panic in the job is its caller's error, not the end of
                // the client: the other jobs and the client run on.
                // Calling `job` happens inside the guard too: a closure
                // that panics before returning its future is caught.
                let result = std::panic::AssertUnwindSafe(async move { job(client).await })
                    .catch_unwind()
                    .await
                    .unwrap_or_else(|panic| Err(Error::Panicked(panic_message(&*panic))));
                let _ = reply.send(result);
            })
        });
        self.commands
            .send(Command::Run(job))
            .map_err(|_| self.stopped())?;
        answer.await.map_err(|_| self.stopped())?
    }

    /// Why calls fail now that the client stopped.
    fn stopped(&self) -> Error {
        if self.info.replaced.load(std::sync::atomic::Ordering::SeqCst) {
            Error::Replaced
        } else {
            Error::Stopped
        }
    }

    /// Its [`crate::JazzSession`] is replacing it.
    pub(crate) fn mark_replaced(&self) {
        self.info
            .replaced
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Sends `job` to the client's thread without waiting for it: for
    /// cleanups that must run even when nobody awaits them (from `Drop`).
    pub(crate) fn fire<F, Fut>(&self, job: F)
    where
        F: FnOnce(JazzClient) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + 'static,
    {
        let job: Job = Box::new(move |client| Box::pin(job(client)));
        let _ = self.commands.send(Command::Run(job));
    }

    /// Every row `query` matches, read as TypeScript reads without a tier
    /// ([`DefaultRead`]): from the server on a backend or backend client;
    /// local first on an end user's client, whose first load waits briefly
    /// for the server; locally without a server.
    pub async fn all<T: Row>(&self, query: Query<T>) -> Result<Vec<T>> {
        let opts = self.info.default_read.opts(&query);
        self.all_opts(query, opts).await
    }

    /// Every row `query` matches, read at `tier`.
    pub async fn all_with<T: Row>(&self, query: Query<T>, tier: ReadTier) -> Result<Vec<T>> {
        let opts = read_opts(&query, tier);
        self.all_opts(query, opts).await
    }

    async fn all_opts<T: Row>(&self, query: Query<T>, opts: ReadOpts) -> Result<Vec<T>> {
        let query = query.into_untyped()?;
        let implementations = self.info.implementations.clone();
        self.with_client(move |client| async move {
            let rows = client.query_with_opts(query, opts).await?;
            rows.into_iter()
                .map(|row| decode_result::<T>(row, &implementations))
                .collect()
        })
        .await
    }

    /// The first row `query` matches.
    pub async fn one<T: Row>(&self, query: Query<T>) -> Result<Option<T>> {
        Ok(self.all(query.limit(1)).await?.into_iter().next())
    }

    /// The row of `T` with this id.
    pub async fn get<T: Table>(&self, id: ObjectId) -> Result<Option<T>> {
        self.one(Query::<T>::all().id(id)).await
    }

    /// Inserts a row. Returns it as stored, once committed locally (or, on
    /// a [`Jazz::durable`] handle, once it reached that tier).
    ///
    /// Like every write, it is sent to the client's thread when called:
    /// dropping the returned future doesn't undo or cancel it.
    pub async fn insert<N: Insert>(&self, new: N) -> Result<N::Row> {
        let implementations = self.info.implementations.clone();
        let settle = self.settler()?;
        self.with_client(move |client| async move {
            let (row, written) = insert(&client, new, &implementations)?;
            settle.settle(&client, written).await?;
            Ok(row)
        })
        .await
    }

    /// Updates the row with this id.
    pub async fn update<U: Update>(&self, id: ObjectId, update: U) -> Result<()> {
        let implementations = self.info.implementations.clone();
        let settle = self.settler()?;
        self.with_client(move |client| async move {
            let cells = update.into_cells(&implementations)?;
            let written = client.update(<U::Row as Table>::NAME, id, cells)?;
            settle.settle(&client, written).await
        })
        .await
    }

    /// Deletes the row of `T` with this id.
    pub async fn delete<T: Table>(&self, id: ObjectId) -> Result<()> {
        let settle = self.settler()?;
        self.with_client(move |client| async move {
            let written = client.delete(T::NAME, id)?;
            settle.settle(&client, written).await
        })
        .await
    }

    /// Subscribes to `query`: a stream of its changes, starting with the
    /// rows it matches now, read as [`Jazz::all`] reads by default.
    pub async fn subscribe<T: Row>(&self, query: Query<T>) -> Result<Subscription<T>> {
        let opts = self.info.default_read.opts(&query);
        self.subscribe_opts(query, opts).await
    }

    /// [`Jazz::subscribe`] at `tier`.
    pub async fn subscribe_with<T: Row>(
        &self,
        query: Query<T>,
        tier: ReadTier,
    ) -> Result<Subscription<T>> {
        let opts = read_opts(&query, tier);
        self.subscribe_opts(query, opts).await
    }

    async fn subscribe_opts<T: Row>(
        &self,
        query: Query<T>,
        opts: ReadOpts,
    ) -> Result<Subscription<T>> {
        let query = query.into_untyped()?;
        let (sender, receiver) = mpsc::unbounded_channel();
        let implementations = self.info.implementations.clone();
        self.with_client(move |client| async move {
            let stream = client.subscribe_with_opts(query, opts).await?;
            tokio::task::spawn_local(subscription::forward::<T>(stream, sender, implementations));
            Ok(())
        })
        .await?;
        Ok(Subscription::new(receiver, self.clone()))
    }

    /// Stops the client for every clone: the calls already made finish
    /// first (for up to 30 seconds, after which they fail with
    /// [`Error::Stopped`]), then open subscriptions end. Calls made once
    /// shutdown has begun, including from a call still finishing, fail
    /// with [`Error::Stopped`]. Call it before the process exits when the app
    /// keeps a persistent store ([`crate::Storage::Dir`]), so its last
    /// writes are flushed and the store closed.
    ///
    /// On a handle acting for a user (`Endpoint::for_user`) it does
    /// nothing: `Endpoint::shutdown` stops the client it shares.
    pub async fn shutdown(&self) -> Result<()> {
        if self.for_user {
            return Ok(());
        }
        let (reply, done) = oneshot::channel();
        if self.commands.send(Command::Shutdown(reply)).is_err() {
            return Ok(());
        }
        done.await.unwrap_or(Ok(()))
    }
}

/// How a read that names no tier reads, as TypeScript's
/// `resolveEffectiveQueryExecutionOptions` decides it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DefaultRead {
    /// No server: local data.
    Local,
    /// An end user's client (TypeScript's browser and React Native
    /// clients): local data first, its first load waiting at most this
    /// long for the server, so it shows neither a stale nor an empty cache.
    LocalFirstWaiting(std::time::Duration),
    /// A backend or backend client (TypeScript's server runtimes): the
    /// server's answer.
    Remote,
}

/// How long an end user's first load waits for the server, as
/// TypeScript's `DEFAULT_CLIENT_FIRST_LOAD_REMOTE_WAIT_MS`.
pub const DEFAULT_FIRST_LOAD_REMOTE_WAIT: std::time::Duration = std::time::Duration::from_secs(5);

impl DefaultRead {
    fn of(config: &Config) -> Self {
        match (&config.server_url, &config.backend, &config.user) {
            (None, _, _) => Self::Local,
            (Some(_), Some(_), _) | (Some(_), _, Some(Auth::Credential(_))) => Self::Remote,
            (Some(_), None, Some(_)) => Self::LocalFirstWaiting(DEFAULT_FIRST_LOAD_REMOTE_WAIT),
            (Some(_), None, None) => Self::Local,
        }
    }

    pub(crate) fn opts<T: Row>(self, query: &Query<T>) -> ReadOpts {
        match self {
            Self::Local => read_opts(query, ReadTier::LocalFirst),
            Self::Remote => read_opts(query, ReadTier::Remote),
            Self::LocalFirstWaiting(wait) => {
                let mut opts = read_opts(query, ReadTier::LocalFirst);
                opts.first_load = jazz::db::FirstLoad::WaitForRemote {
                    timeout_ms: wait.as_millis() as u64,
                };
                opts
            }
        }
    }
}

/// The read options of `query` at `tier`.
pub(crate) fn read_opts<T: Row>(query: &Query<T>, tier: ReadTier) -> ReadOpts {
    let mut opts = ReadOpts::for_read_tier(tier);
    opts.read_view.include_deleted = query.reads_deleted();
    opts
}

/// Inserts `new`, returning it as stored and its write (none when staged in
/// a transaction).
pub(crate) fn insert<N: Insert>(
    client: &JazzClient,
    new: N,
    implementations: &Implementations,
) -> Result<(N::Row, Option<TransactionId>)> {
    let name = <N::Row as Table>::NAME;
    let (id, values, written) = client.insert(name, new.into_cells(implementations)?)?;
    let schema = client.schema()?;
    let table = schema
        .get(&TableName::new(name))
        .ok_or_else(|| Error::App(format!("the app has no table {name}")))?;
    let fields = table
        .columns
        .columns
        .iter()
        .zip(values)
        .map(|(column, value)| jazz::tools::QueryResultField {
            name: column.name.as_str().to_owned(),
            value,
        })
        .collect();
    Ok((table::decode(id, fields, implementations)?, written))
}

pub(crate) fn decode_result<T: Row>(
    row: QueryResult,
    implementations: &Implementations,
) -> Result<T> {
    let id = row.key.row_id().ok_or_else(|| Error::Decode {
        table: <T::Table as Table>::NAME,
        message: "a joined result is not a row".into(),
    })?;
    table::decode(id, row.fields, implementations)
}

/// Which link a backend opens.
#[derive(Clone, Copy, PartialEq, Eq)]
#[cfg_attr(not(feature = "unstable-endpoint"), allow(dead_code))]
enum Link {
    /// Its own: the session of the user it acts as, pinned to its bundle.
    Own,
    /// Its endpoint's, trusted to act for its users (see
    /// [`Jazz::open_for_users`]).
    TrustedForUsers,
}

fn run(
    files: AppFiles,
    config: Config,
    link: Link,
    store: Option<crate::lifecycle::StoreDir>,
    mut commands: mpsc::UnboundedReceiver<Command>,
    ready: oneshot::Sender<Result<Opened>>,
    stopper: mpsc::WeakUnboundedSender<Command>,
) {
    let client_serverless = config.server_url.is_none();
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            let _ = ready.send(Err(Error::Config(format!(
                "cannot start the client runtime: {error}"
            ))));
            return;
        }
    };
    let local = tokio::task::LocalSet::new();
    let stopped = local.block_on(&runtime, async move {
        let (client, opened) = match connect(files, config, link, stopper).await {
            Ok(opened) => opened,
            Err(error) => {
                let _ = ready.send(Err(error));
                return None;
            }
        };
        if ready.send(Ok(opened)).is_err() {
            let _ = client.shutdown().await;
            return None;
        }
        // Refused writes no durable wait claimed go to `mutation_errors`.
        if !client_serverless {
            client.track_mutation_errors();
            tokio::task::spawn_local(crate::lifecycle::report_mutation_errors(client.clone()));
        }
        // Jobs run as local tasks, so a slow one never holds up the rest.
        // Subscription forwarders are spawned by their jobs and are not in
        // here: they end with the client.
        let mut jobs = tokio::task::JoinSet::new();
        let mut stopped = Vec::new();
        loop {
            tokio::select! {
                command = commands.recv() => match command {
                    Some(Command::Run(job)) => {
                        jobs.spawn_local(job(client.clone()));
                    }
                    Some(Command::Shutdown(reply)) => {
                        stopped.push(reply);
                        break;
                    }
                    None => break,
                },
                Some(_) = jobs.join_next(), if !jobs.is_empty() => {}
            }
        }
        // Nothing new starts: a call made from here on (even by a job that
        // is draining) fails with `Stopped` instead of waiting on a job
        // that never runs.
        commands.close();
        let drain = async {
            let mut open = true;
            loop {
                tokio::select! {
                    command = commands.recv(), if open => match command {
                        // Dropping the job drops its reply: `Stopped`.
                        Some(Command::Run(job)) => drop(job),
                        Some(Command::Shutdown(reply)) => stopped.push(reply),
                        None => open = false,
                    },
                    joined = jobs.join_next() => {
                        if joined.is_none() {
                            break;
                        }
                    }
                }
            }
        };
        // The jobs already started finish, within a limit: a remote read
        // waiting on an unreachable server must not hold shutdown forever.
        if tokio::time::timeout(SHUTDOWN_DRAIN, drain).await.is_err() {
            jobs.abort_all();
            while jobs.join_next().await.is_some() {}
        }
        // No job is left: roll back the transactions still open while the
        // client runs, so none is dropped with the thread's locals.
        crate::transaction::roll_back_parked();
        let result = client.shutdown().await;
        Some((result, stopped))
    });
    // The store's connections close with the last clone of the client, and
    // the tasks holding clones go with the `LocalSet`: only once both are
    // gone is the store closed, for `delete_client_storage` to remove (a
    // file it unlinks while SQLite still maps it is a SIGBUS).
    drop(local);
    drop(runtime);
    drop(store);
    let Some((result, stopped)) = stopped else {
        return;
    };
    let failure = result.as_ref().err().map(ToString::to_string);
    let mut replies = stopped.into_iter();
    if let Some(first) = replies.next() {
        let _ = first.send(result.map_err(Error::from));
    }
    // Every other caller learns the same outcome.
    for reply in replies {
        let _ = reply.send(match &failure {
            None => Ok(()),
            Some(error) => Err(Error::Server(format!("shutting down: {error}"))),
        });
    }
}

pub(crate) fn panic_message(panic: &(dyn std::any::Any + Send)) -> String {
    panic
        .downcast_ref::<&str>()
        .map(|message| (*message).to_owned())
        .or_else(|| panic.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "a panic without a message".to_owned())
}

struct Opened {
    bundle: Option<String>,
    account: Option<AccountId>,
    session: Option<UserSession>,
    sync: watch::Receiver<SyncStatus>,
    backend: Option<BackendLink>,
    backend_client: Option<crate::backend_client::BackendClientLink>,
    memory: bool,
}

async fn connect(
    files: AppFiles,
    config: Config,
    link_kind: Link,
    stopper: mpsc::WeakUnboundedSender<Command>,
) -> Result<(JazzClient, Opened)> {
    let mut loaded = files.load()?;
    // The endpoint's client evaluates the sessions it acts for locally too:
    // its schema carries the permissions the backend publishes, as
    // `createJazz`'s backend session compiles them. Policies are not part
    // of the schema's hash.
    // So does a backend client, for the users it acts for (`for_user`), and
    // a scoped one for its own user: TS `createJazzClient` takes the app's
    // permissions for the same reason.
    let backend_client = matches!(config.user, Some(Auth::Credential(_)));
    let folds_permissions = link_kind == Link::TrustedForUsers || backend_client;
    if let (true, Some(app)) = (folds_permissions, &loaded.backend) {
        for (table, policies) in app.permissions().iter() {
            if let Some(schema) = loaded.schema.get_mut(table) {
                schema.policies = policies.clone();
            }
        }
    }
    let (storage, data_dir) = match config.storage {
        Storage::Memory => (ClientStorage::Memory, std::env::temp_dir()),
        Storage::Dir(dir) => (ClientStorage::Persistent, dir),
    };
    let mut context = AppContext {
        app_id: config.app_id,
        client_id: None,
        schema: loaded.schema,
        server_url: config.server_url.clone().unwrap_or_default(),
        data_dir,
        storage,
        account_id: None,
        jwt_token: None,
        backend_secret: None,
        backend_session: None,
        bundle: None,
        admin_secret: None,
        migrations: loaded.migrations,
    };
    let mut bundle = None;
    let mut admitted = None;
    let mut link = None;
    if let (Some(backend), Some(server), Some(app)) =
        (&config.backend, &config.server_url, &loaded.backend)
    {
        let published = backend::publish(server, config.app_id, backend, app).await?;
        context.backend_secret = Some(backend.backend_secret.clone());
        if link_kind == Link::Own {
            let account = backend::enrol(server, config.app_id, backend, BACKEND_ISSUER).await?;
            let mut session = Session::new(BACKEND_ISSUER, backend.user_id.clone());
            session.account_id = Some(account);
            context.account_id = Some(account);
            admitted = Some(account);
            context.backend_session = Some(session);
        }
        context.bundle = Some(published.clone());
        bundle = Some(published);
        link = Some(BackendLink {
            server: server.clone(),
            app_id: config.app_id,
            backend_secret: backend.backend_secret.clone(),
            admin_secret: backend.admin_secret.clone(),
        });
    }
    let connector = Arc::new(jazz_native_transport::NativeWebSocketConnector);
    // A main backend (TS `createJazz`) may not upgrade sockets where it is
    // hosted: its clients fall back to its HTTP stream, as TS clients do.
    let endpoint_connector = Arc::new(jazz_native_transport::HttpStreamFallbackConnector::new());
    let memory = context.storage == ClientStorage::Memory;
    let mut backend_client = None;
    let (client, user) = match (config.user, &config.server_url) {
        (Some(Auth::Credential(credential)), Some(endpoint)) => {
            let (client, user, credential, link) =
                crate::backend_client::connect(context, endpoint_connector, credential, endpoint)
                    .await?;
            if !user.account.is_system() {
                admitted = Some(user.account);
            }
            backend_client = Some(link);
            (client, Some((user, credential)))
        }
        (Some(auth), Some(endpoint)) => {
            let (client, user, credential) =
                connect_user(context, endpoint_connector, auth, endpoint).await?;
            admitted = Some(user.account);
            (client, Some((user, credential)))
        }
        _ if context.server_url.is_empty() => (JazzClient::connect(context).await?, None),
        _ => (
            JazzClient::connect_with_native_transport(context, connector).await?,
            None,
        ),
    };
    let sync = user_session::watch_sync(client.clone());
    let session = user.map(|(user, credential)| {
        let stop = Box::new(move || {
            if let Some(commands) = stopper.upgrade() {
                let (reply, _) = oneshot::channel();
                let _ = commands.send(Command::Shutdown(reply));
            }
        });
        user_session::start(
            client.clone(),
            user,
            credential,
            config.server_url.as_deref().unwrap_or_default(),
            sync.clone(),
            stop,
        )
    });
    Ok((
        client,
        Opened {
            bundle,
            account: admitted,
            session,
            sync,
            backend: link,
            backend_client,
            memory,
        },
    ))
}

/// Connects as an end user through the app's Jazz endpoint, as a browser
/// does: a device account presents its own proof, which the backend vouches
/// for; a bearer's user is signed in first, then states their session with
/// the bearer on the upgrade, which the backend checks it against.
async fn connect_user(
    mut context: AppContext,
    connector: Arc<jazz_native_transport::HttpStreamFallbackConnector>,
    auth: Auth,
    endpoint: &str,
) -> Result<(JazzClient, User, Credential)> {
    let app = context.app_id;
    match auth {
        Auth::Device(key) => {
            let account = key.account(app);
            context.jwt_token = Some(key.mint(app, DEVICE_TOKEN_TTL_SECS)?);
            context.account_id = Some(account);
            let client = JazzClient::connect_with_native_transport(context, connector).await?;
            let user = User {
                account,
                issuer: jazz::tools::identity::LOCAL_FIRST_ISSUER.to_owned(),
                subject: key.user_id(),
                claims: serde_json::json!({}),
                kind: UserKind::Device,
            };
            Ok((client, user, Credential::Device { key, app }))
        }
        Auth::Credential(_) => unreachable!("a backend client connects through backend_client"),
        Auth::Bearer(provider) => {
            let token = provider
                .token(TokenReason::Connect)
                .await?
                .ok_or(Error::SignedOut)?;
            let signed_in = backend::sign_in(endpoint, app, &token)
                .await?
                .ok_or(Error::SignedOut)?;
            let mut session = Session::new(&signed_in.identity.issuer, &signed_in.identity.subject)
                .with_claims(signed_in.claims.clone());
            session.account_id = Some(signed_in.account);
            context.account_id = Some(signed_in.account);
            context.backend_session = Some(session);
            let client =
                JazzClient::connect_with_native_transport_bearer(context, connector, token.clone())
                    .await?;
            let user = User {
                account: signed_in.account,
                issuer: signed_in.identity.issuer,
                subject: signed_in.identity.subject,
                claims: signed_in.claims,
                kind: UserKind::Backend,
            };
            Ok((client, user, Credential::Bearer { provider, token }))
        }
    }
}

#[allow(dead_code)]
fn _assert_send_clone()
where
    Jazz: Send + Sync + Clone,
{
}
