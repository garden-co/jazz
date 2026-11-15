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
use jazz::tools::{
    AppContext, ClientStorage, JazzClient, ObjectId, QueryResult, ReadTier, Session, TableName,
};
use tokio::sync::{mpsc, oneshot, watch};

use crate::app::AppFiles;
use crate::auth::{Auth, DEVICE_TOKEN_TTL_SECS, TokenReason};
use crate::backend;
use crate::config::{Config, Storage};
use crate::error::{Error, Result};
use crate::query::Query;
use crate::session::{
    self as user_session, Credential, Session as UserSession, SyncStatus, User, UserKind,
};
use crate::subscription::{self, Subscription};
use crate::table::{self, Implementations, Insert, Table, Update};

type Job = Box<dyn FnOnce(JazzClient) -> Pin<Box<dyn Future<Output = ()>>> + Send>;

enum Command {
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
    commands: mpsc::UnboundedSender<Command>,
    pub(crate) info: Arc<Info>,
}

pub(crate) struct Info {
    bundle: Option<String>,
    account: Option<AccountId>,
    implementations: Implementations,
    /// An end user's session ([`Config::user`]).
    pub(crate) session: Option<UserSession>,
    pub(crate) sync: watch::Receiver<SyncStatus>,
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
        let files = match config.backend.as_ref().and_then(|b| b.permissions.clone()) {
            Some(permissions) => files.with_permissions(permissions),
            None => files,
        };
        if config.backend.is_some() && config.server_url.is_none() {
            return Err(Error::Config(
                "a backend needs the sync server's URL (Config::server)".into(),
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
        if config.server_url.is_some() && config.backend.is_none() && config.user.is_none() {
            return Err(Error::Config(
                "a server needs someone to connect as: the app's backend (Config::backend) \
                 or an end user (Config::user)"
                    .into(),
            ));
        }
        let (commands, receiver) = mpsc::unbounded_channel();
        let (ready, opened) = oneshot::channel();
        std::thread::Builder::new()
            .name("jazz-client".into())
            .spawn(move || run(files, config, receiver, ready))
            .map_err(|error| Error::Config(format!("cannot start the client thread: {error}")))?;
        let Opened {
            bundle,
            account,
            session,
            sync,
        } = opened.await.map_err(|_| Error::Stopped)??;
        Ok(Self {
            commands,
            info: Arc::new(Info {
                bundle,
                account,
                implementations,
                session,
                sync,
            }),
        })
    }

    /// The permissions bundle this backend published and is pinned to.
    pub fn bundle(&self) -> Option<&str> {
        self.info.bundle.as_deref()
    }

    /// The account of the user this backend acts as, or of the end user
    /// signed in.
    pub fn account(&self) -> Option<AccountId> {
        self.info.account
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
        let job: Job = Box::new(move |client| {
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
            .map_err(|_| Error::Stopped)?;
        answer.await.map_err(|_| Error::Stopped)?
    }

    /// Every row `query` matches, reading local data first.
    pub async fn all<T: Table>(&self, query: Query<T>) -> Result<Vec<T>> {
        self.all_with(query, ReadTier::LocalFirst).await
    }

    /// Every row `query` matches, read at `tier`.
    pub async fn all_with<T: Table>(&self, query: Query<T>, tier: ReadTier) -> Result<Vec<T>> {
        let query = query.into_untyped();
        let implementations = self.info.implementations.clone();
        self.with_client(move |client| async move {
            let rows = client.query(query, tier).await?;
            rows.into_iter()
                .map(|row| decode_result::<T>(row, &implementations))
                .collect()
        })
        .await
    }

    /// The first row `query` matches.
    pub async fn one<T: Table>(&self, query: Query<T>) -> Result<Option<T>> {
        Ok(self.all(query.limit(1)).await?.into_iter().next())
    }

    /// The row of `T` with this id.
    pub async fn get<T: Table>(&self, id: ObjectId) -> Result<Option<T>> {
        self.one(Query::<T>::all().id(id)).await
    }

    /// Inserts a row, committing it locally. Returns it as stored.
    pub async fn insert<N: Insert>(&self, new: N) -> Result<N::Row> {
        let implementations = self.info.implementations.clone();
        self.with_client(move |client| async move { insert(&client, new, &implementations) })
            .await
    }

    /// Updates the row with this id.
    pub async fn update<U: Update>(&self, id: ObjectId, update: U) -> Result<()> {
        let implementations = self.info.implementations.clone();
        self.with_client(move |client| async move {
            let cells = update.into_cells(&implementations)?;
            client.update(<U::Row as Table>::NAME, id, cells)?;
            Ok(())
        })
        .await
    }

    /// Deletes the row of `T` with this id.
    pub async fn delete<T: Table>(&self, id: ObjectId) -> Result<()> {
        self.with_client(move |client| async move {
            client.delete(T::NAME, id)?;
            Ok(())
        })
        .await
    }

    /// Runs `writes` in a serializable transaction: committed if it returns
    /// `Ok` (and wrote anything), rolled back if it returns `Err`.
    pub async fn transaction<F, R>(&self, writes: F) -> Result<R>
    where
        F: FnOnce(&Transaction) -> Result<R> + Send + 'static,
        R: Send + 'static,
    {
        let implementations = self.info.implementations.clone();
        self.with_client(move |client| async move {
            let transaction = Transaction {
                inner: client.begin_transaction()?,
                wrote: std::cell::Cell::new(false),
                implementations,
            };
            match writes(&transaction) {
                Ok(result) => {
                    if transaction.wrote.get() {
                        transaction.inner.commit()?;
                    } else {
                        transaction.inner.rollback()?;
                    }
                    Ok(result)
                }
                Err(error) => {
                    transaction.inner.rollback()?;
                    Err(error)
                }
            }
        })
        .await
    }

    /// Subscribes to `query`: a stream of its changes, starting with the
    /// rows it matches now.
    pub async fn subscribe<T: Table>(&self, query: Query<T>) -> Result<Subscription<T>> {
        self.subscribe_with(query, ReadTier::LocalFirst).await
    }

    /// [`Jazz::subscribe`] at `tier`.
    pub async fn subscribe_with<T: Table>(
        &self,
        query: Query<T>,
        tier: ReadTier,
    ) -> Result<Subscription<T>> {
        let query = query.into_untyped();
        let (sender, receiver) = mpsc::unbounded_channel();
        let implementations = self.info.implementations.clone();
        self.with_client(move |client| async move {
            let stream = client.subscribe_with_read_tier(query, tier).await?;
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
    pub async fn shutdown(&self) -> Result<()> {
        let (reply, done) = oneshot::channel();
        if self.commands.send(Command::Shutdown(reply)).is_err() {
            return Ok(());
        }
        done.await.unwrap_or(Ok(()))
    }
}

/// Writes staged in a [`Jazz::transaction`].
pub struct Transaction {
    inner: jazz::tools::JazzTransaction,
    wrote: std::cell::Cell<bool>,
    implementations: Implementations,
}

impl Transaction {
    pub fn insert<N: Insert>(&self, new: N) -> Result<N::Row> {
        let row = insert(self.inner.client(), new, &self.implementations)?;
        self.wrote.set(true);
        Ok(row)
    }

    pub fn update<U: Update>(&self, id: ObjectId, update: U) -> Result<()> {
        let cells = update.into_cells(&self.implementations)?;
        self.inner.update(<U::Row as Table>::NAME, id, cells)?;
        self.wrote.set(true);
        Ok(())
    }

    pub fn delete<T: Table>(&self, id: ObjectId) -> Result<()> {
        self.inner.delete(T::NAME, id)?;
        self.wrote.set(true);
        Ok(())
    }
}

fn insert<N: Insert>(
    client: &JazzClient,
    new: N,
    implementations: &Implementations,
) -> Result<N::Row> {
    let name = <N::Row as Table>::NAME;
    let (id, values, _) = client.insert(name, new.into_cells(implementations)?)?;
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
    table::decode(id, fields, implementations)
}

fn decode_result<T: Table>(row: QueryResult, implementations: &Implementations) -> Result<T> {
    let id = row.key.row_id().ok_or_else(|| Error::Decode {
        table: T::NAME,
        message: "a joined result is not a row".into(),
    })?;
    table::decode(id, row.fields, implementations)
}

fn run(
    files: AppFiles,
    config: Config,
    mut commands: mpsc::UnboundedReceiver<Command>,
    ready: oneshot::Sender<Result<Opened>>,
) {
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
    local.block_on(&runtime, async move {
        let (client, opened) = match connect(files, config).await {
            Ok(opened) => opened,
            Err(error) => {
                let _ = ready.send(Err(error));
                return;
            }
        };
        if ready.send(Ok(opened)).is_err() {
            let _ = client.shutdown().await;
            return;
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
        let result = client.shutdown().await;
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
    });
}

fn panic_message(panic: &(dyn std::any::Any + Send)) -> String {
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
}

async fn connect(files: AppFiles, config: Config) -> Result<(JazzClient, Opened)> {
    let loaded = files.load()?;
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
    if let (Some(backend), Some(server), Some(app)) =
        (&config.backend, &config.server_url, &loaded.backend)
    {
        let published = backend::publish(server, config.app_id, backend, app).await?;
        let account = backend::enrol(server, config.app_id, backend, BACKEND_ISSUER).await?;
        let mut session = Session::new(BACKEND_ISSUER, backend.user_id.clone());
        session.account_id = Some(account);
        context.account_id = Some(account);
        admitted = Some(account);
        context.backend_secret = Some(backend.backend_secret.clone());
        context.backend_session = Some(session);
        context.bundle = Some(published.clone());
        bundle = Some(published);
    }
    let connector = Arc::new(jazz_native_transport::NativeWebSocketConnector);
    let (client, user) = match (config.user, &config.server_url) {
        (Some(auth), Some(endpoint)) => {
            let (client, user, credential) =
                connect_user(context, connector, auth, endpoint).await?;
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
        user_session::start(client.clone(), user, credential, sync.clone())
    });
    Ok((
        client,
        Opened {
            bundle,
            account: admitted,
            session,
            sync,
        },
    ))
}

/// Connects as an end user through the app's Jazz endpoint, as a browser
/// does: a device account presents its own proof, which the backend vouches
/// for; a bearer's user is signed in first, then states their session with
/// the bearer on the upgrade, which the backend checks it against.
async fn connect_user(
    mut context: AppContext,
    connector: Arc<jazz_native_transport::NativeWebSocketConnector>,
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
