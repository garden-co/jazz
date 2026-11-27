//! The store's SQL owner: one connection and its transactions together.
//!
//! A [`Storage`] runs the store's SQL, and its only transactions are
//! callback scopes ([`Storage::scope`]): nothing stays open after the call
//! that opened it returns. That is the one shape every storage Jazz runs on
//! supports, including Cloudflare Durable Objects, whose SQL
//! (`ctx.storage.sql`) offers no `BEGIN` or `SAVEPOINT`, only
//! `transactionSync(fn)` with a synchronous `fn`.
//!
//! Two owners:
//! - native: a rusqlite connection, its statements cached and its rows
//!   streamed;
//! - managed: a host's SQL ([`Backend`], e.g. a Durable Object's
//!   `ctx.storage.sql`), which binds typed cells and walks a forward-only
//!   cursor. Its only transaction is [`Backend::transaction`].
//!
//! Statements, rows and parameters ([`Statement`], [`Row`], [`Params`])
//! are this module's own, with rusqlite's method names, so jazz-db reads
//! the same on either.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::options::Error;

pub use crate::values::Sql as Value;
mod cell;
pub use cell::{Cell, CellError, FromCell, ToSql};
mod error;
pub use error::{OptionalExtension, SqlError, SqlResult};

// SQLite result codes are part of the managed backend contract too.
const SQLITE_ABORT: i32 = 4;
pub(crate) const SQLITE_INTERRUPT: i32 = 9;
const SQLITE_CONSTRAINT_COMMITHOOK: i32 = 531;

#[derive(Clone, Copy)]
pub enum StatementStatus {
    FullscanStep,
    Sort,
    VmStep,
    RePrepare,
}

#[cfg(feature = "sqlite")]
impl From<StatementStatus> for rusqlite::StatementStatus {
    fn from(status: StatementStatus) -> Self {
        match status {
            StatementStatus::FullscanStep => Self::FullscanStep,
            StatementStatus::Sort => Self::Sort,
            StatementStatus::VmStep => Self::VmStep,
            StatementStatus::RePrepare => Self::RePrepare,
        }
    }
}

/// A managed backend's threading: native hosts share it across threads;
/// a wasm32 host (a Durable Object) has one.
#[cfg(not(target_arch = "wasm32"))]
pub trait BackendThreading: Send + Sync {}
#[cfg(not(target_arch = "wasm32"))]
impl<T: Send + Sync> BackendThreading for T {}
/// A managed backend's threading: native hosts share it across threads;
/// a wasm32 host (a Durable Object) has one.
#[cfg(target_arch = "wasm32")]
pub trait BackendThreading {}
#[cfg(target_arch = "wasm32")]
impl<T> BackendThreading for T {}

/// The backend supplied to the body is the transaction connection. A host
/// may supply a different connection for each nested scope.
pub type TransactionBody<'a> =
    Box<dyn FnOnce(Arc<dyn Backend>) -> futures::future::LocalBoxFuture<'a, bool> + 'a>;

/// Execute a transaction on a host with a synchronous transaction callback.
/// An unexpected suspension refuses the transaction before control returns to the host.
pub fn transaction_sync(
    backend: Arc<dyn Backend>,
    body: TransactionBody<'_>,
    run: impl FnOnce(&mut dyn FnMut() -> bool) -> SqlResult<()>,
) -> SqlResult<()> {
    use futures::FutureExt;
    let mut body = Some(body);
    let mut completed = false;
    run(&mut || {
        let result =
            body.take().expect("transaction body called twice")(backend.clone()).now_or_never();
        completed = result.is_some();
        result.unwrap_or(false)
    })?;
    if completed {
        Ok(())
    } else {
        Err(backend_error(
            SQLITE_ABORT,
            "synchronous transaction body did not complete".into(),
        ))
    }
}

/// A host's SQL, for a managed [`Storage`]: statements run one at a time
/// on one connection. Its only transaction is [`Backend::transaction`],
/// which supplies a scoped backend and runs `body` once. It commits when the
/// body completes with `true` and rolls back on `false`, cancellation or panic.
/// Nested transactions use the backend supplied to the enclosing callback.
/// Synchronous hosts can use [`transaction_sync`]. Transaction control in SQL text
/// (`BEGIN`, `SAVEPOINT`, …) is refused.
#[async_trait::async_trait(?Send)]
pub trait Backend: BackendThreading {
    /// Run a query; its rows, through a forward-only cursor.
    async fn query(&self, sql: &str, parameters: &[Value]) -> SqlResult<Box<dyn Cursor + '_>>;
    /// Run a statement; how many rows it changed.
    async fn execute(&self, sql: &str, parameters: &[Value]) -> SqlResult<usize>;
    /// Run statements with no parameters and no results.
    async fn execute_script(&self, sql: &str) -> SqlResult<()>;
    /// Run `body` atomically (see the trait's documentation).
    async fn transaction(&self, body: TransactionBody<'_>) -> SqlResult<()>;
    /// The rowid of the last row inserted.
    async fn last_insert_rowid(&self) -> SqlResult<i64>;
    /// How many rows all statements so far changed.
    async fn total_changes(&self) -> SqlResult<u64>;
    /// The most `?` parameters a statement may bind.
    fn max_parameters(&self) -> usize;
}

/// A query's rows, one at a time. A host must not materialize an
/// unbounded result set to implement it.
#[async_trait::async_trait(?Send)]
pub trait Cursor {
    /// The next row's cells, or `None` past the last.
    async fn next(&mut self) -> SqlResult<Option<&[Value]>>;
    /// The result's column names.
    fn columns(&self) -> &[String];
}

/// A managed store's commit and rollback hooks, and how deep its scopes are
/// nested: the hooks run as the outermost scope ends, as SQLite's do as a
/// transaction ends.
#[derive(Default)]
struct Hooks {
    depth: usize,
    commit: Option<Box<dyn FnMut() -> bool + Send>>,
    rollback: Option<Box<dyn FnMut() + Send>>,
}

// Dropping an in-flight host command can leave Jazz's in-memory bookkeeping
// ahead of the transaction that the host rolls back. All handles of this open
// store must refuse further SQL until Jazz reconstructs that state on reopen.
async fn checked_command<T>(
    interrupted: &AtomicBool,
    command: impl Future<Output = SqlResult<T>>,
) -> SqlResult<T> {
    if interrupted.load(Ordering::Acquire) {
        return Err(backend_error(
            SQLITE_ABORT,
            "SQL operation interrupted; reopen the database".into(),
        ));
    }
    struct InFlight<'a>(Option<&'a AtomicBool>);
    impl Drop for InFlight<'_> {
        fn drop(&mut self) {
            if let Some(interrupted) = self.0 {
                interrupted.store(true, Ordering::Release);
            }
        }
    }
    let mut command = std::pin::pin!(command);
    let mut guard = InFlight(None);
    std::future::poll_fn(|cx| {
        guard.0 = None;
        let result = command.as_mut().poll(cx);
        if result.is_pending() {
            guard.0 = Some(interrupted);
        }
        result
    })
    .await
}

#[derive(Clone)]
struct Managed {
    backend: SharedBackend,
    interrupted: Arc<AtomicBool>,
    hooks: Arc<Mutex<Hooks>>,
}

/// A managed backend, shared by its store and the store's chunk cache,
/// which the store's commit and rollback hooks (`Send`) hold. Native
/// backends are `Send + Sync` ([`BackendThreading`]); a wasm32 host
/// without threads has one thread, so nothing can send or share it
/// elsewhere. With wasm threads this does not compile, rather than assume.
#[derive(Clone)]
struct SharedBackend(Arc<dyn Backend>);

// SAFETY: wasm32 without the `atomics` target feature runs one thread.
#[cfg(all(target_arch = "wasm32", not(target_feature = "atomics")))]
unsafe impl Send for SharedBackend {}
// SAFETY: as above.
#[cfg(all(target_arch = "wasm32", not(target_feature = "atomics")))]
unsafe impl Sync for SharedBackend {}

impl std::ops::Deref for SharedBackend {
    type Target = dyn Backend;

    fn deref(&self) -> &(dyn Backend + 'static) {
        &*self.0
    }
}

// A Db owns its Storage for its entire lifetime. While polling a Jazz operation
// inside a host transaction, statements issued through that owner (including its
// chunk reader) must use the transaction connection. Restore the binding before
// returning Pending, so a different future polled on the thread cannot inherit it.
thread_local! {
    static TRANSACTION_CONNECTIONS: std::cell::RefCell<Vec<(usize, Arc<dyn Backend>)>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

impl SharedBackend {
    fn key(&self) -> usize {
        Arc::as_ptr(&self.0) as *const () as usize
    }

    fn current(&self) -> Arc<dyn Backend> {
        TRANSACTION_CONNECTIONS.with_borrow(|connections| {
            connections
                .iter()
                .rev()
                .find(|(key, _)| *key == self.key())
                .map_or_else(|| self.0.clone(), |(_, backend)| backend.clone())
        })
    }

    async fn with_transaction<T>(
        &self,
        backend: &SharedBackend,
        future: impl Future<Output = T>,
    ) -> T {
        let mut future = std::pin::pin!(future);
        std::future::poll_fn(|cx| {
            struct Restore;
            impl Drop for Restore {
                fn drop(&mut self) {
                    TRANSACTION_CONNECTIONS.with_borrow_mut(|connections| {
                        connections.pop();
                    });
                }
            }
            TRANSACTION_CONNECTIONS.with_borrow_mut(|connections| {
                connections.push((self.key(), backend.0.clone()));
            });
            let _restore = Restore;
            future.as_mut().poll(cx)
        })
        .await
    }
}

impl Managed {
    /// Run `body` in one [`Backend::transaction`]: kept if it returns
    /// `true` and, at the outermost scope, the commit hook does not veto
    /// it. Whether it was kept.
    async fn scope<'a>(&'a self, body: TransactionBody<'a>) -> SqlResult<bool> {
        let outer = {
            let mut hooks = self.hooks.lock().expect("hooks");
            hooks.depth += 1;
            hooks.depth == 1
        };
        // A body that panics (a native host may catch it) still leaves the
        // scope: its depth goes back and, at the outermost scope, the
        // rollback hook runs (the backend rolled it back), so the store is
        // neither left open nor its chunk cache pinned.
        struct Leave<'a> {
            hooks: &'a Mutex<Hooks>,
            outer: bool,
            unwinding: bool,
        }
        impl Drop for Leave<'_> {
            fn drop(&mut self) {
                let Ok(mut hooks) = self.hooks.lock() else {
                    return;
                };
                hooks.depth -= 1;
                if self.unwinding
                    && self.outer
                    && let Some(hook) = hooks.rollback.as_mut()
                {
                    hook();
                }
            }
        }
        let mut leave = Leave {
            hooks: &self.hooks,
            outer,
            unwinding: true,
        };
        let mut keep = false;
        let kept = &mut keep;
        let connection = self.backend.current();
        let result = checked_command(
            &self.interrupted,
            connection.transaction(Box::new(move |backend| {
                Box::pin(async move {
                    *kept = body(backend).await;
                    if *kept && outer {
                        let mut hooks = self.hooks.lock().expect("hooks");
                        *kept = !hooks.commit.as_mut().is_some_and(|hook| hook());
                    }
                    *kept
                })
            })),
        )
        .await;
        leave.unwinding = false;
        drop(leave);
        let mut hooks = self.hooks.lock().expect("hooks");
        if outer
            && (!keep || result.is_err())
            && let Some(hook) = hooks.rollback.as_mut()
        {
            hook();
        }
        result.map(|()| keep)
    }
}

/// One SQL owner, whose only transactions are callback scopes.
pub struct Storage {
    owner: Owner,
    key: usize,
}

enum Owner {
    #[cfg(feature = "sqlite")]
    Native(rusqlite::Connection),
    Managed(Managed),
}

impl Storage {
    fn new(owner: Owner) -> Self {
        static KEYS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(1);
        Self {
            owner,
            key: KEYS.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        }
    }

    /// Own a native SQLite connection.
    #[cfg(feature = "sqlite")]
    pub fn native(conn: rusqlite::Connection) -> Self {
        Self::new(Owner::Native(conn))
    }

    /// Run on a host's SQL ([`Backend`]).
    pub fn from_backend(backend: impl Backend + 'static) -> Self {
        Self::new(Owner::Managed(Managed {
            backend: SharedBackend(Arc::new(backend)),
            interrupted: Arc::default(),
            hooks: Arc::default(),
        }))
    }

    /// A second handle on a managed store's backend, for the chunk cache to
    /// read what it lacks: a Durable Object has one connection, which the
    /// cache shares (no hooks of its own; it sees the owner's open scope,
    /// whose writes the cache already holds pinned). `None` on native
    /// storage, where a file store opens its own read-only connection.
    pub(crate) fn shared_reader(&self) -> Option<Storage> {
        match &self.owner {
            #[cfg(feature = "sqlite")]
            Owner::Native(_) => None,
            Owner::Managed(managed) => Some(Storage::new(Owner::Managed(Managed {
                backend: managed.backend.clone(),
                interrupted: managed.interrupted.clone(),
                hooks: Arc::default(),
            }))),
        }
    }

    /// Whether this store runs on a host's SQL ([`Storage::from_backend`]),
    /// which keeps nothing open between calls: no savepoint can outlive the
    /// call that opened it.
    pub fn is_managed(&self) -> bool {
        matches!(self.owner, Owner::Managed(_))
    }

    /// Bind SQL issued by Db helpers to the backend supplied to this transaction.
    /// Public storage callers receive the scoped handle explicitly.
    async fn in_db_scope<T>(&self, scoped: &Self, body: impl Future<Output = T>) -> T {
        match (&self.owner, &scoped.owner) {
            (Owner::Managed(root), Owner::Managed(tx)) => {
                root.backend.with_transaction(&tx.backend, body).await
            }
            #[cfg(feature = "sqlite")]
            _ => body.await,
        }
    }

    pub(crate) async fn db_scope<T>(
        &self,
        name: &str,
        body: impl AsyncFnOnce() -> Result<T, Error>,
    ) -> Result<T, Error> {
        self.scope(name, async |scoped| self.in_db_scope(scoped, body()).await)
            .await
    }

    pub(crate) async fn db_scope_then<T>(
        &self,
        name: &str,
        body: impl AsyncFnOnce() -> Scoped<T>,
    ) -> Result<T, Error> {
        self.scope_then(name, async |scoped| self.in_db_scope(scoped, body()).await)
            .await
    }

    /// Run `f` atomically: what it wrote is kept if it returns `Ok`, and
    /// undone if it returns `Err`. Scopes nest; an inner scope's writes
    /// commit with the outermost one. `name` labels the scope (on native
    /// SQLite, its savepoint).
    ///
    /// Run SQL through the storage supplied to `f`. An asynchronous host can
    /// suspend during the transaction. A synchronous host rolls back if `f` yields.
    pub async fn scope<T>(
        &self,
        name: &str,
        f: impl AsyncFnOnce(&Storage) -> Result<T, Error>,
    ) -> Result<T, Error> {
        self.scope_then(name, async |storage| match f(storage).await {
            Ok(value) => Scoped::Keep(Ok(value)),
            Err(error) => Scoped::Undo(Err(error)),
        })
        .await?
    }

    /// Run `f` atomically, keeping or undoing what it wrote as it decides
    /// ([`Scoped`]), and return its value either way: for a dry run (undo
    /// always) or a unit whose failure carries more than an [`Error`].
    /// `Err` only when the scope itself cannot open or close.
    pub async fn scope_then<T>(
        &self,
        name: &str,
        f: impl AsyncFnOnce(&Storage) -> Scoped<T>,
    ) -> Result<T, Error> {
        match &self.owner {
            #[cfg(feature = "sqlite")]
            Owner::Native(_) => {
                crate::values::begin_savepoint(self, name)?;
                struct NativeScope<'a> {
                    conn: &'a Storage,
                    name: &'a str,
                    open: bool,
                }
                impl Drop for NativeScope<'_> {
                    fn drop(&mut self) {
                        if self.open {
                            let _ = crate::values::end_savepoint(self.conn, self.name, false);
                        }
                    }
                }
                let mut scope = NativeScope {
                    conn: self,
                    name,
                    open: true,
                };
                // Erase the body at the transaction boundary so nested core operations
                // do not embed the entire transaction future in every caller.
                let body: futures::future::LocalBoxFuture<'_, Scoped<T>> = Box::pin(f(self));
                let (value, keep) = match body.await {
                    Scoped::Keep(value) => (value, true),
                    Scoped::Undo(value) => (value, false),
                };
                crate::values::end_savepoint(self, name, keep)?;
                scope.open = false;
                Ok(value)
            }
            Owner::Managed(managed) => {
                crate::values::open_tag_undo(self);
                // Closes the scope's tag-undo log as undone if the body
                // panics (the backend rolled it back).
                struct Undo<'a>(Option<&'a Storage>);
                impl Drop for Undo<'_> {
                    fn drop(&mut self) {
                        if let Some(storage) = self.0 {
                            crate::values::close_tag_undo(storage, false, false);
                            crate::values::note_rollback(storage.key());
                        }
                    }
                }
                let mut undo = Undo(Some(self));
                let mut value = None;
                let mut chose_keep = false;
                let output = &mut value;
                let decision = &mut chose_keep;
                let scoped = managed
                    .scope(Box::new(move |backend| {
                        Box::pin(async move {
                            let tx = Storage {
                                key: self.key,
                                owner: Owner::Managed(Managed {
                                    backend: SharedBackend(backend),
                                    interrupted: managed.interrupted.clone(),
                                    hooks: managed.hooks.clone(),
                                }),
                            };
                            let (out, keep) = match f(&tx).await {
                                Scoped::Keep(out) => (out, true),
                                Scoped::Undo(out) => (out, false),
                            };
                            *output = Some(out);
                            *decision = keep;
                            keep
                        })
                    }))
                    .await;
                undo.0 = None;
                let kept = matches!(scoped, Ok(true));
                crate::values::close_tag_undo(self, kept, false);
                if !kept {
                    crate::values::note_rollback(self.key());
                }
                scoped.map_err(crate::values::storage)?;
                // The commit hook vetoed it: as on native SQLite, where the
                // commit then fails.
                if chose_keep && !kept {
                    return Err(crate::values::storage(backend_error(
                        SQLITE_CONSTRAINT_COMMITHOOK,
                        format!("scope `{name}` was rolled back by the commit hook"),
                    )));
                }
                Ok(value.expect("a scope's body ran"))
            }
        }
    }
}

/// The SQL surface jazz-db runs its statements through, with rusqlite's
/// method names.
impl Storage {
    /// Open a private in-memory database.
    #[cfg(feature = "sqlite")]
    pub fn open_in_memory() -> SqlResult<Self> {
        rusqlite::Connection::open_in_memory()
            .map(Self::native)
            .map_err(Into::into)
    }

    /// Open (creating) the database file at `path`.
    #[cfg(feature = "sqlite")]
    pub fn open(path: impl AsRef<std::path::Path>) -> SqlResult<Self> {
        rusqlite::Connection::open(path)
            .map(Self::native)
            .map_err(Into::into)
    }

    /// Open `path` with `flags`.
    #[cfg(feature = "sqlite")]
    pub fn open_with_flags(
        path: impl AsRef<std::path::Path>,
        flags: rusqlite::OpenFlags,
    ) -> SqlResult<Self> {
        rusqlite::Connection::open_with_flags(path, flags)
            .map(Self::native)
            .map_err(Into::into)
    }

    /// Open `path` with `flags` on the SQLite VFS named `vfs`.
    #[cfg(feature = "sqlite")]
    pub fn open_with_flags_and_vfs(
        path: impl AsRef<std::path::Path>,
        flags: rusqlite::OpenFlags,
        vfs: &str,
    ) -> SqlResult<Self> {
        rusqlite::Connection::open_with_flags_and_vfs(path, flags, vfs)
            .map(Self::native)
            .map_err(Into::into)
    }

    /// Identifies this store among the open ones, for state kept beside it
    /// (a scope's tag-undo log, its rollback count).
    pub(crate) fn key(&self) -> usize {
        self.key
    }

    /// The native connection, for what only native SQLite has (its
    /// planner, limits and tracing); `None` on a managed store.
    #[cfg(feature = "sqlite")]
    pub(crate) fn native_conn(&self) -> Option<&rusqlite::Connection> {
        match &self.owner {
            #[cfg(feature = "sqlite")]
            Owner::Native(conn) => Some(conn),
            Owner::Managed(_) => None,
        }
    }

    /// Prepare `sql` through the statement cache.
    pub fn prepare_cached(&self, sql: &str) -> SqlResult<Statement<'_>> {
        match &self.owner {
            #[cfg(feature = "sqlite")]
            Owner::Native(conn) => Ok(Statement::Native(conn.prepare_cached(sql)?)),
            Owner::Managed(managed) => Ok(Statement::Managed {
                backend: managed.backend.current(),
                interrupted: &managed.interrupted,
                sql: sql.to_owned(),
                bindings: Vec::new(),
            }),
        }
    }

    /// Prepare `sql`, uncached on native SQLite.
    pub fn prepare(&self, sql: &str) -> SqlResult<Statement<'_>> {
        match &self.owner {
            #[cfg(feature = "sqlite")]
            Owner::Native(conn) => Ok(Statement::Uncached(conn.prepare(sql)?)),
            Owner::Managed(_) => self.prepare_cached(sql),
        }
    }

    /// Run one statement; how many rows it changed.
    /// Uncached on native SQLite, as rusqlite's own `execute`: one-off
    /// statements (DDL, PRAGMAs) stay out of the statement cache.
    pub async fn execute<P: Params>(&self, sql: &str, params: P) -> SqlResult<usize> {
        self.prepare(sql)?.execute(params).await
    }

    /// Run statements with no parameters and no results.
    pub async fn execute_batch(&self, sql: &str) -> SqlResult<()> {
        match &self.owner {
            #[cfg(feature = "sqlite")]
            Owner::Native(conn) => conn.execute_batch(sql).map_err(Into::into),
            Owner::Managed(managed) => {
                checked_command(
                    &managed.interrupted,
                    managed.backend.current().execute_script(sql),
                )
                .await
            }
        }
    }

    /// Run a query expected to return a row; `f` reads the first.
    pub async fn query_row<T, P: Params>(
        &self,
        sql: &str,
        params: P,
        f: impl FnOnce(&Row<'_>) -> SqlResult<T>,
    ) -> SqlResult<T> {
        // Uncached on native SQLite, as rusqlite's own `query_row`.
        self.prepare(sql)?.query_row(params, f).await
    }

    /// Whether no transaction or scope is open.
    pub fn is_autocommit(&self) -> bool {
        match &self.owner {
            #[cfg(feature = "sqlite")]
            Owner::Native(conn) => conn.is_autocommit(),
            Owner::Managed(managed) => managed.hooks.lock().expect("hooks").depth == 0,
        }
    }

    /// How long a statement waits for another connection's lock (native
    /// only: a managed store has no other connection).
    pub fn busy_timeout(&self, _timeout: std::time::Duration) -> SqlResult<()> {
        match &self.owner {
            #[cfg(feature = "sqlite")]
            Owner::Native(conn) => conn.busy_timeout(_timeout).map_err(Into::into),
            Owner::Managed(_) => Ok(()),
        }
    }

    /// The rowid of the last row inserted.
    pub async fn last_insert_rowid(&self) -> SqlResult<i64> {
        match &self.owner {
            #[cfg(feature = "sqlite")]
            Owner::Native(conn) => Ok(conn.last_insert_rowid()),
            Owner::Managed(managed) => {
                checked_command(
                    &managed.interrupted,
                    managed.backend.current().last_insert_rowid(),
                )
                .await
            }
        }
    }

    /// How many rows all statements so far changed.
    pub async fn total_changes(&self) -> SqlResult<u64> {
        match &self.owner {
            #[cfg(feature = "sqlite")]
            Owner::Native(conn) => Ok(conn.total_changes()),
            Owner::Managed(managed) => {
                checked_command(
                    &managed.interrupted,
                    managed.backend.current().total_changes(),
                )
                .await
            }
        }
    }

    /// The most `?` parameters a statement may bind.
    pub fn max_parameters(&self) -> SqlResult<usize> {
        match &self.owner {
            #[cfg(feature = "sqlite")]
            Owner::Native(conn) => conn
                .limit(rusqlite::limits::Limit::SQLITE_LIMIT_VARIABLE_NUMBER)
                .map(|n| n as usize)
                .map_err(Into::into),
            Owner::Managed(managed) => Ok(managed.backend.current().max_parameters()),
        }
    }

    /// How many prepared statements the cache keeps (native only).
    pub fn set_prepared_statement_cache_capacity(&self, _capacity: usize) {
        #[cfg(feature = "sqlite")]
        if let Owner::Native(conn) = &self.owner {
            conn.set_prepared_statement_cache_capacity(_capacity);
        }
    }

    /// Run `hook` as each outermost transaction (or scope) commits; `true`
    /// turns the commit into a rollback.
    pub fn commit_hook(
        &self,
        hook: Option<impl FnMut() -> bool + Send + 'static>,
    ) -> SqlResult<()> {
        match &self.owner {
            #[cfg(feature = "sqlite")]
            Owner::Native(conn) => conn.commit_hook(hook).map_err(Into::into),
            Owner::Managed(managed) => {
                managed.hooks.lock().expect("hooks").commit = hook.map(|h| Box::new(h) as _);
                Ok(())
            }
        }
    }

    /// Run `hook` as each outermost transaction (or scope) rolls back,
    /// including SQLite's own rollbacks after an error.
    pub fn rollback_hook(&self, hook: Option<impl FnMut() + Send + 'static>) -> SqlResult<()> {
        match &self.owner {
            #[cfg(feature = "sqlite")]
            Owner::Native(conn) => conn.rollback_hook(hook).map_err(Into::into),
            Owner::Managed(managed) => {
                managed.hooks.lock().expect("hooks").rollback = hook.map(|h| Box::new(h) as _);
                Ok(())
            }
        }
    }
}

impl crate::values::CachedRow for Storage {
    async fn cached_row<T, P, F>(&self, sql: &str, params: P, f: F) -> SqlResult<T>
    where
        P: Params,
        F: FnOnce(&Row<'_>) -> SqlResult<T>,
    {
        self.prepare_cached(sql)?.query_row(params, f).await
    }
}

/// Parameters a [`Statement`] binds, in order.
pub trait Params {
    /// Bind them to `statement`.
    fn bind(self, statement: &mut Statement<'_>) -> SqlResult<()>;
}

impl Params for [&dyn ToSql; 0] {
    fn bind(self, statement: &mut Statement<'_>) -> SqlResult<()> {
        bind_iter(statement, self)
    }
}

macro_rules! array_params {
    ($($n:literal),*) => {$(
        impl<T: ToSql> Params for [T; $n] {
            fn bind(self, statement: &mut Statement<'_>) -> SqlResult<()> {
                bind_iter(statement, self.iter())
            }
        }
    )*};
}
array_params!(
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26,
    27, 28, 29, 30, 31, 32
);

impl<T: ToSql> Params for &[T] {
    fn bind(self, statement: &mut Statement<'_>) -> SqlResult<()> {
        bind_iter(statement, self.iter())
    }
}

/// Parameters from an iterator ([`params_from_iter`]).
pub struct ParamsFromIter<I>(I);

/// Bind each item of `iter`, in order.
pub fn params_from_iter<I: IntoIterator>(iter: I) -> ParamsFromIter<I> {
    ParamsFromIter(iter)
}

impl<I: IntoIterator> Params for ParamsFromIter<I>
where
    I::Item: ToSql,
{
    fn bind(self, statement: &mut Statement<'_>) -> SqlResult<()> {
        bind_iter(statement, self.0)
    }
}

pub use crate::params;

fn bind_iter<I: IntoIterator>(statement: &mut Statement<'_>, values: I) -> SqlResult<()>
where
    I::Item: ToSql,
{
    match statement {
        #[cfg(feature = "sqlite")]
        Statement::Native(statement) => bind_native(statement, values),
        #[cfg(feature = "sqlite")]
        Statement::Uncached(statement) => bind_native(statement, values),
        Statement::Managed { bindings, .. } => {
            bindings.clear();
            for value in values {
                bindings.push(Value::from(value.as_cell()));
            }
            Ok(())
        }
    }
}

#[cfg(feature = "sqlite")]
fn bind_native<I: IntoIterator>(statement: &mut rusqlite::Statement<'_>, values: I) -> SqlResult<()>
where
    I::Item: ToSql,
{
    let mut n = 0;
    for value in values {
        n += 1;
        statement.raw_bind_parameter(n, value.as_cell())?;
    }
    if n != statement.parameter_count() {
        return Err(SqlError::InvalidParameterCount(
            n,
            statement.parameter_count(),
        ));
    }
    Ok(())
}

/// A prepared statement.
pub enum Statement<'a> {
    /// Native, from the statement cache.
    #[cfg(feature = "sqlite")]
    Native(rusqlite::CachedStatement<'a>),
    /// Native, uncached.
    #[cfg(feature = "sqlite")]
    Uncached(rusqlite::Statement<'a>),
    /// Managed: the SQL and its bound cells, run by the backend.
    Managed {
        backend: Arc<dyn Backend>,
        interrupted: &'a Arc<AtomicBool>,
        sql: String,
        bindings: Vec<Value>,
    },
}

/// A cached statement (the same type: managed SQL has no cache).
pub type CachedStatement<'a> = Statement<'a>;

impl Statement<'_> {
    /// Run it with `params`; how many rows it changed.
    pub async fn execute<P: Params>(&mut self, params: P) -> SqlResult<usize> {
        params.bind(self)?;
        match self {
            #[cfg(feature = "sqlite")]
            Self::Native(statement) => statement.raw_execute().map_err(Into::into),
            #[cfg(feature = "sqlite")]
            Self::Uncached(statement) => statement.raw_execute().map_err(Into::into),
            Self::Managed {
                backend,
                sql,
                bindings,
                interrupted,
            } => checked_command(interrupted, backend.execute(sql, bindings)).await,
        }
    }

    /// Run it with `params`; its rows.
    pub async fn query<P: Params>(&mut self, params: P) -> SqlResult<Rows<'_>> {
        params.bind(self)?;
        match self {
            #[cfg(feature = "sqlite")]
            Self::Native(statement) => Ok(Rows::Native(statement.raw_query())),
            #[cfg(feature = "sqlite")]
            Self::Uncached(statement) => Ok(Rows::Native(statement.raw_query())),
            Self::Managed {
                backend,
                sql,
                bindings,
                interrupted,
            } => {
                let cursor = checked_command(interrupted, backend.query(sql, bindings)).await?;
                let columns = cursor.columns().to_vec();
                Ok(Rows::Managed {
                    cursor,
                    columns,
                    interrupted: Arc::clone(interrupted),
                })
            }
        }
    }

    /// Run it with `params`; `f` reads the first row.
    pub async fn query_row<T, P: Params>(
        &mut self,
        params: P,
        f: impl FnOnce(&Row<'_>) -> SqlResult<T>,
    ) -> SqlResult<T> {
        let mut rows = self.query(params).await?;
        let row = rows.next().await?.ok_or(SqlError::QueryReturnedNoRows)?;
        f(&row)
    }

    /// Run it with `params`; each row mapped by `f`.
    pub async fn query_map<T, P: Params, F: FnMut(&Row<'_>) -> SqlResult<T>>(
        &mut self,
        params: P,
        f: F,
    ) -> SqlResult<MappedRows<'_, F>> {
        Ok(MappedRows {
            rows: self.query(params).await?,
            f,
        })
    }

    /// Whether it returns a row with `params`.
    pub async fn exists<P: Params>(&mut self, params: P) -> SqlResult<bool> {
        Ok(self.query(params).await?.next().await?.is_some())
    }

    /// A SQLite VM counter (native only; 0 on managed SQL, which has none:
    /// diagnostics only, never a correctness decision).
    pub fn get_status(&self, _status: StatementStatus) -> i32 {
        match self {
            #[cfg(feature = "sqlite")]
            Self::Native(statement) => statement.get_status(_status.into()),
            #[cfg(feature = "sqlite")]
            Self::Uncached(statement) => statement.get_status(_status.into()),
            Self::Managed { .. } => 0,
        }
    }

    /// A SQLite VM counter, reset (as [`Statement::get_status`]).
    pub fn reset_status(&self, _status: StatementStatus) -> i32 {
        match self {
            #[cfg(feature = "sqlite")]
            Self::Native(statement) => statement.reset_status(_status.into()),
            #[cfg(feature = "sqlite")]
            Self::Uncached(statement) => statement.reset_status(_status.into()),
            Self::Managed { .. } => 0,
        }
    }

    /// Release it now rather than when dropped.
    pub fn finalize(self) -> SqlResult<()> {
        match self {
            #[cfg(feature = "sqlite")]
            Self::Uncached(statement) => statement.finalize().map_err(Into::into),
            #[cfg(feature = "sqlite")]
            Self::Native(_) => Ok(()),
            Self::Managed { .. } => Ok(()),
        }
    }
}

/// A query's rows.
pub enum Rows<'a> {
    /// Native: rusqlite's rows.
    #[cfg(feature = "sqlite")]
    Native(rusqlite::Rows<'a>),
    /// Managed: the backend's cursor.
    Managed {
        cursor: Box<dyn Cursor + 'a>,
        interrupted: Arc<AtomicBool>,
        columns: Vec<String>,
    },
}

impl Rows<'_> {
    /// The next row, or `None` past the last. A lending cursor: the row
    /// borrows this call's cursor state.
    #[allow(clippy::should_implement_trait)]
    pub async fn next(&mut self) -> SqlResult<Option<Row<'_>>> {
        match self {
            #[cfg(feature = "sqlite")]
            Self::Native(rows) => Ok(rows.next()?.map(Row::Native)),
            Self::Managed {
                cursor,
                columns,
                interrupted,
            } => Ok(checked_command(interrupted, cursor.next())
                .await?
                .map(|cells| Row::Managed { cells, columns })),
        }
    }
}

/// Rows mapped by a function ([`Statement::query_map`]).
pub struct MappedRows<'a, F> {
    rows: Rows<'a>,
    f: F,
}

impl<T, F: FnMut(&Row<'_>) -> SqlResult<T>> MappedRows<'_, F> {
    pub async fn next(&mut self) -> Option<SqlResult<T>> {
        self.rows
            .next()
            .await
            .transpose()
            .map(|row| row.and_then(|row| (self.f)(&row)))
    }

    pub async fn collect<C: Default + Extend<T>>(mut self) -> SqlResult<C> {
        let mut out = C::default();
        while let Some(item) = self.next().await {
            out.extend(std::iter::once(item?));
        }
        Ok(out)
    }
}

/// One row of a query's result.
pub enum Row<'a> {
    /// Native: rusqlite's row.
    #[cfg(feature = "sqlite")]
    Native(&'a rusqlite::Row<'a>),
    /// Managed: the cursor's cells and the result's column names.
    Managed {
        cells: &'a [Value],
        columns: &'a [String],
    },
}

impl Row<'_> {
    /// Column `index` (a position or a name), converted to `T`.
    pub fn get<I: ColumnIndex, T: FromCell>(&self, index: I) -> SqlResult<T> {
        let index = index.index(self)?;
        let value = self.get_ref(index)?;
        T::from_cell(value).map_err(|reason| SqlError::InvalidColumnType {
            index,
            name: self.column_name(index).unwrap_or("").into(),
            reason,
        })
    }

    /// Column `index`, as SQLite holds it.
    pub fn get_ref(&self, index: usize) -> SqlResult<Cell<'_>> {
        match self {
            #[cfg(feature = "sqlite")]
            Self::Native(row) => row.get_ref(index).map(Cell::from).map_err(Into::into),
            Self::Managed { cells, .. } => cells
                .get(index)
                .map(Value::as_value_ref)
                .ok_or(SqlError::InvalidColumnIndex(index)),
        }
    }

    /// How many columns the row has.
    pub fn column_count(&self) -> usize {
        match self {
            #[cfg(feature = "sqlite")]
            Self::Native(row) => row.as_ref().column_count(),
            Self::Managed { columns, .. } => columns.len(),
        }
    }

    /// Column `index`'s name.
    pub fn column_name(&self, index: usize) -> SqlResult<&str> {
        match self {
            #[cfg(feature = "sqlite")]
            Self::Native(row) => row.as_ref().column_name(index).map_err(Into::into),
            Self::Managed { columns, .. } => columns
                .get(index)
                .map(String::as_str)
                .ok_or(SqlError::InvalidColumnIndex(index)),
        }
    }
}

/// A column, by position or name.
pub trait ColumnIndex {
    /// Its position in `row`.
    fn index(self, row: &Row<'_>) -> SqlResult<usize>;
}

impl ColumnIndex for usize {
    fn index(self, _: &Row<'_>) -> SqlResult<usize> {
        Ok(self)
    }
}

impl ColumnIndex for &str {
    fn index(self, row: &Row<'_>) -> SqlResult<usize> {
        (0..row.column_count())
            .find(|&i| {
                row.column_name(i)
                    .is_ok_and(|n| n.eq_ignore_ascii_case(self))
            })
            .ok_or_else(|| SqlError::InvalidColumnName(self.into()))
    }
}

/// A host's SQL error, as a SQLite error with `code`.
pub fn backend_error(code: i32, message: String) -> SqlError {
    SqlError::Backend {
        code: Some(code),
        message,
    }
}

/// What a scope ([`Storage::scope_then`]) does with what it wrote.
pub enum Scoped<T> {
    /// Keep it (committed with the outermost scope).
    Keep(T),
    /// Undo it.
    Undo(T),
}

#[cfg(feature = "sqlite")]
impl From<rusqlite::Connection> for Storage {
    fn from(conn: rusqlite::Connection) -> Self {
        Self::native(conn)
    }
}

/// What a Durable Object's SQL bridge refuses, checked the same way by
/// the bridge (jazz-wasm) and its emulation: transaction control in SQL
/// text (`BEGIN`, `SAVEPOINT`, …: scopes are `transactionSync`), named
/// parameters (`:name`, `@name`, `$name`), and reading the rows of a
/// data-modifying statement (`… RETURNING`). `reading`: the statement's
/// rows are read. Integers cross the bridge exactly over their whole
/// 64-bit range (as decimal text where a JavaScript number cannot hold
/// them), so they are not refused.
pub fn durable_object_refusal(sql: &str, reading: bool) -> SqlResult<()> {
    let first = sql
        .split(|c: char| c.is_whitespace() || c == ';')
        .find(|word| !word.is_empty())
        .unwrap_or("")
        .to_ascii_uppercase();
    let refuse = |what: String| {
        Err(backend_error(
            1,
            format!("{what} is not supported by Durable Object storage"),
        ))
    };
    if matches!(
        first.as_str(),
        "BEGIN" | "COMMIT" | "END" | "ROLLBACK" | "SAVEPOINT" | "RELEASE"
    ) {
        return refuse(format!("transaction control in SQL ({first})"));
    }
    if reading && matches!(first.as_str(), "INSERT" | "UPDATE" | "DELETE" | "REPLACE") {
        return refuse("reading a data-modifying statement's rows".to_owned());
    }
    // Parameter tokens outside strings, quoted identifiers and comments.
    let bytes = sql.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            quote @ (b'\'' | b'"' | b'`') => {
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == quote {
                        if bytes.get(i + 1) == Some(&quote) {
                            i += 1;
                        } else {
                            break;
                        }
                    }
                    i += 1;
                }
            }
            b'[' => {
                while i < bytes.len() && bytes[i] != b']' {
                    i += 1;
                }
            }
            b'-' if bytes.get(i + 1) == Some(&b'-') => {
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                i += 2;
                while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                    i += 1;
                }
                i += 1;
            }
            b':' | b'@' | b'$'
                if bytes
                    .get(i + 1)
                    .is_some_and(|c| c.is_ascii_alphanumeric() || *c == b'_') =>
            {
                return refuse("a named SQL parameter".to_owned());
            }
            _ => {}
        }
        i += 1;
    }
    Ok(())
}

/// Managed storage over a native connection, for tests: the engine sees
/// the same owner a Durable Object host gives it, so it takes the managed
/// paths (callback scopes only, nothing open between calls) while SQLite
/// itself stays native. It refuses what the Durable Object bridge refuses
/// ([`durable_object_refusal`]).
/// Unlike the bridge, whose cursor is lazy, it reads each result out before
/// returning it; the engine may run more SQL while it walks rows either way.
#[cfg(any(test, feature = "testing"))]
pub mod managed_emulation {
    use super::{Backend, Cursor, SqlResult, Value};
    use std::sync::{Arc, Mutex};

    /// A Durable Object's SQL, emulated. Clones share one database, as a
    /// Durable Object's successive instances share its storage: open a
    /// new [`super::Storage`] over a clone to "restart" it.
    #[derive(Clone)]
    pub struct EmulatedObjectSql {
        conn: Arc<Mutex<rusqlite::Connection>>,
        depth: Arc<Mutex<usize>>,
    }

    struct Materialized {
        columns: Vec<String>,
        rows: std::vec::IntoIter<Vec<Value>>,
        current: Vec<Value>,
    }

    #[async_trait::async_trait(?Send)]
    impl Cursor for Materialized {
        async fn next(&mut self) -> SqlResult<Option<&[Value]>> {
            Ok(match self.rows.next() {
                Some(row) => {
                    self.current = row;
                    Some(&self.current)
                }
                None => None,
            })
        }

        fn columns(&self) -> &[String] {
            &self.columns
        }
    }

    impl EmulatedObjectSql {
        /// A fresh in-memory database.
        #[cfg(feature = "sqlite")]
        pub fn open_in_memory() -> SqlResult<Self> {
            let conn = rusqlite::Connection::open_in_memory()?;
            #[cfg(feature = "durable-object-limits")]
            crate::durable_object_limits::apply(&conn);
            Ok(Self {
                conn: Arc::new(Mutex::new(conn)),
                depth: Arc::default(),
            })
        }
    }

    #[async_trait::async_trait(?Send)]
    impl Backend for EmulatedObjectSql {
        async fn query(&self, sql: &str, parameters: &[Value]) -> SqlResult<Box<dyn Cursor + '_>> {
            super::durable_object_refusal(sql, true)?;
            let conn = self.conn.lock().expect("connection");
            let mut statement = conn.prepare_cached(sql)?;
            let columns: Vec<String> = statement
                .column_names()
                .into_iter()
                .map(str::to_owned)
                .collect();
            let n = columns.len();
            let mut rows = statement.query(rusqlite::params_from_iter(parameters.iter()))?;
            let mut out = Vec::new();
            while let Some(row) = rows.next()? {
                let mut cells = Vec::with_capacity(n);
                for i in 0..n {
                    let cell = Value::from(super::Cell::from(row.get_ref(i)?));
                    cells.push(cell);
                }
                out.push(cells);
            }
            Ok(Box::new(Materialized {
                columns,
                rows: out.into_iter(),
                current: Vec::new(),
            }))
        }

        async fn execute(&self, sql: &str, parameters: &[Value]) -> SqlResult<usize> {
            super::durable_object_refusal(sql, false)?;
            let conn = self.conn.lock().expect("connection");
            let mut statement = conn.prepare_cached(sql)?;
            statement
                .execute(rusqlite::params_from_iter(parameters.iter()))
                .map_err(Into::into)
        }

        async fn execute_script(&self, script: &str) -> SqlResult<()> {
            let mut start = 0;
            for (end, _) in script.match_indices(';') {
                let part = &script[start..=end];
                let complete =
                    std::ffi::CString::new(part).map_err(|_| rusqlite::Error::InvalidQuery)?;
                // SAFETY: SQLite only reads this NUL-terminated SQL string.
                if unsafe { rusqlite::ffi::sqlite3_complete(complete.as_ptr()) } != 0 {
                    super::durable_object_refusal(part, false)?;
                    self.conn.lock().expect("connection").execute_batch(part)?;
                    start = end + 1;
                }
            }
            if !script[start..].trim().is_empty() {
                super::durable_object_refusal(&script[start..], false)?;
                self.conn
                    .lock()
                    .expect("connection")
                    .execute_batch(&script[start..])?;
            }
            Ok(())
        }

        async fn transaction(&self, body: super::TransactionBody<'_>) -> SqlResult<()> {
            super::transaction_sync(Arc::new(self.clone()), body, |body| {
                let name = {
                    let mut depth = self.depth.lock().expect("depth");
                    *depth += 1;
                    format!("do_scope_{depth}")
                };
                let begin = self
                    .conn
                    .lock()
                    .expect("connection")
                    .execute_batch(&format!("SAVEPOINT {name}"));
                if let Err(error) = begin {
                    *self.depth.lock().expect("depth") -= 1;
                    return Err(error.into());
                }
                // As with `transactionSync`, a panic in the body rolls back.
                let keep = std::panic::catch_unwind(std::panic::AssertUnwindSafe(&mut *body));
                let conn = self.conn.lock().expect("connection");
                let end = match keep {
                    Ok(true) => conn.execute_batch(&format!("RELEASE {name}")),
                    _ => conn.execute_batch(&format!("ROLLBACK TO {name}; RELEASE {name}")),
                };
                drop(conn);
                *self.depth.lock().expect("depth") -= 1;
                match keep {
                    Err(panic) => std::panic::resume_unwind(panic),
                    Ok(_) => end.map_err(Into::into),
                }
            })
        }

        async fn last_insert_rowid(&self) -> SqlResult<i64> {
            Ok(self.conn.lock().expect("connection").last_insert_rowid())
        }

        async fn total_changes(&self) -> SqlResult<u64> {
            Ok(self.conn.lock().expect("connection").total_changes())
        }

        fn max_parameters(&self) -> usize {
            100
        }
    }
    #[cfg(test)]
    mod suspending_tests {
        use super::*;
        use crate::db::{Db, DbConfig, DbIdentity};
        use crate::ids::{AuthorSubject, NodeUuid};
        use crate::schema::JazzSchema;
        use crate::storage::{Storage, TransactionBody};
        use futures::{executor::block_on, poll};
        use jazz_model::model::public_schema::{SchemaBuilder, TableSchemaBuilder};
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::task::Poll;

        // Each transaction supplies a distinct connection handle. A statement on
        // the root while a nested transaction is active would escape its rollback.
        #[derive(Clone)]
        struct AsyncSql {
            sql: EmulatedObjectSql,
            active: Arc<AtomicUsize>,
            depth: usize,
        }

        impl AsyncSql {
            async fn step(&self) {
                let mut suspended = false;
                std::future::poll_fn(|cx| {
                    if suspended {
                        assert_eq!(self.active.load(Ordering::Relaxed), self.depth);
                        Poll::Ready(())
                    } else {
                        suspended = true;
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }
                })
                .await
            }
        }

        #[async_trait::async_trait(?Send)]
        impl Backend for AsyncSql {
            async fn query(&self, sql: &str, args: &[Value]) -> SqlResult<Box<dyn Cursor + '_>> {
                self.step().await;
                self.sql.query(sql, args).await
            }
            async fn execute(&self, sql: &str, args: &[Value]) -> SqlResult<usize> {
                self.step().await;
                self.sql.execute(sql, args).await
            }
            async fn execute_script(&self, sql: &str) -> SqlResult<()> {
                self.step().await;
                self.sql.execute_script(sql).await
            }
            async fn transaction(&self, body: TransactionBody<'_>) -> SqlResult<()> {
                self.step().await;
                let depth = self.depth + 1;
                let name = format!("scope_{depth}");
                self.sql
                    .conn
                    .lock()
                    .unwrap()
                    .execute_batch(&format!("SAVEPOINT {name}"))?;
                self.active.store(depth, Ordering::Relaxed);
                struct Rollback<'a>(&'a AsyncSql, String);
                impl Drop for Rollback<'_> {
                    fn drop(&mut self) {
                        self.0.active.store(self.0.depth, Ordering::Relaxed);
                        self.0
                            .sql
                            .conn
                            .lock()
                            .unwrap()
                            .execute_batch(&self.1)
                            .unwrap();
                    }
                }
                let mut end = Rollback(self, format!("ROLLBACK TO {name}; RELEASE {name}"));
                if body(Arc::new(Self {
                    depth,
                    ..self.clone()
                }))
                .await
                {
                    end.1 = format!("RELEASE {name}");
                }
                Ok(())
            }
            async fn last_insert_rowid(&self) -> SqlResult<i64> {
                self.step().await;
                self.sql.last_insert_rowid().await
            }
            async fn total_changes(&self) -> SqlResult<u64> {
                self.step().await;
                self.sql.total_changes().await
            }
            fn max_parameters(&self) -> usize {
                self.sql.max_parameters()
            }
        }

        async fn open() -> Db {
            let storage = Storage::from_backend(AsyncSql {
                sql: EmulatedObjectSql::open_in_memory().unwrap(),
                active: Arc::default(),
                depth: 0,
            });
            let schema = SchemaBuilder::new()
                .table(TableSchemaBuilder::new("notes"))
                .build();
            Db::open_storage(
                DbConfig::new(
                    JazzSchema::new(&schema).unwrap(),
                    DbIdentity {
                        author: AuthorSubject::SYSTEM,
                        node: NodeUuid::from_bytes([1; 16]),
                    },
                ),
                storage,
            )
            .await
            .unwrap()
        }

        #[test]
        fn origin_callback_serializes_overlapping_writes_on_its_handle() {
            block_on(async {
                let db = open().await;
                let key = crate::tx::TxId::new(
                    jazz_types::time::TxTime(1),
                    NodeUuid::from_bytes([2; 16]),
                );
                let author = AuthorSubject::SYSTEM;
                db.write_as_origin(key, &author, async |scoped| {
                    let first = scoped.insert("notes", Default::default(), Default::default());
                    let second = scoped.insert("notes", Default::default(), Default::default());
                    let (first, second) = futures::join!(first, second);
                    second?;
                    first
                })
                .await
                .unwrap();
                let query = db.prepare_query(&db.table("notes")).unwrap();
                assert_eq!(db.read(&query).await.unwrap().len(), 2);
            });
        }

        #[test]
        fn an_upstream_detaches_during_sql_and_its_advice_cannot_cross_a_reconnect() {
            use crate::db::sync::duplex;
            use jazz_protocol::protocol::{PermissionAdvice, PermissionAdviceAction};
            block_on(async {
                let db = open().await;
                let (peer, _remote) = duplex();
                let old = db.connect_upstream(peer).await;
                let mut insert =
                    Box::pin(db.insert("notes", Default::default(), Default::default()));
                assert!(poll!(&mut insert).is_pending());
                assert_eq!(db.pending_upload_count(), 0);
                assert_eq!(db.upload_status().pending, 0);
                assert_eq!(db.own_upload_progress(), (0, 0));
                assert_eq!(db.settled_upstream(), 0);
                let _queued = db.owes_requests_sent();
                assert!(!db.owes_chunk_requests());
                assert!(!db.awaiting_chunks());
                assert!(db.unsupported_schema_versions().is_empty());
                assert_eq!(db.next_chunk_pull_tick_ms(), None);
                assert!(!db.shard().is_shard());
                assert_eq!(db.shard().lowest_hold(), None);
                assert_eq!(db.shard().waits_out(), None);
                let request = db.request_permission_advice(
                    PermissionAdviceAction::Insert {
                        table: "notes".into(),
                        cells: Default::default(),
                    },
                    None,
                );
                assert!(db.detach_connection(&old));
                assert!(!db.detach_connection(&old));
                assert!(db.upstream_refusal(&old).is_none());
                insert.await.unwrap();
                let (next, _remote) = duplex();
                let current = db.connect_upstream(next).await;
                assert!(!db.detach_connection(&old));
                assert_eq!(
                    db.permission_advice(request).await,
                    PermissionAdvice::Unknown
                );
                assert!(db.detach_connection(&current));
            });
        }

        #[test]
        fn disconnect_stops_delivery_while_the_subscribers_join_is_in_sql() {
            use crate::db::sync::{SyncMessage, Transport, duplex};
            use crate::wire::TransportError;
            use std::{cell::Cell, rc::Rc};
            struct Incoming {
                transport: Box<dyn Transport>,
                received: Rc<Cell<bool>>,
            }
            impl Transport for Incoming {
                fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
                    self.transport.send(message)
                }
                fn try_recv(&mut self) -> Option<SyncMessage> {
                    let message = self.transport.try_recv()?;
                    self.received.set(true);
                    Some(message)
                }
            }
            block_on(async {
                let db = open().await;
                db.set_authority().await.unwrap();
                let (peer, mut client) = duplex();
                let received = Rc::new(Cell::new(false));
                let old = db
                    .accept_subscriber(
                        Box::new(Incoming {
                            transport: peer,
                            received: received.clone(),
                        }),
                        AuthorSubject::SYSTEM,
                    )
                    .await
                    .unwrap();
                let join = SyncMessage::Join {
                    version: *db.current_schema().version_id().as_bytes(),
                    known: vec![],
                };
                client.send(join.clone()).unwrap();
                let mut tick = Box::pin(db.tick());
                while !received.get() {
                    assert!(poll!(&mut tick).is_pending());
                }
                assert!(client.try_recv().is_none());
                db.disconnect_subscriber(old);
                assert_eq!(db.open_subscribers(), 0);
                tick.await.unwrap();
                assert!(client.try_recv().is_none());

                let (peer, mut next) = duplex();
                let new = db
                    .accept_subscriber(peer, AuthorSubject::SYSTEM)
                    .await
                    .unwrap();
                assert_eq!(old.slot(), new.slot());
                db.disconnect_subscriber(old);
                next.send(join).unwrap();
                db.tick().await.unwrap();
                assert!(next.try_recv().is_some());
            });
        }

        /// Alice's insert yields inside the host transaction. Bob's insert and a
        /// tick queue behind it. An independent synchronous getter remains usable.
        #[test]
        fn overlapping_writes_and_tick_use_their_own_transaction() {
            block_on(async {
                let db = open().await;
                let schema_version = db.current_schema().version_id();
                let query = db.prepare_query(&db.table("notes")).unwrap();
                let abandoned = crate::tools::OpenTransactionId::new();
                db.begin_mergeable(abandoned).await.unwrap();
                let mut alice =
                    Box::pin(db.insert("notes", Default::default(), Default::default()));
                let mut bob = Box::pin(db.insert("notes", Default::default(), Default::default()));
                let mut tick = Box::pin(db.tick());
                assert!(poll!(&mut alice).is_pending());
                assert!(poll!(&mut bob).is_pending());
                assert!(poll!(&mut tick).is_pending());
                db.abandon_transaction_handle(abandoned).unwrap();
                assert!(!db.is_authority());
                assert_eq!(db.echo_probes(), 0);
                assert_eq!(db.replayed_commits(), 0);
                let floor = db.seq_high_water() + 1000;
                db.seed_seq_floor(floor);
                assert_eq!(db.seq_high_water(), floor);
                assert!(db.current_schema().version_id() == schema_version);
                assert!(db.governed_view(schema_version).is_some());
                assert_eq!(db.fanout_stats().woken, 0);
                db.prepare_query(&db.table("notes")).unwrap();
                let (alice, bob, tick) = futures::join!(alice, bob, tick);
                let alice = alice.unwrap();
                let bob = bob.unwrap();
                assert_ne!(alice.row_uuid(), bob.row_uuid());
                assert!(bob.mergeable_tx_id() > floor);
                tick.unwrap();
                assert_eq!(db.read(&query).await.unwrap().len(), 2);
                let mut interrupted =
                    Box::pin(db.insert("notes", Default::default(), Default::default()));
                assert!(poll!(&mut interrupted).is_pending());
                drop(interrupted);
                let error = db.read(&query).await.unwrap_err();
                assert!(error.to_string().contains("reopen the database"), "{error}");
            });
        }
    }
}

#[cfg(any(test, feature = "testing"))]
impl Storage {
    /// An in-memory store on [`managed_emulation::EmulatedObjectSql`].
    pub fn managed_emulation_in_memory() -> SqlResult<Self> {
        managed_emulation::EmulatedObjectSql::open_in_memory().map(Self::from_backend)
    }
}

// Lower-level tests: these are the contract every storage owner must meet
// (native SQLite, and a host's SQL such as a Durable Object's), checked on
// both. The whole engine on managed storage is exercised through the
// public Db API by the `managed-storage-emulation` suite.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::options::ErrorCode;

    /// A native store and an emulated Durable Object store, each with an
    /// empty table `t`.
    async fn both() -> [(&'static str, Storage); 2] {
        let native = Storage::open_in_memory().unwrap();
        let managed = Storage::managed_emulation_in_memory().unwrap();
        for storage in [&native, &managed] {
            storage
                .execute_batch("CREATE TABLE t (v INTEGER)")
                .await
                .unwrap();
        }
        [("native", native), ("managed", managed)]
    }

    async fn values(storage: &Storage) -> Vec<i64> {
        let mut statement = storage.prepare("SELECT v FROM t ORDER BY v").unwrap();
        statement
            .query_map([], |row| row.get::<_, i64>(0))
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await
            .unwrap()
    }

    async fn insert(storage: &Storage, v: i64) -> Result<(), Error> {
        storage
            .execute("INSERT INTO t (v) VALUES (?1)", [v])
            .await
            .map(drop)
            .map_err(crate::values::storage)
    }

    fn fail() -> Error {
        Error::new(ErrorCode::Storage, "refused")
    }

    /// ```text
    /// scope ── insert 1 ── Ok   → kept
    /// scope ── insert 2 ── Err  → undone
    /// ```
    #[test]
    fn a_scope_keeps_its_writes_on_ok_and_undoes_them_on_err() {
        crate::options::block_on(async {
            for (owner, storage) in both().await {
                storage
                    .scope("a", async |s| insert(s, 1).await)
                    .await
                    .unwrap();
                let failed = storage
                    .scope("b", async |s| {
                        insert(s, 2).await?;
                        Err::<(), _>(fail())
                    })
                    .await;
                assert!(failed.is_err(), "{owner}");
                assert_eq!(values(&storage).await, [1], "{owner}");
                assert!(storage.is_autocommit(), "{owner}: nothing stays open");
            }
        })
    }

    /// An inner scope's failure undoes only its own writes; the outer
    /// scope's commit keeps the rest. An outer failure undoes everything,
    /// including inner scopes that succeeded.
    ///
    /// ```text
    /// outer ── 1 ── inner(2, Err) ── 3 ── Ok   → [1, 3]
    /// outer ── 4 ── inner(5, Ok)  ── Err       → nothing more
    /// ```
    #[test]
    fn scopes_nest() {
        crate::options::block_on(async {
            for (owner, storage) in both().await {
                storage
                    .scope("outer", async |s| {
                        insert(s, 1).await?;
                        let inner = s
                            .scope("inner", async |s| {
                                insert(s, 2).await?;
                                Err::<(), _>(fail())
                            })
                            .await;
                        assert!(inner.is_err());
                        assert!(!s.is_autocommit(), "the outer scope is still open");
                        insert(s, 3).await
                    })
                    .await
                    .unwrap();
                let failed = storage
                    .scope("outer", async |s| {
                        insert(s, 4).await?;
                        s.scope("inner", async |s| insert(s, 5).await).await?;
                        Err::<(), _>(fail())
                    })
                    .await;
                assert!(failed.is_err(), "{owner}");
                assert_eq!(values(&storage).await, [1, 3], "{owner}");
                assert!(storage.is_autocommit(), "{owner}");
            }
        })
    }

    /// A dry run returns what it found and undoes what it wrote; a unit
    /// that decides to keep returns its value too.
    ///
    /// ```text
    /// scope_then ── insert 1 ── count → 1 ── Undo  → [] and 1 returned
    /// scope_then ── insert 2 ──────────────── Keep  → [2]
    /// ```
    #[test]
    fn scope_then_keeps_or_undoes_as_its_body_decides_and_returns_its_value() {
        crate::options::block_on(async {
            for (owner, storage) in both().await {
                let seen = storage
                    .scope_then("dry", async |s| {
                        insert(s, 1).await.unwrap();
                        Scoped::Undo(values(s).await.len())
                    })
                    .await
                    .unwrap();
                assert_eq!(seen, 1, "{owner}");
                assert_eq!(values(&storage).await, Vec::<i64>::new(), "{owner}");
                let kept = storage
                    .scope_then("unit", async |s| Scoped::Keep(insert(s, 2).await))
                    .await
                    .unwrap();
                assert!(kept.is_ok(), "{owner}");
                assert_eq!(values(&storage).await, [2], "{owner}");
                assert!(storage.is_autocommit(), "{owner}");
            }
        })
    }

    /// The commit hook runs as the outermost scope commits, and can veto
    /// it, which undoes the scope and runs the rollback hook. Inner scopes
    /// run neither. An outermost scope its body undoes runs exactly one
    /// hook: the rollback hook on managed storage, but the commit hook on
    /// native SQLite, where `ROLLBACK TO; RELEASE` commits the then empty
    /// transaction.
    ///
    /// ```text
    /// outer(inner(1)) ── Ok  → commit hook once, kept
    /// outer(2)        ── Err → one hook (managed: rollback; native: commit)
    /// outer(3) with the commit hook vetoing → undone, rollback hook
    /// ```
    #[test]
    fn hooks_run_as_the_outermost_scope_ends() {
        crate::options::block_on(async {
            use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
            for (owner, storage) in both().await {
                let commits = Arc::new(AtomicUsize::new(0));
                let rollbacks = Arc::new(AtomicUsize::new(0));
                let veto = Arc::new(AtomicBool::new(false));
                let (c, r, v) = (commits.clone(), rollbacks.clone(), veto.clone());
                storage
                    .commit_hook(Some(move || {
                        c.fetch_add(1, Ordering::SeqCst);
                        v.load(Ordering::SeqCst)
                    }))
                    .unwrap();
                storage
                    .rollback_hook(Some(move || {
                        r.fetch_add(1, Ordering::SeqCst);
                    }))
                    .unwrap();
                storage
                    .scope("outer", async |s| {
                        s.scope("inner", async |s| insert(s, 1).await).await
                    })
                    .await
                    .unwrap();
                assert_eq!(commits.load(Ordering::SeqCst), 1, "{owner}");
                let _ = storage
                    .scope("outer", async |s| {
                        insert(s, 2).await?;
                        Err::<(), _>(fail())
                    })
                    .await;
                let (undone_commits, undone_rollbacks) = match owner {
                    "managed" => (1, 1),
                    _ => (2, 0),
                };
                assert_eq!(commits.load(Ordering::SeqCst), undone_commits, "{owner}");
                assert_eq!(
                    rollbacks.load(Ordering::SeqCst),
                    undone_rollbacks,
                    "{owner}"
                );
                veto.store(true, Ordering::SeqCst);
                let vetoed = storage.scope("outer", async |s| insert(s, 3).await).await;
                assert!(vetoed.is_err(), "{owner}: a vetoed commit fails");
                assert_eq!(
                    rollbacks.load(Ordering::SeqCst),
                    undone_rollbacks + 1,
                    "{owner}"
                );
                assert_eq!(values(&storage).await, [1], "{owner}");
            }
        })
    }

    /// Statements bind every parameter shape jazz-db uses and read rows
    /// by position and by name, through the same calls on both owners.
    #[test]
    fn statements_bind_and_rows_read_alike_on_both_owners() {
        crate::options::block_on(async {
            for (owner, storage) in both().await {
                storage
                    .execute_batch("CREATE TABLE p (n INTEGER, s TEXT, b BLOB, r REAL)")
                    .await
                    .unwrap();
                storage
                    .execute(
                        "INSERT INTO p VALUES (?1, ?2, ?3, ?4)",
                        params![7i64, "seven", vec![7u8], 7.5f64],
                    )
                    .await
                    .unwrap();
                let values: Vec<Value> = vec![Value::Integer(8), Value::Text(b"eight".to_vec())];
                storage
                    .execute(
                        "INSERT INTO p (n, s) VALUES (?1, ?2)",
                        params_from_iter(values.iter()),
                    )
                    .await
                    .unwrap();
                let rows: Vec<(i64, String, Option<Vec<u8>>, Option<f64>)> = storage
                    .prepare_cached("SELECT n, s, b, r FROM p WHERE n >= ?1 ORDER BY n")
                    .unwrap()
                    .query_map([7i64], |row| {
                        Ok((row.get(0)?, row.get("s")?, row.get(2)?, row.get(3)?))
                    })
                    .await
                    .unwrap()
                    .collect::<Vec<_>>()
                    .await
                    .unwrap();
                assert_eq!(
                    rows,
                    [
                        (7, "seven".to_owned(), Some(vec![7]), Some(7.5)),
                        (8, "eight".to_owned(), None, None)
                    ],
                    "{owner}"
                );
                let missing = storage
                    .query_row("SELECT n FROM p WHERE n = 9", [], |row| {
                        row.get::<_, i64>(0)
                    })
                    .await
                    .optional()
                    .unwrap();
                assert_eq!(missing, None, "{owner}");
            }
        })
    }

    /// Managed storage refuses what a Durable Object's SQL bridge refuses
    /// ([`durable_object_refusal`]): transaction control in SQL text (its
    /// only transactions are scopes), named parameters, and reading a
    /// write's rows. Integers keep their whole 64-bit range.
    #[test]
    fn managed_storage_refuses_what_a_durable_object_refuses() {
        crate::options::block_on(async {
            let storage = Storage::managed_emulation_in_memory().unwrap();
            storage
                .execute_batch("CREATE TABLE t (v INTEGER)")
                .await
                .unwrap();
            for sql in ["BEGIN", "SAVEPOINT s", "  release s", "COMMIT", "ROLLBACK"] {
                assert!(storage.execute_batch(sql).await.is_err(), "{sql}");
            }
            assert!(
                storage
                    .execute("INSERT INTO t VALUES (:v)", [1i64])
                    .await
                    .is_err()
            );
            assert!(
                storage
                    .query_row("INSERT INTO t VALUES (1) RETURNING v", [], |r| r
                        .get::<_, i64>(0))
                    .await
                    .is_err()
            );
            // Names inside strings and comments are not parameters.
            storage
                .execute("INSERT INTO t VALUES (length(':v @w $x')) -- :y", [])
                .await
                .unwrap();
            for n in [i64::MIN, -(1 << 53) - 1, (1 << 53) + 1, i64::MAX] {
                storage
                    .execute("INSERT INTO t VALUES (?1)", [n])
                    .await
                    .unwrap();
                let read: i64 = storage
                    .query_row("SELECT v FROM t WHERE v = ?1", [n], |r| r.get(0))
                    .await
                    .unwrap();
                assert_eq!(read, n);
            }
            assert!(storage.is_managed());
            assert!(!Storage::open_in_memory().unwrap().is_managed());
        })
    }

    /// A scope whose body panics, caught by its host, still leaves the
    /// scope on managed storage: what it wrote is undone, nothing stays
    /// open, the rollback hook ran, and later scopes commit as before.
    #[test]
    fn a_panicking_scope_leaves_managed_storage_usable() {
        crate::options::block_on(async {
            use std::sync::atomic::{AtomicUsize, Ordering};
            let storage = Storage::managed_emulation_in_memory().unwrap();
            storage
                .execute_batch("CREATE TABLE t (v INTEGER)")
                .await
                .unwrap();
            let rollbacks = Arc::new(AtomicUsize::new(0));
            let counted = rollbacks.clone();
            storage
                .rollback_hook(Some(move || {
                    counted.fetch_add(1, Ordering::SeqCst);
                }))
                .unwrap();
            let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                crate::options::block_on(async {
                    let _ = storage
                        .scope("outer", async |s| {
                            insert(s, 1).await?;
                            s.scope("inner", async |_| -> Result<(), Error> {
                                panic!("injected")
                            })
                            .await
                        })
                        .await;
                })
            }));
            assert!(panicked.is_err());
            assert!(storage.is_autocommit());
            assert_eq!(rollbacks.load(Ordering::SeqCst), 1);
            assert_eq!(values(&storage).await, Vec::<i64>::new());
            storage
                .scope("after", async |s| insert(s, 2).await)
                .await
                .unwrap();
            assert_eq!(values(&storage).await, [2]);
        })
    }
}
