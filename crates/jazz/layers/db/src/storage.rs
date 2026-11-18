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

use std::sync::{Arc, Mutex};

use crate::options::Error;

pub use crate::values::Sql as Value;
pub use rusqlite::types;
pub use rusqlite::{
    Error as SqlError, OptionalExtension, Result as SqlResult, StatementStatus, ToSql,
};

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

/// A host's SQL, for a managed [`Storage`]: statements run one at a time
/// on one connection. Its only transaction is [`Backend::transaction`],
/// which runs `body` once, synchronously, and keeps what it wrote if it
/// returns `true` (rolling it back if it returns `false` or panics). SQL
/// `body` runs through this backend belongs to that transaction; calls
/// nest as `transactionSync` nests. Transaction control in SQL text
/// (`BEGIN`, `SAVEPOINT`, …) is refused.
pub trait Backend: BackendThreading {
    /// Run a query; its rows, through a forward-only cursor.
    fn query(&self, sql: &str, parameters: &[Value]) -> SqlResult<Box<dyn Cursor + '_>>;
    /// Run a statement; how many rows it changed.
    fn execute(&self, sql: &str, parameters: &[Value]) -> SqlResult<usize>;
    /// Run statements with no parameters and no results.
    fn execute_script(&self, sql: &str) -> SqlResult<()>;
    /// Run `body` atomically (see the trait's documentation).
    fn transaction(&self, body: &mut dyn FnMut() -> bool) -> SqlResult<()>;
    /// The rowid of the last row inserted.
    fn last_insert_rowid(&self) -> SqlResult<i64>;
    /// How many rows all statements so far changed.
    fn total_changes(&self) -> SqlResult<u64>;
    /// The most `?` parameters a statement may bind.
    fn max_parameters(&self) -> usize;
}

/// A query's rows, one at a time. A host must not materialize an
/// unbounded result set to implement it.
pub trait Cursor {
    /// The next row's cells, or `None` past the last.
    fn next(&mut self) -> SqlResult<Option<&[Value]>>;
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

#[derive(Clone)]
struct Managed {
    backend: SharedBackend,
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

impl Managed {
    /// Run `body` in one [`Backend::transaction`]: kept if it returns
    /// `true` and, at the outermost scope, the commit hook does not veto
    /// it. Whether it was kept.
    fn scope(&self, body: &mut dyn FnMut() -> bool) -> SqlResult<bool> {
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
        let result = self.backend.transaction(&mut || {
            keep = body();
            if keep && outer {
                let mut hooks = self.hooks.lock().expect("hooks");
                let veto = hooks.commit.as_mut().is_some_and(|hook| hook());
                keep = !veto;
            }
            keep
        });
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
    pub fn native(conn: rusqlite::Connection) -> Self {
        Self::new(Owner::Native(conn))
    }

    /// Run on a host's SQL ([`Backend`]).
    pub fn from_backend(backend: impl Backend + 'static) -> Self {
        Self::new(Owner::Managed(Managed {
            backend: SharedBackend(Arc::new(backend)),
            hooks: Arc::default(),
        }))
    }

    /// Whether this store runs on a host's SQL ([`Storage::from_backend`]),
    /// which keeps nothing open between calls: no savepoint can outlive the
    /// call that opened it.
    pub fn is_managed(&self) -> bool {
        matches!(self.owner, Owner::Managed(_))
    }

    /// Run `f` atomically: what it wrote is kept if it returns `Ok`, and
    /// undone if it returns `Err`. Scopes nest; an inner scope's writes
    /// commit with the outermost one. `name` labels the scope (on native
    /// SQLite, its savepoint).
    ///
    /// `f` is synchronous, so a scope can never stay open across an await
    /// or past the call that opened it.
    pub fn scope<T>(
        &self,
        name: &str,
        f: impl FnOnce(&Storage) -> Result<T, Error>,
    ) -> Result<T, Error> {
        self.scope_then(name, |storage| match f(storage) {
            Ok(value) => Scoped::Keep(Ok(value)),
            Err(error) => Scoped::Undo(Err(error)),
        })?
    }

    /// Run `f` atomically, keeping or undoing what it wrote as it decides
    /// ([`Scoped`]), and return its value either way: for a dry run (undo
    /// always) or a unit whose failure carries more than an [`Error`].
    /// `Err` only when the scope itself cannot open or close.
    pub fn scope_then<T>(
        &self,
        name: &str,
        f: impl FnOnce(&Storage) -> Scoped<T>,
    ) -> Result<T, Error> {
        match &self.owner {
            Owner::Native(_) => {
                crate::values::begin_savepoint(self, name)?;
                let (value, keep) = match f(self) {
                    Scoped::Keep(value) => (value, true),
                    Scoped::Undo(value) => (value, false),
                };
                crate::values::end_savepoint(self, name, keep)?;
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
                let mut f = Some(f);
                let mut value = None;
                let mut chose_keep = false;
                let scoped = managed.scope(&mut || {
                    let f = f.take().expect("a scope's body runs once");
                    let (out, keep) = match f(self) {
                        Scoped::Keep(out) => (out, true),
                        Scoped::Undo(out) => (out, false),
                    };
                    value = Some(out);
                    chose_keep = keep;
                    keep
                });
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
                    return Err(crate::values::storage(SqlError::SqliteFailure(
                        rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_CONSTRAINT_COMMITHOOK),
                        Some(format!("scope `{name}` was rolled back by the commit hook")),
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
    pub fn open_in_memory() -> SqlResult<Self> {
        rusqlite::Connection::open_in_memory().map(Self::native)
    }

    /// Open (creating) the database file at `path`.
    pub fn open(path: impl AsRef<std::path::Path>) -> SqlResult<Self> {
        rusqlite::Connection::open(path).map(Self::native)
    }

    /// Open `path` with `flags`.
    pub fn open_with_flags(
        path: impl AsRef<std::path::Path>,
        flags: rusqlite::OpenFlags,
    ) -> SqlResult<Self> {
        rusqlite::Connection::open_with_flags(path, flags).map(Self::native)
    }

    /// Open `path` with `flags` on the SQLite VFS named `vfs`.
    pub fn open_with_flags_and_vfs(
        path: impl AsRef<std::path::Path>,
        flags: rusqlite::OpenFlags,
        vfs: &str,
    ) -> SqlResult<Self> {
        rusqlite::Connection::open_with_flags_and_vfs(path, flags, vfs).map(Self::native)
    }

    /// Identifies this store among the open ones, for state kept beside it
    /// (a scope's tag-undo log, its rollback count).
    pub(crate) fn key(&self) -> usize {
        self.key
    }

    /// The native connection, for what only native SQLite has (its
    /// planner, limits and tracing); `None` on a managed store.
    pub(crate) fn native_conn(&self) -> Option<&rusqlite::Connection> {
        match &self.owner {
            Owner::Native(conn) => Some(conn),
            Owner::Managed(_) => None,
        }
    }

    /// Prepare `sql` through the statement cache.
    pub fn prepare_cached(&self, sql: &str) -> SqlResult<Statement<'_>> {
        match &self.owner {
            Owner::Native(conn) => Ok(Statement::Native(conn.prepare_cached(sql)?)),
            Owner::Managed(managed) => Ok(Statement::Managed {
                backend: &*managed.backend,
                sql: sql.to_owned(),
                bindings: Vec::new(),
            }),
        }
    }

    /// Prepare `sql`, uncached on native SQLite.
    pub fn prepare(&self, sql: &str) -> SqlResult<Statement<'_>> {
        match &self.owner {
            Owner::Native(conn) => Ok(Statement::Uncached(conn.prepare(sql)?)),
            Owner::Managed(_) => self.prepare_cached(sql),
        }
    }

    /// Run one statement; how many rows it changed.
    /// Uncached on native SQLite, as rusqlite's own `execute`: one-off
    /// statements (DDL, PRAGMAs) stay out of the statement cache.
    pub fn execute<P: Params>(&self, sql: &str, params: P) -> SqlResult<usize> {
        self.prepare(sql)?.execute(params)
    }

    /// Run statements with no parameters and no results.
    pub fn execute_batch(&self, sql: &str) -> SqlResult<()> {
        match &self.owner {
            Owner::Native(conn) => conn.execute_batch(sql),
            Owner::Managed(managed) => managed.backend.execute_script(sql),
        }
    }

    /// Run a query expected to return a row; `f` reads the first.
    pub fn query_row<T, P: Params>(
        &self,
        sql: &str,
        params: P,
        f: impl FnOnce(&Row<'_>) -> SqlResult<T>,
    ) -> SqlResult<T> {
        // Uncached on native SQLite, as rusqlite's own `query_row`.
        self.prepare(sql)?.query_row(params, f)
    }

    /// Whether no transaction or scope is open.
    pub fn is_autocommit(&self) -> bool {
        match &self.owner {
            Owner::Native(conn) => conn.is_autocommit(),
            Owner::Managed(managed) => managed.hooks.lock().expect("hooks").depth == 0,
        }
    }

    /// How long a statement waits for another connection's lock (native
    /// only: a managed store has no other connection).
    pub fn busy_timeout(&self, timeout: std::time::Duration) -> SqlResult<()> {
        match &self.owner {
            Owner::Native(conn) => conn.busy_timeout(timeout),
            Owner::Managed(_) => Ok(()),
        }
    }

    /// The rowid of the last row inserted.
    pub fn last_insert_rowid(&self) -> SqlResult<i64> {
        match &self.owner {
            Owner::Native(conn) => Ok(conn.last_insert_rowid()),
            Owner::Managed(managed) => managed.backend.last_insert_rowid(),
        }
    }

    /// How many rows all statements so far changed.
    pub fn total_changes(&self) -> SqlResult<u64> {
        match &self.owner {
            Owner::Native(conn) => Ok(conn.total_changes()),
            Owner::Managed(managed) => managed.backend.total_changes(),
        }
    }

    /// The most `?` parameters a statement may bind.
    pub fn max_parameters(&self) -> SqlResult<usize> {
        match &self.owner {
            Owner::Native(conn) => conn
                .limit(rusqlite::limits::Limit::SQLITE_LIMIT_VARIABLE_NUMBER)
                .map(|n| n as usize),
            Owner::Managed(managed) => Ok(managed.backend.max_parameters()),
        }
    }

    /// How many prepared statements the cache keeps (native only).
    pub fn set_prepared_statement_cache_capacity(&self, capacity: usize) {
        if let Owner::Native(conn) = &self.owner {
            conn.set_prepared_statement_cache_capacity(capacity);
        }
    }

    /// Run `hook` as each outermost transaction (or scope) commits; `true`
    /// turns the commit into a rollback.
    pub fn commit_hook(
        &self,
        hook: Option<impl FnMut() -> bool + Send + 'static>,
    ) -> SqlResult<()> {
        match &self.owner {
            Owner::Native(conn) => conn.commit_hook(hook),
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
            Owner::Native(conn) => conn.rollback_hook(hook),
            Owner::Managed(managed) => {
                managed.hooks.lock().expect("hooks").rollback = hook.map(|h| Box::new(h) as _);
                Ok(())
            }
        }
    }
}

impl crate::values::CachedRow for Storage {
    fn cached_row<T, P, F>(&self, sql: &str, params: P, f: F) -> SqlResult<T>
    where
        P: Params,
        F: FnOnce(&Row<'_>) -> SqlResult<T>,
    {
        self.prepare_cached(sql)?.query_row(params, f)
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

pub use rusqlite::params;

fn bind_iter<I: IntoIterator>(statement: &mut Statement<'_>, values: I) -> SqlResult<()>
where
    I::Item: ToSql,
{
    match statement {
        Statement::Native(statement) => bind_native(statement, values),
        Statement::Uncached(statement) => bind_native(statement, values),
        Statement::Managed { bindings, .. } => {
            bindings.clear();
            for value in values {
                bindings.push(match value.to_sql()? {
                    types::ToSqlOutput::Borrowed(value) => Value::from(value),
                    types::ToSqlOutput::Owned(ref value) => {
                        Value::from(types::ValueRef::from(value))
                    }
                    _ => return Err(SqlError::InvalidQuery),
                });
            }
            Ok(())
        }
    }
}

fn bind_native<I: IntoIterator>(statement: &mut rusqlite::Statement<'_>, values: I) -> SqlResult<()>
where
    I::Item: ToSql,
{
    let mut n = 0;
    for value in values {
        n += 1;
        statement.raw_bind_parameter(n, value)?;
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
    Native(rusqlite::CachedStatement<'a>),
    /// Native, uncached.
    Uncached(rusqlite::Statement<'a>),
    /// Managed: the SQL and its bound cells, run by the backend.
    Managed {
        backend: &'a dyn Backend,
        sql: String,
        bindings: Vec<Value>,
    },
}

/// A cached statement (the same type: managed SQL has no cache).
pub type CachedStatement<'a> = Statement<'a>;

impl Statement<'_> {
    /// Run it with `params`; how many rows it changed.
    pub fn execute<P: Params>(&mut self, params: P) -> SqlResult<usize> {
        params.bind(self)?;
        match self {
            Self::Native(statement) => statement.raw_execute(),
            Self::Uncached(statement) => statement.raw_execute(),
            Self::Managed {
                backend,
                sql,
                bindings,
            } => backend.execute(sql, bindings),
        }
    }

    /// Run it with `params`; its rows.
    pub fn query<P: Params>(&mut self, params: P) -> SqlResult<Rows<'_>> {
        params.bind(self)?;
        match self {
            Self::Native(statement) => Ok(Rows::Native(statement.raw_query())),
            Self::Uncached(statement) => Ok(Rows::Native(statement.raw_query())),
            Self::Managed {
                backend,
                sql,
                bindings,
            } => {
                let cursor = backend.query(sql, bindings)?;
                let columns = cursor.columns().to_vec();
                Ok(Rows::Managed { cursor, columns })
            }
        }
    }

    /// Run it with `params`; `f` reads the first row.
    pub fn query_row<T, P: Params>(
        &mut self,
        params: P,
        f: impl FnOnce(&Row<'_>) -> SqlResult<T>,
    ) -> SqlResult<T> {
        let mut rows = self.query(params)?;
        let row = rows.next()?.ok_or(SqlError::QueryReturnedNoRows)?;
        f(&row)
    }

    /// Run it with `params`; each row mapped by `f`.
    pub fn query_map<T, P: Params, F: FnMut(&Row<'_>) -> SqlResult<T>>(
        &mut self,
        params: P,
        f: F,
    ) -> SqlResult<MappedRows<'_, F>> {
        Ok(MappedRows {
            rows: self.query(params)?,
            f,
        })
    }

    /// Whether it returns a row with `params`.
    pub fn exists<P: Params>(&mut self, params: P) -> SqlResult<bool> {
        Ok(self.query(params)?.next()?.is_some())
    }

    /// A SQLite VM counter (native only; 0 on managed SQL, which has none:
    /// diagnostics only, never a correctness decision).
    pub fn get_status(&self, status: StatementStatus) -> i32 {
        match self {
            Self::Native(statement) => statement.get_status(status),
            Self::Uncached(statement) => statement.get_status(status),
            Self::Managed { .. } => 0,
        }
    }

    /// A SQLite VM counter, reset (as [`Statement::get_status`]).
    pub fn reset_status(&self, status: StatementStatus) -> i32 {
        match self {
            Self::Native(statement) => statement.reset_status(status),
            Self::Uncached(statement) => statement.reset_status(status),
            Self::Managed { .. } => 0,
        }
    }

    /// Release it now rather than when dropped.
    pub fn finalize(self) -> SqlResult<()> {
        match self {
            Self::Uncached(statement) => statement.finalize(),
            Self::Native(_) | Self::Managed { .. } => Ok(()),
        }
    }
}

/// A query's rows.
pub enum Rows<'a> {
    /// Native: rusqlite's rows.
    Native(rusqlite::Rows<'a>),
    /// Managed: the backend's cursor.
    Managed {
        cursor: Box<dyn Cursor + 'a>,
        columns: Vec<String>,
    },
}

impl Rows<'_> {
    /// The next row, or `None` past the last. A lending cursor: the row
    /// borrows this call's cursor state.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> SqlResult<Option<Row<'_>>> {
        match self {
            Self::Native(rows) => Ok(rows.next()?.map(Row::Native)),
            Self::Managed { cursor, columns } => {
                Ok(cursor.next()?.map(|cells| Row::Managed { cells, columns }))
            }
        }
    }
}

/// Rows mapped by a function ([`Statement::query_map`]).
pub struct MappedRows<'a, F> {
    rows: Rows<'a>,
    f: F,
}

impl<T, F: FnMut(&Row<'_>) -> SqlResult<T>> Iterator for MappedRows<'_, F> {
    type Item = SqlResult<T>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.rows.next() {
            Ok(Some(row)) => Some((self.f)(&row)),
            Ok(None) => None,
            Err(error) => Some(Err(error)),
        }
    }
}

/// One row of a query's result.
pub enum Row<'a> {
    /// Native: rusqlite's row.
    Native(&'a rusqlite::Row<'a>),
    /// Managed: the cursor's cells and the result's column names.
    Managed {
        cells: &'a [Value],
        columns: &'a [String],
    },
}

impl Row<'_> {
    /// Column `index` (a position or a name), converted to `T`.
    pub fn get<I: ColumnIndex, T: types::FromSql>(&self, index: I) -> SqlResult<T> {
        let index = index.index(self)?;
        let value = self.get_ref(index)?;
        T::column_result(value).map_err(|error| match error {
            types::FromSqlError::InvalidType => SqlError::InvalidColumnType(
                index,
                self.column_name(index).unwrap_or("").into(),
                value.data_type(),
            ),
            types::FromSqlError::OutOfRange(n) => SqlError::IntegralValueOutOfRange(index, n),
            error => SqlError::FromSqlConversionFailure(index, value.data_type(), Box::new(error)),
        })
    }

    /// Column `index`, as SQLite holds it.
    pub fn get_ref(&self, index: usize) -> SqlResult<types::ValueRef<'_>> {
        match self {
            Self::Native(row) => row.get_ref(index),
            Self::Managed { cells, .. } => cells
                .get(index)
                .map(Value::as_value_ref)
                .ok_or(SqlError::InvalidColumnIndex(index)),
        }
    }

    /// How many columns the row has.
    pub fn column_count(&self) -> usize {
        match self {
            Self::Native(row) => row.as_ref().column_count(),
            Self::Managed { columns, .. } => columns.len(),
        }
    }

    /// Column `index`'s name.
    pub fn column_name(&self, index: usize) -> SqlResult<&str> {
        match self {
            Self::Native(row) => row.as_ref().column_name(index),
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
    SqlError::SqliteFailure(rusqlite::ffi::Error::new(code), Some(message))
}

/// What a scope ([`Storage::scope_then`]) does with what it wrote.
pub enum Scoped<T> {
    /// Keep it (committed with the outermost scope).
    Keep(T),
    /// Undo it.
    Undo(T),
}

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

    impl Cursor for Materialized {
        fn next(&mut self) -> SqlResult<Option<&[Value]>> {
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

    impl Backend for EmulatedObjectSql {
        fn query(&self, sql: &str, parameters: &[Value]) -> SqlResult<Box<dyn Cursor + '_>> {
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
                    let cell = Value::from(row.get_ref(i)?);
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

        fn execute(&self, sql: &str, parameters: &[Value]) -> SqlResult<usize> {
            super::durable_object_refusal(sql, false)?;
            let conn = self.conn.lock().expect("connection");
            let mut statement = conn.prepare_cached(sql)?;
            statement.execute(rusqlite::params_from_iter(parameters.iter()))
        }

        fn execute_script(&self, script: &str) -> SqlResult<()> {
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

        fn transaction(&self, body: &mut dyn FnMut() -> bool) -> SqlResult<()> {
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
                return Err(error);
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
                Ok(_) => end,
            }
        }

        fn last_insert_rowid(&self) -> SqlResult<i64> {
            Ok(self.conn.lock().expect("connection").last_insert_rowid())
        }

        fn total_changes(&self) -> SqlResult<u64> {
            Ok(self.conn.lock().expect("connection").total_changes())
        }

        fn max_parameters(&self) -> usize {
            100
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
    fn both() -> [(&'static str, Storage); 2] {
        let native = Storage::open_in_memory().unwrap();
        let managed = Storage::managed_emulation_in_memory().unwrap();
        for storage in [&native, &managed] {
            storage.execute_batch("CREATE TABLE t (v INTEGER)").unwrap();
        }
        [("native", native), ("managed", managed)]
    }

    fn values(storage: &Storage) -> Vec<i64> {
        let mut statement = storage.prepare("SELECT v FROM t ORDER BY v").unwrap();
        statement
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    }

    fn insert(storage: &Storage, v: i64) -> Result<(), Error> {
        storage
            .execute("INSERT INTO t (v) VALUES (?1)", [v])
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
        for (owner, storage) in both() {
            storage.scope("a", |s| insert(s, 1)).unwrap();
            let failed = storage.scope("b", |s| {
                insert(s, 2)?;
                Err::<(), _>(fail())
            });
            assert!(failed.is_err(), "{owner}");
            assert_eq!(values(&storage), [1], "{owner}");
            assert!(storage.is_autocommit(), "{owner}: nothing stays open");
        }
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
        for (owner, storage) in both() {
            storage
                .scope("outer", |s| {
                    insert(s, 1)?;
                    let inner = s.scope("inner", |s| {
                        insert(s, 2)?;
                        Err::<(), _>(fail())
                    });
                    assert!(inner.is_err());
                    assert!(!s.is_autocommit(), "the outer scope is still open");
                    insert(s, 3)
                })
                .unwrap();
            let failed = storage.scope("outer", |s| {
                insert(s, 4)?;
                s.scope("inner", |s| insert(s, 5))?;
                Err::<(), _>(fail())
            });
            assert!(failed.is_err(), "{owner}");
            assert_eq!(values(&storage), [1, 3], "{owner}");
            assert!(storage.is_autocommit(), "{owner}");
        }
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
        for (owner, storage) in both() {
            let seen = storage
                .scope_then("dry", |s| {
                    insert(s, 1).unwrap();
                    Scoped::Undo(values(s).len())
                })
                .unwrap();
            assert_eq!(seen, 1, "{owner}");
            assert_eq!(values(&storage), Vec::<i64>::new(), "{owner}");
            let kept = storage
                .scope_then("unit", |s| Scoped::Keep(insert(s, 2)))
                .unwrap();
            assert!(kept.is_ok(), "{owner}");
            assert_eq!(values(&storage), [2], "{owner}");
            assert!(storage.is_autocommit(), "{owner}");
        }
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
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        for (owner, storage) in both() {
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
                .scope("outer", |s| s.scope("inner", |s| insert(s, 1)))
                .unwrap();
            assert_eq!(commits.load(Ordering::SeqCst), 1, "{owner}");
            let _ = storage.scope("outer", |s| {
                insert(s, 2)?;
                Err::<(), _>(fail())
            });
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
            let vetoed = storage.scope("outer", |s| insert(s, 3));
            assert!(vetoed.is_err(), "{owner}: a vetoed commit fails");
            assert_eq!(
                rollbacks.load(Ordering::SeqCst),
                undone_rollbacks + 1,
                "{owner}"
            );
            assert_eq!(values(&storage), [1], "{owner}");
        }
    }

    /// Statements bind every parameter shape jazz-db uses and read rows
    /// by position and by name, through the same calls on both owners.
    #[test]
    fn statements_bind_and_rows_read_alike_on_both_owners() {
        for (owner, storage) in both() {
            storage
                .execute_batch("CREATE TABLE p (n INTEGER, s TEXT, b BLOB, r REAL)")
                .unwrap();
            storage
                .execute(
                    "INSERT INTO p VALUES (?1, ?2, ?3, ?4)",
                    params![7i64, "seven", vec![7u8], 7.5f64],
                )
                .unwrap();
            let values: Vec<Value> = vec![Value::Integer(8), Value::Text(b"eight".to_vec())];
            storage
                .execute(
                    "INSERT INTO p (n, s) VALUES (?1, ?2)",
                    params_from_iter(values.iter()),
                )
                .unwrap();
            let rows: Vec<(i64, String, Option<Vec<u8>>, Option<f64>)> = storage
                .prepare_cached("SELECT n, s, b, r FROM p WHERE n >= ?1 ORDER BY n")
                .unwrap()
                .query_map([7i64], |row| {
                    Ok((row.get(0)?, row.get("s")?, row.get(2)?, row.get(3)?))
                })
                .unwrap()
                .collect::<Result<_, _>>()
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
                .optional()
                .unwrap();
            assert_eq!(missing, None, "{owner}");
        }
    }

    /// Managed storage refuses what a Durable Object's SQL bridge refuses
    /// ([`durable_object_refusal`]): transaction control in SQL text (its
    /// only transactions are scopes), named parameters, and reading a
    /// write's rows. Integers keep their whole 64-bit range.
    #[test]
    fn managed_storage_refuses_what_a_durable_object_refuses() {
        let storage = Storage::managed_emulation_in_memory().unwrap();
        storage.execute_batch("CREATE TABLE t (v INTEGER)").unwrap();
        for sql in ["BEGIN", "SAVEPOINT s", "  release s", "COMMIT", "ROLLBACK"] {
            assert!(storage.execute_batch(sql).is_err(), "{sql}");
        }
        assert!(
            storage
                .execute("INSERT INTO t VALUES (:v)", [1i64])
                .is_err()
        );
        assert!(
            storage
                .query_row("INSERT INTO t VALUES (1) RETURNING v", [], |r| r
                    .get::<_, i64>(0))
                .is_err()
        );
        // Names inside strings and comments are not parameters.
        storage
            .execute("INSERT INTO t VALUES (length(':v @w $x')) -- :y", [])
            .unwrap();
        for n in [i64::MIN, -(1 << 53) - 1, (1 << 53) + 1, i64::MAX] {
            storage.execute("INSERT INTO t VALUES (?1)", [n]).unwrap();
            let read: i64 = storage
                .query_row("SELECT v FROM t WHERE v = ?1", [n], |r| r.get(0))
                .unwrap();
            assert_eq!(read, n);
        }
        assert!(storage.is_managed());
        assert!(!Storage::open_in_memory().unwrap().is_managed());
    }

    /// A scope whose body panics, caught by its host, still leaves the
    /// scope on managed storage: what it wrote is undone, nothing stays
    /// open, the rollback hook ran, and later scopes commit as before.
    #[test]
    fn a_panicking_scope_leaves_managed_storage_usable() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let storage = Storage::managed_emulation_in_memory().unwrap();
        storage.execute_batch("CREATE TABLE t (v INTEGER)").unwrap();
        let rollbacks = Arc::new(AtomicUsize::new(0));
        let counted = rollbacks.clone();
        storage
            .rollback_hook(Some(move || {
                counted.fetch_add(1, Ordering::SeqCst);
            }))
            .unwrap();
        let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = storage.scope("outer", |s| {
                insert(s, 1)?;
                s.scope("inner", |_| -> Result<(), Error> { panic!("injected") })
            });
        }));
        assert!(panicked.is_err());
        assert!(storage.is_autocommit());
        assert_eq!(rollbacks.load(Ordering::SeqCst), 1);
        assert_eq!(values(&storage), Vec::<i64>::new());
        storage.scope("after", |s| insert(s, 2)).unwrap();
        assert_eq!(values(&storage), [2]);
    }
}
