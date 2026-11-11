//! SQL execution and transaction ownership live on the same storage owner.
//! Native statements remain streamed/cached by rusqlite. Managed hosts expose
//! typed cells and a forward-only cursor, without implementing SQLite's C ABI.
pub use crate::values::Sql as Value;
pub use rusqlite::types;
pub use rusqlite::{Error, OpenFlags, OptionalExtension, Result, StatementStatus, ToSql, params};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[cfg(not(target_arch = "wasm32"))]
pub trait BackendThreading: Send + Sync {}
#[cfg(not(target_arch = "wasm32"))]
impl<T: Send + Sync> BackendThreading for T {}
#[cfg(target_arch = "wasm32")]
pub trait BackendThreading {}
#[cfg(target_arch = "wasm32")]
impl<T> BackendThreading for T {}

/// Numeric interpretation supplied by the query's author. Other JS values
/// (NULL, text and bytes) retain their own type. Unknown physical cells must
/// use Dynamic rather than guessing from a number's fractional part.
#[derive(Clone, Copy, serde::Serialize)]
pub enum ResultType {
    Dynamic,
    Integer,
    Real,
    NonNumeric,
}

#[derive(Default, serde::Serialize)]
pub struct ResultTypes {
    pub positional: Vec<ResultType>,
    pub named: std::collections::BTreeMap<String, ResultType>,
}
impl ResultTypes {
    pub fn positional(types: impl IntoIterator<Item = ResultType>) -> Self {
        Self {
            positional: types.into_iter().collect(),
            ..Self::default()
        }
    }
}

/// One SQL owner, including its transaction API. A transaction invokes its
/// callback synchronously once; false means rollback. Errors also roll back.
/// SQL called through this owner inside the callback belongs to that scope.
pub trait Backend: BackendThreading {
    fn query(&self, sql: &str, parameters: &[Value]) -> Result<Box<dyn Cursor + '_>>;
    fn query_typed(
        &self,
        sql: &str,
        parameters: &[Value],
        _types: &ResultTypes,
    ) -> Result<Box<dyn Cursor + '_>> {
        self.query(sql, parameters)
    }
    fn execute(&self, sql: &str, parameters: &[Value]) -> Result<usize>;
    fn execute_script(&self, sql: &str) -> Result<()>;
    fn transaction(&self, body: &mut dyn FnMut() -> bool) -> Result<()>;
    fn last_insert_rowid(&self) -> Result<i64>;
    fn total_changes(&self) -> Result<u64>;
    fn max_parameters(&self) -> usize;
}

/// At most one row is borrowed at a time. A host must not materialize an
/// unbounded result set to implement this interface.
pub trait Cursor {
    fn next(&mut self) -> Result<Option<&[Value]>>;
    fn columns(&self) -> &[String];
}

#[derive(Default)]
struct Hooks {
    depth: usize,
    commit: Option<Box<dyn FnMut() -> bool + Send>>,
    rollback: Option<Box<dyn FnMut() + Send>>,
}
#[derive(Clone)]
struct Managed {
    backend: Arc<dyn Backend>,
    hooks: Arc<Mutex<Hooks>>,
}
impl Managed {
    pub(crate) fn scope(&self, body: &mut dyn FnMut() -> bool) -> Result<()> {
        let outer = {
            let mut hooks = self.hooks.lock().unwrap();
            let outer = hooks.depth == 0;
            hooks.depth += 1;
            outer
        };
        let mut keep = false;
        let mut veto = false;
        let result = self.backend.transaction(&mut || {
            keep = body();
            if keep && outer {
                let mut hooks = self.hooks.lock().unwrap();
                veto = hooks.commit.as_mut().is_some_and(|hook| hook());
                keep &= !veto;
            }
            keep
        });
        let mut hooks = self.hooks.lock().unwrap();
        hooks.depth -= 1;
        if outer
            && (!keep || result.is_err())
            && let Some(hook) = hooks.rollback.as_mut()
        {
            hook();
        }
        result?;
        if veto {
            return Err(Error::InvalidQuery);
        }
        Ok(())
    }
}

enum Owner {
    #[cfg(feature = "sqlite")]
    Native(rusqlite::Connection),
    Managed(Managed),
}

/// An owned SQL store. Construction selects SQL execution and transactions
/// together. There is no connection-handle registry or separately installed
/// transaction driver.
pub struct Storage {
    owner: Owner,
    id: usize,
}
pub(crate) enum ScopeOutcome<T> {
    Commit(T),
    Rollback(T),
}

/// The engine context handed to a transaction owns this same store throughout
/// its callback. Access does not retain a RefCell borrow over engine re-entry.
pub(crate) trait StorageContext {
    fn with_storage<R>(&self, f: impl FnOnce(&Storage) -> R) -> R;
}
impl StorageContext for &Storage {
    fn with_storage<R>(&self, f: impl FnOnce(&Storage) -> R) -> R {
        f(self)
    }
}

impl Storage {
    pub(crate) fn transaction<C: StorageContext, T>(
        context: &mut C,
        name: &str,
        f: impl FnOnce(&mut C) -> std::result::Result<ScopeOutcome<T>, crate::options::Error>,
    ) -> std::result::Result<T, crate::options::Error> {
        let owner = context.with_storage(|conn| conn.managed());
        if let Some(owner) = owner {
            context.with_storage(crate::values::open_tag_undo);
            let mut callback = Some(f);
            let mut result = None;
            let completed = owner.scope(&mut || {
                let f = callback.take().expect("scope callback called twice");
                result = Some(f(context));
                matches!(&result, Some(Ok(ScopeOutcome::Commit(_))))
            });
            let keep = completed.is_ok() && matches!(&result, Some(Ok(ScopeOutcome::Commit(_))));
            context.with_storage(|conn| {
                crate::values::close_tag_undo(conn, keep, false);
                if !keep {
                    crate::values::note_rollback(conn.identity());
                }
            });
            completed.map_err(crate::values::storage)?;
            return result
                .expect("scope callback was not called")
                .map(|outcome| match outcome {
                    ScopeOutcome::Commit(value) | ScopeOutcome::Rollback(value) => value,
                });
        }
        context.with_storage(|conn| crate::values::begin_savepoint(conn, name))?;
        let result = f(context);
        let keep = matches!(&result, Ok(ScopeOutcome::Commit(_)));
        context.with_storage(|conn| crate::values::end_savepoint(conn, name, keep))?;
        result.map(|outcome| match outcome {
            ScopeOutcome::Commit(value) | ScopeOutcome::Rollback(value) => value,
        })
    }

    fn new(owner: Owner) -> Self {
        static IDS: AtomicUsize = AtomicUsize::new(1);
        Self {
            owner,
            id: IDS.fetch_add(1, Ordering::Relaxed),
        }
    }
    pub fn from_backend(backend: impl Backend + 'static) -> Self {
        Self::new(Owner::Managed(Managed {
            backend: Arc::new(backend),
            hooks: Arc::default(),
        }))
    }
    pub(crate) fn identity(&self) -> usize {
        self.id
    }
    /// Whether a scope can stay open between separate engine calls. Managed
    /// callback-owned stores require a different async/overlay implementation.
    pub(crate) fn supports_open_scopes(&self) -> bool {
        match &self.owner {
            #[cfg(feature = "sqlite")]
            Owner::Native(_) => true,
            Owner::Managed(_) => false,
        }
    }
    fn managed(&self) -> Option<Managed> {
        match &self.owner {
            #[cfg(feature = "sqlite")]
            Owner::Native(_) => None,
            Owner::Managed(owner) => Some(owner.clone()),
        }
    }
    pub fn open_in_memory() -> Result<Self> {
        Self::open(":memory:")
    }
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_flags(path, OpenFlags::default())
    }
    pub fn open_with_flags(path: impl AsRef<Path>, flags: OpenFlags) -> Result<Self> {
        #[cfg(feature = "sqlite")]
        {
            rusqlite::Connection::open_with_flags(path, flags)
                .map(|conn| Self::new(Owner::Native(conn)))
        }
        #[cfg(not(feature = "sqlite"))]
        {
            let _ = (path, flags);
            Err(Error::InvalidQuery)
        }
    }
    pub fn open_with_flags_and_vfs(
        path: impl AsRef<Path>,
        flags: OpenFlags,
        vfs: &str,
    ) -> Result<Self> {
        #[cfg(feature = "sqlite")]
        {
            rusqlite::Connection::open_with_flags_and_vfs(path, flags, vfs)
                .map(|conn| Self::new(Owner::Native(conn)))
        }
        #[cfg(not(feature = "sqlite"))]
        {
            let _ = (path, flags, vfs);
            Err(Error::InvalidQuery)
        }
    }
    #[cfg(any(
        feature = "sqlite",
        feature = "durable-object-limits",
        feature = "sql-trace"
    ))]
    pub(crate) fn native(&self) -> Option<&rusqlite::Connection> {
        match &self.owner {
            #[cfg(feature = "sqlite")]
            Owner::Native(conn) => Some(conn),
            _ => None,
        }
    }
    pub fn prepare_cached(&self, sql: &str) -> Result<Statement<'_>> {
        match &self.owner {
            #[cfg(feature = "sqlite")]
            Owner::Native(conn) => Ok(Statement::Native(conn.prepare_cached(sql)?)),
            Owner::Managed(owner) => Ok(Statement::Managed {
                backend: &*owner.backend,
                sql: sql.to_owned(),
                bindings: Vec::new(),
            }),
        }
    }
    pub fn prepare(&self, sql: &str) -> Result<Statement<'_>> {
        self.prepare_cached(sql)
    }
    pub fn execute<P: Params>(&self, sql: &str, params: P) -> Result<usize> {
        self.prepare_cached(sql)?.execute(params)
    }
    pub fn query_row<T, P: Params>(
        &self,
        sql: &str,
        params: P,
        f: impl FnOnce(&Row<'_>) -> Result<T>,
    ) -> Result<T> {
        self.prepare_cached(sql)?.query_row(params, f)
    }
    pub fn execute_batch(&self, sql: &str) -> Result<()> {
        match &self.owner {
            #[cfg(feature = "sqlite")]
            Owner::Native(conn) => conn.execute_batch(sql),
            Owner::Managed(owner) => owner.backend.execute_script(sql),
        }
    }
    pub fn execute_script(&self, script: &str) -> Result<()> {
        match &self.owner {
            #[cfg(feature = "sqlite")]
            Owner::Native(conn) => {
                let mut start = 0;
                for (end, _) in script.match_indices(';') {
                    let part = &script[start..=end];
                    let c = std::ffi::CString::new(part).map_err(|_| Error::InvalidQuery)?;
                    // SAFETY: SQLite only reads this NUL-terminated SQL string.
                    if unsafe { rusqlite::ffi::sqlite3_complete(c.as_ptr()) } != 0 {
                        conn.execute_batch(part)?;
                        start = end + 1;
                    }
                }
                if !script[start..].trim().is_empty() {
                    conn.execute_batch(&script[start..])?;
                }
                Ok(())
            }
            Owner::Managed(owner) => owner.backend.execute_script(script),
        }
    }
    pub fn is_autocommit(&self) -> bool {
        match &self.owner {
            #[cfg(feature = "sqlite")]
            Owner::Native(conn) => conn.is_autocommit(),
            Owner::Managed(owner) => owner.hooks.lock().unwrap().depth == 0,
        }
    }
    pub fn last_insert_rowid(&self) -> Result<i64> {
        match &self.owner {
            #[cfg(feature = "sqlite")]
            Owner::Native(conn) => Ok(conn.last_insert_rowid()),
            Owner::Managed(owner) => owner.backend.last_insert_rowid(),
        }
    }
    pub fn total_changes(&self) -> Result<u64> {
        match &self.owner {
            #[cfg(feature = "sqlite")]
            Owner::Native(conn) => Ok(conn.total_changes()),
            Owner::Managed(owner) => owner.backend.total_changes(),
        }
    }
    pub fn max_parameters(&self) -> Result<usize> {
        match &self.owner {
            #[cfg(feature = "sqlite")]
            Owner::Native(conn) => conn
                .limit(rusqlite::limits::Limit::SQLITE_LIMIT_VARIABLE_NUMBER)
                .map(|n| n as usize),
            Owner::Managed(owner) => Ok(owner.backend.max_parameters()),
        }
    }
    pub fn set_prepared_statement_cache_capacity(&self, capacity: usize) {
        #[cfg(feature = "sqlite")]
        if let Some(conn) = self.native() {
            conn.set_prepared_statement_cache_capacity(capacity);
        }
        #[cfg(not(feature = "sqlite"))]
        let _ = capacity;
    }
    pub fn busy_timeout(&self, timeout: Duration) -> Result<()> {
        #[cfg(feature = "sqlite")]
        if let Some(conn) = self.native() {
            return conn.busy_timeout(timeout);
        }
        let _ = timeout;
        Err(Error::InvalidQuery)
    }
    pub fn commit_hook(&self, hook: Option<impl FnMut() -> bool + Send + 'static>) -> Result<()> {
        match &self.owner {
            #[cfg(feature = "sqlite")]
            Owner::Native(conn) => conn.commit_hook(hook),
            Owner::Managed(owner) => {
                owner.hooks.lock().unwrap().commit = hook.map(|h| Box::new(h) as _);
                Ok(())
            }
        }
    }
    pub fn rollback_hook(&self, hook: Option<impl FnMut() + Send + 'static>) -> Result<()> {
        match &self.owner {
            #[cfg(feature = "sqlite")]
            Owner::Native(conn) => conn.rollback_hook(hook),
            Owner::Managed(owner) => {
                owner.hooks.lock().unwrap().rollback = hook.map(|h| Box::new(h) as _);
                Ok(())
            }
        }
    }
}

pub trait Params {
    fn bind(self, stmt: &mut Statement<'_>) -> Result<()>;
}
impl Params for [&dyn ToSql; 0] {
    fn bind(self, stmt: &mut Statement<'_>) -> Result<()> {
        bind_iter(stmt, self)
    }
}
macro_rules! array_params {
    ($($n:literal),*) => {$(impl<T: ToSql> Params for [T; $n] {
        fn bind(self, stmt: &mut Statement<'_>) -> Result<()> { bind_iter(stmt, self.iter()) }
    })*};
}
array_params!(
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26,
    27, 28, 29, 30, 31, 32
);
impl<T: ToSql> Params for &[T] {
    fn bind(self, stmt: &mut Statement<'_>) -> Result<()> {
        bind_iter(stmt, self.iter())
    }
}
pub struct ParamsFromIter<I>(I);
pub fn params_from_iter<I: IntoIterator>(iter: I) -> ParamsFromIter<I> {
    ParamsFromIter(iter)
}
impl<I: IntoIterator> Params for ParamsFromIter<I>
where
    I::Item: ToSql,
{
    fn bind(self, stmt: &mut Statement<'_>) -> Result<()> {
        bind_iter(stmt, self.0)
    }
}
fn bind_iter<I: IntoIterator>(stmt: &mut Statement<'_>, values: I) -> Result<()>
where
    I::Item: ToSql,
{
    match stmt {
        #[cfg(feature = "sqlite")]
        Statement::Native(stmt) => {
            let mut n = 0;
            for value in values {
                n += 1;
                stmt.raw_bind_parameter(n, value)?;
            }
            if n != stmt.parameter_count() {
                return Err(Error::InvalidParameterCount(n, stmt.parameter_count()));
            }
        }
        Statement::Managed { bindings, .. } => {
            bindings.clear();
            for value in values {
                let cell = match value.to_sql()? {
                    types::ToSqlOutput::Borrowed(value) => Value::from(value),
                    types::ToSqlOutput::Owned(ref value) => {
                        Value::from(types::ValueRef::from(value))
                    }
                    _ => return Err(Error::InvalidQuery),
                };
                bindings.push(cell);
            }
        }
    }
    Ok(())
}

pub enum Statement<'a> {
    #[cfg(feature = "sqlite")]
    Native(rusqlite::CachedStatement<'a>),
    Managed {
        backend: &'a dyn Backend,
        sql: String,
        bindings: Vec<Value>,
    },
}
impl Statement<'_> {
    pub fn execute<P: Params>(&mut self, params: P) -> Result<usize> {
        params.bind(self)?;
        match self {
            #[cfg(feature = "sqlite")]
            Self::Native(stmt) => stmt.raw_execute(),
            Self::Managed {
                backend,
                sql,
                bindings,
            } => backend.execute(sql, bindings),
        }
    }
    pub fn query<P: Params>(&mut self, params: P) -> Result<Rows<'_>> {
        self.query_typed(params, ResultTypes::default)
    }
    /// Result descriptions are built only for managed storage. Native reads
    /// keep rusqlite's borrowed rows and do not allocate a type description.
    pub fn query_typed<P: Params>(
        &mut self,
        params: P,
        types: impl FnOnce() -> ResultTypes,
    ) -> Result<Rows<'_>> {
        params.bind(self)?;
        match self {
            #[cfg(feature = "sqlite")]
            Self::Native(stmt) => Ok(Rows::Native(stmt.raw_query())),
            Self::Managed {
                backend,
                sql,
                bindings,
            } => {
                let cursor = backend.query_typed(sql, bindings, &types())?;
                let columns = cursor.columns().to_vec();
                Ok(Rows::Managed { cursor, columns })
            }
        }
    }
    pub fn query_row<T, P: Params>(
        &mut self,
        params: P,
        f: impl FnOnce(&Row<'_>) -> Result<T>,
    ) -> Result<T> {
        let mut rows = self.query(params)?;
        let row = rows.next()?.ok_or(Error::QueryReturnedNoRows)?;
        f(&row)
    }
    pub fn query_map<T, P: Params, F: FnMut(&Row<'_>) -> Result<T>>(
        &mut self,
        params: P,
        f: F,
    ) -> Result<MappedRows<'_, F>> {
        Ok(MappedRows {
            rows: self.query(params)?,
            f,
        })
    }
    pub fn query_map_typed<P: Params, T, F: FnMut(&Row<'_>) -> Result<T>>(
        &mut self,
        params: P,
        types: impl FnOnce() -> ResultTypes,
        f: F,
    ) -> Result<MappedRows<'_, F>> {
        Ok(MappedRows {
            rows: self.query_typed(params, types)?,
            f,
        })
    }
    pub fn reset_status(&self, status: StatementStatus) -> i32 {
        match self {
            #[cfg(feature = "sqlite")]
            Self::Native(stmt) => stmt.reset_status(status),
            // Managed SQL has no SQLite VM counters; callers must not use
            // these diagnostics as a query correctness or admission decision.
            Self::Managed { .. } => {
                let _ = status;
                0
            }
        }
    }
}
pub enum Rows<'a> {
    #[cfg(feature = "sqlite")]
    Native(rusqlite::Rows<'a>),
    Managed {
        cursor: Box<dyn Cursor + 'a>,
        columns: Vec<String>,
    },
}
impl Rows<'_> {
    // This is a lending cursor: the row borrows this call's cursor state,
    // which cannot be expressed by std::Iterator's fixed Item lifetime.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<Row<'_>>> {
        match self {
            #[cfg(feature = "sqlite")]
            Self::Native(rows) => Ok(rows.next()?.map(Row::Native)),
            Self::Managed { cursor, columns } => {
                Ok(cursor.next()?.map(|cells| Row::Managed { cells, columns }))
            }
        }
    }
}
pub struct MappedRows<'a, F> {
    rows: Rows<'a>,
    f: F,
}
impl<T, F: FnMut(&Row<'_>) -> Result<T>> Iterator for MappedRows<'_, F> {
    type Item = Result<T>;
    fn next(&mut self) -> Option<Self::Item> {
        match self.rows.next() {
            Ok(Some(row)) => Some((self.f)(&row)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}
pub enum Row<'a> {
    #[cfg(feature = "sqlite")]
    Native(&'a rusqlite::Row<'a>),
    Managed {
        cells: &'a [Value],
        columns: &'a [String],
    },
}
impl Row<'_> {
    pub fn get<I: ColumnIndex, T: types::FromSql>(&self, index: I) -> Result<T> {
        let index = index.index(self)?;
        let value = self.get_ref(index)?;
        T::column_result(value).map_err(|error| match error {
            types::FromSqlError::InvalidType => Error::InvalidColumnType(
                index,
                self.column_name(index).unwrap_or("").into(),
                value.data_type(),
            ),
            types::FromSqlError::OutOfRange(n) => Error::IntegralValueOutOfRange(index, n),
            error => Error::FromSqlConversionFailure(index, value.data_type(), Box::new(error)),
        })
    }
    pub fn get_ref(&self, index: usize) -> Result<types::ValueRef<'_>> {
        match self {
            #[cfg(feature = "sqlite")]
            Self::Native(row) => row.get_ref(index),
            Self::Managed { cells, .. } => cells
                .get(index)
                .map(Value::as_ref)
                .ok_or(Error::InvalidColumnIndex(index)),
        }
    }
    pub fn column_count(&self) -> usize {
        match self {
            #[cfg(feature = "sqlite")]
            Self::Native(row) => row.as_ref().column_count(),
            Self::Managed { columns, .. } => columns.len(),
        }
    }
    pub fn column_name(&self, index: usize) -> Result<&str> {
        match self {
            #[cfg(feature = "sqlite")]
            Self::Native(row) => row.as_ref().column_name(index),
            Self::Managed { columns, .. } => columns
                .get(index)
                .map(String::as_str)
                .ok_or(Error::InvalidColumnIndex(index)),
        }
    }
}
pub trait ColumnIndex {
    fn index(self, row: &Row<'_>) -> Result<usize>;
}
impl ColumnIndex for usize {
    fn index(self, _: &Row<'_>) -> Result<usize> {
        Ok(self)
    }
}
impl ColumnIndex for &str {
    fn index(self, row: &Row<'_>) -> Result<usize> {
        (0..row.column_count())
            .find(|&i| {
                row.column_name(i)
                    .is_ok_and(|n| n.eq_ignore_ascii_case(self))
            })
            .ok_or_else(|| Error::InvalidColumnName(self.into()))
    }
}

pub fn backend_error(code: i32, message: String) -> Error {
    Error::SqliteFailure(rusqlite::ffi::Error::new(code), Some(message))
}

/// Managed storage over a native connection, for tests: the engine sees the
/// same owner a Durable Object host gives it ([`Storage::from_backend`]),
/// so it takes the managed paths (callback scopes only, no open savepoint
/// between calls), while SQLite itself stays native. Like `ctx.storage.sql`,
/// it refuses transaction control in SQL text: scopes come only from
/// [`Backend::transaction`], nested as `transactionSync` nests.
#[cfg(all(feature = "sqlite", any(test, feature = "testing")))]
pub mod managed_emulation {
    use super::{Backend, Cursor, Error, Result, Value};
    use std::sync::Mutex;

    /// Clones share one database, as a Durable Object's successive
    /// instances share its storage: open a new [`super::Storage`] over a
    /// clone to "restart" it.
    #[derive(Clone)]
    pub struct EmulatedObjectSql {
        conn: std::sync::Arc<Mutex<rusqlite::Connection>>,
        depth: std::sync::Arc<Mutex<usize>>,
    }

    struct Materialized {
        columns: Vec<String>,
        rows: std::vec::IntoIter<Vec<Value>>,
        current: Vec<Value>,
    }
    impl Cursor for Materialized {
        fn next(&mut self) -> Result<Option<&[Value]>> {
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

    fn refuse_transaction_control(sql: &str) -> Result<()> {
        let first = sql
            .trim_start()
            .split(|c: char| c.is_whitespace() || c == ';')
            .next()
            .unwrap_or("")
            .to_ascii_uppercase();
        if matches!(
            first.as_str(),
            "BEGIN" | "COMMIT" | "END" | "ROLLBACK" | "SAVEPOINT" | "RELEASE"
        ) {
            return Err(super::backend_error(
                1,
                format!("transaction control in SQL is not allowed on managed storage: {sql}"),
            ));
        }
        Ok(())
    }

    impl EmulatedObjectSql {
        pub fn open_in_memory() -> Result<Self> {
            let conn = rusqlite::Connection::open_in_memory()?;
            #[cfg(feature = "durable-object-limits")]
            crate::durable_object_limits::apply(&conn);
            Ok(Self {
                conn: std::sync::Arc::new(Mutex::new(conn)),
                depth: std::sync::Arc::default(),
            })
        }
    }

    impl Backend for EmulatedObjectSql {
        // Rows are read out before returning, so the engine may issue more
        // SQL while it walks them, as it may with a DO cursor.
        fn query(&self, sql: &str, parameters: &[Value]) -> Result<Box<dyn Cursor + '_>> {
            refuse_transaction_control(sql)?;
            let conn = self.conn.lock().unwrap();
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
                    cells.push(Value::from(row.get_ref(i)?));
                }
                out.push(cells);
            }
            Ok(Box::new(Materialized {
                columns,
                rows: out.into_iter(),
                current: Vec::new(),
            }))
        }
        fn execute(&self, sql: &str, parameters: &[Value]) -> Result<usize> {
            refuse_transaction_control(sql)?;
            let conn = self.conn.lock().unwrap();
            let mut statement = conn.prepare_cached(sql)?;
            statement.execute(rusqlite::params_from_iter(parameters.iter()))
        }
        fn execute_script(&self, script: &str) -> Result<()> {
            let mut start = 0;
            for (end, _) in script.match_indices(';') {
                let part = &script[start..=end];
                let c = std::ffi::CString::new(part).map_err(|_| Error::InvalidQuery)?;
                // SAFETY: SQLite only reads this NUL-terminated SQL string.
                if unsafe { rusqlite::ffi::sqlite3_complete(c.as_ptr()) } != 0 {
                    refuse_transaction_control(part)?;
                    self.conn.lock().unwrap().execute_batch(part)?;
                    start = end + 1;
                }
            }
            if !script[start..].trim().is_empty() {
                refuse_transaction_control(&script[start..])?;
                self.conn.lock().unwrap().execute_batch(&script[start..])?;
            }
            Ok(())
        }
        fn transaction(&self, body: &mut dyn FnMut() -> bool) -> Result<()> {
            let name = {
                let mut depth = self.depth.lock().unwrap();
                *depth += 1;
                format!("do_scope_{depth}")
            };
            let begin = self
                .conn
                .lock()
                .unwrap()
                .execute_batch(&format!("SAVEPOINT {name}"));
            if let Err(error) = begin {
                *self.depth.lock().unwrap() -= 1;
                return Err(error);
            }
            // Like `transactionSync`, a panic in the body rolls back too.
            let keep = std::panic::catch_unwind(std::panic::AssertUnwindSafe(&mut *body));
            let conn = self.conn.lock().unwrap();
            let end = match keep {
                Ok(true) => conn.execute_batch(&format!("RELEASE {name}")),
                _ => conn.execute_batch(&format!("ROLLBACK TO {name}; RELEASE {name}")),
            };
            drop(conn);
            *self.depth.lock().unwrap() -= 1;
            match keep {
                Err(panic) => std::panic::resume_unwind(panic),
                Ok(_) => end,
            }
        }
        fn last_insert_rowid(&self) -> Result<i64> {
            Ok(self.conn.lock().unwrap().last_insert_rowid())
        }
        fn total_changes(&self) -> Result<u64> {
            Ok(self.conn.lock().unwrap().total_changes())
        }
        fn max_parameters(&self) -> usize {
            100
        }
    }
}

#[cfg(all(feature = "sqlite", any(test, feature = "testing")))]
impl Storage {
    /// An in-memory store behind [`managed_emulation::EmulatedObjectSql`].
    pub fn managed_emulation_in_memory() -> Result<Self> {
        managed_emulation::EmulatedObjectSql::open_in_memory().map(Self::from_backend)
    }
}

/// Where the chunk cache reads nodes and epochs it does not hold: a native
/// file's second, read-only connection, or a managed store's own backend.
/// The managed reader sees the owner's open scope too; what that scope
/// wrote is already cached pinned, so it reads only what the cache lacks.
pub(crate) enum FileReader {
    #[cfg(feature = "sqlite")]
    Native(rusqlite::Connection),
    Managed(SharedBackend),
}

/// A managed backend shared with the chunk cache, which the store's commit
/// and rollback hooks (`Send`) hold. Native backends are `Send + Sync`
/// ([`BackendThreading`]). A wasm32 host without threads has one thread, so
/// nothing can send or share it elsewhere; with threads this does not
/// compile rather than assert it.
pub(crate) struct SharedBackend(Arc<dyn Backend>);
// SAFETY: wasm32 without the `atomics` target feature runs one thread.
#[cfg(all(target_arch = "wasm32", not(target_feature = "atomics")))]
unsafe impl Send for SharedBackend {}
// SAFETY: as above.
#[cfg(all(target_arch = "wasm32", not(target_feature = "atomics")))]
unsafe impl Sync for SharedBackend {}

impl FileReader {
    pub(crate) fn open(path: &Path, flags: OpenFlags, vfs: Option<&str>) -> Result<Self> {
        #[cfg(feature = "sqlite")]
        {
            let conn = match vfs {
                Some(vfs) => rusqlite::Connection::open_with_flags_and_vfs(path, flags, vfs)?,
                None => rusqlite::Connection::open_with_flags(path, flags)?,
            };
            Ok(Self::Native(conn))
        }
        #[cfg(not(feature = "sqlite"))]
        {
            let _ = (path, flags, vfs);
            Err(Error::InvalidQuery)
        }
    }
    pub(crate) fn prepare_cached(&self, sql: &str) -> Result<Statement<'_>> {
        match self {
            #[cfg(feature = "sqlite")]
            Self::Native(conn) => Ok(Statement::Native(conn.prepare_cached(sql)?)),
            Self::Managed(SharedBackend(backend)) => Ok(Statement::Managed {
                backend: &**backend,
                sql: sql.to_owned(),
                bindings: Vec::new(),
            }),
        }
    }
}

impl Storage {
    /// A managed store's chunk reader: its own backend, shared. Native
    /// stores have none (a file opens its own; memory has only the cache).
    pub(crate) fn managed_reader(&self) -> Option<FileReader> {
        self.managed()
            .map(|owner| FileReader::Managed(SharedBackend(owner.backend.clone())))
    }
}

#[cfg(all(test, feature = "sqlite"))]
mod scope_failure_tests {
    use super::*;
    use crate::values::{EnumTags, tags_changing};
    use std::{cell::RefCell, rc::Rc};

    /// Adapter-level failure injection: Alice's outer tag and Bob's inner tag
    /// both disappear when SQLite aborts the entire transaction. Public Db
    /// writes cannot install RAISE(ROLLBACK) triggers or inspect enum undo logs.
    #[test]
    fn database_rollback_undoes_all_enclosing_scope_state() {
        let store = Storage::open_in_memory().unwrap();
        store.execute_batch("CREATE TABLE items(id INTEGER); CREATE TRIGGER fail BEFORE INSERT ON items BEGIN SELECT RAISE(ROLLBACK, 'abort'); END;").unwrap();
        let tags = Rc::new(RefCell::new(EnumTags::default()));
        let mut context = &store;
        let result = Storage::transaction(&mut context, "alice", |context| {
            tags_changing(context, &tags);
            tags.borrow_mut().insert("alice".into(), 0);
            Storage::transaction(context, "bob", |context| {
                tags_changing(context, &tags);
                tags.borrow_mut().insert("bob".into(), 1);
                context
                    .execute("INSERT INTO items VALUES (1)", [])
                    .map_err(crate::values::storage)?;
                Ok(ScopeOutcome::Commit(()))
            })?;
            Ok(ScopeOutcome::Commit(()))
        });
        assert!(result.is_err());
        assert!(store.is_autocommit());
        assert_eq!(tags.borrow().tag("alice"), None);
        assert_eq!(tags.borrow().tag("bob"), None);
        assert_eq!(
            store
                .query_row("SELECT count(*) FROM items", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            0
        );
    }
}
