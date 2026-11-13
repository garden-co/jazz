//! Experimental SQL execution boundary. The default build has no SQLite
//! dependency. The optional native adapter executes inline on its owner thread;
//! neither interface requires a runtime, boxed futures, or Send.
//! Jazz Db uses this boundary for SQL execution; connection ownership and
//! complete-operation scheduling live in jazz-db.
//!
//! Compatibility: host tables retain `_jazz_host_<name>`, BLOB primary keys and
//! BLOB values, WITHOUT ROWID. No durable or wire encoding changes.

use std::future::Future;
use std::ops::ControlFlow;

#[cfg(feature = "sqlite")]
pub mod native;

/// Borrow arguments on the native path; asynchronous providers may copy them
/// into their worker command before yielding.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Param<'a> {
    Null,
    Integer(i64),
    Real(f64),
    Blob(&'a [u8]),
    Text(&'a str),
    /// SQLite TEXT without UTF-8 interpretation. A provider unable to retain
    /// these bytes must reject the parameter rather than replace bytes.
    TextBytes(&'a [u8]),
}

/// A borrowed SQLite cell. TEXT retains its bytes, including embedded NULs.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Cell<'a> {
    Null,
    Integer(i64),
    Real(f64),
    Text(&'a [u8]),
    Blob(&'a [u8]),
}

impl<'a> Cell<'a> {
    pub fn as_i64(self) -> Result<i64, CellError> {
        match self {
            Self::Integer(value) => Ok(value),
            _ => Err(CellError("expected an INTEGER cell")),
        }
    }
    pub fn as_blob(self) -> Result<&'a [u8], CellError> {
        match self {
            Self::Blob(value) => Ok(value),
            _ => Err(CellError("expected a BLOB cell")),
        }
    }
}

/// A cell could not be decoded as the requested Rust type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CellError(pub &'static str);

impl std::fmt::Display for CellError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for CellError {}

pub trait FromCell: Sized {
    fn from_cell(cell: Cell<'_>) -> Result<Self, CellError>;
}
impl FromCell for i64 {
    fn from_cell(cell: Cell<'_>) -> Result<Self, CellError> {
        match cell {
            Cell::Integer(value) => Ok(value),
            _ => Err(CellError("expected an INTEGER cell")),
        }
    }
}
impl FromCell for f64 {
    fn from_cell(cell: Cell<'_>) -> Result<Self, CellError> {
        match cell {
            Cell::Real(value) => Ok(value),
            Cell::Integer(value) => Ok(value as f64),
            _ => Err(CellError("expected a REAL or INTEGER cell")),
        }
    }
}
macro_rules! integer_cells {
    ($($ty:ty),* $(,)?) => {$ (
        impl FromCell for $ty {
            fn from_cell(cell: Cell<'_>) -> Result<Self, CellError> {
                i64::from_cell(cell)?.try_into().map_err(|_| CellError("INTEGER is out of range"))
            }
        }
    )*};
}
integer_cells!(i8, i16, i32, isize, u8, u16, u32, u64, usize);

impl FromCell for bool {
    fn from_cell(cell: Cell<'_>) -> Result<Self, CellError> {
        i64::from_cell(cell).map(|value| value != 0)
    }
}
impl FromCell for String {
    fn from_cell(cell: Cell<'_>) -> Result<Self, CellError> {
        match cell {
            Cell::Text(value) => std::str::from_utf8(value)
                .map(str::to_owned)
                .map_err(|_| CellError("TEXT is not valid UTF-8")),
            _ => Err(CellError("expected a TEXT cell")),
        }
    }
}
impl FromCell for Vec<u8> {
    fn from_cell(cell: Cell<'_>) -> Result<Self, CellError> {
        match cell {
            Cell::Blob(value) => Ok(value.to_vec()),
            _ => Err(CellError("expected a BLOB cell")),
        }
    }
}
impl<T: FromCell> FromCell for Option<T> {
    fn from_cell(cell: Cell<'_>) -> Result<Self, CellError> {
        match cell {
            Cell::Null => Ok(None),
            other => T::from_cell(other).map(Some),
        }
    }
}

/// Borrow a bound value without converting it through a native SQLite type.
pub trait ToParam {
    fn as_param(&self) -> Param<'_>;
}
impl ToParam for Param<'_> {
    fn as_param(&self) -> Param<'_> {
        *self
    }
}
impl<T: ToParam + ?Sized> ToParam for &T {
    fn as_param(&self) -> Param<'_> {
        T::as_param(*self)
    }
}
impl<T: ToParam> ToParam for Option<T> {
    fn as_param(&self) -> Param<'_> {
        self.as_ref().map_or(Param::Null, ToParam::as_param)
    }
}
macro_rules! integer_params {
    ($($ty:ty),*) => { $(impl ToParam for $ty {
        fn as_param(&self) -> Param<'_> { Param::Integer(*self as i64) }
    })* };
}
integer_params!(i8, i16, i32, i64, u8, u16, u32, bool);
impl ToParam for f64 {
    fn as_param(&self) -> Param<'_> {
        Param::Real(*self)
    }
}
impl ToParam for str {
    fn as_param(&self) -> Param<'_> {
        Param::Text(self)
    }
}
impl ToParam for String {
    fn as_param(&self) -> Param<'_> {
        Param::Text(self)
    }
}
impl ToParam for [u8] {
    fn as_param(&self) -> Param<'_> {
        Param::Blob(self)
    }
}
impl<const N: usize> ToParam for [u8; N] {
    fn as_param(&self) -> Param<'_> {
        Param::Blob(self)
    }
}
impl ToParam for Vec<u8> {
    fn as_param(&self) -> Param<'_> {
        Param::Blob(self)
    }
}

/// A borrowed, stack-allocated parameter list. Temporaries live through the
/// surrounding awaited call, just as they do for a synchronous native call.
#[macro_export]
macro_rules! params {
    ($($value:expr),* $(,)?) => { &[$($crate::ToParam::as_param(&$value)),*] };
}

/// A row lives only for the query callback. Native providers need not copy it.
pub trait Row {
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn cell(&self, index: usize) -> Option<Cell<'_>>;
    /// Column metadata after the statement has stepped (including reprepare).
    fn column_name(&self, _index: usize) -> Option<&str> {
        None
    }

    fn get<T: FromCell>(&self, index: usize) -> Result<T, CellError> {
        T::from_cell(
            self.cell(index)
                .ok_or(CellError("column index out of bounds"))?,
        )
    }
}

/// Counters available from a provider's query engine. Providers without native
/// statement counters leave these absent rather than inventing measurements.
#[derive(Clone, Copy, Debug, Default)]
pub struct QueryStats {
    pub fullscan_steps: Option<u64>,
    pub sorted: Option<bool>,
}

/// Connection facts required by Jazz in addition to statement execution.
/// These describe the last completed command, even when that command failed.
/// The owner must serialize access; no other user may change this connection
/// between observing state and issuing its next statement.
pub trait Connection: Sql {
    /// True for BEGIN or an outer SAVEPOINT until commit/rollback, including
    /// SQLite's automatic transaction rollback after an error.
    fn transaction_open(&self) -> bool;

    /// Maximum bound parameters accepted by this connection. Providers may
    /// report a verified lower bound; they must not exceed the engine's limit.
    fn parameter_limit(&self) -> Result<usize, Self::Error>;

    /// SQLite's cumulative row-change counter (including triggers and changes
    /// later rolled back). Native providers can override this with an inline
    /// counter read; worker providers use the SQL function on their worker.
    fn total_changes(&mut self) -> impl Future<Output = Result<u64, Self::Error>> {
        self.query_one("SELECT total_changes()", &[], |row| row.get(0))
    }
}

/// Statically dispatched SQL execution. Queries lend rows to the callback;
/// providers may suspend before delivering a row, but callbacks never suspend.
/// A callback can stop iteration without collecting the remaining rows.
/// Providers that materialize results require SQL bounds on range queries.
pub trait Sql {
    type Error: From<CellError>;
    type Row<'a>: Row;

    fn invalid_data(message: &'static str) -> Self::Error;

    /// Execute a SQL script, respecting quoted semicolons and trigger bodies.
    /// This does not start a transaction; callers own its atomicity.
    fn execute_script(&mut self, script: &str) -> impl Future<Output = Result<(), Self::Error>>;

    fn execute(
        &mut self,
        sql: &str,
        params: &[Param<'_>],
    ) -> impl Future<Output = Result<usize, Self::Error>>;

    fn query<F>(
        &mut self,
        sql: &str,
        params: &[Param<'_>],
        visit: F,
    ) -> impl Future<Output = Result<(), Self::Error>>
    where
        F: FnMut(Self::Row<'_>) -> Result<ControlFlow<()>, Self::Error>;

    /// Decode the first row of a query required to return a result.
    fn query_one<T, F>(
        &mut self,
        sql: &str,
        params: &[Param<'_>],
        map: F,
    ) -> impl Future<Output = Result<T, Self::Error>>
    where
        F: FnMut(Self::Row<'_>) -> Result<T, CellError>,
    {
        async move {
            self.query_optional(sql, params, map)
                .await?
                .ok_or_else(|| Self::invalid_data("query returned no rows"))
        }
    }

    /// Decode at most the first result row. Callers must bound the SQL when
    /// the provider materializes rows before invoking callbacks.
    fn query_optional<T, F>(
        &mut self,
        sql: &str,
        params: &[Param<'_>],
        mut map: F,
    ) -> impl Future<Output = Result<Option<T>, Self::Error>>
    where
        F: FnMut(Self::Row<'_>) -> Result<T, CellError>,
    {
        async move {
            let mut out = None;
            self.query(sql, params, |row| {
                out = Some(map(row)?);
                Ok(ControlFlow::Break(()))
            })
            .await?;
            Ok(out)
        }
    }

    fn query_with_stats<F>(
        &mut self,
        sql: &str,
        params: &[Param<'_>],
        visit: F,
    ) -> impl Future<Output = Result<QueryStats, Self::Error>>
    where
        F: FnMut(Self::Row<'_>) -> Result<ControlFlow<()>, Self::Error>,
    {
        async move {
            self.query(sql, params, visit).await?;
            Ok(QueryStats::default())
        }
    }

    /// Collect a query's typed projections. Use `query` when rows can be
    /// processed directly; this helper deliberately owns the projected rows.
    fn query_rows<T, F>(
        &mut self,
        sql: &str,
        params: &[Param<'_>],
        mut map: F,
    ) -> impl Future<Output = Result<Vec<T>, Self::Error>>
    where
        F: FnMut(Self::Row<'_>) -> Result<T, CellError>,
    {
        async move {
            let mut out = Vec::new();
            self.query(sql, params, |row| {
                out.push(map(row)?);
                Ok(ControlFlow::Continue(()))
            })
            .await?;
            Ok(out)
        }
    }
}

/// Validated host table name. The same identifier rules as Jazz's HostTable.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct HostTable(&'static str);

impl HostTable {
    pub const fn new(name: &'static str) -> Self {
        let bytes = name.as_bytes();
        assert!(!bytes.is_empty(), "host table name is empty");
        let mut i = 0;
        while i < bytes.len() {
            let b = bytes[i];
            assert!(
                b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_',
                "host table names are lowercase ASCII letters, digits and _"
            );
            i += 1;
        }
        Self(name)
    }

    pub fn sql_name(self) -> String {
        format!("_jazz_host_{}", self.0)
    }
}

pub async fn get<S: Sql>(
    sql: &mut S,
    table: HostTable,
    key: &[u8],
) -> Result<Option<Vec<u8>>, S::Error> {
    let name = table.sql_name();
    if sql
        .query_optional(
            "SELECT X'01' FROM sqlite_master WHERE type = 'table' AND name = ?1 LIMIT 1",
            &[Param::Text(&name)],
            |row| row.get::<Vec<u8>>(0),
        )
        .await?
        .is_none()
    {
        return Ok(None);
    }
    sql.query_optional(
        &format!("SELECT v FROM {name} WHERE k = ?1"),
        &[Param::Blob(key)],
        |row| row.get::<Vec<u8>>(0),
    )
    .await
}

/// Caller MUST own a transaction for the entire future. Roll back on error or
/// cancellation; do not acknowledge the insertion until commit succeeds.
/// Returns the prior value, or None when this transaction inserted the value.
pub async fn put_if_absent_in_transaction<S: Sql>(
    sql: &mut S,
    table: HostTable,
    key: &[u8],
    value: &[u8],
) -> Result<Option<Vec<u8>>, S::Error> {
    let name = table.sql_name();
    sql.execute(
        &format!(
            "CREATE TABLE IF NOT EXISTS {name} (k BLOB PRIMARY KEY, v BLOB NOT NULL) WITHOUT ROWID"
        ),
        &[],
    )
    .await?;
    let existing = sql
        .query_optional(
            &format!("SELECT v FROM {name} WHERE k = ?1"),
            &[Param::Blob(key)],
            |row| row.get::<Vec<u8>>(0),
        )
        .await?;
    if existing.is_none() {
        sql.execute(
            &format!("INSERT INTO {name} (k, v) VALUES (?1, ?2)"),
            &[Param::Blob(key), Param::Blob(value)],
        )
        .await?;
    }
    Ok(existing)
}

/// Drive a known-synchronous provider exactly once on the calling thread.
/// This is deliberately not a blocking executor: Pending is an implementation
/// error. It must never be used with an asynchronous provider.
pub fn inline<T>(future: impl Future<Output = T>) -> T {
    let mut future = std::pin::pin!(future);
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    match future.as_mut().poll(&mut cx) {
        std::task::Poll::Ready(value) => value,
        std::task::Poll::Pending => panic!("synchronous SQL provider unexpectedly yielded"),
    }
}

/// All keys with `prefix`, in byte order. The upper bound is in SQL so a
/// provider that materializes query results never reads unrelated keys.
pub async fn prefix<S: Sql>(
    sql: &mut S,
    table: HostTable,
    prefix: &[u8],
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, S::Error> {
    if !exists(sql, table).await? {
        return Ok(Vec::new());
    }
    let mut upper = prefix.to_vec();
    while upper.last() == Some(&255) {
        upper.pop();
    }
    if let Some(last) = upper.last_mut() {
        *last += 1;
    }
    let name = table.sql_name();
    let statement = if upper.is_empty() {
        format!("SELECT k, v FROM {name} WHERE k >= ?1 ORDER BY k")
    } else {
        format!("SELECT k, v FROM {name} WHERE k >= ?1 AND k < ?2 ORDER BY k")
    };
    let mut params = vec![Param::Blob(prefix)];
    if !upper.is_empty() {
        params.push(Param::Blob(&upper));
    }
    let mut out = Vec::new();
    sql.query(&statement, &params, |row| {
        match (row.cell(0), row.cell(1)) {
            (Some(Cell::Blob(key)), Some(Cell::Blob(value))) if row.len() == 2 => {
                out.push((key.to_vec(), value.to_vec()));
                Ok(ControlFlow::Continue(()))
            }
            _ => Err(S::invalid_data("host row must contain two BLOBs")),
        }
    })
    .await?;
    Ok(out)
}

pub async fn exists<S: Sql>(sql: &mut S, table: HostTable) -> Result<bool, S::Error> {
    Ok(sql
        .query_optional(
            "SELECT X'01' FROM sqlite_master WHERE type = 'table' AND name = ?1 LIMIT 1",
            &[Param::Text(&table.sql_name())],
            |row| row.get::<Vec<u8>>(0),
        )
        .await?
        .is_some())
}

/// Caller owns the transaction through commit or rollback, including Drop.
pub async fn put_many_in_transaction<S: Sql>(
    sql: &mut S,
    table: HostTable,
    pairs: &[(Vec<u8>, Vec<u8>)],
) -> Result<(), S::Error> {
    if pairs.is_empty() {
        return Ok(());
    }
    let name = table.sql_name();
    sql.execute(
        &format!(
            "CREATE TABLE IF NOT EXISTS {name} (k BLOB PRIMARY KEY, v BLOB NOT NULL) WITHOUT ROWID"
        ),
        &[],
    )
    .await?;
    let insert = format!("INSERT OR REPLACE INTO {name} (k, v) VALUES (?1, ?2)");
    for (key, value) in pairs {
        sql.execute(&insert, &[Param::Blob(key), Param::Blob(value)])
            .await?;
    }
    Ok(())
}

pub async fn delete<S: Sql>(sql: &mut S, table: HostTable, key: &[u8]) -> Result<(), S::Error> {
    if exists(sql, table).await? {
        sql.execute(
            &format!("DELETE FROM {} WHERE k = ?1", table.sql_name()),
            &[Param::Blob(key)],
        )
        .await?;
    }
    Ok(())
}
