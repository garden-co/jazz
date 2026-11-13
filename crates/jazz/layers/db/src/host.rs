//! Tables a host keeps in its store's file beside Jazz's own, so one app's
//! state is one SQLite file: a server's schema catalogue and account
//! journal live next to the rows they describe.
//!
//! Each host table is an ordered key-value table named `_jazz_host_<name>`.
//! Application schemas never see them (every `_jazz` table is Jazz's), and
//! they are created on first use, so a file no host wrote has none. Writes
//! commit on the store's own connection, with its `synchronous` setting: a
//! store opened with [`FileOptions::synchronous_full`] makes every returned
//! host write durable.
//!
//! [`StoreFile`] reaches them before a [`Db`] is opened on the file (a
//! server reads its catalogue to know which schema to open with), and
//! [`Db`] reaches them afterwards. Both go through the one connection, so
//! host writes and row writes never contend for the file. A [`HostReader`]
//! reads committed host tables from another thread without waiting for
//! that connection.
//!
//! [`FileOptions::synchronous_full`]: crate::db::FileOptions::synchronous_full
//! [`Db`]: crate::db::Db

#[cfg(feature = "sqlite")]
use std::path::Path;
use std::path::PathBuf;
#[cfg(feature = "sqlite")]
use std::sync::Mutex;
#[cfg(feature = "sqlite")]
use std::time::Duration;

#[cfg(feature = "sqlite")]
use rusqlite::Connection;
#[cfg(any(test, feature = "testing"))]
#[cfg(feature = "sqlite")]
use rusqlite::params;

use crate::catalogue::{CataloguePut, CatalogueRows};
use crate::db::Db;
#[cfg(feature = "sqlite")]
use crate::db::{FileOptions, Journal};
use crate::options::Error;
#[cfg(feature = "sqlite")]
use crate::options::ErrorCode;
#[cfg(feature = "sqlite")]
use crate::values::in_savepoint;
#[cfg(feature = "sqlite")]
use crate::values::storage;

#[cfg(not(any(test, feature = "testing")))]
#[cfg(feature = "sqlite")]
type NativeConnectionSql<'a> = jazz_sql::native::Native<'a>;
#[cfg(any(test, feature = "testing"))]
#[cfg(feature = "sqlite")]
type NativeConnectionSql<'a> = crate::sql_test::ObservedSql<'a>;

#[cfg(feature = "sqlite")]
pub(crate) struct OwnedConnection {
    conn: Connection,
    #[cfg(any(test, feature = "testing"))]
    pub(crate) sql_suspensions: Option<std::sync::Arc<std::sync::atomic::AtomicUsize>>,
}

#[cfg(feature = "sqlite")]
impl From<Connection> for OwnedConnection {
    fn from(conn: Connection) -> Self {
        Self {
            conn,
            #[cfg(any(test, feature = "testing"))]
            sql_suspensions: None,
        }
    }
}

#[cfg(feature = "sqlite")]
impl std::ops::Deref for OwnedConnection {
    type Target = Connection;
    fn deref(&self) -> &Connection {
        &self.conn
    }
}

#[cfg(feature = "sqlite")]
impl std::ops::DerefMut for OwnedConnection {
    fn deref_mut(&mut self) -> &mut Connection {
        &mut self.conn
    }
}

#[cfg(feature = "sqlite")]
impl OwnedConnection {
    pub(crate) fn sql(&self) -> NativeConnectionSql<'_> {
        #[cfg(not(any(test, feature = "testing")))]
        {
            jazz_sql::native::Native(&self.conn)
        }
        #[cfg(any(test, feature = "testing"))]
        {
            crate::sql_test::ObservedSql {
                native: jazz_sql::native::Native(&self.conn),
                suspensions: self.sql_suspensions.as_deref(),
            }
        }
    }
}

pub use crate::backend::ConnectionSql;
pub(crate) use crate::backend::DbConnection;

#[cfg(feature = "sqlite")]
impl crate::values::TransactionConnection for OwnedConnection {
    fn transaction_open(&self) -> bool {
        use jazz_sql::Connection as _;
        self.sql().transaction_open()
    }
    async fn transaction_sql(&self, sql: &str) -> Result<(), Error> {
        use jazz_sql::Sql as _;
        self.sql().execute_script(sql).await.map_err(storage)
    }
    fn abandon_savepoint(&self, name: &str) {
        let _ = crate::values::end_savepoint_inline(self, name, false);
    }
}
#[cfg(feature = "sqlite")]
impl crate::values::NativeTransactionConnection for OwnedConnection {
    fn sqlite(&self) -> &Connection {
        self
    }
}

/// One host table: `_jazz_host_<name>`.
pub use jazz_sql::HostTable;

/// An ordered key-value pair.
pub type HostPair = (Vec<u8>, Vec<u8>);

/// Reads and writes of host tables, on whatever holds the store's
/// connection.
#[allow(async_fn_in_trait)]
pub trait HostTables {
    /// Execute one complete host operation, optionally in a write savepoint.
    /// The callback runs only on the connection's owner lane.
    async fn with_sql<T>(
        &self,
        write: bool,
        f: impl FnOnce(crate::backend::ConnectionSql<'_>) -> Result<T, Error>,
    ) -> Result<T, Error>;

    /// The value at `key`.
    async fn host_get(&self, table: HostTable, key: &[u8]) -> Result<Option<Vec<u8>>, Error>;
    /// Every pair whose key starts with `prefix`, in key order.
    async fn host_prefix(&self, table: HostTable, prefix: &[u8]) -> Result<Vec<HostPair>, Error>;
    /// Write every pair (replacing what is there) in one commit: all or none.
    async fn host_put_many(&self, table: HostTable, pairs: &[HostPair]) -> Result<(), Error>;
    /// Write `value` unless `key` exists; returns the existing value if so.
    async fn host_put_if_absent(
        &self,
        table: HostTable,
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<Vec<u8>>, Error>;
    /// Remove `key`.
    async fn host_delete(&self, table: HostTable, key: &[u8]) -> Result<(), Error>;
    /// The store's catalogue (see [`crate::catalogue`]).
    async fn catalogue(&self) -> Result<CatalogueRows, Error>;
    /// Publish `put` into the store's catalogue in one commit: all or none.
    async fn catalogue_put(&self, put: &CataloguePut) -> Result<(), Error>;
    /// Run `f` on the store's connection inside one `host_write` savepoint:
    /// what it wrote commits when it returns `Ok`, nothing does otherwise.
    /// For host tables with columns of their own (`_jazz_host_<name>`,
    /// created by `f` itself), which plain key-value pairs would hide from
    /// SQL.
    #[cfg(feature = "sqlite")]
    async fn host_sql(
        &self,
        f: &mut dyn FnMut(&Connection) -> Result<(), Error>,
    ) -> Result<(), Error>;
}

/// Turn on SQLite's query planner stability guarantee
/// (`SQLITE_DBCONFIG_ENABLE_QPSG`) for every statement `conn` prepares from
/// now on.
///
/// Without it, SQLite folds the value bound to `LIMIT ?` into the plan and
/// marks the statement as depending on it, and *any* rebind of that
/// parameter, even to the same value, expires the statement. Every compiled
/// read ends in `LIMIT ? OFFSET ?` and rusqlite rebinds every parameter on
/// each run, so each cached read was parsed and planned again on every run
/// (thousands of reprepares per write on a busy client). The engine never
/// runs `ANALYZE`, uses no `LIKE` on bound patterns and no partial indexes,
/// so the guarantee costs no plan it would otherwise pick.
#[cfg(feature = "sqlite")]
pub(crate) fn stable_plans(conn: &Connection) -> Result<(), Error> {
    conn.set_db_config(
        rusqlite::config::DbConfig::SQLITE_DBCONFIG_ENABLE_QPSG,
        true,
    )
    .map_err(storage)?;
    Ok(())
}

/// A store's file opened and admitted, before a [`Db`] is opened on it:
/// [`StoreFile::open`] then [`Db::open_file`] is [`Db::open_at`].
pub struct StoreFile {
    pub(crate) conn: DbConnection,
    pub(crate) path: Option<(PathBuf, Option<&'static str>)>,
}

impl StoreFile {
    async fn with_connection<T>(
        &self,
        f: impl AsyncFnOnce(&DbConnection) -> Result<T, Error>,
    ) -> Result<T, Error> {
        f(&self.conn).await
    }

    /// Admit an exclusively owned worker before schema/catalogue initialization.
    pub async fn worker(worker: impl crate::backend::WorkerSql + 'static) -> Result<Self, Error> {
        let conn = DbConnection::worker(worker);
        crate::format::admit(&mut conn.sql()).await?;
        Ok(Self { conn, path: None })
    }
    /// Wait for the worker to drain and close before releasing its actor generation.
    pub async fn close_worker(&self) -> Result<(), Error> {
        self.conn.close_worker().await
    }

    /// Open (creating) the file at `path` with `options`, and admit its
    /// storage format (see [`crate::format`]); a refused file is left as it
    /// was.
    #[cfg(feature = "sqlite")]
    pub fn open(path: &Path, options: FileOptions) -> Result<Self, Error> {
        // SQLite cannot open a directory; one the Groove engine wrote gets
        // the typed refusal.
        if path.is_dir() {
            crate::format::refuse_groove_store(path)?;
        }
        let flags = rusqlite::OpenFlags::default();
        let conn = match options.vfs {
            None => Connection::open_with_flags(path, flags),
            Some(vfs) => Connection::open_with_flags_and_vfs(path, flags, vfs),
        }
        .map_err(storage)?;
        let synchronous = match (options.journal, options.synchronous_full) {
            (Journal::Memory, _) => "OFF",
            (_, true) => "FULL",
            (_, false) => "NORMAL",
        };
        let journal = match options.journal {
            Journal::Wal => "WAL",
            Journal::Rollback => "TRUNCATE",
            Journal::Memory => "MEMORY",
        };
        let cache_spill = if options.journal == Journal::Wal {
            ""
        } else {
            " PRAGMA cache_spill = OFF;"
        };
        // Before anything writes: a new file takes it on creation.
        if let Some(page_size) = options.page_size {
            conn.execute_batch(&format!("PRAGMA page_size = {page_size}"))
                .map_err(storage)?;
        }
        // Before the journal mode, which rewrites the header: a refused file
        // is left as it was.
        jazz_sql::inline(crate::format::admit(&mut NativeSql(&conn)))?;
        // `journal_mode` answers with a row, so it is queried, not batched.
        let mode: String = conn
            .query_row(&format!("PRAGMA journal_mode = {journal}"), [], |r| {
                r.get(0)
            })
            .map_err(storage)?;
        if !mode.eq_ignore_ascii_case(journal) {
            return Err(Error::new(
                ErrorCode::Storage,
                format!("journal_mode {journal} is not supported on this VFS yet (got {mode})"),
            ));
        }
        conn.execute_batch(&format!(
            "PRAGMA synchronous = {synchronous}; PRAGMA cache_size = -{};{cache_spill}",
            options.cache_kib
        ))
        .map_err(storage)?;
        Ok(Self {
            conn: conn.into(),
            path: Some((path.to_path_buf(), options.vfs)),
        })
    }

    /// A store that lives only as long as what is opened on it.
    #[cfg(feature = "sqlite")]
    pub fn memory() -> Result<Self, Error> {
        let conn = Connection::open_in_memory().map_err(storage)?;
        jazz_sql::inline(crate::format::admit(&mut NativeSql(&conn)))?;
        Ok(Self {
            conn: conn.into(),
            path: None,
        })
    }

    /// An in-memory store this process shares by `name`: it lives while any
    /// connection to it is open, so a host keeping one can reopen the store
    /// (or read it with a [`HostReader`]) without losing it.
    #[cfg(feature = "sqlite")]
    pub fn shared_memory(name: &str) -> Result<Self, Error> {
        let conn =
            Connection::open_with_flags(shared_memory_uri(name), uri_flags()).map_err(storage)?;
        jazz_sql::inline(crate::format::admit(&mut NativeSql(&conn)))?;
        Ok(Self {
            conn: conn.into(),
            path: None,
        })
    }
}

/// Reads committed host tables on a connection of its own, so a reader on
/// another thread never waits for work on the store's connection (a WAL
/// file serves readers beside its writer). It sees every write that
/// returned before the read began.
#[cfg(feature = "sqlite")]
pub struct HostReader {
    conn: Mutex<Option<Connection>>,
}

#[cfg(feature = "sqlite")]
impl HostReader {
    /// Read through the portable SQL interface on the native read-only connection.
    pub fn with_sql<T>(
        &self,
        f: impl FnOnce(crate::backend::ConnectionSql<'_>) -> Result<T, Error>,
    ) -> Result<T, Error> {
        self.read(|conn| f(crate::backend::ConnectionSql::reader(conn)))
    }

    /// Read the file at `path`, which a [`StoreFile`] opened already.
    #[cfg(feature = "sqlite")]
    pub fn open(path: &Path, vfs: Option<&'static str>) -> Result<Self, Error> {
        let flags = rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
            | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX
            | rusqlite::OpenFlags::SQLITE_OPEN_URI;
        let conn = match vfs {
            None => Connection::open_with_flags(path, flags),
            Some(vfs) => Connection::open_with_flags_and_vfs(path, flags, vfs),
        }
        .map_err(storage)?;
        Self::with(conn)
    }

    /// Read the store [`StoreFile::shared_memory`] opened by `name`.
    #[cfg(feature = "sqlite")]
    pub fn shared_memory(name: &str) -> Result<Self, Error> {
        Self::with(
            Connection::open_with_flags(shared_memory_uri(name), uri_flags()).map_err(storage)?,
        )
    }

    fn with(conn: Connection) -> Result<Self, Error> {
        // A WAL reader waits only while another connection recovers the
        // log.
        conn.busy_timeout(Duration::from_secs(5)).map_err(storage)?;
        Ok(Self {
            conn: Mutex::new(Some(conn)),
        })
    }

    fn read<T>(&self, f: impl FnOnce(&Connection) -> Result<T, Error>) -> Result<T, Error> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| Error::new(ErrorCode::Storage, "host reader lock poisoned"))?;
        match &*conn {
            Some(conn) => f(conn),
            None => Err(Error::new(ErrorCode::Storage, "host reader is closed")),
        }
    }

    /// The value at `key`.
    pub fn get(&self, table: HostTable, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        self.read(|conn| jazz_sql::inline(get(conn, table, key)))
    }

    /// Every pair whose key starts with `prefix`, in key order.
    pub fn prefix(&self, table: HostTable, prefix: &[u8]) -> Result<Vec<HostPair>, Error> {
        self.read(|conn| jazz_sql::inline(prefix_scan(conn, table, prefix)))
    }

    /// The store's catalogue as committed (see [`crate::catalogue`]).
    pub fn catalogue(&self) -> Result<CatalogueRows, Error> {
        self.read(|conn| jazz_sql::inline(crate::catalogue::read(&mut NativeSql(conn))))
    }

    /// Run `f` on the read-only connection, for host tables that
    /// [`HostTables::host_sql`] shaped. A table no write created yet is
    /// missing here: `f` must not assume it exists.
    pub fn sql<T>(&self, f: impl FnOnce(&Connection) -> Result<T, Error>) -> Result<T, Error> {
        self.read(f)
    }

    /// Close the connection; reads fail afterwards. Close readers before
    /// the store's own connection, which then checkpoints the WAL as the
    /// file's last connection.
    pub fn close(&self) {
        if let Ok(mut conn) = self.conn.lock() {
            conn.take();
        }
    }
}

/// SQLite's `memdb` VFS shares a database whose name starts with `/`
/// between the process's connections.
#[cfg(feature = "sqlite")]
fn shared_memory_uri(name: &str) -> String {
    format!("file:/{name}?vfs=memdb")
}

#[cfg(feature = "sqlite")]
fn uri_flags() -> rusqlite::OpenFlags {
    rusqlite::OpenFlags::default() | rusqlite::OpenFlags::SQLITE_OPEN_URI
}

impl HostTables for StoreFile {
    async fn with_sql<T>(
        &self,
        write: bool,
        f: impl FnOnce(crate::backend::ConnectionSql<'_>) -> Result<T, Error>,
    ) -> Result<T, Error> {
        self.with_connection(async |conn| {
            if write {
                crate::values::in_savepoint_async(conn, "host_write", async || f(conn.sql())).await
            } else {
                f(conn.sql())
            }
        })
        .await
    }

    async fn host_get(&self, table: HostTable, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        self.with_connection(async |conn| jazz_sql::get(&mut conn.sql(), table, key).await)
            .await
    }

    async fn host_prefix(&self, table: HostTable, prefix: &[u8]) -> Result<Vec<HostPair>, Error> {
        self.with_connection(async |conn| jazz_sql::prefix(&mut conn.sql(), table, prefix).await)
            .await
    }

    async fn host_put_many(&self, table: HostTable, pairs: &[HostPair]) -> Result<(), Error> {
        self.with_connection(async |conn| {
            crate::values::in_savepoint_async(conn, "host_write", async || {
                jazz_sql::put_many_in_transaction(&mut conn.sql(), table, pairs).await
            })
            .await
        })
        .await
    }

    async fn host_put_if_absent(
        &self,
        table: HostTable,
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<Vec<u8>>, Error> {
        self.with_connection(async |conn| {
            crate::values::in_savepoint_async(conn, "host_write", async || {
                jazz_sql::put_if_absent_in_transaction(&mut conn.sql(), table, key, value).await
            })
            .await
        })
        .await
    }

    async fn host_delete(&self, table: HostTable, key: &[u8]) -> Result<(), Error> {
        self.with_connection(async |conn| jazz_sql::delete(&mut conn.sql(), table, key).await)
            .await
    }

    async fn catalogue(&self) -> Result<CatalogueRows, Error> {
        self.with_connection(async |conn| crate::catalogue::read(&mut conn.sql()).await)
            .await
    }

    async fn catalogue_put(&self, put: &CataloguePut) -> Result<(), Error> {
        self.with_connection(async |conn| {
            crate::values::in_savepoint_async(conn, "host_write", async || {
                crate::catalogue::put(&mut conn.sql(), put).await
            })
            .await
        })
        .await
    }

    #[cfg(feature = "sqlite")]
    async fn host_sql(
        &self,
        f: &mut dyn FnMut(&Connection) -> Result<(), Error>,
    ) -> Result<(), Error> {
        self.with_connection(async |conn| {
            let native = conn.native()?;
            in_savepoint(conn, "host_write", || f(native))
        })
        .await
    }
}

impl HostTables for Db {
    async fn with_sql<T>(
        &self,
        write: bool,
        f: impl FnOnce(crate::backend::ConnectionSql<'_>) -> Result<T, Error>,
    ) -> Result<T, Error> {
        self.with_host_connection(async |conn| {
            if write {
                crate::values::in_savepoint_async(conn, "host_write", async || f(conn.sql())).await
            } else {
                f(conn.sql())
            }
        })
        .await
    }

    async fn host_get(&self, table: HostTable, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        self.with_host_connection(async |conn| jazz_sql::get(&mut conn.sql(), table, key).await)
            .await
    }

    async fn host_prefix(&self, table: HostTable, prefix: &[u8]) -> Result<Vec<HostPair>, Error> {
        self.with_host_connection(async |conn| {
            jazz_sql::prefix(&mut conn.sql(), table, prefix).await
        })
        .await
    }

    async fn host_put_many(&self, table: HostTable, pairs: &[HostPair]) -> Result<(), Error> {
        self.with_host_connection(async |conn| {
            crate::values::in_savepoint_async(conn, "host_write", async || {
                jazz_sql::put_many_in_transaction(&mut conn.sql(), table, pairs).await
            })
            .await
        })
        .await
    }

    async fn host_put_if_absent(
        &self,
        table: HostTable,
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<Vec<u8>>, Error> {
        self.with_host_connection(async |conn| {
            crate::values::in_savepoint_async(conn, "host_write", async || {
                jazz_sql::put_if_absent_in_transaction(&mut conn.sql(), table, key, value).await
            })
            .await
        })
        .await
    }

    async fn host_delete(&self, table: HostTable, key: &[u8]) -> Result<(), Error> {
        self.with_host_connection(async |conn| jazz_sql::delete(&mut conn.sql(), table, key).await)
            .await
    }

    async fn catalogue(&self) -> Result<CatalogueRows, Error> {
        self.with_host_connection(async |conn| crate::catalogue::read(&mut conn.sql()).await)
            .await
    }

    async fn catalogue_put(&self, put: &CataloguePut) -> Result<(), Error> {
        self.with_host_connection(async |conn| {
            crate::values::in_savepoint_async(conn, "host_write", async || {
                crate::catalogue::put(&mut conn.sql(), put).await
            })
            .await
        })
        .await
    }

    #[cfg(feature = "sqlite")]
    async fn host_sql(
        &self,
        f: &mut dyn FnMut(&Connection) -> Result<(), Error>,
    ) -> Result<(), Error> {
        self.with_host_connection(async |conn| {
            let native = conn.native()?;
            in_savepoint(conn, "host_write", || f(native))
        })
        .await
    }
}

#[cfg(any(test, feature = "testing"))]
#[cfg(feature = "sqlite")]
impl Db {
    /// Tests: suspend a host write while it owns its SQL savepoint and store
    /// lease. Dropping this future must undo the write before another operation
    /// can acquire that lease.
    pub async fn host_put_then_wait_for_test(
        &self,
        table: HostTable,
        key: &[u8],
        value: &[u8],
        wait: impl std::future::Future<Output = ()>,
    ) -> Result<(), Error> {
        self.with_host_connection(async |conn| {
            crate::values::in_savepoint_async(conn, "host_write", async || {
                jazz_sql::put_if_absent_in_transaction(&mut NativeSql(conn), table, key, value)
                    .await
                    .map_err(storage)?;
                wait.await;
                Ok(())
            })
            .await
        })
        .await
    }

    /// Tests: a host write that fails half done. Writes `pairs` inside the
    /// write's savepoint, then panics before releasing it.
    pub fn host_put_many_then_panic_for_test(
        &self,
        table: HostTable,
        pairs: &[HostPair],
    ) -> Result<(), Error> {
        jazz_sql::inline(self.with_host_connection(async |conn| {
            crate::values::begin_savepoint(conn, "host_write").await?;
            create(conn, table)?;
            for (key, value) in pairs {
                conn.execute(
                    &format!(
                        "INSERT OR REPLACE INTO {} (k, v) VALUES (?1, ?2)",
                        table.sql_name()
                    ),
                    params![key, value],
                )
                .map_err(storage)?;
            }
            panic!("injected failure mid host write")
        }))
    }
}

#[cfg(any(test, feature = "testing"))]
#[cfg(feature = "sqlite")]
fn create(conn: &Connection, table: HostTable) -> Result<(), Error> {
    conn.execute_batch(&format!(
        "CREATE TABLE IF NOT EXISTS {} (k BLOB PRIMARY KEY, v BLOB NOT NULL) WITHOUT ROWID",
        table.sql_name()
    ))
    .map_err(storage)
}

#[cfg(feature = "sqlite")]
async fn get(conn: &Connection, table: HostTable, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
    jazz_sql::get(&mut NativeSql(conn), table, key)
        .await
        .map_err(storage)
}

#[cfg(feature = "sqlite")]
use jazz_sql::native::Native as NativeSql;

#[cfg(feature = "sqlite")]
async fn prefix_scan(
    conn: &Connection,
    table: HostTable,
    prefix: &[u8],
) -> Result<Vec<HostPair>, Error> {
    jazz_sql::prefix(&mut NativeSql(conn), table, prefix)
        .await
        .map_err(storage)
}

#[cfg(test)]
mod tests {
    use rusqlite::{Connection, StatementStatus};

    /// Reprepares of a cached `LIMIT ?` read run `runs` times on `conn`.
    fn reprepares(conn: &Connection, runs: i64) -> i32 {
        conn.execute_batch("CREATE TABLE t (x INTEGER); INSERT INTO t VALUES (1), (2), (3);")
            .unwrap();
        let sql = "SELECT x FROM t ORDER BY x LIMIT ?1 OFFSET ?2";
        for _ in 0..runs {
            let mut stmt = conn.prepare_cached(sql).unwrap();
            let rows: Vec<i64> = stmt
                .query_map([2i64, 0], |r| r.get(0))
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();
            assert_eq!(rows, [1, 2]);
        }
        conn.prepare_cached(sql)
            .unwrap()
            .get_status(StatementStatus::RePrepare)
    }

    #[test]
    fn a_cached_limit_read_is_not_planned_again_on_each_run() {
        // The cost this guards against: rebinding `LIMIT ?` expires it.
        let plain = Connection::open_in_memory().unwrap();
        assert!(
            reprepares(&plain, 5) >= 4,
            "SQLite no longer expires on LIMIT rebinds"
        );

        let conn = Connection::open_in_memory().unwrap();
        super::stable_plans(&conn).unwrap();
        assert_eq!(reprepares(&conn, 5), 0);
    }
}
