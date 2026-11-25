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

use crate::storage::{OptionalExtension, params};

use crate::catalogue::{CataloguePut, CatalogueRows};
use crate::db::Db;
#[cfg(feature = "sqlite")]
use crate::db::{FileOptions, Journal};
use crate::options::Error;
#[cfg(feature = "sqlite")]
use crate::options::ErrorCode;
use crate::storage::Storage;
use crate::values::storage;

/// One host table: `_jazz_host_<name>`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct HostTable(&'static str);

impl HostTable {
    /// `name` is lowercase ASCII letters, digits and `_`.
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

    /// The SQLite table's name.
    pub fn sql_name(self) -> String {
        format!("_jazz_host_{}", self.0)
    }
}

/// An ordered key-value pair.
pub type HostPair = (Vec<u8>, Vec<u8>);

/// A host transaction body can borrow both its captured state and the scoped
/// connection while its SQL runs. Host jobs dispatch through `dyn HostTables`.
#[async_trait::async_trait(?Send)]
pub trait HostSqlBody {
    async fn run(&mut self, conn: &Storage) -> Result<(), Error>;
}

#[async_trait::async_trait(?Send)]
impl<F: AsyncFnMut(&Storage) -> Result<(), Error>> HostSqlBody for F {
    async fn run(&mut self, conn: &Storage) -> Result<(), Error> {
        self(conn).await
    }
}

/// Reads and writes of host tables, on whatever holds the store's
/// connection.
#[async_trait::async_trait(?Send)]
pub trait HostTables {
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
    async fn host_sql(&self, f: &mut dyn HostSqlBody) -> Result<(), Error>;
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
pub(crate) fn stable_plans(_conn: &Storage) -> Result<(), Error> {
    // Managed SQL plans as its host does.
    #[cfg(feature = "sqlite")]
    {
        let Some(conn) = _conn.native_conn() else {
            return Ok(());
        };
        conn.set_db_config(
            rusqlite::config::DbConfig::SQLITE_DBCONFIG_ENABLE_QPSG,
            true,
        )
        .map_err(storage)?;
    }
    Ok(())
}

/// A store's file opened and format-checked, before a [`Db`] is opened on it:
/// [`StoreFile::open`] then [`Db::open_file`] is [`Db::open_at`].
pub struct StoreFile {
    operation: futures::lock::Mutex<()>,
    pub(crate) conn: Storage,
    pub(crate) path: Option<(PathBuf, Option<&'static str>)>,
}

impl StoreFile {
    /// Open (creating) the file at `path` with `options`, and admit its
    /// storage format (see [`crate::format`]); a refused file is left as it
    /// was. New stores are stamped transactionally with their first write,
    /// not by opening this handle.
    #[cfg(feature = "sqlite")]
    pub async fn open(path: &Path, options: FileOptions) -> Result<Self, Error> {
        // SQLite cannot open a directory; one the Groove engine wrote gets
        // the typed refusal.
        if path.is_dir() {
            crate::format::refuse_groove_store(path)?;
        }
        let flags = rusqlite::OpenFlags::default();
        let conn = match options.vfs {
            None => Storage::open_with_flags(path, flags),
            Some(vfs) => Storage::open_with_flags_and_vfs(path, flags, vfs),
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
                .await
                .map_err(storage)?;
        }
        // Before the journal mode, which rewrites the header: a refused file
        // is left as it was.
        crate::format::check(&conn).await?;
        // `journal_mode` answers with a row, so it is queried, not batched.
        let mode: String = conn
            .query_row(&format!("PRAGMA journal_mode = {journal}"), [], |r| {
                r.get(0)
            })
            .await
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
        .await
        .map_err(storage)?;
        Ok(Self {
            operation: futures::lock::Mutex::new(()),
            conn,
            path: Some((path.to_path_buf(), options.vfs)),
        })
    }

    /// A store on `storage` a host opened (a Durable Object's SQL:
    /// [`Storage::from_backend`]), admitted as a file is, with no path:
    /// its chunks live in the store itself.
    pub async fn from_storage(storage: Storage) -> Result<Self, Error> {
        crate::format::check(&storage).await?;
        Ok(Self {
            operation: futures::lock::Mutex::new(()),
            conn: storage,
            path: None,
        })
    }

    /// A store that lives only as long as what is opened on it.
    #[cfg(feature = "sqlite")]
    pub async fn memory() -> Result<Self, Error> {
        let conn = Storage::open_in_memory().map_err(storage)?;
        crate::format::check(&conn).await?;
        Ok(Self {
            conn,
            path: None,
            operation: futures::lock::Mutex::new(()),
        })
    }

    /// An in-memory store this process shares by `name`: it lives while any
    /// connection to it is open, so a host keeping one can reopen the store
    /// (or read it with a [`HostReader`]) without losing it.
    #[cfg(feature = "sqlite")]
    pub async fn shared_memory(name: &str) -> Result<Self, Error> {
        let conn =
            Storage::open_with_flags(shared_memory_uri(name), uri_flags()).map_err(storage)?;
        crate::format::check(&conn).await?;
        Ok(Self {
            conn,
            path: None,
            operation: futures::lock::Mutex::new(()),
        })
    }
}

/// Reads committed host tables on a connection of its own, so a reader on
/// another thread never waits for work on the store's connection (a WAL
/// file serves readers beside its writer). It sees every write that
/// returned before the read began.
#[cfg(feature = "sqlite")]
pub struct HostReader {
    conn: Mutex<Option<Storage>>,
}

#[cfg(feature = "sqlite")]
impl HostReader {
    /// Read the file at `path`, which a [`StoreFile`] opened already.
    pub fn open(path: &Path, vfs: Option<&'static str>) -> Result<Self, Error> {
        let flags = rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
            | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX
            | rusqlite::OpenFlags::SQLITE_OPEN_URI;
        let conn = match vfs {
            None => Storage::open_with_flags(path, flags),
            Some(vfs) => Storage::open_with_flags_and_vfs(path, flags, vfs),
        }
        .map_err(storage)?;
        Self::from_storage(conn)
    }

    /// Read the store [`StoreFile::shared_memory`] opened by `name`.
    pub fn shared_memory(name: &str) -> Result<Self, Error> {
        Self::from_storage(
            Storage::open_with_flags(shared_memory_uri(name), uri_flags()).map_err(storage)?,
        )
    }

    /// Read host tables on an independently opened storage handle.
    pub fn from_storage(conn: Storage) -> Result<Self, Error> {
        // A WAL reader waits only while another connection recovers the
        // log.
        conn.busy_timeout(Duration::from_secs(5)).map_err(storage)?;
        Ok(Self {
            conn: Mutex::new(Some(conn)),
        })
    }

    fn read<T>(&self, f: impl FnOnce(&Storage) -> Result<T, Error>) -> Result<T, Error> {
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
        self.read(|conn| crate::options::block_on(get(conn, table, key)))
    }

    /// Every pair whose key starts with `prefix`, in key order.
    pub fn prefix(&self, table: HostTable, prefix: &[u8]) -> Result<Vec<HostPair>, Error> {
        self.read(|conn| crate::options::block_on(prefix_scan(conn, table, prefix)))
    }

    /// The store's catalogue as committed (see [`crate::catalogue`]).
    pub fn catalogue(&self) -> Result<CatalogueRows, Error> {
        self.read(|conn| crate::options::block_on(crate::catalogue::read(conn)))
    }

    /// Run `f` on the read-only connection, for host tables that
    /// [`HostTables::host_sql`] shaped. A table no write created yet is
    /// missing here: `f` must not assume it exists.
    pub fn sql<T>(&self, f: impl FnOnce(&Storage) -> Result<T, Error>) -> Result<T, Error> {
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

#[async_trait::async_trait(?Send)]
impl HostTables for StoreFile {
    async fn host_get(&self, table: HostTable, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let _operation = self.operation.lock().await;
        get(&self.conn, table, key).await
    }

    async fn host_prefix(&self, table: HostTable, prefix: &[u8]) -> Result<Vec<HostPair>, Error> {
        let _operation = self.operation.lock().await;
        prefix_scan(&self.conn, table, prefix).await
    }

    async fn host_put_many(&self, table: HostTable, pairs: &[HostPair]) -> Result<(), Error> {
        let _operation = self.operation.lock().await;
        put_many(&self.conn, table, pairs).await
    }

    async fn host_put_if_absent(
        &self,
        table: HostTable,
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<Vec<u8>>, Error> {
        let _operation = self.operation.lock().await;
        put_if_absent(&self.conn, table, key, value).await
    }

    async fn host_delete(&self, table: HostTable, key: &[u8]) -> Result<(), Error> {
        let _operation = self.operation.lock().await;
        delete(&self.conn, table, key).await
    }

    async fn catalogue(&self) -> Result<CatalogueRows, Error> {
        let _operation = self.operation.lock().await;
        crate::catalogue::read(&self.conn).await
    }

    async fn catalogue_put(&self, put: &CataloguePut) -> Result<(), Error> {
        let _operation = self.operation.lock().await;
        self.conn
            .scope("host_write", async |conn| {
                crate::format::admit(conn).await?;
                crate::catalogue::put(conn, put).await
            })
            .await
    }

    async fn host_sql(&self, f: &mut dyn HostSqlBody) -> Result<(), Error> {
        let _operation = self.operation.lock().await;
        self.conn
            .scope("host_write", async |conn| {
                crate::format::admit(conn).await?;
                f.run(conn).await
            })
            .await
    }
}

#[async_trait::async_trait(?Send)]
impl HostTables for Db {
    async fn host_get(&self, table: HostTable, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        self.with_host_connection(async |conn| get(conn, table, key).await)
            .await
    }

    async fn host_prefix(&self, table: HostTable, prefix: &[u8]) -> Result<Vec<HostPair>, Error> {
        self.with_host_connection(async |conn| prefix_scan(conn, table, prefix).await)
            .await
    }

    async fn host_put_many(&self, table: HostTable, pairs: &[HostPair]) -> Result<(), Error> {
        self.with_host_connection(async |conn| put_many(conn, table, pairs).await)
            .await
    }

    async fn host_put_if_absent(
        &self,
        table: HostTable,
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<Vec<u8>>, Error> {
        self.with_host_connection(async |conn| put_if_absent(conn, table, key, value).await)
            .await
    }

    async fn host_delete(&self, table: HostTable, key: &[u8]) -> Result<(), Error> {
        self.with_host_connection(async |conn| delete(conn, table, key).await)
            .await
    }

    async fn catalogue(&self) -> Result<CatalogueRows, Error> {
        self.with_host_connection(crate::catalogue::read).await
    }

    async fn catalogue_put(&self, put: &CataloguePut) -> Result<(), Error> {
        self.with_host_connection(async |conn| {
            conn.scope("host_write", async |conn| {
                crate::catalogue::put(conn, put).await
            })
            .await
        })
        .await
    }

    async fn host_sql(&self, f: &mut dyn HostSqlBody) -> Result<(), Error> {
        self.with_host_connection(async |conn| {
            conn.scope("host_write", async |conn| f.run(conn).await)
                .await
        })
        .await
    }
}

#[cfg(any(test, feature = "testing"))]
impl Db {
    /// Tests: a host write that fails half done. Writes `pairs` inside the
    /// write's savepoint, then panics before releasing it.
    pub async fn host_put_many_then_panic_for_test(
        &self,
        table: HostTable,
        pairs: &[HostPair],
    ) -> Result<(), Error> {
        self.with_host_connection(async |conn| {
            crate::values::begin_savepoint(conn, "host_write")?;
            create(conn, table).await?;
            for (key, value) in pairs {
                conn.execute(
                    &format!(
                        "INSERT OR REPLACE INTO {} (k, v) VALUES (?1, ?2)",
                        table.sql_name()
                    ),
                    params![key, value],
                )
                .await
                .map_err(storage)?;
            }
            panic!("injected failure mid host write")
        })
        .await
    }
}

/// Whether `table` exists yet; reads of one that does not find nothing.
async fn exists(conn: &Storage, table: HostTable) -> Result<bool, Error> {
    (async {
        let mut statement = conn.prepare_cached(
            "SELECT count(*) > 0 FROM sqlite_master WHERE type = 'table' AND name = ?1",
        )?;
        statement
            .query_row([table.sql_name()], |row| row.get(0))
            .await
    })
    .await
    .map_err(storage)
}

async fn create(conn: &Storage, table: HostTable) -> Result<(), Error> {
    conn.execute_batch(&format!(
        "CREATE TABLE IF NOT EXISTS {} (k BLOB PRIMARY KEY, v BLOB NOT NULL) WITHOUT ROWID",
        table.sql_name()
    ))
    .await
    .map_err(storage)
}

async fn get(conn: &Storage, table: HostTable, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
    if !exists(conn, table).await? {
        return Ok(None);
    }
    (async {
        let mut statement =
            conn.prepare_cached(&format!("SELECT v FROM {} WHERE k = ?1", table.sql_name()))?;
        statement
            .query_row([key], |row| row.get(0))
            .await
            .optional()
    })
    .await
    .map_err(storage)
}

async fn prefix_scan(
    conn: &Storage,
    table: HostTable,
    prefix: &[u8],
) -> Result<Vec<HostPair>, Error> {
    if !exists(conn, table).await? {
        return Ok(Vec::new());
    }
    let mut statement = conn
        .prepare_cached(&format!(
            "SELECT k, v FROM {} WHERE k >= ?1 ORDER BY k",
            table.sql_name()
        ))
        .map_err(storage)?;
    let mut rows = statement
        .query_map([prefix], |row| Ok((row.get(0)?, row.get(1)?)))
        .await
        .map_err(storage)?;
    let mut out = Vec::new();
    while let Some(row) = rows.next().await {
        let (key, value): HostPair = row.map_err(storage)?;
        if !key.starts_with(prefix) {
            break;
        }
        out.push((key, value));
    }
    Ok(out)
}

async fn put_many(conn: &Storage, table: HostTable, pairs: &[HostPair]) -> Result<(), Error> {
    if pairs.is_empty() {
        return Ok(());
    }
    conn.scope("host_write", async |conn| {
        crate::format::admit(conn).await?;
        create(conn, table).await?;
        let mut statement = conn
            .prepare_cached(&format!(
                "INSERT OR REPLACE INTO {} (k, v) VALUES (?1, ?2)",
                table.sql_name()
            ))
            .map_err(storage)?;
        for (key, value) in pairs {
            statement
                .execute(params![key, value])
                .await
                .map_err(storage)?;
        }
        Ok(())
    })
    .await
}

pub(crate) async fn put_if_absent(
    conn: &Storage,
    table: HostTable,
    key: &[u8],
    value: &[u8],
) -> Result<Option<Vec<u8>>, Error> {
    conn.scope("host_write", async |conn| {
        crate::format::admit(conn).await?;
        create(conn, table).await?;
        let existing: Option<Vec<u8>> = (async {
            let mut statement =
                conn.prepare_cached(&format!("SELECT v FROM {} WHERE k = ?1", table.sql_name()))?;
            statement
                .query_row([key], |row| row.get(0))
                .await
                .optional()
        })
        .await
        .map_err(storage)?;
        if existing.is_none() {
            (async {
                let mut statement = conn.prepare_cached(&format!(
                    "INSERT INTO {} (k, v) VALUES (?1, ?2)",
                    table.sql_name()
                ))?;
                statement.execute(params![key, value]).await
            })
            .await
            .map_err(storage)?;
        }
        Ok(existing)
    })
    .await
}

async fn delete(conn: &Storage, table: HostTable, key: &[u8]) -> Result<(), Error> {
    if !exists(conn, table).await? {
        return Ok(());
    }
    (async {
        let mut statement =
            conn.prepare_cached(&format!("DELETE FROM {} WHERE k = ?1", table.sql_name()))?;
        statement.execute([key]).await
    })
    .await
    .map(|_| ())
    .map_err(storage)
}

#[cfg(test)]
mod tests {
    use crate::storage::StatementStatus;
    use crate::storage::Storage;

    /// Reprepares of a cached `LIMIT ?` read run `runs` times on `conn`.
    fn reprepares(conn: &Storage, runs: i64) -> i32 {
        crate::options::block_on(
            conn.execute_batch("CREATE TABLE t (x INTEGER); INSERT INTO t VALUES (1), (2), (3);"),
        )
        .unwrap();
        let sql = "SELECT x FROM t ORDER BY x LIMIT ?1 OFFSET ?2";
        for _ in 0..runs {
            let mut stmt = conn.prepare_cached(sql).unwrap();
            let rows: Vec<i64> = crate::options::block_on(
                crate::options::block_on(stmt.query_map([2i64, 0], |r| r.get::<_, i64>(0)))
                    .unwrap()
                    .collect::<Vec<_>>(),
            )
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
        let plain = Storage::open_in_memory().unwrap();
        assert!(
            reprepares(&plain, 5) >= 4,
            "SQLite no longer expires on LIMIT rebinds"
        );

        let conn = Storage::open_in_memory().unwrap();
        super::stable_plans(&conn).unwrap();
        assert_eq!(reprepares(&conn, 5), 0);
    }
}
