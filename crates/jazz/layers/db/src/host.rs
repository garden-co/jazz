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

use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;

use rusqlite::{Connection, OptionalExtension, params};

use crate::db::{Db, FileOptions, Journal};
use crate::options::{Error, ErrorCode};
use crate::values::{in_savepoint, storage};

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

/// Reads and writes of host tables, on whatever holds the store's
/// connection.
pub trait HostTables {
    /// The value at `key`.
    fn host_get(&self, table: HostTable, key: &[u8]) -> Result<Option<Vec<u8>>, Error>;
    /// Every pair whose key starts with `prefix`, in key order.
    fn host_prefix(&self, table: HostTable, prefix: &[u8]) -> Result<Vec<HostPair>, Error>;
    /// Write every pair (replacing what is there) in one commit: all or none.
    fn host_put_many(&self, table: HostTable, pairs: &[HostPair]) -> Result<(), Error>;
    /// Write `value` unless `key` exists; returns the existing value if so.
    fn host_put_if_absent(
        &self,
        table: HostTable,
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<Vec<u8>>, Error>;
    /// Remove `key`.
    fn host_delete(&self, table: HostTable, key: &[u8]) -> Result<(), Error>;
    /// Run `f` on the store's connection inside one `host_write` savepoint:
    /// what it wrote commits when it returns `Ok`, nothing does otherwise.
    /// For host tables with columns of their own (`_jazz_host_<name>`,
    /// created by `f` itself), which plain key-value pairs would hide from
    /// SQL.
    fn host_sql(&self, f: &mut dyn FnMut(&Connection) -> Result<(), Error>) -> Result<(), Error>;
}

/// A store's file opened and admitted, before a [`Db`] is opened on it:
/// [`StoreFile::open`] then [`Db::open_file`] is [`Db::open_at`].
pub struct StoreFile {
    pub(crate) conn: Connection,
    pub(crate) path: Option<(PathBuf, Option<&'static str>)>,
}

impl StoreFile {
    /// Open (creating) the file at `path` with `options`, and admit its
    /// storage format (see [`crate::format`]); a refused file is left as it
    /// was.
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
        crate::format::admit(&conn)?;
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
            conn,
            path: Some((path.to_path_buf(), options.vfs)),
        })
    }

    /// A store that lives only as long as what is opened on it.
    pub fn memory() -> Result<Self, Error> {
        let conn = Connection::open_in_memory().map_err(storage)?;
        crate::format::admit(&conn)?;
        Ok(Self { conn, path: None })
    }

    /// An in-memory store this process shares by `name`: it lives while any
    /// connection to it is open, so a host keeping one can reopen the store
    /// (or read it with a [`HostReader`]) without losing it.
    pub fn shared_memory(name: &str) -> Result<Self, Error> {
        let conn =
            Connection::open_with_flags(shared_memory_uri(name), uri_flags()).map_err(storage)?;
        crate::format::admit(&conn)?;
        Ok(Self { conn, path: None })
    }
}

/// Reads committed host tables on a connection of its own, so a reader on
/// another thread never waits for work on the store's connection (a WAL
/// file serves readers beside its writer). It sees every write that
/// returned before the read began.
pub struct HostReader {
    conn: Mutex<Option<Connection>>,
}

impl HostReader {
    /// Read the file at `path`, which a [`StoreFile`] opened already.
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
        self.read(|conn| get(conn, table, key))
    }

    /// Every pair whose key starts with `prefix`, in key order.
    pub fn prefix(&self, table: HostTable, prefix: &[u8]) -> Result<Vec<HostPair>, Error> {
        self.read(|conn| prefix_scan(conn, table, prefix))
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
fn shared_memory_uri(name: &str) -> String {
    format!("file:/{name}?vfs=memdb")
}

fn uri_flags() -> rusqlite::OpenFlags {
    rusqlite::OpenFlags::default() | rusqlite::OpenFlags::SQLITE_OPEN_URI
}

impl HostTables for StoreFile {
    fn host_get(&self, table: HostTable, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        get(&self.conn, table, key)
    }

    fn host_prefix(&self, table: HostTable, prefix: &[u8]) -> Result<Vec<HostPair>, Error> {
        prefix_scan(&self.conn, table, prefix)
    }

    fn host_put_many(&self, table: HostTable, pairs: &[HostPair]) -> Result<(), Error> {
        put_many(&self.conn, table, pairs)
    }

    fn host_put_if_absent(
        &self,
        table: HostTable,
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<Vec<u8>>, Error> {
        put_if_absent(&self.conn, table, key, value)
    }

    fn host_delete(&self, table: HostTable, key: &[u8]) -> Result<(), Error> {
        delete(&self.conn, table, key)
    }

    fn host_sql(&self, f: &mut dyn FnMut(&Connection) -> Result<(), Error>) -> Result<(), Error> {
        in_savepoint(&self.conn, "host_write", || f(&self.conn))
    }
}

impl HostTables for Db {
    fn host_get(&self, table: HostTable, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        self.with_host_connection(|conn| get(conn, table, key))
    }

    fn host_prefix(&self, table: HostTable, prefix: &[u8]) -> Result<Vec<HostPair>, Error> {
        self.with_host_connection(|conn| prefix_scan(conn, table, prefix))
    }

    fn host_put_many(&self, table: HostTable, pairs: &[HostPair]) -> Result<(), Error> {
        self.with_host_connection(|conn| put_many(conn, table, pairs))
    }

    fn host_put_if_absent(
        &self,
        table: HostTable,
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<Vec<u8>>, Error> {
        self.with_host_connection(|conn| put_if_absent(conn, table, key, value))
    }

    fn host_delete(&self, table: HostTable, key: &[u8]) -> Result<(), Error> {
        self.with_host_connection(|conn| delete(conn, table, key))
    }

    fn host_sql(&self, f: &mut dyn FnMut(&Connection) -> Result<(), Error>) -> Result<(), Error> {
        self.with_host_connection(|conn| in_savepoint(conn, "host_write", || f(conn)))
    }
}

#[cfg(any(test, feature = "testing"))]
impl Db {
    /// Tests: a host write that fails half done. Writes `pairs` inside the
    /// write's savepoint, then panics before releasing it.
    pub fn host_put_many_then_panic_for_test(
        &self,
        table: HostTable,
        pairs: &[HostPair],
    ) -> Result<(), Error> {
        self.with_host_connection(|conn| {
            crate::values::begin_savepoint(conn, "host_write")?;
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
        })
    }
}

/// Whether `table` exists yet; reads of one that does not find nothing.
fn exists(conn: &Connection, table: HostTable) -> Result<bool, Error> {
    conn.prepare_cached("SELECT count(*) > 0 FROM sqlite_master WHERE type = 'table' AND name = ?1")
        .and_then(|mut statement| statement.query_row([table.sql_name()], |row| row.get(0)))
        .map_err(storage)
}

fn create(conn: &Connection, table: HostTable) -> Result<(), Error> {
    conn.execute_batch(&format!(
        "CREATE TABLE IF NOT EXISTS {} (k BLOB PRIMARY KEY, v BLOB NOT NULL) WITHOUT ROWID",
        table.sql_name()
    ))
    .map_err(storage)
}

fn get(conn: &Connection, table: HostTable, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
    if !exists(conn, table)? {
        return Ok(None);
    }
    conn.prepare_cached(&format!("SELECT v FROM {} WHERE k = ?1", table.sql_name()))
        .and_then(|mut statement| statement.query_row([key], |row| row.get(0)).optional())
        .map_err(storage)
}

fn prefix_scan(conn: &Connection, table: HostTable, prefix: &[u8]) -> Result<Vec<HostPair>, Error> {
    if !exists(conn, table)? {
        return Ok(Vec::new());
    }
    let mut statement = conn
        .prepare_cached(&format!(
            "SELECT k, v FROM {} WHERE k >= ?1 ORDER BY k",
            table.sql_name()
        ))
        .map_err(storage)?;
    let rows = statement
        .query_map([prefix], |row| Ok((row.get(0)?, row.get(1)?)))
        .map_err(storage)?;
    let mut out = Vec::new();
    for row in rows {
        let (key, value): HostPair = row.map_err(storage)?;
        if !key.starts_with(prefix) {
            break;
        }
        out.push((key, value));
    }
    Ok(out)
}

fn put_many(conn: &Connection, table: HostTable, pairs: &[HostPair]) -> Result<(), Error> {
    if pairs.is_empty() {
        return Ok(());
    }
    in_savepoint(conn, "host_write", || {
        create(conn, table)?;
        let mut statement = conn
            .prepare_cached(&format!(
                "INSERT OR REPLACE INTO {} (k, v) VALUES (?1, ?2)",
                table.sql_name()
            ))
            .map_err(storage)?;
        for (key, value) in pairs {
            statement.execute(params![key, value]).map_err(storage)?;
        }
        Ok(())
    })
}

fn put_if_absent(
    conn: &Connection,
    table: HostTable,
    key: &[u8],
    value: &[u8],
) -> Result<Option<Vec<u8>>, Error> {
    in_savepoint(conn, "host_write", || {
        create(conn, table)?;
        let existing: Option<Vec<u8>> = conn
            .prepare_cached(&format!("SELECT v FROM {} WHERE k = ?1", table.sql_name()))
            .and_then(|mut statement| statement.query_row([key], |row| row.get(0)).optional())
            .map_err(storage)?;
        if existing.is_none() {
            conn.prepare_cached(&format!(
                "INSERT INTO {} (k, v) VALUES (?1, ?2)",
                table.sql_name()
            ))
            .and_then(|mut statement| statement.execute(params![key, value]))
            .map_err(storage)?;
        }
        Ok(existing)
    })
}

fn delete(conn: &Connection, table: HostTable, key: &[u8]) -> Result<(), Error> {
    if !exists(conn, table)? {
        return Ok(());
    }
    conn.prepare_cached(&format!("DELETE FROM {} WHERE k = ?1", table.sql_name()))
        .and_then(|mut statement| statement.execute([key]))
        .map(|_| ())
        .map_err(storage)
}
