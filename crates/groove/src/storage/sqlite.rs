//! SQLite implementation of the ordered KV storage trait.
//!
//! One database file backs each store. A single `kv` table keyed by an
//! interned column-family id and key blob supplies bytewise ordered reads;
//! `meta` identifies the format and records explicit durable boundaries.
//! Like the sibling backends this type is thread-affine: it owns one
//! connection behind a `RefCell`, with no pool and no async runtime.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use rusqlite::{Connection, OptionalExtension, TransactionBehavior};

use super::{
    ColumnFamilyName, Durability, Error, Key, KeyValue, ScanVisitor, Value, WriteOperation,
    apply_storage_delta,
};

const FORMAT: &[u8] = b"jazz-groove-kv";
const FORMAT_VERSION: &[u8] = &[1];

const CREATE_META: &str = "CREATE TABLE meta (key TEXT PRIMARY KEY, value BLOB NOT NULL)";
const CREATE_COLUMN_FAMILIES: &str =
    "CREATE TABLE column_families (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE)";
const CREATE_KV: &str = "CREATE TABLE kv (\n  cf INTEGER NOT NULL,\n  k  BLOB    NOT NULL,\n  v  BLOB    NOT NULL,\n  PRIMARY KEY (cf, k)\n) WITHOUT ROWID";

#[derive(Clone, Copy, Debug)]
struct WriteFlushCadence {
    every: usize,
    pending: usize,
}

/// SQLite implementation of [`super::OrderedKvStorage`].
///
/// Exactly one owning `SqliteStorage` should open a file at a time, matching
/// the ownership assumption of the sibling backends. Boundary-counter updates
/// nevertheless use an IMMEDIATE transaction so concurrent connections cannot
/// silently lose an increment.
#[derive(Debug)]
pub struct SqliteStorage {
    path: PathBuf,
    durability: Durability,
    column_families: RefCell<BTreeMap<String, i64>>,
    connection: RefCell<Option<Connection>>,
    write_flush_cadence: RefCell<Option<WriteFlushCadence>>,
}

impl SqliteStorage {
    /// Open with WAL atomicity and SQLite's `synchronous=NORMAL` mode.
    pub fn open(path: impl AsRef<Path>, column_families: &[&str]) -> Result<Self, Error> {
        Self::open_with_durability(path, column_families, Durability::WalNoSync)
    }

    /// Open a SQLite store with the requested local durability mode.
    pub fn open_with_durability(
        path: impl AsRef<Path>,
        column_families: &[&str],
        durability: Durability,
    ) -> Result<Self, Error> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent).map_err(|error| {
                Error::InvalidStorageLayout(format!(
                    "cannot create sqlite storage directory {}: {error}",
                    parent.display()
                ))
            })?;
        }

        let mut connection = Connection::open(&path)?;
        let mode: String =
            connection.pragma_update_and_check(None, "journal_mode", "WAL", |row| row.get(0))?;
        if !mode.eq_ignore_ascii_case("wal") {
            return Err(Error::InvalidStorageLayout(format!(
                "sqlite journal_mode is {mode:?}, expected wal"
            )));
        }
        connection.pragma_update(
            None,
            "synchronous",
            match durability {
                Durability::FullSync => "FULL",
                Durability::WalNoSync => "NORMAL",
            },
        )?;
        // These pragmas select F_FULLFSYNC on Apple platforms and are no-ops
        // elsewhere. Ordinary fsync is insufficient for the Apple power-loss
        // guarantee this durability mode advertises.
        connection.pragma_update(None, "fullfsync", "ON")?;
        connection.pragma_update(None, "checkpoint_fullfsync", "ON")?;
        connection.busy_timeout(std::time::Duration::from_secs(5))?;

        // Freshness is based on schema objects, not file existence. A crash
        // before the atomic initialization transaction commits may leave a
        // zero-byte or header-only file, both of which are safe to initialize.
        let object_count: i64 =
            connection.query_row("SELECT COUNT(*) FROM sqlite_master", [], |row| row.get(0))?;
        if object_count == 0 {
            Self::create_schema(&mut connection)?;
        } else {
            Self::validate_schema(&connection)?;
        }

        let storage = Self {
            path,
            durability,
            column_families: RefCell::new(BTreeMap::new()),
            connection: RefCell::new(Some(connection)),
            write_flush_cadence: RefCell::new(None),
        };
        storage.intern_column_families(column_families)?;
        Ok(storage)
    }

    fn create_schema(connection: &mut Connection) -> Result<(), Error> {
        // DDL and format markers are one transaction: initialization leaves
        // either no schema objects or one complete, identifiable store.
        let transaction = connection.transaction()?;
        transaction.execute_batch(&format!(
            "{CREATE_META};\n{CREATE_COLUMN_FAMILIES};\n{CREATE_KV};"
        ))?;
        let boundary_seq = 0u64.to_be_bytes();
        transaction.execute(
            "INSERT INTO meta (key, value) VALUES ('format', ?1), ('format_version', ?2), ('boundary_seq', ?3)",
            rusqlite::params![FORMAT, FORMAT_VERSION, &boundary_seq],
        )?;
        #[cfg(test)]
        tests::kill_test_init_barrier();
        transaction.commit()?;
        Ok(())
    }

    fn validate_schema(connection: &Connection) -> Result<(), Error> {
        // Enumerate every non-internal object. Extra tables, indexes, views,
        // or triggers can alter semantics and make the file unsafe to adopt.
        let mut statement = connection.prepare(
            "SELECT type, name, sql FROM sqlite_master \
             WHERE name NOT LIKE 'sqlite\\_%' ESCAPE '\\' ORDER BY name",
        )?;
        let objects: Vec<(String, String, Option<String>)> = statement
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
            .collect::<Result<_, _>>()?;

        let mut expected = BTreeMap::from([
            ("meta", CREATE_META),
            ("column_families", CREATE_COLUMN_FAMILIES),
            ("kv", CREATE_KV),
        ]);
        for (object_type, name, sql) in &objects {
            match expected.remove(name.as_str()) {
                Some(expected_sql) if object_type == "table" => {
                    if sql.as_deref() != Some(expected_sql) {
                        return Err(Error::InvalidStorageLayout(format!(
                            "table {name} shape diverges from the expected layout: {sql:?}"
                        )));
                    }
                }
                _ => {
                    return Err(Error::InvalidStorageLayout(format!(
                        "unexpected schema object {object_type} {name}; refusing to adopt this file"
                    )));
                }
            }
        }
        if let Some((missing, _)) = expected.into_iter().next() {
            return Err(Error::InvalidStorageLayout(format!(
                "table {missing} is missing; refusing to adopt this file"
            )));
        }

        let format: Vec<u8> = connection
            .query_row("SELECT value FROM meta WHERE key = 'format'", [], |row| {
                row.get(0)
            })
            .optional()?
            .ok_or_else(|| Error::InvalidStorageLayout("meta.format row is missing".into()))?;
        if format != FORMAT {
            return Err(Error::InvalidStorageLayout(format!(
                "meta.format is {:?}, expected {:?}",
                String::from_utf8_lossy(&format),
                String::from_utf8_lossy(FORMAT)
            )));
        }

        let version: Vec<u8> = connection
            .query_row(
                "SELECT value FROM meta WHERE key = 'format_version'",
                [],
                |row| row.get(0),
            )
            .optional()?
            .ok_or_else(|| {
                Error::InvalidStorageLayout("meta.format_version row is missing".into())
            })?;
        if version != FORMAT_VERSION {
            return Err(Error::InvalidStorageLayout(format!(
                "meta.format_version is {version:?}, supported version is {FORMAT_VERSION:?}"
            )));
        }

        let boundary: Vec<u8> = connection
            .query_row(
                "SELECT value FROM meta WHERE key = 'boundary_seq'",
                [],
                |row| row.get(0),
            )
            .optional()?
            .ok_or_else(|| {
                Error::InvalidStorageLayout("meta.boundary_seq row is missing".into())
            })?;
        if boundary.len() != 8 {
            return Err(Error::InvalidStorageLayout(format!(
                "meta.boundary_seq must be exactly 8 bytes, found {}",
                boundary.len()
            )));
        }
        Ok(())
    }

    fn intern_column_families(&self, column_families: &[&str]) -> Result<(), Error> {
        let connection = self.connection.borrow();
        let connection = connection.as_ref().ok_or(Error::StorageClosed)?;
        let mut interned = self.column_families.borrow_mut();
        for name in column_families {
            connection.execute(
                "INSERT OR IGNORE INTO column_families (name) VALUES (?1)",
                [name],
            )?;
            let id: i64 = connection.query_row(
                "SELECT id FROM column_families WHERE name = ?1",
                [name],
                |row| row.get(0),
            )?;
            interned.insert((*name).to_owned(), id);
        }

        let mut statement = connection.prepare("SELECT name, id FROM column_families")?;
        let rows = statement.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })?;
        for row in rows {
            let (name, id) = row?;
            interned.entry(name).or_insert(id);
        }
        Ok(())
    }

    fn cf_id(&self, cf: &ColumnFamilyName) -> Result<i64, Error> {
        self.column_families
            .borrow()
            .get(cf)
            .copied()
            .ok_or_else(|| Error::ColumnFamilyNotFound(cf.to_owned()))
    }

    fn with_connection<T>(
        &self,
        operate: impl FnOnce(&Connection) -> Result<T, Error>,
    ) -> Result<T, Error> {
        let connection = self.connection.borrow();
        let connection = connection.as_ref().ok_or(Error::StorageClosed)?;
        operate(connection)
    }

    fn scan_prefix_directed(
        &self,
        cf: &ColumnFamilyName,
        prefix: &Key,
        direction: ScanDirection,
        limit_one: bool,
        upper_inclusive: Option<&Key>,
        visit: &mut ScanVisitor<'_>,
    ) -> Result<(), Error> {
        let cf_id = self.cf_id(cf)?;
        let upper = prefix_upper_bound(prefix);
        let order = match direction {
            ScanDirection::Forward => "ORDER BY k",
            ScanDirection::Reverse => "ORDER BY k DESC",
        };
        self.with_connection(|connection| {
            let mut clause = String::from("AND k >= ?2");
            let mut parameters: Vec<&dyn rusqlite::ToSql> = vec![&cf_id, &prefix];
            if let Some(upper) = upper.as_ref() {
                clause.push_str(" AND k < ?3");
                parameters.push(upper);
            }
            let query = format!("SELECT k, v FROM kv WHERE cf = ?1 {clause} {order}");
            let mut statement = connection.prepare_cached(&query)?;
            let mut rows = statement.query(&parameters[..])?;
            while let Some(row) = rows.next()? {
                let key: Vec<u8> = row.get(0)?;
                if !key.starts_with(prefix) {
                    match direction {
                        ScanDirection::Forward => break,
                        ScanDirection::Reverse => {
                            if key.as_slice() < prefix {
                                break;
                            }
                            continue;
                        }
                    }
                }
                if upper_inclusive.is_some_and(|upper| key.as_slice() > upper) {
                    continue;
                }
                let value: Vec<u8> = row.get(1)?;
                visit(&key, &value)?;
                if limit_one {
                    break;
                }
            }
            Ok(())
        })
    }
}

#[derive(Clone, Copy)]
enum ScanDirection {
    Forward,
    Reverse,
}

fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut bound = prefix.to_vec();
    for index in (0..bound.len()).rev() {
        if bound[index] != u8::MAX {
            bound[index] += 1;
            bound.truncate(index + 1);
            return Some(bound);
        }
    }
    None
}

#[inline(always)]
fn maybe_test_kill_barrier(op_index: usize) {
    #[cfg(test)]
    tests::kill_test_barrier(op_index);
    #[cfg(not(test))]
    let _ = op_index;
}

impl super::OrderedKvStorage for SqliteStorage {
    fn get(&self, cf: &ColumnFamilyName, key: &Key) -> Result<Option<Value>, Error> {
        let cf_id = self.cf_id(cf)?;
        self.with_connection(|connection| {
            Ok(connection
                .query_row(
                    "SELECT v FROM kv WHERE cf = ?1 AND k = ?2",
                    rusqlite::params![cf_id, key],
                    |row| row.get::<_, Vec<u8>>(0),
                )
                .optional()?)
        })
    }

    fn set(&self, cf: &ColumnFamilyName, key: &Key, value: &[u8]) -> Result<(), Error> {
        let cf_id = self.cf_id(cf)?;
        self.with_connection(|connection| {
            connection.execute(
                "INSERT OR REPLACE INTO kv (cf, k, v) VALUES (?1, ?2, ?3)",
                rusqlite::params![cf_id, key, value],
            )?;
            Ok(())
        })
    }

    fn delete(&self, cf: &ColumnFamilyName, key: &Key) -> Result<(), Error> {
        let cf_id = self.cf_id(cf)?;
        self.with_connection(|connection| {
            connection.execute(
                "DELETE FROM kv WHERE cf = ?1 AND k = ?2",
                rusqlite::params![cf_id, key],
            )?;
            Ok(())
        })
    }

    fn scan_range(
        &self,
        cf: &ColumnFamilyName,
        start: &Key,
        end: &Key,
        visit: &mut ScanVisitor<'_>,
    ) -> Result<(), Error> {
        let cf_id = self.cf_id(cf)?;
        self.with_connection(|connection| {
            let mut statement = connection.prepare_cached(
                "SELECT k, v FROM kv WHERE cf = ?1 AND k >= ?2 AND k < ?3 ORDER BY k",
            )?;
            let mut rows = statement.query(rusqlite::params![cf_id, start, end])?;
            while let Some(row) = rows.next()? {
                let key: Vec<u8> = row.get(0)?;
                let value: Vec<u8> = row.get(1)?;
                visit(&key, &value)?;
            }
            Ok(())
        })
    }

    fn scan_prefix(
        &self,
        cf: &ColumnFamilyName,
        prefix: &Key,
        visit: &mut ScanVisitor<'_>,
    ) -> Result<(), Error> {
        self.scan_prefix_directed(cf, prefix, ScanDirection::Forward, false, None, visit)
    }

    fn scan_prefix_reverse(
        &self,
        cf: &ColumnFamilyName,
        prefix: &Key,
        visit: &mut ScanVisitor<'_>,
    ) -> Result<(), Error> {
        self.scan_prefix_directed(cf, prefix, ScanDirection::Reverse, false, None, visit)
    }

    fn last_with_prefix(
        &self,
        cf: &ColumnFamilyName,
        prefix: &Key,
    ) -> Result<Option<KeyValue>, Error> {
        let mut last = None;
        self.scan_prefix_directed(
            cf,
            prefix,
            ScanDirection::Reverse,
            true,
            None,
            &mut |key, value| {
                last = Some((key.to_vec(), value.to_vec()));
                Ok(())
            },
        )?;
        Ok(last)
    }

    fn last_with_prefix_before_or_at(
        &self,
        cf: &ColumnFamilyName,
        prefix: &Key,
        upper: &Key,
    ) -> Result<Option<KeyValue>, Error> {
        let mut last = None;
        self.scan_prefix_directed(
            cf,
            prefix,
            ScanDirection::Reverse,
            true,
            Some(upper),
            &mut |key, value| {
                last = Some((key.to_vec(), value.to_vec()));
                Ok(())
            },
        )?;
        Ok(last)
    }

    fn write_many(&self, operations: &[WriteOperation<'_>]) -> Result<(), Error> {
        // Validate every family before opening the transaction so a bad name
        // can never leave a partial batch.
        for operation in operations {
            let cf = match operation {
                WriteOperation::Set { cf, .. }
                | WriteOperation::Delete { cf, .. }
                | WriteOperation::Delta { cf, .. } => *cf,
            };
            self.cf_id(cf)?;
        }

        {
            let mut connection = self.connection.borrow_mut();
            let connection = connection.as_mut().ok_or(Error::StorageClosed)?;
            let transaction = connection.transaction()?;
            for (index, operation) in operations.iter().enumerate() {
                maybe_test_kill_barrier(index);
                match operation {
                    WriteOperation::Set { cf, key, value } => {
                        let cf_id = self.cf_id(cf)?;
                        transaction.execute(
                            "INSERT OR REPLACE INTO kv (cf, k, v) VALUES (?1, ?2, ?3)",
                            rusqlite::params![cf_id, key, value],
                        )?;
                    }
                    WriteOperation::Delete { cf, key } => {
                        let cf_id = self.cf_id(cf)?;
                        transaction.execute(
                            "DELETE FROM kv WHERE cf = ?1 AND k = ?2",
                            rusqlite::params![cf_id, key],
                        )?;
                    }
                    WriteOperation::Delta { cf, key, delta } => {
                        let cf_id = self.cf_id(cf)?;
                        let current: Option<Vec<u8>> = transaction
                            .query_row(
                                "SELECT v FROM kv WHERE cf = ?1 AND k = ?2",
                                rusqlite::params![cf_id, key],
                                |row| row.get(0),
                            )
                            .optional()?;
                        let encoded = delta.encode()?;
                        let merged = apply_storage_delta(current.as_deref(), &encoded)?;
                        transaction.execute(
                            "INSERT OR REPLACE INTO kv (cf, k, v) VALUES (?1, ?2, ?3)",
                            rusqlite::params![cf_id, key, merged],
                        )?;
                    }
                }
            }
            transaction.commit()?;
        }

        let should_flush = match self.write_flush_cadence.borrow_mut().as_mut() {
            Some(cadence) => {
                cadence.pending += 1;
                if cadence.pending == cadence.every {
                    cadence.pending = 0;
                    true
                } else {
                    false
                }
            }
            None => false,
        };
        if should_flush {
            self.flush_write_boundary()?;
        }
        Ok(())
    }

    fn set_write_flush_cadence(&self, every: usize) -> Result<(), Error> {
        assert!(every > 0, "write flush cadence must be non-zero");
        self.with_connection(|_| Ok(()))?;
        *self.write_flush_cadence.borrow_mut() = Some(WriteFlushCadence { every, pending: 0 });
        Ok(())
    }

    fn flush_write_boundary(&self) -> Result<(), Error> {
        {
            let mut connection = self.connection.borrow_mut();
            let connection = connection.as_mut().ok_or(Error::StorageClosed)?;
            connection.pragma_update(None, "synchronous", "FULL")?;
            let result = (|| -> Result<(), Error> {
                let transaction =
                    connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let bytes: Vec<u8> = transaction
                    .query_row(
                        "SELECT value FROM meta WHERE key = 'boundary_seq'",
                        [],
                        |row| row.get(0),
                    )
                    .optional()?
                    .ok_or_else(|| {
                        Error::InvalidStorageLayout("meta.boundary_seq row is missing".into())
                    })?;
                let bytes: [u8; 8] = bytes.try_into().map_err(|found: Vec<u8>| {
                    Error::InvalidStorageLayout(format!(
                        "meta.boundary_seq must be exactly 8 bytes, found {}",
                        found.len()
                    ))
                })?;
                let next = u64::from_be_bytes(bytes).wrapping_add(1).to_be_bytes();
                let changed = transaction.execute(
                    "UPDATE meta SET value = ?1 WHERE key = 'boundary_seq'",
                    [&next[..]],
                )?;
                if changed != 1 {
                    return Err(Error::InvalidStorageLayout(format!(
                        "boundary_seq update touched {changed} rows, expected 1"
                    )));
                }
                transaction.commit()?;
                Ok(())
            })();
            let restore = match self.durability {
                Durability::FullSync => "FULL",
                Durability::WalNoSync => "NORMAL",
            };
            let restore_result = connection.pragma_update(None, "synchronous", restore);
            result?;
            restore_result?;
        }
        if let Some(cadence) = self.write_flush_cadence.borrow_mut().as_mut() {
            cadence.pending = 0;
        }
        Ok(())
    }

    fn close(&self) -> Result<(), Error> {
        let Some(connection) = self.connection.borrow_mut().take() else {
            return Ok(());
        };
        if let Err(error) = connection.busy_timeout(std::time::Duration::from_millis(250)) {
            self.connection.borrow_mut().replace(connection);
            return Err(Error::Sqlite(error));
        }
        if let Err(error) = connection.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
            ))
        }) {
            self.connection.borrow_mut().replace(connection);
            return Err(Error::Sqlite(error));
        }
        // A busy TRUNCATE is only a missed compaction opportunity. Committed
        // frames remain in the WAL and SQLite replays them on the next open,
        // so closing the handle is both safe and preferable to surfacing a
        // non-retryable shutdown error through mobile bindings.
        match connection.close() {
            Ok(()) => Ok(()),
            Err((connection, error)) => {
                self.connection.borrow_mut().replace(connection);
                Err(Error::Sqlite(error))
            }
        }
    }

    fn column_family_names(&self) -> Option<Vec<String>> {
        Some(self.column_families.borrow().keys().cloned().collect())
    }
}

impl super::ReopenableStorage for SqliteStorage {
    fn reopen(self, column_families: &[&str]) -> Result<Self, Error> {
        let path = self.path.clone();
        let durability = self.durability;
        super::OrderedKvStorage::close(&self)?;
        Self::open_with_durability(path, column_families, durability)
    }
}

#[cfg(test)]
mod tests {
    // Backend layout, byte ordering, and crash boundaries are not observable
    // through Jazz's public API, so these remain internal contract tests like
    // the sibling memory and OPFS backend suites.
    use super::super::{
        Durability, Error, OrderedKvStorage, ReopenableStorage, StorageDelta, StorageDeltaKind,
        WriteOperation,
    };
    use super::SqliteStorage;

    fn db_path(dir: &tempfile::TempDir) -> std::path::PathBuf {
        dir.path().join("store.db")
    }

    fn open_records(dir: &tempfile::TempDir) -> SqliteStorage {
        SqliteStorage::open(db_path(dir), &["records"]).unwrap()
    }

    #[test]
    fn open_creates_fresh_store_and_reopens_it() {
        let dir = tempfile::tempdir().unwrap();
        let storage = SqliteStorage::open(db_path(&dir), &["records", "meta_cf"]).unwrap();
        drop(storage);
        let storage = SqliteStorage::open(db_path(&dir), &["records", "meta_cf"]).unwrap();
        drop(storage);
    }

    #[test]
    fn open_treats_empty_or_schemaless_files_as_fresh() {
        let dir = tempfile::tempdir().unwrap();
        let zero_byte = dir.path().join("zero.db");
        std::fs::write(&zero_byte, b"").unwrap();
        drop(SqliteStorage::open(&zero_byte, &["records"]).unwrap());

        let headers_only = dir.path().join("headers.db");
        let conn = rusqlite::Connection::open(&headers_only).unwrap();
        conn.pragma_update_and_check(None, "journal_mode", "WAL", |_| Ok(()))
            .unwrap();
        conn.pragma_update(None, "user_version", 0).unwrap();
        drop(conn);
        drop(SqliteStorage::open(&headers_only, &["records"]).unwrap());
    }

    #[test]
    fn open_rejects_alien_sqlite_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = db_path(&dir);
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute("CREATE TABLE not_ours (x INTEGER)", [])
            .unwrap();
        drop(conn);

        let error = SqliteStorage::open(&path, &["records"]).unwrap_err();
        assert!(
            matches!(&error, Error::InvalidStorageLayout(message) if message.contains("not_ours") || message.contains("meta")),
            "alien sqlite file must be rejected as a layout error, got: {error:?}"
        );
    }

    #[test]
    fn open_rejects_tampered_table_shape() {
        let dir = tempfile::tempdir().unwrap();
        let path = db_path(&dir);
        drop(SqliteStorage::open(&path, &["records"]).unwrap());
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch(
            "ALTER TABLE kv RENAME TO kv_old;
             CREATE TABLE kv (cf INTEGER, k BLOB, v BLOB);",
        )
        .unwrap();
        drop(conn);

        let error = SqliteStorage::open(&path, &["records"]).unwrap_err();
        assert!(
            matches!(&error, Error::InvalidStorageLayout(message) if message.contains("kv")),
            "shape-diverged kv table must be rejected, got: {error:?}"
        );
    }

    #[test]
    fn open_rejects_unexpected_schema_objects() {
        let dir = tempfile::tempdir().unwrap();
        let path = db_path(&dir);
        drop(SqliteStorage::open(&path, &["records"]).unwrap());
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch("CREATE TRIGGER sneaky AFTER INSERT ON kv BEGIN DELETE FROM kv; END;")
            .unwrap();
        drop(conn);

        let error = SqliteStorage::open(&path, &["records"]).unwrap_err();
        assert!(
            matches!(&error, Error::InvalidStorageLayout(message) if message.contains("sneaky")),
            "unexpected trigger must be rejected, got: {error:?}"
        );
    }

    #[test]
    fn open_rejects_unknown_format_version() {
        let dir = tempfile::tempdir().unwrap();
        let path = db_path(&dir);
        drop(SqliteStorage::open(&path, &["records"]).unwrap());
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute(
            "UPDATE meta SET value = X'02' WHERE key = 'format_version'",
            [],
        )
        .unwrap();
        drop(conn);

        let error = SqliteStorage::open(&path, &["records"]).unwrap_err();
        assert!(
            matches!(&error, Error::InvalidStorageLayout(message) if message.contains("format_version")),
            "unknown format version must be rejected, got: {error:?}"
        );
    }

    #[test]
    fn open_rejects_malformed_boundary_seq() {
        let dir = tempfile::tempdir().unwrap();
        let path = db_path(&dir);
        drop(SqliteStorage::open(&path, &["records"]).unwrap());
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute(
            "UPDATE meta SET value = X'00' WHERE key = 'boundary_seq'",
            [],
        )
        .unwrap();
        drop(conn);

        let error = SqliteStorage::open(&path, &["records"]).unwrap_err();
        assert!(
            matches!(&error, Error::InvalidStorageLayout(message) if message.contains("boundary_seq")),
            "boundary_seq must be exactly 8 bytes, got: {error:?}"
        );
    }

    #[test]
    fn open_reports_garbage_file_as_sqlite_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = db_path(&dir);
        std::fs::write(&path, b"this is not a sqlite database, not even close").unwrap();
        let error = SqliteStorage::open(&path, &["records"]).unwrap_err();
        assert!(matches!(error, Error::Sqlite(_)));
    }

    #[test]
    fn point_ops_round_trip_and_unknown_cf_errors() {
        let dir = tempfile::tempdir().unwrap();
        let storage = open_records(&dir);
        assert_eq!(storage.get("records", b"a").unwrap(), None);
        storage.set("records", b"a", b"one").unwrap();
        assert_eq!(storage.get("records", b"a").unwrap(), Some(b"one".to_vec()));
        storage.set("records", b"a", b"two").unwrap();
        assert_eq!(storage.get("records", b"a").unwrap(), Some(b"two".to_vec()));
        storage.delete("records", b"a").unwrap();
        assert_eq!(storage.get("records", b"a").unwrap(), None);
        storage.set("records", b"", b"empty-key").unwrap();
        assert_eq!(
            storage.get("records", b"").unwrap(),
            Some(b"empty-key".to_vec())
        );
        assert!(matches!(
            storage.get("absent", b"a").unwrap_err(),
            Error::ColumnFamilyNotFound(_)
        ));
    }

    #[test]
    fn sqlite_storage_passes_read_semantics_conformance() {
        let dir = tempfile::tempdir().unwrap();
        super::super::conformance::ordered_scans_scope_prefixes_including_all_ff(open_records(
            &dir,
        ));
        let dir = tempfile::tempdir().unwrap();
        super::super::conformance::reverse_and_last_lookups_match_forward_scans(open_records(&dir));
    }

    #[test]
    fn scan_visitor_errors_abort_the_scan() {
        let dir = tempfile::tempdir().unwrap();
        let storage = open_records(&dir);
        storage.set("records", b"a", b"1").unwrap();
        storage.set("records", b"b", b"2").unwrap();
        let mut visited = 0;
        let result = storage.scan_prefix("records", b"", &mut |_key, _value| {
            visited += 1;
            Err(Error::InvalidStorageKey("stop".into()))
        });
        assert!(matches!(result.unwrap_err(), Error::InvalidStorageKey(_)));
        assert_eq!(visited, 1);
    }

    #[test]
    fn write_many_is_atomic_and_same_batch_deltas_see_staged_state() {
        let dir = tempfile::tempdir().unwrap();
        let storage = open_records(&dir);
        storage
            .write_many(&[
                WriteOperation::set("records", b"a", b"one"),
                WriteOperation::delete("records", b"a"),
                WriteOperation::set("records", b"b", b"two"),
            ])
            .unwrap();
        assert_eq!(storage.get("records", b"a").unwrap(), None);
        assert_eq!(storage.get("records", b"b").unwrap(), Some(b"two".to_vec()));

        let dir2 = tempfile::tempdir().unwrap();
        super::super::conformance::set_then_delta_in_one_batch_observes_staged_value(open_records(
            &dir2,
        ));

        let bogus = StorageDelta {
            kind: StorageDeltaKind::CurrentWinnerV1,
            payload: b"not a valid current-winner delta".to_vec(),
        };
        let error = storage
            .write_many(&[
                WriteOperation::set("records", b"c", b"staged"),
                WriteOperation::delta("records", b"c", &bogus),
            ])
            .unwrap_err();
        assert!(matches!(
            error,
            Error::InvalidStorageDelta(_) | Error::Record(_)
        ));
        assert_eq!(storage.get("records", b"c").unwrap(), None);
    }

    fn read_boundary_seq(path: &std::path::Path) -> u64 {
        let conn = rusqlite::Connection::open(path).unwrap();
        let bytes: Vec<u8> = conn
            .query_row(
                "SELECT value FROM meta WHERE key = 'boundary_seq'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        u64::from_be_bytes(bytes.try_into().unwrap())
    }

    #[test]
    fn boundary_flush_increments_the_sequence_exactly() {
        let dir = tempfile::tempdir().unwrap();
        let storage = open_records(&dir);
        storage.set_write_flush_cadence(2).unwrap();
        storage
            .write_many(&[WriteOperation::set("records", b"a", b"1")])
            .unwrap();
        storage
            .write_many(&[WriteOperation::set("records", b"b", b"2")])
            .unwrap();
        storage.flush_write_boundary().unwrap();
        storage.close().unwrap();
        assert_eq!(read_boundary_seq(&db_path(&dir)), 2);
    }

    #[test]
    fn synchronous_pragma_matches_durability_mode_and_survives_boundary() {
        fn synchronous_level(storage: &SqliteStorage) -> i64 {
            storage
                .with_connection(|connection| {
                    Ok(connection.pragma_query_value(None, "synchronous", |row| row.get(0))?)
                })
                .unwrap()
        }

        let dir = tempfile::tempdir().unwrap();
        let full =
            SqliteStorage::open_with_durability(db_path(&dir), &["records"], Durability::FullSync)
                .unwrap();
        assert_eq!(synchronous_level(&full), 2);
        full.flush_write_boundary().unwrap();
        assert_eq!(synchronous_level(&full), 2);
        full.close().unwrap();

        let dir = tempfile::tempdir().unwrap();
        let normal = open_records(&dir);
        assert_eq!(synchronous_level(&normal), 1);
        normal.flush_write_boundary().unwrap();
        assert_eq!(synchronous_level(&normal), 1);
    }

    #[test]
    fn close_then_reopen_preserves_data_and_post_close_calls_error() {
        let dir = tempfile::tempdir().unwrap();
        let storage = open_records(&dir);
        storage.set("records", b"a", b"one").unwrap();
        storage.close().unwrap();
        assert!(matches!(
            storage.get("records", b"a").unwrap_err(),
            Error::StorageClosed
        ));
        assert!(matches!(
            storage.set("records", b"a", b"x").unwrap_err(),
            Error::StorageClosed
        ));

        let storage = SqliteStorage::open(db_path(&dir), &["records"]).unwrap();
        assert_eq!(storage.get("records", b"a").unwrap(), Some(b"one".to_vec()));
        let reopened = storage.reopen(&["records", "added_family"]).unwrap();
        reopened.set("added_family", b"k", b"v").unwrap();
        assert_eq!(
            reopened.get("added_family", b"k").unwrap(),
            Some(b"v".to_vec())
        );
    }

    #[test]
    fn close_with_blocked_checkpoint_leaves_recoverable_wal() {
        // This close-time storage behavior is not observable through a higher
        // database API: the external reader deliberately blocks SQLite's WAL
        // compaction while the storage handle itself shuts down.
        let dir = tempfile::tempdir().unwrap();
        let storage = open_records(&dir);
        storage.set("records", b"a", b"one").unwrap();
        let mut reader = rusqlite::Connection::open(db_path(&dir)).unwrap();
        let tx = reader.transaction().unwrap();
        let _: i64 = tx
            .query_row("SELECT COUNT(*) FROM kv", [], |row| row.get(0))
            .unwrap();
        storage.close().unwrap();
        assert!(matches!(
            storage.get("records", b"a").unwrap_err(),
            Error::StorageClosed
        ));
        drop(tx);
        drop(reader);

        let reopened = SqliteStorage::open(db_path(&dir), &["records"]).unwrap();
        assert_eq!(
            reopened.get("records", b"a").unwrap(),
            Some(b"one".to_vec())
        );
    }

    #[test]
    fn sqlite_storage_passes_shared_persistence_conformance() {
        let dir = tempfile::tempdir().unwrap();
        super::super::conformance::persistence_order_and_batch_atomicity(open_records(&dir));
    }

    #[test]
    fn sqlite_storage_passes_shared_delta_conformance() {
        let dir = tempfile::tempdir().unwrap();
        super::super::conformance::delta_append_current_winner_observes_merged_state(open_records(
            &dir,
        ));
    }

    #[test]
    fn sqlite_storage_passes_shared_reopen_conformance() {
        let dir = tempfile::tempdir().unwrap();
        super::super::conformance::reopen_preserves_data_and_adds_families(open_records(&dir));
    }

    #[test]
    fn multi_megabyte_values_round_trip_at_both_durability_levels() {
        for durability in [Durability::WalNoSync, Durability::FullSync] {
            let dir = tempfile::tempdir().unwrap();
            let storage =
                SqliteStorage::open_with_durability(db_path(&dir), &["records"], durability)
                    .unwrap();
            let value: Vec<u8> = (0..(4 * 1024 * 1024u32))
                .map(|index| (index % 251) as u8)
                .collect();
            storage.set("records", b"blob", &value).unwrap();
            storage.close().unwrap();
            let storage = open_records(&dir);
            assert_eq!(storage.get("records", b"blob").unwrap(), Some(value));
        }
    }

    /// Parks inside the initialization transaction so the parent can SIGKILL
    /// the child after DDL and meta staging but before commit.
    pub(super) fn kill_test_init_barrier() {
        if std::env::var("SQLITE_KILL_TEST_INIT").is_err() {
            return;
        }
        let ready = std::env::var("SQLITE_KILL_TEST_READY").unwrap();
        std::fs::write(&ready, b"parked-in-init").unwrap();
        loop {
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
    }

    /// Parks the selected `write_many` call inside its open transaction after
    /// a deterministic number of operations have been staged.
    pub(super) fn kill_test_barrier(op_index: usize) {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static WRITE_MANY_CALL: AtomicUsize = AtomicUsize::new(0);

        let Ok(barrier_call) = std::env::var("SQLITE_KILL_TEST_BARRIER_CALL") else {
            return;
        };
        let barrier_call: usize = barrier_call.parse().unwrap();
        let barrier_after: usize = std::env::var("SQLITE_KILL_TEST_BARRIER_AFTER")
            .unwrap()
            .parse()
            .unwrap();

        if op_index == 0 {
            WRITE_MANY_CALL.fetch_add(1, Ordering::SeqCst);
        }
        let call = WRITE_MANY_CALL.load(Ordering::SeqCst);
        if call == barrier_call && op_index == barrier_after {
            let ready = std::env::var("SQLITE_KILL_TEST_READY").unwrap();
            std::fs::write(&ready, b"parked").unwrap();
            loop {
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
        }
    }

    // ---- abrupt-termination recovery --------------------------------------
    // These tests exercise app-crash WAL recovery, initialization atomicity,
    // batch atomicity, and explicit-boundary persistence. They do not simulate
    // power loss; that guarantee follows SQLite's synchronous/fullfsync
    // semantics and is separately guarded by pragma-state assertions.

    #[test]
    fn sqlite_kill_test_child_writer() {
        let Ok(role) = std::env::var("SQLITE_KILL_TEST_ROLE") else {
            return;
        };
        let path = std::env::var("SQLITE_KILL_TEST_DB").unwrap();
        let ready = std::env::var("SQLITE_KILL_TEST_READY").unwrap();

        if role == "init" {
            let _ = SqliteStorage::open(&path, &["records"]);
            unreachable!("init barrier must park inside create_schema");
        }

        let durability = match role.as_str() {
            "full_sync" => Durability::FullSync,
            "wal_no_sync" | "torn_batch" => Durability::WalNoSync,
            other => panic!("unknown kill-test role {other}"),
        };
        let storage = SqliteStorage::open_with_durability(&path, &["records"], durability).unwrap();

        if role == "torn_batch" {
            for generation in 0u8..2 {
                let keys: Vec<Vec<u8>> = (0u8..8)
                    .map(|slot| vec![b'g', b'e', b'n', generation, b':', slot])
                    .collect();
                let value = [generation];
                let operations: Vec<WriteOperation<'_>> = keys
                    .iter()
                    .map(|key| WriteOperation::set("records", key, &value))
                    .collect();
                storage.write_many(&operations).unwrap();
            }
            unreachable!("barrier must park inside generation 1's transaction");
        }

        storage.set("records", b"before-boundary", b"1").unwrap();
        if role == "wal_no_sync" {
            storage.flush_write_boundary().unwrap();
        }
        std::fs::write(&ready, b"ready").unwrap();
        storage.set("records", b"after-boundary", b"2").unwrap();
        loop {
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
    }

    #[cfg(unix)]
    fn run_kill_test(
        role: &str,
        init_barrier: bool,
        batch_barrier: Option<(usize, usize)>,
    ) -> (tempfile::TempDir, SqliteStorage) {
        let dir = tempfile::tempdir().unwrap();
        let db = db_path(&dir).to_string_lossy().into_owned();
        let ready = dir.path().join("ready").to_string_lossy().into_owned();
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command
            .args(["sqlite_kill_test_child_writer", "--nocapture"])
            .env("SQLITE_KILL_TEST_ROLE", role)
            .env("SQLITE_KILL_TEST_DB", &db)
            .env("SQLITE_KILL_TEST_READY", &ready);
        if init_barrier {
            command.env("SQLITE_KILL_TEST_INIT", "1");
        }
        if let Some((call, after)) = batch_barrier {
            command
                .env("SQLITE_KILL_TEST_BARRIER_CALL", call.to_string())
                .env("SQLITE_KILL_TEST_BARRIER_AFTER", after.to_string());
        }

        let mut child = command.spawn().unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        while !std::path::Path::new(&ready).exists() {
            assert!(
                std::time::Instant::now() < deadline,
                "child never became ready"
            );
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        child.kill().unwrap();
        child.wait().unwrap();
        let storage = SqliteStorage::open(db_path(&dir), &["records"]).unwrap();
        (dir, storage)
    }

    #[cfg(unix)]
    #[test]
    fn sigkill_during_initialization_recovers_as_fresh() {
        let (_dir, storage) = run_kill_test("init", true, None);
        storage.set("records", b"post-recovery", b"ok").unwrap();
        assert_eq!(
            storage.get("records", b"post-recovery").unwrap(),
            Some(b"ok".to_vec())
        );
    }

    #[cfg(unix)]
    #[test]
    fn sigkill_after_full_sync_commit_preserves_every_commit() {
        let (_dir, storage) = run_kill_test("full_sync", false, None);
        assert_eq!(
            storage.get("records", b"before-boundary").unwrap(),
            Some(b"1".to_vec()),
            "FullSync: a committed write must survive SIGKILL"
        );
    }

    #[cfg(unix)]
    #[test]
    fn sigkill_after_boundary_preserves_the_boundary_prefix() {
        let (_dir, storage) = run_kill_test("wal_no_sync", false, None);
        assert_eq!(
            storage.get("records", b"before-boundary").unwrap(),
            Some(b"1".to_vec()),
            "WalNoSync: writes before flush_write_boundary must survive SIGKILL"
        );
    }

    #[cfg(unix)]
    #[test]
    fn sigkill_mid_batch_preserves_whole_generations_only() {
        let (_dir, storage) = run_kill_test("torn_batch", false, Some((2, 4)));
        let mut generation_counts = std::collections::BTreeMap::<u8, u32>::new();
        storage
            .scan_prefix("records", b"gen", &mut |key, _value| {
                *generation_counts.entry(key[3]).or_insert(0) += 1;
                Ok(())
            })
            .unwrap();
        assert_eq!(
            generation_counts.get(&0),
            Some(&8),
            "generation 0 must survive whole; counts: {generation_counts:?}"
        );
        assert_eq!(
            generation_counts.get(&1),
            None,
            "generation 1 was mid-transaction and must be absent; counts: {generation_counts:?}"
        );
    }
}
