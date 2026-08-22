//! SQLite implementation of Groove's async ordered key/value storage seam.
//!
//! This crate deliberately owns no Jazz query, sync, or permission semantics.
//! It maps the portable [`groove::storage::OrderedKvStorage`] contract onto one
//! SQLite WAL file.  The same logical contract is the boundary a future
//! Cloudflare Durable Objects adapter must implement; this native adapter does
//! not claim to run in a Durable Object.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use groove::storage::{
    Error, KeyValue, OrderedKvStorage, OwnedWriteOperation, ReopenableStorage, StorageCursor,
    StorageFactory, StorageFuture, StorageScan, Value, apply_storage_delta,
};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};

const FORMAT: &[u8] = b"jazz-groove-ordered-kv";
const FORMAT_VERSION: i64 = 1;
const BUSY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// Local persistence policy for the SQLite WAL.
///
/// `WalNoSync` preserves SQLite atomicity and survives process crashes, but may
/// lose the most recent OS-buffered commits on power loss. `FullSync` syncs
/// each commit and is appropriate for callers that require that stronger
/// boundary.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Durability {
    FullSync,
    #[default]
    WalNoSync,
}

impl Durability {
    fn synchronous_pragma(self) -> &'static str {
        match self {
            Self::FullSync => "FULL",
            Self::WalNoSync => "NORMAL",
        }
    }
}

/// One thread-affine SQLite ordered-KV store.
///
/// The storage is executor-local like the rest of the async Groove seam. A
/// relay owns exactly one instance per persistence scope; callers must not
/// treat clones or separate processes as a multi-writer API.
pub struct SqliteStorage {
    path: PathBuf,
    durability: Durability,
    column_families: RefCell<BTreeMap<String, i64>>,
    connection: RefCell<Option<Connection>>,
    write_flush_cadence: RefCell<Option<WriteFlushCadence>>,
}

#[derive(Clone, Copy, Debug)]
struct WriteFlushCadence {
    every: usize,
    pending: usize,
}

struct SqliteCursor {
    values: std::vec::IntoIter<KeyValue>,
}

impl SqliteCursor {
    fn new(values: Vec<KeyValue>) -> Self {
        Self {
            values: values.into_iter(),
        }
    }
}

impl StorageCursor for SqliteCursor {
    fn next_batch(&mut self) -> StorageFuture<'_, Result<Option<Vec<KeyValue>>, Error>> {
        Box::pin(async move {
            let batch = self.values.by_ref().take(256).collect::<Vec<_>>();
            Ok((!batch.is_empty()).then_some(batch))
        })
    }
}

/// Opens SQLite stores for generic Jazz persistent-client shells.
#[derive(Clone, Copy, Debug, Default)]
pub struct SqliteStorageFactory;

impl StorageFactory for SqliteStorageFactory {
    fn open(
        &self,
        path: PathBuf,
        column_families: Vec<String>,
    ) -> StorageFuture<'_, Result<groove::storage::BoxedStorage, Error>> {
        Box::pin(async move {
            let refs = column_families
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>();
            Ok(groove::storage::BoxedStorage::new(SqliteStorage::open(
                path, &refs,
            )?))
        })
    }
}

impl SqliteStorage {
    pub fn open(path: impl AsRef<Path>, column_families: &[&str]) -> Result<Self, Error> {
        Self::open_with_durability(path, column_families, Durability::default())
    }

    pub fn open_with_durability(
        path: impl AsRef<Path>,
        column_families: &[&str],
        durability: Durability,
    ) -> Result<Self, Error> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent).map_err(backend)?;
        }
        let mut connection = Connection::open(&path).map_err(backend)?;
        connection.busy_timeout(BUSY_TIMEOUT).map_err(backend)?;
        let mode: String = connection
            .pragma_update_and_check(None, "journal_mode", "WAL", |row| row.get(0))
            .map_err(backend)?;
        if !mode.eq_ignore_ascii_case("wal") {
            return Err(Error::InvalidStorageLayout(format!(
                "sqlite journal mode is {mode:?}, expected WAL"
            )));
        }
        connection
            .pragma_update(None, "synchronous", durability.synchronous_pragma())
            .map_err(backend)?;
        connection
            .pragma_update(None, "foreign_keys", "ON")
            .map_err(backend)?;

        let objects: i64 = connection
            .query_row("SELECT COUNT(*) FROM sqlite_master", [], |row| row.get(0))
            .map_err(backend)?;
        if objects == 0 {
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
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(backend)?;
        transaction
            .execute_batch(
                "CREATE TABLE meta (key TEXT PRIMARY KEY, value BLOB NOT NULL) STRICT;\
                 CREATE TABLE column_families (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE) STRICT;\
                 CREATE TABLE kv (cf INTEGER NOT NULL, k BLOB NOT NULL, v BLOB NOT NULL, \
                    PRIMARY KEY (cf, k)) WITHOUT ROWID, STRICT;",
            )
            .map_err(backend)?;
        transaction
            .execute(
                "INSERT INTO meta (key, value) VALUES ('format', ?1), ('format_version', ?2)",
                params![FORMAT, FORMAT_VERSION.to_be_bytes().to_vec()],
            )
            .map_err(backend)?;
        transaction.commit().map_err(backend)
    }

    fn validate_schema(connection: &Connection) -> Result<(), Error> {
        let mut statement = connection
            .prepare(
                "SELECT type, name FROM sqlite_master \
                 WHERE name NOT LIKE 'sqlite_%' ORDER BY name",
            )
            .map_err(backend)?;
        let objects = statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(backend)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(backend)?;
        let expected = ["column_families", "kv", "meta"];
        if objects.len() != expected.len()
            || objects
                .iter()
                .zip(expected)
                .any(|((kind, name), expected)| kind != "table" || name != expected)
        {
            return Err(Error::InvalidStorageLayout(
                "sqlite schema does not match jazz ordered-kv v1".to_owned(),
            ));
        }
        let format = connection
            .query_row("SELECT value FROM meta WHERE key = 'format'", [], |row| {
                row.get::<_, Vec<u8>>(0)
            })
            .optional()
            .map_err(backend)?
            .ok_or_else(|| {
                Error::InvalidStorageLayout("missing sqlite format marker".to_owned())
            })?;
        let version = connection
            .query_row(
                "SELECT value FROM meta WHERE key = 'format_version'",
                [],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .optional()
            .map_err(backend)?
            .ok_or_else(|| {
                Error::InvalidStorageLayout("missing sqlite format version".to_owned())
            })?;
        if format != FORMAT || version.as_slice() != FORMAT_VERSION.to_be_bytes() {
            return Err(Error::InvalidStorageLayout(
                "unsupported sqlite ordered-kv format".to_owned(),
            ));
        }
        Ok(())
    }

    fn with_connection<T>(
        &self,
        operation: impl FnOnce(&Connection) -> Result<T, Error>,
    ) -> Result<T, Error> {
        let borrowed = self.connection.borrow();
        operation(borrowed.as_ref().ok_or_else(closed)?)
    }

    fn with_connection_mut<T>(
        &self,
        operation: impl FnOnce(&mut Connection) -> Result<T, Error>,
    ) -> Result<T, Error> {
        let mut borrowed = self.connection.borrow_mut();
        operation(borrowed.as_mut().ok_or_else(closed)?)
    }

    fn cf_id(&self, name: &str) -> Result<i64, Error> {
        self.column_families
            .borrow()
            .get(name)
            .copied()
            .ok_or_else(|| Error::ColumnFamilyNotFound(name.to_owned()))
    }

    fn intern_column_families(&self, names: &[&str]) -> Result<(), Error> {
        self.with_connection(|connection| {
            let transaction = connection.unchecked_transaction().map_err(backend)?;
            for name in names {
                transaction
                    .execute(
                        "INSERT OR IGNORE INTO column_families (name) VALUES (?1)",
                        [name],
                    )
                    .map_err(backend)?;
            }
            transaction.commit().map_err(backend)?;
            Ok(())
        })?;
        let discovered = self.with_connection(|connection| {
            let mut statement = connection
                .prepare("SELECT name, id FROM column_families")
                .map_err(backend)?;
            statement
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
                })
                .map_err(backend)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(backend)
        })?;
        *self.column_families.borrow_mut() = discovered.into_iter().collect();
        Ok(())
    }

    fn scan(
        &self,
        cf: String,
        start: Vec<u8>,
        end: Option<Vec<u8>>,
        reverse: bool,
    ) -> Result<Vec<KeyValue>, Error> {
        let cf = self.cf_id(&cf)?;
        self.with_connection(|connection| {
            let order = if reverse { "DESC" } else { "ASC" };
            let sql = if end.is_some() {
                format!(
                    "SELECT k, v FROM kv WHERE cf = ?1 AND k >= ?2 AND k < ?3 ORDER BY k {order}"
                )
            } else {
                format!("SELECT k, v FROM kv WHERE cf = ?1 AND k >= ?2 ORDER BY k {order}")
            };
            let mut statement = connection.prepare(&sql).map_err(backend)?;
            match end.as_deref() {
                Some(end) => statement
                    .query_map(params![cf, start, end], |row| {
                        Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?))
                    })
                    .map_err(backend)?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(backend),
                None => statement
                    .query_map(params![cf, start], |row| {
                        Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?))
                    })
                    .map_err(backend)?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(backend),
            }
        })
    }
}

impl ReopenableStorage for SqliteStorage {
    fn reopen(self, column_families: Vec<String>) -> StorageFuture<'static, Result<Self, Error>> {
        Box::pin(async move {
            if column_families
                .iter()
                .all(|name| self.column_families.borrow().contains_key(name))
            {
                return Ok(self);
            }
            let path = self.path.clone();
            let durability = self.durability;
            drop(self);
            let refs = column_families
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>();
            Self::open_with_durability(path, &refs, durability)
        })
    }
}

impl OrderedKvStorage for SqliteStorage {
    fn get(&self, cf: String, key: Vec<u8>) -> StorageFuture<'_, Result<Option<Value>, Error>> {
        Box::pin(async move {
            let cf = self.cf_id(&cf)?;
            self.with_connection(|connection| {
                connection
                    .query_row(
                        "SELECT v FROM kv WHERE cf = ?1 AND k = ?2",
                        params![cf, key],
                        |row| row.get(0),
                    )
                    .optional()
                    .map_err(backend)
            })
        })
    }

    fn set(
        &self,
        cf: String,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> StorageFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            let cf = self.cf_id(&cf)?;
            self.with_connection(|connection| {
                connection
                    .execute(
                        "INSERT OR REPLACE INTO kv (cf, k, v) VALUES (?1, ?2, ?3)",
                        params![cf, key, value],
                    )
                    .map_err(backend)?;
                Ok(())
            })
        })
    }

    fn delete(&self, cf: String, key: Vec<u8>) -> StorageFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            let cf = self.cf_id(&cf)?;
            self.with_connection(|connection| {
                connection
                    .execute("DELETE FROM kv WHERE cf = ?1 AND k = ?2", params![cf, key])
                    .map_err(backend)?;
                Ok(())
            })
        })
    }

    fn close(&self) -> StorageFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            let connection = self.connection.borrow_mut().take().ok_or_else(closed)?;
            // A passive checkpoint is deliberately best-effort: another read
            // connection can keep WAL frames live without invalidating this
            // durable database. Dropping the connection is the close boundary.
            let _ = connection.execute_batch("PRAGMA wal_checkpoint(PASSIVE)");
            Ok(())
        })
    }

    fn set_write_flush_cadence(&self, every: usize) -> StorageFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            if every == 0 {
                return Err(Error::InvalidStorageLayout(
                    "write flush cadence must be non-zero".to_owned(),
                ));
            }
            *self.write_flush_cadence.borrow_mut() = Some(WriteFlushCadence { every, pending: 0 });
            Ok(())
        })
    }

    fn flush_write_boundary(&self) -> StorageFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            self.with_connection(|connection| {
                connection
                    .execute_batch("PRAGMA wal_checkpoint(PASSIVE)")
                    .map_err(backend)?;
                Ok(())
            })?;
            if let Some(cadence) = self.write_flush_cadence.borrow_mut().as_mut() {
                cadence.pending = 0;
            }
            Ok(())
        })
    }

    fn approximate_class_bytes(&self, cf: String) -> StorageFuture<'_, Result<Option<u64>, Error>> {
        Box::pin(async move {
            let cf = self.cf_id(&cf)?;
            self.with_connection(|connection| {
                connection
                    .query_row(
                        "SELECT COALESCE(SUM(length(k) + length(v)), 0) FROM kv WHERE cf = ?1",
                        [cf],
                        |row| row.get::<_, i64>(0),
                    )
                    .map(|bytes| Some(bytes.max(0) as u64))
                    .map_err(backend)
            })
        })
    }

    fn scan_range(
        &self,
        cf: String,
        start: Vec<u8>,
        end: Vec<u8>,
    ) -> StorageFuture<'_, Result<StorageScan<'_>, Error>> {
        Box::pin(async move {
            let values = self.scan(cf, start, Some(end), false)?;
            Ok(Box::new(SqliteCursor::new(values)) as StorageScan<'_>)
        })
    }

    fn scan_prefix(
        &self,
        cf: String,
        prefix: Vec<u8>,
    ) -> StorageFuture<'_, Result<StorageScan<'_>, Error>> {
        Box::pin(async move {
            let upper = prefix_upper_bound(&prefix);
            let mut values = self.scan(cf, prefix.clone(), upper, false)?;
            if prefix_upper_bound(&prefix).is_none() {
                values.retain(|(key, _)| key.starts_with(&prefix));
            }
            Ok(Box::new(SqliteCursor::new(values)) as StorageScan<'_>)
        })
    }

    fn scan_prefix_reverse(
        &self,
        cf: String,
        prefix: Vec<u8>,
    ) -> StorageFuture<'_, Result<StorageScan<'_>, Error>> {
        Box::pin(async move {
            let upper = prefix_upper_bound(&prefix);
            let mut values = self.scan(cf, prefix.clone(), upper, true)?;
            if prefix_upper_bound(&prefix).is_none() {
                values.retain(|(key, _)| key.starts_with(&prefix));
            }
            Ok(Box::new(SqliteCursor::new(values)) as StorageScan<'_>)
        })
    }

    fn last_with_prefix(
        &self,
        cf: String,
        prefix: Vec<u8>,
    ) -> StorageFuture<'_, Result<Option<KeyValue>, Error>> {
        Box::pin(async move {
            let mut scan = self.scan_prefix_reverse(cf, prefix).await?;
            Ok(scan
                .next_batch()
                .await?
                .and_then(|batch| batch.into_iter().next()))
        })
    }

    fn last_with_prefix_before_or_at(
        &self,
        cf: String,
        prefix: Vec<u8>,
        upper: Vec<u8>,
    ) -> StorageFuture<'_, Result<Option<KeyValue>, Error>> {
        Box::pin(async move {
            let cf_id = self.cf_id(&cf)?;
            self.with_connection_mut(|connection| {
                let mut statement = connection
                    .prepare(
                        "SELECT k, v FROM kv WHERE cf = ?1 AND k >= ?2 AND k <= ?3 ORDER BY k DESC",
                    )
                    .map_err(backend)?;
                let mut rows = statement
                    .query(params![cf_id, prefix.clone(), upper])
                    .map_err(backend)?;
                while let Some(row) = rows.next().map_err(backend)? {
                    let key: Vec<u8> = row.get(0).map_err(backend)?;
                    if key.starts_with(&prefix) {
                        return Ok(Some((key, row.get(1).map_err(backend)?)));
                    }
                }
                Ok(None)
            })
        })
    }

    fn write_many(
        &self,
        operations: Vec<OwnedWriteOperation>,
    ) -> StorageFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            // Resolve every family before opening a write transaction. This
            // makes an unknown family a no-op batch, matching the atomic
            // contract of the in-memory and RocksDB adapters.
            let operations = operations
                .into_iter()
                .map(|operation| {
                    let name = match &operation {
                        OwnedWriteOperation::Set { cf, .. }
                        | OwnedWriteOperation::Delete { cf, .. }
                        | OwnedWriteOperation::Delta { cf, .. } => cf,
                    };
                    Ok((self.cf_id(name)?, operation))
                })
                .collect::<Result<Vec<_>, Error>>()?;
            self.with_connection_mut(|connection| {
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(backend)?;
                for (cf, operation) in operations {
                    match operation {
                        OwnedWriteOperation::Set { key, value, .. } => {
                            transaction
                                .execute(
                                    "INSERT OR REPLACE INTO kv (cf, k, v) VALUES (?1, ?2, ?3)",
                                    params![cf, key, value],
                                )
                                .map_err(backend)?;
                        }
                        OwnedWriteOperation::Delete { key, .. } => {
                            transaction
                                .execute(
                                    "DELETE FROM kv WHERE cf = ?1 AND k = ?2",
                                    params![cf, key],
                                )
                                .map_err(backend)?;
                        }
                        OwnedWriteOperation::Delta { key, delta, .. } => {
                            let existing = transaction
                                .query_row(
                                    "SELECT v FROM kv WHERE cf = ?1 AND k = ?2",
                                    params![cf, &key],
                                    |row| row.get::<_, Vec<u8>>(0),
                                )
                                .optional()
                                .map_err(backend)?;
                            let merged =
                                apply_storage_delta(existing.as_deref(), &delta.encode()?)?;
                            transaction
                                .execute(
                                    "INSERT OR REPLACE INTO kv (cf, k, v) VALUES (?1, ?2, ?3)",
                                    params![cf, key, merged],
                                )
                                .map_err(backend)?;
                        }
                    }
                }
                transaction.commit().map_err(backend)
            })?;
            let should_flush =
                self.write_flush_cadence
                    .borrow_mut()
                    .as_mut()
                    .is_some_and(|cadence| {
                        cadence.pending += 1;
                        if cadence.pending == cadence.every {
                            cadence.pending = 0;
                            true
                        } else {
                            false
                        }
                    });
            if should_flush {
                self.flush_write_boundary().await?;
            }
            Ok(())
        })
    }

    fn column_family_names(&self) -> Option<Vec<String>> {
        Some(self.column_families.borrow().keys().cloned().collect())
    }
}

fn closed() -> Error {
    Error::Backend {
        backend: "sqlite",
        message: "storage is closed".to_owned(),
    }
}

fn backend(error: impl std::fmt::Display) -> Error {
    Error::Backend {
        backend: "sqlite",
        message: error.to_string(),
    }
}

fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut result = prefix.to_vec();
    while let Some(last) = result.pop() {
        if last != u8::MAX {
            result.push(last + 1);
            return Some(result);
        }
    }
    None
}
