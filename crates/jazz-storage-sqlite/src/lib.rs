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
    Error, KeyValue, OrderedKvStorage, OwnedWriteOperation, ReopenableStorage, ScanBounds,
    ScanDirection, ScanRequest, StorageCodecProfile, StorageCursor, StorageEpochManifest,
    StorageFactory, StorageFuture, StorageScan, Value, WriteManyOutcome,
    validate_physical_storage_names,
};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};

const FORMAT: &[u8] = b"jazz-groove-ordered-kv";
const FORMAT_VERSION: i64 = 1;
/// `JAZZ` in the SQLite application-id header field. This identifies the
/// database before its tables or blobs are interpreted.
const APPLICATION_ID: i64 = 0x4a41_5a5a;
/// SQLite's schema-facing version for the ordered-KV physical layout.
const USER_VERSION: i64 = 1;
/// An immutable identity for the v1 DDL, stored beside the format marker so a
/// same-shaped foreign database cannot be silently adopted.
const DDL_ID: &[u8] = b"jazz-groove-ordered-kv-ddl-v1";
const EPOCH_MANIFEST_KEY: &str = "epoch_manifest";
const META_DDL: &str = "CREATE TABLE meta (key TEXT PRIMARY KEY, value BLOB NOT NULL) STRICT";
const COLUMN_FAMILIES_DDL: &str =
    "CREATE TABLE column_families (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE) STRICT";
const KV_DDL: &str = "CREATE TABLE kv (cf INTEGER NOT NULL, k BLOB NOT NULL, v BLOB NOT NULL, PRIMARY KEY (cf, k)) WITHOUT ROWID, STRICT";
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
/// relay normally owns one instance per persistence scope. Independent handles
/// are supported specifically for the atomic conditional primitives; this does
/// not promote the rest of the interface to a general multi-writer API.
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
        codec_profile: StorageCodecProfile,
    ) -> StorageFuture<'_, Result<groove::storage::BoxedStorage, Error>> {
        Box::pin(async move {
            let refs = column_families
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>();
            Ok(groove::storage::BoxedStorage::new(
                SqliteStorage::open_with_durability_and_codec_profile(
                    path,
                    &refs,
                    Durability::default(),
                    &codec_profile,
                )?,
            ))
        })
    }
}

impl SqliteStorage {
    pub fn open(path: impl AsRef<Path>, column_families: &[&str]) -> Result<Self, Error> {
        Self::open_with_durability_and_codec_profile(
            path,
            column_families,
            Durability::default(),
            &StorageCodecProfile::groove_epoch_1(),
        )
    }

    pub fn open_with_durability(
        path: impl AsRef<Path>,
        column_families: &[&str],
        durability: Durability,
    ) -> Result<Self, Error> {
        Self::open_with_durability_and_codec_profile(
            path,
            column_families,
            durability,
            &StorageCodecProfile::groove_epoch_1(),
        )
    }

    /// Open with the caller's closed persistent-codec profile. The adapter
    /// records and compares opaque IDs but does not interpret Jazz semantics.
    pub fn open_with_durability_and_codec_profile(
        path: impl AsRef<Path>,
        column_families: &[&str],
        durability: Durability,
        codec_profile: &StorageCodecProfile,
    ) -> Result<Self, Error> {
        validate_physical_storage_names(column_families)?;
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent).map_err(backend)?;
        }
        let mut connection = Connection::open(&path).map_err(backend)?;
        connection.busy_timeout(BUSY_TIMEOUT).map_err(backend)?;
        let objects: i64 = connection
            .query_row("SELECT COUNT(*) FROM sqlite_master", [], |row| row.get(0))
            .map_err(backend)?;
        if objects == 0 {
            // A table-free SQLite file is not necessarily ours: another
            // application can already have claimed its physical header. Only
            // the neutral SQLite header is a fresh root we may adopt.
            Self::validate_neutral_empty_header(&connection)?;
            Self::create_schema(&mut connection, codec_profile)?;
        } else {
            // Validation is deliberately before WAL/synchronous setup: an
            // incompatible store must fail before this adapter changes it.
            Self::validate_schema(&connection, codec_profile)?;
        }
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

    fn validate_neutral_empty_header(connection: &Connection) -> Result<(), Error> {
        let application_id: i64 = connection
            .pragma_query_value(None, "application_id", |row| row.get(0))
            .map_err(backend)?;
        let user_version: i64 = connection
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .map_err(backend)?;
        if application_id != 0 || user_version != 0 {
            return Err(Error::InvalidStorageLayout(
                "table-free sqlite root has a non-neutral application_id or user_version"
                    .to_owned(),
            ));
        }
        Ok(())
    }

    fn create_schema(
        connection: &mut Connection,
        codec_profile: &StorageCodecProfile,
    ) -> Result<(), Error> {
        let manifest = sqlite_manifest(codec_profile)?.encode()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(backend)?;
        transaction
            .execute_batch(&format!("{META_DDL};{COLUMN_FAMILIES_DDL};{KV_DDL};"))
            .map_err(backend)?;
        transaction
            .pragma_update(None, "application_id", APPLICATION_ID)
            .map_err(backend)?;
        transaction
            .pragma_update(None, "user_version", USER_VERSION)
            .map_err(backend)?;
        transaction
            .execute(
                "INSERT INTO meta (key, value) VALUES \
                 ('format', ?1), ('format_version', ?2), ('ddl_id', ?3), ('epoch_manifest', ?4)",
                params![
                    FORMAT,
                    FORMAT_VERSION.to_be_bytes().to_vec(),
                    DDL_ID,
                    manifest
                ],
            )
            .map_err(backend)?;
        transaction.commit().map_err(backend)
    }

    fn validate_schema(
        connection: &Connection,
        codec_profile: &StorageCodecProfile,
    ) -> Result<(), Error> {
        let application_id: i64 = connection
            .pragma_query_value(None, "application_id", |row| row.get(0))
            .map_err(backend)?;
        let user_version: i64 = connection
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .map_err(backend)?;
        if application_id != APPLICATION_ID || user_version != USER_VERSION {
            return Err(Error::InvalidStorageLayout(
                "unsupported sqlite application_id or user_version".to_owned(),
            ));
        }
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
        validate_table_columns(
            connection,
            "meta",
            &[("key", "TEXT", 1), ("value", "BLOB", 0)],
        )?;
        validate_table_columns(
            connection,
            "column_families",
            &[("id", "INTEGER", 1), ("name", "TEXT", 0)],
        )?;
        validate_table_ddl(connection, "meta", META_DDL)?;
        validate_table_ddl(connection, "column_families", COLUMN_FAMILIES_DDL)?;
        validate_table_ddl(connection, "kv", KV_DDL)?;
        validate_table_columns(
            connection,
            "kv",
            &[("cf", "INTEGER", 1), ("k", "BLOB", 2), ("v", "BLOB", 0)],
        )?;
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
        let ddl_id = connection
            .query_row("SELECT value FROM meta WHERE key = 'ddl_id'", [], |row| {
                row.get::<_, Vec<u8>>(0)
            })
            .optional()
            .map_err(backend)?
            .ok_or_else(|| Error::InvalidStorageLayout("missing sqlite DDL identity".to_owned()))?;
        let epoch_manifest = connection
            .query_row(
                "SELECT value FROM meta WHERE key = ?1",
                [EPOCH_MANIFEST_KEY],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .optional()
            .map_err(backend)?
            .ok_or_else(|| {
                Error::InvalidStorageLayout("missing sqlite epoch manifest".to_owned())
            })?;
        if format != FORMAT
            || version.as_slice() != FORMAT_VERSION.to_be_bytes()
            || ddl_id != DDL_ID
        {
            return Err(Error::InvalidStorageLayout(
                "unsupported sqlite ordered-kv format".to_owned(),
            ));
        }
        sqlite_manifest(codec_profile)?.admit_existing(&epoch_manifest)?;
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
        self.validate_discovered_column_families()?;
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
        let discovered = self.discover_column_families()?;
        validate_physical_storage_names(discovered.iter().map(|(name, _)| name))?;
        *self.column_families.borrow_mut() = discovered.into_iter().collect();
        Ok(())
    }

    fn validate_discovered_column_families(&self) -> Result<(), Error> {
        let discovered = self.discover_column_families()?;
        validate_physical_storage_names(discovered.iter().map(|(name, _)| name))
    }

    fn discover_column_families(&self) -> Result<Vec<(String, i64)>, Error> {
        self.with_connection(|connection| {
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
        })
    }

    fn scan_rows(
        &self,
        cf: String,
        start: Vec<u8>,
        end: Option<Vec<u8>>,
        reverse: bool,
        max_items: Option<usize>,
    ) -> Result<Vec<KeyValue>, Error> {
        let cf = self.cf_id(&cf)?;
        if max_items == Some(0) {
            return Ok(Vec::new());
        }
        self.with_connection(|connection| {
            let order = if reverse { "DESC" } else { "ASC" };
            let limit = max_items
                .map(|limit| i64::try_from(limit).unwrap_or(i64::MAX))
                .unwrap_or(-1);
            let sql = if end.is_some() {
                format!(
                    "SELECT k, v FROM kv WHERE cf = ?1 AND k >= ?2 AND k < ?3 ORDER BY k {order} LIMIT ?4"
                )
            } else {
                format!(
                    "SELECT k, v FROM kv WHERE cf = ?1 AND k >= ?2 ORDER BY k {order} LIMIT ?3"
                )
            };
            let mut statement = connection.prepare(&sql).map_err(backend)?;
            match end.as_deref() {
                Some(end) => statement
                    .query_map(params![cf, start, end, limit], |row| {
                        Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?))
                    })
                    .map_err(backend)?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(backend),
                None => statement
                    .query_map(params![cf, start, limit], |row| {
                        Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?))
                    })
                    .map_err(backend)?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(backend),
            }
        })
    }
}

fn sqlite_manifest(codec_profile: &StorageCodecProfile) -> Result<StorageEpochManifest, Error> {
    StorageEpochManifest::epoch_1_with_codec_profile(
        "sqlite",
        1,
        BTreeMap::from([
            (
                "application-id".to_owned(),
                APPLICATION_ID.to_be_bytes().to_vec(),
            ),
            ("ddl-id".to_owned(), DDL_ID.to_vec()),
            ("key-order".to_owned(), b"unsigned-lexicographic".to_vec()),
        ]),
        codec_profile,
    )
}

fn validate_table_columns(
    connection: &Connection,
    table: &str,
    expected: &[(&str, &str, i64)],
) -> Result<(), Error> {
    // `table` is an internal literal, never caller data. PRAGMA does not bind
    // identifiers, so keeping it non-public avoids an SQL identifier seam.
    let mut statement = connection
        .prepare(&format!("PRAGMA table_info({table})"))
        .map_err(backend)?;
    let actual = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(5)?,
            ))
        })
        .map_err(backend)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(backend)?;
    if actual.len() != expected.len()
        || actual
            .iter()
            .zip(expected)
            .any(|((name, kind, pk), expected)| {
                name != expected.0 || !kind.eq_ignore_ascii_case(expected.1) || *pk != expected.2
            })
    {
        return Err(Error::InvalidStorageLayout(format!(
            "sqlite table {table} columns do not match jazz ordered-kv v1"
        )));
    }
    Ok(())
}

fn validate_table_ddl(connection: &Connection, table: &str, expected: &str) -> Result<(), Error> {
    let actual = connection
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?1",
            [table],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(backend)?;
    if actual.as_deref() != Some(expected) {
        return Err(Error::InvalidStorageLayout(format!(
            "sqlite table {table} DDL does not match jazz ordered-kv v1"
        )));
    }
    Ok(())
}

impl ReopenableStorage for SqliteStorage {
    fn reopen(self, column_families: Vec<String>) -> StorageFuture<'static, Result<Self, Error>> {
        Box::pin(async move {
            validate_physical_storage_names(&column_families)?;
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

    fn put_if_absent(
        &self,
        cf: String,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> StorageFuture<'_, Result<Option<Value>, Error>> {
        Box::pin(async move {
            let cf = self.cf_id(&cf)?;
            self.with_connection_mut(|connection| {
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(backend)?;
                let existing = transaction
                    .query_row(
                        "SELECT v FROM kv WHERE cf = ?1 AND k = ?2",
                        params![cf, &key],
                        |row| row.get::<_, Vec<u8>>(0),
                    )
                    .optional()
                    .map_err(backend)?;
                if existing.is_none() {
                    transaction
                        .execute(
                            "INSERT INTO kv (cf, k, v) VALUES (?1, ?2, ?3)",
                            params![cf, key, value],
                        )
                        .map_err(backend)?;
                }
                transaction.commit().map_err(backend)?;
                Ok(existing)
            })
        })
    }

    fn compare_and_delete(
        &self,
        cf: String,
        key: Vec<u8>,
        expected: Vec<u8>,
    ) -> StorageFuture<'_, Result<bool, Error>> {
        Box::pin(async move {
            let cf = self.cf_id(&cf)?;
            self.with_connection_mut(|connection| {
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(backend)?;
                let removed = transaction
                    .execute(
                        "DELETE FROM kv WHERE cf = ?1 AND k = ?2 AND v = ?3",
                        params![cf, key, expected],
                    )
                    .map_err(backend)?
                    != 0;
                transaction.commit().map_err(backend)?;
                Ok(removed)
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

    fn scan(&self, request: ScanRequest) -> StorageFuture<'_, Result<StorageScan<'_>, Error>> {
        Box::pin(async move {
            let ScanRequest {
                cf,
                bounds,
                direction,
                max_items,
            } = request;
            let (start, end) = match bounds {
                ScanBounds::Prefix(prefix) => {
                    let end = groove::storage::prefix_successor(&prefix);
                    (prefix, end)
                }
                ScanBounds::Range { start, end } => (start, Some(end)),
            };
            let values = self.scan_rows(
                cf,
                start,
                end,
                direction == ScanDirection::Reverse,
                max_items,
            )?;
            Ok(Box::new(SqliteCursor::new(values)) as StorageScan<'_>)
        })
    }

    fn last_with_prefix(
        &self,
        cf: String,
        prefix: Vec<u8>,
    ) -> StorageFuture<'_, Result<Option<KeyValue>, Error>> {
        Box::pin(async move {
            let mut scan = self
                .scan(ScanRequest::prefix(cf, prefix).reversed().with_max_items(1))
                .await?;
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
                        | OwnedWriteOperation::Delete { cf, .. } => cf,
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

    fn write_many_outcome(
        &self,
        operations: Vec<OwnedWriteOperation>,
    ) -> StorageFuture<'_, WriteManyOutcome> {
        Box::pin(async move {
            for operation in &operations {
                let cf = match operation {
                    OwnedWriteOperation::Set { cf, .. }
                    | OwnedWriteOperation::Delete { cf, .. } => cf,
                };
                if let Err(error) = self.cf_id(cf) {
                    return WriteManyOutcome::Uncommitted(error);
                }
            }
            match self.write_many(operations).await {
                Ok(()) => WriteManyOutcome::Committed,
                Err(error) => WriteManyOutcome::PossiblyCommitted(error),
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on;
    use groove::storage::ReopenableStorage;
    use std::sync::{Arc, Barrier};

    #[test]
    fn independent_handles_racing_put_if_absent_choose_one_winner() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("race.sqlite");
        drop(SqliteStorage::open(&path, &["records"]).unwrap());
        let barrier = Arc::new(Barrier::new(2));
        let handles = [b"first".to_vec(), b"second".to_vec()].map(|value| {
            let path = path.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let storage = SqliteStorage::open(path, &["records"]).unwrap();
                barrier.wait();
                block_on(storage.put_if_absent("records".into(), b"key".to_vec(), value)).unwrap()
            })
        });
        let outcomes = handles.map(|handle| handle.join().unwrap());
        assert_ne!(outcomes[0].is_none(), outcomes[1].is_none());
    }

    #[test]
    fn reopen_fast_path_rejects_invalid_existing_family_without_mutation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fast-path.sqlite");
        let invalid = "records\0evil";
        let storage = SqliteStorage::open(&path, &["records"]).unwrap();
        // Simulate a legacy/injected in-memory family catalogue: this used to
        // take the all-existing early return before physical-name validation.
        storage
            .column_families
            .borrow_mut()
            .insert(invalid.to_owned(), 99);
        assert!(block_on(storage.reopen(vec![invalid.to_owned()])).is_err());
        let connection = Connection::open(path).unwrap();
        let count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM column_families WHERE name = ?1",
                [invalid],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 0, "reopen must reject before inserting the family");
    }

    #[test]
    fn open_rejects_invalid_persisted_family_before_admission() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("invalid-existing.sqlite");
        drop(SqliteStorage::open(&path, &["records"]).unwrap());
        let invalid = "records\0evil";
        let connection = Connection::open(&path).unwrap();
        connection
            .execute("INSERT INTO column_families (name) VALUES (?1)", [invalid])
            .unwrap();
        drop(connection);

        assert!(SqliteStorage::open(&path, &["must-not-be-admitted"]).is_err());
        let connection = Connection::open(&path).unwrap();
        let count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM column_families WHERE name = ?1",
                ["must-not-be-admitted"],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            count, 0,
            "open must reject before admitting requested families"
        );
    }

    #[test]
    fn caller_selected_codec_profile_is_pinned_and_required_on_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("profile.sqlite");
        let profile = StorageCodecProfile::groove_epoch_1()
            .with_additional_codecs(["jazz.example-opaque.v1"])
            .unwrap();
        drop(
            SqliteStorage::open_with_durability_and_codec_profile(
                &path,
                &["records"],
                Durability::default(),
                &profile,
            )
            .unwrap(),
        );

        let connection = Connection::open(&path).unwrap();
        let bytes: Vec<u8> = connection
            .query_row(
                "SELECT value FROM meta WHERE key = 'epoch_manifest'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(bytes, sqlite_manifest(&profile).unwrap().encode().unwrap());
        drop(connection);

        SqliteStorage::open_with_durability_and_codec_profile(
            &path,
            &["records"],
            Durability::default(),
            &profile,
        )
        .unwrap();
        assert!(SqliteStorage::open(&path, &["records"]).is_err());
    }
}
