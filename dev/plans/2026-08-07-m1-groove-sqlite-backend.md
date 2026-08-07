# M1: Groove SQLite Storage Backend — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A production `SqliteStorage` backend in `crates/groove` implementing the ordered-KV contract over bundled rusqlite, conformance-equal to `MemoryStorage`, with verified durability boundaries — plus the `jazz` `sqlite` feature re-pointed onto it.

**Architecture:** One SQLite file per store (`journal_mode=WAL`), a single `kv (cf INTEGER, k BLOB, v BLOB, PRIMARY KEY(cf,k)) WITHOUT ROWID` table with interned column-family ids, a `meta` table carrying format identity and the durable-boundary counter. Thread-affine (`RefCell`), single connection, no async. `Durability` is lifted out of the RocksDB-gated module first so the backend can share it.

**Tech Stack:** Rust 2024, rusqlite 0.34 (`bundled`), thiserror 2, existing groove storage traits (`OrderedKvStorage`, `ReopenableStorage`), `apply_storage_delta` for `WriteOperation::Delta`.

**Spec:** `dev/RN_BINDING_REWRITE_DESIGN.md` §3, §8.1–8.3. This plan is M1 only; jazz-rn (M2) is a separate plan.

## Global Constraints

- Feature name is `sqlite` in both `groove` and `jazz`; `jazz`'s becomes `sqlite = ["groove/sqlite"]`.
- rusqlite pin: `0.34` with `features = ["bundled"]` (already the dev-dependency pin; keep both entries).
- `SqliteStorage::open` defaults to `Durability::WalNoSync`, mirroring `RocksDbStorage::open`'s documented default; `open_with_durability` opts into `FullSync`.
- Format identity: `meta` rows `format = 'jazz-groove-kv'`, `format_version = 1`. Any existing file that does not validate ⇒ `Error::InvalidStorageLayout` (never adopt an alien file).
- Corruption (`SQLITE_CORRUPT`/`SQLITE_NOTADB`) surfaces through a new transparent `Error::Sqlite(#[from] rusqlite::Error)` variant, feature-gated.
- All-`0xFF` prefixes have no finite upper bound: prefix scans must terminate by `starts_with`, never rely on an incremented bound alone.
- Storage-level tests are internal (`#[cfg(test)]` in `sqlite.rs`), matching `memory.rs`/`opfs.rs`; per `crates/jazz/TESTING_GUIDELINES.md` internal tests must say why — backend contract behavior is not observable through public jazz APIs.
- Node-level tests (Task 7) use public builders; no JSON-literal schemas/queries.
- Gates for this milestone: `cargo test -p groove` (default features), `cargo test -p groove --features sqlite`, `cargo check -p groove --no-default-features`, and after Task 7 `cargo test -p jazz --no-default-features --features test`. Landing tier additionally: `cargo test -p jazz`, `cargo test -p jazz-server`, `cargo check -p jazz-sim --benches`, `dev/benchmarks/smoke.sh` (storage touched).
- Commit messages follow repo style (`feat(groove): …`, `test(groove): …`, `chore(jazz): …`); no AI attribution anywhere.

---

### Task 1: Lift `Durability` out of the RocksDB-gated module

**Files:**

- Modify: `crates/groove/src/storage/rocksdb.rs:41-50` (remove enum definition; import instead)
- Modify: `crates/groove/src/storage/mod.rs:38-39` (define enum; adjust re-exports)

**Interfaces:**

- Produces: `groove::storage::Durability` exported unconditionally (variants `FullSync`, `WalNoSync`, `#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]`, `FullSync` is `#[default]`). Task 2+ consume it. `groove::storage::rocksdb_storage::Durability` keeps resolving (re-export) so existing imports don't break.

- [ ] **Step 1: Move the enum**

In `crates/groove/src/storage/mod.rs`, replace:

```rust
#[cfg(feature = "rocksdb")]
pub use rocksdb_storage::{Durability, RocksDbStorage};
```

with:

```rust
#[cfg(feature = "rocksdb")]
pub use rocksdb_storage::RocksDbStorage;

/// Local durability tier used for writes by file-backed storage backends.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Durability {
    /// Sync every write batch through the OS for the strongest local durability.
    #[default]
    FullSync,
    /// Keep WAL atomicity but do not fsync every commit, like SQLite WAL/NORMAL.
    WalNoSync,
}
```

In `crates/groove/src/storage/rocksdb.rs`, delete the `pub enum Durability { … }` block (lines 41-50, including its doc comment "RocksDB durability tier used for writes.") and add to the file's `use super::…` imports: `Durability`. Then add a compatibility re-export at the top of the module body:

```rust
pub use super::Durability;
```

- [ ] **Step 2: Fix any remaining path references**

Run: `grep -rn "rocksdb_storage::Durability" crates/ dev/ examples/ 2>/dev/null`
Expected: no hits needing changes (the re-export covers them); fix any that import via a removed path.

- [ ] **Step 3: Verify both feature configurations compile and pass**

Run: `cargo check -p groove --no-default-features && cargo test -p groove -j 8`
Expected: check passes (Durability now exists without rocksdb); full default-feature suite passes (includes `wal_no_sync_durability_mode_keeps_wal_writes_enabled` at `mod.rs:3104`).

- [ ] **Step 4: Commit**

```bash
git add crates/groove/src/storage/mod.rs crates/groove/src/storage/rocksdb.rs
git commit -m "refactor(groove): lift Durability out of the rocksdb-gated module"
```

---

### Task 2: `sqlite` feature, error variant, and validated open

**Files:**

- Modify: `crates/groove/Cargo.toml` (feature + optional dependency)
- Modify: `crates/groove/src/storage/mod.rs` (module wiring + `Error::Sqlite`)
- Create: `crates/groove/src/storage/sqlite.rs`

**Interfaces:**

- Consumes: `Durability` from Task 1; `Error`, `ColumnFamilyName`, `Key`, `Value`, `KeyValue`, `ScanVisitor`, `WriteOperation`, `apply_storage_delta` from `storage/mod.rs`.
- Produces: `groove::storage::SqliteStorage` with:
  - `pub fn open(path: impl AsRef<Path>, column_families: &[&str]) -> Result<Self, Error>` (WalNoSync)
  - `pub fn open_with_durability(path: impl AsRef<Path>, column_families: &[&str], durability: Durability) -> Result<Self, Error>`
  - `groove::storage::Error::Sqlite(rusqlite::Error)` (feature-gated, `#[from]`, transparent)

- [ ] **Step 1: Feature and dependency wiring**

In `crates/groove/Cargo.toml`, `[features]` section, add:

```toml
sqlite = ["dep:rusqlite"]
```

In `[dependencies]` (keep the existing `[dev-dependencies]` rusqlite entry — benches use it unconditionally):

```toml
rusqlite = { version = "0.34", features = ["bundled"], optional = true }
```

In `crates/groove/src/storage/mod.rs`, next to the rocksdb module wiring:

```rust
#[cfg(feature = "sqlite")]
#[path = "sqlite.rs"]
pub mod sqlite_storage;
```

and next to the `RocksDbStorage` re-export:

```rust
#[cfg(feature = "sqlite")]
pub use sqlite_storage::SqliteStorage;
```

In the `pub enum Error` block (`mod.rs`, after the `RocksDb` variant):

```rust
#[cfg(feature = "sqlite")]
#[error(transparent)]
Sqlite(#[from] ::rusqlite::Error),
```

- [ ] **Step 2: Write the failing open/validation tests**

Create `crates/groove/src/storage/sqlite.rs` containing only the test module for now (the struct comes in Step 4). These are internal storage-backend tests, like `memory.rs`/`opfs.rs`: backend contract behavior is not observable through public jazz APIs.

```rust
//! SQLite implementation of the ordered KV storage trait.
//!
//! One database file per store. A single `kv` table keyed on
//! `(interned column family id, key blob)` provides the ordered contract via
//! the composite primary key; `meta` carries format identity and the durable
//! boundary counter. Thread-affine like its siblings: one connection behind a
//! `RefCell`, no pool, no async.

#[cfg(test)]
mod tests {
    // Internal tests, matching memory.rs/opfs.rs: ordered-KV backend behavior
    // (format validation, scan order, durability boundaries) is not observable
    // through public jazz APIs.
    //
    // Lifecycle here uses drop(), not close(): the OrderedKvStorage impl (and
    // with it the close() override) arrives in Tasks 3–4. Connection drop
    // closes cleanly, and SQLite auto-checkpoints WAL on last-connection
    // close, so a second connection sees committed meta rows.
    use super::super::{Durability, Error};
    use super::SqliteStorage;

    fn db_path(dir: &tempfile::TempDir) -> std::path::PathBuf {
        dir.path().join("store.db")
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
    fn open_rejects_alien_sqlite_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = db_path(&dir);
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute("CREATE TABLE not_ours (x INTEGER)", []).unwrap();
        drop(conn);

        let error = SqliteStorage::open(&path, &["records"]).unwrap_err();
        assert!(
            matches!(&error, Error::InvalidStorageLayout(message) if message.contains("meta")),
            "alien sqlite file must be rejected as a layout error, got: {error:?}"
        );
    }

    #[test]
    fn open_rejects_unknown_format_version() {
        let dir = tempfile::tempdir().unwrap();
        let path = db_path(&dir);
        let storage = SqliteStorage::open(&path, &["records"]).unwrap();
        drop(storage);
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute("UPDATE meta SET value = X'02' WHERE key = 'format_version'", [])
            .unwrap();
        drop(conn);

        let error = SqliteStorage::open(&path, &["records"]).unwrap_err();
        assert!(
            matches!(&error, Error::InvalidStorageLayout(message) if message.contains("format_version")),
            "unknown format version must be rejected, got: {error:?}"
        );
    }

    #[test]
    fn open_reports_garbage_file_as_sqlite_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = db_path(&dir);
        std::fs::write(&path, b"this is not a sqlite database, not even close").unwrap();

        let error = SqliteStorage::open(&path, &["records"]).unwrap_err();
        assert!(
            matches!(error, Error::Sqlite(_)),
            "garbage file must surface the sqlite error, got: {error:?}"
        );
    }
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cargo test -p groove --features sqlite sqlite_storage -j 8`
Expected: FAIL to compile — `SqliteStorage` not defined.

- [ ] **Step 4: Implement the struct, open, and validation**

Add above the test module in `sqlite.rs`:

```rust
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use rusqlite::{Connection, OptionalExtension};

use super::{
    ColumnFamilyName, Durability, Error, Key, ScanVisitor, Value, WriteOperation,
    apply_storage_delta,
};

const FORMAT: &[u8] = b"jazz-groove-kv";
const FORMAT_VERSION: &[u8] = &[1];

#[derive(Clone, Copy, Debug)]
struct WriteFlushCadence {
    every: usize,
    pending: usize,
}

/// SQLite implementation of [`OrderedKvStorage`].
pub struct SqliteStorage {
    path: PathBuf,
    durability: Durability,
    column_families: RefCell<BTreeMap<String, i64>>,
    connection: RefCell<Option<Connection>>,
    write_flush_cadence: RefCell<Option<WriteFlushCadence>>,
}

impl SqliteStorage {
    /// Open with the default durability tier.
    ///
    /// Default is [`Durability::WalNoSync`] (WAL on, no per-commit fsync —
    /// crash-safe, never corrupts, bounded power-loss window), matching
    /// [`super::RocksDbStorage::open`]. Callers that need strict per-commit
    /// power-loss durability opt in via [`Self::open_with_durability`].
    pub fn open(path: impl AsRef<Path>, column_families: &[&str]) -> Result<Self, Error> {
        Self::open_with_durability(path, column_families, Durability::WalNoSync)
    }

    pub fn open_with_durability(
        path: impl AsRef<Path>,
        column_families: &[&str],
        durability: Durability,
    ) -> Result<Self, Error> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|error| Error::InvalidStorageLayout(format!(
                    "cannot create sqlite storage directory {}: {error}",
                    parent.display()
                )))?;
        }
        let fresh = !path.exists();
        let connection = Connection::open(&path)?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(
            None,
            "synchronous",
            match durability {
                Durability::FullSync => "FULL",
                Durability::WalNoSync => "NORMAL",
            },
        )?;
        connection.busy_timeout(std::time::Duration::from_secs(5))?;

        if fresh {
            Self::create_schema(&connection)?;
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

    fn create_schema(connection: &Connection) -> Result<(), Error> {
        connection.execute_batch(
            "CREATE TABLE meta (key TEXT PRIMARY KEY, value BLOB NOT NULL);
             CREATE TABLE column_families (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE);
             CREATE TABLE kv (
               cf INTEGER NOT NULL,
               k  BLOB    NOT NULL,
               v  BLOB    NOT NULL,
               PRIMARY KEY (cf, k)
             ) WITHOUT ROWID;",
        )?;
        connection.execute(
            "INSERT INTO meta (key, value) VALUES ('format', ?1), ('format_version', ?2), ('boundary_seq', X'00')",
            rusqlite::params![FORMAT, FORMAT_VERSION],
        )?;
        Ok(())
    }

    fn validate_schema(connection: &Connection) -> Result<(), Error> {
        let meta_exists: Option<String> = connection
            .query_row(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'meta'",
                [],
                |row| row.get(0),
            )
            .optional()?;
        if meta_exists.is_none() {
            return Err(Error::InvalidStorageLayout(
                "existing sqlite file has no meta table; refusing to adopt it".into(),
            ));
        }
        let format: Vec<u8> = connection
            .query_row("SELECT value FROM meta WHERE key = 'format'", [], |row| row.get(0))
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
            .query_row("SELECT value FROM meta WHERE key = 'format_version'", [], |row| row.get(0))
            .optional()?
            .ok_or_else(|| Error::InvalidStorageLayout("meta.format_version row is missing".into()))?;
        if version != FORMAT_VERSION {
            return Err(Error::InvalidStorageLayout(format!(
                "meta.format_version is {version:?}, supported version is {FORMAT_VERSION:?}"
            )));
        }
        for (table, expected) in [
            ("column_families", vec!["id", "name"]),
            ("kv", vec!["cf", "k", "v"]),
        ] {
            let mut statement = connection.prepare(&format!("PRAGMA table_info({table})"))?;
            let columns: Vec<String> = statement
                .query_map([], |row| row.get::<_, String>(1))?
                .collect::<Result<_, _>>()?;
            if columns != expected {
                return Err(Error::InvalidStorageLayout(format!(
                    "table {table} has columns {columns:?}, expected {expected:?}"
                )));
            }
        }
        Ok(())
    }

    fn intern_column_families(&self, column_families: &[&str]) -> Result<(), Error> {
        let connection = self.connection.borrow();
        let connection = connection.as_ref().expect("open holds a live connection");
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
            interned.insert((*name).to_string(), id);
        }
        // Pre-existing families from an earlier open stay visible.
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
}
```

Also add `close` so the Step 2 tests compile (full lifecycle behavior is Task 4; this minimal version just drops the connection):

```rust
impl SqliteStorage {
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
        let connection = connection
            .as_ref()
            .ok_or_else(|| Error::InvalidStorageLayout("sqlite storage is closed".into()))?;
        operate(connection)
    }
}
```

(`cf_id` and `with_connection` are unused until Task 3 wires the trait — same intermediate-commit caveat as Step 7's note.)

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p groove --features sqlite sqlite_storage -j 8`
Expected: 4 tests PASS. (The garbage-file test may surface the error from `pragma_update` or validation — either path must yield `Error::Sqlite`.)

- [ ] **Step 6: Verify feature independence**

Run: `cargo check -p groove --no-default-features && cargo check -p groove -j 8`
Expected: both pass — `sqlite` off means no rusqlite in the build graph (dev-dep still compiles for tests/benches only).

- [ ] **Step 7: Commit**

```bash
git add crates/groove/Cargo.toml crates/groove/src/storage/mod.rs crates/groove/src/storage/sqlite.rs
git commit -m "feat(groove): add sqlite storage feature with validated open"
```

Note: `path`, `durability`, and `write_flush_cadence` are declared here but first _used_ in Task 4 (`reopen`, `flush_write_boundary`). If the pre-commit clippy hook rejects the intermediate dead fields, don't add `#[allow(dead_code)]` — carry Tasks 2–4 as one commit at Task 4's commit step instead.

---

### Task 3: Point operations and ordered scans

**Files:**

- Modify: `crates/groove/src/storage/sqlite.rs`

**Interfaces:**

- Produces: `impl OrderedKvStorage for SqliteStorage` — `get`, `set`, `delete`, `scan_range`, `scan_prefix`, `column_family_names`, `approximate_class_bytes` (returns `Ok(None)`). Defaults supply `scan_prefix_reverse` / `last_with_prefix` / `last_with_prefix_before_or_at` over `scan_prefix` — correct by construction; SQL-native reverse is a recorded perf follow-up, not part of M1.

- [ ] **Step 1: Write the failing tests**

Append inside `mod tests` in `sqlite.rs`:

```rust
    fn open_records(dir: &tempfile::TempDir) -> SqliteStorage {
        SqliteStorage::open(db_path(dir), &["records"]).unwrap()
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
        assert_eq!(storage.get("records", b"").unwrap(), Some(b"empty-key".to_vec()));

        assert!(matches!(
            storage.get("absent", b"a").unwrap_err(),
            Error::ColumnFamilyNotFound(_)
        ));
        assert!(matches!(
            storage.set("absent", b"a", b"x").unwrap_err(),
            Error::ColumnFamilyNotFound(_)
        ));
    }

    #[test]
    fn scans_are_bytewise_ordered_and_prefix_scoped() {
        let dir = tempfile::tempdir().unwrap();
        let storage = open_records(&dir);
        for (key, value) in [
            (b"user:1".as_slice(), b"one".as_slice()),
            (b"user:10", b"ten"),
            (b"user:2", b"two"),
            (b"visit:1", b"v"),
        ] {
            storage.set("records", key, value).unwrap();
        }

        let mut seen = Vec::new();
        storage
            .scan_range("records", b"user:", b"user;", &mut |key, value| {
                seen.push((key.to_vec(), value.to_vec()));
                Ok(())
            })
            .unwrap();
        assert_eq!(
            seen,
            vec![
                (b"user:1".to_vec(), b"one".to_vec()),
                (b"user:10".to_vec(), b"ten".to_vec()),
                (b"user:2".to_vec(), b"two".to_vec()),
            ]
        );

        assert_eq!(
            storage.prefix("records", b"user:").unwrap().len(),
            3,
            "prefix scan must exclude non-matching tails"
        );
        assert_eq!(storage.prefix("records", b"").unwrap().len(), 4);
    }

    #[test]
    fn all_ff_prefix_scans_terminate_and_scope_correctly() {
        let dir = tempfile::tempdir().unwrap();
        let storage = open_records(&dir);
        storage.set("records", &[0xfe, 0x01], b"before").unwrap();
        storage.set("records", &[0xff], b"exact").unwrap();
        storage.set("records", &[0xff, 0x00], b"ff-zero").unwrap();
        storage.set("records", &[0xff, 0xff, 0x07], b"ff-ff").unwrap();

        assert_eq!(
            storage.prefix("records", &[0xff]).unwrap(),
            vec![
                (vec![0xff], b"exact".to_vec()),
                (vec![0xff, 0x00], b"ff-zero".to_vec()),
                (vec![0xff, 0xff, 0x07], b"ff-ff".to_vec()),
            ]
        );
        assert_eq!(
            storage.prefix("records", &[0xff, 0xff]).unwrap(),
            vec![(vec![0xff, 0xff, 0x07], b"ff-ff".to_vec())]
        );
        // Reverse + last derive from scan_prefix defaults; exercise them here too.
        assert_eq!(
            storage.last_with_prefix("records", &[0xff]).unwrap(),
            Some((vec![0xff, 0xff, 0x07], b"ff-ff".to_vec()))
        );
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
        assert_eq!(visited, 1, "scan must stop at the first visitor error");
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p groove --features sqlite sqlite_storage -j 8`
Expected: FAIL to compile — `SqliteStorage` does not implement `OrderedKvStorage`.

- [ ] **Step 3: Implement the trait (reads/scans)**

Add to `sqlite.rs`. Prefix upper bound: increment the rightmost non-`0xFF` byte when one exists (index-range optimization); an all-`0xFF` (or empty) prefix runs upper-unbounded. Both paths keep the `starts_with` guard and stop past the prefix, so correctness never depends on the bound.

```rust
fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut bound = prefix.to_vec();
    for index in (0..bound.len()).rev() {
        if bound[index] != 0xff {
            bound[index] += 1;
            bound.truncate(index + 1);
            return Some(bound);
        }
    }
    None
}

impl SqliteStorage {
    fn scan_where(
        &self,
        cf: &ColumnFamilyName,
        clause: &str,
        parameters: &[&dyn rusqlite::ToSql],
        prefix_guard: Option<&[u8]>,
        visit: &mut ScanVisitor<'_>,
    ) -> Result<(), Error> {
        let cf_id = self.cf_id(cf)?;
        self.with_connection(|connection| {
            let mut statement = connection.prepare_cached(&format!(
                "SELECT k, v FROM kv WHERE cf = ?1 {clause} ORDER BY k"
            ))?;
            let mut bound: Vec<&dyn rusqlite::ToSql> = vec![&cf_id];
            bound.extend_from_slice(parameters);
            let mut rows = statement.query(&bound[..])?;
            while let Some(row) = rows.next()? {
                let key = row.get_ref(0)?.as_blob()?;
                if let Some(prefix) = prefix_guard {
                    if !key.starts_with(prefix) {
                        break; // ordered scan has left the prefix range
                    }
                }
                let value = row.get_ref(1)?.as_blob()?;
                visit(key, value)?;
            }
            Ok(())
        })
    }
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
        self.scan_where(cf, "AND k >= ?2 AND k < ?3", &[&start, &end], None, visit)
    }

    fn scan_prefix(
        &self,
        cf: &ColumnFamilyName,
        prefix: &Key,
        visit: &mut ScanVisitor<'_>,
    ) -> Result<(), Error> {
        match prefix_upper_bound(prefix) {
            Some(upper) => self.scan_where(
                cf,
                "AND k >= ?2 AND k < ?3",
                &[&prefix, &upper],
                Some(prefix),
                visit,
            ),
            None => self.scan_where(cf, "AND k >= ?2", &[&prefix], Some(prefix), visit),
        }
    }

    fn write_many(&self, _operations: &[WriteOperation<'_>]) -> Result<(), Error> {
        unimplemented!("Task 4")
    }

    fn column_family_names(&self) -> Option<Vec<String>> {
        Some(self.column_families.borrow().keys().cloned().collect())
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p groove --features sqlite sqlite_storage -j 8`
Expected: all Task 2 + Task 3 tests PASS (`write_many` is not exercised yet).

- [ ] **Step 5: Commit**

```bash
git add crates/groove/src/storage/sqlite.rs
git commit -m "feat(groove): sqlite point ops and ordered scans"
```

---

### Task 4: Atomic batches, deltas, durability boundary, close/reopen

**Files:**

- Modify: `crates/groove/src/storage/sqlite.rs`

**Interfaces:**

- Consumes: `apply_storage_delta(current: Option<&[u8]>, encoded: &[u8]) -> Result<Vec<u8>, Error>` and `StorageDelta::encode` (existing, `storage/mod.rs`).
- Produces: full `OrderedKvStorage` (`write_many` with `Set`/`Delete`/`Delta`, `set_write_flush_cadence`, `flush_write_boundary`, `close`) and `impl ReopenableStorage` (`reopen(self, column_families)`). Behavior contracts: batches are all-or-nothing; a `Delta` in a batch observes earlier operations of the same batch; `flush_write_boundary` commits a `meta.boundary_seq` bump with WAL sync forced regardless of durability mode; post-close calls error; close checkpoints (`TRUNCATE`) and surfaces an incomplete checkpoint as an error.

- [ ] **Step 1: Write the failing tests**

Append inside `mod tests`:

```rust
    use super::super::{ReopenableStorage, StorageDelta, StorageDeltaKind, WriteOperation};

    #[test]
    fn write_many_is_atomic_and_deltas_observe_the_batch() {
        let dir = tempfile::tempdir().unwrap();
        let storage = open_records(&dir);

        // Mirror the shared conformance delta encoding: a delta over a value
        // written earlier in the same batch must observe that staged value.
        // (Exact merged bytes are asserted by the shared conformance function
        // in Task 5; here we assert batch visibility and atomicity.)
        storage
            .write_many(&[
                WriteOperation::set("records", b"a", b"one"),
                WriteOperation::delete("records", b"a"),
                WriteOperation::set("records", b"b", b"two"),
            ])
            .unwrap();
        assert_eq!(storage.get("records", b"a").unwrap(), None);
        assert_eq!(storage.get("records", b"b").unwrap(), Some(b"two".to_vec()));

        // Invalid delta payload ⇒ whole batch rolls back, including the Set
        // before it. StorageDelta's fields are public: a syntactically valid
        // envelope with a garbage CurrentWinner payload encodes fine and
        // fails only at application time.
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
        assert!(
            matches!(error, Error::InvalidStorageDelta(_) | Error::Record(_)),
            "invalid delta must fail the batch, got: {error:?}"
        );
        assert_eq!(
            storage.get("records", b"c").unwrap(),
            None,
            "failed batch must leave no partial state"
        );
    }

    #[test]
    fn boundary_flush_bumps_the_sequence_and_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let storage = open_records(&dir);
        storage.set_write_flush_cadence(2).unwrap();
        storage.write_many(&[WriteOperation::set("records", b"a", b"1")]).unwrap();
        storage.write_many(&[WriteOperation::set("records", b"b", b"2")]).unwrap();
        storage.flush_write_boundary().unwrap();
        storage.close().unwrap();

        let conn = rusqlite::Connection::open(db_path(&dir)).unwrap();
        let seq: Vec<u8> = conn
            .query_row("SELECT value FROM meta WHERE key = 'boundary_seq'", [], |row| row.get(0))
            .unwrap();
        assert!(
            seq.iter().any(|byte| *byte != 0),
            "boundary flushes must bump the persisted sequence (cadence hit + explicit call)"
        );
    }

    #[test]
    fn close_then_reopen_preserves_data_and_post_close_calls_error() {
        let dir = tempfile::tempdir().unwrap();
        let storage = open_records(&dir);
        storage.set("records", b"a", b"one").unwrap();
        storage.close().unwrap();

        assert!(storage.get("records", b"a").is_err(), "post-close reads must error");
        assert!(storage.set("records", b"a", b"x").is_err(), "post-close writes must error");

        let storage = SqliteStorage::open(db_path(&dir), &["records"]).unwrap();
        assert_eq!(storage.get("records", b"a").unwrap(), Some(b"one".to_vec()));

        let reopened = storage.reopen(&["records", "added_family"]).unwrap();
        assert_eq!(reopened.get("records", b"a").unwrap(), Some(b"one".to_vec()));
        reopened.set("added_family", b"k", b"v").unwrap();
        assert_eq!(reopened.get("added_family", b"k").unwrap(), Some(b"v".to_vec()));
    }
```

(`StorageDelta { kind, payload }` fields are `pub` in `storage/mod.rs:124-127`; `StorageDeltaKind::CurrentWinnerV1` is its only variant. No test-only constructor is needed and none may be added.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p groove --features sqlite sqlite_storage -j 8`
Expected: FAIL — `write_many` panics `unimplemented!`, `reopen` missing.

- [ ] **Step 3: Implement**

Replace the `write_many` stub and add the lifecycle methods:

```rust
    fn write_many(&self, operations: &[WriteOperation<'_>]) -> Result<(), Error> {
        // Validate families first so a bad name cannot leave a partial batch
        // (mirrors MemoryStorage's preflight).
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
            let connection = connection
                .as_mut()
                .ok_or_else(|| Error::InvalidStorageLayout("sqlite storage is closed".into()))?;
            let transaction = connection.transaction()?;
            for operation in operations {
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
                        // Reads inside the transaction observe earlier staged
                        // statements: read-your-own-writes inside the batch.
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
        *self.write_flush_cadence.borrow_mut() =
            Some(WriteFlushCadence { every, pending: 0 });
        Ok(())
    }

    fn flush_write_boundary(&self) -> Result<(), Error> {
        // A durable boundary is a meta.boundary_seq bump committed with WAL
        // sync forced for this commit, independent of checkpointing and of the
        // store's durability mode.
        self.with_connection(|connection| {
            connection.pragma_update(None, "synchronous", "FULL")?;
            let result = connection.execute(
                "UPDATE meta
                 SET value = CAST((CAST(COALESCE(value, X'00') AS INTEGER) + 1) AS BLOB)
                 WHERE key = 'boundary_seq'",
                [],
            );
            let restore = match self.durability {
                Durability::FullSync => "FULL",
                Durability::WalNoSync => "NORMAL",
            };
            connection.pragma_update(None, "synchronous", restore)?;
            result?;
            Ok(())
        })?;
        if let Some(cadence) = self.write_flush_cadence.borrow_mut().as_mut() {
            cadence.pending = 0;
        }
        Ok(())
    }

    fn close(&self) -> Result<(), Error> {
        let Some(connection) = self.connection.borrow_mut().take() else {
            return Ok(()); // idempotent close, matching sibling backends
        };
        let (log_frames, checkpointed): (i64, i64) = connection.query_row(
            "PRAGMA wal_checkpoint(TRUNCATE)",
            [],
            |row| Ok((row.get(1)?, row.get(2)?)),
        )?;
        if log_frames != checkpointed {
            return Err(Error::InvalidStorageLayout(format!(
                "close checkpoint incomplete: {checkpointed}/{log_frames} WAL frames"
            )));
        }
        connection.close().map_err(|(_, error)| Error::Sqlite(error))?;
        Ok(())
    }
```

(SQLite's `wal_checkpoint` pragma returns `(busy, log, checkpointed)`; `busy_timeout` is already set at open. On an empty WAL both counts are `0` or `-1` — treat equal values as complete.)

And the reopen implementation after the trait impl:

```rust
impl super::ReopenableStorage for SqliteStorage {
    fn reopen(self, column_families: &[&str]) -> Result<Self, Error> {
        let path = self.path.clone();
        let durability = self.durability;
        self.close()?;
        Self::open_with_durability(path, column_families, durability)
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p groove --features sqlite sqlite_storage -j 8`
Expected: all sqlite tests PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/groove/src/storage/sqlite.rs
git commit -m "feat(groove): sqlite atomic batches, durability boundary, close/reopen"
```

---

### Task 5: Conformance parity with the shared suite

**Files:**

- Modify: `crates/groove/src/storage/sqlite.rs` (tests only)

**Interfaces:**

- Consumes: `super::super::conformance::{persistence_order_and_batch_atomicity, reopen_preserves_data_and_adds_families, delta_append_current_winner_observes_merged_state}` — the same parametrized functions `opfs.rs:373-475` runs for `NativeBtreeStorage`. They are `pub(crate)`, so the sqlite test module reaches them exactly as opfs does.

- [ ] **Step 1: Write the conformance + large-value tests**

Append inside `mod tests`:

```rust
    #[test]
    fn sqlite_storage_passes_shared_persistence_conformance() {
        let dir = tempfile::tempdir().unwrap();
        let storage = SqliteStorage::open(db_path(&dir), &["records"]).unwrap();
        super::super::conformance::persistence_order_and_batch_atomicity(storage);
    }

    #[test]
    fn sqlite_storage_passes_shared_delta_conformance() {
        let dir = tempfile::tempdir().unwrap();
        let storage = SqliteStorage::open(db_path(&dir), &["records"]).unwrap();
        super::super::conformance::delta_append_current_winner_observes_merged_state(storage);
    }

    #[test]
    fn sqlite_storage_passes_shared_reopen_conformance() {
        let dir = tempfile::tempdir().unwrap();
        let storage = SqliteStorage::open(db_path(&dir), &["records"]).unwrap();
        super::super::conformance::reopen_preserves_data_and_adds_families(storage);
    }

    #[test]
    fn multi_megabyte_values_round_trip_at_both_durability_levels() {
        for durability in [Durability::WalNoSync, Durability::FullSync] {
            let dir = tempfile::tempdir().unwrap();
            let storage =
                SqliteStorage::open_with_durability(db_path(&dir), &["records"], durability)
                    .unwrap();
            let value: Vec<u8> = (0..(4 * 1024 * 1024u32)).map(|i| (i % 251) as u8).collect();
            storage.set("records", b"blob", &value).unwrap();
            storage.close().unwrap();
            let storage = SqliteStorage::open(db_path(&dir), &["records"]).unwrap();
            assert_eq!(storage.get("records", b"blob").unwrap(), Some(value));
        }
    }
```

Fixture note: all three conformance functions seed the `"records"` family only; `reopen_preserves_data_and_adds_families` itself reopens with `["records", "indices"]` — so opening with `&["records"]` is exactly right, matching the `opfs.rs:373-480` call sites.

- [ ] **Step 2: Run to verify current state**

Run: `cargo test -p groove --features sqlite sqlite_storage -j 8`
Expected: PASS if Tasks 3–4 are correct — the conformance functions are the cross-backend oracle; any failure here is a real contract divergence to fix in `sqlite.rs`, not in the test.

- [ ] **Step 3: Run the whole groove suite both ways**

Run: `cargo test -p groove -j 8 && cargo test -p groove --features sqlite -j 8`
Expected: PASS; default-feature run proves no regression for existing backends.

- [ ] **Step 4: Commit**

```bash
git add crates/groove/src/storage/sqlite.rs
git commit -m "test(groove): run shared storage conformance over sqlite backend"
```

---

### Task 6: Abrupt-termination durability tests

**Files:**

- Modify: `crates/groove/src/storage/sqlite.rs` (tests only, `#[cfg(unix)]`)

**Interfaces:**

- Consumes: the self-exec pattern — the test spawns `std::env::current_exe()` (the test binary itself) filtered to a helper "test" that only acts when `SQLITE_KILL_TEST_ROLE` is set, then SIGKILLs it at a controlled point. No new binaries, no harness changes.

- [ ] **Step 1: Write the child writer and the kill tests**

Append inside `mod tests`:

```rust
    // ---- abrupt-termination durability ------------------------------------
    // A clean close cannot stand in for jetsam: these tests SIGKILL a child
    // process at controlled points and assert exactly which writes survive
    // WAL recovery on reopen.

    /// Child entry point. Runs only when the env marker is set; otherwise it
    /// is a no-op test. The child never closes the storage — the kill is the
    /// point.
    #[test]
    fn sqlite_kill_test_child_writer() {
        let Ok(role) = std::env::var("SQLITE_KILL_TEST_ROLE") else { return };
        let path = std::env::var("SQLITE_KILL_TEST_DB").unwrap();
        let ready = std::env::var("SQLITE_KILL_TEST_READY").unwrap();
        let durability = match role.as_str() {
            "full_sync" => Durability::FullSync,
            "wal_no_sync" | "torn_batch" => Durability::WalNoSync,
            other => panic!("unknown kill-test role {other}"),
        };
        let storage =
            SqliteStorage::open_with_durability(&path, &["records"], durability).unwrap();

        if role == "torn_batch" {
            // Churn multi-op generational batches until killed mid-flight.
            // Generation g writes gen:<g>:0 .. gen:<g>:7 in ONE batch; the
            // parent kills without waiting for quiescence, so some batch is
            // likely in flight at kill time.
            std::fs::write(&ready, b"ready").unwrap();
            for generation in 0u64.. {
                let generation_bytes = generation.to_be_bytes();
                let keys: Vec<Vec<u8>> = (0u8..8)
                    .map(|slot| {
                        let mut key = b"gen:".to_vec();
                        key.extend_from_slice(&generation_bytes);
                        key.push(b':');
                        key.push(slot);
                        key
                    })
                    .collect();
                let operations: Vec<WriteOperation<'_>> = keys
                    .iter()
                    .map(|key| WriteOperation::set("records", key, &generation_bytes))
                    .collect();
                storage.write_many(&operations).unwrap();
            }
            unreachable!();
        }

        storage.set("records", b"before-boundary", b"1").unwrap();
        if role == "wal_no_sync" {
            storage.flush_write_boundary().unwrap();
        }
        // Signal the parent that the guaranteed-durable point is on disk.
        std::fs::write(&ready, b"ready").unwrap();
        // Keep writing past the durable point, then spin until killed.
        storage.set("records", b"after-boundary", b"2").unwrap();
        loop {
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
    }

    #[cfg(unix)]
    fn run_kill_test(role: &str) -> SqliteStorage {
        let dir = tempfile::tempdir().unwrap();
        let db = db_path(&dir).to_string_lossy().into_owned();
        let ready = dir.path().join("ready").to_string_lossy().into_owned();
        // Substring filter (NOT --exact: that would require the full module
        // path `storage::sqlite_storage::tests::sqlite_kill_test_child_writer`).
        // The name is unique in the crate, so the filter runs exactly one test.
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["sqlite_kill_test_child_writer", "--nocapture"])
            .env("SQLITE_KILL_TEST_ROLE", role)
            .env("SQLITE_KILL_TEST_DB", &db)
            .env("SQLITE_KILL_TEST_READY", &ready)
            .spawn()
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        while !std::path::Path::new(&ready).exists() {
            assert!(std::time::Instant::now() < deadline, "child never became ready");
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        child.kill().unwrap(); // SIGKILL on unix
        child.wait().unwrap();
        // The TempDir must outlive the returned storage: leak it for the test.
        std::mem::forget(dir);
        SqliteStorage::open(std::path::PathBuf::from(db), &["records"]).unwrap()
    }

    #[cfg(unix)]
    #[test]
    fn sigkill_after_full_sync_commit_preserves_every_commit() {
        let storage = run_kill_test("full_sync");
        assert_eq!(
            storage.get("records", b"before-boundary").unwrap(),
            Some(b"1".to_vec()),
            "FullSync: a committed write must survive SIGKILL"
        );
        // after-boundary raced the kill; both outcomes are legal. What is not
        // legal is corruption: the store opened and served reads above.
    }

    #[cfg(unix)]
    #[test]
    fn sigkill_after_boundary_preserves_the_boundary_prefix() {
        let storage = run_kill_test("wal_no_sync");
        assert_eq!(
            storage.get("records", b"before-boundary").unwrap(),
            Some(b"1".to_vec()),
            "WalNoSync: writes before flush_write_boundary must survive SIGKILL"
        );
    }

    #[cfg(unix)]
    #[test]
    fn sigkill_mid_batch_never_leaves_a_torn_generation() {
        let storage = run_kill_test("torn_batch");
        // Every generation with any surviving key must have all 8 slots: a
        // batch is all-or-nothing even when the process dies mid-commit.
        let mut generation_counts = std::collections::BTreeMap::<Vec<u8>, u32>::new();
        storage
            .scan_prefix("records", b"gen:", &mut |key, _value| {
                // key = "gen:" + 8-byte generation + ":" + slot
                let generation = key[4..12].to_vec();
                *generation_counts.entry(generation).or_insert(0) += 1;
                Ok(())
            })
            .unwrap();
        for (generation, count) in generation_counts {
            assert_eq!(
                count, 8,
                "generation {generation:?} is torn: {count}/8 keys survived"
            );
        }
    }
```

- [ ] **Step 2: Run the kill tests**

Run: `cargo test -p groove --features sqlite sigkill -j 8 -- --test-threads=1`
Expected: 3 tests PASS (`full_sync`, `wal_no_sync` boundary, `torn_batch`). Also run the child no-op path: `cargo test -p groove --features sqlite sqlite_kill_test_child_writer -j 8` — PASS immediately (env unset ⇒ no-op).

- [ ] **Step 3: Commit**

```bash
git add crates/groove/src/storage/sqlite.rs
git commit -m "test(groove): sigkill durability coverage for sqlite backend"
```

---

### Task 7: Re-point jazz's `sqlite` feature and add the node-harness slot

**Files:**

- Modify: `crates/jazz/Cargo.toml:12` (feature) and `crates/jazz/Cargo.toml:75` (drop rusqlite dependency)
- Modify: `crates/jazz/src/node/tests/sync.rs` (sqlite sibling of the btree node constructor + smoke test)

**Interfaces:**

- Consumes: `groove::storage::SqliteStorage` (Tasks 2–4); the existing backend matrix in the test that prints `policy_graph_perf_dropdown_entry_reset_ingest_timing` (`sync.rs:2153-2186`), which runs `apply_policy_graph_reset_receipt` over memory / rocksdb / native-btree; the `open_policy_graph_native_btree_node` constructor at `sync.rs:1981-1997`.
- Produces: `jazz` feature `sqlite = ["groove/sqlite"]`; a fourth, `#[cfg(feature = "sqlite")]` leg in that matrix exercising `NodeState<SqliteStorage>`.

- [ ] **Step 1: Re-point the feature**

In `crates/jazz/Cargo.toml` replace:

```toml
sqlite = ["dep:rusqlite"]
```

with:

```toml
sqlite = ["groove/sqlite"]
```

and delete the line:

```toml
rusqlite = { version = "0.34", features = ["bundled"], optional = true }
```

Run: `grep -rn "rusqlite" crates/jazz/src crates/jazz/tests` — Expected: no hits (verified during design; if one appears, stop and reassess rather than force the removal).

- [ ] **Step 2: Add the sqlite leg to the backend matrix**

In `crates/jazz/src/node/tests/sync.rs`, directly below `open_policy_graph_native_btree_node` (`sync.rs:1981-1997`), add the sqlite sibling, mirroring its body exactly (same `column_families()` refs derivation, same `NodeState::new` call):

```rust
#[cfg(feature = "sqlite")]
fn open_policy_graph_sqlite_node(
    node_uuid: NodeUuid,
    schema: JazzSchema,
) -> (tempfile::TempDir, NodeState<groove::storage::SqliteStorage>) {
    let temp_dir = tempfile::tempdir().unwrap();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage =
        groove::storage::SqliteStorage::open(temp_dir.path().join("node.db"), &refs).unwrap();
    (
        temp_dir,
        NodeState::new(node_uuid, schema, storage).unwrap(),
    )
}
```

Then, in the matrix test (`sync.rs:2153-2186`), insert a fourth leg **before** the native-btree leg (which consumes `update` by move — the new leg clones like the earlier legs do):

```rust
    #[cfg(feature = "sqlite")]
    {
        let (_sqlite_dir, mut sqlite_reader) =
            open_policy_graph_sqlite_node(node(0x26), schema.clone());
        let sqlite_elapsed = apply_policy_graph_reset_receipt(
            "sqlite",
            &mut sqlite_reader,
            &shape,
            &binding,
            update.clone(),
            entry_count,
        );
        println!(
            "policy_graph_perf_dropdown_entry_reset_ingest_timing sqlite_apply_ms={:.3}",
            sqlite_elapsed.as_secs_f64() * 1000.0
        );
    }
```

Do not invent a new scenario: the value is backend parity inside this existing, known-good matrix (same receipt, same assertions inside `apply_policy_graph_reset_receipt`).

- [ ] **Step 3: Run the matrix test with sqlite enabled**

Run: `cargo test -p jazz --no-default-features --features test policy_graph_perf_dropdown_entry_reset_ingest_timing -j 8`
Expected: PASS with the sqlite leg's timing line in the output (`--nocapture` to see it). A failure here is a real backend divergence surfaced by the node semantics — investigate in `sqlite.rs`, do not touch the test.

- [ ] **Step 4: Run the canonical jazz gate**

Run: `cargo test -p jazz --no-default-features --features test -j 8`
Expected: PASS (the `test-utils` feature already includes `sqlite`, so this gate now builds and exercises the backend).

- [ ] **Step 5: Commit**

```bash
git add crates/jazz/Cargo.toml crates/jazz/src/node/tests/sync.rs
git commit -m "chore(jazz): re-point sqlite feature onto groove backend"
```

---

### Task 8: Landing-tier gates and documentation

**Files:**

- Modify: `.claude/CLAUDE.md` (canonical gate list)
- Modify: `dev/RN_BINDING_REWRITE_DESIGN.md` (status line only)

**Interfaces:**

- Consumes: everything above.
- Produces: M1 recorded as landed; gates updated so the backend cannot rot silently.

- [ ] **Step 1: Update the canonical gate list**

In `.claude/CLAUDE.md`, in the canonical-gates bullet list, add after the `cargo test -p groove` line:

```markdown
- `cargo test -p groove --features sqlite` (mobile storage backend; added with M1
  of dev/RN_BINDING_REWRITE_DESIGN.md)
```

- [ ] **Step 2: Mark M1 landed in the design doc**

In `dev/RN_BINDING_REWRITE_DESIGN.md`, change the Status line to record M1: `Status: in progress (M1 landed <date>; M2–M5 pending).`

- [ ] **Step 3: Run the full landing-tier gate set**

Run, in order (use `-j` fitting the box):

```bash
cargo test -p jazz -j 8
cargo test -p groove -j 8
cargo test -p groove --features sqlite -j 8
cargo test -p jazz --no-default-features --features test -j 8
cargo test -p jazz-server -j 8
cargo check -p jazz-sim --benches
dev/gates/ts-wire-codec.sh
JAZZ_SEED_COUNT=300 cargo test -p jazz m3_maintained_one_shot_differential_oracle
cargo test -p jazz --test incremental_delivery_canary maintained_relation_include_single_row_changes_are_scale_independent -- --exact
dev/benchmarks/smoke.sh
```

Expected: all green. Storage was touched, so smoke.sh is mandatory here. The jazz-private sensitive-data guard is currently not installed on this machine — clone `jazz-private` first or note its absence in the PR.

- [ ] **Step 4: Commit**

```bash
git add .claude/CLAUDE.md dev/RN_BINDING_REWRITE_DESIGN.md
git commit -m "chore: add sqlite backend gate and record M1"
```

---

## Not in this plan (deliberately)

- SQL-native `scan_prefix_reverse` / `last_with_prefix*` (defaults are correct; optimize only when a profile demands it — record in the M2 plan if it comes up).
- `approximate_class_bytes` heuristics (contract answer `Ok(None)`).
- Any `crates/jazz-rn` change (M2 plan), TS change (M3 plan), or mobile build (M4 plan).
- Wiring the new conformance additions (`0xFF`, kill tests) back over Memory/NativeBtree/RocksDB — worth doing if free, but any latent failure it exposes in an existing backend is its own investigation, not an M1 blocker.
