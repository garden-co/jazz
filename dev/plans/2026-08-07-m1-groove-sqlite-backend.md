# M1: Groove SQLite Storage Backend — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A production `SqliteStorage` backend in `crates/groove` implementing the ordered-KV contract over bundled rusqlite, conformance-equal to `MemoryStorage`, with verified durability boundaries — plus the `jazz` `sqlite` feature re-pointed onto it.

**Architecture:** One SQLite file per store (`journal_mode=WAL`), a single `kv (cf INTEGER, k BLOB, v BLOB, PRIMARY KEY(cf,k)) WITHOUT ROWID` table with interned column-family ids, a `meta` table carrying format identity and the durable-boundary counter. Thread-affine (`RefCell`), single connection, no async. `Durability` is lifted out of the RocksDB-gated module first so the backend can share it.

**Tech Stack:** Rust 2024, rusqlite 0.34 (`bundled`), thiserror 2, existing groove storage traits (`OrderedKvStorage`, `ReopenableStorage`), `apply_storage_delta` for `WriteOperation::Delta`.

**Spec:** `dev/RN_BINDING_REWRITE_DESIGN.md` §3, §8.1–8.3. This plan is M1 only; jazz-rn (M2) is a separate plan.

**Revision note (2026-08-08):** amended after plan review — init atomicity, exact schema validation, u64 boundary counter, checkpoint busy handling, deterministic torn-batch failpoint, same-batch delta coverage, Tasks 2–4 as one commit, sqlite-only gate, AGENTS.md/symlink and jazz-server gate corrections, SPEC updates pulled into M1.

## Global Constraints

- Feature name is `sqlite` in both `groove` and `jazz`; `jazz`'s becomes `sqlite = ["groove/sqlite"]`.
- rusqlite pin: `0.34` with `features = ["bundled"]` (already the dev-dependency pin; keep both entries).
- `SqliteStorage::open` defaults to `Durability::WalNoSync`, mirroring `RocksDbStorage::open`'s documented default; `open_with_durability` opts into `FullSync`.
- Format identity: `meta` rows `format = 'jazz-groove-kv'`, `format_version = 1`, `boundary_seq` = 8-byte big-endian `u64`. Store initialization is **atomic** (one transaction); fresh-vs-existing is decided by `sqlite_master` emptiness, never by file existence.
- Any existing non-empty file that does not validate **exactly** (stored `CREATE` SQL text per table, meta rows present, `boundary_seq` exactly 8 bytes) ⇒ `Error::InvalidStorageLayout`. Never adopt an alien file.
- Corruption (`SQLITE_CORRUPT`/`SQLITE_NOTADB`) surfaces through a new transparent `Error::Sqlite(#[from] rusqlite::Error)` variant, feature-gated.
- All-`0xFF` prefixes have no finite upper bound: prefix scans must terminate by `starts_with`, never rely on an incremented bound alone.
- Storage-level tests are internal (`#[cfg(test)]` in `sqlite.rs`), matching `memory.rs`/`opfs.rs`; per `crates/jazz/TESTING_GUIDELINES.md` internal tests must say why — backend contract behavior is not observable through public jazz APIs.
- Node-level tests (Task 7) use public builders; no JSON-literal schemas/queries.
- Per-task iteration gate: `cargo test -p groove --no-default-features --features sqlite` — the sqlite-only configuration jazz-rn will actually build (and it skips the RocksDB compile). Combined-features runs (`cargo test -p groove --features sqlite`) happen at Task 5 and landing.
- Landing tier (Task 8): `cargo test -p jazz`, `cargo test -p groove`, `cargo test -p groove --features sqlite`, `cargo test -p groove --no-default-features --features sqlite`, `cargo test -p jazz --no-default-features --features test`, `cargo test -p jazz --bin jazz-server`, `cargo check -p jazz-sim --benches`, ts-wire-codec gate, oracle + canary, `dev/benchmarks/smoke.sh` (storage touched), and the jazz-private sensitive-data guard — the guard's absence **blocks push** (clone `jazz-private` or obtain an explicit owner exception first).
- `.claude/CLAUDE.md` is a symlink to `AGENTS.md` — documentation edits go to `AGENTS.md` and stage `AGENTS.md`.
- Tasks 2–4 form **one commit** (Task 4's final step): no intermediate commit may ship a public `OrderedKvStorage` impl with an `unimplemented!()` method or dead fields.
- Commit messages follow repo style (`feat(groove): …`, `test(groove): …`, `chore(jazz): …`); no AI attribution anywhere.

---

### Task 1: Lift `Durability` out of the RocksDB-gated module

**Files:**

- Modify: `crates/groove/src/storage/rocksdb.rs:41-50` (remove enum definition; re-export instead)
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

In `crates/groove/src/storage/rocksdb.rs`, delete the `pub enum Durability { … }` block (lines 41-50, including its doc comment "RocksDB durability tier used for writes.") and add — as the **only** new item for this name, do NOT also add `Durability` to the existing `use super::…` list (that would be E0252, a duplicate import):

```rust
pub use super::Durability;
```

`pub use` both imports the name for the module body and re-exports it, so `rocksdb_storage::Durability` still resolves for external callers.

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

### Task 2: `sqlite` feature, error variant, and atomic validated open

**Files:**

- Modify: `crates/groove/Cargo.toml` (feature + optional dependency)
- Modify: `crates/groove/src/storage/mod.rs` (module wiring + `Error::Sqlite`)
- Create: `crates/groove/src/storage/sqlite.rs`

**Interfaces:**

- Consumes: `Durability` from Task 1; `Error`, `ColumnFamilyName`, `Key`, `Value`, `KeyValue`, `ScanVisitor`, `WriteOperation`, `apply_storage_delta` from `storage/mod.rs`.
- Produces: `groove::storage::SqliteStorage` (`#[derive(Debug)]` — tests use `unwrap_err()` on `Result<SqliteStorage, _>`, which needs the Ok type `Debug`) with:
  - `pub fn open(path: impl AsRef<Path>, column_families: &[&str]) -> Result<Self, Error>` (WalNoSync)
  - `pub fn open_with_durability(path: impl AsRef<Path>, column_families: &[&str], durability: Durability) -> Result<Self, Error>`
  - `groove::storage::Error::Sqlite(rusqlite::Error)` (feature-gated, `#[from]`, transparent)

**No commit in this task** — Tasks 2–4 land as one commit at Task 4's final step (a partial trait surface must never be committed).

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
    fn open_treats_empty_or_schemaless_files_as_fresh() {
        // Crash-during-initialization recovery: SQLite may leave a zero-byte
        // file, or a file with a valid header but no objects (create
        // transaction never committed). Both must open as fresh stores, not
        // be rejected as alien.
        let dir = tempfile::tempdir().unwrap();

        let zero_byte = dir.path().join("zero.db");
        std::fs::write(&zero_byte, b"").unwrap();
        let storage = SqliteStorage::open(&zero_byte, &["records"]).unwrap();
        drop(storage);

        let headers_only = dir.path().join("headers.db");
        let conn = rusqlite::Connection::open(&headers_only).unwrap();
        conn.pragma_update_and_check(None, "journal_mode", "WAL", |_| Ok(())).unwrap();
        // Force header materialization without creating any schema object.
        conn.pragma_update(None, "user_version", 0).unwrap();
        drop(conn);
        let storage = SqliteStorage::open(&headers_only, &["records"]).unwrap();
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
    fn open_rejects_tampered_table_shape() {
        let dir = tempfile::tempdir().unwrap();
        let path = db_path(&dir);
        let storage = SqliteStorage::open(&path, &["records"]).unwrap();
        drop(storage);
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
    fn open_rejects_malformed_boundary_seq() {
        let dir = tempfile::tempdir().unwrap();
        let path = db_path(&dir);
        let storage = SqliteStorage::open(&path, &["records"]).unwrap();
        drop(storage);
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute("UPDATE meta SET value = X'00' WHERE key = 'boundary_seq'", [])
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
        assert!(
            matches!(error, Error::Sqlite(_)),
            "garbage file must surface the sqlite error, got: {error:?}"
        );
    }
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cargo test -p groove --no-default-features --features sqlite sqlite_storage -j 8`
Expected: FAIL to compile — `SqliteStorage` not defined.

- [ ] **Step 4: Implement the struct and atomic validated open**

Add above the test module in `sqlite.rs`. Initialization rules:

- Fresh-vs-existing is decided by `SELECT COUNT(*) FROM sqlite_master` — **not** file existence — so a crash that left a zero-byte or headers-only file recovers as fresh.
- Creation is one transaction (`execute_batch` wrapped in `BEGIN`/`COMMIT`): after a mid-initialization kill the schema either fully exists or not at all; no third state can brick the store.
- The exact `CREATE` statements are constants, reused verbatim by validation against `sqlite_master.sql` — types, `NOT NULL`, primary keys, `UNIQUE`, and `WITHOUT ROWID` are all validated because the full SQL text is compared, satisfying the design's "exact expected shape" contract.
- `journal_mode=WAL` is set via `pragma_update_and_check` and the returned mode is asserted to be `wal` — `pragma_update` alone would error on the returned row, and silently-not-WAL would void the durability model.

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

const CREATE_META: &str =
    "CREATE TABLE meta (key TEXT PRIMARY KEY, value BLOB NOT NULL)";
const CREATE_COLUMN_FAMILIES: &str =
    "CREATE TABLE column_families (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE)";
const CREATE_KV: &str = "CREATE TABLE kv (\n  cf INTEGER NOT NULL,\n  k  BLOB    NOT NULL,\n  v  BLOB    NOT NULL,\n  PRIMARY KEY (cf, k)\n) WITHOUT ROWID";

#[derive(Clone, Copy, Debug)]
struct WriteFlushCadence {
    every: usize,
    pending: usize,
}

/// SQLite implementation of [`super::OrderedKvStorage`].
#[derive(Debug)]
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
    /// `RocksDbStorage::open`. Callers that need strict per-commit
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
            // A bare relative filename yields Some("") — nothing to create.
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent).map_err(|error| {
                    Error::InvalidStorageLayout(format!(
                        "cannot create sqlite storage directory {}: {error}",
                        parent.display()
                    ))
                })?;
            }
        }
        let connection = Connection::open(&path)?;
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
        connection.busy_timeout(std::time::Duration::from_secs(5))?;

        // Fresh means "no schema objects at all" — a zero-byte or headers-only
        // file left by a crash before the (single-transaction) initialization
        // committed recovers as fresh here.
        let object_count: i64 =
            connection.query_row("SELECT COUNT(*) FROM sqlite_master", [], |row| row.get(0))?;
        if object_count == 0 {
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
        // One transaction: initialization is atomic. execute_batch runs each
        // statement in autocommit otherwise, and a kill between them would
        // otherwise leave a half-initialized store.
        connection.execute_batch(&format!(
            "BEGIN;\n{CREATE_META};\n{CREATE_COLUMN_FAMILIES};\n{CREATE_KV};\nCOMMIT;"
        ))?;
        connection.execute(
            "INSERT INTO meta (key, value) VALUES ('format', ?1), ('format_version', ?2), ('boundary_seq', ?3)",
            rusqlite::params![FORMAT, FORMAT_VERSION, 0u64.to_be_bytes()],
        )?;
        Ok(())
    }

    fn validate_schema(connection: &Connection) -> Result<(), Error> {
        // Exact-shape validation: compare the stored CREATE statements against
        // the constants used to create them. SQLite preserves the original SQL
        // text verbatim in sqlite_master, so this checks names, types,
        // NOT NULL, primary keys, UNIQUE, and WITHOUT ROWID in one comparison.
        for (table, expected) in [
            ("meta", CREATE_META),
            ("column_families", CREATE_COLUMN_FAMILIES),
            ("kv", CREATE_KV),
        ] {
            let stored: Option<String> = connection
                .query_row(
                    "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?1",
                    [table],
                    |row| row.get(0),
                )
                .optional()?;
            match stored {
                None => {
                    return Err(Error::InvalidStorageLayout(format!(
                        "table {table} is missing; refusing to adopt this file"
                    )));
                }
                Some(sql) if sql != expected => {
                    return Err(Error::InvalidStorageLayout(format!(
                        "table {table} shape diverges from the expected layout: {sql:?}"
                    )));
                }
                Some(_) => {}
            }
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
        let boundary: Vec<u8> = connection
            .query_row("SELECT value FROM meta WHERE key = 'boundary_seq'", [], |row| row.get(0))
            .optional()?
            .ok_or_else(|| Error::InvalidStorageLayout("meta.boundary_seq row is missing".into()))?;
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

Note: `meta` insertion runs right after the schema transaction. If the process dies between the two, the file has tables but no meta rows — `validate_schema` rejects it with "meta.format row is missing". To keep even that window closed, the meta `INSERT` may be folded into the same `execute_batch` transaction with inline blob literals (`X'...'`); implementer's choice, but the batch + params split shown above MUST then be replaced by literals since `execute_batch` takes no params:

```rust
connection.execute_batch(&format!(
    "BEGIN;\n{CREATE_META};\n{CREATE_COLUMN_FAMILIES};\n{CREATE_KV};\n\
     INSERT INTO meta (key, value) VALUES\
     ('format', X'{}'), ('format_version', X'{}'), ('boundary_seq', X'0000000000000000');\nCOMMIT;",
    hex_upper(FORMAT),
    hex_upper(FORMAT_VERSION),
))?;
```

with this local helper. Prefer this fully-atomic form.

```rust
fn hex_upper(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02X}")).collect()
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p groove --no-default-features --features sqlite sqlite_storage -j 8`
Expected: 7 tests PASS.

- [ ] **Step 6: Verify feature independence**

Run: `cargo check -p groove --no-default-features && cargo check -p groove -j 8`
Expected: both pass — `sqlite` off means no rusqlite in the build graph (dev-dep still compiles for tests/benches only).

Do NOT commit yet (see task header).

---

### Task 3: Point operations and ordered scans

**Files:**

- Modify: `crates/groove/src/storage/sqlite.rs`

**Interfaces:**

- Produces: `impl OrderedKvStorage for SqliteStorage` — `get`, `set`, `delete`, `scan_range`, `scan_prefix`, `column_family_names`, `approximate_class_bytes` (returns `Ok(None)`). Defaults supply `scan_prefix_reverse` / `last_with_prefix` / `last_with_prefix_before_or_at` over `scan_prefix` — correct by construction; SQL-native reverse is a recorded perf follow-up, not part of M1. `write_many` remains a `todo!("Task 4")` stub — which is exactly why this task has no commit.

- [ ] **Step 1: Extend the test module imports and write the failing tests**

First extend the test-module `use` block (the trait must be in scope for method calls):

```rust
    use super::super::{Durability, Error, OrderedKvStorage};
```

Then append inside `mod tests`:

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

Run: `cargo test -p groove --no-default-features --features sqlite sqlite_storage -j 8`
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
        todo!("Task 4 — never committed in this state; Tasks 2-4 are one commit")
    }

    fn column_family_names(&self) -> Option<Vec<String>> {
        Some(self.column_families.borrow().keys().cloned().collect())
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p groove --no-default-features --features sqlite sqlite_storage -j 8`
Expected: all Task 2 + Task 3 tests PASS (`write_many` is not exercised yet).

Do NOT commit yet (see Task 2 header).

---

### Task 4: Atomic batches, deltas, durability boundary, close/reopen

**Files:**

- Modify: `crates/groove/src/storage/sqlite.rs`
- Modify: `crates/groove/src/storage/mod.rs` (hoist two conformance helpers; add one conformance fn)

**Interfaces:**

- Consumes: `apply_storage_delta(current: Option<&[u8]>, encoded: &[u8]) -> Result<Vec<u8>, Error>`; `StorageDelta`/`StorageDeltaKind` (`pub` fields); the conformance module's `record`/`delta` builders (hoisted in Step 1).
- Produces: full `OrderedKvStorage` (`write_many` with `Set`/`Delete`/`Delta`, `set_write_flush_cadence`, `flush_write_boundary`, `close`), `impl ReopenableStorage`, and a new shared conformance fn `set_then_delta_in_one_batch_observes_staged_value`. Behavior contracts: batches are all-or-nothing; a `Delta` in a batch observes earlier operations of the same batch; `flush_write_boundary` reads-increments-writes `meta.boundary_seq` as a big-endian `u64` (exactly one row updated) with WAL sync forced; post-close calls error; close checkpoints (`TRUNCATE`) and errors on `busy` or partial checkpoint.

- [ ] **Step 1: Hoist the conformance delta builders and add the same-batch conformance fn**

In `crates/groove/src/storage/mod.rs`, inside `pub(crate) mod conformance`: the `record` and `delta` helper fns are currently _nested inside_ `delta_append_current_winner_observes_merged_state`. Move them unchanged to conformance-module level as `pub(crate) fn record(…)` / `pub(crate) fn delta(…)` (the existing conformance fn keeps calling them — pure hoist, no behavior change), then add:

```rust
    /// A Delta later in a write_many batch must observe values staged earlier
    /// in the SAME batch (read-your-own-writes inside the batch). The existing
    /// delta conformance applies deltas across separate batches only.
    pub(crate) fn set_then_delta_in_one_batch_observes_staged_value<S>(storage: S)
    where
        S: OrderedKvStorage,
    {
        let base = record(10, 1, b"base");
        let child = record(11, 2, b"child");

        storage
            .write_many(&[
                WriteOperation::set("records", b"row", &base),
                WriteOperation::delta(
                    "records",
                    b"row",
                    &delta(11, 2, vec![(10, 1)], child.clone()),
                ),
            ])
            .unwrap();

        // The delta's parent points at the staged base record; current-winner
        // merge must resolve to the child. If the delta had been applied
        // against the pre-batch state (None), the merge result would differ.
        assert_eq!(storage.get("records", b"row").unwrap(), Some(child));
    }
```

Also add a `MemoryStorage` call in the existing `mod tests` of `mod.rs` (memory is the semantic oracle and must pass the new fn too):

```rust
    #[test]
    fn memory_storage_observes_staged_values_for_same_batch_deltas() {
        let storage = MemoryStorage::new(&["records"]);
        conformance::set_then_delta_in_one_batch_observes_staged_value(storage);
    }
```

- [ ] **Step 2: Write the failing sqlite tests**

Extend the sqlite test-module imports:

```rust
    use super::super::{
        Durability, Error, OrderedKvStorage, ReopenableStorage, StorageDelta, StorageDeltaKind,
        WriteOperation,
    };
```

Append inside `mod tests`:

```rust
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

        // Valid same-batch Set → Delta visibility, via the shared conformance fn.
        let dir2 = tempfile::tempdir().unwrap();
        let storage2 = open_records(&dir2);
        super::super::conformance::set_then_delta_in_one_batch_observes_staged_value(storage2);

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

    fn read_boundary_seq(path: &std::path::Path) -> u64 {
        let conn = rusqlite::Connection::open(path).unwrap();
        let bytes: Vec<u8> = conn
            .query_row("SELECT value FROM meta WHERE key = 'boundary_seq'", [], |row| row.get(0))
            .unwrap();
        u64::from_be_bytes(bytes.try_into().expect("boundary_seq is 8 bytes"))
    }

    #[test]
    fn boundary_flush_increments_the_sequence_exactly() {
        let dir = tempfile::tempdir().unwrap();
        let storage = open_records(&dir);
        storage.set_write_flush_cadence(2).unwrap();
        storage.write_many(&[WriteOperation::set("records", b"a", b"1")]).unwrap();
        storage.write_many(&[WriteOperation::set("records", b"b", b"2")]).unwrap(); // cadence hit → bump 1
        storage.flush_write_boundary().unwrap(); // explicit → bump 2
        storage.close().unwrap();

        assert_eq!(
            read_boundary_seq(&db_path(&dir)),
            2,
            "exactly two boundary bumps: one cadence-driven, one explicit"
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

    #[test]
    fn close_reports_checkpoint_blocked_by_concurrent_reader() {
        let dir = tempfile::tempdir().unwrap();
        let storage = open_records(&dir);
        storage.set("records", b"a", b"one").unwrap();

        // A second connection holding an open read transaction blocks a
        // TRUNCATE checkpoint; close must surface that, not silently succeed.
        let reader = rusqlite::Connection::open(db_path(&dir)).unwrap();
        let tx = reader.unchecked_transaction().unwrap();
        let _: i64 = tx
            .query_row("SELECT COUNT(*) FROM kv", [], |row| row.get(0))
            .unwrap();

        let error = storage.close().unwrap_err();
        assert!(
            matches!(&error, Error::InvalidStorageLayout(message) if message.contains("checkpoint")),
            "blocked checkpoint must error, got: {error:?}"
        );
        drop(tx);
        drop(reader);
    }
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cargo test -p groove --no-default-features --features sqlite -j 8`
Expected: FAIL — `write_many` panics `todo!`, `reopen`/`close` override missing; the new conformance fn compiles (memory test passes).

- [ ] **Step 4: Implement**

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
            for (index, operation) in operations.iter().enumerate() {
                #[cfg(test)]
                tests::kill_test_barrier(index);
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
        // store's durability mode. The counter is a fixed-width big-endian u64
        // decoded and incremented in Rust — SQL blob/int casts do not produce
        // fixed-width bytes.
        self.with_connection(|connection| {
            let bytes: Vec<u8> = connection
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

            connection.pragma_update(None, "synchronous", "FULL")?;
            let update = connection.execute(
                "UPDATE meta SET value = ?1 WHERE key = 'boundary_seq'",
                [&next[..]],
            );
            let restore = match self.durability {
                Durability::FullSync => "FULL",
                Durability::WalNoSync => "NORMAL",
            };
            connection.pragma_update(None, "synchronous", restore)?;
            let changed = update?;
            if changed != 1 {
                return Err(Error::InvalidStorageLayout(format!(
                    "boundary_seq update touched {changed} rows, expected 1"
                )));
            }
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
        // Bounded close latency: a blocked checkpoint should fail fast, not
        // wait out the 5s operational busy_timeout. If the checkpoint fails,
        // the connection still drops (resources released) and the error is
        // reported — the WAL is replayed on the next open.
        connection.busy_timeout(std::time::Duration::from_millis(250))?;
        let (busy, log_frames, checkpointed): (i64, i64, i64) = connection.query_row(
            "PRAGMA wal_checkpoint(TRUNCATE)",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        if busy != 0 || log_frames != checkpointed {
            return Err(Error::InvalidStorageLayout(format!(
                "close checkpoint incomplete (busy={busy}): {checkpointed}/{log_frames} WAL frames"
            )));
        }
        connection.close().map_err(|(_, error)| Error::Sqlite(error))?;
        Ok(())
    }
```

(SQLite's `wal_checkpoint` pragma returns `(busy, log, checkpointed)`; `busy = 1` means the checkpoint could not run to completion because of a competing reader/writer — for `TRUNCATE` that is a failure even when the two frame counts happen to match. On an empty WAL the counts are `0`/`-1` and equal — complete.)

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

Add the (for now inert) barrier hook at the top of `mod tests` — Task 6 gives it its real body; a stub keeps this task compiling:

```rust
    /// Test-only failpoint used by the SIGKILL suite (Task 6). Inert unless
    /// the kill-test env vars are set.
    pub(super) fn kill_test_barrier(_op_index: usize) {}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p groove --no-default-features --features sqlite -j 8`
Expected: all sqlite tests + the memory same-batch conformance test PASS. (The reader-blocked-checkpoint test takes ~250ms by design.)

- [ ] **Step 6: Commit Tasks 2–4 as one change**

```bash
git add crates/groove/Cargo.toml crates/groove/src/storage/mod.rs crates/groove/src/storage/sqlite.rs
git commit -m "feat(groove): sqlite ordered-kv storage backend"
```

---

### Task 5: Conformance parity with the shared suite

**Files:**

- Modify: `crates/groove/src/storage/sqlite.rs` (tests only)

**Interfaces:**

- Consumes: `super::super::conformance::{persistence_order_and_batch_atomicity, reopen_preserves_data_and_adds_families, delta_append_current_winner_observes_merged_state, set_then_delta_in_one_batch_observes_staged_value}` — the same parametrized functions `opfs.rs:373-475` runs for `NativeBtreeStorage`.

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

Fixture note: all three shared conformance functions seed the `"records"` family only; `reopen_preserves_data_and_adds_families` itself reopens with `["records", "indices"]` — so opening with `&["records"]` is exactly right, matching the `opfs.rs:373-480` call sites. (The same-batch fn is already exercised in Task 4's test.)

- [ ] **Step 2: Run to verify current state**

Run: `cargo test -p groove --no-default-features --features sqlite sqlite_storage -j 8`
Expected: PASS if Tasks 3–4 are correct — the conformance functions are the cross-backend oracle; any failure here is a real contract divergence to fix in `sqlite.rs`, not in the test.

- [ ] **Step 3: Run the whole groove suite in all three feature configurations**

Run: `cargo test -p groove -j 8 && cargo test -p groove --features sqlite -j 8 && cargo test -p groove --no-default-features --features sqlite -j 8`
Expected: PASS ×3; the default-feature run proves no regression for existing backends, the combined run proves rocksdb+sqlite coexistence, the no-default run is the jazz-rn build shape.

- [ ] **Step 4: Commit**

```bash
git add crates/groove/src/storage/sqlite.rs
git commit -m "test(groove): run shared storage conformance over sqlite backend"
```

---

### Task 6: Abrupt-termination durability tests

**Files:**

- Modify: `crates/groove/src/storage/sqlite.rs` (barrier body + tests, `#[cfg(unix)]` for the kill tests)

**Interfaces:**

- Consumes: the self-exec pattern — the test spawns `std::env::current_exe()` (the unit-test binary; the library is compiled with `cfg(test)` there, so the barrier is live in the child) filtered by unique substring, then SIGKILLs it at a controlled point.
- Produces: a real body for `kill_test_barrier` (env-triggered, inert in production and in unrelated tests).

- [ ] **Step 1: Give the barrier its real body**

Replace the Task 4 stub in `mod tests`:

```rust
    /// Test-only failpoint for the SIGKILL suite. When the env vars are set,
    /// the Nth write_many call parks forever INSIDE its open transaction after
    /// staging `barrier_after` operations — the parent then kills the process
    /// mid-transaction, deterministically.
    ///
    /// Inert unless SQLITE_KILL_TEST_BARRIER_CALL is set, so every other test
    /// (and production code — this module is cfg(test)) is unaffected.
    pub(super) fn kill_test_barrier(op_index: usize) {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static WRITE_MANY_CALL: AtomicUsize = AtomicUsize::new(0);

        let Ok(barrier_call) = std::env::var("SQLITE_KILL_TEST_BARRIER_CALL") else { return };
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
```

- [ ] **Step 2: Write the child writer and the kill tests**

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
            // Batch 1 (complete): generation 0, keys gen0:0..gen0:7.
            // Batch 2 (parked mid-transaction by the barrier, then killed):
            // generation 1. The barrier env (set by the parent) parks the 2nd
            // write_many call after 4 staged ops — readiness is signaled from
            // INSIDE the open transaction, so the kill is deterministic.
            for generation in 0u8..2 {
                let keys: Vec<Vec<u8>> = (0u8..8)
                    .map(|slot| vec![b'g', b'e', b'n', generation, b':', slot])
                    .collect();
                let operations: Vec<WriteOperation<'_>> = keys
                    .iter()
                    .map(|key| WriteOperation::set("records", key, &[generation]))
                    .collect();
                storage.write_many(&operations).unwrap();
            }
            unreachable!("barrier must park inside generation 1's transaction");
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
    fn run_kill_test(role: &str, barrier: Option<(usize, usize)>) -> (tempfile::TempDir, SqliteStorage) {
        let dir = tempfile::tempdir().unwrap();
        let db = db_path(&dir).to_string_lossy().into_owned();
        let ready = dir.path().join("ready").to_string_lossy().into_owned();
        // Substring filter (NOT --exact: that would require the full module
        // path `storage::sqlite_storage::tests::sqlite_kill_test_child_writer`).
        // The name is unique in the crate, so the filter runs exactly one test.
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command
            .args(["sqlite_kill_test_child_writer", "--nocapture"])
            .env("SQLITE_KILL_TEST_ROLE", role)
            .env("SQLITE_KILL_TEST_DB", &db)
            .env("SQLITE_KILL_TEST_READY", &ready);
        if let Some((call, after)) = barrier {
            command
                .env("SQLITE_KILL_TEST_BARRIER_CALL", call.to_string())
                .env("SQLITE_KILL_TEST_BARRIER_AFTER", after.to_string());
        }
        let mut child = command.spawn().unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        while !std::path::Path::new(&ready).exists() {
            assert!(std::time::Instant::now() < deadline, "child never became ready");
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        child.kill().unwrap(); // SIGKILL on unix
        child.wait().unwrap();
        let storage = SqliteStorage::open(db_path(&dir), &["records"]).unwrap();
        (dir, storage)
    }

    #[cfg(unix)]
    #[test]
    fn sigkill_after_full_sync_commit_preserves_every_commit() {
        let (_dir, storage) = run_kill_test("full_sync", None);
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
        let (_dir, storage) = run_kill_test("wal_no_sync", None);
        assert_eq!(
            storage.get("records", b"before-boundary").unwrap(),
            Some(b"1".to_vec()),
            "WalNoSync: writes before flush_write_boundary must survive SIGKILL"
        );
    }

    #[cfg(unix)]
    #[test]
    fn sigkill_mid_batch_preserves_whole_generations_only() {
        // Barrier parks the 2nd write_many call (generation 1) after staging
        // 4 of its 8 ops; the kill lands inside that open transaction.
        let (_dir, storage) = run_kill_test("torn_batch", Some((2, 4)));
        let mut generation_counts = std::collections::BTreeMap::<u8, u32>::new();
        storage
            .scan_prefix("records", b"gen", &mut |key, _value| {
                *generation_counts.entry(key[3]).or_insert(0) += 1;
                Ok(())
            })
            .unwrap();
        // Generation 0 committed before the barrier: it must be complete.
        // Generation 1 was mid-transaction at kill time: it must be absent.
        // An empty map would mean the barrier fired too early — that is a
        // failure, not a vacuous pass.
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
```

- [ ] **Step 3: Run the kill tests**

Run: `cargo test -p groove --no-default-features --features sqlite sigkill -j 8 -- --test-threads=1`
Expected: 3 tests PASS (`full_sync`, `wal_no_sync` boundary, deterministic `torn_batch`). Also run the child no-op path: `cargo test -p groove --no-default-features --features sqlite sqlite_kill_test_child_writer -j 8` — PASS immediately (env unset ⇒ no-op), and the full sqlite suite once more to prove the barrier is inert: `cargo test -p groove --no-default-features --features sqlite -j 8`.

- [ ] **Step 4: Commit**

```bash
git add crates/groove/src/storage/sqlite.rs
git commit -m "test(groove): sigkill durability coverage for sqlite backend"
```

---

### Task 7: Re-point jazz's `sqlite` feature and add the node-harness slot

**Files:**

- Modify: `crates/jazz/Cargo.toml:12` (feature) and `crates/jazz/Cargo.toml:75` (drop rusqlite dependency)
- Modify: `crates/jazz/src/node/tests/sync.rs` (sqlite sibling of the btree node constructor + matrix leg)

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

### Task 8: Landing-tier gates, gate-list corrections, and documentation

**Files:**

- Modify: `AGENTS.md` (canonical gate list — `.claude/CLAUDE.md` is a symlink to it; edit and stage `AGENTS.md`)
- Modify: `crates/jazz/SPEC/13_db_api.md` (RN storage-driver open question)
- Modify: `crates/jazz/SPEC/17_integrability.md` (§17.6 implementation status)
- Modify: `dev/RN_BINDING_REWRITE_DESIGN.md` (status line only)

**Interfaces:**

- Consumes: everything above.
- Produces: M1 recorded as landed; gates updated so the backend cannot rot silently; the stale `jazz-server` gate line corrected; SPEC statements no longer contradict the landed code.

- [ ] **Step 1: Update the canonical gate list in AGENTS.md**

In `AGENTS.md`, in the canonical-gates bullet list:

1. Add after the `cargo test -p groove` line:

```markdown
- `cargo test -p groove --no-default-features --features sqlite` (mobile storage
  backend in the exact feature shape jazz-rn builds; added with M1 of
  dev/RN_BINDING_REWRITE_DESIGN.md)
```

2. Correct the stale line `cargo test -p jazz-server`: no such package exists — `jazz-server` is a `[[bin]]` target of the `jazz` package (`crates/jazz/Cargo.toml:158`). Replace it with:

```markdown
- `cargo test -p jazz --bin jazz-server`
```

This is a correction of a gate that cannot currently run as written, not a gate-policy change; call it out in the PR description for the gate owner's eyes.

- [ ] **Step 2: Record the decision in the SPECs**

These edits keep authoritative docs from contradicting landed code; keep them surgical:

1. `crates/jazz/SPEC/13_db_api.md`, the 🔶 **React Native storage driver** open question (~line 614): rewrite as resolved — the route is the `crates/jazz-rn` native module over a Rust-side groove SQLite `OrderedKvStorage` backend (landed; see `dev/RN_BINDING_REWRITE_DESIGN.md`), not `op-sqlite`/`expo-sqlite` TS drivers; what remains open there is only the RN binding work itself (M2–M5).
2. `crates/jazz/SPEC/17_integrability.md`, §17.6 "Implementation status" (~line 185): append one sentence — the SQLite `OrderedKvStorage` backend is implemented (groove `sqlite` feature) per the RN owner's 2026-08-07 decision to go directly to SQLite, superseding the RocksDB-first ordering for RN.

- [ ] **Step 3: Mark M1 landed in the design doc**

In `dev/RN_BINDING_REWRITE_DESIGN.md`, change the Status line to record M1: `Status: in progress (M1 landed <date>; M2–M5 pending).`

- [ ] **Step 4: Run the full landing-tier gate set**

The jazz-private sensitive-data guard must run for this batch: it is part of the landing tier (`AGENTS.md`), and it is currently NOT installed on this machine (lefthook warns). **Clone `jazz-private` so the hook resolves, or obtain an explicit owner exception — its absence blocks the push.**

Run, in order (use `-j` fitting the box):

```bash
cargo test -p jazz -j 8
cargo test -p groove -j 8
cargo test -p groove --features sqlite -j 8
cargo test -p groove --no-default-features --features sqlite -j 8
cargo test -p jazz --no-default-features --features test -j 8
cargo test -p jazz --bin jazz-server -j 8
cargo check -p jazz-sim --benches
dev/gates/ts-wire-codec.sh
JAZZ_SEED_COUNT=300 cargo test -p jazz m3_maintained_one_shot_differential_oracle
cargo test -p jazz --test incremental_delivery_canary maintained_relation_include_single_row_changes_are_scale_independent -- --exact
dev/benchmarks/smoke.sh
```

Expected: all green. Storage was touched, so smoke.sh is mandatory here.

- [ ] **Step 5: Commit**

```bash
git add AGENTS.md crates/jazz/SPEC/13_db_api.md crates/jazz/SPEC/17_integrability.md dev/RN_BINDING_REWRITE_DESIGN.md
git commit -m "chore: add sqlite backend gate and record M1"
```

---

## Not in this plan (deliberately)

- SQL-native `scan_prefix_reverse` / `last_with_prefix*` (defaults are correct; optimize only when a profile demands it — record in the M2 plan if it comes up).
- `approximate_class_bytes` heuristics (contract answer `Ok(None)`).
- Any `crates/jazz-rn` change (M2 plan), TS change (M3 plan), or mobile build (M4 plan).
- Wiring the new conformance additions (`0xFF`, kill tests) over NativeBtree/RocksDB — the same-batch delta conformance fn IS wired to Memory (oracle); broader rollout is its own investigation if a latent failure appears, not an M1 blocker.
