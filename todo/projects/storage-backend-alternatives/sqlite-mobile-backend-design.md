# SQLite Mobile Backend

Storage backend targeting React Native / mobile (iOS, Android) using `rusqlite` with bundled SQLite.

## Schema

Single KV table, matching the flat key-value model used by Fjall and RocksDB:

```sql
CREATE TABLE IF NOT EXISTS kv (
    key   BLOB    PRIMARY KEY,
    value BLOB    NOT NULL
) WITHOUT ROWID;
```

`WITHOUT ROWID` clusters on `key`, giving ordered iteration for prefix and range scans without a separate index.

### Why BLOB, not TEXT

Keys are BLOB rather than TEXT. Our key scheme (see `key_codec.rs`) produces UTF-8 strings, but the prefix-scan and range-scan callbacks rely on byte-ordered comparison. SQLite TEXT uses its own collation rules, which could disagree with raw byte ordering — the prefix upper bound trick (increment last byte) assumes `memcmp` semantics. BLOB comparison is `memcmp`, so it matches exactly. Keys bind as `key.as_bytes()` and decode back to `String` on read since the underlying bytes are valid UTF-8. This avoids needing a reversible ASCII-safe encoding layer that TEXT would require to guarantee correct ordering.

## Storage struct

```rust
struct SqliteInner {
    conn: rusqlite::Connection,
    path: PathBuf,
}

pub struct SqliteStorage {
    inner: RefCell<Option<SqliteInner>>,
}
```

Mirrors the Fjall/RocksDB shape: `RefCell<Option<Inner>>` for interior mutability and take-on-close semantics. A `with_inner()` helper borrows the inner and panics if already closed. `close()` calls `take()` on the inner, runs a final WAL checkpoint, then drops the connection — releasing the file lock so the same path can be reopened. The conformance suite's reopen-after-close test verifies this.

## Callback mapping

Delegates to `storage_core.rs` functions via callbacks, same pattern as Fjall and RocksDB.

### Primitives

| Callback                     | SQLite SQL                                                            |
| ---------------------------- | --------------------------------------------------------------------- |
| `get(key)`                   | `SELECT value FROM kv WHERE key = ?1`                                 |
| `set(key, value)`            | `INSERT OR REPLACE INTO kv (key, value) VALUES (?1, ?2)`              |
| `delete(key)`                | `DELETE FROM kv WHERE key = ?1`                                       |
| `scan_prefix(prefix)`        | `SELECT key, value FROM kv WHERE key >= ?1 AND key < ?2 ORDER BY key` |
| `scan_prefix_keys(prefix)`   | `SELECT key FROM kv WHERE key >= ?1 AND key < ?2 ORDER BY key`        |
| `scan_key_range(start, end)` | `SELECT key FROM kv WHERE key >= ?1 AND key < ?2 ORDER BY key`        |

`scan_prefix` and `scan_prefix_keys` compute the upper bound by incrementing the last byte of the prefix (same as RocksDB's `prefix_upper_bound()`). `scan_key_range` takes explicit start/end bounds from the caller.

### Read vs write paths

Read-only trait methods (`load_object_metadata`, `load_branch`, `load_catalogue_manifest`, index lookups/range/scan) call the primitives directly against the connection.

Write trait methods (`append_commit`, `delete_commit`, `set_branch_tails`, `store_ack_tier`, `append_catalogue_manifest_op`, index insert/remove) open a `SAVEPOINT` and pass tx-scoped get/set/delete/scan helpers to the `_core` functions. This lets read-modify-write sequences (e.g. `append_commit_core` reads tips, appends commit, updates tips) see their own uncommitted writes within the same savepoint. The savepoint is released on success, rolled back on error.

## PRAGMA configuration

```sql
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA cache_size = -2000;
PRAGMA busy_timeout = 5000;
PRAGMA foreign_keys = OFF;
```

- **`journal_mode = WAL`** — Write-Ahead Logging. The default rollback journal blocks readers during writes. WAL lets readers proceed concurrently with a single writer, which matters on mobile where sync and UI may touch the DB from different contexts. WAL also batches writes more efficiently — appending to a log rather than rewriting pages in place.
- **`synchronous = NORMAL`** — With WAL mode, NORMAL is safe against data corruption on power loss (WAL checksums detect partial writes). FULL would `fsync` on every commit, which is expensive on mobile flash storage. The risk with NORMAL is losing the last transaction on a crash — acceptable for a local cache that syncs with the server.
- **`cache_size = -2000`** — 2MB page cache (negative value = KiB). Conservative default that avoids competing with the app's own memory budget. Tunable later if profiling shows cache pressure.
- **`busy_timeout = 5000`** — If another connection holds a lock, retry for up to 5 seconds before returning SQLITE_BUSY. Defensive measure — our single-connection model shouldn't contend, but this avoids hard failures if something unexpected holds the WAL.
- **`foreign_keys = OFF`** — We have a single table with no foreign key relationships. Disabling avoids per-statement FK enforcement overhead.

### flush, flush_wal, and close

- `flush_wal()` → **no-op** (trait default). SQLite targets mobile clients, not the cloud server. In WAL mode with autocommit, each SAVEPOINT/RELEASE commits to the WAL — writes survive process crashes without an explicit fsync. Power-safe durability is the cloud server's job (RocksDB/Fjall handle that via their own `flush_wal` implementations). Keeping this as a no-op means `batched_tick` does zero storage I/O on mobile, which is the right trade-off.
- `flush()` → `PRAGMA wal_checkpoint(PASSIVE)` — non-blocking compaction that moves checkpointed pages from WAL to the main DB file without blocking concurrent readers. This is cleanup, not a durability barrier.
- `close()` → passive checkpoint, then drop the connection. The checkpoint is best-effort cleanup. Dropping the connection releases the file lock so the same path can be reopened (required by the conformance suite's reopen-after-close test).

## Feature flag and mobile wiring

### jazz-tools crate

```toml
# crates/jazz-tools/Cargo.toml
rusqlite = { version = "0.34", features = ["bundled"], optional = true }

[features]
sqlite = ["dep:rusqlite"]
```

In `storage/mod.rs`: add `mod sqlite; pub use sqlite::SqliteStorage;` gated behind `#[cfg(feature = "sqlite")]`.

### jazz-rn crate

`crates/jazz-rn/rust/Cargo.toml`: change the `jazz_tools` dependency from `features = ["fjall"]` to `features = ["sqlite"]`.

`crates/jazz-rn/rust/src/lib.rs`:

- Replace `use jazz_tools::storage::{FjallStorage, Storage}` → `use jazz_tools::storage::{SqliteStorage, Storage}`
- Replace the type alias `type RnCoreType = RuntimeCore<FjallStorage, ...>` → `RuntimeCore<SqliteStorage, ...>`
- Replace `FjallStorage::open(&resolved_data_path, cache_size_bytes)` with `SqliteStorage::open(&resolved_data_path)` (SQLite doesn't take a cache size arg — cache is set via PRAGMA)
- Replace the default filename from `{sanitized_app_id}.fjall` → `{sanitized_app_id}.sqlite`
- Replace the error text from `"Failed to open Fjall storage"` → `"Failed to open SQLite storage"`

## Conformance tests

Plug in via `storage_conformance_tests_persistent!` macro with tempdir factory and reopen factory, identical to Fjall and RocksDB test modules.

## Integration tests

Cover SQLite with the same integration test suite used for RocksDB — exercising real read/write paths through `RuntimeCore`, multi-user sync scenarios, and schema lifecycle, not just the `Storage` trait in isolation.

## Benchmark

Add SQLite engine to `crates/opfs-btree/benches/compare_native.rs` cross-engine benchmark.

## Mobile build verification

The `bundled` feature in rusqlite compiles SQLite from C source, which handles cross-compilation. Normal CI excludes jazz-rn (`cargo test --exclude jazz-rn` in `.github/workflows/ci.yml`), so verification runs through the existing RN build workflows in `.github/workflows/rn-build-reusable.yml` which compile for Android (aarch64, armv7, i686, x86_64) and iOS (aarch64, aarch64-sim, x86_64) targets.
