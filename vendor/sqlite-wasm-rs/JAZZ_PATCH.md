# Jazz SQLite feature patch

Pristine source: `sqlite-wasm-rs` 0.5.5, crates.io archive, upstream Git commit
`f2bb5109c07bf3d7ab800704c0f2373e6c7aa3b4`.
Source and license: <https://github.com/Spxg/sqlite-wasm-rs/tree/f2bb5109c07bf3d7ab800704c0f2373e6c7aa3b4>.
The MIT license is preserved in LICENSE. The library source, build script, shim,
SQLite amalgamation (`sqlite3/`), Cargo manifest, README and license are copied.
The SQLite3 Multiple Ciphers amalgamation (`sqlite3mc/`, 14 MB) is omitted, so
the `sqlite3mc` feature fails with a `compile_error!`. Jazz doesn't enable it.
The crates.io metadata files `.cargo_vcs_info.json`, `Cargo.toml.orig` and
`Cargo.lock` are also omitted. Every copied file is byte-identical to the
archive, except `build.rs` as `jazz.patch` records.

`jazz.patch` records every change to copied upstream files. To reproduce, unpack
the crates.io 0.5.5 archive and run `patch -p1 < jazz.patch` at its root.
The package keeps its upstream name. Root `Cargo.toml` installs it with
`[patch.crates-io]`, so rusqlite's wasm FFI (`ffi-sqlite-wasm-rs`) and
`jazz-wasm` link this one copy. Two copies would link two SQLites.

Pristine crates.io archive SHA-256 (also the `Cargo.lock` checksum it replaces):
`dc3efc0da82635d7e1ced0053bbbfa8c7ab9645d0bf36ceb4f7127bb85315d75`.

## Why a local patch

Upstream 0.5.5 hard-codes its SQLite compile flags in `build.rs`. It has no
feature or environment variable to drop one, and Cargo's `CFLAGS` can't `-U`
them, because `cc` puts environment flags before the crate's own `-D` flags.
The patch removes the options that only add SQL features Jazz never uses. In
the browser build (release, `wasm-opt -g`) they cost 299 KB raw, 131 KB gzip -9
and 94 KB Brotli q11. The removed options:

| Option                          | What it adds                                                                   | Why Jazz doesn't need it                                                             |
| ------------------------------- | ------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------ |
| `SQLITE_ENABLE_FTS5`            | full-text search virtual table                                                 | no `fts5` tables or queries                                                          |
| `SQLITE_ENABLE_RTREE`           | R-Tree virtual table                                                           | no `rtree` tables                                                                    |
| `SQLITE_ENABLE_SESSION`         | session/changeset C API                                                        | rusqlite's `session` feature is off; no `sqlite3session_*` calls                     |
| `SQLITE_ENABLE_PREUPDATE_HOOK`  | pre-update hook C API (needed by SESSION)                                      | rusqlite's `preupdate_hook` feature is off                                           |
| `SQLITE_ENABLE_DBSTAT_VTAB`     | `dbstat` virtual table                                                         | not queried                                                                          |
| `SQLITE_ENABLE_DBPAGE_VTAB`     | `sqlite_dbpage` virtual table                                                  | not queried                                                                          |
| `SQLITE_ENABLE_BYTECODE_VTAB`   | `bytecode()` / `tables_used()`                                                 | not queried                                                                          |
| `SQLITE_ENABLE_STMTVTAB`        | `sqlite_stmt` virtual table                                                    | not queried                                                                          |
| `SQLITE_ENABLE_OFFSET_SQL_FUNC` | `sqlite_offset()` SQL function                                                 | not used in any SQL                                                                  |
| `SQLITE_ENABLE_UNLOCK_NOTIFY`   | `sqlite3_unlock_notify`                                                        | needs shared cache, which the build omits; rusqlite's `unlock_notify` feature is off |
| `SQLITE_ENABLE_COLUMN_METADATA` | `sqlite3_column_{database,table,origin}_name`, `sqlite3_table_column_metadata` | rusqlite's `column_metadata` feature is off                                          |

The build keeps `MATH_FUNCTIONS`, `API_ARMOR`, `UNKNOWN_SQL_FUNCTION` and the
platform options (`OS_OTHER`, `THREADSAFE=0`, `OMIT_LOAD_EXTENSION`, …).
The pregenerated Rust bindings in `src/bindings` stay as upstream generated
them. They still declare the removed C functions, but nothing calls them, so the
link never needs them. If something starts calling one, the WASM gets an
unresolved `env` import and the build fails.

Before a Jazz feature uses one of these, re-enable it in `build.rs` and
`jazz.patch`, and remove its prefix from the release check below.

## Validation

`dev/artifacts/build.mjs` checks every release WASM build with
`assertNoOmittedSqliteExtensions` (`dev/artifacts/wasm-names.mjs`), which
fails the build if the name section still holds FTS5, R-Tree, session,
`dbstat`, `sqlite_dbpage`, `sqlite_stmt` or `bytecode()` functions. Its unit tests are in
`dev/artifacts/wasm-names.test.mjs`.

## Removal

sqlite-wasm-rs 0.6.3 adds `SQLITE_WASM_RS_FLAGS`, which takes `-U` flags.
rusqlite 0.40 requires sqlite-wasm-rs 0.5, and rusqlite 0.42 is the first
release that accepts 0.6.3. When Jazz moves to rusqlite ≥ 0.42, delete this
directory and the `[patch.crates-io]` entry, and instead set
`SQLITE_WASM_RS_FLAGS` in `.cargo/config.toml` `[env]` to `-U` the same options.
