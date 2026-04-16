# Storage — Status Quo

Storage is the synchronous substrate underneath the Jazz runtime.

Everything above it assumes reads and writes happen immediately:

- queries settle synchronously
- local mutations update subscriptions in the same call stack
- sync replay can apply row batch entries without waiting for an async database callback

That is why the `Storage` trait is synchronous even in the browser. The browser gets there by putting durable storage in a dedicated worker, where OPFS exposes synchronous file access.

For the current direct/transactional batch lifecycle layered on top of that storage, see
[Batches](batches.md).

## What Storage Owns

The current storage layer is responsible for six kinds of data:

### 1. Raw tables

Application tables are stored as raw keyed tables. This is the lowest-level "table first" surface: storage knows how to put, get, delete, and scan raw rows by table name and encoded key.

### 2. Indices

Queries are index-first, so storage also owns index persistence:

- point lookups
- range scans
- full scans by prefix/order
- insert/remove maintenance

### 3. Row locators and metadata

The engine keeps a row locator that maps a logical row id back to its owning table. That lets query and sync code start from a row id and still find the correct raw table without a separate object catalog.

Storage also owns small engine metadata rows used for runtime bookkeeping.

### 4. Row histories and visible entries

For user data, storage persists both:

- the append-friendly history region for row batch entries
- the compact visible region for current reads

Both regions are stored as flat `row_format` rows containing reserved `_jazz_*` columns plus the
table's user columns. Storage exposes them through dedicated helpers such as history scans,
visible-row loads, and row-state patch operations.

### 5. Batch bookkeeping

Replayable write state is also durable storage state now. Storage persists:

- branch ord registry in `__branch_ord_registry`
- local batch records in `__local_batch_record`
- authoritative settlements in `__authoritative_batch_settlement`
- sealed transactional submissions in `__sealed_batch_submission`

The branch ord registry is one durable row that stores:

- general branch-ord registry format version
- next branch ord counter
- the full `(branch_ord, branch_name)` mapping set

That single-row shape matters because RocksDB and OPFS do not provide a cross-call atomic
multi-put primitive through the shared `Storage` trait. Keeping the whole mapping in one row
avoids torn `name -> ord` / `ord -> name` state after crashes.

The batch rows themselves are keyed by `batch:<batch_id_hex>` and let reconnect/restart recover
batch fate without depending on a live ack having been observed.

### 6. Catalogue entries

Schemas and lenses live in a separate `catalogue` table. They do not reuse the user-row history path, but they do reuse the same underlying storage and row encoding machinery.

## The Current Table-First Layout

At a high level, the durable model looks like this:

```text
raw user tables
  -> application rows and index keys

row-history namespaces
  -> one namespace per (storage kind, logical table, full schema hash)
  -> visible namespace-local keys: (branch, row_id)
  -> history namespace-local keys: (row_id, branch, batch_id)

system tables
  -> __metadata
  -> __row_locator
  -> __branch_ord_registry
  -> __local_batch_record
  -> __authoritative_batch_settlement
  -> __sealed_batch_submission
  -> catalogue
```

That is the core architectural shift to keep in mind while reading the rest of the runtime docs: the engine is organized around raw tables and engine-managed row metadata, not around a second abstraction layer that later gets reinterpreted as rows.

## Shared Row Encoding

Storage does not invent its own payload format. It relies on `row_format` for:

- encoding application rows
- encoding flat history and visible rows
- reprojection into column subsets
- deterministic decoding for engine-managed rows
- validating values against column descriptors

This shared binary format is what lets user rows, visible rows, history rows, and catalogue rows
all move through the system without every layer inventing a different shape.

At the durable storage level, row-history state is moving toward schema-qualified namespaces rather
than one mixed raw table per logical table. Each namespace carries a small header with:

- general storage format version
- full schema hash
- logical table name

Once a caller knows which namespace it is reading, that namespace header is what makes flat row
bytes self-describing enough to decode in O(1) without scanning all catalogue schema history.

In practice the engine now resolves namespaces like this:

- exact point loads prefer the row locator's persisted `origin_schema_hash`
- branch scans union all namespaces for that logical table and filter by the branch key inside each namespace

So namespace selection no longer depends on parsing branch-name short hashes during ordinary row
loads.

The full meaning of those namespaces and their local keys is documented in [Batches](batches.md).

## Durable Backends

### MemoryStorage

Used heavily in tests and for ephemeral runtimes. It gives the full storage surface without on-disk persistence.

### OpfsBTreeStorage

Used by browser workers. It stores durable state in OPFS through `opfs-btree`, which gives Jazz synchronous storage inside a dedicated worker.

This is why the browser runtime is split in two:

- main thread: in-memory runtime for UI-facing work
- worker: persistent runtime that owns OPFS and upstream sync

### SqliteStorage

Used by the native bindings that want a simple embedded durable store, including the current NAPI and React Native runtimes.

### RocksDBStorage

Used by the cloud/server side where high write volume and durable restart behavior matter most.

## Browser Topology

```text
Main thread runtime
  -> MemoryStorage
  -> immediate UI-facing queries and callbacks
  -> forwards durable sync traffic to worker

Dedicated worker runtime
  -> OpfsBTreeStorage
  -> upstream /sync and /events ownership
  -> durable local row histories, visible entries, and batch records
```

The important point is that both runtimes still use the same synchronous `Storage` trait. The browser-specific complexity lives in the worker split, not in two different storage APIs.

## Flushing and Durability

`RuntimeCore` now tracks whether a tick actually wrote to storage before asking the backend to flush its WAL/checkpoint state.

That means:

- read-only ticks stay cheap
- write-heavy ticks still get durable progress
- backends are free to map "flush" to the right durability primitive for that engine

## Key Files

| File                                          | Purpose                                                        |
| --------------------------------------------- | -------------------------------------------------------------- |
| `crates/jazz-tools/src/storage/mod.rs`        | Storage trait plus in-memory implementation and shared helpers |
| `crates/jazz-tools/src/storage/opfs_btree.rs` | Browser worker durable backend                                 |
| `crates/jazz-tools/src/storage/sqlite.rs`     | SQLite durable backend                                         |
| `crates/jazz-tools/src/storage/rocksdb.rs`    | RocksDB durable backend                                        |
| `crates/jazz-tools/src/row_format.rs`         | Shared row encoding                                            |
| `crates/jazz-wasm/src/runtime.rs`             | Browser runtime bridge into storage                            |
| `crates/jazz-napi/src/lib.rs`                 | SQLite-backed NAPI runtime                                     |
| `crates/jazz-cloud-server/src/server.rs`      | RocksDB-backed server runtime                                  |
