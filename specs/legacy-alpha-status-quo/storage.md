# Storage — Legacy Alpha Status Quo

> Historical alpha note: this document describes deleted or legacy `jazz-tools` alpha internals. It is retained for migration context only; do not treat module paths or implementation details here as active architecture.

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

### 1. Raw table instances

The low-level primitive is now a raw table instance:

- one durable header row with free-form key/value metadata
- one ordered key/value row space under that header

Every raw table instance has at least:

- `storage_kind`
- `storage_format_version`

Everything durable in Jazz is built from that same primitive:

- visible rows for one logical table + one full schema hash
- row history for one logical table + one full schema hash
- system tables such as metadata, row locators, branch ord registry, local batch records, sealed submissions, authoritative settlements, and catalogue entries

### 2. Indices

Queries are index-first, so storage also owns index persistence:

- point lookups
- range scans
- full scans by prefix/order
- insert/remove maintenance

### 3. Row locators and metadata

The engine keeps a row locator that maps a logical row id back to its owning logical table. That
is lineage/context metadata, not an exact storage pointer.

Exact raw-table routing now lives in dedicated system tables:

- `__visible_row_table_locator`: maps `(branch_name, row_id)` to the exact visible raw table
- `__history_row_batch_table_locator`: maps `(row_id, branch_name, batch_id)` to the exact
  history raw table

That split matters because point loads should be O(1) against the real current row-table instance,
while `RowLocator` still describes the logical row across schema evolution.

Storage also owns small engine metadata rows used for runtime bookkeeping.

### 4. Row histories and visible entries

For user data, storage persists both:

- the append-friendly history region for row batch entries
- the compact visible region for current reads

Both regions are raw table instances whose rows are flat `row_format` records containing reserved
`_jazz_*` columns plus the table's user columns. Storage exposes them through dedicated helpers
such as history scans, visible-row loads, and row-state patch operations.

Exact visible/history loads use the exact locator tables above and do not scan all row-table
headers on the hot path. Branch/table scans still union compatible raw tables by logical table.

### 5. Batch bookkeeping

Replayable write state is also durable storage state now. Storage persists:

- branch ord registry in `__branch_ord_registry`
- local batch records in `__local_batch_record`
- authoritative batch fate in `__authoritative_batch_settlement`
- sealed batch submissions in `__sealed_batch_submission`

`__authoritative_batch_settlement` is a legacy table name and row shape. New code treats it as the
durable `BatchFate` table: successful fate is keyed by `batch_id` and applies to the whole sealed
batch. The legacy `visible_members` field is forward-compatible compatibility data and should not
be required by hot visible-row loads. Additive sidecar tables or caches may be used to index
`batch_id -> fate/tier` without changing the existing row format.

The branch ord registry is one durable row that stores:

- general branch-ord registry format version
- next branch ord counter
- the full `(branch_ord, branch_name)` mapping set

That single-row shape matters because RocksDB and OPFS do not provide a cross-call atomic
multi-put primitive through the shared `Storage` trait. Keeping the whole mapping in one row
avoids torn `name -> ord` / `ord -> name` state after crashes.

The batch rows themselves are keyed by `batch:<batch_id_hex>` and let reconnect/restart recover
batch fate without depending on a live ack having been observed.

These are now just system raw table instances with uniform `row_format` rows. The migration slot
lives in the raw table header's `storage_format_version`, not inside every row payload.

### 6. Catalogue entries

Schemas and lenses live in a separate `catalogue` table. They do not reuse the user-row history path, but they do reuse the same underlying storage and row encoding machinery.

## The Legacy Alpha Table-First Layout

At a high level, the durable model looks like this:

```text
raw table instances
  -> raw app/index tables
  -> visible row tables
  -> history row tables
  -> system tables
```

That is the core architectural shift to keep in mind while reading the rest of the runtime docs: the engine is organized around raw tables and engine-managed row metadata, not around a second abstraction layer that later gets reinterpreted as rows.

## Raw Table Headers

Headers are free-form, but current row-table headers carry:

- `storage_kind`
- `storage_format_version`
- `logical_table_name`
- `schema_hash`

Current system-table headers carry:

- `storage_kind`
- `storage_format_version`

Examples:

- visible rows for `todos` at schema `abcd...`: one raw table instance with `storage_kind =
visible_rows`
- history rows for `todos` at schema `abcd...`: one raw table instance with `storage_kind =
row_history`
- local batch records: one raw table instance with `storage_kind = local_batch_record`

The important rule is that every row in one raw table instance has one uniform key format and one
uniform value format. Per-row payloads do not need their own format version markers.

## Shared Row Encoding

Storage does not invent its own payload format. It relies on `row_format` for:

- encoding application rows
- encoding flat history and visible rows
- reprojection into column subsets
- deterministic decoding for engine-managed rows
- validating values against column descriptors

This shared binary format is what lets user rows, visible rows, history rows, and catalogue rows
all move through the system without every layer inventing a different shape.

At the durable storage level, row-history state is stored in schema-qualified raw table instances
rather than one mixed raw table per logical table.

Read-time routing now works like this:

- exact point loads prefer the row locator's persisted `origin_schema_hash`
- branch scans union all raw row tables for that logical table and filter by the branch key inside each one
- once a raw table instance has been resolved, the caller already knows the exact row format for that table

That means row decode does not reread the raw table header for each row. The engine resolves raw
table context once, then decodes all rows in that table against the already-known descriptor.

This is also why durable decode no longer depends on branch-name short hashes or scanning all
historical catalogue schemas.

The full meaning of those row raw tables and their local keys is documented in
[Batches](batches.md).

## Representation and Allocation Guidance

The current performance rule is "no second formats."

The canonical record encoding is the format. Runtime code may share, slice, copy, project, or
reorder encoded records, but it should not create a parallel decoded representation and then keep
that representation alive as if it were the data model. Decoding is a boundary operation or a
fallback for genuinely computed expressions, not the normal internal representation for maintained
arrangements.

The review question is:

> Is there a format here that is not the record encoding?

If the answer is yes, the code needs a specific reason and a bounded lifetime.

The standing canaries are:

- memory amplification: peak RSS divided by encoded storage bytes
- allocations per materialized row in the customer cold-start benchmark

Current July 2026 baselines after the C-lane representation work:

- member 100% cold: about 6,000 allocations per row, 7.3s settle, and about 20x memory
  amplification
- member 100% warm had previously exposed higher amplification, around 46x, which remains a
  design-session target

The implemented delta representation follows the same rule:

- `RecordDelta` carries `bytes::Bytes` handles to encoded records
- pass-through operators clone handles, not record byte vectors
- transform operators build a batch of output records into `BytesMut`, freeze once, and emit
  `Bytes` slices for individual records
- consolidation uses in-place sort plus adjacent weight folding instead of per-call hash maps
- join-key construction uses inline small buffers for common keys

This is an ownership and buffering change only. The record bytes are still the same canonical
encoding, and storage-read boundaries wrap owned storage bytes into shared handles rather than
introducing a second payload format.

## Durable Backends

### MemoryStorage

Used heavily in tests and for ephemeral runtimes. It gives the full storage surface without on-disk persistence.

### OpfsBTreeStorage

Used by browser workers. It stores durable state in OPFS through `opfs-btree`, which gives Jazz synchronous storage inside a dedicated worker.

This is why the browser runtime is split in two:

- main thread: in-memory runtime for UI-facing work
- worker: persistent runtime that owns OPFS and upstream sync

### SqliteStorage

Used by native/client runtimes that want a simple embedded durable store. It is not a supported core `jazz-tools server` catalogue or sync backend; durable server storage is RocksDB-only for now.

### RocksDBStorage

Used by the cloud/server side where high write volume and durable restart behavior matter most. The core `jazz-tools server` uses RocksDB for both sync state and catalogue persistence when it is not running in ephemeral in-memory mode.

## Browser Topology

```text
Main thread runtime
  -> MemoryStorage
  -> immediate UI-facing queries and callbacks
  -> forwards durable sync traffic to worker

Dedicated worker runtime
  -> OpfsBTreeStorage
  -> upstream /apps/<appId>/ws ownership
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
| `crates/jazz-tools/src/server/hosted.rs`      | Hosted server runtime wiring over the durable storage backends |
