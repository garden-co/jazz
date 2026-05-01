# Runtime-Owned Schema Alignment

## Goal

Move schema-dependent row ordering and schema-version compatibility logic out of
TypeScript hot paths and into the Rust runtimes.

All runtimes must return row values in the client-declared schema order. This is
mandatory immediately across WASM, NAPI, React Native, and test runtimes.
TypeScript should stop polling runtime schema/hash state during ordinary writes
and reads.

## Motivation

After memoizing TypeScript runtime schema cache keys, write-heavy profiles still
spend significant time recomputing schema hashes during inserts:

```text
insert
-> createInternal
-> alignRowValuesToDeclaredSchema
-> getSchema
-> getSchemaHash
-> SchemaHash::compute
-> hash_row_descriptor
-> blake3
```

and:

```text
batch.insert / insert
-> resolveInputSchema
-> getSchema
-> getSchemaHash
-> SchemaHash::compute
```

The core problem is not that schema hash computation is slow in isolation. The
problem is that TypeScript asks the runtime to prove the current schema has not
changed on every row, even though client-mode runtimes already have a stable
current schema after construction.

Rust `SchemaManager` is already the owner of:

- current schema and current schema hash
- known/live historical schemas
- lens paths
- copy-on-write updates into the current schema branch
- row descriptors used for encoding, decoding, and policy evaluation

TypeScript should not duplicate schema reconciliation or use schema hash polling
as a cache-invalidation mechanism in the write loop.

## Schema Roles

Use these terms consistently:

- **Declared schema**: the schema passed by this client bundle at runtime
  construction. This is the schema generated TypeScript table/query handles are
  typed against.
- **Current runtime schema**: the Rust client-mode write target held by
  `SchemaManager`. For client runtimes this is stable after construction.
- **Known schema**: any schema learned through catalogue state.
- **Live historical schema**: a known schema that is connected to the current
  schema through a live lens path and participates in branch/schema-family query
  planning.
- **Server explicit schema context**: server-mode query/write context for serving
  multiple client schema hashes. This remains dynamic and explicit.

Learning migrations/catalogue entries can add known/live historical schemas. It
must not make ordinary client writes revalidate the current write schema per row.

## Mandatory Runtime Contract

Every runtime binding must expose this contract:

```ts
returnsDeclaredSchemaRows: true;
```

For runtimes with this capability, all row values crossing the FFI boundary must
already be aligned to the declared schema order for the caller.

This applies to:

- `jazz-wasm`
- `jazz-napi`
- `jazz-rn`

There should be no supported runtime where TypeScript must call
`getSchema()` or `getSchemaHash()` during ordinary CRUD/query/subscription result
handling to reorder row values.

Because Jazz is still alpha, there is no legacy compatibility requirement for
runtime bindings that do not satisfy this contract. Tests and fake runtimes
should be updated to the new expectation instead of preserving old behavior.

## Runtime Responsibilities

### Writes

For `insert` and `insertWithSession`, the runtime should:

1. accept input values keyed by column name
2. encode/write using Rust's current schema context
3. return inserted row values aligned to the declared schema table descriptor

For `upsert`, `update`, and `delete`, no row value alignment is needed unless a
method returns row values in the future.

### Queries

For query result rows, the runtime should:

1. execute against the current/live schema context as it does today
2. transform historical rows through lenses as needed
3. align output row values to the declared query output table
4. recursively align included row values and array subquery rows

Projection rows must preserve the projection order requested by the query. Only
full table row descriptors should be reordered against declared schema columns.

### Subscriptions

Subscription initial snapshots and deltas should use the same declared-schema
alignment rules as queries.

For deltas, inserted/updated row payloads must be aligned before crossing FFI.
Deleted rows or metadata-only deltas should not require schema work.

### Schema Hash Access

`getSchemaHash()` should be an introspection/debug API, not an internal hot-path
dependency.

When exposed, it must return the already-stored current schema hash from
`SchemaManager`, not recompute `SchemaHash::compute(current_schema)`.

## TypeScript Responsibilities

TypeScript should own:

- typed table/query handles
- object-to-runtime input conversion
- runtime-output-to-object conversion
- user-defined column transforms
- high-level API ergonomics

TypeScript should not own:

- current-vs-declared schema comparison in hot paths
- row descriptor reordering for runtime outputs
- runtime schema hash cache invalidation on every operation
- recursive include/subquery alignment in TypeScript hot paths

## Required TypeScript Cleanup

1. Treat `returnsDeclaredSchemaRows: true` as mandatory for all runtimes,
   including fake runtimes in tests.
2. Remove runtime `getSchema()` / `getSchemaHash()` calls from:
   - `JazzClient.createInternal`
   - query result alignment paths
   - subscription delta alignment paths
   - ordinary `Db.insert` / `DbDirectBatch.insert` /
     `DbTransaction.insert` schema resolution
3. Delete TypeScript runtime-output alignment once all bindings satisfy the
   runtime-owned contract.
4. Make runtime input-schema lookup lazy everywhere:
   - use `table._schema` when it contains `table._table`
   - call runtime schema only if the generated table schema is absent
5. Clarify API naming:
   - `getSchema()` should mean declared/client schema, or be documented as
     debug-only if it returns runtime current schema
   - add separate names for runtime-current schema inspection if needed

## Required Rust/WASM/NAPI/RN Cleanup

1. Store declared schema on every runtime binding.
2. Align write return rows to declared schema before FFI serialization.
3. Align query rows to declared schema before FFI serialization.
4. Align subscription snapshots/deltas to declared schema before FFI
   serialization.
5. Return stored current schema hash from `getSchemaHash()`.
6. Make the capability flag unconditional for every runtime binding.

NAPI already has much of this shape:

- it stores `declared_schema`
- it aligns insert/query output in Rust
- it exposes `returnsDeclaredSchemaRows`

WASM and React Native should be brought to the same contract rather than making
TypeScript compensate.

## Important Edge Cases

### Projections

Projected query values are not necessarily full table rows. Runtime alignment
must not reorder projected columns unless it has a full descriptor/order for that
projection.

### Includes and Array Subqueries

Nested rows must be recursively aligned. This is the hardest part currently
covered by TypeScript alignment helpers and needs direct Rust coverage before
the TypeScript alignment helpers are deleted.

### Historical Rows and Lenses

Rows read from old schema branches should still be transformed through the lens
graph into the runtime current schema, then aligned to declared schema for FFI.

### Server Mode

Server-mode runtimes may serve multiple client schema hashes. Their query
execution should continue to use explicit schema contexts. The mandatory
declared-schema row contract applies to each client-facing runtime binding or
request context, not to the server's global known-schema set.

### Debug Schema Mutation

Debug helpers such as `__debugSeedLiveSchema` can add live historical schemas.
They should not change the declared schema. They also should not force
TypeScript to resume per-row schema hash polling.

## Test Plan

### Binding-Level Tests

For WASM, NAPI, and RN:

- insert returns row values in declared schema order when runtime/current schema
  column order differs
- query returns row values in declared schema order
- included rows are recursively aligned
- array subquery rows are recursively aligned
- projection result order is preserved
- subscription initial rows and deltas are aligned
- `getSchemaHash()` does not recompute schema descriptors per call

### TypeScript Tests

- repeated `db.insert(table, ...)` does not call runtime `getSchemaHash()`
- repeated `db.insert(table, ...)` does not call runtime `getSchema()` when
  `table._schema` contains the table
- direct batch inserts do not call runtime schema APIs in the common generated
  schema case
- transaction inserts do not call runtime schema APIs in the common generated
  schema case
- fake runtimes used in tests expose `returnsDeclaredSchemaRows: true` or return
  already-aligned rows

### Perf Benchmarks

Use a focused write benchmark:

- large declared schema, e.g. 24 tables x 24 columns
- 10k+ repeated inserts
- assert or report:
  - total insert loop time
  - `getSchemaHash()` call count
  - `getSchema()` call count
  - schema JSON/stringify/hash count if instrumented

Expected outcome:

- runtimes perform zero runtime schema/hash calls during repeated inserts
- schema hash computation disappears from the JavaScript CPU profile for writes

## Implementation Order

1. [x] Make `getSchemaHash()` cheap in WASM/NAPI/RN by returning stored current
       hash.
2. [x] Bring WASM output alignment to NAPI parity and set
       `returnsDeclaredSchemaRows: true`.
3. [x] Bring RN output alignment to the same contract.
4. [~] Add binding-level alignment tests for writes, queries, includes, array
   subqueries, and subscriptions.
   - Rust shared binding coverage exists for full-row, include, and
     projection-plus-include alignment.
   - Dedicated WASM/NAPI/RN subscription integration coverage is still worth
     adding.
5. [x] Remove TypeScript hot-path output alignment.
6. [x] Make all TypeScript runtime input-schema lookups lazy.
7. [x] Update fake runtimes and tests to the mandatory aligned-output contract.
8. [ ] Run focused write benchmark before/after.

## Non-Goals

- Do not redesign schema catalogue replication.
- Do not change how schemas/lenses are persisted.
- Do not change the row history or visible-row storage format.
- Do not remove support for multiple live historical schemas.
- Do not make TypeScript responsible for selecting server-mode explicit schema
  contexts.

## Open Questions

- Should `getSchema()` return declared schema or runtime current schema? The
  current name is ambiguous across bindings.
- Should the runtime expose both `getDeclaredSchema()` and
  `getRuntimeCurrentSchema()` for debugging?
- Can projected query alignment be represented in a single Rust helper shared by
  query and subscription code?
- How much of the current TypeScript alignment test suite should be ported to
  Rust binding tests before deleting the TypeScript alignment helpers?
