# TypeScript Client Codegen — Status Quo

This is what application developers actually touch. Instead of calling the WASM runtime directly, developers define schemas (via [Schema Files](schema_files.md)) and get a generated `app.ts` with type-safe query builders, mutation helpers, and subscription APIs.

The generated code sits on top of the [Worker Bridge and WASM runtime](storage.md), which in turn wraps the Rust [Runtime Core](batched_tick_orchestration.md). The codegen ensures that TypeScript types match the schema exactly — a column rename in the schema propagates to compile errors in application code.

## Developer Workflow

```
schema/current.ts ──► node ./packages/jazz-tools/dist/cli.js build ──► schema/app.ts (generated)
                           │
                           ▼
                  WasmSchema JSON (intermediate)
```

## Design Decisions

| Decision           | Choice                 | Rationale                                                      |
| ------------------ | ---------------------- | -------------------------------------------------------------- |
| Schema source      | WasmSchema JSON        | Types already resolved, consistent with runtime                |
| Relations          | `col.ref('table')`     | All refs are UUIDs, simple syntax                              |
| Relation naming    | Strip `_id` suffix     | `parent_id` → `.include({ parent })`                           |
| Reverse relations  | `tableViaColumn`       | `blockersViaBlocking` — auto-derived                           |
| Output             | Single `schema/app.ts` | Simple imports, easy to understand                             |
| Subscription shape | Full state + delta     | `{ all, added, updated, removed }`                             |
| DB interface       | Generic + schema       | `createDb(config)`, `db.all(query)`                            |
| Mutations          | Async local-first      | local state updates immediately; promises are durability-gated |

## Part 1: Schema DSL Extension

`col.ref('table')` generates a UUID column with `references` metadata. Supports `.optional()` for nullable FKs. SQL output: `UUID REFERENCES table [NOT NULL]`.

> `packages/jazz-tools/src/dsl.ts:44-62` (RefBuilder class)
> `packages/jazz-tools/src/sql-gen.test.ts:110-165` (ref SQL generation tests)

## Part 2: WasmSchema Enhancement

Schema reader converts DSL `Column.references` to `ColumnDescriptor.references` in WasmSchema JSON. Preserves reference metadata through the pipeline.

> `packages/jazz-tools/src/codegen/schema-reader.ts:27-47` (schemaToWasm with references)
> `packages/jazz-tools/src/codegen/codegen.test.ts:61-97` (ref conversion tests)

## Part 3: Codegen Pipeline

### Module Structure

```
packages/jazz-tools/src/
├── codegen/
│   ├── index.ts                    # Entry: generateClient()
│   ├── schema-reader.ts            # Parse WasmSchema JSON
│   ├── relation-analyzer.ts        # Derive forward + reverse relations
│   ├── type-generator.ts           # Generate TypeScript interfaces
│   └── query-builder-generator.ts  # Generate query builder classes
└── cli.ts                          # TypeScript schema CLI orchestration
```

### CLI Integration

`node ./packages/jazz-tools/dist/cli.js build --schema-dir ./schema` compiles TS DSL to SQL and generates `schema/app.ts`.

> `packages/jazz-tools/src/cli.ts:63-72` (generateAppTs)
> `packages/jazz-tools/src/cli.ts:150-175` (build command orchestration)

### Relation Analysis

Derives forward relations (strip `_id` suffix) and reverse relations (`tableViaColumn` naming). Handles self-referential relations. Validates referenced tables exist.

> `packages/jazz-tools/src/codegen/relation-analyzer.ts:48-98` (analyzeRelations)
> `packages/jazz-tools/src/codegen/codegen.test.ts:272-420` (relation analysis tests)

## Part 4: Type Generation

Generated `schema/app.ts` includes:

1. **Base types** (with `id: string`) — one per table
2. **Init types** (without `id`) — for insert/update mutations
3. **WhereInput types** — operator objects per column type (text: `eq/ne/contains`, number: `eq/ne/gt/gte/lt/lte`, FK: `eq/ne/isNull`)
4. **Include types** — union: `boolean | IncludeInterface | QueryBuilder`
5. **Relations types** — maps relation names to their types (reverse as arrays)
6. **WithIncludes types** — generic `TodoWithIncludes<I extends TodoInclude>` for type-safe results

> `packages/jazz-tools/src/codegen/type-generator.ts:238-260` (base + init types)
> `packages/jazz-tools/src/codegen/type-generator.ts:106-155` (include + relations types)
> `packages/jazz-tools/src/codegen/type-generator.ts:171-214` (WithIncludes generics)

## Part 5: Query Builder Generation

Generates fluent, immutable query builders per table:

- `.where(conditions)` — type-safe operators per column type
- `.include(relations)` — generic type union for relation loading (only on tables with relations)
- `.orderBy(column, direction)` — type-safe column names via `keyof`
- `.limit(n)` / `.offset(n)` — pagination
- `._build()` — serializes to JSON for runtime translation
- `._clone()` — deep copy for immutability

> `packages/jazz-tools/src/codegen/query-builder-generator.ts:75-189` (QueryBuilder class generation)
> `packages/jazz-tools/src/codegen/query-builder-generator.ts:51-70` (WhereInput generation)
> `packages/jazz-tools/src/codegen/codegen.test.ts:553-643` (query builder tests)

## Part 6: Runtime Integration

### Db Class

`createDb(config)` is the main entry point for application code. It's async because it pre-loads the WASM module. Once initialized, mutations apply local-first immediately and return promises that resolve at the configured durability tier. The Db lazily creates and memoizes `JazzClient` instances per schema hash, so multiple schemas can coexist in one app.

> `packages/jazz-tools/src/runtime/db.ts:93-450` (Db class)
> `packages/jazz-tools/src/runtime/db.ts:479-484` (createDb factory)

### Queries

- `db.all<T>(query, options?)` — translates query → WasmQueryBuilder, transforms rows to typed objects
- `db.one<T>(query, options?)` — wraps `all()` with `[0] ?? null`

`options` supports:

- `tier?: "worker" | "edge" | "global"`
- `localUpdates?: "immediate" | "deferred"` (default: `immediate`)
- `propagation?: "full" | "local-only"` (default `full`)

### Mutations (Async Local-First)

- `db.insert(table, data, options?)` — applies local write immediately and resolves with new ID at durability tier
- `db.update(table, id, data, options?)` — applies local update immediately and resolves at durability tier
- `db.deleteFrom(table, id, options?)` — applies local delete immediately and resolves at durability tier

`options` supports:

- `tier?: "worker" | "edge" | "global"`

### Subscriptions

`db.subscribeAll(query, callback, options?)` — the local-first alternative to polling. The callback fires whenever the query's results change (local writes, sync updates). It receives `{ all, added, updated, removed }` — the full result set plus a delta. With tier-gated reads, initial delivery still waits for the requested tier; `localUpdates: "immediate"` affects only subsequent local-write updates.

The SubscriptionManager preserves object identity for unchanged items: if a new todo is added, existing todo objects in the array keep the same JavaScript reference. This makes React's `useMemo`/referential equality checks work naturally.

> `packages/jazz-tools/src/runtime/subscription-manager.ts` (delta management, 10 tests)

### Supporting Infrastructure

- **Query Adapter**: translates generated JSON to WasmQueryBuilder format (25 tests)
- **Row Transformer**: converts WasmRow to typed objects (16 tests)
- **Value Converter**: JS ↔ WasmValue conversion (`toValueArray`, `toUpdateRecord`) (22 tests)

> `packages/jazz-tools/src/runtime/query-adapter.ts`
> `packages/jazz-tools/src/runtime/row-transformer.ts`
> `packages/jazz-tools/src/runtime/value-converter.ts`

### Reconnection + Query Replay (Runtime/Worker)

The TS runtime intentionally treats upstream attachment as replay boundary for subscriptions:

- On stream failure, runtime detaches upstream (`removeServer`) and schedules reconnect.
- On `Connected`, runtime stores server-provided `client_id`, re-attaches upstream (`addServer`), and resets backoff.
- Re-attach triggers replay of active query subscriptions, so subscriptions created while offline still converge after reconnect.
- Backoff uses exponential delay with jitter (`300ms * 2^attempt`, capped at `10s`, plus `0-199ms` jitter).

> `packages/jazz-tools/src/runtime/client.ts:572-663`
> `packages/jazz-tools/src/worker/jazz-worker.ts:152-241`

## Part 7: React Bindings

`jazz-tools` now ships first-party React bindings via the `jazz-tools/react` subpath export:

- `JazzProvider` — async `createDb(config)` on mount, provides `Db` through context, calls `db.shutdown()` on unmount
- `useDb()` — context hook returning `Db` (throws outside provider)
- `useAll(query)` — `useSyncExternalStore` wrapper over `db.subscribeAll(...)`
- `useAllSuspense(query)` — suspense-enabled query hook

`useAll(query)` returns `T[] | undefined` and resolves as subscription data arrives. Durability-tier gating is configured through `db.subscribeAll(..., options)` when needed.

> `packages/jazz-tools/package.json` (`./react` export)
> `packages/jazz-tools/src/react/provider.tsx`
> `packages/jazz-tools/src/react/use-all.ts`

## Test Coverage

| Suite                        | Tests                       | Scope                                           |
| ---------------------------- | --------------------------- | ----------------------------------------------- |
| sql-gen.test.ts              | 11                          | DSL to SQL generation                           |
| codegen.test.ts              | 45                          | Schema reader, types, relations, query builders |
| query-adapter.test.ts        | 25                          | Query translation                               |
| row-transformer.test.ts      | 16                          | Row transformation                              |
| value-converter.test.ts      | 22                          | Value conversion                                |
| subscription-manager.test.ts | 10                          | Delta management                                |
| worker-bridge.test.ts        | 10+                         | Browser E2E (Worker + OPFS + sync)              |
| **Total**                    | **129+ unit + browser E2E** |                                                 |

## Example Application

`examples/todo-client-localfirst-ts/` — working browser app with basic CRUD, subscriptions, and server sync. Schema uses simple columns (title, done, description) without relations.

> `examples/todo-client-localfirst-ts/schema/current.ts` (schema definition)
> `examples/todo-client-localfirst-ts/schema/app.ts` (generated client)
> `examples/todo-client-localfirst-ts/src/main.ts` (application code)

`examples/todo-client-localfirst-react/` provides the same flow with the React bindings (`JazzProvider` + `useAll` + `useDb`), including browser e2e coverage.

> `examples/todo-client-localfirst-react/src/App.tsx`
> `examples/todo-client-localfirst-react/src/TodoList.tsx`
> `examples/todo-client-localfirst-react/tests/browser/todo-app.test.tsx`
