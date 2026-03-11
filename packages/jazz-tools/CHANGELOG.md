# jazz-tools

## 2.0.0-alpha.15

### Patch Changes

- 5684a18: Normalize schema manager table columns before hashing sorting by name.

  This makes logically equivalent schemas produce the same schema hash even when their column declarations are ordered differently.

- 6664ee5: Use the derived local anonymous/demo session for `JazzClient` query and subscription permission checks when no JWT is configured.
- 8877b8b: Fix runtime schema-order compatibility after sorted table columns.

  `Db` mutations and query transforms now tolerate runtime schemas returned as `Map`s, and low-level `JazzClient` create/query/subscribe APIs preserve the declared schema column order expected by generated bindings and app code.

- ac3a73e: Fix Rust schema-order compatibility when runtime table columns are sorted differently from the declared app schema, including `JazzClient` create/query flows and `SchemaManager` inserts.
- f9812d7: Fix lens SQL parsing for `TIMESTAMP` defaults so numeric defaults like `DEFAULT 0` are coerced to timestamp values instead of integers.

  This resolves type mismatches when applying migrations that add timestamp columns with numeric defaults, and adds regression coverage for `TIMESTAMP DEFAULT 0`.

- 4871b02: Switch the native persistent storage engine from SurrealKV to Fjall for the CLI, NAPI bindings, and React Native bindings.

  Native local data now lives in Fjall-backed stores and uses `.fjall` database paths by default.

- 4fff7e9: Improve type inference for `include` and `select` in TS queries
- e32e6a9: Fix backend N-API sync regression where outbound messages were dropped before they reached the server.

  `createJazzContext(...).asBackend()` now accepts the real nested N-API sync callback shape used by published alpha builds, so backend query subscriptions and other upstream sync traffic can leave the local runtime again.

- 971f8cf: Add `$canRead`, `$canEdit`, and `$canDelete` permission introspection magic columns to queries, and reserve the `$` column prefix for system magic fields.
- bb39e15: Modify inserts to return the inserted row instead of just the id
- 8571fdb: Make query optional in `useAll` to support conditionally running queries when inputs are missing
- 9accce0: `QuerySubscription` in the Svelte bindings now accepts an options object as its second argument (e.g. `{ tier: 'edge' }`), matching the React `useAll` API. The previous bare-string form is removed.
- 78e074f: Split the local-first insert APIs in `jazz-tools`.
  - `db.insert(...)` now applies the write immediately and returns the inserted row synchronously.
  - `db.insertDurable(...)` waits for the requested durability tier before resolving.

- 4fd041c: Split the local-first update/delete APIs in `jazz-tools`.
  - `db.update(...)` and `db.delete(...)` now apply immediately and return `void`.
  - `db.updateDurable(...)` and `db.deleteDurable(...)` wait for the requested durability tier before resolving.
  - `db.deleteFrom(...)` has been renamed to `db.delete(...)`.

- Add Vue bindings and a `jazz-tools/vue` entrypoint, with matching docs and example coverage.
- Updated dependencies [bb39e15]
  - jazz-wasm@2.0.0-alpha.15

## 2.0.0-alpha.14

### Patch Changes

- ad29f43: fix query sync provenance for paginated, nested subquery, and recursive subscriptions
- a4da52d: Wait for the initial server event stream handshake before returning from `JazzClient::connect`, preventing `EdgeServer` settled queries from racing the connection after server restart.
- 78092d3: Add support for `EXISTS (SELECT FROM <table> WHERE <expr>)` in SQL policy expressions.
- 78092d3: Fix `@session.__jazz_outer_row.id` not resolving inside EXISTS subquery policies. Previously the outer row's UUID was silently treated as an unresolvable column, causing all EXISTS policy checks to evaluate to false on the server.
- dc25263: Fix: sync server now falls back to the server-established session when a `QuerySubscription` payload omits one.

  Demo and anonymous auth clients sent `session: None` in subscription payloads, causing all their queries to return empty results after the payload-session change in #147. The server now prefers the session it validated from auth headers during the SSE handshake, falling back to the payload only for fully unauthenticated clients. Payload sessions that differ from the server-established session are ignored and a warning is logged.

- 2943587: Fix a race condition in `subscribe_internal` where the callback could be called before it was registered.
- a952d98: Fix missing `id` fields on rows returned from included array subqueries, including nested relation results.
- ec0ff2d: Add a built-in MCP server (`npx jazz-tools mcp`) that exposes Jazz documentation as tools for AI assistants. Supports full-text search via SQLite FTS5 (Node 22.13+) with a plain-text fallback for older runtimes.
- 2f5ccba: Add an in-memory storage driver across the Jazz JS, WASM, NAPI, and React Native runtimes.

  Backend contexts can now opt into memory-backed runtimes without local persistence, and runtime driver-mode coverage was expanded to exercise the new in-memory path.

- 49307fa: Quote keyword and non-bare identifiers when emitting frozen schema and lens SQL from Rust so round-tripping generated SQL continues to parse.
- Updated dependencies [2f5ccba]
  - jazz-wasm@2.0.0-alpha.14

## 2.0.0-alpha.13

### Patch Changes

- ff4ccb3: Support quoted SQL identifiers in `jazz-tools` schema parsing/generation, including reserved keyword column names like `"table"`.
  - jazz-wasm@2.0.0-alpha.13

## 2.0.0-alpha.12

### Patch Changes

- 8bcde79: Harden runtime sync outbox handling across WASM/RN and NAPI callback contracts by typing both callback shapes, routing both through a shared normalizer, and adding conformance tests that assert identical `/sync` behavior.
  - jazz-wasm@2.0.0-alpha.12

## 2.0.0-alpha.11

### Patch Changes

- 969a139: Overhauled durability APIs to use a single `DurabilityTier` model across reads and writes.
  - Reads now take `{ tier, localUpdates }`, where `localUpdates` defaults to `"immediate"` so local writes are reflected right away even when waiting for a more remote durability tier.
  - Writes now use the base methods with optional `{ tier }` and environment-aware defaults (`"worker"` for clients, `"edge"` for backend contexts).
  - Renamed the top tier from `"core"` to `"global"` for clearer semantics.
  - Added multi-tier node identity support so single-node deployments (like CLI and cloud-server today) can acknowledge both `"edge"` and `"global"`.

- 98ba0f9: Fixed array subquery incremental updates so parent row fields stay correct. Previously, when related rows changed after subscribing, update payloads could return corrupted parent values (for example, garbled `id` or `name`).
- 48053ac: fix(codegen): generate DROP COLUMN statements for all affected tables in multi-table migrations
- debd2c3: Add `asBackend()` for server-side Jazz clients using backend-secret auth, and enforce backend-role limits so backend sync can write row data but cannot write schema/permissions catalogue entries.
- a955504: Allow backend `JazzClient` and `SessionClient` query/subscribe calls to consume generated query builders directly. Query-builder payloads with `_schema` are now translated automatically to runtime query JSON (`relation_ir`), so backend code can call `context.forRequest(...).query(app.todos.where(...))` without manual `translateQuery(...)`.
  - jazz-wasm@2.0.0-alpha.11

## 2.0.0-alpha.10

### Patch Changes

- b058893: fix `jazz-tools build` bootstrap behavior by routing through the TypeScript schema CLI when `schema/current.ts` exists and `schema/current.sql` is missing
- ddf7756: Tighten generated query helper and include types for stronger inference and stricter contracts.

  This preserves include-aware returned row types by keeping `QueryBuilder<...WithIncludes<I>>` / `_rowType` aligned with selected includes, narrows generated `*Include` relation flags to `true` (instead of `boolean`), tightens `gather(...)` step callback typing, avoids optional-include selector collapse to `never` in nested array includes, and removes unnecessary `unknown` casts in generated include helpers.
  - jazz-wasm@2.0.0-alpha.10

## 2.0.0-alpha.9

### Patch Changes

- eef9942: Fix WebAssembly fetch behavior in Next.js runtimes.
  - jazz-wasm@2.0.0-alpha.9

## 2.0.0-alpha.8

### Patch Changes

- 401db01: fix cold load of object history
- d1f17a9: fix: ensure query subgraphs share branch and schema context of parent graph
- 4775a79: Add a high-level server-side `createJazzContext` API in `jazz-tools/backend` with lazy runtime setup from generated app DSL objects, plus request/session-scoped helpers (`forRequest`, `forSession`) and lifecycle helpers (`flush`, `shutdown`).
  - jazz-wasm@2.0.0-alpha.8

## 2.0.0-alpha.7

### Patch Changes

- Add Expo support.
- 6b19ea3: Add support for JSON columns.
- 47dbdba: Added Svelte support.
  - jazz-wasm@2.0.0-alpha.7
