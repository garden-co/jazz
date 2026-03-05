# jazz-tools

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
