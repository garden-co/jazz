# Wrong type on queries with `hopTo`

## What

`Query.hopTo("relation")` keeps the source table's row type instead of switching to the destination, so any consumer that infers from `QueryBuilder<T>` (e.g. svelte `QuerySubscription`, `RowOf<>`) sees the wrong row type.

## Priority

medium

## Notes

- Reported upstream: https://github.com/garden-co/jazz/issues/772
- Bug is in `packages/jazz-tools/src/typed-app.ts`, not Svelte-specific:
  - `TypedTableQueryBuilder.hopTo` (line 890) returns `MetaQueryHandle<TMeta, ...>` with `TMeta` unchanged.
  - `Query.hopTo` (line 1088) returns `Query<TTable, ...>` (source `TTable`).
  - `RequiredQuery.hopTo` (line 1120): same.
- Runtime is correct: `_build()` emits `{ table, hops }` and `resolveHopsOutputTable` (`packages/jazz-tools/src/runtime/query-adapter.ts:699`) resolves to the destination. Bug is purely type-level.
- Existing helper `RelationTargetFromMeta<TMeta, TRelation>` (line 621) already resolves a relation name to the destination `TableMeta` — that's the building block for the fix.
- After `hopTo`, `TInclude` and `TSelection` were built against the source's relations/columns — they must reset for the destination.
- `permissions/type-inference.test.ts:188` chains `.where(...)` after `hopTo`, but uses hand-rolled mock query builder classes (`TeamQueryBuilder`, `ResourceGrantQueryBuilder` at lines 80-99) — not the real `Query<TTable, TSchema>`. The fix did not affect it.
- `TypedTableQueryBuilder._table` is set to the source table at runtime and not mutated by `hopTo` (destination lives in the `_hops` array). Switching `TMeta` post-hop means TS will compute `_table` as the destination name while the runtime value remains the source. **This divergence is intentional and out of scope for this fix** — the field is `_`-prefixed/internal and no consumer reads it for logic; aligning the runtime would mean reworking `_build()` and the query-adapter hop-lowering, which is a deeper change that should be evaluated on its own merits, not bundled with a type-inference bug fix.
