# TypeScript Client Codegen — TODO (MVP)

Remaining gaps in the TypeScript client codegen.

> Status quo: [specs/status-quo/ts_client_codegen.md](../../status-quo/ts_client_codegen.md)

## Example App: Add Relations Demo

The example schema (`examples/todo-ts-client/schema/current.ts`) doesn't use `col.ref()`. The generated `app.ts` therefore lacks relation types, Include types, and reverse relations. Update the example to show:

- Schema with `col.ref()` (self-referential + cross-table)
- Generated code with Include/Relations types
- Application code using `.include({ parent: true })`

> `examples/todo-ts-client/schema/current.ts` (currently no relations)
> `packages/jazz-ts/src/codegen/codegen.test.ts:272-420` (relation analysis is tested, just not demoed)

## Row Transformer: Nested Relation Mapping

Row transformer has a TODO for mapping nested arrays to relation names:

```typescript
// TODO: Map nested arrays to relation names once we have that metadata
```

Nested relation loading in row transformation may have edge cases when includes are multi-level.

> `packages/jazz-ts/src/runtime/row-transformer.ts:157`

## Client Internal TODOs

Minor implementation gaps that must be fixed for production:

- `// TODO: Compute actual schema hash` — currently uses a placeholder
- `// TODO: use real client_id` — client ID generation

> `packages/jazz-ts/src/runtime/client.ts:268,355`

## Later: Future Work

- **React/Vue bindings**: Generate framework-specific hooks (see `react_bindings.md`)
- **Filtered includes**: Allow `QueryBuilder` as alternative to `boolean` for filtered relation loading
- **Cursor-based pagination**: For very large result sets with stable ordering
- **Conflict resolution**: Surface merge conflicts to application layer
- **Explicit joins**: Investigate join support beyond array subqueries
