# TypeScript Client Codegen — TODO

Remaining gaps in the TypeScript client codegen.

> Status quo: [specs/status-quo/ts_client_codegen.md](../status-quo/ts_client_codegen.md)

## Example App: Add Relations Demo

**Priority: Medium**

The example schema (`examples/todo-ts-client/schema/current.ts`) doesn't use `col.ref()`. The generated `app.ts` therefore lacks relation types, Include types, and reverse relations. The spec envisions:

```typescript
parent_id: col.ref("todos").optional(),
```

This means `.include()` usage is never demonstrated end-to-end. Update the example to show:

- Schema with `col.ref()` (self-referential + cross-table)
- Generated code with Include/Relations types
- Application code using `.include({ parent: true })`

> `examples/todo-ts-client/schema/current.ts` (currently no relations)
> `packages/jazz-ts/src/codegen/codegen.test.ts:272-420` (relation analysis is tested, just not demoed)

## Row Transformer: Nested Relation Mapping

**Priority: Medium**

Row transformer has a TODO for mapping nested arrays to relation names:

```typescript
// TODO: Map nested arrays to relation names once we have that metadata
```

Nested relation loading in row transformation may have edge cases when includes are multi-level.

> `packages/jazz-ts/src/runtime/row-transformer.ts:157`

## Client Internal TODOs

**Priority: Low**

Minor implementation gaps in the client layer:

- `// TODO: Compute actual schema hash` — currently uses a placeholder
- `// TODO: use real client_id` — client ID generation

These don't affect generated code functionality but should be cleaned up.

> `packages/jazz-ts/src/runtime/client.ts:268,355`

## Future Work

**Priority: Deferred**

- **React/Vue bindings**: Generate framework-specific hooks (e.g., `useTodos()`)
- **Filtered includes**: Allow `QueryBuilder` as alternative to `boolean` for filtered relation loading (e.g., `include({ posts: postsQuery.where({ published: true }) })`) — Include types already accept QueryBuilder union, runtime translation not yet implemented
- **Cursor-based pagination**: For very large result sets with stable ordering
- **Conflict resolution**: Surface merge conflicts to application layer
- **Explicit joins**: Investigate join support beyond array subqueries for cross-table queries
