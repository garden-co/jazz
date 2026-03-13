# Include Missing References + `requireIncludes` — TODO (MVP)

## Motivation

Jazz cannot assume referential integrity for foreign keys when using `include` in queries.

A forward reference may fail to materialize because:

- the referenced row was deleted after the source row was written,
- the referenced row exists on another peer but has not synced locally yet,
- permissions hide the referenced row from the current session,

Today, the runtime already reflects this reality for forward includes by returning `undefined` when a referenced row is unavailable in scalar references (array references skip missing rows). The generated TypeScript types do not reflect that behavior though, so application code can compile while still crashing or misbehaving at runtime.

Example:

```ts
const project = insertProject(db);
const todo = db.insert(app.todos, {
  title: "Test Todo",
  done: false,
  project: project.id,
});

await db.delete(app.projects, project.id);

const result = await db.one(app.todos.where({ id: { eq: todo.id } }).include({ project: true }));

assert(result, "Result is not defined");
expect(result.project).toBeUndefined();
```

We need two changes:

1. Align generated include result types with runtime behavior.
2. Provide an opt-in query mode that filters out rows whose included references are not available.

## Goals

- Make missing forward includes explicit in generated TypeScript types.
- Preserve the current default runtime behavior for existing callers.
- Add an opt-in query mode that guarantees required includes are present.
- Support nested includes in a composable way.

## Non-goals

- Modify reverse relations. These relations always omit missing referencing rows.
- Enforce referential integrity at write time for ordinary `UUID REFERENCES ...` columns (since it is impossible in a distributed database with support for offline writes).
- Change delete semantics.
- Introduce placeholder rows or error-returning include materialization.

## Proposed API

Add a query-level boolean option: `requireIncludes`.

Public builder surface:

```ts
app.todos.include({ project: true }).requireIncludes();
```

Serialized query shape:

```ts
{
  table,
  conditions,
  includes,
  requireIncludes: true,
  ...
}
```

Nested includes can opt in independently by using an include query builder:

```ts
app.todos.include({
  project: app.projects.include({ owner: true }).requireIncludes(),
});
```

This keeps the policy local to the query node that owns the include set.

## Type System Changes

### 1. Default include types must model missing forward references

For forward scalar relations, the included value should be typed as possibly missing even when the FK column is non-nullable.

Current generated type:

```ts
project: Project;
```

New generated type:

```ts
project?: Project;
```

Reason: non-nullable FK means "the source row stores an id", not "the referenced row is readable and present".

This applies to:

- `RelationInclude extends true`
- nested include objects
- include query builders (`Any<Project>QueryBuilder<...>`)
- `WithIncludes`
- `SelectedWithIncludes`

For forward array relations don't require any changes at the type level, since we skip missing rows.

### 2. `requireIncludes` narrows forward include results

When a query node has `requireIncludes: true`, every explicitly included forward relation owned by that query node is guaranteed to be present in the returned row.

That means:

- forward scalar includes become non-optional in the result type for that query node (unless the fk was optional to begin with),
- forward array contain all elements in the FK array,
- reverse relations are unchanged, because "missing" rows are never included

Examples:

```ts
const todo = await db.one(app.todos.include({ project: true }));
// todo?.project: Project | undefined
```

```ts
const todo = await db.one(app.todos.include({ project: true }).requireIncludes());
// todo?.project: Project
```

For nested builders, narrowing should happen only where `requireIncludes` is enabled:

```ts
app.todos.include({
  project: app.projects.include({ owner: true }).requireIncludes(),
});
```

In that case:

- `project` is still `ProjectWithIncludes<...> | undefined` unless the outer `todos` query also requires includes,
- but inside `project`, `owner` is non-optional.

## Runtime Semantics

### Default behavior

Without `requireIncludes`, behavior stays as-is:

- missing forward scalar include -> `undefined`
- missing forward array members -> skipped
- reverse include -> empty array when no matching rows are available
- root row is still returned as long as the base query matches

### `requireIncludes: true`

When enabled on a query node, a row is returned only if every explicitly included forward relation on that node is fully satisfiable.

For forward scalar relations the referenced row must be available and visible.

For forward array relations (`UUID[] REFERENCES ...`):

- every referenced id must resolve to an available and visible row,
- materialized order must still match source order,
- duplicates must still be preserved.

If any referenced element is unavailable, the source row is filtered out.

For reverse relations:

- `requireIncludes` does not filter on them,
- they continue to return arrays, possibly empty.

This keeps the feature targeted at "my row depends on referenced rows being present", which is only well-defined for forward references.

`requireIncludes` should be allowed on queries with no includes, and be a no-op.

## Nested Semantics

`requireIncludes` is evaluated per query node.

That gives predictable composition:

- outer query without `requireIncludes`, inner include builder with it:
  - outer row may still be returned,
  - the singular included relation may become `undefined` if the nested query filters it out.
- outer query with `requireIncludes`, inner include builder without it:
  - outer row requires the included relation itself to exist,
  - nested relations under that included row may still be missing.
- both outer and inner query nodes with `requireIncludes`:
  - guarantees compose recursively.

## Query Translation / Execution Notes

The built-query shape and normalization logic should carry `requireIncludes`.

Likely implementation shape:

- add `requireIncludes?: boolean` to the built query shape,
- preserve it in `normalizeBuiltQuery`,
- preserve it in normalized include entries so nested include builders can carry it,
- pass it through query translation so execution can filter rows before result materialization.

Execution should apply the filter at the query engine level rather than in post-transform TypeScript code, so:

- `limit`/`offset` semantics stay correct,
- subscriptions produce consistent membership,
- `db.one(...)` does not briefly see then drop invalid rows client-side.

## Backward Compatibility

Runtime behavior for existing queries does not change.

The breaking change is type-level only:

- code that assumed included forward relations always exist will now fail to compile,
- that is intentional and fixes an existing unsoundness.

Callers that want the old "loaded row implies included references exist" ergonomics can opt into:

```ts
query.include(...).requireIncludes()
```

## Testing Strategy

### Typegen tests

- Non-nullable scalar FK include generates `Target | undefined` by default.
- Nullable scalar FK include still generates `Target | undefined`.
- `requireIncludes` narrows forward scalar includes to `Target`.
- Nested `requireIncludes` narrows only the query node where it is applied.

### Runtime query tests

- Missing scalar forward include returns base row with included property `undefined`.
- Missing scalar forward include with `requireIncludes` drops the base row.
- `null` scalar FK with `requireIncludes` preserves the base row.
- Missing nested include under a required child drops the child row from that child query node.
- Missing nested include under required outer + required inner drops the root row.

### Array FK tests

- Forward `UUID[]` include without `requireIncludes` omits missing elements but still returns the base row.
- Forward `UUID[]` include with `requireIncludes` drops the base row if any element is unavailable.
- Preserves duplicate ids and source order when all elements are available.

### Permissions / visibility tests

- Hidden referenced row behaves the same as deleted/missing for both default and required modes.
- A row can disappear from a subscription when an included required reference becomes unreadable.

## Open Questions

- Naming: `requireIncludes()` is direct, but `strictIncludes()` or `requiredIncludes()` may read better.
