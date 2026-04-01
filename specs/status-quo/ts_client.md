# App Surface — Status Quo

Today, the important idea is simpler:

- developers write `schema.ts`
- `schema.ts` exports a typed `app`
- that `app` object is both the typed query surface for application code and the source of runtime schema metadata

See also [Schema Files](schema_files.md) for the current validation and migration workflow.

## Current Mental Model

Typical shape:

```typescript
import { schema as s } from "jazz-tools";

const schema = {
  todos: s.table({
    title: s.string(),
    done: s.boolean(),
    projectId: s.ref("projects").optional(),
  }),
  projects: s.table({
    name: s.string(),
  }),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);
```

`defineApp(...)` builds an object with:

- one typed table handle per table, like `app.todos`
- a `wasmSchema` payload used by the runtime

> [schema.ts](/Users/nicolasr/Desktop/Jazz/jazz2/examples/docs/todo-client-localfirst-react/schema.ts#L2)
> [typed-app.ts](/Users/nicolasr/Desktop/Jazz/jazz2/packages/jazz-tools/src/typed-app.ts#L1116)

## What Developers Use

Application code usually works with the exported `app` directly.

- `app.todos` is a typed table/query handle
- `s.RowOf<typeof app.todos>` gives the row type
- `s.InsertOf<typeof app.todos>` gives the insert shape
- `s.WhereOf<typeof app.todos>` gives the `where(...)` input shape

Example:

```typescript
import { app, type Todo } from "../schema.js";

const query = app.todos.where({ done: false }).orderBy("title");
```

> [typed-app.ts](/Users/nicolasr/Desktop/Jazz/jazz2/packages/jazz-tools/src/typed-app.ts#L1043)
> [typed-app.ts](/Users/nicolasr/Desktop/Jazz/jazz2/packages/jazz-tools/src/typed-app.ts#L1053)

## Query Surface

The typed table handles are query builders. The core methods are:

- `where(...)`
- `select(...)`
- `include(...)`
- `requireIncludes()`
- `orderBy(...)`
- `limit(...)`
- `offset(...)`
- `hopTo(...)`
- `gather(...)`

These builders are immutable and serialize into the runtime query format through `._build()`.

> [typed-app.ts](/Users/nicolasr/Desktop/Jazz/jazz2/packages/jazz-tools/src/typed-app.ts#L731)

## Runtime Integration

The runtime revolves around `createDb(...)` and `Db`.

The key integration point is that `Db` methods accept the typed table/query handles exported from `schema.ts`:

- `db.all(app.todos.where(...))`
- `db.one(app.todos.where(...))`
- `db.insert(app.todos, data)`
- `db.update(app.todos, id, data)`
- `db.delete(app.todos, id)`
- `db.subscribeAll(app.todos, callback)`

> [main.ts](/Users/nicolasr/Desktop/Jazz/jazz2/examples/docs/todo-client-localfirst-ts/src/main.ts#L1)
> [db.ts](/Users/nicolasr/Desktop/Jazz/jazz2/packages/jazz-tools/src/runtime/db.ts#L1141)
> [db.ts](/Users/nicolasr/Desktop/Jazz/jazz2/packages/jazz-tools/src/runtime/db.ts#L1244)

## Framework Bindings

There are first-party framework wrappers on top of the same runtime surface:

- React: `JazzProvider`, `useDb`, `useAll`, `useAllSuspense`
- Vue: `JazzProvider`, `useDb`, `useAll`
- Svelte: `useAll` helpers over the same query API

These do not change the core model. They consume the same `app` query handles and `Db` behavior.

> [react/index.ts](/Users/nicolasr/Desktop/Jazz/jazz2/packages/jazz-tools/src/react/index.ts)
> [vue/index.ts](/Users/nicolasr/Desktop/Jazz/jazz2/packages/jazz-tools/src/vue/index.ts)
> [svelte/use-all.svelte.ts](/Users/nicolasr/Desktop/Jazz/jazz2/packages/jazz-tools/src/svelte/use-all.svelte.ts)

## Key Files

| File                                                   | Purpose                                   |
| ------------------------------------------------------ | ----------------------------------------- |
| `packages/jazz-tools/src/typed-app.ts`                 | Typed schema and `app` definitions        |
| `packages/jazz-tools/src/runtime/db.ts`                | Query, mutation, and subscription runtime |
| `packages/jazz-tools/src/index.ts`                     | Public `schema` namespace and exports     |
| `examples/docs/todo-client-localfirst-react/schema.ts` | Current `defineApp(...)` example          |
| `examples/docs/todo-client-localfirst-ts/src/main.ts`  | Current `createDb(...)` + `app` usage     |
