# Queries and writes

Use this reference for the stable application-level patterns. Confirm details against the installed
types when the project uses a different `jazz-tools` version.

## Contents

- [Schema and typed app](#schema-and-typed-app)
- [One-shot and subscribed reads](#one-shot-and-subscribed-reads)
- [Relations](#relations)
- [Mutations](#mutations)
- [Durability](#durability)
- [Batches and transactions](#batches-and-transactions)

## Schema and typed app

```ts
import { schema as s } from "jazz-tools";

const schema = {
  projects: s.table({
    name: s.string(),
  }),
  todos: s.table({
    title: s.string(),
    done: s.boolean(),
    projectId: s.ref("projects").optional(),
  }),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);

export type Todo = s.RowOf<typeof app.todos>;
export type NewTodo = s.InsertOf<typeof app.todos>;
export type TodoWhere = s.WhereOf<typeof app.todos>;
```

Name scalar ref columns with an `Id` or `_id` suffix. Name arrays of refs with an `Ids` or `_ids`
suffix. Make a column nullable with `.optional()` and add creation defaults with `.default(...)`.

## One-shot and subscribed reads

```ts
const openTodos = app.todos.where({ done: false });

const rows = await db.all(openTodos.orderBy("title", "asc").limit(50));
const row = await db.one(app.todos.where({ id: todoId }));

const unsubscribe = db.subscribeAll(openTodos, ({ all }) => {
  render(all);
});

// Call when the subscription owner is destroyed.
unsubscribe();
```

Query builders are immutable and composable. Multiple `where(...)` calls compose with AND semantics.
Common forms include exact values and operator objects:

```ts
app.todos.where({ done: false, title: { contains: "docs" } });
app.todos.where({ id: { in: selectedIds } });
app.todos.where({ projectId: { isNull: false } });
```

Use `select(...)` to narrow columns. Magic columns such as `$createdBy`, `$createdAt`, `$updatedBy`,
`$updatedAt`, `$canRead`, `$canEdit`, and `$canDelete` must be selected explicitly; `select("*")`
does not include them.

## Relations

A ref named `projectId` creates the forward relation `project`. Jazz derives a reverse relation on
the target table, such as `todosViaProject`.

```ts
const todos = await db.all(app.todos.where({ done: false }).include({ project: true }));

const projects = await db.all(
  app.projects.include({
    todosViaProject: app.todos.where({ done: false }).orderBy("title", "asc"),
  }),
);
```

Use `.requireIncludes()` when the root row should be omitted unless its requested includes are
present.

## Mutations

```ts
const pending = db.insert(app.todos, {
  title: "Ship docs",
  done: false,
  projectId,
});

db.update(app.todos, pending.value.id, { done: true });
db.delete(app.todos, pending.value.id);
```

Updates are partial. Omitting a key or passing `undefined` leaves it unchanged. Pass `null` to clear
an optional column.

Use `upsert(table, data, { id })` when the application controls the row ID. Use `restore(...)` for a
soft-deleted row; ordinary insert or update does not restore deleted data.

## Durability

```ts
const todo = await db.insert(app.todos, { title: "Ship docs", done: false }).wait({ tier: "edge" });

await db.update(app.todos, todo.id, { done: true }).wait({ tier: "global" });

const authoritative = await db.all(app.todos, { tier: "global" });
```

- `local`: persisted locally; normal browser/client default.
- `edge`: acknowledged by the nearest sync server; normal backend/server default.
- `global`: propagated to the global core.

Offline writes still apply locally. `edge` and `global` waits remain pending until reconnection and
propagation. Higher tiers change when the promise resolves, not whether the local write happened.

For reads and subscriptions, `tier` sets the minimum durability for delivered data. With
`localUpdates: "immediate"`, an already-settled subscription can overlay optimistic local writes
while stronger durability catches up. With `"deferred"`, delivery waits for the requested tier; this
is the default used by the high-level `Db` read and subscription helpers. `propagation: "local-only"`
prevents the read from being sent upstream.

## Batches and transactions

```ts
const batchResult = db.batch((batch) => {
  batch.insert(app.todos, { title: "First", done: false });
  batch.insert(app.todos, { title: "Second", done: false });
});
await batchResult.wait({ tier: "edge" });

const transactionResult = await db.transaction(async (tx) => {
  tx.update(app.todos, firstId, { done: true });
  const staged = await tx.all(app.todos.where({ done: true }));
  tx.update(app.todos, secondId, { done: staged.length > 0 });
});
await transactionResult.wait({ tier: "global" });
```

Callback forms commit when the callback returns or resolves and invoke rollback when it throws or
rejects. Handles created by `beginBatch()` and `beginTransaction()` require an explicit `commit()` or
`rollback()`.
