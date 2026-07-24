# Framework bindings

Choose imports and lifecycle patterns from the framework already used by the project.

| Concern       | React / Expo    | Vue             | Svelte                               | Solid                       |
| ------------- | --------------- | --------------- | ------------------------------------ | --------------------------- |
| Provider      | `JazzProvider`  | `JazzProvider`  | `JazzSvelteProvider`                 | `JazzProvider`              |
| Reactive rows | `useAll(query)` | `useAll(query)` | `new QuerySubscription(() => query)` | `useAll(() => ({ query }))` |
| Database      | `useDb()`       | `useDb()`       | `getDb()`                            | `useDb()` accessor          |
| Session       | `useSession()`  | `useSession()`  | `getSession()`                       | `useSession()`              |

For Svelte and Solid, pass an accessor whenever query inputs are reactive. Passing an already-built
query captures its current inputs instead of tracking later changes.

## React and Expo

Import browser React bindings from `jazz-tools/react`. Import Expo bindings from
`jazz-tools/expo` and load `jazz-tools/expo/polyfills` before other Jazz imports.

```tsx
import { useAll, useDb } from "jazz-tools/react";
import { app } from "./schema";

function TodoList() {
  const db = useDb();
  const todos = useAll(app.todos.where({ done: false }).orderBy("title", "asc"));

  if (todos === undefined) return <p>Connecting…</p>;
  if (todos.length === 0) return <p>No open todos.</p>;

  return todos.map((todo) => (
    <button key={todo.id} onClick={() => db.update(app.todos, todo.id, { done: true })}>
      {todo.title}
    </button>
  ));
}
```

Pass `undefined` instead of a query to skip a conditional query. Use `useAllSuspense` only when the
component tree intentionally uses React Suspense.

## Vue

Import from `jazz-tools/vue`. `useAll(query)` returns reactive `data`, `error`, and `loading` refs.
Read and write through the existing provider and `useDb()` setup rather than creating a second
client in a component.

## Svelte

Import from `jazz-tools/svelte`. Create `QuerySubscription` in component state and pass an accessor
when its query inputs are reactive. Read its `.current`, `.loading`, and `.error` values. Use
`getDb()` for writes. Follow the existing Svelte lifecycle so subscriptions are disposed with the
component.

## Solid

Import from `jazz-tools/solid`. Pass a function to `useAll` so Solid can track reactive query inputs.
`useDb()` returns an accessor; call it before invoking database methods.

## Plain TypeScript

Use the `Db` returned by `createDb(config)`. Use `db.subscribeAll(...)` for reactive behavior and
call `db.shutdown()` when the owning application or test shuts down.

## Loading and reactive identity

- Preserve `undefined` as the first-result loading state.
- Treat `[]` as a completed query with no rows.
- Vue, Svelte, and Solid reconcile row changes into reactive structures. Do not copy those results
  into another store merely to obtain reactivity.
- React follows normal React rendering and returns a new result array when a subscription updates.

## Client configuration guardrails

- Preserve the project's existing auth mode: anonymous, local-first secret, external JWT, or cookie
  session.
- Anonymous clients cannot write.
- A local-first secret is the user's identity; do not regenerate it on every render or startup.
- Use a persistent driver for offline browser storage unless the feature explicitly needs an
  ephemeral client.
- Use a unique persistent `dbName` when tests or multiple clients must not share browser storage.
