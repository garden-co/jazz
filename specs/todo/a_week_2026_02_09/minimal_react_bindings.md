# Minimal React Bindings — TODO

Smallest useful React integration: context, hook, type inference from query builders.

React bindings now live in `packages/jazz-tools/src/react/` and are published from the `jazz-tools/react` subpath export. They share the same build as `jazz-tools` and use `react` as a peer dependency.

## API

### `<JazzProvider>`

```tsx
const JazzContext = createContext<Db | null>(null);

interface JazzProviderProps {
  config: DbConfig; // pass-through to createDb
  children: React.ReactNode;
  fallback?: React.ReactNode; // shown while db initializes
}
```

- Calls `createDb(config)` on mount, stores `Db` in state
- Calls `db.shutdown()` on unmount
- Renders `fallback` (or nothing) until db is ready, then renders `children`

### `useDb(): Db`

- Reads `JazzContext`, throws if null (outside provider or still loading)

### `useAll<T>(query, tier?)`

Two overloads:

```ts
function useAll<T extends { id: string }>(query: QueryBuilder<T>): T[];
function useAll<T extends { id: string }>(
  query: QueryBuilder<T>,
  tier: PersistenceTier,
): T[] | undefined;
```

**No tier → `T[]` (always defined).** `RuntimeCore.subscribe_impl()` calls `immediate_tick()` synchronously before `subscribeAll()` returns, so the first callback fires in the same call stack. Use `useSyncExternalStore` to wire this: the `subscribe` function calls `db.subscribeAll()`, which synchronously populates the snapshot before React calls `getSnapshot()`.

**With tier → `T[] | undefined`.** `undefined` signals "not yet settled at requested tier." Data arrives asynchronously after the tier confirms.

Query identity: memoize via `query._build()` string — if the user recreates `app.todos.where({ done: false })` every render, the JSON string stays stable. Unsubscribes on unmount or query change.

## Example app: `todo-client-localfirst-react`

React port of `examples/todo-client-localfirst-ts/`. Same schema, same todo CRUD.

### Dependencies

- `jazz-tools` (workspace, including `jazz-tools/react`)
- `react`, `react-dom`
- devDeps: `@vitejs/plugin-react`, `vite`, `typescript`, `@types/react`, `@types/react-dom`

### Structure

```
examples/todo-client-localfirst-react/
  package.json
  tsconfig.json
  vite.config.ts        # same as TS example + @vitejs/plugin-react
  index.html            # <div id="root">, script tag
  schema/               # copied from todo-client-localfirst-ts
    current.ts
    current.sql
    app.ts
  src/
    main.tsx            # createRoot → <App />
    App.tsx             # <JazzProvider config={...}> wrapping <TodoList />
    TodoList.tsx        # useAll(app.todos) for list, useDb() for mutations
```

### Mutations

Direct calls on the `Db` instance (mutations are synchronous):

```tsx
const db = useDb();
db.insert(app.todos, { title: "New", done: false });
db.update(app.todos, id, { done: !todo.done });
db.deleteFrom(app.todos, id);
```

## E2E tests

Browser e2e tests in the example app, following the pattern in `packages/jazz-tools/tests/browser/`:

- Vitest browser mode + Playwright chromium (headless)
- `global-setup.ts` spawns a real jazz CLI server on a test port
- `vitest.config.browser.ts` with `vite-plugin-wasm` + `vite-plugin-top-level-await`

### Test cases

- **Render todos from subscription**: insert via `db.insert()`, verify the React component renders the item (useAll fires synchronously → immediate DOM update)
- **Toggle todo**: click checkbox, verify `db.update()` triggers re-render with updated `done` state
- **Delete todo**: click delete button, verify item removed from DOM
- **Add todo via form**: submit form, verify new item appears in list
- **OPFS persistence across reload**: insert, shutdown db, create new db with same `dbName`, verify todos survive (query with `"worker"` tier)
- **Server sync**: two db instances with JWT tokens syncing through real server, verify insert on one appears on the other

### Structure

```
examples/todo-client-localfirst-react/
  tests/
    browser/
      global-setup.ts        # spawn jazz server (reuse pattern from jazz-tools tests)
      test-constants.ts
      todo-app.test.ts        # e2e tests against the React app
  vitest.config.browser.ts
```

## Verification

- `pnpm build` succeeds
- Example e2e tests pass: `pnpm --filter todo-client-localfirst-react test`
- `pnpm --filter todo-client-localfirst-react dev` shows working todo app in browser

## Non-goals (this week)

- Suspense integration
- Mutation hooks
- Settlement tier indicators in UI
- React Server Components
