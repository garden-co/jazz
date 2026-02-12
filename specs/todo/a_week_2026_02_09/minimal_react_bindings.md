# Minimal React Bindings — TODO

Smallest useful React integration: context, hooks, type inference from query builders.

## Scope

### `<JazzProvider>` / app context

- Wraps a `Db` instance (already handles worker bridge, OPFS, server sync)
- Props: `appId`, `serverUrl`, optional config
- Initializes `Db` on mount, calls `db.shutdown()` on unmount

### `useDb()`

- Consumes the provider context, returns the `Db` instance
- Throws if used outside `<JazzProvider>`

### `useAll(queryBuilder, tier?)`

- Wraps `db.subscribeAll(query, tier, callback)`
- Returns `T[] | undefined` (undefined until first emission)
- Type `T` inferred from the query builder passed in
- Unsubscribes on unmount / query identity change

### `useOne(queryBuilder, tier?)`

- Wraps `db.subscribeOne(query, tier, callback)` (or subscribeAll + take first)
- Returns `T | undefined`
- Same type inference and lifecycle as `useAll`

### `todo-client-localfirst-react` example

- React port of `examples/todo-client-localfirst-ts`
- Same schema, same server — just swap vanilla TS DOM code for React + hooks
- Demonstrates `<JazzProvider>`, `useAll`, `useOne`, and direct `db.update`/`db.insert` calls
- Lives at `examples/todo-client-localfirst-react/`

## Non-goals (this week)

- Suspense integration
- Mutation hooks
- Settlement tier indicators in UI
- React Server Components
