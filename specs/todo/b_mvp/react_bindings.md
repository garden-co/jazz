# React Bindings — TODO

First-class React hooks for reactive queries against the local database.

## Overview

Provide a React integration layer on top of the browser `Db` class:

- `useQuery(sql, params, tier?)` — reactive hook that re-renders on data changes
- `useMutation()` — returns typed insert/update/delete functions
- `<JazzProvider>` — context provider that initializes Db, worker, and server connection
- Suspense support for initial query settlement
- Optimistic UI via local-first writes (instant) + settlement tier tracking

## Open Questions

- Hook API: SQL strings vs. type-safe query builder?
- Generated types from schema (codegen from `ts_client_codegen`)?
- How to expose settlement tier to components (loading → local → worker → server)?
- React Server Components compatibility?
- Bundle size considerations — tree-shaking the WASM module
