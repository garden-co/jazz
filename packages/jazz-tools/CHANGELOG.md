# jazz-tools

## 2.0.0-alpha.9

### Patch Changes

- eef9942: Fix WebAssembly fetch behavior in Next.js runtimes.
  - jazz-wasm@2.0.0-alpha.9

## 2.0.0-alpha.8

### Patch Changes

- 401db01: fix cold load of object history
- d1f17a9: fix: ensure query subgraphs share branch and schema context of parent graph
- 4775a79: Add a high-level server-side `createJazzContext` API in `jazz-tools/backend` with lazy runtime setup from generated app DSL objects, plus request/session-scoped helpers (`forRequest`, `forSession`) and lifecycle helpers (`flush`, `shutdown`).
  - jazz-wasm@2.0.0-alpha.8

## 2.0.0-alpha.7

### Patch Changes

- Add Expo support.
- 6b19ea3: Add support for JSON columns.
- 47dbdba: Added Svelte support.
  - jazz-wasm@2.0.0-alpha.7
