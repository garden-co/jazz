# Svelte 5 Bindings — TODO

First-party Svelte 5 bindings for jazz2, shipped as a `jazz-tools/svelte` sub-export.

## Overview

Svelte bindings follow the same sub-export packaging pattern as the React bindings (`jazz-tools/react`), living inside `packages/jazz-tools/src/svelte/` and built separately via `svelte-package`. They use Svelte 5 idioms (runes, snippets, reactive classes) rather than mirroring React hook conventions.

## Design Decisions

| Decision                      | Choice                                   | Rationale                                                                                                      |
| ----------------------------- | ---------------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| Packaging                     | Sub-export (`jazz-tools/svelte`)         | Matches React bindings pattern; avoids a separate package                                                      |
| Query API                     | `QuerySubscription` reactive class       | Idiomatic Svelte 5; reactive classes (`$state` fields) compose naturally with `$derived`, `$effect`, `{#each}` |
| Context access                | `getDb()`, `getSession()`                | Matches Svelte's `getContext()` naming; avoids React `useXxx` idiom                                            |
| Provider                      | `<JazzSvelteProvider>` component         | Wraps `createDb()` lifecycle; uses Svelte 5 snippets for children/fallback                                     |
| No `useOne` / single-row hook | Omitted                                  | Not present in React bindings; unnecessary for current scope                                                   |
| Config                        | Mount-only (no reactive reconfiguration) | Provider calls `createDb()` once on mount, tears down on destroy                                               |

## Exports

```typescript
// jazz-tools/svelte
export { JazzSvelteProvider } from "./JazzSvelteProvider.svelte";
export { getDb, getSession, getJazzContext, type JazzContext } from "./context.svelte.js";
export { QuerySubscription } from "./use-all.svelte.js";
export { SyntheticUserSwitcher } from "./SyntheticUserSwitcher.svelte";
export {
  useLinkExternalIdentity,
  type LinkExternalIdentityInput,
  type UseLinkExternalIdentityOptions,
} from "./use-link-external-identity.js";
```

## API Surface

### `<JazzSvelteProvider>`

Async context provider. Calls `createDb(config)` on mount, provides `Db` and `Session` through Svelte context, calls `db.shutdown()` on destroy.

```svelte
<JazzSvelteProvider config={dbConfig}>
  {#snippet children({ db })}
    <MyApp {db} />
  {/snippet}
  {#snippet fallback()}
    <p>Loading...</p>
  {/snippet}
</JazzSvelteProvider>
```

### `getDb()` / `getSession()`

Context accessors. `getDb()` throws if the database is not yet initialised (still loading). `getSession()` returns `Session | null`.

### `QuerySubscription`

Reactive class wrapping `db.subscribeAll()`. Instantiate in a component script block; access results via reactive properties.

```svelte
<script lang="ts">
  const todos = new QuerySubscription(app.todos.where({ done: false }));
</script>

{#if todos.loading}
  <p>Loading...</p>
{:else if todos.error}
  <p>Error: {todos.error.message}</p>
{:else}
  {#each todos.current ?? [] as todo}
    <p>{todo.title}</p>
  {/each}
{/if}
```

Properties:

- `.current: T[] | undefined` — result array (`undefined` while loading with a tier, `[]` without)
- `.loading: boolean` — `true` until first delta arrives
- `.error: Error | null` — set if `subscribeAll` throws synchronously

The constructor accepts an optional `tier` parameter (e.g. `'worker'`). Without a tier, `.current` starts as `[]` (synchronous path). With a tier, `.current` starts as `undefined` until the tier settles.

### `<SyntheticUserSwitcher>`

Dev tool for switching between synthetic user profiles during development. Manages local storage of user profiles and triggers page reloads on switch.

### `useLinkExternalIdentity(options)`

Factory function returning an async callback that links the active local anonymous/demo identity to an external JWT identity. Plain function (no React-style memoisation needed in Svelte).

## Build

```bash
# Build Svelte bindings (run from packages/jazz-tools/)
pnpm run build:svelte
# Equivalent to: svelte-package -i src/svelte -o dist/svelte --tsconfig tsconfig.svelte.json
```

The main `build` script chains `build:svelte` automatically.

## File Layout

```
packages/jazz-tools/
├── src/svelte/
│   ├── index.ts                          # barrel export
│   ├── context.svelte.ts                 # getDb, getSession, initJazzContext
│   ├── use-all.svelte.ts                 # QuerySubscription reactive class
│   ├── use-link-external-identity.ts     # external identity linking
│   ├── JazzSvelteProvider.svelte         # provider component
│   ├── SyntheticUserSwitcher.svelte      # dev user switcher
│   ├── context.test.ts                   # 6 tests
│   ├── use-all.test.ts                   # 10 tests
│   └── use-link-external-identity.test.ts # 5 tests
├── svelte.config.js                      # vitePreprocess
├── tsconfig.svelte.json                  # extends base tsconfig
└── package.json                          # "./svelte" export entry
```

## Test Coverage

| Suite                              | Tests  | Scope                                                          |
| ---------------------------------- | ------ | -------------------------------------------------------------- |
| context.test.ts                    | 6      | Context init, get/set, getDb/getSession guards                 |
| use-all.test.ts                    | 10     | Subscription wiring, loading/error states, context integration |
| use-link-external-identity.test.ts | 5      | Auth resolution, fallback, overrides, error cases              |
| **Total**                          | **21** |                                                                |

Tests run within the jazz-tools vitest suite (`vitest run src/svelte/`), mocking `svelte` module functions.

## Open Questions

- Should `QuerySubscription` be renamed to something shorter (e.g. `Query`, `LiveQuery`)?
- Granular reactivity: when the runtime supports patch-based deltas, `QuerySubscription` could update per-field rather than replacing the full array (see `granular_reactivity.md`).
- Should `getDb()` block (return a promise) rather than throw when the db is still loading?
