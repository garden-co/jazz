---
"jazz-tools": patch
---

**Breaking (small):** `<JazzProvider>`'s `error` view receives the same `{ error, recovery, retry }` in every framework (the new `JazzSessionErrorViewProps` type). `error` is always set, `recovery` says what `retry()` does (`"action"`: repeat the operation that failed; `"session"`: reopen the signed-in account), and this `retry()` never rejects: a new failure shows in the error view again, so `onClick={retry}` is safe. For the rest of the session (its `user`, `signOut()`, ...), call `useJazzSession()` (Svelte: `getJazzSession()`) inside the view.

### Migrating a custom error view

- **Solid:** `error={(error, retry) => …}` becomes `error={({ error, retry }) => …}`.
- **Svelte:** `{#snippet error(error, retry)}` becomes `{#snippet error({ error, retry })}`.
- **React, React Native, Expo:** `error={(state) => …}` receives `{ error, recovery, retry }` instead of the whole `useJazzSession()` result. Views that read only `state.error` and `state.retry()` keep working (and `state.error` is no longer optional). Views that read anything else from `state` call `useJazzSession()` inside the view instead.
- **Vue:** the `#error` slot gains `recovery`, and its `error` is always set. Nothing to change.
