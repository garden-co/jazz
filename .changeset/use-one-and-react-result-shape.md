---
"jazz-tools": minor
---

Add `useOne`/`useOneSuspense` single-row query bindings across the framework layers (React, Expo, Vue, Solid `useOne`; Svelte `SingleRowSubscription`). Each runs the query with `limit 1` (mirroring `db.one`) and exposes the first matching row: `undefined` while loading, `null` once resolved with no match, the row otherwise.

**Breaking (React/Expo):** the non-suspense `useAll` and `useOne` now return `{ data, isLoading, error }` instead of a bare value, aligning React with Solid's binding shape. Read rows from `.data` (e.g. `const { data: todos } = useAll(query)`). The Suspense variants `useAllSuspense`/`useOneSuspense` are unchanged and still return the bare value.
