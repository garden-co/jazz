---
"jazz-tools": patch
---

The background upgrade from the thin client to the local-first client now happens in place. The session keeps its client and `Db`, and that `Db` hands its live queries, listeners and writes to the local-first one. Apps that kept the `Db` in a variable keep working. Framework providers no longer remount their tree (Vue, Svelte, Solid), so component state and half-typed input survive. React queries don't drop back to loading either.

Signing in, signing out and switching accounts still give the session a new `Db`. The old one now ends its live queries with a `DbReplacedError`, and throws it on writes, `subscribe` and `onMutationError`, instead of going silent. The error says to read the current `Db` from `session.getSnapshot().client` inside `session.subscribe(...)`.
