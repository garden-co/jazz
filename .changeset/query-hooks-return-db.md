---
"jazz-tools": patch
---

Query hooks now return the `db` handle next to their data, so a component that reads and writes needs one hook instead of two: `const { data: todos = [], db } = useAll(app.todos.where({ done: false }))`. This covers `useAll` and `useOne` in React, React Native, Expo, Vue and Solid, and the `QuerySubscription` / `QuerySubscriptionOne` classes in Svelte (`todos.db`). The React suspense hooks change shape to match: `useAllSuspense` and `useOneSuspense` return `{ data, db }` instead of the bare rows, so `const todos = useAllSuspense(query)` becomes `const { data: todos, db } = useAllSuspense(query)`. Vue's `useAllSuspense` / `useOneSuspense` gain `db` too. `useDb()` and `getDb()` stay for components that only write.
