---
"jazz-tools": patch
---

Align React and React Native `useAll` with the other framework bindings by returning `{ data, isLoading, error }` instead of a bare `T[] | undefined`. `useAllSuspense` still returns `T[]`.
