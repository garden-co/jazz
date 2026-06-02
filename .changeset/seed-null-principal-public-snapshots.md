---
"jazz-tools": patch
---

Allow seeding `null`-principal SSR snapshots into any session (the public-data pattern).

`<JazzProvider snapshot={...}>` previously discarded a server-rendered snapshot whenever its `principalId` didn't match the live client's authenticated user. A `null` `principalId` now marks a public snapshot — prefetched without user scoping (e.g. via `context.asBackend()`) — and seeds into any session. User-scoped (non-null) snapshots still require an exact principal match.
