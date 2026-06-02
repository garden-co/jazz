---
"jazz-tools": patch
---

Render SSR snapshot seeds synchronously on first paint.

`<JazzProvider snapshot={...}>` now seeds a synchronous, read-only orchestrator from the snapshot and renders the prefetched rows on the first render — server-side included — instead of suspending on the async client. The server-rendered HTML already contains the data and matches the client's first paint, so there is no hydration re-render. Once the live client connects it is seeded with the same rows and swapped in transparently, after which live updates stream from it. Reads (`useAll`) use this seed-or-live orchestrator; hooks that need the live client (`useDb`, `useSession`, `useAuthState`) suspend until it connects, as before. Behaviour is unchanged when no snapshot is passed.
