---
"jazz-tools": patch
---

Browser and React Native reads that name no tier now wait up to 5 s for the server on their first load while online (`firstLoadRemoteWaitMs: 5000`), so a one-shot read no longer returns a stale cache. Pass `tier: "local-first"` or `firstLoadRemoteWaitMs: 0` to show local data at once. Servers and Node still default to `"remote"`.
