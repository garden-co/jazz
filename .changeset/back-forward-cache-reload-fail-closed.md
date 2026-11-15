---
"jazz-tools": patch
---

A local-first tab coming back from Chrome's back/forward cache to a store that was wiped or reset meanwhile never reconnects to it, even when it can't reload because it already reloaded for an invalidation moments before. Its reads and writes that need the worker fail with an error asking to reload the page, instead of writing into the new store.
