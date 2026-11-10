---
"jazz-tools": patch
---

`StoreHeldByAnotherTabError` now says to close or reload the other tab, and that reloading the waiting tab before it joins loses its unsynced writes. Its old wording ("close or reload it") read as "reload this tab", which loses them. `session.restoreLocalFirst()` with a malformed secret (or none) now rejects without shutting the current client down, so the session goes on as it was instead of ending in the error state.
