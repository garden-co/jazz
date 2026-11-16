---
"jazz-tools": patch
---

A thin client's transactions now run in the app's backend when it connects through it (`createJazz`), which sends each finished transaction to the server when it commits, instead of the server holding it open while it is staged. The server checks the transaction as a whole, so the order of its writes no longer matters for a thin client, as for a local-first one. A staged write the backend refuses fails the whole transaction, and a commit the server doesn't confirm within 30 seconds fails with `ThinCommitOutcomeUnknownError`: it may have applied.
