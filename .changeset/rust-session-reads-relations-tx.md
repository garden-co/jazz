---
"jazz-tools": patch
---

**Rust: reads made for a user also apply the user's read policy to related rows and to reads inside a transaction.** For a session-scoped client (`JazzClient::for_session`, and `jazz-app-support`'s `for_user`):

- a local one-shot read with array relations nests only the related rows that user may read, as their subscriptions do;
- a read inside a transaction opened from that client reads as that user, with that handle's claims.

This matches the TypeScript runtime.
