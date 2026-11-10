---
"jazz-tools": patch
---

The thin client (`jazz-tools/client` before its local-first upgrade) now reports its link in `getSyncStatus()`: connected, offline after a sustained outage, and offline with reason `"auth"` when the server refuses its sign-in. Before, it always said "connecting", so a browser whose user lost their account on the core (a reset dev store, a restore, a new core) never signed in again: its writes hung and "Account auth refresh failed" repeated, even after a reload. It now signs the user in again, as documented.
