---
"jazz-tools": patch
---

The session snapshot has a `localFirst` field: whether its client reads and writes through a local store (`client.db.isLocalFirst()`). The session publishes a new snapshot when it upgrades a thin client to local-first in place, so apps can show "offline support ready", or notice they are still thin, by subscribing instead of polling `db.isLocalFirst()`.
