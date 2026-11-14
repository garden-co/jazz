---
"jazz-tools": patch
---

Roll back callback transactions when their future is cancelled or their callback
panics, releasing the SQLite writer lock and preventing cancelled writes from
leaking into subsequent transactions.
