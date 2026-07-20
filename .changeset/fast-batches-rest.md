---
"jazz-tools": patch
---

Improve write and sync performance by replacing full row-history scans with batch-indexed or exact row lookups across batch tracking, transaction validation, permission rejection, and common parent resolution.

Disable automatic full-storage reconciliation when connecting to a server, avoiding a replay of all stored rows' history on every connection. This means rows that couldn't be synced to the server will not be sent on reconnection, instead needing to wait until a query loads them again. Also, the full client storage won't be automatically reconciled when connecting to a new server.
