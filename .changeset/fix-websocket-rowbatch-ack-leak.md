---
"jazz-tools": patch
---

Fixed the flood of ws messages by stop forwarding client-origin `RowBatchStateChanged` acknowledgements to other subscribers. This keeps per-client row-batch durability bookkeeping local to the server/client pair and avoids leaking `BatchSettlement` echoes to unrelated WebSocket clients.
