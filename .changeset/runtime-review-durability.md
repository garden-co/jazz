---
"jazz-tools": patch
"jazz-wasm": patch
---

Keep browser storage failures visible to local durability waits, terminate pending reads when their storage owner closes, and retry abandoned upload cleanup after rolled-back maintenance. Propagate terminal subscription failures without changing write settlement, while retaining permission checks for previously delivered rows.
