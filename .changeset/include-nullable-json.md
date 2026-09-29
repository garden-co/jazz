---
"jazz-tools": patch
---

`include()` now delivers rows whose nullable JSON column (`s.json().optional()`) is unset. Previously the read timed out with "Timed out waiting for query coverage" and subscriptions never called back, in both include directions.
