---
"jazz-tools": minor
---

Use aggregates such as `count()` on tables with encrypted columns. Aggregate arguments, grouping and filters can use plaintext columns. Queries that use encrypted columns in any of those places are refused. `count()` includes matching rows even while this device is waiting for keys to open their encrypted values.
