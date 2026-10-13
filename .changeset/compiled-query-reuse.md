---
"jazz-tools": patch
---

Queries are compiled once per shape and bound to their parameters, so many subscriptions of the same query with different parameters reuse one compiled program and plan. Opening each extra subscription is much cheaper, and inserts and updates do less work.
