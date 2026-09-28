---
"jazz-tools": patch
---

Local-tier client subscriptions of the same query with different parameters now share one prepared query shape once a second binding opens, so each write is evaluated once for all of them instead of once per subscription. A lone subscription, or reopening the same binding, keeps its own plan as before. Results are unchanged.
