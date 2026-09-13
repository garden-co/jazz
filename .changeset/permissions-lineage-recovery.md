---
"jazz-tools": patch
---

Recover permissions publication when a schema migration is already durable but missing from the running server. Reject missing or ambiguous lineage before advancing the permissions head, so publication can be retried safely.
