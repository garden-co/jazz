---
"jazz-tools": patch
---

Reduce encrypted lifecycle query work by reusing accepted histories and snapshots, sharing account-history preparation and coalescing concurrent device challenge responses. Keep private metadata prefetches account-scoped and refresh every history predicate after snapshot conflicts.
