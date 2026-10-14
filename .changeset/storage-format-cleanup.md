---
"jazz-tools": patch
---

**Breaking:** Simplified Jazz's persisted storage format:

- Removed the retired subscription-cache stores and their recovery cleanup.
- Replaced the nullable string marker for partial transactions with an `is_partial` boolean, preserving transaction validation, forwarding, and partial-to-complete application behavior.
- Consolidated Jazz compatibility markers into one format per node store, server catalogue, and account registry. Independent stores no longer inherit unrelated Jazz formats.

Databases using the preceding formats are rejected at open. There is no automatic migration; create a fresh database and restore or resync its data. Current-format databases continue to retain native rows and pending writes across restarts.
