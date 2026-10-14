---
"jazz-tools": patch
---

**Breaking:** Simplified Jazz's persisted storage format:

- Removed the retired subscription-cache stores and their recovery cleanup.
- Replaced the nullable string marker for partial transactions with an `is_partial` boolean, preserving transaction validation, forwarding, and partial-to-complete application behavior.
- Consolidated Jazz compatibility markers into one format per node store, server catalogue, and account registry. Independent stores no longer inherit unrelated Jazz formats.
- Removed retired Groove large-value scalar variants; the two supported tags are now primitive `0` and chunked `1`, guarded by `groove.large-value.v2`.
- Store only the upload ID in completed-receipt reverse mappings, instead of duplicating the full upload record and chunk list. Atomic promotion, restart retries, and receipt cleanup retain their bidirectional checks.

Databases using the preceding formats are rejected at open. There is no automatic migration; create a fresh database and restore or resync its data. Current-format databases continue to retain native rows and pending writes across restarts.
