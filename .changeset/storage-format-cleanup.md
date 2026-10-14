---
"jazz-tools": patch
---

**Breaking:** Simplified Jazz's persisted storage format:

- Removed the retired subscription-cache stores and their recovery cleanup.
- Replaced the nullable string marker for partial transactions with an `is_partial` boolean, preserving transaction validation, forwarding, and partial-to-complete application behavior.
- Consolidated Jazz compatibility markers into one format per node store, server catalogue, and account registry. Independent stores no longer inherit unrelated Jazz formats.
- Removed retired Groove large-value scalar variants; the two supported tags are now primitive `0` and chunked `1`, guarded by `groove.large-value.v2`.
- Store only the upload ID in completed-receipt reverse mappings, instead of duplicating the full upload record and chunk list. Atomic promotion, restart retries, and receipt cleanup retain their bidirectional checks.
- Give completed uploads a dedicated record with required receipt and descriptor fields, removing their unused chunk lists and creation timestamps.

- Remove the repeated 32-byte magic prefix from each stored chunk (`groove.ordered-chunk-storage.v2`), retaining installation receipts and content integrity checks.

- Derive inserted UTF-16 lengths from the stored insertion bytes instead of persisting them, saving eight bytes per large-value edit.

- Omit the two always-zero UTF-16 coordinates from binary large-value edits, saving another sixteen bytes per edit; text edits retain them.

- Omit UTF-16 length from binary large-value descriptors and store it as a required integer for text/JSON, saving nine bytes per binary descriptor and one byte per text descriptor.

Databases using the preceding formats are rejected at open. There is no automatic migration; create a fresh database and restore or resync its data. Current-format databases continue to retain native rows and pending writes across restarts.
