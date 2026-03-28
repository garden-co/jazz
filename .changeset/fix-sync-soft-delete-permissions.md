---
"jazz-tools": patch
---

Fix a sync-server permission bypass where replicated soft deletes could skip `DELETE` policy evaluation.

User writes received as `ObjectUpdated` payloads now inspect delete metadata before the sync permission check is queued. Soft-delete commits are classified as `DELETE` operations instead of `UPDATE`, so replicated row deletions correctly use delete policies and are rejected when the client lacks delete access.
