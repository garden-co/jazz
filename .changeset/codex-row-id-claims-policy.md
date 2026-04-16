---
"jazz-tools": patch
---

Fix row-level policies that reference a row's own `id`, including claim checks like `id IN @session.claims.editable_doc_ids`, so write permissions evaluate against the row `ObjectId` even when the table has no explicit `id` column.
