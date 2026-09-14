---
"jazz-tools": patch
---

Reject explicit table-level `id` columns in schema definitions, including at TypeScript type-check time. Every table uses its automatically generated UUID `id` as its row identity; remove authored `id` declarations before upgrading.
