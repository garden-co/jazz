---
"jazz-tools": patch
---

Normalize schema manager table columns before hashing sorting by name.

This makes logically equivalent schemas produce the same schema hash even when their column declarations are ordered differently.
