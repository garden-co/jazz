---
"jazz-tools": patch
---

Avoid rejecting exclusive single-table predicate reads when unrelated local writes leave their result unchanged. Matching local or remote phantoms still reject, and relational, whole-table and aggregate reads retain their existing conservative local conflict checks.
