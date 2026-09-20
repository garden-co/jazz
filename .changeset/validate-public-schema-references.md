---
"jazz-tools": patch
---

Reject schema references to undeclared tables and references on non-UUID columns with source-specific diagnostics. References to declared tables may still point to rows that do not exist yet.
