---
"jazz-tools": patch
---

Fix lens SQL parsing for `TIMESTAMP` defaults so numeric defaults like `DEFAULT 0` are coerced to timestamp values instead of integers.

This resolves type mismatches when applying migrations that add timestamp columns with numeric defaults, and adds regression coverage for `TIMESTAMP DEFAULT 0`.
