---
"jazz-tools": patch
---

Nullable columns with ordinary filter operators now expose `isNull` in TypeScript filters, including scalar, byte, and array columns. Byte and array `isNull` conditions lower without attempting to convert the boolean flag as a column value.
