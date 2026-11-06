---
"jazz-tools": patch
---

Decode typed payload enum rows, including nested enum values, instead of returning
undefined. Refuse values that do not match the declared enum case or field count.
