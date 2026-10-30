---
"jazz-tools": patch
---

Variable-length array fields in stored records now reject offset tables whose arithmetic would overflow, instead of wrapping. Encoded bytes are unchanged.
