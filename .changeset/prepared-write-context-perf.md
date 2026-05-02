---
"jazz-tools": patch
---

Improve local insert performance by reusing prepared write context through row-history application and caching catalogue row descriptors for repeated same-schema writes.
