---
"jazz-tools": patch
---

Fix SQL array equality joins to compare complete ordered values, including empty arrays, without duplicating result weights. Prepared SQL bindings preserve independent subscriber ownership when floating-point signed zeros compare equal. Graph-based reference and policy membership joins are unchanged.
