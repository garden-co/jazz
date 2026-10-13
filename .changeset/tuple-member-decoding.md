---
"jazz-tools": patch
---

Tuple values now decode exactly as they encode: a present nullable integer inside a tuple no longer reads back byte-swapped, and `F64` tuple members encode, decode and sort numerically instead of failing. Stored tuple bytes are unchanged.
