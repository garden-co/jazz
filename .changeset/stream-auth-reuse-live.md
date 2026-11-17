---
"jazz-tools": patch
---

`streamAuthReuseSeconds` now takes effect on a hot reload (a later `createJazz` for the same store), for open HTTP stream sessions too, as `sessionTtlSeconds` does.
