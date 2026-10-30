---
"jazz-tools": patch
---

Reduce memory use and CPU time when reading large values by checking each stored chunk's canonical encoding in place instead of rebuilding a temporary copy of it.
