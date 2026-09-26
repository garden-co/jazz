---
"jazz-tools": patch
---

Share pending page reads between concurrent browser queries within the exclusively owned IndexedDB store. Preserve write/reset ordering and ownership handoff, and retry failed reads with fresh I/O.
