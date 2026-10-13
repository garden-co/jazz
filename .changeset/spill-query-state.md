---
"jazz-tools": patch
---

Servers with a memory limit now spill query join state to a scratch store on disk when memory runs high, instead of growing until the container is killed. Results are unchanged; spilled queries read their state back from disk.
