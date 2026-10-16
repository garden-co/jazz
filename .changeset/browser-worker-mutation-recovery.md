---
"jazz-tools": patch
---

Detect unresponsive browser storage workers while mutations await local durability or when a page resumes, and automatically replace failed worker connections while preserving queued transactions. Recovery works offline and retains the original write handles.
