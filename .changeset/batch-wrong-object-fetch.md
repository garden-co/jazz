---
"jazz-tools": patch
---

A chunk I/O batch that fetched the wrong object for a node now counts that fetch as failed, so the host backs off as it does for the fetch lane.
