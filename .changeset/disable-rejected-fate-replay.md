---
"jazz-tools": patch
"jazz-napi": patch
---

Disable cold-start replay of persisted rejected batch fates so runtimes no longer surface stale mutation errors or retract visible rows from stored rejection records on startup.
