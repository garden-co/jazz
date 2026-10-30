---
"jazz-tools": patch
---

Closing a transport while an operation on the same runtime is suspended no longer aborts the process. The connection is detached on the next tick instead.
