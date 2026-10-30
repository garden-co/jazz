---
"jazz-tools": patch
---

A server pump failure while a native read is waiting now surfaces as a transport error instead of an unhandled promise rejection that could crash Node.
