---
"jazz-tools": patch
---

Resolve a stored version's writer node in constant time instead of scanning every node the server has seen. Servers with many client nodes no longer slow down per write or open in quadratic time.
