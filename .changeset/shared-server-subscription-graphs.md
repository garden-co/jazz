---
"jazz-tools": patch
---

Servers now share one compiled query graph between subscribers of the same query. Previously every connected subscriber got a private copy, so server memory grew by a full query graph per open subscription. Each subscriber still sees only the rows its own permissions allow.
