---
"jazz-tools": patch
---

Servers now share one compiled query graph between subscribers of the same query. Previously every connected subscriber got a private copy, so server memory grew by a full query graph per open subscription. Each subscriber still sees only the rows its own permissions allow. Subscriptions whose query parameters differ, for example a chat id, still get separate graphs for now (#4017), and so do queries whose graph cannot yet keep subscribers apart, such as a count over rows with per-user permissions.
