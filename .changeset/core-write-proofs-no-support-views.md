---
"jazz-tools": patch
---

Prove writes allowed by `inherits`, join or `reachable` write policies from the sync server's own storage, without keeping a view of every row the policy matches for the life of the writer's connection.
