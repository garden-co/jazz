---
"jazz-tools": patch
---

Relation-backed read policies that refer to the guarded row only by its id (an access edge naming the row, reached through a recursive group closure) now evaluate once per query instead of once per row. A first sync over many rows inheriting such a policy through their parent reads several times faster on the server.
