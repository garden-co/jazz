---
"jazz-tools": patch
---

Session claims now compare as the type of the column they meet, the same way in every policy form (a row's own column, `exists.where`, relation-style `exists(...)`) and in query filters: a uuid-shaped claim matches a text column by its text, a string claim matches a uuid column (or `id`) by the uuid it spells, and an integer column accepts `"42"` as well as `42`. Before, some of these combinations silently granted nothing.
