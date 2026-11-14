---
"jazz-tools": patch
---

Session claims now compare as the type of what they meet, the same way in every policy form (a row's own column, `exists.where`, relation-style `exists(...)`, claim lists and `contains`) and in query filters:

- A string claim matches a uuid column (or `id`, or a uuid literal) by the uuid it spells, in any spelling the uuid parser accepts.
- A uuid-shaped claim matches a text column by its exact text.
- An Integer or BigInt column accepts a canonical decimal string (`"42"`, not `"042"`) as well as a number. Timestamps take numbers only.
- A claim the column can't hold matches nothing.

Before, some of these combinations silently granted nothing. Note what changes for negated or `!=` policies: a claim that used to compare as unequal and grant can now compare as equal (or as unmatched) and deny. For example, `not(...)` around a uuid-shaped claim compared with a text column, or a `"042"` claim compared with an int column.

Provider claims that are uuid-shaped strings now reach host code as strings rather than uuid values.
