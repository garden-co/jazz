---
"jazz-tools": patch
---

Fix `@session.__jazz_outer_row.id` not resolving inside EXISTS subquery policies. Previously the outer row's UUID was silently treated as an unresolvable column, causing all EXISTS policy checks to evaluate to false on the server.
