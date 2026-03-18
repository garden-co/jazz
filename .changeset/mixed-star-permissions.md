---
"jazz-tools": patch
---

Fix mixed `select("*", "$canDelete")` projections so permission introspection columns can be combined with wildcard row selection, including nested include projections, and document the supported query shape.
