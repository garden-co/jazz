---
"jazz-tools": patch
---

Permission advice for reads, deletes and the `using` clause of updates now proves only the target row instead of re-evaluating the policy across the table, which makes permissioned writes on large tables much faster.
