---
"jazz-tools": patch
---

`jazz-tools validate` no longer tells you to run `migrations create` when schema.ts differs from its latest snapshot only in its indexes (`indexOnly`, `compositeIndex`). It says no migration is needed and to commit the regenerated files.
