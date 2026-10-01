---
"jazz-tools": patch
---

BREAKING CHANGE: Separate schema and migration namespaces; remove the combined `col` export.

```ts
import { schema as s, migration as m } from "jazz-tools";
```

- Replace `col` column builders with `s` (e.g. `col.int()` → `s.int()`).
- Move `add`, `drop`, and `renameFrom` calls from `s` or `col` to `m`.
- Replace `s.defineMigration` and `s.renameTableFrom` with `m.defineMigration` and `m.renameTableFrom`.

Schema and permission helpers stay under `s`. Migration behavior and serialized formats are unchanged.
