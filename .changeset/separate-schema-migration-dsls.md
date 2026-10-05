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

Rewrite existing files in `migrations/` too, including already-deployed ones: `deploy` loads the whole migration chain, and a file that still uses the old builders now fails with `Failed to load migration <file>` and a pointer to `migration as m`.
