---
"jazz-tools": patch
---

Allow TypeScript `update(...)` and `updateDurable(...)` calls to clear nullable fields with `null`.

Passing `undefined` still leaves a field unchanged, and required fields still reject `null`.
