---
"jazz-tools": patch
---

Fix `jazz-tools deploy` so apps without a `permissions.ts` file still publish their structural schema. The CLI now skips the permissions publish step instead of failing when no current permissions are defined.
