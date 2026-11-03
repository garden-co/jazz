---
"jazz-tools": patch
---

Migrations written before schema hashes moved to SHA-256 no longer apply. `jazz-tools validate` now moves them to `migrations/legacy/` and renames old-hash snapshots to their current hash, and the app runs on `schema.ts` as it is. The dev server refuses to publish while such migrations are still in place, and its error points at `validate`. createJazz publishes the generated `migrations/index.ts`, which never includes them.
