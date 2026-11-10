---
"jazz-tools": patch
---

Migrations written before schema hashes moved to SHA-256 no longer apply. `jazz-tools validate` now moves them to `migrations/legacy/` and renames old-hash snapshots to their current hash, and the app runs on `schema.ts` as it is. createJazz publishes the generated `migrations/index.ts`, which never includes them. `jazz-tools validate --check` moves nothing and fails while any remain, for CI.
