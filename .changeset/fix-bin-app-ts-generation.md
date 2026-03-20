---
"jazz-tools": patch
---

Fix `jazz build` not regenerating `app.ts` on subsequent builds.

The bin entry point was treating the TypeScript schema build as a one-time bootstrap step, skipping it whenever `current.sql` already existed. Removed the guard so `app.ts` and `current.sql` are always regenerated when `current.ts` is present.
