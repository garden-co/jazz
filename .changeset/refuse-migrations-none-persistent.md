---
"jazz-tools": patch
---

BREAKING CHANGE: `createJazz` refuses an app defined with `s.defineApp(schema, { migrations: "none" })` when its store keeps data (`sqlite()`, `vercel()` or `remote()`). Such an app ships no schema history, so the store could not follow its first schema change and would refuse the app. `createJazz` now throws when it is called, with an error that names the fix. `memory()` stores and the helpers in `jazz-tools/testing` still take `"none"`.

To upgrade, ship your migrations: add `import { migrations } from "./migrations"` to `schema.ts` and pass `{ migrations }` to `defineApp`, then run `npx jazz-tools migrations create`. It records `schema.ts` as the app's first version and generates `migrations/index.ts`. Commit the `migrations/` directory with your schema.
