---
"jazz-tools": patch
---

`jazz-tools validate` and `migrations create` now say when `schema.ts` doesn't import `./migrations` (an app still on `{ migrations: "none" }`), so `migrations/index.ts` isn't written and the app ships none of its migrations. The first-snapshot advice says plainly to run `migrations create` once now, and `migrations create` says whether the stub has steps to fill in.
