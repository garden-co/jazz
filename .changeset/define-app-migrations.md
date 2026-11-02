---
"jazz-tools": patch
---

BREAKING CHANGE: `s.defineApp` and `s.defineSliceableApp` now take the app's migrations as a second argument, and throw without it. `jazz-tools validate` and `deploy` write them to `migrations/index.ts` beside `schema.ts`, even while the app has one schema version. Clients use them to store every version exactly as the server does, so they no longer rename stored columns when they sync.

To upgrade, run `jazz-tools validate`, then pass the generated module to your app:

```ts
import { migrations } from "./migrations";

export const app = s.defineApp(schema, { migrations });
```

Commit `migrations/index.ts` with your schema. Apps that are never deployed, such as a throwaway test app, can pass `{ migrations: "none" }`. Tests that define their schemas in code can build the module with `appMigrations` from `jazz-tools/testing`. A client whose schema version is missing from its migrations refuses to open and tells you to regenerate them. Rust apps load the same module with `jazz::tools::deployment::published::client_migrations_module`.
