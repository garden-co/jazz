---
"jazz-tools": minor
"jazz-napi": minor
---

### Upgrading from alpha.64: run `jazz-tools upgrade` and review the diff

Breaking: `jazz.app.json` and `jazz.permissions.json` are now an app's only source of its schema, migrations and permissions, in TypeScript as in every other language. `schema.ts`, `permissions.ts` and `migrations/` are gone, and so is the TypeScript schema DSL.

**Upgrade:** run `npx jazz-tools@alpha upgrade` in each app *before* you bump `jazz-tools` in its `package.json`, then review the diff. Upgrade reads your schema.ts types through your installed alpha.64 copy, and it refuses files `validate --check` would refuse, so run alpha.64's `jazz-tools validate` first if they are stale. It then:
- moves `s.computed` fields and `.transform` columns into `jazz.app.json` (`computed` per table, top-level `transforms`), with their callbacks in a new `jazz.computed.ts`;
- checks that `jazz.app.json` already carries the history and migrations of `migrations/`;
- deletes `schema.ts`, `permissions.ts` and `migrations/`;
- rewrites imports (`./schema` becomes `./jazz.generated`, and backends import `./jazz.permissions.json`) and every `app.slice(...)`;
- generates `jazz.generated.ts` and typechecks the app, reporting what is left to fix.

Upgrade refuses a transform with `dependsOn`, which the files can't express yet.

**What changes for your code:**
- **Bindings.** `jazz-tools validate` checks both files and generates `jazz.generated.ts`, the app's typed bindings. Commit it. App code keeps its shape: `import { app } from "./jazz.generated"`, `app.chats.where(…).include(…)`, `useAll(…)`, `db.insert(app.chats, …)`. Row and insert types are the generated interfaces, and `include` types now survive `useAll` and `useOne`.
- **Computed fields and transforms** are declared in `jazz.app.json` and implemented in `jazz.computed.ts`. A missing implementation is a type error and a startup error.
- **Migrations.** After you edit `tables`, `validate` refuses with "the schema changed since the last version: run `jazz-tools migrations create --name <name>`". That command records the previous version in `history` and writes the migration, with `"TODO: …"` strings where it can't decide; the loaders refuse those until you resolve them. `--fromHash`, `--toHash` and `--migrations-dir` are gone.
- **Backends.** Backends call `createJazz({ app, permissions })`, with `app` from `jazz.generated.ts` and `import permissions from "./jazz.permissions.json" with { type: "json" }`. Jazz's Rust loader checks and compiles the permissions. `createJazz` refuses to start when the bindings were generated from another `jazz.app.json` than the project's ("bindings out of date: run `jazz-tools validate`"); a deployed bundle without `jazz.app.json` checks the permissions only.
- **Tests.**
  - `createPolicyTestApp(app, permissions, expect)` and `startTestJazz({ app, permissions })` take the permissions file.
  - `jazz-tools/testing` adds `testApp(appJson)` and `testPermissions(app, permissionsJson)` for apps a test defines inline.
  - `appMigrations` and `defineShippedApp` are gone.

**Removed:** `schema`/`s`, `migration`/`m`, `defineApp`, `defineSchema`, `definePermissions` and `jazz-tools/permissions`, `defineMigration`, `renameTableFrom`, `schemaToWasm`, `{ migrations: "none" }`, and schema slicing (`defineSliceableApp`, `app.slice(...)`), which only existed to relieve TypeScript inference. They throw `JAZZ_API_REMOVED` errors pointing at `jazz-tools upgrade`. `validate --strict-provenance` is gone too. See "Upgrading from alpha.64" in the docs.

jazz-napi exports Jazz's code generator and loaders: `generate`, `compileApp`, `schemaSourceHash`, `migrationsCreate`, `validateBindings` and `loadBackendApp`.
