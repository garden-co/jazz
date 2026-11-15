---
"jazz-tools": minor
"jazz-napi": minor
---

### Upgrading from alpha.64: run `jazz-tools upgrade` and review the diff

Breaking: `jazz.app.json` and `jazz.permissions.json` are now an app's only source of its schema, migrations and permissions, in TypeScript as in every other language. `schema.ts`, `permissions.ts` and `migrations/` are gone, and so is the TypeScript schema DSL.

**Upgrade:** run `npx jazz-tools@2.0.0-alpha.65 upgrade` in each app _before_ you bump `jazz-tools` in its `package.json`, then review the diff. Upgrade reads your schema.ts types through your installed alpha.64 copy, and it refuses files alpha.64's `validate --check` would refuse, so run alpha.64's `jazz-tools validate` first if they are stale. It then:

- moves `s.computed` fields and `.transform` columns into `jazz.app.json` (`computed` per table, top-level `transforms`), with their callbacks in a new `jazz.computed.ts`;
- checks that `jazz.app.json` already carries the history and migrations of `migrations/`;
- deletes `schema.ts`, `permissions.ts` and `migrations/`;
- rewrites imports (`./schema` becomes `./jazz.generated`, and backends import `./jazz.permissions.json`) and every `app.slice(...)`;
- rewrites `jazz-tools validate` in your package.json scripts to `jazz-tools generate`;
- generates `jazz.generated.ts` (`src/jazz_app.rs` for a `jazz-app` Rust crate) and typechecks the app, reporting what is left to fix.

Upgrade refuses, before writing anything, a transform with `dependsOn` or a computed field read from another computed field (the files can't express them yet), and permissions that read `process.env`/`import.meta.env`: policies are data, so a per-deployment value belongs in `session.claims`.

**What changes for your code:**

- **`validate` is renamed `generate`.** `jazz-tools generate` checks both files and writes `jazz.generated.ts`, the app's typed bindings (and `src/jazz_app.rs` when a `Cargo.toml` sits beside `jazz.app.json`). Commit them. `generate --check` is for CI; `jazz-tools validate` fails, naming the new command. App code keeps its shape: `import { app } from "./jazz.generated"`, `app.chats.where(…).include(…)`, `useAll(…)`, `db.insert(app.chats, …)`. Row and insert types are the generated types, and `include` types now survive `useAll` and `useOne`.
- **Computed fields and transforms** are declared in `jazz.app.json` and implemented in `jazz.computed.ts`. A missing implementation is a type error and a startup error. Jazz passes `null` through a transform without calling it; a transform's `from`/`to` may return `null` only when every column stored through it is nullable (a type error otherwise, and refused at runtime).
- **Migrations.** After you edit `tables`, `generate` refuses with "the schema changed since the last version: run `jazz-tools migrations create --name <name>`". That command records the previous version in `history` and writes the migration, with `"TODO: …"` strings where it can't decide; the loaders refuse those until you resolve them. `--fromHash`, `--toHash` and `--migrations-dir` are gone.
- **Backends.** Backends call `createJazz({ app, permissions })`, with `app` from `jazz.generated.ts` and `import permissions from "./jazz.permissions.json" with { type: "json" }`. Jazz's Rust loader checks and compiles the permissions. `createJazz` refuses to start when the bindings were generated from another `jazz.app.json` than the project's ("bindings out of date: run `jazz-tools generate`"); a deployed bundle without `jazz.app.json` checks the permissions only (set `JAZZ_APP_DIR` when the process starts outside the app's directory, e.g. at a monorepo root).
- **Tests.**
  - `createPolicyTestApp(app, permissions, expect)`, `startTestJazz({ app, permissions })`, `startLocalJazzServer({ schema, permissions })`, `deploy({ app, permissions })` and a backend `createJazzClient({ app, permissions })` take the permissions file.
  - `jazz-tools/testing` adds `testApp(appJson)` and `testPermissions(app, permissionsJson)` for apps a test defines inline.
  - `appMigrations` and `defineShippedApp` are gone.

**Removed:** `schema`/`s`, `migration`/`m`, `defineApp`, `defineSchema`, `definePermissions` and `jazz-tools/permissions`, `defineMigration`, `renameTableFrom`, `schemaToWasm`, `{ migrations: "none" }`, and schema slicing (`defineSliceableApp`, `app.slice(...)`), which only existed to relieve TypeScript inference. They throw `JAZZ_API_REMOVED` errors pointing at `jazz-tools upgrade`. `validate --strict-provenance`, `migrations create --fromHash/--toHash/--migrations-dir` and the Better Auth adapter's schema.ts generator are gone too: Better Auth's `generate` now writes its tables into `jazz.app.json`. See "Upgrading from alpha.64" in the docs.

jazz-napi exports Jazz's code generator and loaders: `generate`, `compileApp`, `schemaSourceHash`, `migrationsCreate`, `validateBindings` and `loadBackendApp`. Backends and `deploy` publish the deployment the Rust loader builds; the TypeScript deployment builder and schema hash are gone.
