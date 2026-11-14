---
"jazz-tools": patch
"jazz-rn": patch
---

BREAKING CHANGE: apps ship `jazz.app.json` and `jazz.permissions.json` instead of `migrations/index.ts`. `jazz-tools validate`, `migrations create` and `deploy` write both beside `schema.ts`, even while the app has one schema version. `jazz.app.json` holds the app's schema versions, the migrations between them and metadata for typed code; the app and its clients ship it. `jazz.permissions.json` holds the current version's permissions and their bundle id; only backends read it, so client bundles don't carry the app's compiled permissions. `migrations/index.ts` is no longer generated. `validate` removes one that an earlier version generated, and `validate --check` refuses it. `migrations/snapshots/` and the migration files stay as the history the files are built from.

To upgrade, change `schema.ts` to import `jazz.app.json` instead of `./migrations` and pass it to `defineApp` (or `defineSliceableApp`). Remove the old import first: `validate` deletes the generated `migrations/index.ts`, and a `schema.ts` that still imports it no longer loads. `schema.ts` imports only `jazz.app.json`.

```ts
// Remove: import { migrations } from "./migrations";
import artifact from "./jazz.app.json" with { type: "json" };

export const app = s.defineApp(schema, { artifact });
```

Then pass `jazz.permissions.json` to your backend instead of what `permissions.ts` exports:

```ts
import permissions from "./jazz.permissions.json" with { type: "json" };

export const jazz = createJazz({ app, permissions, store });
```

Run `npx jazz-tools validate` and commit both files. TypeScript needs `"resolveJsonModule": true` for the imports. `validate --check` fails when either committed file differs byte for byte from a fresh build, or is missing. Keep formatters off both files by adding them to `.prettierignore` or your formatter's ignore file. Passing the old `{ migrations }` throws an error that says what to change. `{ migrations: "none" }` stays for apps that only run on `memory()`.

`createJazz` now publishes exactly what the two files hold (schemas, migrations and permissions) and pins its sessions to the permissions file's bundle. It refuses `definePermissions(...)` output for an app that ships its `jazz.app.json`, saying to pass `jazz.permissions.json`; an app on `{ migrations: "none" }` still takes the `definePermissions(...)` object, and refuses the file. `startTestJazz`, `startLocalJazzServer` and `deployApp` follow the same rule. It refuses to start when `jazz.app.json` is out of date with `schema.ts` (the app's schema isn't the file's current version), or when the two files weren't written together. After you edit `permissions.ts`, run `validate` and commit `jazz.permissions.json`, or the backend keeps serving the old permissions; `validate --check` in CI catches a forgotten one.

Types: the `ClientMigrations` export is removed. `jazz.app.json`'s type is `AppArtifact` and `jazz.permissions.json`'s is `AppPermissions`. `DefineAppOptions` is now `{ artifact: AppArtifact } | { migrations: "none" }` instead of `{ migrations: ClientMigrations | "none" }`. `createJazz`'s `permissions` takes `AppPermissions` or, for an app on `{ migrations: "none" }`, the `definePermissions(...)` map.

`jazz-tools/testing`: `appMigrations` is replaced by `appArtifact(app, { steps?: [{ to, migration }] })`, which returns the `jazz.app.json` to pass as `{ artifact }`, and `appPermissions(app, permissions)`, which returns the `jazz.permissions.json` a backend (`createJazz`, `startTestJazz`) takes for the `definePermissions(...)` map `permissions`.

React Native: the native relay ABI is now 7, so the JavaScript bundle and the native build must both come from this release.

Rust:

- Backends read both files with `jazz::tools::deployment::artifact::load(app, permissions)` (see `examples/todo-server-rs`), which refuses a permissions file whose `schema` isn't the app file's `current`.
- Clients read `jazz.app.json` alone with `artifact::load_client`, which skips its codegen section unread.
- `artifact::write` returns both texts.
- `published::client_migrations_module`, `published::client_migrations` and `published::check_client_migrations` are removed.
- In jazz-napi, `checkClientMigrations` is replaced by `writeAppArtifact` (which returns `{ app, permissions }`), `loadAppArtifact(app, permissions)` and `schemaHash`.
