---
"jazz-tools": patch
"jazz-rn": patch
---

BREAKING CHANGE: apps ship `jazz.app.json` instead of `migrations/index.ts`. `jazz-tools validate`, `migrations create` and `deploy` write it beside `schema.ts`, even while the app has one schema version. It holds the app's schema versions, the migrations between them and the current version's permissions. `migrations/index.ts` is no longer generated. `validate` removes one that an earlier version generated, and `validate --check` refuses it. `migrations/snapshots/` and the migration files stay as the history the file is built from.

To upgrade, change `schema.ts` to import the file and pass it to `defineApp` (or `defineSliceableApp`):

```ts
import artifact from "./jazz.app.json" with { type: "json" };

export const app = s.defineApp(schema, { artifact });
```

Then run `npx jazz-tools validate` and commit `jazz.app.json`. TypeScript needs `"resolveJsonModule": true` for the import. `validate --check` fails when the committed file differs byte for byte from a fresh build, or is missing. Keep formatters off the file by adding it to `.prettierignore` or your formatter's ignore file. Passing the old `{ migrations }` throws an error that says what to change. `{ migrations: "none" }` stays for apps that only run on `memory()`.

`createJazz` now publishes exactly the file's deployment (schemas, migrations and permissions) and pins its sessions to the file's bundle. It refuses to start when the file is out of date: when the app's schema isn't the file's current version ("out of date with schema.ts"), or when the permissions passed to `createJazz` don't make the file's bundle ("out of date with permissions.ts"). Run `validate` and commit the file.

`jazz-tools/testing`: `appMigrations` is replaced by `appArtifact(app, { steps?: [{ to, migration }], permissions? })`, which returns an artifact to pass as `{ artifact }`. A backend (`createJazz`, `startTestJazz`) needs the artifact built with the permissions it is given.

React Native: the native relay ABI is now 7, so the JavaScript bundle and the native build must both come from this release.

Rust:

- Apps read the file with `jazz::tools::deployment::artifact::load` (see `examples/todo-server-rs`).
- Clients read it with `artifact::load_client`, which takes only its schemas and migrations and skips the permissions and codegen unread.
- `published::client_migrations_module`, `published::client_migrations` and `published::check_client_migrations` are removed.
- In jazz-napi, `checkClientMigrations` is replaced by `writeAppArtifact`, `loadAppArtifact`, `schemaHash` and `permissionsBundleId`.
