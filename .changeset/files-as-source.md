---
"jazz-tools": minor
"jazz-napi": minor
---

### Upgrading from alpha.65: run `jazz-tools upgrade` and review the diff

Breaking: alpha.66 gives the schema a cleaner home and permissions a much leaner DSL. Each app defines its tables, permissions and migrations in one `schema.ts`, in every language, and one command, `npx jazz-tools generate`, turns it into readable `jazz.app.json` / `jazz.permissions.json` files and typed bindings for TypeScript, Rust, Go, Swift and Kotlin. The table, column and migration builders are the ones you know from alpha.65; how the schema is mounted (`defineApp`) and how permissions are written change. `jazz-tools upgrade` converts an alpha.65 app for you and proves nothing it deploys changed.

**`schema.ts`** (beside the app's manifest: package.json, Cargo.toml, go.mod, Package.swift or build.gradle):

```ts
import { schema as s } from "jazz-tools";

export const schema = s.defineSchema({
  chats: s.table({ isPublic: s.boolean() }, { membersViaChat: s.reverse("chatMembers", "chat") }),
  chatMembers: s.table({ chatId: s.uuid(), userId: s.uuid() }, { chat: s.rel("chats", "chatId") }),
});

const { definePermissions, exists, session, anyOf } = schema;
const isMember = (chatId: s.Uuid) => exists.chatMembers({ chatId, userId: session.user });

export const permissions = definePermissions({
  chats: { read: (chat) => anyOf({ isPublic: true }, isMember(chat.id)) },
  chatMembers: { read: (m) => isMember(m.chatId), insert: { userId: session.user } },
});
```

- **Permissions** use a new, much shorter DSL on the schema: per table `read`, `insert`, `update` (or `updateIf` + `updateTo`, where `updateTo: (row, old) => …` can compare with the old row) and `delete`; conditions are objects (`{ col: v }`, arrays mean `in`), `exists.<table>({…})`, `row.<relation>.where({…})`, `allowedTo.<op>(row.<relation>)`, `anyOf`/`allOf`/`not`, comparison helpers and `session`. Helpers are plain functions. Every table must be listed (`{}` denies everything), an operation left out is denied, and types check tables, columns, values, relations and rule keys. It covers the whole expression grammar of `jazz.permissions.json`. A separate `permissions.ts` exporting `permissions` still works; both at once is refused.
- **`generate`** (renamed from `validate`, which now fails naming it) loads `schema.ts` and writes `jazz.app.json` and `jazz.permissions.json`, then the bindings: `jazz.generated.ts`, or Rust, Go, Swift or Kotlin bindings, detected from the manifest beside `schema.ts` (`--lang` overrides). Commit them all. `generate --check` (for CI) writes nothing and fails when anything is stale. Writing needs `npx`, so Node, in every language; the standalone Rust `jazz-tools` binary only runs `generate --check`.
- **The JSON files are generated.** They carry a versioned `$schema` (`https://jazz.tools/schemas/jazz.app.v1.json`, `…/jazz.permissions.v1.json`) and a `$generated` digest. `generate` refuses to overwrite a file edited by hand, or one it didn't write, unless given `--force`. Backends and other languages read them: `createJazz({ app, permissions })` with `app` from `./jazz.generated` and `import permissions from "./jazz.permissions.json" with { type: "json" }`.
- **Migrations** live in `migrations/*.ts` (`m.defineMigration`, as in alpha.64) with tool-written snapshots in `migrations/snapshots/`, and become `history` and `migrations` in `jazz.app.json`. After a schema change `generate` asks for `npx jazz-tools migrations create --name <name>`; resolve each `TODO` it writes. There is no `migrations/index.ts`.
- **Computed fields and transforms** are declared in `schema.ts` (`s.computed(type, ["from", …])` among the columns, `.transform(name, viewType)`) and implemented in `jazz.computed.ts`.
- **Runtime code** imports `app` and the row types from `./jazz.generated`; nothing imports `schema.ts` at runtime.
- **Better Auth:** the adapter's generator writes `schema-better-auth/schema.ts` exporting `tables` and `permissions`, which `schema.ts` spreads.
- **Tests:** `createPolicyTestApp`, `startTestJazz`, `deploy` and backend clients take the generated `app` and permissions file; `jazz-tools/testing` adds `testApp(appJson)` and `testPermissions(app, permissionsJson)` for apps a test defines inline.

**Upgrade:** run `npx jazz-tools@2.0.0-alpha.66 upgrade` in each app _before_ bumping `jazz-tools`, then review the diff. It first checks with your installed alpha.65 (or .64) that the committed JSON is fresh, then:

- rewrites `schema.ts` from `defineApp(schema, { migrations })` to `export const schema = s.defineSchema({ … })`, keeping your comments;
- prints the permissions from the committed `jazz.permissions.json` into `schema.ts` in the new DSL (or a separate `permissions.ts` when the old one was large), factoring repeated checks into named helpers and keeping the old file's named constants;
- converts Better Auth's module to export `tables` and `permissions`;
- rewrites imports of `./schema` to `./jazz.generated`, `app.slice(…)` to `app`, and `validate` to `generate` in package.json scripts;
- deletes `migrations/index.ts` (stubs and snapshots stay);
- runs `generate`, and checks that the schema hashes and bundle id are unchanged. Otherwise it puts every file back and refuses.

Upgrade refuses, before writing anything, an app with computed columns or transforms (move those into `schema.ts` by hand), a policy that reads `process.env` and compiled to a bare `true`, and what alpha.65's own `validate --check` refuses. Other environment reads are listed under "Left for you".

**Removed:** `defineApp` and `{ migrations }` mounting, schema slicing (`defineSliceableApp`, `app.slice(…)`), the callback permissions DSL (`definePermissions(app, ({ policy }) => …)`, root-level `anyOf`/`allOf`/`createSessionContext`), `migrations/index.ts`, `schemaToWasm`, `validate --strict-provenance` and `migrations create --fromHash/--toHash/--migrations-dir`. Removed names throw `JAZZ_API_REMOVED` errors naming their replacement and pointing at `jazz-tools upgrade`. See "Upgrading from alpha.65" in the docs.

jazz-napi exports Jazz's code generator and loaders (`generate`, `compileApp`, `schemaSourceHash`, `migrationsCreate`, `validateBindings`, `loadBackendApp`) and the generated-file writer `writeAppFiles` with `verifyGenerated`.
