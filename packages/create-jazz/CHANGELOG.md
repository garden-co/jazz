# create-jazz

## 2.0.0-alpha.57

## 2.0.0-alpha.56

### Patch Changes

- 326e00f: Jazz now uses explicit table-local relationship declarations instead of inferred relationship names. `s.table(columns, relations)` now requires a relationship map (use `{}` when empty). Store references as `s.uuid()` or UUID arrays, declare forward navigation with `s.rel(targetTable, column)`, and declare reverse navigation with `s.reverse(sourceTable, forwardRelationName)`.

  Jazz no longer adds automatic reverse relationships or derives relationship names from reference-name suffixes and pluralization. Declarations are validated in TypeScript and at runtime, including cross-table targets, reserved names, and conflicting reference targets. Equivalent declarations preserve existing core reference metadata and storage identity. The examples, starters, permissions, and migration tooling now use the explicit API.

  ### Migrating an existing app

  Follow this order when upgrading from alpha.55 to alpha.56:

  Run these commands from the app package directory. The examples assume its schema is in `src`; replace `src` with the actual schema directory in every command. Repeat the checks for each independently defined app schema, keeping separate snapshots.
  1. **Before upgrading Jazz packages**, export the public compiled schema with the existing alpha.55 packages and schema. Keep this file outside the schema directory and preserve it through the upgrade:

     ```sh
     pnpm exec jazz-tools schema export --schema-dir src > schema-before-alpha56.json
     ```

  2. Upgrade the app's Jazz packages together to alpha.56, then convert the whole app to the schema form below. Preserve the underlying schema identity and update query and permission callers together.
  3. Export the converted schema with alpha.56:

     ```sh
     pnpm exec jazz-tools schema export --schema-dir src > schema-after-alpha56.json
     ```

     Run this comparison using Node. It removes only table-local navigation metadata (`relations`), which does not enter storage identity, and checks everything else. It preserves column order, defaults, reference targets, indexes, branch keys, and any other exported metadata; object property order does not matter.

     ```sh
     node --input-type=module <<'NODE'
     import { readFileSync } from "node:fs";
     import { deepStrictEqual } from "node:assert";

     function structure(file) {
       const schema = JSON.parse(readFileSync(file, "utf8"));
       for (const table of Object.values(schema)) delete table.relations;
       return schema;
     }

     deepStrictEqual(
       structure("schema-after-alpha56.json"),
       structure("schema-before-alpha56.json"),
       "Stored schema changed: inspect the conversion before deploying; do not reset data.",
     );
     console.log("Stored schema structure is unchanged.");
     NODE
     ```

     If this comparison fails, stop before deploying and inspect the conversion. Do not remove additional fields to make it pass or reset existing data. If you already upgraded without a snapshot, recover the original schema and alpha.55 package versions in a separate checkout and export there; do not reconstruct the baseline from the converted schema.

  4. Run the app's TypeScript checks (for example, `pnpm exec tsc --noEmit` where that is the app's check command) and `pnpm exec jazz-tools validate --schema-dir src`. Exercise the app's queries and permissions against its existing data before deploying. Republish permissions if you changed their authored traversals. Structural equality does not prove that every query and permission caller is correct.

  Every `s.table` now requires two arguments: stored columns and explicitly named relationships. Pass `{}` when there are no relationships. Replace `s.ref(target)` with `s.uuid()` and move the target information into a forward relationship declaration. **To keep the same schema identity and existing data, preserve every stored column name, modifier, default, optionality, array shape, and reference target.** Add a matching forward relationship for every former reference column, even if no query currently uses it. This conversion needs no data rewrite or database reset.

  Before:

  ```ts
  const schema = s.defineSchema({
    users: s.table({ name: s.string() }),
    posts: s.table({
      title: s.string(),
      authorId: s.ref("users"),
      editorId: s.ref("users").optional(),
      reviewerIds: s.array(s.ref("users")).default([]),
    }),
  });
  ```

  After, with the same schema identity:

  ```ts
  const schema = s.defineSchema({
    posts: s.table(
      {
        title: s.string(),
        authorId: s.uuid(),
        editorId: s.uuid().optional(),
        reviewerIds: s.array(s.uuid()).default([]),
      },
      {
        author: s.rel("users", "authorId"),
        editor: s.rel("users", "editorId"),
        reviewers: s.rel("users", "reviewerIds"),
      },
    ),
    users: s.table(
      { name: s.string() },
      {
        postsViaAuthor: s.reverse("posts", "author"),
        postsViaEditor: s.reverse("posts", "editor"),
        postsViaReviewers: s.reverse("posts", "reviewers"),
      },
    ),
  });
  ```

  The required `authorId`, optional `editorId`, and array `reviewerIds` keep their names and still point to `users`. The array also keeps its `[]` default.
  - Declare a forward relation for **every former reference column**, including references not currently used by an include.
  - Declare reverse relations explicitly if your queries or permissions use them. The second argument to `s.reverse` is the **forward relationship name**, not its UUID column. No reverse navigation is added automatically.
  - You can keep previous navigation names, as above, or choose names such as `authoredPosts`. For example, renaming `author` to `writer` keeps `authorId` unchanged, but requires `s.reverse("posts", "writer")` and `.include({ writer: true })` in place of the old names. Update `hopTo`, relation-based query filters, and permission `hopTo` calls too, including names used in untyped query objects.
  - Relationship names cannot shadow columns, `id`, reserved `$...` fields, or reserved prototype names. If an old include replaced a same-named UUID column, preserve that stored column and choose a distinct relationship name instead.
  - Keep `{}` on tables without relationships, including tables in test fixtures, migration schema witnesses, and dynamically generated schema source. Migration operations such as `s.add.ref(...)` and `s.drop.ref(...)` remain separate APIs; do not mechanically replace those operations.
  - Update schemas shared by browser, server, and React Native consumers. Complete the structural comparison, TypeScript, validation, and app checks above before deploying.

  ### Migrating with an agent

  Point your coding agent at this guide and the release notes, and give it this instruction:

  > Follow the before/after schema export commands and Node comparison in this guide, using the app's actual schema directory. Export with alpha.55 before upgrading, convert the whole app to alpha.56 while preserving stored schema metadata, and update query and permission callers to the declared relationships. Export again and compare, removing only each table's `relations` metadata. Stop before deployment if the comparison fails. Run the app's TypeScript checks, schema validation, and queries and permissions against existing data. Never reset data to make the upgrade pass.

  Have it use the actual app schema directory as described above and check every schema consumer (browser, server, React Native, tests, migration schema witnesses, and generated schema sources). Preserve all column definitions and reference targets, indexes, and branch keys. Update permission traversals together with the declared relationship names. Keep existing stores and pending writes.

  ### Schema identity and existing data

  The underlying mapping, for example `posts.authorId → users`, already participates in schema identity and is used by core query validation and indexing. Both the old `s.ref("users")` and the new `s.rel("users", "authorId")` produce that same mapping.

  An equivalent conversion that preserves column definitions and all reference targets requires no stored-data rewrite or database reset. Keep existing stores and pending writes; do not clear browser data as an upgrade shortcut. Relationship aliases and reverse navigation declarations do not themselves change stored schema identity: renaming `author` to `writer` only changes the authored query API, provided you also update reverse declarations and callers.

  Replacing a former reference with a plain UUID **without** its forward declaration removes reference metadata; changing its target changes that metadata. Those are actual schema changes, not the API-only conversion described here. Do not use a database reset as a migration shortcut or assume an empty migration can retarget references.

  The docs, maintained examples, starter templates, permission examples, and schema-export tooling have been updated to this API. Historical fixture producers pinned to older published packages intentionally retain their original API.

- 8b34245: Bound hosted app provisioning so stalled requests and response bodies fail with a useful timeout error.
- bad023c: Report starter delete progress through local persistence, preserve local delete failures during concurrent writes, and surface later authority rejections for all mutations without retaining completed delete handles or waiting indefinitely for a disconnected server.
- 1319778: Configure matching JWT issuer and audience in Better Auth and hybrid starters so valid provider sessions can log in to Jazz.

  Enable the embedded inspector in Next development through a loopback asset server and a rewrite that preserves application routes. Honor inspector opt-out and leave production configurations unchanged.

- f3d7fea: Preserve successful workspace lookup results when an alternate workspace request fails.
- c6bc881: Validate Better Auth sessions on the server in the SvelteKit starter, redirect missing or invalid sessions from protected routes, and propagate operational session-renewal errors.

## 2.0.0-alpha.55

### Patch Changes

- d545098: Enforce an eight-character minimum password in the SvelteKit Better Auth starter.
- fc43839: Return the Next.js Better Auth starter to the sign-in route after logout and keep its cookie integration plugin last.

## 2.0.0-alpha.54

### Patch Changes

- bc70549: Jazz now runs on Groove, a new low-level database engine built around incremental view maintenance. It shares work between similar query subscriptions and provides a foundation for improving correctness and performance. This release remains an alpha: please report anything that breaks or feels slow.

  ### Breaking storage change

  Alpha.54 changes the storage format without automatic migration from alpha.53. If you have existing production data, contact us for migration help before upgrading. For a fresh start, create a new Jazz Cloud app, or use the alpha.54 CLI with fresh server storage when self-hosting. Future storage-format changes will include automatic migration.

  ### Four major changes

  **Simpler read tiers.** Choose `local-first` for immediate local results with background sync; `remote-if-possible` for remotely confirmed state when online, local fallback when offline, and immediate local writes; or `remote` for remotely confirmed state only, including confirmation of local writes before they appear.

  **Clearer account lifecycle.** Configure your account and client once with `createJazzSession`. The session manages auth transitions, graceful shutdown and client replacement. Framework adapters expose that state without app-owned lifecycle plumbing. External authentication can use `loginOrRegisterJWT`; explicit linking remains available for attaching a fresh external identity to an existing account. Backends use the same session API with `initial: { backendSecret }` or `becomeBackend({ backendSecret })`.

  **Build-your-own branching.** Define branched table views with `branchBy` columns and explicit head/base view options. Branch identifiers can be strings or references to your own branch table, letting your app define branch metadata, workflows and row-level permissions.

  **Large values are ordinary columns.** Store files and streams in `s.bytes()`, large JSON documents in `s.json()`, and text in `s.string()`. Read complete values or stream supported ranges and JSON subpaths; update them with byte patches, appends, JSON edits or string splices. Content-based chunking in a Prolly Tree makes these operations efficient while retaining ordinary column permissions and query semantics. JSON and rich-text merge strategies are planned for a later release.

  ### API migration checklist
  - Replace `Db.subscribeAll` with `Db.subscribe` for complete current results.
  - React and React Native `useAll` now return `{ data, isLoading, error }`; `useAllSuspense` still returns rows.
  - Replace `localUpdates` and `propagation` options with read-tier selection.
  - Move account/client lifecycle handling to `createJazzSession` and the framework adapters. Linking happens outside contexts; the session gracefully shuts down the old client and syncs pending writes before replacing it.
  - `$createdBy` and `$updatedBy` are non-null `{ account, identity: { issuer, subject } }` values. Use `.account` for ownership comparisons. SYSTEM authorship uses a reserved account and issuer, with the originating node as subject.
  - Direct `jazz-wasm` consumers must await `acceptSubscriber` and `acceptSubscriberWithSelfSignedProof`.

  ### Reliability improvements

  This release also improves browser and React Native persistence, offline recovery and reconnect behavior; query correctness across relations, aggregates, branches and permissions; and authentication and framework lifecycles. It includes fixes for large JSON relation values, native runtime packaging and generated starter apps.

## 2.0.0-alpha.53

## 2.0.0-alpha.52

## 2.0.0-alpha.51

## 2.0.0-alpha.50

### Patch Changes

- f463ae9: Drop Node.js 20 support. Minimum is now Node.js 22.12 (Jod LTS). `engines.node` is set to `>=22.12` on `jazz-tools` and `create-jazz`; consumers on Node 20 will see an `EBADENGINE` warning (npm/pnpm) or a hard install failure (Yarn).

## 2.0.0-alpha.49

## 2.0.0-alpha.48

## 2.0.0-alpha.47

### Patch Changes

- bc68b95: Add three TypeScript (no framework) starters: `ts-localfirst`, `ts-hybrid`, and `ts-betterauth`. Each mirrors its `react-*` counterpart but uses direct DOM manipulation inside the Jazz subscription callback, so users can see the underlying Jazz API without a UI framework in the way. The Hono + BetterAuth server in `ts-hybrid` and `ts-betterauth` is byte-identical to the corresponding `react-*` server (enforced by the parity script).

  Also expose the existing `react-localfirst`, `react-hybrid`, and `react-betterauth` starters in the interactive picker as the "React (Vite)" framework option, and accept them via `--starter` (they were previously rejected as unknown).

## 2.0.0-alpha.46

## 2.0.0-alpha.45

### Patch Changes

- 2ee98be: Add sync protocol version checks to the WebSocket handshake so incompatible clients and servers fail with an explicit update prompt.

## 2.0.0-alpha.44

## 2.0.0-alpha.43

### Patch Changes

- 5dec68f: Advance the `create-jazz` spinner to "Provisioning Jazz Cloud app" during the dashboard call, and stop credential/banner output from concatenating onto the active spinner line.

## 2.0.0-alpha.42

## 2.0.0-alpha.41

## 2.0.0-alpha.40

### Patch Changes

- 206f0a9: The "Resolving dependencies" spinner now updates as each package resolves (e.g. `Resolving dependencies (2/5)`), so `npm create jazz` no longer appears frozen during that step.
- b988375: chore: expand scaffold test coverage to all six self-hosted starters

## 2.0.0

### Patch Changes

- c5534e1: Initial release of `create-jazz` — an interactive CLI scaffolder (`npm create jazz`) with six starter templates spanning Next.js and SvelteKit across three auth modes (local-first, hybrid, BetterAuth). Resolves `workspace:*` and `catalog:` dependency references to published versions at scaffold time.
