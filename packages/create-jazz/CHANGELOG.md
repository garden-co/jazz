# create-jazz

## 2.0.0-alpha.56

### Patch Changes

- 326e00f: Replace inferred relationship names with explicit table-local declarations. `s.table(columns, relations)` now requires a relationship map (use `{}` when empty). Store references as `s.uuid()` or UUID arrays, declare forward navigation with `s.rel(targetTable, column)`, and declare reverse navigation with `s.reverse(sourceTable, forwardRelationName)`.

  Remove automatic reverse relationships, reference-name suffix restrictions, and pluralization. Validate declarations in TypeScript and at runtime, including cross-table targets, reserved names, and conflicting reference targets. Preserve existing core reference metadata and storage identity when migrating equivalent declarations. Update examples, starters, permissions, and migration tooling to the explicit API.

  ### Migrating an existing app

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
  - You can keep previous navigation names, as above, or choose names such as `authoredPosts`. For example, renaming `author` to `writer` keeps `authorId` unchanged, but requires `s.reverse("posts", "writer")` and `.include({ writer: true })` in place of the old names. Update `hopTo`, relation-based filters, and permission traversals too, including names used in untyped query objects.
  - Relationship names cannot shadow columns, `id`, reserved `$...` fields, or reserved prototype names. If an old include replaced a same-named UUID column, preserve that stored column and choose a distinct relationship name instead.
  - Keep `{}` on tables without relationships, including tables in test fixtures, migration schema witnesses, and dynamically generated schema source. Migration operations such as `s.add.ref(...)` and `s.drop.ref(...)` remain separate APIs; do not mechanically replace those operations.
  - Update schemas shared by browser, server, and React Native consumers, then run TypeScript checking and `jazz-tools validate --schema-dir <your-schema-directory>`. Republish permissions if you changed their authored traversals.

  ### Migrating with an agent

  Point your coding agent at this guide and ask it to migrate the whole app while preserving schema identity. Have it check every schema consumer (browser, server, React Native, tests, and generated schema sources), preserve the column definitions and targets above, update query and permission traversals together, and run the app's TypeScript checks plus `jazz-tools validate --schema-dir <your-schema-directory>`. Keep existing stores and pending writes; do not reset data to make the migration pass.

  ### Schema identity and existing data

  The underlying mapping, for example `posts.authorId → users`, already participates in schema identity and is used by core query validation and indexing. Both the old `s.ref("users")` and the new `s.rel("users", "authorId")` produce that same mapping.

  An equivalent conversion that preserves column definitions and all reference targets requires no stored-data rewrite or database reset. Keep existing stores and pending writes; do not clear browser data as an upgrade shortcut. Relationship aliases and reverse navigation declarations do not themselves enter the core schema hash: renaming `author` to `writer` only changes the authored query API, provided you also update reverse declarations and callers.

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
