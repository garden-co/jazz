---
name: jazz-core
description: Build, modify, and troubleshoot ordinary TypeScript application features using Jazz. Use for defining an initial app schema, querying or subscribing to rows, making optimistic local-first writes, waiting for sync confirmation, integrating React, Vue, Svelte, Solid, or plain TypeScript clients, and investigating Jazz application code that is not behaving as intended. Use jazz-auth for identity, jazz-backend for TypeScript servers, jazz-expo for Expo or React Native, jazz-files for chunked storage, jazz-rust for Rust, jazz-sync for convergence or offline conflict design, and jazz-schema-evolution for changes to an existing schema, permissions, or migrations.
---

# Jazz Core

Implement application features through the installed `jazz-tools` public API. Start from the
project's installed Jazz version, schema, framework setup, and tests. Preserve those choices unless
the requested change intentionally replaces them.

## Start from the project

1. Read the installed `jazz-tools` version from the nearest `package.json` or lockfile.
2. Locate `schema.ts`, optional `permissions.ts`, the Jazz provider/client setup, and representative
   reads and writes.
3. Follow the project's established framework, auth mode, import style, and file layout.
4. Inspect the installed type declarations when an API shape is uncertain. Do not substitute an API
   remembered from another Jazz version or a similar database.
5. Read the bundled reference that matches the task:
   - [queries-and-writes.md](references/queries-and-writes.md) for schema usage, reads, writes,
     relations, batching, and durability.
   - [frameworks.md](references/frameworks.md) for providers, reactive queries, database access,
     loading states, and framework-specific imports.

## Preserve the Jazz model

- Define data with the TypeScript schema DSL and export a typed `app` with `s.defineApp(...)`.
- Use typed table handles such as `app.todos` for queries and mutations.
- Keep query builders immutable and compose them with `where`, `select`, `include`, `orderBy`,
  `limit`, and `offset`.
- Treat writes as local-first and optimistic. `insert`, `update`, and `delete` apply locally and
  return a write handle; call `.wait({ tier })` when the caller needs sync confirmation.
- Treat subscriptions as long-lived reactive reads. Preserve the distinction between “not delivered
  yet” (`undefined` in framework bindings) and “delivered with no matches” (`[]`).
- Keep permissions in `permissions.ts`; do not encode authorization by hiding UI or filtering only on
  the client.

## Choose the read shape

- Use `db.all(query)` or `db.one(query)` for a one-shot read.
- Use the framework's reactive query API inside components.
- Use `db.subscribeAll(query, callback)` outside framework bindings and dispose the returned
  subscription when its owner is destroyed.
- Use `include(...)` for typed forward and reverse relations instead of manually joining IDs in
  application code.
- Omit an explicit read tier for normal local-first UI. Request `edge` or `global` only when delivered
  data must have reached that sync tier.
- A read or subscription tier remains part of its delivery semantics. Use `localUpdates` to decide
  whether optimistic local writes can appear while stronger durability catches up; high-level `Db`
  reads and subscriptions default to `"deferred"`.

## Choose the write shape

- Use a single mutation for an independent insert, update, delete, upsert, or restore.
- Use `db.batch(...)` when writes should become optimistically visible together after the callback
  commits, then settle as one direct batch.
- Use `db.transaction(...)` when writes must remain staged until authority acceptance and then become
  globally visible together.
- Read staged rows through the batch or transaction handle when the operation depends on its own
  pending writes.
- Await the returned batch result, not individual writes inside an open batch or transaction.
- Handle `PersistedWriteRejectedError` from `.wait(...)` or configure `db.onMutationError(...)`.
  Rejection reverts optimistic data; without handling, data previously shown by the client can
  silently disappear.

## Cross into schema work deliberately

If the feature changes a table, column, relation, merge strategy, or permission, load the
`jazz-schema-evolution` skill before editing. Do not create ad hoc migration formats or place
permissions inside `schema.ts`. When `jazz-schema-evolution` already owns the task, do not load this
skill solely because the edit uses the schema DSL; load both only when application reads, writes, or
framework code also changes.

Use the dedicated skill when the feature's main difficulty is authentication (`jazz-auth`),
server-side context and authority (`jazz-backend`), or chunked file/blob storage (`jazz-files`). Load
`jazz-core` alongside one of them only when ordinary application queries, writes, or framework UI
also change.

Use `jazz-expo` for native setup, storage, networking, and lifecycle; `jazz-rust` for the Rust crate;
and `jazz-sync` for merge strategies, offline conflict behavior, reconnect correctness, or
convergence testing. Load `jazz-core` alongside them only when the task also changes ordinary
TypeScript application code.

## Verify the change

1. Run the narrowest existing typecheck, build, and integration tests that exercise the changed
   surface.
2. Exercise loading and empty states for reactive reads.
3. Exercise offline behavior when a feature depends on immediate local writes.
4. Await the required tier in tests that assert cross-client or server-visible results.
5. Preserve existing tests unless the requested behavior intentionally changes.

## Avoid these failure modes

- Do not author schemas, filters, permissions, or queries as untyped JSON-like objects outside the
  public builders.
- Do not treat a locally applied write as globally confirmed.
- Do not add a remote-fetch cache around a live Jazz query unless the application has a separate,
  explicit need for one.
- Do not copy rows into component state merely to make a Jazz subscription reactive.
- Do not use `undefined` to clear a nullable column; it means “leave unchanged.” Use `null`.
- Do not guess relation names. Derive them from the ref column and confirm them through the typed
  `app` surface.
- Do not update tests to accept implementation drift without confirming that behavior was meant to
  change.
