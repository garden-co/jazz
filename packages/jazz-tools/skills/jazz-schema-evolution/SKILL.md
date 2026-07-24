---
name: jazz-schema-evolution
description: Change existing Jazz schemas, row-level permissions, and migration lenses safely. Use instead of jazz-core when adding, removing, renaming, or transforming tables and columns; changing refs, defaults, or merge strategies; authoring schema.ts or permissions.ts for an established app; validating schema policy coverage; creating or reviewing migrations; or preparing a Jazz deployment across client versions.
---

# Jazz Schema Evolution

Treat schema evolution as a compatibility workflow, not a text edit. Keep structural schema,
permissions, and migrations separate and use the installed `jazz-tools` CLI and TypeScript DSL.
This skill includes the schema DSL guidance needed for evolution work; do not also load `jazz-core`
unless the task changes application queries, writes, or framework integration.

## Inspect before editing

1. Read the installed `jazz-tools` version and use its local binary.
2. Locate `schema.ts`, optional `permissions.ts`, `migrations/`, saved snapshots, and the bundler/dev
   plugin configuration.
3. Determine whether the app is still disposable local development or whether rows already exist in
   a shared or deployed schema.
4. Read existing migration files and permission conventions before introducing a new pattern.
5. Read the relevant bundled references:
   - [permissions.md](references/permissions.md) for policy semantics and patterns.
   - [migrations.md](references/migrations.md) for snapshots, lenses, validation, and publishing.

## Change the structural schema

- Define tables and columns only through `schema as s` in `schema.ts`.
- Preserve ref naming rules: scalar refs end in `Id` or `_id`; arrays of refs end in `Ids` or
  `_ids`.
- Use `.optional()` for nullable columns and `.default(...)` when new rows need a creation default.
- Account for old clients when adding required columns, removing fields, changing types, or
  reinterpreting data.
- Use the generated migration DSL for row translation. Do not hand-author JSON schema documents or
  rewrite stored rows directly.

## Change permissions

- Keep policies in `permissions.ts` next to `schema.ts` (`src/lib/` for the established SvelteKit
  layout).
- Grant read, insert, update, and delete independently. A loaded policy bundle denies operations
  without an explicit grant.
- Check ownership on both sides of an update when the update could transfer ownership or alter a
  protected relation.
- Prefer relation-aware `allowedTo.*`, existence checks, or a share table for relational access.
- Use creator metadata only when authorship truly represents ownership.
- Treat permission-only changes as deployable policy changes, not structural migrations.

## Run the compatibility workflow

1. Establish the initial schema snapshot if the project does not have one.
2. Edit `schema.ts` and `permissions.ts` through their public DSLs.
3. Run the local `jazz-tools validate` command.
4. Create a named migration from the latest snapshot to the new schema when existing data or old
   clients must remain reachable.
5. Review every generated operation, default, rename inference, and backwards value. Resolve draft
   or ambiguous lenses before publishing.
6. Add black-box tests for transformed rows and cross-version reads and writes.
7. Publish only when the user requested it or the surrounding workflow explicitly authorizes the
   external change. Otherwise provide the reviewed command and required inputs.

## Decide whether a migration is required

- Permission-only change: no structural migration; validate and deploy the permission bundle.
- First local iteration with disposable data: a migration can be deferred, but historical rows on
  disconnected schema branches will not be readable from the new schema until a path exists.
- Shared, deployed, or offline-capable clients: create and publish a migration for structural
  changes that must interoperate across versions.
- Merge-strategy-only change: treat it as a structural schema change even when no row transform is
  required.

## Verify the result

1. Confirm `validate` passes and investigate every policy warning.
2. Inspect the local schema hash and the generated from/to hashes.
3. Test old-schema writes read through the new schema when backwards compatibility matters.
4. Test new-schema writes read through the old schema when old clients remain supported.
5. Test permissions as multiple sessions, including denied reads and writes.
6. Run the existing application typecheck and integration suite.

## Avoid these failure modes

- Do not place permissions in `schema.ts`.
- Do not treat exported compiled JSON as the authoring format.
- Do not silence missing-operation policy warnings by granting `.always()` unless public access is
  intended.
- Do not rely on client-side filters for authorization.
- Do not publish a draft migration or accept an inferred rename without reviewing it.
- Do not deploy against a shared server as an incidental validation step.
- Do not edit existing tests merely because a new schema implementation disagrees with them.
