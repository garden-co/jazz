# Schema migrations

Jazz keeps schema versions addressable by hash and translates rows between them through migration
lenses. Existing rows remain stored under their original schema branch.

Use the project's installed CLI. The commands below use `pnpm exec jazz-tools`; use the equivalent
local-package command for another package manager.

Run commands from the app root containing `schema.ts`, or pass `--schema-dir <path>` and, when
needed, `--migrations-dir <path>` explicitly in a monorepo. Use `jazz-tools help` to inspect the
installed command surface. Do not assume nested commands accept `--help`: versions that do not parse
that flag may execute the command instead.

## Contents

- [Normal workflow](#normal-workflow)
- [Publishing](#publishing)
- [Historical and explicit hashes](#historical-and-explicit-hashes)
- [Review checklist](#review-checklist)

## Normal workflow

Establish the first snapshot before the first structural change:

```bash
pnpm exec jazz-tools migrations create
```

Then edit `schema.ts`, validate it, and generate a migration with a descriptive filename label:

```bash
pnpm exec jazz-tools validate
pnpm exec jazz-tools migrations create --name add-projects
```

`--name` is normalized and included in the generated filename; it is not stored as migration
metadata. Review the generated file under `migrations/`. A migration carries `fromHash`, `toHash`,
generated schema witnesses, and declarative operations:

```ts
import { schema as s } from "jazz-tools";

export default s.defineMigration({
  fromHash: "aaaaaaaaaaaa",
  toHash: "bbbbbbbbbbbb",
  from: {
    todos: s.table({ title: s.string() }),
  },
  to: {
    todos: s.table({ title: s.string(), description: s.string() }),
  },
  migrate: {
    todos: {
      description: s.add.string({ default: "" }),
    },
  },
});
```

The CLI adds the minimal `from` and `to` witness objects needed to type-check the migration. Keep
them with the generated file; do not replace them with imports of the mutable current schema.

Use the generated operation appropriate to the installed version. Review forward defaults for new
fields and backwards defaults for fields removed from the new schema. Resolve ambiguous add/remove
pairs instead of assuming they represent a rename.

## Publishing

Publish schema, required migration, and current permissions together:

```bash
pnpm exec jazz-tools deploy <appId>
```

Or push a reviewed migration edge explicitly:

```bash
pnpm exec jazz-tools migrations push <appId> <fromHash> <toHash>
```

Permission-only changes do not need a migration but still need deployment.

Treat the first permissions publish as a lock-down event: before it, the row-policy layer is
permissive; after it, missing grants deny operations. Also review historical data against the new
permissions. Historical rows are evaluated using the currently applied bundle, not the bundle that
was active when each row was created.

Publishing changes external state. Do not run these commands without authorization and the correct
server URL, app ID, and administrative credential.

## Historical and explicit hashes

Create a path involving a stored historical schema with explicit hashes:

```bash
pnpm exec jazz-tools migrations create <appId> --fromHash <fromHash>
pnpm exec jazz-tools migrations create <appId> --fromHash <fromHash> --toHash <toHash>
```

The target defaults to the current local schema. Missing snapshots can be resolved from the server
when the app ID and credentials are available.

Inspect the current local hash without publishing:

```bash
pnpm exec jazz-tools schema hash
```

## Review checklist

- Confirm source and target hashes match the intended snapshots.
- Confirm every added required field has a valid value for old rows.
- Confirm removed fields have a backwards value when old clients still read them.
- Confirm type changes preserve the intended meaning in both directions.
- Confirm inferred renames are real renames.
- Confirm the migration is not marked draft.
- Confirm permissions compile against the target schema.
- Before the first permissions publish, verify every actor and operation that must retain access.
- Check whether current permissions expose or hide historical rows contrary to their original intent.
- Test old and new clients against one server before publishing.
