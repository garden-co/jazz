# Schema Directory Layout & Checkpoint-Based Migrations — TODO (MVP)

Two related problems with the current build behaviour:

1. **Directory clutter.** Every build emits `schema_v*.sql` and `migration_v*_v*_*.{sql,ts}` files directly into the schema root alongside user-authored files (`current.ts`, `permissions.ts`).
2. **Over-eager versioning.** A versioned schema + migration pair is created on every build where the hash changes, turning every iterative edit during development into a permanent record.

---

## Part 1 — Subfolder Reorganisation

Move all auto-generated artefacts except `current.ts`, `current.sql`, `app.ts`, `permissions.ts`, and `permissions.test.ts` into subdirectories.

### Target layout

```
schema/
├── current.ts          ← user-authored schema DSL
├── current.sql         ← auto-generated from current.ts, but can be custom written outside of TS contexts, so should not be hidden away
├── app.ts              ← auto-generated TypeScript client, needs to be imported, nice to be able to refer to types etc. easily
├── permissions.ts      ← user-authored permissions (optional)
├── permissions.test.ts ← auto-generated stub (users may customise)
├── versions/
│   ├── schema_v1_101ec784deec.sql
│   └── schema_v2_ba2b07e7513e.sql
└── migrations/
    ├── migration_v1_v2_101ec784deec_ba2b07e7513e.ts
    ├── migration_v1_v2_fwd_101ec784deec_ba2b07e7513e.sql
    └── migration_v1_v2_bwd_101ec784deec_ba2b07e7513e.sql
```

### Files to change

- **`crates/jazz-tools/src/schema_manager/files.rs`** — `SchemaDirectory` reads versioned schemas from `versions/` and migrations from `migrations/`; all write paths updated accordingly.
- **`crates/jazz-tools/src/commands/build.rs`** — update paths when reading/writing versioned schemas and migration files.
- **`packages/jazz-tools/src/cli.ts`** — scan `migrations/` for `.ts` stubs; update `migrationSqlFilename()` to write into `migrations/`.
- **`specs/status-quo/schema_files.md`** — update directory diagram and file references.

---

## Part 2 — Checkpoint-Based Migrations

### Current behaviour

`jazz-tools build` creates a new `versions/schema_v{N}_{hash}.sql` and corresponding migration files on every build where the schema hash has changed. This means each iterative schema edit during development produces a new permanent version + migration pair, which is rarely what the developer wants.

### Target behaviour

**`jazz-tools build` (default)**

- Regenerates `current.sql` and `app.ts`.
- Does **not** create any new file in `versions/` or `migrations/`.
- Compares the hash of `current.sql` against the hash of the highest-version file in `versions/` (the last checkpoint). If they differ, prints:

  ```
  Schema has changed since last checkpoint.
  Run `jazz-tools build --checkpoint` to save this version and generate
  migrations so old clients can migrate to the current schema.
  ```

  If `versions/` is empty (no checkpoint exists yet) the message is suppressed.

**`jazz-tools build --checkpoint`**

- Runs normal codegen.
- If the current schema hash matches the most recent checkpoint: prints `Schema unchanged — no checkpoint created.` and exits.
- Otherwise:
  - Determines baseline: the highest-version file in `versions/`, or the empty schema if `versions/` is empty.
  - Writes `versions/schema_v{N}_{hash}.sql` (N = last checkpoint version + 1, or 1 for the first checkpoint).
  - Generates `migrations/migration_v{N-1}_v{N}_*.{ts,sql}` for the baseline → current transition.

The baseline is implicit — always the highest version number in `versions/`. No pointer file needed. The existing `parse_versioned_schema_filename()` logic already extracts version numbers.

### First checkpoint

`versions/` is empty → baseline is the empty schema. Creates `versions/schema_v1_{hash}.sql` and a migration in `migrations/` that creates all tables from scratch (the initial schema).

### `main.rs` / CLI wiring

Add `--checkpoint` flag to the `Build` subcommand in `main.rs`. Pass it through to `commands::build::run()`.

In `packages/jazz-tools/src/cli.ts`, pass `--checkpoint` to the Rust binary when the TypeScript CLI is itself invoked with `--checkpoint`. Update `parseArgs()` and `BuildOptions` accordingly.

---

## Rejected alternatives

- **Single `checkpoint.sql`:** Simpler on disk but breaks `SchemaManager`'s lens DAG construction, which relies on discrete versioned files.
- **Git-integrated checkpointing:** Adds a git dependency and trades explicit intent for implicit magic.
- **`jazz clean` pruning command:** Doesn't fix the over-eager generation problem, just cleans up after it.

---

## Verification

1. Default build → `versions/` and `migrations/` unchanged; divergence warning if schema differs from last checkpoint.
2. No checkpoint yet → no warning on default build.
3. `--checkpoint` with changed schema → new `versions/schema_v{N}_*.sql` and `migrations/migration_v*` files.
4. `--checkpoint` with unchanged schema → "Schema unchanged" message, no new files.
5. First-ever checkpoint (empty `versions/`) → `schema_v1_*.sql` + initial migration.
6. Existing Rust tests for `SchemaDirectory` pass with updated paths.

> **Files:** `crates/jazz-tools/src/schema_manager/files.rs`, `crates/jazz-tools/src/commands/build.rs`, `crates/jazz-tools/src/main.rs`, `packages/jazz-tools/src/cli.ts`, `specs/status-quo/schema_files.md`
