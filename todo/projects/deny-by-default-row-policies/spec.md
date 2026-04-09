# Deny-by-Default Row Policies

Today, missing row policies often behave like implicit grants in session-scoped row access. That makes it easy to ship a table with no explicit `read`, `insert`, `update`, or `delete` rule and still get access at runtime. Developers are affected in exactly the cases where they think they are being cautious: a missing `permissions.ts`, a partially filled table policy, or a table that defines `update` but forgets `delete`. The result is a footgun where "forgot to grant" can silently become "allowed by default."

## Solution

### Chosen approach

Make session-scoped row permission evaluation fail closed. Use `jazz-tools validate` as a DX tool that points out missing explicit grants, but keep the runtime semantics independent of whether validation was run.

This keeps the runtime aligned with the intended security posture while preserving a lightweight developer workflow:

- missing row policies do not grant access at runtime
- `jazz-tools validate` points out missing explicit grants early
- malformed permissions and unknown tables remain hard validation failures
- missing explicit `delete` still warns even though runtime delete may fall back to `update.using`

### Fat marker sketch

```text
Developer authoring
schema.ts + permissions.ts
        |
        v
Session-scoped runtime
query or write with session
        |
        v
resolve relevant row policy for operation
        |
        +-- explicit policy exists -> evaluate it
        |
        +-- explicit policy missing
              read   -> filter row out
              insert -> reject write
              update -> reject write
              delete -> reject write, unless update.using fallback exists

Inherited policy evaluation
child row checks parent access
        |
        +-- parent policy exists -> evaluate recursively
        +-- parent policy missing -> deny

DX feedback loop
developer runs jazz-tools validate
        |
        v
warn once per missing table x operation
but do not define or override runtime semantics
```

### Breadboards

#### 1. Query flow

When `alice` queries a table through a session-scoped path:

1. The query compiler always attaches a row-policy filter for row tables when a session is present.
2. If the table has an explicit `read` policy, the filter evaluates that policy.
3. If the table has no explicit `read` policy, the filter behaves like `false` and the row is hidden.
4. The same fail-closed behavior applies to:
   - the base table
   - joined tables
   - server-side subscription filtering
   - sync-scope derivation from query results

The intended developer mental model becomes: "rows are invisible until `read` is explicitly granted."

#### 2. Write flow

When `alice` writes a row through a session-scoped path:

- `insert` is allowed only if an explicit `insert.with_check` policy exists and passes.
- `update` is allowed only if at least one explicit update clause exists (`using` and/or `with_check`) and every present clause passes.
- `delete` is allowed only if an explicit delete check can be evaluated:
  - prefer `delete.using`
  - otherwise fall back to `update.using`
  - if neither exists, reject the delete

This applies consistently in:

- local session-backed writes
- branch-specific write helpers
- server-side pending permission checks against the current permission schema

Non-row objects remain outside this deny-by-default change.

#### 3. Inherited and recursive policy flow

When a policy uses `INHERITS`, `INHERITS REFERENCING`, or related recursive evaluation:

1. Jazz resolves the parent row or referencing row as it does today.
2. Jazz looks up the parent policy for the requested operation.
3. If the parent policy exists, Jazz evaluates it recursively.
4. If the parent policy is absent, Jazz denies access instead of treating the absence as a pass.

Helpers that cannot build the contextual policy graph needed to evaluate a complex clause must also fail closed rather than silently succeeding.

#### 4. Validation as DX

When a developer runs `jazz-tools validate`:

1. Jazz loads `schema.ts` and, if present, `permissions.ts`.
2. Jazz validates structural compatibility exactly as today.
3. Jazz runs an explicit-policy diagnostics pass across every table in the structural schema.
4. Jazz emits one warning per missing explicit operation:
   - `read` requires `select.using`
   - `insert` requires `insert.with_check`
   - `update` requires `update.using` or `update.with_check`
   - `delete` requires `delete.using`
5. Jazz still prints the normal success summary and exits successfully.

If `permissions.ts` is missing, Jazz treats it as "no explicit row policies declared" for diagnostics and warns for every table and every operation.

If `permissions.ts` is malformed or references unknown tables, Jazz still fails validation immediately instead of downgrading those cases to warnings.

The important boundary is:

- validation helps authors notice missing grants
- runtime deny-by-default does not depend on validation having run
- validation does not become the enforcement mechanism

### Core implementation shape

#### Rust runtime layer

Thread the same deny-by-default rule through every shipped session-aware row-permission entry point instead of relying on "missing policy means skip the check."

Representative read-path shape:

```rust
let policy = table_schema
    .policies
    .select
    .using
    .clone()
    .unwrap_or(PolicyExpr::False);
```

Representative write-path rule:

- no explicit policy for the requested operation => reject
- explicit policy exists but fails => reject
- explicit policy exists and passes => allow
- delete may reuse `update.using` at runtime, but validation still warns if `delete.using` is absent

#### TypeScript validation layer

Add a reusable explicit-policy diagnostics pass in `packages/jazz-tools` that compares structural table names against compiled permissions.

The pass does not change the public permissions DSL or runtime behavior. It only answers:

- which operations are explicitly declared for this table?
- which warnings should `validate` print?

Representative shape:

```ts
for (const table of schema.tables) {
  for (const operation of ["read", "insert", "update", "delete"]) {
    if (!hasExplicitPolicy(table, compiledPermissions, operation)) {
      console.warn(
        `Warning: table "${table.name}" has no explicit ${operation} policy in permissions.ts; runtime defaults to deny.`,
      );
    }
  }
}
```

### Examples and docs

Update shipped examples and comments so the docs teach the same mental model as the runtime:

- comments that currently say empty row policies "allow all" must be rewritten
- example apps with intentionally denied operations should declare that explicitly with `never()`
- example apps with allowed operations should declare them explicitly rather than relying on omission

## Rabbit Holes

- The current behavior is split across multiple permission entry points. If even one of local writes, server permission checks, query filtering, or recursive inherited evaluation stays fail-open, the core deny-by-default story becomes inconsistent and easy to misunderstand.
- `delete` is intentionally asymmetric: runtime may reuse `update.using`, while validation still warns that `delete` was not explicitly granted. The spec must keep that asymmetry deliberate rather than letting it look like an accident.
- Complex policy helpers can accidentally reintroduce fail-open behavior when they cannot build a graph or cannot resolve a parent row. Those "could not evaluate" cases must be treated as denial in session-scoped row paths.
- Missing `permissions.ts` must produce DX warnings without weakening the existing hard-failure behavior for malformed files or unknown tables.
- Example apps and status-quo docs can lag behind the runtime semantics and keep teaching the wrong default if they are not updated as part of the same change.

## No-gos

- Do not add new permission DSL primitives, syntax, or public API surface for this change.
- Do not make `jazz-tools validate` the enforcement mechanism; it is a DX surface only.
- Do not make `jazz-tools validate` fail on missing explicit row policies; those diagnostics stay warnings in this spec.
- Do not change backend/admin/no-session/internal paths that intentionally bypass session-scoped row-policy evaluation.
- Do not remove the runtime `delete -> update.using` fallback in this spec.
- Do not expand this work into broader policy-engine redesign, policy optimization, or migration tooling.

## Testing Strategy

Use integration-first tests with realistic actor names and end-to-end flows.

- Runtime behavior is the primary test target:
  - session-scoped reads against tables without `read` policy return no rows
  - session-scoped `insert`, `update`, and `delete` deny when the relevant explicit policy is absent
  - delete still succeeds through `update.using` fallback when that fallback is present
  - inherited and recursive policy evaluation denies when the parent operation policy is missing
  - `alice` can still read rows when an explicit `read` policy allows it
  - `alice` cannot insert into the same table when `insert` is omitted
  - server-side session-scoped permission checks match local behavior
- TypeScript CLI tests:
  - `validate` with no `permissions.ts` warns once per table per operation
  - partial permissions warn only for missing operations
  - `always()` and `never()` count as explicit policies
  - missing explicit `delete` still warns when runtime delete can fall back to `update.using`
- Example/docs smoke:
  - shipped example permissions validate without missing-policy warnings unless the example is intentionally demonstrating omission

## Confidence

8/10. The behavior and scope are clear, and the asymmetry around delete fallback is now explicit. The main implementation risk is semantic drift between the several permission entry points, which is why the spec leans hard on integration coverage instead of helper-only tests.
