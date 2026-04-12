# Deny-by-Default Row Policies, Split by Runtime

Today, missing row policies often behave like implicit grants in session-scoped row access. That makes it easy to ship a table with no explicit `read`, `insert`, `update`, or `delete` rule and still get access at runtime. Developers are affected in exactly the cases where they think they are being cautious: a missing `permissions.ts`, a partially filled table policy, or a table that defines `update` but forgets `delete`.

That footgun is real, but our runtime topology is split. Backend and sync runtimes can load compiled policies at runtime or receive them via `jazz-tools permissions push`. Frontend clients usually do not import `permissions.ts` at all, and local-only or offline apps may never have a policy bundle on-device. A single global fail-closed rule would fix the server-side security story by breaking end-device behavior.

## Solution

### Chosen approach

Split row-policy semantics by whether the current runtime actually has compiled policies loaded.

This keeps local-first clients usable while still making server-enforced paths fail closed:

- frontend clients with no loaded policies stay permissive locally for session-scoped reads and writes
- backend and sync runtimes, and any other runtime with a loaded policy bundle, evaluate row policies fail closed
- `jazz-tools validate` points out missing explicit grants early
- malformed permissions and unknown tables remain hard validation failures
- missing explicit `delete` still warns even though enforcing runtimes may fall back to `update.using`

The intended mental model becomes:

- no policies loaded in this runtime -> local permissive mode
- policies loaded in this runtime -> explicit grants only

### Fat marker sketch

```text
Developer authoring
schema.ts + permissions.ts
        |
        +-- jazz-tools validate
        |       |
        |       v
        |   warn once per missing table x operation
        |   but do not define runtime semantics
        |
        +-- frontend client runtime
        |       |
        |       +-- no policies loaded
        |       |       read/write locally without row-policy enforcement
        |       |       keep offline/local-only apps usable
        |       |
        |       +-- policies loaded
        |               use enforcing-runtime behavior below
        |
        +-- backend / sync / enforcing runtime
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

Inherited policy evaluation on enforcing runtimes
child row checks parent access
        |
        +-- parent policy exists -> evaluate recursively
        +-- parent policy missing -> deny
```

### Breadboards

#### 1. Runtime split

When Jazz handles a session-scoped row query or write, it first decides whether the executing runtime has compiled row policies loaded.

1. If the runtime has no loaded policies, it runs in local permissive mode.
2. If the runtime has loaded policies, it runs in enforcing mode.
3. `jazz-tools validate` always reasons about the authored policy surface, independent of which runtime will later execute the app.

This first branch is the core change. "Deny by default" is no longer a statement about every client process. It is a statement about runtimes that are actually in possession of policy state.

#### 2. Query flow

When `alice` queries a table through a session-scoped path:

On a frontend client with no loaded policies:

- Jazz does not synthesize a deny-all row filter just because `permissions.ts` is absent from the bundle.
- Local queries continue to read from local state so offline and local-only apps keep functioning.
- If the query result comes from a sync server, that remote result is still shaped by the server's enforcing-runtime policy evaluation.

On an enforcing runtime:

1. The query compiler attaches a row-policy filter for row tables when a session is present.
2. If the table has an explicit `read` policy, the filter evaluates that policy.
3. If the table has no explicit `read` policy, the filter behaves like `false` and the row is hidden.
4. The same fail-closed behavior applies to:
   - the base table
   - joined tables
   - server-side subscription filtering
   - sync-scope derivation from query results

The intended developer mental model becomes: "server-enforced rows are invisible until `read` is explicitly granted."

#### 3. Write flow

When `alice` writes a row through a session-scoped path:

On a frontend client with no loaded policies:

- `insert`, `update`, and `delete` are allowed locally.
- Offline queues and local-only apps remain usable even before any permissions have been pushed or fetched.
- A later sync attempt to an enforcing runtime may still be rejected there; local permissive mode does not imply eventual server acceptance.

On an enforcing runtime:

- `insert` is allowed only if an explicit `insert.with_check` policy exists and passes.
- `update` is allowed only if at least one explicit update clause exists (`using` and/or `with_check`) and every present clause passes.
- `delete` is allowed only if an explicit delete check can be evaluated:
  - prefer `delete.using`
  - otherwise fall back to `update.using`
  - if neither exists, reject the delete

This enforcing behavior applies consistently in:

- backend session-backed writes
- sync-server pending permission checks against the current policy head
- branch-specific write helpers in runtimes that have policies loaded

Non-row objects remain outside this change.

#### 4. Inherited and recursive policy flow

When a policy uses `INHERITS`, `INHERITS REFERENCING`, or related recursive evaluation:

On a frontend client with no loaded policies:

- Jazz does not try to reconstruct or partially evaluate the policy graph locally.
- The client stays permissive locally and leaves enforcement to runtimes that actually have the policy bundle.

On an enforcing runtime:

1. Jazz resolves the parent row or referencing row as it does today.
2. Jazz looks up the parent policy for the requested operation.
3. If the parent policy exists, Jazz evaluates it recursively.
4. If the parent policy is absent, Jazz denies access instead of treating the absence as a pass.

Helpers that cannot build the contextual policy graph needed to evaluate a complex clause must also fail closed on enforcing runtimes rather than silently succeeding.

#### 5. Validation as DX

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
- local client permissive mode exists only because policies are not loaded there
- enforcing runtimes still deny by default when explicit grants are absent
- validation does not become the enforcement mechanism

### Core implementation shape

#### Runtime capability split

Thread one explicit branch through the permission entry points:

- no compiled policies loaded in this runtime => skip row-policy enforcement locally
- compiled policies loaded in this runtime => enforce with deny-by-default fallbacks

This branch should be made deliberately, not inferred ad hoc in each helper. Otherwise we will drift into a mix of permissive and fail-closed behavior that depends on which call path was used.

#### Frontend runtime layer

Frontend clients should not become unusable just because they do not bundle `permissions.ts`.

Representative rule:

- no local policy bundle => do not reject session-scoped row reads or writes solely because a policy is absent
- if a frontend runtime later has a compiled policy bundle available, it should use the same enforcing behavior as server runtimes

The frontend side is intentionally permissive only in the "no policies loaded" state.

#### Backend and sync runtime layer

Backend and sync runtimes are enforcing runtimes. They either load policies at runtime or receive them through CLI-pushed state, and once they are responsible for enforcement they must fail closed.

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

If an enforcing runtime has no current policy state for an app, that still must not become an implicit grant.

#### TypeScript validation layer

Add a reusable explicit-policy diagnostics pass in `packages/jazz-tools` that compares structural table names against compiled permissions.

The pass does not change the public permissions DSL or local client permissive mode. It only answers:

- which operations are explicitly declared for this table?
- which warnings should `validate` print?

Representative shape:

```ts
for (const table of schema.tables) {
  for (const operation of ["read", "insert", "update", "delete"]) {
    if (!hasExplicitPolicy(table, compiledPermissions, operation)) {
      console.warn(
        `Warning: table "${table.name}" has no explicit ${operation} policy in permissions.ts; enforcing runtimes default to deny.`,
      );
    }
  }
}
```

### Examples and docs

Update shipped examples and comments so the docs teach the same split mental model as the runtime:

- comments that currently say empty row policies "allow all" must be rewritten
- docs must explain that frontend clients without loaded policies stay permissive locally
- docs must explain that backend and sync runtimes deny by default once they are enforcing
- example apps with intentionally denied operations should declare that explicitly with `never()`
- synced example apps with allowed operations should declare them explicitly rather than relying on omission
- local-only examples may omit permissions entirely without implying that synced/server runtimes will allow the same writes

## Rabbit Holes

- The biggest semantic risk is drift between policy-less frontend permissive mode and backend or sync enforcing mode. If even one server-side query, write, or recursive check stays fail-open, the security story breaks. If even one client-side local path starts denying because policies are absent, offline UX breaks.
- `delete` is intentionally asymmetric: runtime may reuse `update.using`, while validation still warns that `delete` was not explicitly granted. The spec must keep that asymmetry deliberate rather than letting it look accidental.
- Complex policy helpers can accidentally reintroduce fail-open behavior when they cannot build a graph or cannot resolve a parent row. Those "could not evaluate" cases must be treated as denial on enforcing runtimes.
- Missing `permissions.ts` must produce DX warnings without weakening hard-failure behavior for malformed files or unknown tables.
- Example apps and status-quo docs can lag behind the runtime semantics and keep teaching the wrong default if they are not updated as part of the same change.

## No-gos

- Do not require frontend apps to import or bundle `permissions.ts` just to keep local writes working.
- Do not let backend, sync, or other enforcing runtimes fall open when policy state is missing.
- Do not make `jazz-tools validate` the enforcement mechanism; it is a DX surface only.
- Do not make `jazz-tools validate` fail on missing explicit row policies; those diagnostics stay warnings in this spec.
- Do not change backend, admin, no-session, or internal paths that intentionally bypass session-scoped row-policy evaluation.
- Do not remove the runtime `delete -> update.using` fallback in this spec.
- Do not expand this work into broader policy-engine redesign, policy synchronization protocol redesign, or migration tooling.

## Testing Strategy

Use integration-first tests with realistic actor names and end-to-end flows.

- Frontend runtime behavior with no loaded policies:
  - session-scoped reads continue to see local rows when no policy bundle is present
  - session-scoped `insert`, `update`, and `delete` continue to work locally with no policy bundle
  - offline writes can be queued locally before any permissions have been pushed
  - local-only apps remain functional without a sync server or policy download
- Enforcing runtime behavior is the primary security test target:
  - session-scoped reads against tables without `read` policy return no rows
  - session-scoped `insert`, `update`, and `delete` deny when the relevant explicit policy is absent
  - delete still succeeds through `update.using` fallback when that fallback is present
  - inherited and recursive policy evaluation denies when the parent operation policy is missing
  - `alice` can still read rows when an explicit `read` policy allows it
  - `alice` cannot insert into the same table when `insert` is omitted
  - sync-server permission checks match backend helper behavior
  - server-side rejection paths are covered for writes that were accepted locally by policy-less clients
- TypeScript CLI tests:
  - `validate` with no `permissions.ts` warns once per table per operation
  - partial permissions warn only for missing operations
  - `always()` and `never()` count as explicit policies
  - missing explicit `delete` still warns when runtime delete can fall back to `update.using`
- Example/docs smoke:
  - synced example permissions validate without missing-policy warnings unless the example is intentionally demonstrating omission
  - local-only examples still operate without a permissions bundle

## Confidence

8/10. The policy-source split now matches how the product is actually deployed: end-device clients can stay usable without bundling permissions, while runtimes that do enforce policies are explicitly fail closed. The main risk is semantic drift between those two modes, so the spec leans on end-to-end coverage and documentation that makes the split obvious.
