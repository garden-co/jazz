# Commit Author as Principal + Created-By Permissions — TODO (MVP)

Make `Commit.author` mean "the Jazz principal that performed this commit", then use that provenance as the foundation for simple creator-based permissions.

This MVP is intentionally narrow:

- it fixes commit authorship semantics first,
- it adds a minimal permission-facing provenance surface,
- it exposes initial provenance magic columns for reads,
- it does **not** try to solve dynamic/group ownership via commit history.

For anything more dynamic than "the creator may access this row", developers should model ownership explicitly with row columns, tables, and reference-based policies.

## Why This Exists

Today `Commit.author` is not actually an actor identity.

- `Commit.author` is currently an `ObjectId`
- row write paths populate it with the row object's own id (`self-authored`)
- the auth stack already resolves a stable Jazz principal in `session.user_id`
- external auth may map or derive a principal that is intentionally **not** the raw provider `sub`

That mismatch blocks a clean "created by current user" permission story:

- the field named `author` does not identify a user
- using raw external auth ids would couple commit history to provider-specific subjects
- later auth upgrades/linking would break provenance-based access

## Goals

- Make commit authorship represent the acting Jazz principal.
- Use the resolved Jazz principal, not raw external provider ids.
- Preserve creator provenance across later edits.
- Add a small permission surface for creator-based policies.
- Expose `$createdBy`, `$createdAt`, `$updatedBy`, and `$updatedAt` as magic columns.
- Support explicit backend attribution overrides without conflating them with requester-scoped permissions.
- Keep the MVP compatible with the existing recommendation for complex ownership:
  explicit schema columns, join tables, and ReBAC policies.

## Non-goals (MVP)

- No attempt to infer group/org/team ownership from commit history.
- No "owner transfer" semantics driven by commit provenance.
- No schema/catalog exposure or wildcard inclusion for provenance metadata.
- No broad migration layer for mixed old/new commit-author encodings.
- No change to the existing recommendation that dynamic access should be modeled explicitly in data.

## Core Decisions

### 1. `Commit.author` becomes a principal id

`Commit.author` should identify the acting Jazz principal for that commit.

- For authenticated/local/demo/external user writes: use `session.user_id`
- For backend impersonation: use the impersonated `session.user_id`
- For system-generated/internal commits: use a reserved system principal id

MVP reserved system principal:

- `jazz:system`

The low-level row-history write path may still accept an explicit author parameter, but its meaning changes from "row id" to "principal id".

### 2. Use the resolved Jazz principal, not raw external ids

Commit authorship must use the same stable principal that permissions already use:

- linked external identities resolve to the same principal as their local predecessor
- provider-specific `iss` / `sub` are only inputs into principal resolution
- a raw external `sub` must not be written directly into commit provenance unless it is also the resolved Jazz principal

### 3. Creator provenance is preserved explicitly on row commits

Changing `Commit.author` is necessary, but it is not sufficient for stable created-by semantics once a row has many later commits or truncated history.

For row-object commits, MVP stores creator provenance in commit metadata and carries it forward:

- `created_by`
- `created_at`

Semantics:

- On row insert/root commit:
  - `author = acting_principal`
  - `created_by = acting_principal`
  - `created_at = commit.timestamp`
- On later row commits:
  - `author = acting_principal`
  - copy forward the existing `created_by` / `created_at`

This keeps creator provenance:

- stable across edits by other users
- available in O(1) from the current visible commit
- robust to history truncation that removes the original root commit

`updated_by` and `updated_at` do not need separate persisted metadata in MVP:

- `updated_by` = current visible commit's `author`
- `updated_at` = current visible commit's `timestamp`

### 4. Provenance is a required invariant

In the greenfield MVP, row commits are required to carry valid creator provenance.

If a row commit expected to carry creator provenance does not have valid `created_by` metadata, that is an invariant violation. Provenance-based permission checks must still fail closed rather than exposing data.

That means:

- `createdBy(...)` conditions evaluate to `false`
- `updatedBy(...)` conditions evaluate to `false`
- rows are not accidentally exposed because provenance is missing or malformed

For reads, the provenance magic columns are part of the non-null query contract. Missing provenance should be treated as a bug/corruption path, not as a nullable result shape.

### 5. Provenance is also surfaced as magic columns

The first query/read surface for edit provenance should be magic columns, following the same opt-in model as the existing permission introspection columns.

MVP provenance magic columns:

- `$createdBy`
- `$createdAt`
- `$updatedBy`
- `$updatedAt`

Semantics on the visible row commit:

- `$createdBy` = current commit metadata `created_by`
- `$createdAt` = current commit metadata `created_at`
- `$updatedBy` = current commit `author`
- `$updatedAt` = current commit `timestamp`

Types:

- `$createdBy`: `TEXT`
- `$createdAt`: `TIMESTAMP`
- `$updatedBy`: `TEXT`
- `$updatedAt`: `TIMESTAMP`

These are non-null in the MVP contract because:

- greenfield rollout can require provenance from day one
- system/sessionless writes still stamp concrete provenance via `jazz:system` and commit timestamps
- `created_by` / `created_at` are copied forward on every row commit

## Permission Surface (MVP)

This MVP adds a small provenance-aware policy surface and aligns it with the new provenance magic columns.

The intended mental model is:

- app code reads `$createdBy`, `$createdAt`, `$updatedBy`, `$updatedAt`
- policy code uses the same provenance magic-column names for authorization decisions

### TypeScript DSL

Do not introduce a separate `meta` helper.

Instead, permission conditions should accept provenance magic-column names anywhere ordinary column keys already work in `where(...)`, `whereOld(...)`, and `whereNew(...)`.

```ts
definePermissions(app, ({ policy, session, anyOf }) => {
  policy.todos.allowRead.where({ $createdBy: session.user_id });
  policy.todos.allowDelete.where({ $createdBy: session.user_id });
  policy.todos.allowUpdate.whereOld(
    anyOf([{ $createdBy: session.user_id }, session.where({ "claims.role": "admin" })]),
  );
});
```

In other words, the policy-side surface should reuse the same names as reads:

- `$createdBy`
- `$createdAt`
- `$updatedBy`
- `$updatedAt`

This keeps policy authoring aligned with the existing permissions DSL shape:

- object-literal where clauses
- `anyOf(...)` / `allOf(...)`
- `whereOld(...)` / `whereNew(...)`

Accepted right-hand-side values in MVP:

- string literal
- `session.user_id` / other session ref

Not supported in MVP:

- row-ref comparisons against provenance magic columns
- timestamp-based provenance policies beyond ordinary comparisons using `$createdAt` / `$updatedAt`
- a separate SQL-only provenance helper surface in MVP

## Magic Columns (MVP)

### Explicit opt-in

Like `$canRead` / `$canEdit` / `$canDelete`, provenance magic columns are omitted from `select("*")`.

Examples:

- `select("*", "$createdBy", "$updatedAt")`
- `select("title", "$createdAt")`

### Joined queries

They should work in joined queries through the same existing scoped magic-column path:

- `select("users.name", "posts.title", "posts.$createdBy")`

### Filters and ordering

They should be usable in non-indexed filters and sort clauses through the same planner path already used for existing magic columns.

Examples:

- `where("$createdBy", "eq", sessionUserId)` in runtime query payloads
- `orderBy("$updatedAt", "desc")`

They remain non-indexed/system-computed values.

### Session behavior

Unlike permission introspection magic columns, provenance magic columns are **not** session-scoped.

If the row is visible to the query, provenance magic columns evaluate from the visible row commit even when no session is present.

## Execution Semantics

### What row batch member is checked?

Policy evaluation should use the same visible row batch member the query/mutation path already uses.

- `created_by` comes from the visible row commit metadata
- `updated_by` comes from the visible row commit author

This means provenance checks align with the current row state rather than arbitrary historical commits.

### Update and delete behavior

Creator-based update/delete permissions work like normal policy checks:

- `allowUpdate.whereOld({ "$createdBy": session.user_id })`
- `allowDelete.where({ "$createdBy": session.user_id })`

There is no special "creator override" path outside the policy system.

## Write Path Semantics

### Row writes

All row-level mutation paths should stamp principals consistently:

- insert
- update
- soft delete
- hard delete tombstone commit

If the write has a session, use `session.user_id`.

If the write does not have a session, use `jazz:system`.

This keeps the rule simple:

- if an app wants per-user provenance, it must either write through a session-scoped path or opt into explicit backend attribution override
- sessionless/internal writes are explicitly system-authored

### Explicit backend attribution override

Some backend/admin flows need to create or update rows with user attribution even though the operation should not run under that user's permission scope.

Examples:

- system-generated tasks created "for" a user
- admin/backfill jobs that should preserve end-user provenance
- backend workflows that should stamp `createdBy` for downstream policy/UI behavior without impersonating the user

For that case, MVP should add an explicit attribution override surface:

- `context.withAttribution(principalId)`
- `context.withAttributionForSession(session)`
- `context.withAttributionForRequest(request)`

Semantics:

- these helpers affect commit provenance only
- they set commit `author` and row `created_by` / `updated_by` provenance to the attributed principal
- they do **not** change permission evaluation context to that user
- they do **not** grant the attributed user's read/write powers
- they are backend/admin helpers, not normal app-client helpers

Mental model:

- `context.forRequest(req)` = act as the requester
- `context.withAttributionForRequest(req)` = act as backend/system, but attribute writes to the requester

The low-level primitive is `withAttribution(principalId)`. The `ForSession` / `ForRequest` variants are convenience helpers that resolve `principalId` from `session.user_id`.

Without an explicit attribution override, backend/system/sessionless writes remain `jazz:system`.

### Catalogue / index / derived-data writes

Internal writes that are not user actions should also use `jazz:system`.

These commits do not need row provenance metadata unless they are row-object commits.

## Compatibility and Migration

This change is intentionally breaking at the commit layer.

Why:

- `Commit.author` changes type and serialized representation
- `Commit.author` participates in `CommitId` hashing
- new commits created under the new semantics will hash differently

MVP stance:

- no mixed old/new commit-author compatibility layer
- local stores may need recreation
- peers participating in sync should be on the same version

If we later need migration, it can be designed separately. This MVP optimizes for a clear semantic reset.

## Implementation Shape

### Rust core

- Replace `Commit.author: ObjectId` with a string-backed principal id type.
- Update object-manager/storage/sync serialization paths accordingly.
- Preserve `created_by` / `created_at` metadata on row commits.
- Add reserved system principal constant.
- Add write-time attribution override plumbing separate from session-scoped policy evaluation.

### Shared provenance payload

Current graph materialization carries row bytes and `commit_id`, but not commit provenance.

For this MVP, extend the loaded-row pipeline with commit provenance from the visible row commit:

- commit author
- commit timestamp
- commit metadata required for `created_by` / `created_at`

The cleanest MVP route is to widen `LoadedRow` with a compact provenance payload and thread it through the existing row-loader closures. That keeps one source of truth for:

- materialization,
- provenance magic columns,
- provenance-aware policy evaluation.

### Magic column implementation strategy

The current magic-column pipeline is already a good fit:

- magic columns are planner-recognized,
- opt-in,
- non-indexed,
- available in projections/filters/order-by,
- and computed in a dedicated `MagicColumnsNode`.

MVP implementation strategy:

1. Extend the magic-column registries in Rust and TypeScript with the four provenance columns.
2. Teach `MagicColumnsNode` to assign per-kind output types instead of hard-coding `BOOLEAN`.
3. Keep policy dependency-table tracking only for permission introspection kinds; provenance kinds have no cross-table dependency list.
4. Evaluate provenance kinds directly from loaded visible-commit provenance, without requiring a session.
5. Reuse the same provenance extraction helper in policy evaluation so reads and policies stay aligned.

This should not require a new planner node; it should fit inside the existing magic-column pipeline.

### Permissions engine

- Reuse the existing column-comparison policy IR rather than introducing provenance-specific policy nodes.
- Teach policy validation/evaluation to recognize provenance magic-column names in policy expressions.
- Evaluate those magic-column comparisons from the current visible row commit.
- Keep fail-closed behavior when provenance is unavailable or malformed.

### TypeScript permissions DSL

- Reuse the existing where-object API rather than adding new provenance helpers.
- Extend permissions typing so `$createdBy`, `$createdAt`, `$updatedBy`, `$updatedAt` are accepted in policy where-objects and row callback contexts.
- Compile those comparisons into the existing Rust policy representation as normal column comparisons on known magic-column names.

### Backend context surface

- Add `withAttribution(principalId)`, `withAttributionForSession(session)`, and `withAttributionForRequest(request)` on the backend context surface.
- Keep these separate from `forSession(...)` / `forRequest(...)`, which still mean requester-scoped authorization.
- Document that attribution helpers only affect provenance stamping, not policy identity.

### Query/runtime TypeScript surface

- Extend the shared magic-column registry and TS typing for `$createdBy`, `$createdAt`, `$updatedBy`, `$updatedAt`.
- Keep them opt-in and excluded from wildcard selection.
- Ensure row transformation maps timestamp values to the same runtime shape already used for normal timestamp columns.

## Relationship to Explicit Ownership Modeling

This feature is for the simple case:

- "the creator of this row may read/update/delete it"

It is **not** the recommended model for:

- transferable ownership
- org/team/project membership access
- role-based access
- delegated access
- shared rows

For those cases, the right pattern remains:

- explicit row columns like `owner_id`
- membership/share tables
- `allowedTo.*(...)`, `exists(...)`, and normal ReBAC policies

## Testing Strategy

Add focused coverage for:

- external/local/demo auth all resolving to principal-based authorship
- linked external identity preserving principal continuity
- insert/update/delete stamping the right author
- creator provenance surviving later edits by another user
- creator provenance surviving hard-delete truncation boundaries
- backend/system writes defaulting to `jazz:system` without attribution override
- `withAttribution(...)`, `withAttributionForSession(...)`, and `withAttributionForRequest(...)` stamping user provenance without switching policy evaluation to that user
- selecting/projecting/filtering/ordering on `$createdBy`, `$createdAt`, `$updatedBy`, `$updatedAt`
- joined-query scoping for provenance magic columns
- provenance magic columns working without a session
- direct policy usage like `{ "$createdBy": session.user_id }` and `{ "$updatedBy": session.user_id }` in TS DSL
- fail-closed behavior when provenance metadata is missing or malformed

## Follow-ups (Later)

- possible future first-class provenance fields beyond the magic-column surface, if we later want schema/catalog-visible edit metadata
- richer provenance inspection in reads/devtools
- backwards-compatible migration strategy for old commit-author encodings if needed
- possible reuse of a shared `PrincipalId` type across more of the auth/session stack
