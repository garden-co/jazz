# Commit Author as Principal + Created-By Permissions — TODO (MVP)

Make `Commit.author` mean "the Jazz principal that performed this commit", then use that provenance as the foundation for simple creator-based permissions.

This MVP is intentionally narrow:

- it fixes commit authorship semantics first,
- it adds a minimal permission-facing provenance surface,
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
- Keep the MVP compatible with the existing recommendation for complex ownership:
  explicit schema columns, join tables, and ReBAC policies.

## Non-goals (MVP)

- No attempt to infer group/org/team ownership from commit history.
- No "owner transfer" semantics driven by commit provenance.
- No general query/read surface for edit metadata columns yet.
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

The low-level object layer may still accept an explicit author parameter, but its meaning changes from "object id" to "principal id".

### 2. Use the resolved Jazz principal, not raw external ids

Commit authorship must use the same stable principal that permissions already use:

- linked external identities resolve to the same principal as their local predecessor
- provider-specific `iss` / `sub` are only inputs into principal resolution
- a raw external `sub` must not be written directly into commit provenance unless it is also the resolved Jazz principal

### 3. Creator provenance is preserved explicitly on row commits

Changing `Commit.author` is necessary, but it is not sufficient for stable `_created_by` semantics once a row has many later commits or truncated history.

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

### 4. Fail closed on missing or inconsistent provenance

If a row commit expected to carry creator provenance does not have valid `created_by` metadata, provenance-based permission checks must fail closed.

That means:

- `createdBy(...)` conditions evaluate to `false`
- rows are not accidentally exposed because provenance is missing

## Permission Surface (MVP)

This MVP adds a small provenance-aware policy surface without pulling the whole edit-metadata query feature into scope.

### TypeScript DSL

Extend `definePermissions(...)` context with a `meta` helper:

```ts
definePermissions(app, ({ policy, meta, session, anyOf }) => {
  policy.todos.allowRead.where(meta.createdBy(session.user_id));
  policy.todos.allowDelete.where(meta.createdBy(session.user_id));
  policy.todos.allowUpdate.whereOld(
    anyOf([meta.createdBy(session.user_id), session.where({ "claims.role": "admin" })]),
  );
});
```

MVP helpers:

- `meta.createdBy(value)`
- `meta.updatedBy(value)`

Accepted `value` types in MVP:

- string literal
- `session.user_id` / other session ref

Not supported in MVP:

- comparing provenance to row refs
- using provenance as a general query filter/projection surface

### SQL Policy Syntax

Add matching SQL helper functions for policies:

```sql
CREATE POLICY todos_select_policy ON todos FOR SELECT
  USING (CREATED_BY() = @session.user_id);

CREATE POLICY todos_delete_policy ON todos FOR DELETE
  USING (CREATED_BY() = @session.user_id);
```

MVP functions:

- `CREATED_BY()`
- `UPDATED_BY()`

These are policy/runtime concepts, not user-declared schema columns.

## Execution Semantics

### What row version is checked?

Policy evaluation should use the same visible row version the query/mutation path already uses.

- `created_by` comes from the visible row commit metadata
- `updated_by` comes from the visible row commit author

This means provenance checks align with the current row state rather than arbitrary historical commits.

### Update and delete behavior

Creator-based update/delete permissions work like normal policy checks:

- `allowUpdate.whereOld(meta.createdBy(session.user_id))`
- `allowDelete.where(meta.createdBy(session.user_id))`

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

- if an app wants per-user provenance, it must write through a session-scoped path
- sessionless/internal writes are explicitly system-authored

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

### Permissions engine

- Extend policy IR with provenance-aware conditions.
- Evaluate provenance-aware conditions from the current visible row commit.
- Keep fail-closed behavior when provenance is unavailable or malformed.

### TypeScript permissions DSL

- Add `meta.createdBy(...)` and `meta.updatedBy(...)`.
- Compile them into the Rust policy representation.
- Keep the helper small and intentionally scoped to simple comparisons.

### SQL parser/generator

- Parse `CREATED_BY()` / `UPDATED_BY()` in policy expressions.
- Round-trip them through schema SQL generation.

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
- `meta.createdBy(session.user_id)` and `meta.updatedBy(session.user_id)` in TS DSL
- SQL parse/generate for `CREATED_BY()` / `UPDATED_BY()`
- fail-closed behavior when provenance metadata is missing or malformed

## Follow-ups (Later)

- Query-time edit metadata columns (`_created_by`, `_updated_by`, `_created_at`, `_updated_at`)
- richer provenance inspection in reads/devtools
- backwards-compatible migration strategy for old commit-author encodings if needed
- possible reuse of a shared `PrincipalId` type across more of the auth/session stack
