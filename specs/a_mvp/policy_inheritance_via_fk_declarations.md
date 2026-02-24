# Policy Inheritance via FK Declarations — TODO (MVP)

Implements gap #3 from [`built_in_file_storage.md`](../todo/a_mvp/built_in_file_storage.md): declarative inheritance edges on foreign keys, with OR semantics across inbound references.

## Goal

Allow tables like `files` and `file_parts` to inherit access from referencing rows (for example `todos.image -> files`, `files.parts -> file_parts`) without hand-writing equivalent policy chains everywhere.

## Scope

- Add schema-level declaration for FK edges that opt into policy inheritance.
- Evaluate inherited access by scanning inbound declared edges.
- Combine inherited grants with OR semantics.
- Support scalar and `UUID[]` inheritance edges.
- Add cycle safety and dependency invalidation semantics.

Out of scope:

- Delete cascade behavior.
- New auth providers/claims models.

## Semantics

### Declaration Model

- Add FK edge metadata flag: `inherit_policy`.
- SQL surface (MVP): `REFERENCES <table> INHERIT POLICY`.
- TS DSL surface (MVP): `col.ref("table").inheritPolicy()` and array equivalent.

### Access Evaluation

For `(target_table, target_row_id, operation)`:

1. Evaluate existing local table policy result (`local_allow`).
2. Collect all FK columns across schema where:
   - `references == target_table`
   - `inherit_policy == true`
3. For each such edge, load referencing rows that point to `target_row_id`:
   - Scalar FK: `fk == target_row_id`
   - Array FK: `target_row_id ∈ fk_array`
4. Evaluate referencing-row policy for the same operation.
5. `inherited_allow` is true if any referencing row passes.
6. Final decision: `local_allow OR inherited_allow`.

### Multi-Hop Behavior

- Inheritance composes transitively via recursive policy evaluation.
- Example path for built-in file storage:
  - `todos.image -> files` (inherit)
  - `files.parts -> file_parts` (inherit)

### Safety

- Detect cycles on `(table, row_id, operation)` during recursive evaluation.
- Cycle behavior is fail-closed for the recursive branch.
- Maximum recursion depth is bounded and configurable via existing policy-depth controls.

### Reactivity/Invalidation

- Policy graph tracking must mark dependent target rows dirty when referencing rows or FK values change.
- Both scalar and array inheritance edges participate in dirty propagation.

## Invariants

- Inherited permissions never remove existing local access; they only add access via OR.
- If no inbound inheritance edge grants access, behavior equals current local-policy behavior.
- Array-FK inheritance is membership-based and deterministic.
- Cycles do not cause infinite recursion or accidental allow-all.

## Testing Strategy

### Declaration/Schema Tests

- Parse and generate FK declarations with `INHERIT POLICY`.
- Preserve flag through schema encoding/decoding and WASM conversion.

### Policy Evaluation Tests

- Single inbound edge: grant and deny cases.
- Multiple inbound tables: OR semantics (`any` grant wins).
- No inbound grants: deny unless local policy allows.
- Two-hop inheritance chain for `todos -> files -> file_parts`.

### Array Edge Tests

- `UUID[]` edge grants access when target ID is present.
- Reordering and duplicate entries do not break grant semantics.

### Safety/Invalidation Tests

- Cycle detection denies recursive loop path.
- Updates to referencing rows trigger re-evaluation of affected target rows.

## Dependency on Built-in File Storage

This is the authorization model needed by [`built_in_file_storage.md`](../todo/a_mvp/built_in_file_storage.md) so `files` and `file_parts` access follows the parent application rows that reference them, without custom per-app policy duplication.
