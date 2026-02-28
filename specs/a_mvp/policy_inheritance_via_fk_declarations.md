# Policy Inheritance via permissions.ts Referencing Rules — TODO (MVP)

Implements gap #3 from [`built_in_file_storage.md`](../todo/a_mvp/built_in_file_storage.md): declarative inbound inheritance via permissions DSL, with OR semantics across inbound references.

## Goal

Allow tables like `files` and `file_parts` to inherit access from referencing rows (for example `todos.image -> files`, `files.parts -> file_parts`) without hand-writing equivalent policy chains everywhere.

## Scope

- Add policy-level declaration for inbound FK edges (`allowedTo.*Referencing(...)`).
- Evaluate inherited access by scanning source-table rows that reference the target row.
- Combine inherited grants with OR semantics.
- Support scalar and `UUID[]` inheritance edges.
- Add cycle safety and dependency invalidation semantics.

Out of scope:

- Delete cascade behavior.
- New auth providers/claims models.

## Semantics

### Declaration Model

- TS permissions DSL surface:
  - `allowedTo.readReferencing(policy.<sourceTable>, "<fkColumn>")`
  - operation variants for `insert/update/delete` as needed
- SQL policy surface:
  - `INHERITS <OPERATION> REFERENCING <source_table> VIA <fk_column>`

### Access Evaluation

For `(target_table, target_row_id, operation)`:

1. Evaluate target-table policy expression normally.
2. For each `INHERITS ... REFERENCING` clause, load source rows from `source_table` where `<fk_column>` points to `target_row_id`:
   - Scalar FK: `fk == target_row_id`
   - Array FK: `target_row_id ∈ fk_array`
3. Evaluate source-row policy for the same operation.
4. Clause result is true if any source row passes.
5. Final decision follows the composed policy expression (typically `local_allow OR inherited_allow` via `anyOf`).

### Multi-Hop Behavior

- Inheritance composes transitively via recursive policy evaluation.
- Example path for built-in file storage:
  - `files` policy contains `allowedTo.readReferencing(policy.todos, "image")`
  - `file_parts` policy contains `allowedTo.readReferencing(policy.files, "parts")`

### Safety

- Detect cycles on `(table, row_id, operation)` during recursive evaluation.
- Cycle behavior is fail-closed for the recursive branch.
- Maximum recursion depth is bounded and configurable via existing policy-depth controls.

### Reactivity/Invalidation

- Policy graph tracking must mark dependent target rows dirty when source rows or FK values change.
- Both scalar and array inheritance edges participate in dirty propagation.

## Invariants

- Inherited permissions never remove existing local access; they only add access when composed with OR.
- If no referencing-row grant exists, behavior equals local-policy behavior.
- Array-FK inheritance is membership-based and deterministic.
- Cycles do not cause infinite recursion or accidental allow-all.

## Testing Strategy

### DSL/Compiler Tests

- Compile `allowedTo.*Referencing(policy.<table>, "<fk>")` to policy IR.
- Validate source FK exists and points to the current target table.

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
