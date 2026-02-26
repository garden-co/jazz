# `uuid[]` Foreign Key Semantics — TODO (MVP)

Implements gap #2 from [`built_in_file_storage.md`](../todo/a_mvp/built_in_file_storage.md): ordered array FK semantics for `files.parts uuid[] references file_parts`.

## Goal

Upgrade `references` on `UUID[]` from metadata-only to enforced FK semantics with deterministic ordering behavior, so file part lists are valid, queryable, and safe.

## Scope

- Validate schema rules for `references` on scalar and array UUID columns.
- Enforce referential integrity for `UUID[] REFERENCES ...` on writes.
- Define query/include/hop semantics for array-backed references.
- Ensure updates maintain correct membership indexing.

Out of scope:

- Cascade delete and refcount cleanup (covered by `file_storage_cascade_integration.md`).
- Mutable file diff/chunk rewriting strategy.

## Semantics

### Schema Rules

- `references` is valid only for:
  - `UUID REFERENCES <table>`
  - `UUID[] REFERENCES <table>`
- Reject `references` on non-UUID element types (for example `TEXT[] REFERENCES ...`).

### Write-Time FK Validation

- For `UUID[] REFERENCES target`:
  - Every element must resolve to an existing row in `target`.
  - Empty arrays are valid.
  - Duplicates are valid and preserved.
  - Ordering is preserved exactly as provided.
- Validation runs on insert and update.

### Query Semantics

- Forward relation over array FK uses membership against target IDs.
- Forward materialization preserves source array order and duplicate IDs.
- Reverse relation is membership-based (`contains`) from target ID to source rows.
- Index maintenance updates membership entries when arrays change.

## Invariants

- Array element order is stable through encode/decode and query materialization.
- Duplicate IDs are never deduplicated implicitly.
- A row cannot commit with unresolved referenced IDs in an array FK column.
- Reverse membership results are consistent after add/remove/reorder updates.

## Testing Strategy

### Schema Validation Tests

- Accept: `parts UUID[] REFERENCES file_parts NOT NULL`.
- Reject: non-UUID referenced arrays.

### Integrity Tests

- Insert/update succeeds when all IDs exist.
- Insert/update fails when any ID is missing.
- Empty array accepted.
- Duplicate IDs accepted.

### Query/Relation Tests

- Forward include over array FK returns rows in the same array order.
- Reverse relation returns parent rows when target ID is present in array.
- Updating array values updates relation visibility and membership index state.

### Regression Tests

- Existing scalar `UUID REFERENCES` behavior remains unchanged.
- Self-referential and nullable scalar FK behavior remains unchanged.

## Dependency on Built-in File Storage

`built_in_file_storage.md` depends on this for `files.parts` correctness. Content-addressed file chunks are only usable if array FK membership is enforced and ordering is deterministic.
