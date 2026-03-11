# Magic Columns for Permission Introspection

Permission introspection columns expose whether the current session could act on a row using the same policy machinery as real mutations.

## Initial Columns

- `$canEdit`
- `$canDelete`

These are virtual/magic columns, not user-declared schema columns.

## Semantics

### Session Context

The columns are evaluated in the context of the current session, exactly like session-aware mutation checks.

If no session is present, the value should be `NULL` rather than `true` or `false`.

### `$canEdit`

`$canEdit` answers:

> Does this session pass the row's `UPDATE USING` policy for the current row?

This is intentionally narrower than "could some update succeed?" and does **not** attempt to predict `WITH CHECK` because no candidate new row exists at read time.

If the table has no `UPDATE USING` policy, `$canEdit` is `true`.

### `$canDelete`

`$canDelete` answers:

> Does this session pass the row's effective delete policy?

This should use the same policy resolution as real deletes:

- explicit `DELETE USING`, if present
- otherwise the existing fallback to `UPDATE USING`

If the table has no delete/update-using policy, `$canDelete` is `true`.

## Query Surface

### Explicit Opt-In

These columns are excluded from `SELECT *`.

Examples:

- `SELECT * FROM todos` returns only normal columns
- `SELECT *, $canEdit, $canDelete FROM todos` opts into the magic columns
- `SELECT t.$canEdit FROM todos AS t` is allowed

This follows the "hidden/system column" model used by several SQL systems and keeps default reads cheap.

### Joined Queries

Magic columns must work in joined queries.

They should bind to a specific row source, for example:

- `SELECT u.name, p.title, u.$canEdit, p.$canDelete ...`

This requires projection metadata to preserve source scope/table identity rather than flattening everything down to unqualified column strings too early.

### Filters

Magic columns should be usable in `WHERE`, but only as non-indexed filters.

Examples:

- `WHERE $canDelete = true`
- `WHERE p.$canEdit = true`

If a query references a magic column in filtering, the planner should compute the relevant magic columns before the filter stage. If a magic column is only projected, it can be computed later.

## Reuse of Mutation Policy Logic

The implementation should reuse the same permission evaluation path as real mutations rather than creating a second policy engine.

At a high level:

1. Resolve the relevant operation and policy:
   - `$canEdit` -> `Operation::Update`, using only `UPDATE USING`
   - `$canDelete` -> `Operation::Delete`, using the effective delete policy
2. Evaluate the simple predicate parts directly from row bytes
3. Reuse the existing complex-clause handling for:
   - `INHERITS`
   - `EXISTS`
   - `EXISTS REL`
   - `INHERITS REFERENCING`
4. Reuse the same dependency tracking so values re-evaluate when referenced policy rows change

This keeps introspection aligned with real authorization behavior.

## Output Shape

Once explicitly requested, magic columns should appear in graph output descriptors like ordinary nullable boolean columns.

That means downstream nodes can treat them as normal columns for:

- projection
- filtering
- output encoding

But they should remain hidden from schema/catalog metadata and wildcard expansion.

## Required Groundwork

Two planner refactors are the intended first implementation steps:

1. Make projection metadata precise
   - preserve source scope/table identity
   - preserve aliases
   - support projecting columns from joined inputs without collapsing to `Vec<String>`

2. Make condition/filter metadata precise
   - preserve source scope/table identity in conditions
   - resolve filters against joined tuple descriptors correctly
   - allow future non-indexed filters over computed/magic columns

These refactors should land before the actual `$canEdit` / `$canDelete` computation node.
