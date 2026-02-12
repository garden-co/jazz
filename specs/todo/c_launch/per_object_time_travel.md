# Per-Object Time Travel — TODO (Launch)

Navigate an individual object's commit history.

## Overview

Every object has a commit graph. Per-object time travel lets users:

- View the full edit history of a single row
- Read the state of a row at any previous commit
- See who made each change and when (pairs with `../b_mvp/magic_columns_edit_metadata.md`)

This is straightforward because the commit graph can be navigated directly — no need to reconstruct state by replaying history.

## Use Cases

- Audit logs: "who changed this record and when?"
- Undo: revert a row to a previous state
- Diff: show what changed between two versions of a row

## Open Questions

- API shape: method on the row object? Query modifier? Separate endpoint?
- How deep is history kept? (Truncation policy?)
- How to expose in the React bindings and database viewer?

## Future: Full Point-in-Time Queries

Querying _across_ rows at a historical timestamp (e.g., "all todos as of last Tuesday") requires historical index support and is deferred. See `../d_later/point_in_time_queries.md`.
