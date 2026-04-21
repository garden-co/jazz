# Better-Auth adapter: generalize unique-field enforcement

## What

The adapter currently guards unique columns in `create` / `update` / `updateMany` with a separate `SELECT` before the write (excluding the current row id on updates). That works end-to-end for the common case (`email` on users) but has two shortcomings:

- Race: the pre-check and the write are not atomic. Concurrent callers setting the same unique value can both pass the check and both insert/update.
- Coverage: Better Auth may expose other unique fields via plugins (e.g. `username`, OAuth account ids). The current path handles whatever `field.unique` reports, but we have no tests beyond `email`.

## Priority

medium

## Notes

- Proper fix is atomic unique enforcement at the query/runtime level (unique indexes or a single-statement upsert-with-conflict path), not in the adapter.
- As a stopgap, consider serializing writes per unique column via an in-process mutex keyed on `(table, column, value)` to at least close the per-process race.
- Add adapter tests for update paths that mutate a unique column to a colliding value (expect error) and to its own current value (expect pass).
