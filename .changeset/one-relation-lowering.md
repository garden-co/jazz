---
"jazz-tools": patch
---

Relation queries and relation-backed policies now share one relation IR and one lowering, so a relation means the same thing in both.

Some permission policies that used to compile while silently ignoring part of their condition, and so granted more access than written, are now rejected when the schema is loaded. These shapes can be built with the `jazz-tools` permission builders:

- Chaining `.where()` or `.join()` after `gather(...)` (or after a hop from it) so that the access rows are tied to the protected row a second time, for example `.where({ resource: todo.id, grant_role: todo.ownerId })`, or so that a joined relation itself filters on the protected row.
- Filtering the rows a gather reaches while keying them on a column of the protected row, for example `.where({ id: todo.teamId, archived: false })`. That filter used to be checked against the protected row instead of the team. Put such filters in the gather's `start` or `step` instead.

Policies built only from the supported single-correlation shape, such as `reachable.hopTo(...).where({ resource: row.id, grant_role: "viewer" })`, are unaffected.

Hand-written relation IR changes:

- Policies accept unbounded (`Fixpoint`) gathers.
- Queries accept the scalar recursive step and seed hops that policies already accepted.
- In queries, `= null` and `!= null` now test nullness, as the `jazz-tools` builders already emit them.
- Both reject ordering comparisons against null.
- A joined gather step may only project columns of its output row, named by scope.
- A gather seed hop must name the hop target's `id` by scope.
- Policies now reject, instead of silently dropping:
  - a row correlation (`= RowId(Outer)`) under `Or`, `Not` or an enum payload;
  - gather seed or edge filters that correlate with the protected row, and frontier row reads outside a gather step;
  - access rows correlated with the protected row more than once, correlated below their join, or joined to further tables;
  - a relation join on more than one equality, including inside a legacy `Exists` condition;
  - a correlation more than one join below the correlated root.
