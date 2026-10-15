---
"jazz-tools": patch
---

Relation queries and relation-backed policies now share one relation IR and one lowering, so a hand-written relation means the same thing in both:

- Policies accept unbounded (`Fixpoint`) gathers.
- Queries accept the scalar recursive step and seed hops that policies already accepted.
- In queries, `= null` and `!= null` now test nullness, as the `jazz-tools` builders already emit them.
- Both reject ordering comparisons against null.
- A joined gather step may only project columns of its output row, named by scope.
- A policy row correlation (`= RowId(Outer)`) under `Or` or `Not` is now rejected instead of being treated as always true.

Relations built with the `jazz-tools` builders are unaffected.
