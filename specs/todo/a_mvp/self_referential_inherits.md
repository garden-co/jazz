# Self-Referential INHERITS — TODO (MVP)

Tracked as part of the generic recursive query + permission-check work:

- `specs/todo/a_mvp/recursive_queries_and_permission_checks.md`

Self-referential INHERITS then uses that recursion machinery directly (bounded, unrolled recursion with default max depth + per-query override), instead of a separate self-INHERITS special case.
