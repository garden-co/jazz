# INV-API-1

- Status: now
- Coverage: ✓

## Invariant

`Db` MUST be the high-level runtime-typed client facade — a thin wrapper over a participant `Node` (which owns the `NodeState` engine, connections, and serving) — and it MUST validate user `Query` values into `PreparedQuery` before executing reads/subscriptions.

## Enforced by (tests)

`jazz::db::tests::db_facade_opens_writes_and_reads_todos_end_to_end`; `jazz::db::tests::db_query_builder_expresses_s1_shaped_filters_and_include_modes`

## Implementation

`jazz/src/db.rs::Db::prepare_query`; `jazz/src/db.rs::Db::read`; `jazz/src/db.rs::Db::all`; `jazz/src/db.rs::Db::subscribe`
