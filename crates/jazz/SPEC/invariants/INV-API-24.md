# INV-API-24

- Status: now
- Coverage: ✓

## Invariant

The query builder exposed through `Db::table` MUST support OR/AND/NOT predicates, `contains`, `in_list`, `is_null`, includes with `JoinMode::Holes`, required includes, select, limit, and offset.

## Enforced by (tests)

`jazz::db::tests::db_query_builder_expresses_s1_shaped_filters_and_include_modes`

## Implementation

`jazz/src/query.rs::Query`; `jazz/src/query.rs::Include`; `jazz/src/query.rs::JoinMode`; `jazz/src/db.rs::Db::table`
