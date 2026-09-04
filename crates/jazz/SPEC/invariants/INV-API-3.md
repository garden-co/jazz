# INV-API-3

- Status: now
- Coverage: ✓

## Invariant

`Db::read` and `Db::one` MUST be synchronous local reads and MUST NOT wait for upstream sync; `Db::all` MUST use `ReadOpts` to choose the effective durability tier.

## Enforced by (tests)

`jazz::db::tests::db_facade_opens_writes_and_reads_todos_end_to_end`; `jazz::node::tests::queries::db_facade_current_rows_match_seeded_create_delete_sequence`

## Implementation

`jazz/src/db.rs::Db::read`; `jazz/src/db.rs::Db::one`; `jazz/src/db.rs::Db::all`; `jazz/src/db.rs::effective_read_tier`
