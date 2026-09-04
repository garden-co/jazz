# INV-API-2

- Status: now
- Coverage: ✓

## Invariant

`Db` is a client only and has no role: `Db::open` MUST construct a non-history-complete client `NodeState`. A history-complete, fate-deciding authority is a core `Node` opened directly at the node level (ch. 9), never a `Db`.

## Enforced by (tests)

`jazz::db::tests::db_facade_opens_writes_and_reads_todos_end_to_end`

## Implementation

`jazz/src/db.rs::Db::open`
