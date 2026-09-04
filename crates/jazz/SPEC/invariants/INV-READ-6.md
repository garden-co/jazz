# INV-READ-6

- Status: now
- Coverage: untested

## Invariant

`tx_current_rows` and `tx_query` MUST record predicate reads as `PredicateRead` values carrying `table`, `shape_id`, `shape`, `binding_id`, and `binding_values`; whole-table transaction reads are degenerate query shapes.

## Enforced by (tests)

NONE-FOUND

## Implementation

`jazz/src/node/open_tx.rs::NodeState::tx_current_rows`; `jazz/src/node/query_eval.rs::NodeState::tx_query`
