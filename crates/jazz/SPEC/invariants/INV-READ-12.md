# INV-READ-12

- Status: now
- Coverage: ✓

## Invariant

Per-layer global-current tables MUST equal accepted argmax winners over stored versions and remain consistent after reopen.

## Enforced by (tests)

`jazz::node::tests::recovery::persisted_currency_tables_match_history_rows_after_reopen`

## Implementation

`jazz/src/node/global_state.rs::NodeState::global_current_updates`; `jazz/src/node/currency.rs::NodeState::query_global_layer_winner`
