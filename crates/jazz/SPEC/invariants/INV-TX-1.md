# INV-TX-1

- Status: now
- Coverage: ✓

## Invariant

A transaction MUST NOT expose `open` writes to ordinary reads or subscriptions before commit.

## Enforced by (tests)

`jazz::node::tests::exclusive_transactions::exclusive_tx_open_state_is_invisible_outside_transaction`

## Implementation

`jazz/src/node/open_tx.rs::NodeState::tx_write`; `jazz/src/node/open_tx.rs::NodeState::overlay_pending_writes`
