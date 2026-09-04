# INV-TX-15

- Status: now
- Coverage: ✓

## Invariant

Reads inside an exclusive transaction MUST observe that transaction's own pending writes.

## Enforced by (tests)

`jazz::node::tests::exclusive_transactions::exclusive_tx_reads_own_pending_writes`

## Implementation

`jazz/src/node/open_tx.rs::NodeState::overlay_pending_writes`; `jazz/src/node/open_tx.rs::NodeState::tx_read`; `jazz/src/node/open_tx.rs::NodeState::tx_current_rows`
