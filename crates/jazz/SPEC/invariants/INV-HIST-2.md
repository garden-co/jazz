# INV-HIST-2

- Status: now
- Coverage: ✓

## Invariant

Among content heads not dominated by known parents, the current content version MUST be the head with the greatest made-at/`TxId` sort key.

## Enforced by (tests)

`jazz::oracle::tests::concurrent_heads_use_hlc_lww_with_node_tiebreak`; `jazz::node::tests::counter_merge::core_local_currency_uses_argmax_not_sender_arrival_order`

## Implementation

`jazz/src/node/codec.rs::current_version_index`; `jazz/src/node/codec.rs::version_wins_over_open_winner`
