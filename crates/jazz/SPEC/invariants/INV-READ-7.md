# INV-READ-7

- Status: now
- Coverage: ✓

## Invariant

Local current-row reads MUST use argmax `TxId` currency per `(row_uuid, VersionLayer)` over held non-rejected versions, independent of sender arrival order.

## Enforced by (tests)

`jazz::node::tests::counter_merge::core_local_currency_uses_argmax_not_sender_arrival_order`

## Implementation

`jazz/src/node/currency.rs::NodeState::query_layer_winner_from_pk`
