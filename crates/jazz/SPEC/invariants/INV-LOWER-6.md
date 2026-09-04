# INV-LOWER-6

- Status: now
- Coverage: ✓

## Invariant

Local/non-global current-row lowering MUST use groove `arg_max_by` over `(tx_time, tx_node_id)` per `row_uuid` for both content and deletion-register tables.

## Enforced by (tests)

`jazz::node::tests::queries::groove_current_rows_match_oracle_for_seeded_m1_commits`

## Implementation

`jazz/src/node/codec.rs::visible_current_graph`
