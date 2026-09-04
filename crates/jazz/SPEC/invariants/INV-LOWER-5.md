# INV-LOWER-5

- Status: now
- Coverage: ✓

## Invariant

Visible current rows MUST be computed as current content winners anti-joined with current deletion winners where `_deletion == deleted`.

## Enforced by (tests)

`jazz::node::tests::queries::groove_current_rows_match_oracle_for_seeded_m1_commits`

## Implementation

`jazz/src/node/codec.rs::visible_current_graph`; `jazz/src/node/mod.rs::NodeState::global_current_rows_from_storage`
