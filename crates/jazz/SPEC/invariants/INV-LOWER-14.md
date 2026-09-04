# INV-LOWER-14

- Status: planned
- Coverage: [#1777](https://github.com/garden-co/jazz/issues/1777)

## Invariant

Sync query updates SHOULD consume maintained terminal facts for result membership, path/correlation coverage, payload/replacement/version witnesses, policy witnesses, and read-frontier settlement; query-row recompute paths are migration/oracle debt, not an alternate production engine.

## Enforced by (tests)

`jazz::node::query_engine::tests::app_rows_are_separate_from_hidden_terminal_facts`; `jazz::node::query_engine::tests::read_frontier_facts_are_outputs_not_delivery_profiles`

## Implementation

`jazz/src/node/query_engine/mod.rs::ProgramFactKey`; `jazz/src/node/query_engine/mod.rs::ProgramFactSchema`; `jazz/src/peer.rs::PeerState::query_update_inner`; `jazz/src/node/views.rs::NodeState::view_update_for_query_binding_with_peer_payload_inventory_and_plan`
