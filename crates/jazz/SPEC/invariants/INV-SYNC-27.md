# INV-SYNC-27

- Status: now
- Coverage: ✓

## Invariant

A fast known-state declaration MUST only be made for contiguously applied, unevicted served streams; any local eviction touching stored row-version bodies invalidates persisted fast declarations before another declaration can be made.

## Enforced by (tests)

`jazz::node::tests::harness::fast_known_state_fact_survives_reopen_and_eviction_clears_it`

## Implementation

`schema.rs::KNOWN_STATE_FACTS_STORE`; `node/views.rs::NodeState::apply_view_update`; `node/mod.rs::NodeState::recover_known_state_facts`; `node/eviction.rs::NodeState::evict_cold`
