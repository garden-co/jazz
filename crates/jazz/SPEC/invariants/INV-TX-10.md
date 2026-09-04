# INV-TX-10

- Status: now
- Coverage: ✓

## Invariant

Applying a fate update MUST NOT move `global_time` backward and MUST update `durability` only monotonically upward.

## Enforced by (tests)

`jazz::node::tests::harness::fate_update_rejects_backward_global_time_and_keeps_durability_monotone`

## Implementation

`jazz/src/node/ingest.rs::NodeState::apply_fate_update`
