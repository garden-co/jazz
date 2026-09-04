# INV-TICK-20

- Status: now
- Coverage: ✓

## Invariant

Contextual recursive child state MUST NOT be persisted in `operator_states` after recursive recompute; retained child operator state outside `FrontierSource` context remains root-scoped.

## Enforced by (tests)

groove::ivm::runtime::tests::recursive_recompute_reuses_graph_nodes_without_persisting_contextual_child_state

## Implementation

groove/src/ivm/runtime/mod.rs::TickEvaluator::operator_scope; groove/src/ivm/runtime/mod.rs::TickEvaluator::update_recursive
