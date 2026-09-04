# INV-REC-12

- Status: now
- Coverage: ✓

## Invariant

Recursive recompute MUST NOT persist per-context child operator state in the runtime state maps after recompute completes.

## Enforced by (tests)

`groove::ivm::runtime::tests::recursive_recompute_reuses_graph_nodes_without_persisting_contextual_child_state`

## Implementation

`groove/src/ivm/runtime/recursion.rs::HydrationEvaluator`; `groove/src/ivm/runtime/mod.rs::NodeState::update_recursive`
