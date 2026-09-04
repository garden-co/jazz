# INV-QUERY-5

- Status: now
- Coverage: ✓

## Invariant

`Subscribe` MUST name a registered shape and match inferred parameter arity; the supplied usage-site subscription id is independent from the binding id, and `Unsubscribe` MUST drop that usage subscription's settled result set.

## Enforced by (tests)

`jazz::node::tests::queries::binding_delta_validates_shape_arity_and_cleans_up_binding_usage`

## Implementation

`node/query_eval.rs::NodeState::apply_subscribe`
