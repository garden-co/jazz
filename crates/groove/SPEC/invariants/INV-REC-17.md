# INV-REC-17

- Status: now
- Coverage: ✓

## Invariant

Semantically depth-bounded recursion MUST include the depth-zero seed and facts produced by at most `max_iters` step evaluations, including no step frontier when `max_iters = 0`, and discard the next frontier without reporting non-convergence; maintained base or binding changes MUST preserve that seed-relative depth.

## Enforced by (tests)

`jazz::node::tests::harness::max_depth_zero_is_seed_only_and_one_adds_exactly_one_authorization_hop`; `jazz::node::tests::harness::scalar_frontier_policy_maintains_raw_evidence_without_disclosing_dependencies`; `jazz::node::tests::harness::scalar_frontier_read_and_all_write_actions_share_one_relation`

## Implementation

`groove/src/ivm/runtime/recursion.rs::recursive_delta`; `jazz/src/node/query_engine/lowering/graph_lowering.rs::lower_recursive_relation_cached`
