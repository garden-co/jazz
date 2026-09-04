# INV-LOWER-24

- Status: now
- Coverage: ✓

## Invariant

Dry-run policy probes and recursion seed hydration MUST use the same deterministic source access-path selection as ordinary one-shot reads, with equivalence to the full-scan path and counters proving the selected path.

## Enforced by (tests)

`jazz::node::tests::policies_rls::recursive_reachable_write_policy_allows_direct_and_closure_docs`; `jazz::node::query_eval::tests::reachable_relation_seed_hydrates_from_primary_key_scan`

## Implementation

`jazz/src/node/query_eval.rs::NodeState::add_reachable_access_paths`; `jazz/src/node/query_eval.rs::NodeState::dry_run_write_current_allows`; `jazz/src/node/query_eval.rs::CurrentQuerySourceResolver::selected_global_current_source_graph`
