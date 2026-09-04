# INV-LOWER-22

- Status: now
- Coverage: ✓

## Invariant

One normalized-program access-path derivation MUST serve ordinary Global current reads and read-policy authorization programs. It MAY narrow only with a primary-key or declared-index equality; residual predicates remain in the lowered graph, while alternative branches, predicates without an eligible equality, joins, missing/nullable claims, and non-Global reads fall back to the counted full scan.

## Enforced by (tests)

`jazz::node::tests::harness::one_shot_filtered_read_uses_primary_key_scan_for_id_equality`; `jazz::node::tests::harness::one_shot_filtered_read_uses_declared_index_for_indexed_column_equality`; `jazz::node::tests::harness::indexed_read_policy_matches_local_scan_for_allowed_and_denied_identities`; `jazz::node::tests::harness::policy_access_path_planner_falls_back_for_or_and_non_equality`; `jazz::node::tests::harness::policy_access_path_planner_falls_back_for_missing_or_nullable_claims_and_joins`

## Implementation

`jazz/src/node/query_eval/read_sources.rs::NodeState::query_program_access_paths`; `jazz/src/node/query_eval/read_sources.rs::JazzSourceGraphPreparer::cached_policy_authorization_access_path`; `jazz/src/node/query_eval/normalization.rs::select_current_access_path`
