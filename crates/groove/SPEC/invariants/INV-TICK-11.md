# INV-TICK-11

- Status: now
- Coverage: ✓

## Invariant

Anti-join output deltas MUST represent the visibility diff of left records for keys whose left or right inputs changed.

## Enforced by (tests)

groove::db::tests::prepared_recursive_binding_retracts_transitive_paths_from_antijoin_input; groove::db::tests::prepared_recursive_binding_retracts_first_paths_from_antijoin_input

## Implementation

groove/src/ivm/runtime/join.rs::AntiJoinState::apply
