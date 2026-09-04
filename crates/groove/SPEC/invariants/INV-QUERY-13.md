# INV-QUERY-13

- Status: now
- Coverage: ✓

## Invariant

`AntiJoin` MUST retract or restore visible left rows only when the right-side count crosses zero; changes that keep the right count nonzero MUST NOT emit anti-join deltas.

## Enforced by (tests)

`groove::db::tests::anti_join_retracts_and_restores_on_right_threshold_transitions`; `groove::db::tests::anti_join_only_changes_when_right_count_crosses_zero`

## Implementation

`groove/src/ivm/runtime/join.rs::AntiJoinState::apply`
