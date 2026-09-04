# INV-REC-10

- Status: now
- Coverage: ✓

## Invariant

Context-dependent recursive arrangements MUST be keyed by `ScopePath` and recursive `sub_tick`; root-scope arrangements MUST use `sub_tick = 0` and MUST absorb a public tick's table delta exactly once even when recursive and non-recursive consumers share them.

## Enforced by (tests)

`arrangement_regressions::recursive_incremental_ticks_do_not_inflate_shared_edge_arrangements`; `arrangement_regressions::arrangement_shared_across_sub_ticks_is_applied_once_per_tick`

## Implementation

`groove/src/ivm/runtime/mod.rs::NodeState::operator_scope`; `groove/src/ivm/runtime/mod.rs::NodeState::arrangement_sub_tick`; `groove/src/ivm/runtime/mod.rs::NodeState::update_join`
