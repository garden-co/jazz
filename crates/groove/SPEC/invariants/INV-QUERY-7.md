# INV-QUERY-7

- Status: now
- Coverage: ✓

## Invariant

`Union` MUST require all non-empty inputs to have the same output descriptor and MUST preserve duplicate derivations as separate weighted deltas (`UNION ALL` semantics).

## Enforced by (tests)

`groove::db::tests::union_all_subscriptions_preserve_duplicate_derivations`

## Implementation

`groove/src/ivm/runtime/mod.rs::NodeState::update_union`
