# INV-REC-11

- Status: now
- Coverage: ✓

## Invariant

Hydrating a new subscriber to an already-shared recursive node MUST return the full current recursive result and MUST NOT consume or suppress future tick deltas for existing subscribers.

## Enforced by (tests)

`snapshot_subscription_regressions::second_subscriber_to_prepared_recursive_graph_gets_full_initial_message`; `snapshot_subscription_regressions::hydrating_a_new_subscriber_must_not_steal_tick_deltas_from_existing_recursive_subscribers`; `snapshot_subscription_regressions::new_subscriber_uses_current_state_not_stale_hydrated_accumulated`; `groove::db::tests::subscribe_supports_recursive_hydration_snapshot_message`

## Implementation

`groove/src/ivm/runtime/mod.rs::NodeState::update_recursive`; `groove/src/ivm/runtime/recursion.rs::recompute_recursive`
