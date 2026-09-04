# INV-QUERY-22

- Status: now
- Coverage: ✓

## Invariant

Structured query output MUST be constructed only at the Groove output terminal as an ordered recursive tree. Hydration and authoritative reset replace complete terminal state; maintained updates are stable-keyed root/path `Insert`, `Update`, `Remove`, and `Move` operations whose explicit indices define order. Every child record MUST carry its source row id as an explicitly projected field. No public consumer may infer structure or ordering from flat relation deltas.

## Enforced by (tests)

`jazz::db::tests::structured_subscription_splices_in_terminal_root_order_after_insert`; `jazz::db::tests::array_subquery_subscription_reflects_child_mutations_and_parent_removal`; `incremental_delivery_canary::maintained_relation_include_single_row_changes_are_scale_independent`

## Implementation

`groove/src/ivm/op_types.rs::TerminalOperation`; `groove/src/ivm/runtime/mod.rs::update_unbounded_collect_by_terminal_state`; `jazz/src/db.rs::refresh_subscriptions_in`
