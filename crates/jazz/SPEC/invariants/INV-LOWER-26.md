# INV-LOWER-26

- Status: now
- Coverage: full: local and remote carriers publish only authoritative terminal roots

## Invariant

A structured query MUST expose one authoritative terminal output relation keyed by its public result identity. Groove MUST assemble nested paths into that terminal, so a child change is a root addition/retraction/replacement (or an equivalent root-addressed structural patch); public carriers MUST NOT require a second relation-row/edge delta stream or a higher-level assembler. Empty optional collections remain encoded in an ordinary root row. Ordered root changes MUST remain incremental: an identity-stable content change is an update, while a positional change is an ordered root-addressed splice rather than a terminal reset.

## Enforced by (tests)

`jazz::node::tests::queries::maintained_array_collector_retains_authorized_parent_trees_incrementally`; `jazz::db::tests::array_subquery_live_subscription_publishes_only_terminal_root_rows`; `jazz::db::tests::structured_subscription_splices_in_terminal_root_order_after_insert`; `jazz::db::tests::array_subquery_subscription_updates_child_order_limit_boundary`

## Implementation

`jazz/src/node/query_engine/lowering.rs::lower_collect_by_app_rows`; `groove/src/ivm/runtime/mod.rs::TickEvaluator::update_collect_by`; `jazz/src/db.rs::refresh_subscriptions_in`
