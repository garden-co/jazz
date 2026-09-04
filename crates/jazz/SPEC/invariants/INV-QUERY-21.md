# INV-QUERY-21

- Status: now
- Coverage: ✓

## Invariant

Array subqueries MUST remain distinct from forward `Include` paths and MUST be assembled by Groove's sole public output terminal into recursive root values. Public one-shot, subscription, sync, N-API, WASM, and TypeScript carriers MUST expose complete roots for hydration/reset or typed root/path structural edits for maintained changes; they MUST NOT expose a parallel relation-edge plus row-batch representation or require a higher-level assembler. Child filters/select/order/offset/limit affect only their terminal slot, optional unreadable children are omitted while readable parents remain, and only explicit requirements may filter root membership.

## Enforced by (tests)

`jazz::db::tests::array_subquery_live_subscription_publishes_only_terminal_root_rows`; `jazz::db::tests::array_subquery_remote_subscription_hydrates_edge_referenced_child_rows`; `jazz::db::tests::array_subquery_subscription_updates_child_order_limit_boundary`; `jazz::tests::structured_result_tree::nested_tree_preserves_projection_order_offset_and_reset`

## Implementation

`query.rs::Query::array_subqueries`; `node/query_engine/lowering.rs::lower_collect_by_app_rows`; `groove/src/ivm/runtime/mod.rs::TickEvaluator::update_collect_by`; `db.rs::SubscriptionEvent`; `protocol.rs::SyncMessage::ViewUpdate`
