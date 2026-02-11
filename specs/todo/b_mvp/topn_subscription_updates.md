# Top-N Subscription Updates — TODO (MVP)

Subscriptions with `ORDER BY ... LIMIT N` don't receive incremental updates when new rows enter the top N. LimitOffsetNode may need rework or a dedicated TopN node.

> `crates/groove/src/query_manager/graph_nodes/limit_offset.rs`
