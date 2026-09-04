# INV-API-6

- Status: now
- Coverage: ✓

## Invariant

`Db::subscribe` MUST support live subscriptions at the requested effective tier. Local subscriptions are first-class application-facing subscriptions that include the node's own pending committed writes and MUST publish their truthful node-local opening, including an empty opening, even when `Propagation::Full` concurrently requests upstream coverage; propagation does not raise the requested observation tier. Edge/global subscriptions apply the same query semantics over their accepted-state frontiers and MUST withhold an empty opening until it is authority-backed. The target implementation is maintained subscription views for every tier; until local maintained views are fully unified with the edge/global path, local effective-tier subscriptions MAY serve alpha-style local live reads from an explicitly named local materialized-row bridge. No tier may introduce a second facade-side query engine as the target semantics.

## Enforced by (tests)

`jazz::db::tests::db_facade_subscription_accepts_local_tier_for_alpha_style_live_reads`; `jazz::db::tests::local_tier_full_propagation_publishes_truthful_empty_opening`; `jazz::db::tests::subscription_opening_publication_follows_upstream_coverage_lifecycle`

## Implementation

`jazz/src/db.rs::Db::prepare_query`; `jazz/src/db.rs::Db::subscribe`; `jazz/src/node/query_eval.rs::NodeState::open_local_maintained_view_subscription`
