# INV-ARR-6

- Status: now
- Coverage: ✓

## Invariant

Source operators MAY hydrate from static point, prefix, or range scan specs over their table/index arrangement key; the scan spec MUST participate in node identity, and one-shot static scans MUST NOT perturb existing subscriptions.

## Enforced by (tests)

`groove::db::tests::table_static_scan_specs_hydrate_like_full_scan_then_filter`; `groove::db::tests::index_static_scan_specs_filter_index_records`; `groove::db::tests::static_scan_specs_participate_in_node_identity`; `groove::db::tests::one_shot_static_scan_does_not_perturb_existing_subscription`

## Implementation

groove/src/ivm/graph.rs::GraphBuilder::table_scan; groove/src/ivm/graph.rs::GraphBuilder::index_scan; groove/src/ivm/runtime/mod.rs::hydrate_source; groove/src/ivm/runtime/mod.rs::persisted_index_scan_bounds
