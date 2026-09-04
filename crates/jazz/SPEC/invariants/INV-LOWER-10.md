# INV-LOWER-10

- Status: now
- Coverage: ✓

## Invariant

Parameterized query plans MUST be prepared as groove shapes with binding descriptor and stable name `jazz-query:<shape_id>`, then executed through `Database::bind_shape`; maintained subscription views with hidden routing provenance MUST prepare a clean output graph plus an internal routing graph through `Database::prepare_one_sink_with_routing`.

## Enforced by (tests)

`jazz::node::tests::policies_rls::maintained_view_cold_snapshot_seeds_maintained_indexes_equal_one_shot`

## Implementation

`jazz/src/node/query_eval.rs::NodeState::prepared_query_plan`; `jazz/src/node/query_eval.rs::NodeState::query_rows_with_prepared_plan`; `jazz/src/node/query_eval.rs::NodeState::subscribe_maintained_view_tagged_graph`
