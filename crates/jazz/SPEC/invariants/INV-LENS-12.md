# INV-LENS-12

- Status: now
- Coverage: ✓

## Invariant

Natural lens reads MUST select winners from the shared physical lineage before projecting rows into the requested schema, including rows materialized from settled subscription state.

## Enforced by (tests)

`jazz::node::tests::catalogue_lenses::shared_physical_reads_project_natural_lenses_after_schema_agnostic_winner`

## Implementation

`jazz/src/node/source_resolution.rs::NodeState::current_rows_for_schema`; `jazz/src/node/query_eval.rs::NodeState::settled_binding_view_source_rows`; `jazz/src/node/query_eval.rs::NodeState::translate_cells`
