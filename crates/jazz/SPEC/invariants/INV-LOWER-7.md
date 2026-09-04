# INV-LOWER-7

- Status: now
- Coverage: ✓

## Invariant

Global current-row reads MUST use the physical lineage's content and register global-current tables, not scan history, and MUST hide content when the register winner is `Deleted`.

## Enforced by (tests)

`jazz::node::tests::queries::view_update_result_set_matches_groove_current_rows_for_seeded_commits`; `jazz::node::tests::catalogue_lenses::physical_deletion_register_spans_renamed_schemas_and_reopens`

## Implementation

`jazz/src/node/physical.rs::NodeState::physical_current_source_graph`
