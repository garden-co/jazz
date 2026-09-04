# INV-LENS-13

- Status: now
- Coverage: ✓

## Invariant

Natural forward/reverse lens projection MUST implement `RenameColumn`, `CopyColumn`, `AddColumn`, and `DropColumn.backwards_default` deterministically, and MUST reject `TransformColumn`/`RejectSourceDelta` during projection.

## Enforced by (tests)

`jazz::node::tests::catalogue_lenses::shared_physical_reads_project_natural_lenses_after_schema_agnostic_winner`; `jazz::node::tests::catalogue_lenses::lens_parallel_materialization_oracle_matches_engine_reads_seeded`

## Implementation

`jazz/src/node/physical.rs::NodeState::physical_content_projection_case`; `jazz/src/node/source_resolution.rs::NodeState::current_rows_for_schema`
