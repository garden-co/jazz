# INV-BVIEW-19

- Status: now
- Coverage: ✓

## Invariant

Schema evolution MAY only add a `branchBy` column with an immutable typed default. Historical versions and old-schema selectors MUST normalize to that default; removal and type/encoding/default change are forbidden.

## Enforced by (tests)

`jazz::node::tests::harness::added_branch_column_defaults_old_history_and_survives_column_rename`; `jazz::node::tests::harness::branch_column_evolution_rejects_non_monotone_changes`

## Implementation

`node/ingest/catalogue.rs::NodeState::validate_migration_lens_between`; `node/source_resolution.rs::NodeState::equivalent_stored_branch_keys`
