# INV-LENS-17

- Status: now
- Coverage: ✓

## Invariant

`TransformColumn` MUST be accepted only when its transform key is registered as bijective and canonical-equality-preserving; the current registry is identity/no-op only.

## Enforced by (tests)

`jazz::node::tests::catalogue_lenses::registered_transform_column_identity_is_accepted_and_projected`; `jazz::node::tests::catalogue_lenses::transform_column_rejects_unregistered_transform_at_publish`

## Implementation

`jazz/src/schema.rs::registered_column_transform`; `jazz/src/node/ingest.rs::NodeState::validate_migration_lens`; `jazz/src/node/query_eval.rs::apply_table_lens_forward`; `jazz/src/node/query_eval.rs::apply_table_lens_reverse`
