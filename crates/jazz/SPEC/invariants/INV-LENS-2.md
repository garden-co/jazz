# INV-LENS-2

- Status: now
- Coverage: ✓

## Invariant

A published `MigrationLens` MUST have `lens.id == lens.content_id()` and both `lens.source` and `lens.target` MUST be known `SchemaVersionId`s; `content_id()` MUST hash the canonical lens payload and exclude the embedded id field.

## Enforced by (tests)

`jazz::node::tests::catalogue_lenses::catalogue_lens_publish_validates_admin_id_and_known_endpoints`; `jazz::protocol::tests::migration_lens_content_id_uses_canonical_payload_not_id_field`; `jazz::protocol::tests::migration_lens_content_id_changes_when_structural_field_changes`

## Implementation

`jazz/src/node/ingest.rs::NodeState::apply_publish_lens`; `jazz/src/protocol.rs::MigrationLens::content_id`; `jazz/src/protocol.rs::canonical_lens_bytes`
