# INV-LENS-15

- Status: now
- Coverage: ✓

## Invariant

`ShapeId` MUST include the authored `SchemaVersionId`; identical canonical query bytes against different schema versions MUST produce different shape ids.

## Enforced by (tests)

`jazz::query::tests::schema_version_context_changes_shape_id`

## Implementation

`jazz/src/query.rs::validate_query`
