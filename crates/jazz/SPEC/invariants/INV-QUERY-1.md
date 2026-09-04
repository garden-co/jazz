# INV-QUERY-1

- Status: now
- Coverage: ✓

## Invariant

`Query::validate` MUST stamp a shape with the schema version it validated against, and `ShapeId` MUST include both canonical query bytes and `SchemaVersionId`.

## Enforced by (tests)

`jazz::query::tests::schema_version_context_changes_shape_id`

## Implementation

`query.rs::validate_query`
