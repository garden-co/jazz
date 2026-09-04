# INV-DATA-6

- Status: now
- Coverage: ✓

## Invariant

`SchemaVersionId` MUST be UUIDv5 over `JazzSchema::canonical_bytes()` in namespace `SCHEMA_VERSION_NAMESPACE`.

## Enforced by (tests)

`jazz::schema::tests::schema_version_id_is_stable_and_content_addressed`

## Implementation

`schema.rs::SCHEMA_VERSION_NAMESPACE`; `schema.rs::JazzSchema::canonical_bytes`; `schema.rs::JazzSchema::version_id`; `schema.rs::canonical_schema_bytes`
