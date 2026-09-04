# INV-DATA-8

- Status: now
- Coverage: ✓

## Invariant

Structural schema identity MUST recursively encode every portable column type, including payload-enum case and field structure: type tags, declared case and field order, names, field types, and nullability are identity-bearing.

## Enforced by (tests)

`jazz::tools::public_api::types::tests::schema_hash_matches_portable_column_types_cross_runtime_fixture`; `jazz-tools/src/dev/schema-utils.test.ts`

## Implementation

`jazz/src/tools/public_api/types/branch.rs::hash_column_type`; `jazz-tools/src/dev/schema-utils.ts::hashColumnType`
