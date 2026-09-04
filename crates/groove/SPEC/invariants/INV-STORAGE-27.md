# INV-STORAGE-27

- Status: now
- Coverage: ✓

## Invariant

A record-valued `ValueType` MUST carry its descriptor inline and accept only canonical child bytes; it MUST NOT appear, directly or recursively, in a durable primary key.

## Enforced by (tests)

`groove::db::tests::direct_record_store_rejects_record_containing_durable_keys_at_schema_admission`; `groove::records::tests::record_values_reject_non_canonical_child_bytes`

## Implementation

`groove/src/records/values.rs::ValueType::contains_record`; `groove/src/records/values.rs::ensure_value_type`; `groove/src/db/mod.rs::validate_durable_key_schema`; `groove/src/schema.rs::PrimaryKeyType`
