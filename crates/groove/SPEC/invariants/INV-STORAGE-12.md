# INV-STORAGE-12

- Status: now
- Coverage: ✓

## Invariant

`F64` record and ordered-key values MUST NOT be NaN, including caller-supplied raw records at durable admission.

## Enforced by (tests)

`groove::records::tests::{f64_accessors_reject_nan_and_record_field_round_trips,epoch_1_scalar_record_fixture_is_exact_and_rejects_nan_and_noncanonical_null}`; `groove::tests::versioned_rows::raw_variant_record_with_nan_is_rejected_before_durable_admission`; `groove::ivm::runtime::tests::key_encoding_preserves_value_order_for_index_range_scans`; `groove::db::tests::epoch_1_primary_and_index_key_fixtures_are_exact_and_fail_closed`

## Implementation

`records/values.rs::{encode_fixed_value,validate_value_inner}`; `records/mod.rs::BorrowedRecord::{validate,get_f64}`; `db/encoding.rs::resolve_variant_record`; `ivm/runtime/mod.rs::encode_key_part`; `db/encoding.rs::decode_index_key_part`
