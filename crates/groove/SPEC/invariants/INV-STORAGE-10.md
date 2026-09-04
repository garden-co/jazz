# INV-STORAGE-10

- Status: now
- Coverage: ✓

## Invariant

Fixed-width nullable nulls MUST encode as flag `0` plus zero-filled reserved payload width; variable-width nullable nulls MUST encode as only flag `0`.

## Enforced by (tests)

`groove::records::tests::encodes_nullable_fixed_size_values_with_flag_and_reserved_width`; `groove::records::tests::encodes_nullable_variable_size_null_as_only_flag_byte`; `groove::records::tests::enum_nullable_layout_stays_fixed_width_and_patchable`

## Implementation

`records/values.rs::encode_nullable`; `records/values.rs::decode_nullable`; `records/mod.rs::BorrowedRecord::nullable_field`
