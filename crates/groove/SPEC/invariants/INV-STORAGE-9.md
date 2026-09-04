# INV-STORAGE-9

- Status: now
- Coverage: ✓

## Invariant

Fixed-width record scalar payloads and record/array offsets MUST use little-endian encoding inside record values; fixed-width tuple integer members MUST use big-endian order-preserving member encoding.

## Enforced by (tests)

`groove::records::tests::encodes_all_scalar_value_types_little_endian`; `groove::records::tests::encodes_record_offsets_relative_to_record_start`; `groove::records::tests::encodes_variable_array_offsets_relative_to_array_start`; `groove::records::tests::tuple_integer_members_are_big_endian_even_inside_little_endian_records`

## Implementation

`records/values.rs::encode_fixed_value`; `records/values.rs::encode_tuple_member`; `records/values.rs::write_u32`; `records/mod.rs::RecordDescriptor::create`; `records/values.rs::encode_array`
