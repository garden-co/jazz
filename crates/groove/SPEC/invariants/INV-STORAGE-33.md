# INV-STORAGE-33

- Status: now
- Coverage: ✓

## Invariant

A payload `EnumValue` MUST persist its declaration-order `u32` case tag as a minimal little-endian base-128 varint immediately followed by the selected case's canonical record payload; unknown, truncated, overflowing, and non-minimal tags are invalid.

## Enforced by (tests)

`groove::records::tests::{epoch_1_variable_scalar_array_and_payload_enum_goldens_are_exact_and_fail_closed,epoch_1_payload_enum_tag_128_uses_the_exact_two_byte_envelope}`; `groove::tests::variant_tables::table_variant_tags_use_canonical_bounded_varints`

## Implementation

`groove/src/records/{mod,values}.rs::{encode_variant_record,split_variant_record,decode_value}`
