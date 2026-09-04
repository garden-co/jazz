# INV-STORAGE-11

- Status: now
- Coverage: ✓

## Invariant

Fixed-width arrays MUST encode as concatenated element encodings without an element count; variable-width arrays MUST encode `count: u32`, offsets for all but the final element, then payloads.

## Enforced by (tests)

`groove::records::tests::encodes_fixed_size_arrays_without_count`; `groove::records::tests::encodes_empty_fixed_size_arrays_as_empty_payloads`; `groove::records::tests::encodes_variable_size_arrays_with_offsets`; `groove::records::tests::encodes_empty_variable_size_arrays_with_zero_count`; `groove::records::tests::encodes_nested_variable_arrays`

## Implementation

`records/values.rs::encode_array`; `records/values.rs::decode_array`
