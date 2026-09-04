# INV-LOWER-19

- Status: now
- Coverage: ✓

## Invariant

Lowered record wrapper field indexes MUST match the groove schema record descriptors used at node open.

## Enforced by (tests)

`jazz::node::tests::general::lowered_record_wrapper_field_indexes_match_open_descriptors`

## Implementation

`jazz/src/node/codec.rs::debug_assert_lowered_layouts`
