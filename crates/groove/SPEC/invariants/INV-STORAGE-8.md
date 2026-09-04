# INV-STORAGE-8

- Status: now
- Coverage: ✓

## Invariant

`RecordDescriptor::fields()` and field indices MUST remain in logical declaration order even though encoded bytes may reorder fixed-width fields before variable-width fields.

## Enforced by (tests)

`groove::records::tests::descriptor_fields_remain_in_declaration_order`; `groove::records::tests::record_newtype_tail_wrapper_uses_logical_tail_despite_physical_reordering`; `groove::records::tests::logical_order_reads_match_full_decode_for_interleaved_seeded_schemas`

## Implementation

`records/mod.rs::RecordDescriptor::from_logical_fields`; `records/mod.rs::RecordDescriptor::fields`; `records/mod.rs::RecordDescriptor::get_idx`
