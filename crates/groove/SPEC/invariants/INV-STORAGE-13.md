# INV-STORAGE-13

- Status: now
- Coverage: ✓

## Invariant

`ScalarEnumSchema` values MUST persist and compare by declaration-order `u8` discriminant; appending variants is compatible, but reordering/removing variants changes stored meaning.

## Enforced by (tests)

`groove::records::tests::enum_values_decode_as_discriminants_and_store_discriminants`; `groove::db::tests::enum_index_keys_follow_declaration_order`

## Implementation

`records/values.rs::ScalarEnumSchema::discriminant`; `records/values.rs::ScalarEnumSchema::variant`; `records/values.rs::encode_value`; `ivm/runtime/mod.rs::encode_record_field_key_part`
