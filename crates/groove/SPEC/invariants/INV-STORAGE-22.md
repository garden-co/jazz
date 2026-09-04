# INV-STORAGE-22

- Status: now
- Coverage: ✓

## Invariant

Non-unique durable index logical keys MUST append a `0xff` separator and encoded primary-key bytes; unique index keys MUST omit that suffix.

## Enforced by (tests)

`groove::db::tests::durable_non_unique_index_keys_append_separator_and_primary_key_suffix`; `groove::db::tests::durable_unique_index_keys_omit_primary_key_suffix`

## Implementation

`ivm/runtime/mod.rs::index_key`
