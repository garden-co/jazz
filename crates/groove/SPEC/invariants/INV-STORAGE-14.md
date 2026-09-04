# INV-STORAGE-14

- Status: now
- Coverage: ✓

## Invariant

Primary-key bytes MUST be order-preserving tagged encodings: integer payloads big-endian, `Bool` as `0|1`, `Uuid` raw bytes, and `String`/`Bytes` escaped with embedded NUL `00 ff` plus terminator `00 00`.

## Enforced by (tests)

`groove::db::tests::epoch_1_primary_and_index_key_fixtures_are_exact_and_fail_closed`; `groove::db::tests::direct_record_store_tuple_keys_set_prefix_order_and_reopen_symmetrically`

## Implementation

`db/batch.rs::PrimaryKeyValue::into_bytes`; `db/encoding.rs::encode_primary_key_part`; `db/encoding.rs::decode_primary_key_part`; `db/encoding.rs::encode_ordered_bytes`
