# INV-STORAGE-25

- Status: now
- Coverage: ✓

## Invariant

Ordered index key encoding via `encode_key_part` MUST preserve logical ordering for supported key values in RocksDB lexicographic order and MUST reject arrays as keys.

## Enforced by (tests)

`groove::ivm::runtime::tests::key_encoding_preserves_value_order_for_index_range_scans`

## Implementation

`ivm/runtime/mod.rs::encode_key_part`; `ivm/runtime/mod.rs::order_preserving_f64_bits`; `ivm/runtime/mod.rs::encode_ordered_bytes`
