# INV-STORAGE-34

- Status: now
- Coverage: ✓

## Invariant

`MemoryStorage` restart snapshots MUST use only the explicit canonical V1 `GMS1` length-delimited map codec; malformed, truncated, trailing, duplicate, or alternate-order bytes MUST fail before they replace resident storage, and earlier serde/postcard snapshot bytes have no compatibility path.

## Enforced by (tests)

`groove::storage::memory::tests::{snapshot_v1_golden_bytes_are_exact_and_reject_alternate_encodings,import_snapshot_rejects_invalid_families_without_replacing_state}`

## Implementation

`groove/src/storage/memory.rs::{MemoryStorage::export_snapshot,MemoryStorage::import_snapshot,encode_snapshot,decode_snapshot}`
