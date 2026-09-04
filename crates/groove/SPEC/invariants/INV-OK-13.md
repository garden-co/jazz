# INV-OK-13

- Status: now
- Coverage: ✓

## Invariant

Persisted schema index reads MUST match a full-scan oracle over committed base-table state.

## Enforced by (tests)

`groove::db::tests::randomized_index_reads_match_full_scan_oracle`

## Implementation

`src/ivm/runtime/persist.rs::apply_persist_delta`; `src/db/mod.rs::Database::index_scan`; `src/db/mod.rs::Database::decode_raw_index_entries`
