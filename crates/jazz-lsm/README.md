# jazz-lsm (prototype)

A simple synchronous LSM-tree prototype for Jazz.

## Goals covered

- Synchronous API (`put/get/delete/merge/scan_range/flush_wal/flush/compact_step`)
- Works with a swappable synchronous filesystem abstraction (`SyncFs`)
- Native filesystem implementation (`StdFs`) + in-memory implementation (`MemoryFs`)
- OPFS-backed worker implementation (`OpfsFs`) for `wasm32`
- Merge operators registered at open time with stable numeric IDs
- `flush_wal()` durability contract: acknowledged writes survive crash/restart
- Size-tiered foreground compaction (no background thread required)
- Tombstones are dropped only when safe (deepest-level full compaction)

## Non-goals for this prototype

- RocksDB-class performance
- Bloom filters, block indexes, or advanced compaction heuristics
- Concurrent writers

## Future extension points (already in public options)

- `KeyPrefixMode` (currently `Disabled`) for key-prefix truncation in SST format
- `ValueCompression` (currently `None`) for compressed SST value blocks

These are API-level hooks now; codec/format implementation can evolve without changing call sites.

## Test harness

- Shared behavior scenarios live in `/Users/anselm/jazz2-clean/crates/jazz-lsm/tests/support/scenarios.rs`
- Native tests reuse those scenarios in `/Users/anselm/jazz2-clean/crates/jazz-lsm/tests/lsm.rs`
- WASM worker tests reuse the same scenarios in `/Users/anselm/jazz2-clean/crates/jazz-lsm/tests/wasm.rs`

Compile checks used during development:

- `cargo test -p jazz-lsm`
- `RUSTFLAGS='--cfg=web_sys_unstable_apis' cargo test -p jazz-lsm --target wasm32-unknown-unknown --no-run`

## Benchmarks

- Native benchmark (criterion, quick mode):
  - `cargo bench -p jazz-lsm --bench lsm_native -- --quick`
- WASM OPFS benchmark harness (headless Chromium via Playwright):
  - `pnpm --dir /Users/anselm/jazz2-clean/crates/jazz-lsm run bench:wasm`
  - Optional args passed to the runner:
    - `pnpm --dir /Users/anselm/jazz2-clean/crates/jazz-lsm run bench:wasm:opfs -- --count 1000 --value-sizes 32,256 --json`
