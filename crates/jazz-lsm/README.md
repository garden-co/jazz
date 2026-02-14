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

- High performance on native and WASM/OPFS
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
  - Optional env overrides:
    - `JAZZ_LSM_BENCH_KEY_COUNT`
    - `JAZZ_LSM_BENCH_VALUE_SIZES` (comma-separated bytes, e.g. `32,256,4096`)
- Native mixed benchmark matrix (ops/s + p95 op latency + operation counters) across `jazz_lsm` and `opfs_btree`:
  - `cargo run -p jazz-lsm --release --bin mixed_bench_native -- --count 5000 --value-sizes 32,256,4096 --json`
  - Optional engine filter:
    - `cargo run -p jazz-lsm --release --bin mixed_bench_native -- --engines jazz_lsm,opfs_btree --count 5000 --value-sizes 32,256,4096 --json`
  - Comparative engines (`bf_tree`, `rocksdb`, `surrealkv`, `fjall`) are available with `compare-native`:
    - `cargo run -p jazz-lsm --release --features compare-native --bin mixed_bench_native -- --engines all --count 500 --value-sizes 32,256,4096 --json`
  - Optional cold read scenarios:
    - `cargo run -p jazz-lsm --release --bin mixed_bench_native -- --count 5000 --value-sizes 32,256,4096 --include-cold-read --json`
- Native comparative benchmark (same workload matrix across `jazz-lsm`, `opfs_btree`, `bf-tree`, `rocksdb`, `surrealkv`, `fjall`):
  - `cargo bench -p jazz-lsm --features compare-native --bench compare_native -- --quick`
  - Includes `compare_native_cold_seq_read` and `compare_native_cold_random_read` groups (cold groups currently exclude `bf-tree` because reopen validation is unstable there).
  - Uses the same optional env overrides:
    - `JAZZ_LSM_BENCH_KEY_COUNT`
    - `JAZZ_LSM_BENCH_VALUE_SIZES`
  - On macOS, `rocksdb` build may require LLVM `libclang` in env:
    - `DYLD_LIBRARY_PATH=/opt/homebrew/opt/llvm/lib DYLD_FALLBACK_LIBRARY_PATH=/opt/homebrew/opt/llvm/lib LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib cargo bench -p jazz-lsm --features compare-native --bench compare_native -- --quick`
- WASM OPFS benchmark harness (headless Chromium via Playwright):
  - `pnpm --dir /Users/anselm/jazz2-clean/crates/jazz-lsm run bench:wasm`
  - Optional args passed to the runner:
    - `pnpm --dir /Users/anselm/jazz2-clean/crates/jazz-lsm run bench:wasm:opfs -- --count 1000 --value-sizes 32,256 --json`
    - `pnpm --dir /Users/anselm/jazz2-clean/crates/jazz-lsm run bench:wasm:opfs -- --profile mixed --count 1000 --value-sizes 32,256 --json`
    - `pnpm --dir /Users/anselm/jazz2-clean/crates/jazz-lsm run bench:wasm:opfs -- --count 1000 --value-sizes 32,256 --include-cold-read --json`
