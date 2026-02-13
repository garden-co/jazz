# jazz-lsm Benchmark Overview

Generated on 2026-02-13.

The comparative benchmark currently measures:

- `jazz-lsm`
- `rocksdb`
- `fjall`

Run commands:

- `cargo bench -p jazz-lsm --bench lsm_native -- --quick`
- `cargo bench -p jazz-lsm --features compare-native --bench compare_native -- --quick`
- `pnpm --dir /Users/anselm/jazz2-clean/crates/jazz-lsm run bench:wasm:opfs -- --count 500 --value-sizes 32,256,4096 --json`
