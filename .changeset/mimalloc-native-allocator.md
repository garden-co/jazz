---
"jazz-tools": patch
"jazz-napi": patch
---

Switch native targets to `mimalloc` as the global allocator. The `jazz-tools` CLI server binary and the `jazz-napi` Node native module now run on `mimalloc` (via `mimalloc-safe` for napi, the napi-rs–maintained fork). Yields ~12–26% throughput on alloc-heavy database paths (insert/update/observer) on Linux and macOS without API changes. Bundle-size impact is negligible (~+43 KB gzipped on the napi `.node`).
