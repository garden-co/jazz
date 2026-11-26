---
"jazz-tools": patch
"jazz-wasm": patch
---

The pure-Rust zstd decoder for channel streams is now zrip from crates.io, in place of Jazz's patched ruzstd fork. Its Cargo feature `transport-compression-ruzstd` (on by default in `jazz-wasm`) is now `transport-compression-zrip`, and the `jazz-ruzstd` crate is no longer published. Wire bytes don't change.
