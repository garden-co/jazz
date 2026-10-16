# Vendored C libraries (napkin port runtime)

Only the source files the runtime needs were copied (from raw.githubusercontent.com at the
tags below). They are wrapped by `rt/crypto.c` and `rt/zstd.c` and stand in for the Rust
crates the browser build ships. Per-directory compile flags live in `cflags.txt` next to the
sources; `build/link.sh` applies them (and `-DNDEBUG` everywhere).

| Directory | Library | Version | License | Files | Replaces (Rust) |
|---|---|---|---|---|---|
| `monocypher/` | [Monocypher](https://github.com/LoupVaillant/Monocypher) | tag `4.0.2` | BSD-2-Clause OR CC0-1.0 (`LICENCE.md`) | `src/monocypher.{c,h}`, `src/optional/monocypher-ed25519.{c,h}` | curve25519-dalek + ed25519-dalek, sha2 (SHA-512) |
| `blake3/` | [BLAKE3 official C](https://github.com/BLAKE3-team/BLAKE3/tree/master/c) | tag `1.5.4` | CC0-1.0 OR Apache-2.0 (`LICENSE_CC0`, `LICENSE_A2`) | `c/blake3.{c,h}`, `c/blake3_impl.h`, `c/blake3_dispatch.c`, `c/blake3_portable.c` | blake3 |
| `crypto-algorithms/` | [Brad Conte crypto-algorithms](https://github.com/B-Con/crypto-algorithms) | `master` (last upstream change 2015; no tags) | public domain (see `README.md`) | `sha256.{c,h}`, `sha1.{c,h}` | sha2 (SHA-256), sha1_smol |
| `zstd/` | [Zstandard](https://github.com/facebook/zstd) decompressor | tag `v1.5.6` | BSD-3-Clause OR GPL-2.0 (`LICENSE`) | `lib/zstd.h`, `lib/zstd_errors.h`, `lib/common/{allocations,bits,bitstream,compiler,cpu,debug,error_private,fse,huf,mem,portability_macros,xxhash,zstd_deps,zstd_internal,zstd_trace}.h`, `lib/common/{debug,entropy_common,error_private,fse_decompress,xxhash,zstd_common}.c`, `lib/decompress/*` (no `huf_decompress_amd64.S`) | ruzstd (vendored `jazz-ruzstd`) |

## Build configuration

- **zstd** (`zstd/cflags.txt`), configured for size:
  - `-DZSTD_NO_INLINE=1`: drop zstd's `FORCE_INLINE` (lets `-Oz` decide).
  - `-DHUF_FORCE_DECOMPRESS_X1`: single-symbol Huffman decoder only (no X2 tables/code).
  - `-DZSTD_FORCE_DECOMPRESS_SEQUENCES_SHORT`: only the short-offset sequence decoder (no long-offset/prefetch variant).
  - `-DZSTD_LEGACY_SUPPORT=0`: no legacy (pre-v0.8) frame formats; `lib/legacy` not vendored.
  - `-DZSTD_TRACE=0`, `-DZSTD_DISABLE_ASM`, `-DDEBUGLEVEL=0`, `-DZSTD_NO_UNUSED_FUNCTIONS`.
  - No compressor, no dictionary builder, no multithreading (none vendored).
  - Error strings are **kept** (`ZSTD_STRIP_ERROR_STRINGS` not set): ruzstd's errors carry
    Display texts too, and the port formats them into `channel codec: ...` messages.
    Stripping would save roughly 1-1.5 KB of data.
- **BLAKE3** (`blake3/cflags.txt`): `-DBLAKE3_NO_SSE2 -DBLAKE3_NO_SSE41 -DBLAKE3_NO_AVX2
  -DBLAKE3_NO_AVX512 -DBLAKE3_USE_NEON=0` (portable backend only; on wasm32 the dispatcher
  selects it anyway, the flags make it explicit). Rust's blake3 crate on wasm32 also uses
  its portable implementation.
- **Monocypher**, **crypto-algorithms**: no flags.

## Isolated sizes (wasm32, -Oz -flto, wasm-opt -O, module minus an empty reactor)

Each library was linked alone with a tiny export harness calling its entry points
(see `reports/runtime.md` for method and the full table). Code + data bytes:

| Library | code | data | Rust crate it replaces (raw code, from the shipped build) |
|---|---:|---:|---|
| Monocypher ed25519 (keypair/sign/check, incl. its SHA-512) | ~10.8 KB | 3.8 KB | curve25519_dalek + ed25519 ~28 KB |
| Monocypher SHA-512 alone | ~1.7 KB | 0.6 KB | (part of sha2 ~11 KB) |
| BLAKE3 portable | ~13.2 KB | 0.04 KB | blake3 ~13 KB |
| SHA-256 + SHA-1 (crypto-algorithms) | ~2.2 KB | 0.3 KB | sha2 (part) + sha1_smol ~4.6 KB |
| zstd decompressor (streaming API) | ~22 KB | ~3.9 KB | ruzstd ~8 KB (+ some inlined into callers) |
