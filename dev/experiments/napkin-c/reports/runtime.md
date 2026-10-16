# Unit "runtime": rt/ (jz_rt.h implementation) + third_party/

This unit plays the role of Rust's core/alloc/std, serde + postcard + serde_json, hashbrown,
internment, futures, uuid, base64/hex, the crypto crates and ruzstd. Every declaration in
`rt/jz_rt.h` is implemented. Everything is type-erased and written once: containers take
element sizes, comparators and hash/drop function pointers instead of being monomorphised.

## Files

C lines are `wc -l`. "Code bytes" are function-body bytes in the runtime-only build
(`rt_all` below, before wasm-opt), attributed to the file whose object defines the function
(LTO inlining inside the runtime is credited to the caller's file).

| File | C lines | Code bytes | What it implements |
|---|---:|---:|---|
| `rt/err_alloc.c` | 126 | 2,197 | `jz_panic` (console + `js_throw` + trap), `jz_unwrap_failed`, `jz_oom`, `jz_err_new/fmt/drop/context`, `jz_alloc/zalloc/realloc/free/memdup` over wasi-libc's dlmalloc (the same allocator Rust ships), RawVec-style capacity growth, generic swap |
| `rt/str.c` | 214 | 1,981 | `jz_str_*`, `jz_bytes_*`, slice compare, `jz_format` / `jz_str_appendf` (vsnprintf with a 256-byte stack first try), `core::str::from_utf8`-exact UTF-8 validation (overlongs, surrogates, > U+10FFFF, word-at-a-time ASCII path) |
| `rt/vec.c` | 130 | 1,295 | type-erased Vec: reserve (std's min non-zero cap 8/4/1), push/insert/remove/swap_remove/truncate/drop/clone/extend/dedup/retain with std's panic texts |
| `rt/sort.c` | 243 | 2,867 | `jz_sort`: stable natural merge sort (std's pre-driftsort TimSort: run detection + reverse of strict descents, insertion-sort extension to 20, TimSort collapse rules, n/2 scratch); `jz_sort_unstable`: introsort (median-of-3 / ninther pivot, Hoare partition, heapsort fallback at 2·log2 n, insertion sort ≤ 20); `jz_bsearch` = `binary_search_by` (Ok/Err index); comparators |
| `rt/btree.c` | 573 | 7,414 | real B-tree, B = 6 like std (≤ 11 keys, ≥ 5 in non-root nodes), slots 8-byte aligned; insert with split propagation, remove with predecessor swap + steal-left / steal-right / merge, root collapse; get/contains/entry/first/last/pop_first; stack-based in-order and range iteration (inclusive/exclusive bounds, std's range panics); clone, retain, append, eq, from_pairs (stable sort, last duplicate wins) |
| `rt/hmap.c` | 347 | 3,816 | hashbrown SwissTable with the portable 8-byte SWAR group: h1/h2 split, EMPTY/DELETED/FULL control bytes with mirrored tail, triangular probing, 7/8 load factor, tombstone vs EMPTY decision on erase exactly like hashbrown, resize/rehash, group-skipping iteration, retain; foldhash-style hashing (folded 64×64→128 multiply) |
| `rt/deque.c` | 73 | 795 | VecDeque ring buffer with `handle_capacity_increase`-style un-wrapping on growth |
| `rt/rc.c` | 43 | 310 | Rc/Arc header (strong, weak with std's implicit weak, drop fn), clone/release/strong/make_mut |
| `rt/intern.c` | 54 | 554 | internment::Intern: per-type pool, linear-probing (hash, ptr) table, values leaked forever |
| `rt/uuid.c` | 183 | 2,098 | v4/v5/v7, hyphenated lowercase formatting, parsing of simple/hyphenated/braced/urn forms with the uuid crate's error diagnosis texts; host imports `random_bytes` (chunked to 64 KiB like getrandom) and `now_ms` (f64 like `Date.now()`) |
| `rt/postcard.c` | 196 | 2,141 | postcard writer + reader: LEB128 varints with per-width overflow checks (u16/u32/u64), zigzag, LE floats, length-prefixed bytes/str (UTF-8 checked), uuid as serde bytes, option/seq/variant, trailing-bytes and depth checks, postcard's/serde's error texts |
| `rt/json.c` | 844 | 11,544 | serde_json: `Value` tree (objects = BTreeMap<String, Value>), recursive-descent parser with serde_json's error texts + line/column, recursion limit 128, number handling (u64 / i64 / f64 inference, "-0" → f64, long integers → f64, exact POW10 fast path else correctly rounded `strtod`), string escapes incl. surrogate pairs; compact and pretty (2-space) writers with the ESCAPE table and itoa digit-pair LUT; shortest round-trip float digits laid out like ryu (`1e300`, `1.5e-7`, `-0.0`); streaming writer `jz_jw_*`; Deserialize helpers with serde's "invalid type: …, expected …", "missing field", "unknown variant … expected one of" texts; Value clone/eq/accessors |
| `rt/base64_hex.c` | 119 | 1,585 | base64 STANDARD / URL_SAFE, pad / no-pad encode, padding-indifferent decode with canonical trailing-bit check and the crate's error texts; hex encode/decode with the hex crate's errors |
| `rt/crypto.c` | 68 | 3,264 ¹ | wrappers: sha256/sha1 (crypto-algorithms), sha512 + ed25519 (Monocypher), blake3 one-shot + streaming hasher (official C) |
| `rt/zstd.c` | 189 | 6,488 ¹ | `jz_zstd_decompress` (multi-frame, output cap) **and the vendored-ruzstd API the protocol/compression ports call**: `ruzstd__decoding__frame_decoder__FrameDecoder__{default,drop,init,set_max_block_output,decode_blocks_upto,read_streaming,streaming_available}`, `ruzstd__decoding__errors__Display__fmt`, `ruzstd__decoding__streaming_decoder__StreamingDecoder__{new,read_to_end_take,drop}` (signatures as declared in `src/jazz_protocol/_ext_protocol-types.h`), on zstd's streaming DCtx |
| `rt/task.c` | 80 | 551 | spawn_local-style FIFO task queue with re-entrancy guard, unbounded mpsc channel (2 refs: tx + rx half), waker |
| `rt/trace.c` | 91 | 936 | level-filtered events "LEVEL span:span: target: msg" → `js_log`, span stack with busy-time on close at TRACE, named counters |
| `rt/rt_internal.h` | 13 | | helpers shared by rt/*.c only |
| `rt/probe/rt_probe.c`, `rt/probe/lib_probe.c` | 264, 42 | 0 | size-measurement harnesses; compile to nothing unless `-DJZ_RT_PROBE` / `-DJZ_LIB_PROBE_*` |
| **total rt/*.c (excl. probes)** | **3,573** | **49,836** | |

¹ includes library code LTO inlined into the wrapper (e.g. sha256/sha1 compress, ZSTD_decompressStream).

Third-party C (copied unmodified, details in `third_party/README.md`): Monocypher 4.0.2
(3,456 .c lines), BLAKE3 1.5.4 portable (1,114), crypto-algorithms sha256/sha1 (307),
zstd 1.5.6 decompressor (7,622 .c + 14,451 .h lines).

## Additions to jz_rt.h (nothing changed or removed)

- `jz_btree_from_pairs_ex(t, d, pairs, n, stride, val_off)`: the existing `jz_btree_from_pairs`
  cannot know the C struct layout of a `(K, V)` pair; it assumes packed `key|val`. Use `_ex`
  with `sizeof(pair)` / `offsetof(pair, v)` for padded structs.
- `jz_intern_ex(..., bool *inserted)`: tells the caller whether its value was moved into the
  pool (when not, the caller still owns it and drops it, as `Intern::new` drops its argument).
- `enum { JZ_UUID_ERR = 0xffd0, JZ_BASE64_ERR, JZ_HEX_ERR, JZ_ZSTD_ERR }`: error codes of rt helpers.

## Semantics worth knowing (for the other units)

- `jz_btree_insert` / `jz_hmap_insert` on an existing key keep the map's key and drop the
  passed key (Rust's behaviour); the old value goes to `old_val` if non-NULL, else is dropped.
- `jz_btree_entry` / `jz_hmap_entry` consume the key: if the entry already exists the passed key
  is dropped (`entry(key)` semantics); a new entry gets a zeroed value.
- `jz_btree_remove` / `jz_hmap_remove` move key/value out if the out-pointers are non-NULL,
  otherwise drop them. An empty tree has `root == NULL`; zero-initialised maps are valid
  after `*_init` (which only stores the desc).
- `jz_chan_new` returns a channel with 2 references (sender half + receiver half); each calls
  `jz_chan_release` once. Releasing either half closes it; sends to a closed channel are dropped.
- `jz_json_i64(v)` with `v >= 0` produces a U64 value, like `serde_json::Number::from(i64)`;
  `jz_json_f64` of NaN/inf gives Null like `Value::from(f64)`.
- `jz_pc_uuid` writes uuid's serde form for binary formats: length-prefixed 16 bytes.
- Spans are recorded only at `jz_trace_level >= 4`; `jz_counter_add` is a no-op while tracing is off.

## Sizes (wasm32-wasip1, clang 19 / wasi-sdk 25, -Oz -flto, wasm-opt -O)

Measured with `build/link.sh` and the probe harnesses (exports that call every entry point
of the component; the probe is compiled with `-fno-lto` so the runtime stays behind a real
call boundary). Module = whole reactor module after wasm-opt; an empty module is 562 B.

### Third-party libraries alone (only the library + a tiny export harness)

| Library | module raw / gzip-9 / brotli-11 | code + data in it | Rust counterpart (raw code) |
|---|---|---|---|
| Monocypher ed25519 key_pair/sign/check (incl. its SHA-512) | 15,990 / 10,008 / 8,926 | 10.8 KB code + 3.8 KB data | curve25519_dalek + ed25519 ~28 KB |
| Monocypher SHA-512 only | 2,963 / 2,245 / 2,055 | 1.7 KB + 0.6 KB | sha2 ~11 KB (sha256 + sha512) |
| BLAKE3 portable (init/update/finalize) | 13,930 / 5,818 / 4,548 | 13.2 KB + 0.04 KB | blake3 ~13 KB |
| SHA-256 + SHA-1 (crypto-algorithms) | 3,116 / 1,970 / 1,703 | 2.2 KB + 0.3 KB | sha2 (part) + sha1_smol ~4.6 KB |
| zstd decompressor, streaming API (+ dlmalloc 8 KB) | 36,278 / 17,252 / 14,806 | 22.2 KB + 3.9 KB (excl. dlmalloc) | ruzstd ~8 KB (+ inlined) |

The C zstd decompressor is ~3× ruzstd even with every size option; it is the one place where
the C stand-in is clearly larger than the Rust crate. Monocypher is about half of
curve25519-dalek + ed25519-dalek, BLAKE3 is a wash.

### Runtime by topic (rt/ + third_party/, probe for one topic)

"rt own" = code bytes in rt/*.o functions before wasm-opt; "libc" = wasi-libc + compiler-rt.

| Probe | module raw / gzip-9 / brotli-11 | rt own | third-party | libc |
|---|---|---:|---:|---:|
| alloc only (`jz_alloc/realloc/free`) | 11,932 / 5,030 / 4,097 | 1.2 KB | 0 | 12.1 KB |
| alloc + `jz_format` | 26,178 / 11,595 / 9,912 | 1.9 KB | 0 | 24.5 KB |
| containers (btree, hmap, vec, sort, deque, rc, intern) | 30,007 / 13,656 / 11,824 | 19.3 KB | 0 | 12.6 KB |
| strings + base64/hex + errors | 30,872 / 14,095 / 12,207 | 6.0 KB | 0 | 24.8 KB |
| uuid | 30,720 / 14,112 / 12,244 | 5.2 KB | 0.7 KB | 24.8 KB |
| postcard | 30,099 / 13,337 / 11,477 | 4.9 KB | 0 | 24.6 KB |
| json | 48,789 / 22,198 / 19,323 | 17.6 KB | 0 | 32.6 KB |
| crypto | 39,986 / 20,168 / 16,807 | 4.9 KB | 26.4 KB | 8.9 KB |
| zstd + ruzstd API | 57,071 / 26,074 / 22,617 | 8.8 KB | 20.0 KB | 24.7 KB |
| tasks / channels / tracing | 28,493 / 12,788 / 11,007 | 4.2 KB | 0 | 24.6 KB |
| **everything** | **135,825 / 63,982 / 55,507** | **49.8 KB** | **46.4 KB** | **32.8 KB** |

(The "everything" module also holds 6.9 KB of probe code and 14.8 KB of data; rt own + third
party + libc are pre-wasm-opt bytes, wasm-opt then removes ~10%.)

libc composition in the full runtime build: printf core 9.8 KB (+ pop_arg/stdio glue ~1 KB;
pulled in by `jz_format`, float formatting included), dlmalloc ~10.2 KB (dlmalloc + dlfree +
dispose_chunk + sbrk), strtod ~6.7 KB (strtod + scanexp + __shgetc, used by JSON number
parsing and the shortest-float search), memset/scalbn/frexp/__multi3 small. No soft-float
(`long double`) code is linked. For comparison the Rust build ships dlmalloc ~6 KB plus
core::fmt and float formatting/parsing inside std.

Reference points: serde_json ~78 KB vs `json.c` 11.5 KB (+ shared strtod/printf);
hashbrown ~139 KB (monomorphised) vs `hmap.c` 3.8 KB; postcard ~7 KB vs 2.1 KB; uuid ~10 KB
vs 2.1 KB; base64 ~6 KB vs 1.6 KB.

### Reproduce

```sh
cd dev/experiments/napkin-c
# runtime, one topic or all (P_ALL, P_CONTAINERS, P_STRINGS, P_UUID, P_POSTCARD, P_JSON,
# P_CRYPTO, P_ZSTD, P_TASK, P_BASE_ALLOC, P_BASE_FMT)
build/link.sh --roots "rt third_party" --cflags "-DJZ_RT_PROBE -DP_ALL" --out out/rt_all.wasm
# one library (EMPTY, MONOCYPHER, SHA512, BLAKE3, SHA, ZSTD)
build/link.sh --roots "third_party/blake3 rt/probe" --cflags -DJZ_LIB_PROBE_BLAKE3 --out out/lib_blake3.wasm
```

## build/link.sh

Compiles every `.c` under `rt/`, `third_party/`, `src/` (recursively) with wasi-sdk clang
`--target=wasm32-wasip1 -Oz -flto -std=gnu11 -DNDEBUG -I rt -I src -I <file dir>` (+ per-dir
`cflags.txt`), in parallel (`-j`, default 2); failures are listed with their first errors and
skipped. Links a reactor (`-mexec-model=reactor -Wl,--gc-sections -Wl,--strip-debug
-Wl,--error-limit=0`; wasm-ld needs no `--no-entry` because the reactor crt provides
`_initialize`), keeps the name section, writes `out/napkin.pre-opt.wasm` and `out/napkin.wasm`
(after `wasm-opt -O -g --low-memory-unused`), and prints raw / gzip -9 / brotli -11 for both.
Without `--allow-undefined` a link with undefined symbols fails and the full sorted list goes
to `out/undefined.txt` (first 60 printed); with it, all module imports are listed in
`out/imports.txt`. Other options: `--roots`, `--extra FILE.c`, `--exclude GLOB`, `--cflags`,
`--out`, `--name`, `--no-opt`, `--quiet` (`build/link.sh --help`).

First run on the whole tree (port in progress, 172 files): 155 compiled, 17 failed (all from
one conflicting prototype in `src/jazz_node/node/mod.h`), 37 undefined symbols; with
`--allow-undefined` the module was 81,406 / 39,414 / 34,081 bytes (most code is still gc'd
because few exports exist yet).

## Verification

Besides `build/check.sh rt/*.c`, the runtime was built natively (gcc, ASan + UBSan) and
exercised: 20k-key B-tree insert/lookup/range/remove (every third key)/clone/retain/pop_first
with ordering checks; hash map insert/remove/reinsert over tombstones; both sorts against each
other and a stability check; deque wrap-around; JSON parse → write → parse round-trip (u64 max,
2^64 → f64, -0, 1e300, surrogate pairs), pretty output, 8 error cases, enum variants, streaming
writer; postcard round-trip; base64/hex; uuid v5 against the RFC example
(886313e1-3b8a-5372-9b90-0c9aee199e5d); sha256/sha1/sha512/blake3 of "abc" against known
vectors; ed25519 sign/verify (+ tamper); zstd on real frames made by the zstd compressor
(native test only): whole frame, output limit error, `StreamingDecoder` with take(1000), and
`FrameDecoder` fed header + one block per packet exactly as `jazz_compression::stream` does.

## Not done / napkin simplifications

- No CSV lookup was needed for this unit (it has no Rust module of its own); everything in
  the header is implemented.
- `jz_btree_retain` / `append` / `from_pairs` rebuild via repeated insertion instead of
  std's bulk-push of sorted runs (same asymptotics as building from sorted input, less code).
- HashMap hashing is foldhash-like rather than std's SipHash-1-3 `RandomState` (no per-map seed).
- JSON float output finds the shortest round-trip digits by trying `%.{p}e` precisions with
  `strtod` checks instead of a Ryu/Grisu implementation (compact, uses libc already linked).
- The task queue runs tasks to completion (synchronous port, see PORTING.md).
