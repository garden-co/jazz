# Napkin port of the Jazz browser client to C: size results

Experiment, not for merge. Question: if the Rust that ships in the browser WASM
(`crates/jazz-wasm` and everything it pulls in) were hand-written C, how big would the
WASM be, and what does that say about where Rust's bytes come from?

Scope: jazz-types, groove, jazz-model, jazz-engine, jazz-protocol, jazz-node, jazz-db,
jazz-compression, jazz-wasm bindings, idb-tree, wasm-tracing. The TypeScript layer of
jazz-tools is unchanged and left out of both sides. The `jazz` facade crate ships 0 bytes.

The C compiles but never ran. It was written by 15 parallel workers, one per module group,
function by function from the Rust (same modules, data structures, algorithms, error
strings), against one shared runtime (`rt/jz_rt.h`): type-erased Vec, B-tree, hash map,
sort, postcard, JSON, Rc, interning, tasks. Real third-party C stands in for the crates:
Monocypher (ed25519), BLAKE3, zstd decompress-only, small sha256/sha1. Rules: `PORTING.md`.
Per-unit notes: `reports/`.

## Totals

| Build | raw | gzip-9 | brotli-11 |
|---|---:|---:|---:|
| **Rust, main ea554f29** (fat LTO, jazz-wasm opt "s", wasm-opt -O -g, names kept) | 16,264,536 | 5,528,763 | 3,744,823 |
| **C, clang -Oz + LTO, wasm-opt -O -g** (names kept) | **3,218,314** | **1,281,192** | **1,047,440** |
| C, -Os + LTO | 3,717,022 | 1,438,543 | 1,143,063 |
| C, -Oz without LTO | 3,281,044 | 1,295,407 | 1,066,647 |
| C, -Oz, only what the exports reach (lower bound, see caveats) | 1,888,062 | 787,552 | 657,486 |
| Rust, names stripped | 14,964,906 | 5,356,336 | 3,643,648 |
| C -Oz, names stripped | 2,774,813 | 1,175,651 | 963,071 |

**C is about 5× smaller raw, 4.3× smaller gzip, 3.6× smaller brotli.**
Sizes: 182k non-blank lines of C (.c) + 27k of headers, for ~208k non-test Rust lines
(of which ~195k ship).

Names-stripped C was measured with `wasm-opt --strip-debug` only. A second `-O` pass would inline the linker's signature-mismatch trap stubs and wrongly delete ~0.7 MB of callers; `measure.py` marks those stubs `--no-inline` for that reason.

Sections (Rust → C -Oz): code 14.49 MB → 2.53 MB, name 1.33 MB → 0.45 MB,
data 413 KB → 205 KB.

## Breakdown by crate (code-section bytes)

Rust: own-crate functions plus std generics whose name mentions that crate's types.
Most std generic bytes can't be attributed by name, so they sit in the "std" row.

| Crate | Rust | C | Rust/C |
|---|---:|---:|---:|
| jazz_node | 2,803,897 | 770,271 | 3.6× |
| groove | 2,625,083 | 679,527 | 3.9× |
| jazz_db | 1,236,361 | 328,070 | 3.8× |
| jazz_model | 1,148,996 | 230,395 | 5.0× |
| jazz_engine | 673,362 | 211,809 | 3.2× |
| jazz_protocol | 311,602 | 74,166 | 4.2× |
| bindings: jazz_wasm, idb_tree, wasm_tracing, js_sys, serde_wasm_bindgen, export shims | 359,279 | 81,475 | 4.4× |
| jazz_types | 46,593 | 19,890 | 2.3× |
| std/alloc/serde/hashbrown/... (unattributed) | ~5,100,000 | 74,309 (runtime 36.8 KB + libc 37.6 KB) | ~70× |
| crypto + compression deps | ~64,000 | 41,532 (zstd 17.8, blake3 13.1, monocypher 10.0, sha 0.6) | 1.7× |

By kind (Rust classifier on demangled names vs C names):

| Kind | Rust | C | Rust/C |
|---|---:|---:|---:|
| BTree + sort + hashbrown monomorphs | 3.36 MB | ~37 KB (one B-tree, one sort, one hash map) | ~90× |
| serde / postcard / JSON | 1.21 MB | 234 KB | 5× |
| drop glue | 640 KB | 40 KB | 16× |
| Clone | 366 KB | 59 KB | 6× |
| Eq/Ord/Hash | 245 KB | 66 KB | 4× |
| Debug/Display/fmt | 244 KB | 43 KB | 6× |
| everything else (plain fns + async bodies) | 8.40 MB | ~2.0 MB | ~4.2× |

Bytes per source line: Rust own-crate code ≈ 43 B/line (70 B/line counting all code);
C ≈ 13 B/line.

## What this teaches

1. **Generic collections are the single largest gap, and C solves it by construction.**
   3.4 MB of Rust code is BTreeMap/sort/hashbrown instantiated per key/value type. The C has
   one type-erased copy of each (~37 KB including the JSON and postcard code). Earlier
   experiments showed de-duplicating these in Rust gains little under gzip because the copies
   compress well. Here they don't exist at all, which is worth roughly 1–1.2 MB gzip.
2. **Own code is still ~3.5–4× bigger per line, across every crate.** That is the bigger
   surprise. The ratio barely depends on style: jazz_engine (no async) is 3.2×, jazz_db
   (mostly async) 3.8×, jazz_node (half async) 3.6×. So async state machines are not uniquely
   bad; they cost maybe 15–20% on top. The multiplier is everywhere: std code inlined into own
   functions (Vec growth, Option/Result moves of large enums, iterator adapters), drop glue on
   every path, and bounds and overflow panics with location data.
3. **Derived traits are cheap to write but expensive to ship.** drop glue (16×), serde (5×),
   Clone (6×) and Debug (6×) are mostly per-type generated code. C writes them once per type
   and only where they are used, often sharing helpers. jazz_model, the most serde-heavy crate,
   has the highest own-code ratio (5×).
4. **Compression narrows the gap a bit.** Rust's gzip/raw is 0.34, C's is 0.40: Rust's
   duplication is partly redundant, so the gap is 5× raw but 4.3× gzip and 3.6× brotli.
5. **The C toolchain choices matter little.** LTO on or off is ±2% and -Oz beats -Os by 11%.
   For Rust, the equivalent levers (LTO, opt-level "s", wasm-opt flags) were already worth
   tens of percent. C has less redundancy for the optimiser to remove.
6. **The C third-party stand-ins are about a wash.** zstd's C decoder is ~2× ruzstd and
   Monocypher is smaller than dalek. libc (printf, dlmalloc, strtod) is 38 KB.

Taken together, a Rust build only gets near the C number by removing per-type
instantiation: type-erased containers, `dyn` instead of generics in hot generic paths,
hand-written or table-driven codecs instead of serde derives, and fewer types needing drop
glue. Each of those is also a per-call cost in Rust that C pays anyway.

## Caveats

- **Compile-only.** Nothing ran, and fidelity was checked only by line counts and spot reads.
  Worker reports list what each skipped: test-only code, server-only `runtime` feature code,
  and functions absent from the shipped-function list (`/mnt/project-files/code-size-loop/browser-coverage.csv`).
- **Every ported function is forced alive.** Workers wrote cross-unit calls by a naming
  convention, but 1,853 names didn't match exactly, and about 1,600 calls have mismatched
  prototypes. So the main build keeps every non-static function of `src/` alive through a
  per-file `used` address table (16 KB of elem section). Unresolved names become opaque stubs.
  This slightly overcounts small helpers LTO would otherwise inline away. The "only what the
  exports reach" row is the opposite bound: there, broken call edges drop real code.
- **Async became synchronous C.** A real C port would need explicit continuations at the
  true suspension points (IndexedDB, network, timers). Those are few compared to the
  in-process awaits, but this undercounts the C somewhat.
- **Plain C semantics.** No bounds checks, no RefCell borrow checks, no mutex poisoning.
  These are part of what Rust pays for.
- **Rust's numbers depend on name attribution.** Legacy mangling hides generic arguments,
  so the per-crate Rust column undercounts generics and the std row absorbs them.

## Reproduce

    # Rust baseline
    PATH=/path/to/binaryen-117/bin:$PATH dev/benchmarks/wasm-size/build.sh OUT
    python3 dev/experiments/napkin-c/build/wasm_funcs.py OUT/jazz_wasm_bg.wasm --csv rust-funcs.csv
    python3 dev/experiments/napkin-c/build/classify_rust.py rust-funcs.csv
    # C port (wasi-sdk 25, binaryen 117; paths at the top of measure.py)
    python3 dev/experiments/napkin-c/build/measure.py --opt-flag -Oz --tag oz
    python3 dev/experiments/napkin-c/build/wasm_funcs.py out/oz/napkin.wasm --csv out/oz/funcs.csv
    python3 dev/experiments/napkin-c/build/classify_c.py out/oz

`results/` has the Rust and C breakdowns, the per-function Rust list (gzipped) and a list of
same-named functions compared directly. That list is skewed, because Rust inlines callees
that C keeps separate.
