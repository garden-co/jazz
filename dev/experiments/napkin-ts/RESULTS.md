# Napkin port of the browser client to TypeScript

Question (Anselm, 2026-10-15): if everything the browser client gets from Rust/WASM had been written in TypeScript,
how big would the client be, where would the bytes go, and what does that teach us about where Rust's bytes come from?

Code: branch `claude/project-thread-5n1xz0` of garden-co/jazz, directory `dev/experiments/napkin-ts/`
(`npm install && npm run typecheck && node measure.mjs`). Not for merge.

## Headline

|                                                                                          |         raw |     gzip-9 |  brotli-11 |
| ---------------------------------------------------------------------------------------- | ----------: | ---------: | ---------: |
| Browser WASM on main `ea554f29` (fat LTO, jazz-wasm opt-level "s", wasm-opt, names kept) |    16.26 MB |    5.53 MB |    3.74 MB |
| **TypeScript port of the same crates** (esbuild, minified, ES2022, tree-shaken)          | **2.13 MB** | **619 KB** | **500 KB** |
| Ratio                                                                                    |        7.6× |       8.9× |       7.5× |

About 29 KB raw of the TS figure is measurement harness (the keep-alive list in the entry, plus placeholder bindings at
partition boundaries). Without tree-shaking at all the port is 2.10 MB raw / 606 KB gzip, so almost nothing in it is dead.

Whole client, gzip (the existing jazz-tools TS layer bundled from main source for the todo-client-localfirst-ts example,
measured separately for the page bundle and the broker worker):

|         | page JS | worker JS |       engine |    total |
| ------- | ------: | --------: | -----------: | -------: |
| Today   |  169 KB |     96 KB | 5.53 MB WASM | ≈5.79 MB |
| TS-only |  169 KB |     96 KB |   0.62 MB JS | ≈0.88 MB |

The TS layer was kept as is, as briefed. In a real TS-only client, part of it (wasm loading, binding codecs, worker
marshalling) would go away, so 0.88 MB is a slight overestimate.

## Scope and method

- Ported: every non-test line of the crates that are compiled into the browser WASM: jazz-types, jazz-protocol,
  jazz-compression (decode only, as in the browser build), groove, jazz-model, jazz-engine, jazz-node, jazz-db, the
  jazz-wasm bindings, idb-tree and wasm-tracing. That is ≈194k non-test Rust lines. (The `jazz` crate's tools/serving
  modules don't reach the WASM; `oracle.rs` and `delivery_diagnostics.rs` are test-only.)
- 18 parallel workers each took ≈7–14k Rust lines, mirrored the module tree, and kept data structures, algorithms,
  control flow and every error/format string verbatim. They dropped derives, lifetimes, `Rc<RefCell>`, `Box::pin` and
  `Result` plumbing. Rules: `dev/experiments/napkin-ts/CONVENTIONS.md`.
- Result: 283 files, 111k TS lines excluding `support/` and the boundary placeholders (57% of the Rust line count). It typechecks with 0 errors, and no file is stubbed
  (per-file manifests are in `manifests/`). It has never run.
- Deps with a well-known JS equivalent were used and counted: @noble/hashes and @noble/curves (blake3, sha2, sha1,
  ed25519) and fzstd. Everything else that Rust gets from std, serde or postcard comes from the JS engine (Map, Array
  sort, JSON, Promise), plus a 9 KB `support/` module (sorted map, postcard codec, channels).
- Rust attribution: per-function code-section body sizes from the name section of the same build. Each function is
  bucketed by its crate path, and std generics are split into sort, BTree and other (other is attributed to an own
  crate when one appears in its type arguments). gzip per bucket = gzip of that bucket's bodies on their own.
  Data: `rust-wasm-buckets.csv`, `ts-bundle-report.json`.

## Breakdown, side by side

Per bucket, gzip is standalone (each bucket compressed on its own), so the columns sum to more than the whole-file numbers.

| Area                                                            | Rust lines |       WASM raw |     WASM gz | TS lines |                                              JS min |               JS gz | raw ratio |
| --------------------------------------------------------------- | ---------: | -------------: | ----------: | -------: | --------------------------------------------------: | ------------------: | --------: |
| groove (IVM, records, db, storage)                              |      57.0k |       2,598 KB |      902 KB |    31.7k |                                              559 KB |              167 KB |      4.6× |
| jazz-node                                                       |      63.9k |       2,780 KB |    1,144 KB |    34.3k |                                              700 KB |              210 KB |      4.0× |
| jazz-db                                                         |      28.2k |       1,242 KB |      525 KB |    17.0k |                                              311 KB |               92 KB |      4.0× |
| jazz-model                                                      |      16.6k |       1,136 KB |      325 KB |     9.6k |                                              187 KB |               59 KB |      6.1× |
| jazz-engine                                                     |      13.8k |         661 KB |      236 KB |     9.1k |                                              129 KB |               40 KB |      5.1× |
| jazz-protocol + compression                                     |       6.8k |         311 KB |      115 KB |     4.9k |                                               87 KB |               34 KB |      3.6× |
| jazz-types                                                      |       2.3k |          38 KB |       15 KB |     1.3k |                                               26 KB |                9 KB |      1.5× |
| jazz-wasm bindings (+ js-sys, wasm-bindgen, serde-wasm-bindgen) |       3.1k |         199 KB |       81 KB |     1.4k |                                               22 KB |                9 KB |        9× |
| idb-tree                                                        |       2.0k |          50 KB |       23 KB |     1.3k |                                               25 KB |                9 KB |      2.0× |
| wasm-tracing                                                    |       0.5k |          23 KB |       11 KB |     0.2k |                                                3 KB |                1 KB |        7× |
| **Own code subtotal**                                           |   **194k** |    **≈9.0 MB** | **≈3.4 MB** | **111k** |                                        **≈2.05 MB** |         **≈630 KB** |  **4.4×** |
| std BTreeMap/BTreeSet monomorphs                                |            |       2,105 KB |      490 KB |          |                       (engine `Map` + 9 KB support) |                     |           |
| std slice sort monomorphs                                       |            |       1,231 KB |      271 KB |          |                     (engine `Array.prototype.sort`) |                     |           |
| other std (Vec, fmt, hashbrown, …)                              |            |       1,158 KB |      394 KB |          |                                            (engine) |                     |           |
| serde, serde_json, postcard, `Debug` fmt, export shims          |            |         791 KB |      224 KB |          |             (codecs are counted in the areas above) |                     |           |
| crypto, hashing, uuid                                           |            |          73 KB |       28 KB |          |                                       42 KB (noble) |                     |           |
| allocator, futures, smallvec, interning                         |            |          58 KB |       21 KB |          |                                      9 KB (support) | 17 KB gz with noble |           |
| name section / data section                                     |            | 1,326 / 413 KB |             |          | (property names stay in the JS; locals are mangled) |                     |           |

Rust line counts are non-test, non-comment lines. jazz-node and groove each include their share of the
per-crate "std other" generics.

## What this teaches about Rust's bytes

1. **A third of the WASM is library code that a JS engine already ships.** BTree (2.1 MB), sort (1.2 MB), other std
   generics (1.2 MB) and serde/fmt (0.8 MB) add up to about 5.3 MB raw / ~1.4 MB gzip. That is more than the entire TS
   port. The cost is monomorphisation: one copy per (K, V) or element type, which the generic-attribution scout already
   found. JS pays for this once, inside the engine.
2. **Own code is ~4.4× bigger as WASM than as minified JS for the same logic.** That is about 46 bytes of WASM per Rust
   line against about 11 bytes of JS. Line count explains little of the gap: TS needed 57% of the lines, and once the
   ceremony goes, the gap is still about 2.5–3× per line of logic. Likely causes, all of which the earlier scouts saw:
   - inlining and async state machines copied per caller (the 4.2 MB of poll bodies);
   - drop glue, `Result`/panic paths and bounds checks that JS doesn't spell out;
   - hand-inlined container operations, where JS calls `map.get()` and the engine does the work.

   The highest multipliers are in jazz-model (6.1×) and jazz-engine (5.1×), which are generic- and
   enum-heavy. The lowest are byte-level code (types 1.5×, idb-tree 2.0×), where both languages do the same arithmetic.

3. **Source-level ceremony is real but cheap in bytes.** The workers cut 40% of the lines (derives, `Rc<RefCell>`
   plumbing, `map_err` chains, `Box::pin` stack boxing, forwarding wrappers, repeated `ok_or(Error::…)` blocks). Most of
   that compiles to little WASM. This matches the own-code loop's experience: line deletions buy few gzip bytes.
4. **The bindings boundary costs 9×.** jazz-wasm, js-sys, wasm-bindgen and serde-wasm-bindgen come to 199 KB of
   marshalling with no counterpart in a TS-only client.
5. **For Rust reductions, the levers are multipliers in code generation, not source size.** These are the number of
   generic instantiations (BTree K,V pairs, sort element types), the inlining and async-body duplication, and serde
   derive volume. Even a perfect line-for-line cleanup of own code leaves the ≈5 MB std/serde layer and the ≈4×
   codegen factor in place. The C port running alongside this one will show how much of the factor is Rust
   (monomorphisation, async lowering) and how much is WASM itself.

## Caveats

- Typechecked, never run. "full" in the manifests is each worker's own judgement. A spot check reads as real logic,
  but fidelity is unverified, and calls across partition boundaries go through ≈1,600 loosely typed placeholders.
- Some of the TS shrink is deduplication a Rust author could also do (repeated blocks in read_sources.rs,
  recursion.rs, projections.rs). It is a small part of the byte gap.
- Size only. JS parse/compile time for ~2 MB and runtime speed (IVM and codec hot paths in JS, `bigint` for 64-bit
  hashes and keys) were not measured, and they are where a TS engine would pay.
- The Rust WASM keeps its name section (1.33 MB raw, ≈274 KB gzip) by choice. The JS keeps property and method names
  and mangles locals, so neither side is fully stripped.
- TS-layer numbers come from bundling main's jazz-tools source with esbuild (stubbing the two generated fingerprint
  modules; `node:*` imports left external). They are not the published dist.
