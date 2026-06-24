# opfs-btree vs SQLite benchmark

Raw key/value benchmark comparing `opfs-btree` against **SQLite** on real,
openly-licensed data — with **both engines compiled to wasm and driven from
Rust, in-process**, persisting to OPFS. There is no JavaScript in either query
loop, so the comparison measures the storage engines, not calling convention.

- **opfs-btree** — the Rust engine itself, wrapped as a `BenchEngine` in
  `src/btree_engine.rs`, compiled into a Rust `gloo-worker`.
- **SQLite** — `rusqlite` linked against [`sqlite-wasm-rs`](https://github.com/Spxg/sqlite-wasm-rs)
  with the [`sqlite-wasm-vfs`](https://crates.io/crates/sqlite-wasm-vfs)
  **sahpool OPFS VFS** from the nested `sqlite/` package, wrapped as a
  `BenchEngine`, compiled into a separate Rust `gloo-worker`.

Both engines implement one small trait (`BenchEngine`) from the shared
`dev/benchmarks/storage/bench-core` crate; the workload, phase semantics,
timing, and checksum all live there once (see [Workload](#workload)), so each
engine file is just the trait impl.

The primary harness is the Yew/Trunk app in this directory. It spawns two Rust
workers, one per engine, over OPFS (sync access handles only exist in a Worker).
A small Node launcher serves the built harness in headless Chromium, waits for
the Yew app's automation result, and prints the table. The Yew app asserts both
engines produce **identical checksums** before exposing successful results.

## Workload

The benchmarks are **declared in code** in
`dev/benchmarks/storage/bench-core/src/benchmarks.rs`: each profile lists its
`.kv` data fixture, a fixed RNG seed, and an ordered list of phases (`Load`,
`GetRandom`, `Mixed { get, put, del }`, `ColdGetRandom`, …). At runtime each
phase expands to a deterministic operation stream from the seed, so both engines
replay byte-for-byte identical operations — which is what makes the cross-engine
checksum comparison meaningful. To add or tune a benchmark, edit
`benchmarks.rs`; no fixtures to regenerate.

Each phase's _semantics_ (what `get_skewed` or `mixed_70_20_10` actually does)
are written once in `dev/benchmarks/storage/bench-core/src/phases.rs`.
`bench-core` is unit-tested on native against an in-memory engine
(`cargo test -p jazz-storage-bench-core`).

## Datasets

The real key/value data is committed as ready-to-run `.kv` fixtures under
`dev/benchmarks/storage/data/` — no download or network access is needed to run
the benchmark. The fixtures were derived from the public sources below; the raw
source data is not vendored in the tree. (The synthetic operation streams used
to be committed as `.ops` files; they now live in `benchmarks.rs`.)

| Profile     | Data fixture   | Source                                                         | License                     |
| ----------- | -------------- | -------------------------------------------------------------- | --------------------------- |
| `objects`   | `objects.kv`   | [The Met Open Access](https://github.com/metmuseum/openaccess) | **CC0 1.0** (public domain) |
| `wikipedia` | `wikipedia.kv` | [Wikipedia](https://en.wikipedia.org/) article wikitext        | **CC BY-SA 4.0**            |

`objects` = Met museum-object metadata (medium structured records, ~900 B).
`wikipedia` = real article wikitext (large text values), exercising the
large-value path. The Met data is CC0 (no obligation); the Wikipedia text is
CC BY-SA 4.0 (© Wikipedia contributors, share-alike).

### Re-downloading the source data

`objects` — the Met's full object catalogue is a single git-LFS CSV in the
openaccess repo:

```bash
curl -L https://media.githubusercontent.com/media/metmuseum/openaccess/master/MetObjects.csv -o MetObjects.csv
```

`wikipedia` — article wikitext comes from the MediaWiki API (`prop=revisions`,
`rvprop=content`), one record per article. Example for a single title:

```bash
curl -G https://en.wikipedia.org/w/api.php \
  --data-urlencode action=query \
  --data-urlencode format=json \
  --data-urlencode prop=revisions \
  --data-urlencode rvslots=main \
  --data-urlencode rvprop=content \
  --data-urlencode titles='John Wyndham'
```

The committed `.kv` fixtures are derived from these sources (CSV rows /
`{title, text}` JSONL → key/value records); the exact record selection used to
build them is not scripted in the tree.

## Prerequisites

- Rust with the `wasm32-unknown-unknown` target.
- **A clang with the WebAssembly backend** to compile SQLite's C amalgamation —
  Apple's clang lacks it, so install Homebrew LLVM: `brew install llvm`.
- `trunk` for the Yew harness.
- `wasm-bindgen` CLI matching the crate: `cargo install wasm-bindgen-cli --version 0.2.125`.
- Playwright's Chromium (`pnpm --dir crates/opfs-btree exec playwright install chromium`).

The package scripts run through `with-wasm-llvm.sh`, which discovers
`brew --prefix llvm` or `brew --prefix llvm@20` and sets Cargo's wasm C
toolchain environment before invoking Trunk. Without Homebrew LLVM installed,
SQLite's C build will fall back to Apple's clang and fail with
`No available targets are compatible with triple "wasm32-unknown-unknown"`.

## Running

The supported path is one command:

```bash
# build the Yew harness + Rust workers, run headless Chromium,
# verify checksums against committed fixtures, and print a table
pnpm --dir crates/opfs-btree run bench:compare
```

For manual inspection:

```bash
pnpm --dir crates/opfs-btree run bench:compare:open
```

Both commands build `--release`. This matters: SQLite's C core is always
optimized (it comes prebuilt from `sqlite-wasm-rs`, independent of the Rust
profile), so a debug build compiles only opfs-btree's Rust unoptimized and makes
it look 10×+ slower than it is. Never read perf numbers from a debug build.

**Runner flags:** `--profiles objects,wikipedia` (default both), `--json` (raw
results instead of the table). Example:

```bash
pnpm --dir crates/opfs-btree run bench:compare -- --profiles objects
```

Build output lives under `wasm-bench/dist/` and `wasm-bench/target/`. These are
ignored and safe to delete. The `dev/benchmarks/storage/data/*.kv` files are
committed benchmark inputs and should stay in the tree.

## Interpreting output

A table per (profile, phase): `btree_ms`/`sqlite_ms` (wall-clock) and
`btree_ops_s`/`sqlite_ops_s`. The process exits non-zero if any profile's
checksums diverge — a clean run means both engines did identical, verified work.

Single-run, in-browser numbers — directional, not statistically rigorous (no
warmup/averaging yet, expect ±~20%). Typical shape (against the page-size-tuned
SQLite baseline): **reads are roughly a tie** — SQLite can even edge ahead on a
few scan/cold phases — while **opfs-btree leads on writes** (bulk load ~2–9×,
random updates and mixed ~6–13×). One subtlety that matters: SQLite read phases
are wrapped in a transaction — without it, per-statement autocommit dominates and
makes reads look ~100× slower than they are.

## Configuration parity

Both engines use a 32 MB cache and a **16 KB page size**. SQLite: WAL,
`synchronous=NORMAL`, `temp_store=MEMORY`, a `WITHOUT ROWID` k/v table,
reads/writes batched per phase in one transaction. opfs-btree checkpoints after
each write phase.

The SQLite page size is set to match opfs-btree's: the sahpool OPFS VFS has high
per-I/O-call overhead, so matching the 16 KB page granularity (rather than
SQLite's 4 KB default) roughly halves its total time here — the fair baseline to
compare against. `locking_mode=EXCLUSIVE` was measured and made it _slower_, so
it is deliberately not used.
