# opfs-btree vs SQLite benchmark

Raw key/value benchmark comparing `opfs-btree` against **SQLite** on real,
openly-licensed data — with **both engines compiled to wasm and driven from
Rust, in-process**, persisting to OPFS. There is no JavaScript in either query
loop, so the comparison measures the storage engines, not calling convention.

- **opfs-btree** — the Rust engine itself (`run_dataset_result` in
  `src/wasm_bench.rs`), compiled into a Rust `gloo-worker`.
- **SQLite** — `rusqlite` linked against [`sqlite-wasm-rs`](https://github.com/Spxg/sqlite-wasm-rs)
  with the [`sqlite-wasm-vfs`](https://crates.io/crates/sqlite-wasm-vfs)
  **sahpool OPFS VFS**, compiled into a separate Rust `gloo-worker`
  (`crates/opfs-btree/bench-sqlite/`).

The primary harness is a Yew/Trunk app under `wasm-bench/harness/`. It spawns
two Rust workers, one per engine, over OPFS (sync access handles only exist in a
Worker). A small Node launcher serves the built harness in headless Chromium,
waits for the Yew app's automation result, and prints the table. The Yew app
asserts both engines produce **identical checksums** before exposing successful
results.

## Datasets

Two datasets are **vendored (committed)** under `datasets/` as gzipped files — no
download or network access is needed.

| Profile     | Vendored file        | Source                                                         | License                     |
| ----------- | -------------------- | -------------------------------------------------------------- | --------------------------- |
| `objects`   | `objects.csv.gz`     | [The Met Open Access](https://github.com/metmuseum/openaccess) | **CC0 1.0** (public domain) |
| `wikipedia` | `wikipedia.jsonl.gz` | [Wikipedia](https://en.wikipedia.org/) article wikitext        | **CC BY-SA 4.0**            |

`objects` = Met museum-object metadata (medium structured records, ~900 B).
`wikipedia` = real article wikitext (large text values), exercising the
large-value path. The Met data is CC0 (no obligation); the Wikipedia text is
CC BY-SA 4.0 (© Wikipedia contributors, share-alike — kept isolated in its gz).

## Prerequisites

- Rust with the `wasm32-unknown-unknown` target.
- **A clang with the WebAssembly backend** to compile SQLite's C amalgamation —
  Apple's clang lacks it, so install Homebrew LLVM: `brew install llvm`.
- `trunk` for the Yew harness.
- `wasm-bindgen` CLI matching the crate: `cargo install wasm-bindgen-cli --version 0.2.125`.
- Playwright's Chromium (`pnpm --dir crates/opfs-btree exec playwright install chromium`).

Set the wasm C toolchain environment before building the harness or running the
benchmark. Without it, Cargo can fall back to Apple's clang and fail with
`No available targets are compatible with triple "wasm32-unknown-unknown"`.

```bash
LLVM_PREFIX=""
for prefix in "$(brew --prefix llvm 2>/dev/null)" "$(brew --prefix llvm@20 2>/dev/null)"; do
  if [ -x "$prefix/bin/clang" ] && "$prefix/bin/clang" --print-targets | grep -qi wasm32; then
    LLVM_PREFIX="$prefix"
    break
  fi
done
test -n "$LLVM_PREFIX" || { echo "install Homebrew LLVM: brew install llvm"; exit 1; }

CC_wasm32_unknown_unknown="$LLVM_PREFIX/bin/clang" \
AR_wasm32_unknown_unknown="$LLVM_PREFIX/bin/llvm-ar" \
CFLAGS_wasm32_unknown_unknown="-O3 -DSQLITE_THREADSAFE=0" \
pnpm --dir crates/opfs-btree run bench:compare
```

## Running

With the environment above configured, the supported path is one command:

```bash
# normalize data, build the Yew harness + Rust workers, run headless Chromium,
# verify checksums, and print a table
pnpm --dir crates/opfs-btree run bench:compare
```

For manual inspection:

```bash
pnpm --dir crates/opfs-btree run bench:compare:open
```

**Runner flags:** `--profiles objects,wikipedia` (default both), `--json` (raw
results instead of the table). Example:

```bash
pnpm --dir crates/opfs-btree run bench:compare -- --profiles objects
```

Generated benchmark data lives under `wasm-bench/bench-data/`, copied harness
data under `wasm-bench/harness/public/data/`, and build output under
`wasm-bench/harness/dist/` and `wasm-bench/harness/target/`. These are ignored
and safe to delete.

## Interpreting output

A table per (profile, phase): `btree_ms`/`sqlite_ms` (wall-clock) and
`btree_ops_s`/`sqlite_ops_s`. The process exits non-zero if any profile's
checksums diverge — a clean run means both engines did identical, verified work.

Single-run, in-browser numbers — directional, not statistically rigorous (no
warmup/averaging yet, expect ±~20%). Typical shape: **reads are roughly a tie**
(opfs-btree ~1.5×), while **opfs-btree leads on writes** (~3× bulk load, ~15–25×
random updates). One subtlety that matters: SQLite read phases are wrapped in a
transaction — without it, per-statement autocommit dominates and makes reads look
~100× slower than they are.

## Configuration parity

Both engines use a 32 MB cache. SQLite: WAL, `synchronous=NORMAL`, a
`WITHOUT ROWID` k/v table, reads/writes batched per phase in one transaction.
opfs-btree checkpoints after each write phase.
