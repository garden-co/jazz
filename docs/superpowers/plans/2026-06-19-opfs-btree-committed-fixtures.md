# opfs-btree Committed Benchmark Fixtures Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the benchmark data generator crate by committing the ready-to-run `.kv/.ops` inputs consumed by the Yew harness.

**Architecture:** The harness will serve committed files from `wasm-bench/harness/public/data/`. `bench:compare` will build the harness and run the small Node launcher; there will be no data generation or copy step in the CLI path.

**Tech Stack:** Rust, Yew, Trunk, pnpm scripts, Node launcher.

---

### Task 1: Commit ready-to-run fixture data

**Files:**

- Add: `crates/opfs-btree/wasm-bench/harness/public/data/objects.kv`
- Add: `crates/opfs-btree/wasm-bench/harness/public/data/objects.ops`
- Add: `crates/opfs-btree/wasm-bench/harness/public/data/wikipedia.kv`
- Add: `crates/opfs-btree/wasm-bench/harness/public/data/wikipedia.ops`
- Modify: `crates/opfs-btree/wasm-bench/harness/public/data/.gitignore`

- [x] **Step 1: Generate fixtures into the public data directory**

Run `cargo run -p bench-data-tools -- --profiles all --out crates/opfs-btree/wasm-bench/harness/public/data`.

- [x] **Step 2: Stop ignoring committed fixtures**

Keep `.gitignore` for future scratch files if useful, but allow `.kv/.ops` fixture files to be tracked.

### Task 2: Remove generator plumbing

**Files:**

- Delete: `crates/opfs-btree/bench-data-tools/`
- Delete: `crates/opfs-btree/wasm-bench/prepare-harness-assets.cjs`
- Modify: `Cargo.toml`
- Modify: `crates/opfs-btree/package.json`

- [x] **Step 1: Remove the workspace member**

Remove `crates/opfs-btree/bench-data-tools` from the root workspace.

- [x] **Step 2: Simplify pnpm scripts**

Remove `bench:compare:data` and `bench:harness:assets`. Make `bench:compare` run `bench:harness:build` and `run-compare.cjs`; make `bench:compare:open` run `trunk serve`.

### Task 3: Update docs

**Files:**

- Modify: `crates/opfs-btree/wasm-bench/README.md`

- [x] **Step 1: Document committed fixtures**

State that `.kv/.ops` under `harness/public/data/` are the benchmark inputs and that `datasets/*.gz` are source provenance only.

### Task 4: Verify and commit

**Files:**

- All touched files.

- [x] **Step 1: Run stale reference check**

Run `rg -n "bench-data-tools|bench:compare:data|prepare-harness-assets|wasm-bench/bench-data" crates/opfs-btree Cargo.toml`.

- [x] **Step 2: Run formatting**

Run `pnpm format:check`.

- [x] **Step 3: Run Rust checks**

Run `cargo check -p opfs-btree --target wasm32-unknown-unknown`.

- [x] **Step 4: Run harness check**

Run the harness `cargo check` with explicit LLVM env.

- [x] **Step 5: Run one benchmark profile**

Run `pnpm --dir crates/opfs-btree run bench:compare -- --profiles objects` with explicit LLVM env.

- [x] **Step 6: Commit**

Commit message: `bench: commit opfs-btree benchmark fixtures`
