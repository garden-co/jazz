# opfs-btree Benchmark Cleanup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce `crates/opfs-btree` benchmark clutter to one supported Yew + Rust worker comparison harness.

**Architecture:** Keep the dataset-driven benchmark path: `bench-data-tools` generates `.kv/.ops`, the Yew/Trunk harness runs two Rust workers, and the small Node launcher serves and prints results. Delete the old standalone `wasm-pack` OPFS microbench runner and local generated artifacts.

**Tech Stack:** Rust, Yew, gloo-worker, Trunk, pnpm scripts, small Node launchers.

---

### Task 1: Remove obsolete runner path

**Files:**

- Modify: `crates/opfs-btree/package.json`
- Delete: `crates/opfs-btree/wasm-bench/run-opfs-bench.cjs`
- Delete: `crates/opfs-btree/wasm-bench/build-sqlite-wasm.sh`

- [x] **Step 1: Remove old package scripts**

Keep only the data generation, harness build, compare, and open scripts. Remove `bench:wasm:*` scripts and the `wasm-pack` dev dependency.

- [x] **Step 2: Delete old standalone runners**

Remove `run-opfs-bench.cjs` and the untracked `build-sqlite-wasm.sh`; the Yew harness compiles both Rust workers through Trunk.

- [x] **Step 3: Check references**

Run: `rg -n "run-opfs-bench|bench:wasm|wasm-pack|build-sqlite-wasm|pkg-sqlite" crates/opfs-btree`

Expected: no source references remain except stale docs that are updated in Task 4.

### Task 2: Trim wasm benchmark Rust module

**Files:**

- Modify: `crates/opfs-btree/src/wasm_bench.rs`

- [x] **Step 1: Keep dataset replay API**

Keep `DatasetPhaseResult`, `DatasetRunResult`, `run_dataset_result`, `replay_phase`, and the `bench_dataset_run` wasm export.

- [x] **Step 2: Remove old microbench exports**

Remove the old `bench_opfs_*` microbench functions, cache tuning exports, synthetic key/value helpers, RNG, mixed scenario definitions, and result types used only by `run-opfs-bench.cjs`.

- [x] **Step 3: Format and check**

Run: `cargo fmt --check`

Expected: formatting passes after `cargo fmt` if needed.

### Task 3: Prune local generated output

**Files/Dirs:**

- Delete ignored local dirs under `crates/opfs-btree`: `bench-sqlite/target`, `wasm-bench/harness/target`, `wasm-bench/harness/dist`, `wasm-bench/bench-data`, `wasm-bench/pkg`, `wasm-bench/pkg-sqlite`, `node_modules`, and empty nested `crates/`.
- Delete generated files under `crates/opfs-btree/wasm-bench/harness/public/data/`, keeping `.gitignore`.

- [x] **Step 1: Remove ignored generated outputs**

Use `rm -rf` only for ignored/generated paths listed above.

- [x] **Step 2: Confirm status is source-only**

Run: `git status --ignored --short crates/opfs-btree`

Expected: no ignored generated benchmark outputs remain under `crates/opfs-btree`.

### Task 4: Update benchmark docs

**Files:**

- Modify: `crates/opfs-btree/wasm-bench/README.md`
- Modify or delete: `crates/opfs-btree/BENCHMARK_OVERVIEW.md`

- [x] **Step 1: Update README prerequisites and running docs**

Document only the supported single command: `pnpm --dir crates/opfs-btree run bench:compare`. Keep the LLVM prerequisite as an external requirement for SQLite's C compilation.

- [x] **Step 2: Remove stale overview content**

Delete or replace `BENCHMARK_OVERVIEW.md` if it mainly documents the removed old runner path.

### Task 5: Verify and commit

**Files:**

- All touched cleanup files.

- [x] **Step 1: Run reference checks**

Run: `rg -n "run-opfs-bench|bench:wasm|wasm-pack|build-sqlite-wasm|pkg-sqlite" crates/opfs-btree`

Expected: no stale references.

- [x] **Step 2: Run formatting**

Run: `pnpm format:check`

Expected: passes.

- [x] **Step 3: Run Rust wasm checks**

Run: `cargo check -p opfs-btree --target wasm32-unknown-unknown`

Expected: passes.

- [x] **Step 4: Run harness check with LLVM env if available**

Run: `cargo check --manifest-path crates/opfs-btree/wasm-bench/harness/Cargo.toml --target wasm32-unknown-unknown`

Expected: passes when `CC_wasm32_unknown_unknown` and `AR_wasm32_unknown_unknown` point to a clang/llvm-ar with wasm32 support.

- [x] **Step 5: Commit**

Commit message: `bench: simplify opfs-btree benchmark harness`
