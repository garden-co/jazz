# CI Build Speedups — TODO (This Week)

Reduce PR CI wall time with low-risk build pipeline changes before larger refactors.

## Motivation

Current CI is stable and mostly warm-cache friendly, but still spends significant time in build steps that do duplicate work across Rust/WASM/Node task boundaries.

## This Week Scope

### 1. De-duplicate Rust/WASM work in `pnpm build`

- Current root build script runs:
  - `turbo run build:crates`
  - then `turbo run build`
- This causes `groove-wasm` and `jazz-napi` build work to run again in the second turbo pass.
- Change build orchestration so crates work runs once and the second pass skips duplicate package builds.

Expected impact:
- Lower `pnpm build` step time.
- Lower total CI wall time with no coverage regression.

### 2. Fix Turbo caching for `@jazz/rust#build:crates`

- Current logs show: `no output files found for task @jazz/rust#build:crates`.
- Root cause: task outputs are declared relative to `crates/`, but command writes to repo-root `target/` and `crates/groove-wasm/pkg/`.
- Update `build:crates` output globs to match actual produced paths.

Expected impact:
- Restores meaningful Turbo task caching metadata for the expensive crates aggregation task.

### 3. Narrow `build:crates` scope

- Replace broad `cargo build --workspace` with minimum required crate set for CI build/test prerequisites.
- Validate required binaries/artifacts (`jazz` CLI, wasm package) are still produced where downstream tasks expect them.

Expected impact:
- Less Rust compile work on cache misses and partial invalidations.

### 4. Use non-release N-API build in PR CI

- Evaluate switching `jazz-napi` CI validation path from release build to debug build for PR workflows.
- Keep release build coverage in release/publish workflow.

Expected impact:
- Faster PR builds while preserving release correctness checks where they matter.

### 5. Evaluate `sccache` + sticky disk for Rust object reuse

- Add a short spike to benchmark `sccache` with Blacksmith StickyDisk for `~/.cache/sccache`.
- Compare against current `Swatinem/rust-cache` only setup.

Expected impact:
- Additional Rust rebuild speedup when cache keys partially invalidate.

## Execution Order

1. Implement items 1 and 2 first.
2. Run timing comparison on CI.
3. If gains are meaningful, continue with items 3–5.
