# CI Pipeline — TODO

Get a GitHub Actions pipeline that catches broken builds and test failures before they land on `main`.

## What Exists

Two Claude Code workflows (`claude.yml`, `claude-code-review.yml`) for interactive help and PR review. No build, test, or lint automation — everything runs locally via `pnpm build` / `pnpm test`.

## PR Validation Workflow

Single workflow, triggers on PRs targeting `main`. Single job — everything runs through unified pnpm scripts.

### Prep: Unify lint/format scripts

Add `lint` and `format:check` scripts to `crates/package.json`:

```json
"lint": "cargo clippy --workspace -- -D warnings",
"format:check": "cargo fmt --check"
```

Update root `package.json` so `pnpm lint` and `pnpm format:check` cover both Rust and JS/TS:

```json
"format:check": "oxfmt --check . && turbo run format:check",
"lint": "oxlint . && turbo run lint"
```

Update `lefthook.yml` to use `--workspace` instead of single-crate paths:

```yaml
rustfmt:
  run: cargo fmt --check
clippy:
  run: cargo clippy --workspace -- -D warnings
```

### CI Steps

Single job. Everything runs through pnpm. We can split into separate jobs later if needed, but for now the full chain is fast enough on a single runner and keeps things simple.

```
pnpm install --frozen-lockfile
pnpm format:check       # oxfmt (JS/TS) + cargo fmt (Rust)
pnpm lint               # oxlint (JS/TS) + cargo clippy (Rust)
pnpm build              # turbo: cargo build + wasm-pack + tsc + napi
pnpm test               # turbo: cargo test + vitest (node + browser)
```

`--frozen-lockfile` ensures the lockfile is committed and up to date. `pnpm build` handles the full dependency chain (rust → WASM/NAPI → TS packages → examples) via turbo.

### Runner & Toolchain

- `blacksmith-4vcpu-ubuntu`, 15 min timeout. We can scale up to more powerful runners later if needed
- Rust: stable toolchain + `wasm32-unknown-unknown` target
- Node: match `.nvmrc` or pin to current LTS
- pnpm: `10.14.0` (matches `packageManager` field)

### Caching

- **Cargo**: cache `~/.cargo/registry` + `target/` keyed on `Cargo.lock` hash
- **pnpm**: cache pnpm store keyed on `pnpm-lock.yaml` hash
- **Turbo**: remote cache not needed yet; local turbo cache within the run is sufficient

### Branch Protection

Enable GitHub branch protection on `main`:

- Do not require CI status checks to pass before merge, we are iterating fast and can fix broken builds quickly. (We can enable this later once the pipeline is stable.)
- Require PR (no direct pushes)

## Non-Goals (This Week)

- Cross-platform builds (macOS, Windows) — we only need Linux CI for now
- NAPI prebuilt binaries for distribution — dev builds on contributors' machines
- Browser E2E tests in CI — see section below
- Benchmark regression tracking — see `benchmarks_and_performance.md` open questions
- Deploy pipeline — see `multi_tenant_sync_server.md`

## Browser E2E Tests

24 browser E2E tests exist across 3 suites (core worker bridge, React todo, TS todo). They run via `pnpm test` — vitest browser mode + Playwright headless Chromium + real WASM + real OPFS.

These are included in `pnpm test` so they'll run in the TS job. Extra CI setup needed:

- `npx playwright install chromium` before test step
- Jazz CLI binary must be built (handled by rust job)
- Each suite spawns its own server on unique ports (19876–19878), no conflicts

If browser tests prove flaky in CI (OPFS timing, Chromium startup), we can split them into a separate optional job later. Start by running everything together.
