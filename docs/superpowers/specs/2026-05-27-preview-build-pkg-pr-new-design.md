# Preview-build pipeline via pkg.pr.new

Status: approved, in implementation.

## Goal

When a maintainer adds the `preview-build` label to a pull request, build the
full set of Jazz JavaScript packages (with all native binaries) and publish
them to [pkg.pr.new](https://pkg.pr.new) so reviewers can `npm i` them into a
real consumer app using a URL keyed by the PR's commit SHA.

The label is the only trigger: no version bumps, no npm publish, no PR
synchronize re-builds (re-add the label to refresh).

## Non-goals

- Publishing to the real npm registry from a PR (the alpha pipeline does this
  from `changeset-release/*` only).
- Auto-running preview builds on every PR or every push (label opt-in keeps
  CI cost off the default path).
- Re-architecting the changeset/alpha flow itself — only the build steps are
  factored out so the new preview path can reuse them.

## Decisions captured from brainstorming

- **Shape:** composite action `setup-build` + label-gated workflow
  `preview-build.yml` + a shared reusable workflow `build-jazz-packages.yml`
  extracted from `publish-jazz-tools-alpha.yml`.
- **Package set:** full parity with the alpha pipeline — `jazz-tools`,
  `jazz-wasm`, `jazz-napi` (loader + 4 platform packages), `jazz-rn`,
  `create-jazz`.
- **Publishing backend:** [pkg.pr.new](https://pkg.pr.new). No version
  bumping; pkg.pr.new keys by commit SHA and auto-comments install URLs on
  the PR.
- **Build-pipeline reuse:** extract a reusable workflow so the alpha publish
  and preview build share the build matrix. Cleaner than duplicating ~600
  lines, accepted refactor cost on the existing 1262-line workflow.
- **Trigger semantics:** `pull_request: types: [labeled]` only, gated on
  `github.event.label.name == 'preview-build'`. Single build per label add.
- **Verify-packed steps:** alpha only. Preview builds skip the packed-manifest
  / workspace-protocol checks for speed.

## Architecture

```
.github/
├── actions/
│   └── setup-build/                 ← NEW composite action
│       └── action.yml
└── workflows/
    ├── build-jazz-packages.yml      ← NEW reusable workflow
    ├── preview-build.yml            ← NEW label-gated workflow
    └── publish-jazz-tools-alpha.yml ← REFACTORED to call the reusable workflow
```

Flow on `preview-build` label add:

```
PR labeled "preview-build"
        │
        ▼
preview-build.yml      (gate: github.event.label.name == 'preview-build')
        │ uses
        ▼
build-jazz-packages.yml   (build-linux | build-macos | build-wasm
                           | build-napi | build-rn-ios | build-rn-android
                           → assemble-packages)
        │ uploads pkg-{jazz-tools,jazz-wasm,jazz-napi,jazz-rn,create-jazz}
        ▼
publish-pkg-pr-new job    (downloads pkg-* artifacts; runs `npx pkg-pr-new
                           publish ...`)
        │
        ▼
pkg.pr.new bot comments install URLs on the PR (`--comment=update` so
successive label adds update the same comment).
```

The alpha publish flow keeps its existing release gate and artifact-reuse
logic; only its inline build jobs are replaced with a `uses:` of the new
reusable workflow.

## Composite action: `.github/actions/setup-build/action.yml`

Consolidates the "Rust toolchain + Node/pnpm + caches" stanza repeated across
every build job.

### Inputs

| input             | type   | default                                                            | what it controls                                                         |
| ----------------- | ------ | ------------------------------------------------------------------ | ------------------------------------------------------------------------ |
| `rust-toolchain`  | string | `1.93.1`                                                           | passed to `dtolnay/rust-toolchain`                                       |
| `rust-targets`    | string | `''`                                                               | comma-separated targets; skipped if empty                                |
| `rust-components` | string | `''`                                                               | comma-separated components; skipped if empty                             |
| `rust-cache-key`  | string | `repo-${{ env.CACHE_SCOPE_REPOSITORY_ID }}`                        | `Swatinem/rust-cache` key                                                |
| `rust-cache-bin`  | string | `'false'`                                                          | passed through to `Swatinem/rust-cache`                                  |
| `setup-node`      | string | `'true'`                                                           | when `true`, sets up Node/pnpm and runs `pnpm install --frozen-lockfile` |
| `sccache`         | string | `'false'`                                                          | when `true`, installs sccache and mounts the Blacksmith sticky disk      |
| `sccache-key`     | string | `sccache-${{ env.CACHE_SCOPE_REPOSITORY_ID }}-${{ runner.os }}-v1` | sccache sticky-disk key                                                  |

### Steps (in order, each gated on the relevant input)

1. Write `.github-cache-scope`.
2. If `setup-node == 'true'`: corepack enable → `useblacksmith/setup-node`
   with pnpm cache → `pnpm install --frozen-lockfile`.
3. `dtolnay/rust-toolchain` with `toolchain`/`targets`/`components` from
   inputs (only if `rust-toolchain` is non-empty).
4. `Swatinem/rust-cache` (`cache-on-failure: true`, `prefix-key: rocksdb-v1`,
   `key:` from input).
5. If `sccache == 'true'`: install sccache via `taiki-e/install-action`,
   mount Blacksmith sticky disk, export `RUSTC_WRAPPER=sccache` /
   `SCCACHE_DIR` env so subsequent Rust commands pick it up.

`CACHE_SCOPE_REPOSITORY_ID` stays a workflow-level env (it's already set in
`publish-jazz-tools-alpha.yml` and `ci.yml`); the action consumes it.

This replaces `.github/actions/source-code` for the build/publish workflows;
`source-code` is deleted in the same PR. `ci.yml`'s `lint`/`test-rust`/
`test-ts` jobs migrate to `setup-build` in the same change.

## Reusable workflow: `.github/workflows/build-jazz-packages.yml`

Single source of truth for "produce every Jazz npm package, fully assembled,
ready to publish."

`on: workflow_call:` only.

### Jobs (extracted from `publish-jazz-tools-alpha.yml`)

| job                 | matrix                                            | runner                       | uploads artifact(s)                                                                  |
| ------------------- | ------------------------------------------------- | ---------------------------- | ------------------------------------------------------------------------------------ |
| `build-linux`       | x86_64-musl, aarch64-musl                         | blacksmith-4vcpu-ubuntu-2404 | `jazz-tools-linux-{x64,arm64}`                                                       |
| `build-macos`       | aarch64-apple, x86_64-apple                       | blacksmith-6vcpu-macos-15    | `jazz-tools-darwin-{arm64,x64}`                                                      |
| `build-wasm`        | –                                                 | blacksmith-4vcpu-ubuntu-2404 | `jazz-wasm-pkg`                                                                      |
| `build-napi`        | linux-x64-gnu, darwin-{x64,arm64}, win32-x64-msvc | matrix runner                | `jazz-napi-binding-<platform>` + (once) `jazz-napi-loader`                           |
| `build-rn-ios`      | –                                                 | blacksmith-6vcpu-macos-15    | `jazz-rn-ios-payload`                                                                |
| `build-rn-android`  | arm64-v8a, armeabi-v7a, x86, x86_64               | blacksmith-4vcpu-ubuntu-2404 | `jazz-rn-android-payload-<abi>`                                                      |
| `assemble-packages` | –                                                 | blacksmith-4vcpu-ubuntu-2404 | `pkg-jazz-tools`, `pkg-jazz-wasm`, `pkg-jazz-napi`, `pkg-jazz-rn`, `pkg-create-jazz` |

### `assemble-packages` job

Depends on all builds. Wraps the shared assembly logic so callers don't
duplicate it.

1. Checkout + `setup-build` (Node only, no Rust).
2. Download all `jazz-tools-*` CLI binaries → `dist/`.
3. Download `jazz-wasm-pkg` → `crates/jazz-wasm/pkg/`.
4. Download `jazz-napi-loader` + `jazz-napi-binding-*` →
   `crates/jazz-napi{,/artifacts}`.
5. Download `jazz-rn-{ios,android}-payload-*` →
   `dist/rn-{ios,android}/`.
6. Stage jazz-rn native payload into `crates/jazz-rn/` (same logic as the
   current `Stage jazz-rn native payload into npm package sources` step in
   `publish-npm`).
7. `pnpm exec napi create-npm-dirs` + `pnpm exec napi artifacts -d artifacts
--npm-dir npm` to stage jazz-napi platform packages.
8. `pnpm --filter jazz-tools build`.
9. `pnpm --filter create-jazz build`.
10. Stage CLI binaries into `packages/jazz-tools/bin/native/` with `chmod
755`.
11. Upload one artifact per package (`pkg-jazz-tools`, `pkg-jazz-wasm`,
    `pkg-jazz-napi`, `pkg-jazz-rn`, `pkg-create-jazz`) containing the full
    package directory tree ready to publish.

### Explicitly not in this reusable workflow

- Release gate (caller-specific — alpha matches `changeset-release/*`,
  preview gates on label).
- Version bumping / changeset version logic (alpha-only).
- Verify-packed-manifest steps (alpha-only per design decision).
- npm credentials or pkg.pr.new auth (caller-specific).

## Label workflow: `.github/workflows/preview-build.yml`

```yaml
name: Preview build (pkg.pr.new)

on:
  pull_request:
    types: [labeled]

concurrency:
  group: preview-build-${{ github.event.pull_request.number }}
  cancel-in-progress: true

permissions:
  contents: read
  pull-requests: write
  id-token: write

jobs:
  build:
    if: github.event.label.name == 'preview-build'
    uses: ./.github/workflows/build-jazz-packages.yml
    secrets: inherit

  publish-pkg-pr-new:
    name: Publish to pkg.pr.new
    needs: [build]
    if: github.event.label.name == 'preview-build'
    runs-on: blacksmith-4vcpu-ubuntu-2404
    permissions:
      contents: read
      pull-requests: write
      id-token: write
    steps:
      - uses: actions/checkout@de0fac2e4500dabe0009e67214ff5f5447ce83dd
        with:
          ref: ${{ github.event.pull_request.head.sha }}

      - uses: ./.github/actions/setup-build
        with:
          rust-toolchain: "" # no Rust needed in this job
          setup-node: "true"

      - uses: actions/download-artifact@634f93cb2916e3fdff6788551b99b062d0335ce0
        with:
          name: pkg-jazz-tools
          path: packages/jazz-tools
      - uses: actions/download-artifact@634f93cb2916e3fdff6788551b99b062d0335ce0
        with:
          name: pkg-jazz-wasm
          path: crates/jazz-wasm
      - uses: actions/download-artifact@634f93cb2916e3fdff6788551b99b062d0335ce0
        with:
          name: pkg-jazz-napi
          path: crates/jazz-napi
      - uses: actions/download-artifact@634f93cb2916e3fdff6788551b99b062d0335ce0
        with:
          name: pkg-jazz-rn
          path: crates/jazz-rn
      - uses: actions/download-artifact@634f93cb2916e3fdff6788551b99b062d0335ce0
        with:
          name: pkg-create-jazz
          path: packages/create-jazz

      - name: Publish to pkg.pr.new
        run: |
          npx pkg-pr-new publish \
            --pnpm \
            --comment=update \
            --packageManager=pnpm \
            './packages/jazz-tools' \
            './crates/jazz-wasm' \
            './crates/jazz-napi' \
            './crates/jazz-rn' \
            './packages/create-jazz'
```

### External one-time setup

Install the [pkg.pr.new GitHub App](https://github.com/apps/pkg-pr-new) on
the `garden-co/jazz` repo before the first label-triggered run. Without it
the publish step exits non-zero with a clear error.

### Fork PR handling

`pull_request` (not `pull_request_target`) — fork PRs run with the limited
`GITHUB_TOKEN`. pkg.pr.new uses OIDC via its GitHub App, not repo secrets,
so this works. Only users with write access to the base repo can apply
labels, so fork-PR preview builds remain maintainer-gated by design.

## Refactor of `publish-jazz-tools-alpha.yml`

- **Remove** the inline `build-linux`, `build-macos`, `build-wasm`,
  `build-napi`, `build-rn-ios`, `build-rn-android` jobs.
- **Add** a `build` job:
  ```yaml
  build:
    needs: [release-push-gate, resolve-preview-artifacts]
    if: needs.release-push-gate.outputs.should_run == 'true' && needs.resolve-preview-artifacts.outputs.use_preview_artifacts != 'true'
    uses: ./.github/workflows/build-jazz-packages.yml
    secrets: inherit
  ```
- **Simplify `publish-npm`**:
  - Replace the ~7 `download-artifact` stanzas + manual staging with one
    download per `pkg-*` artifact into the corresponding package directory.
  - `needs:` becomes `[release-push-gate, resolve-preview-artifacts, build]`.
  - Keep all version-bump steps, alpha-guard steps, all `Verify packed *`
    steps, all `Publish *` steps.
  - Net delete: roughly 150 lines.
- **Update `resolve-preview-artifacts`**'s `requiredArtifacts` list to the
  new artifact names: `["pkg-jazz-tools","pkg-jazz-wasm","pkg-jazz-napi",
"pkg-jazz-rn","pkg-create-jazz"]`.
- `deploy-inspector-production` unchanged.

The alpha publish behaviour is preserved end to end; only the artifact
boundaries move.

## Testing & rollout

1. **Composite action validation.** Switch `ci.yml`'s `lint` job to use
   `setup-build`. Push to a feature branch; verify CI is green. If green,
   migrate `test-rust` and `test-ts` too.
2. **Reusable workflow validation.** On the same branch, trigger
   `publish-jazz-tools-alpha.yml` via `workflow_dispatch` with
   `mode: dry-run`. Verify all five `pkg-*` artifacts produce and all
   verify-packed-\* steps pass.
3. **Preview workflow validation.** Open a no-op draft PR on the feature
   branch, apply the `preview-build` label, watch the pkg.pr.new comment
   appear. Install one of the URLs into a scratch Next.js or Expo app to
   verify the published tarball.
4. **Rollout.** Merge once steps 1-3 are green. The alpha publish path stays
   working throughout — the refactor is behaviour-preserving from npm's
   perspective.

## Risks

- The `assemble-packages` job runs on a single Linux runner and has to
  download artifacts from all build matrices first. Adds ~5-10 min on the
  critical path vs. doing assembly inline in `publish-npm`. Acceptable trade
  for not duplicating the logic.
- Refactor of a 1262-line workflow is non-trivial. The dry-run validation
  step is the safety net.
- pkg.pr.new is third-party infrastructure. If their service is down,
  preview builds fail with a clear error; alpha publish is unaffected.
