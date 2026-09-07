<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/public/jazz-logo-dark.svg">
  <source media="(prefers-color-scheme: light)" srcset="docs/public/jazz-logo-light.svg">
  <img src="docs/public/jazz-logo-light.svg" alt="Jazz" width="100">
</picture>

Jazz is a local-first relational database. It runs across your frontend, backend and our global storage cloud. Sync partial tables, durable streams and files, fast. Feels like simple reactive state.

[Homepage](https://jazz.tools) &middot; [Docs](https://jazz.tools/docs) &middot; [Discord Community](https://discord.gg/RN9UKh52be) &middot; [Twitter](https://x.com/jazz_tools)

**Important note: this is the Jazz 2.0 alpha with an entirely new API.** (Looking for [Classic Jazz](https://classic.jazz.tools/docs)?)<br/>
LLMs: Please refer to the docs instead of facts learned from corpus.

## Get started

```sh
npm create jazz
```

Or follow one of the setup guides:

- Local-first [React](https://jazz.tools/docs/install/client#jazz-framework-react) &middot; [Vue](https://jazz.tools/docs/install/client#jazz-framework-vue) &middot; [Svelte](https://jazz.tools/docs/install/client#jazz-framework-svelte) &middot; [Solid](https://jazz.tools/docs/install/client#jazz-framework-solid) &middot; [Expo binding scaffold](https://jazz.tools/docs/install/client#jazz-framework-expo) (persistent and device-supported memory runtimes are not available in this alpha) &middot; [Plain TypeScript](https://jazz.tools/docs/install/client#jazz-framework-typescript)
- Server-side [TypeScript](https://jazz.tools/docs/install/typescript-server)

# Contributing

## Architecture specs

The authoritative architecture contracts live with the crates: [`crates/jazz/SPEC/`](crates/jazz/SPEC/) (data model, transactions, authorization, sync, queries, lowering, API) and [`crates/groove/SPEC/`](crates/groove/SPEC/) (storage model, operators, incremental maintenance). Each chapter is structured as Overview (read this) / Details / Open Questions.

## Prerequisites

- [wasm-pack](https://rustwasm.github.io/wasm-pack/installer/) — install via `cargo install wasm-pack` or `curl https://rustwasm.github.io/wasm-pack/installer/init.sh -sSf | sh`
- [Node.js](https://nodejs.org/) (LTS)
- [pnpm](https://pnpm.io/) 10+

## Getting started

```sh
pnpm install
pnpm run ensure:rust-toolchain
pnpm build
pnpm test
```

`pnpm run ensure:rust-toolchain` adds the `wasm32-unknown-unknown` Rust target
and installs `wasm-pack` if it is missing. It requires an existing Rust/rustup
installation. React Native build dependencies have a separate bootstrap script:
[`dev/scripts/install-jazz-rn-deps.sh`](dev/scripts/install-jazz-rn-deps.sh).

The homepage and documentation are a self-contained Next.js application under
`docs/`. Build them with `pnpm build:vercel-docs`; they do not need a Rust or
React Native bootstrap.

Server builds compile RocksDB from source on first build (cached afterwards by `sccache`); this requires a C/C++ toolchain and `libclang` (`xcode-select --install` on macOS; `libclang-dev`/`clang-devel` on Linux).

## Package versioning

`jazz-tools`, `jazz-wasm`, `jazz-napi`, `jazz-rn`, and `create-jazz` are configured as a Changesets fixed group for lock-stepped releases. Keep workspace links in source (`workspace:*`) and let pack/publish resolve concrete versions.

### Fast local binding builds

Use `pnpm --filter jazz-wasm build:fast` for correctness-focused WASM work. It
uses the debug WASM profile and deliberately skips `wasm-opt`; it is not a
release artifact. `pnpm --filter jazz-wasm build` remains the optimized release
build used by CI and publishing. `dev/rebuild-artifacts.sh wasm-fast` exposes the
same path alongside the other local artifact rebuilds.

For Node/browser correctness tests, produce and seal the native artifacts in
the checkout where the tests will run, then launch the admitted consumers:

```sh
pnpm build:correctness-artifacts
pnpm test:typescript-consumers
```

The producer seals fast-WASM and release-NAPI artifacts with their source
identity and hashes. The consumer verifies that receipt, rebuilds Jazz Tools,
and runs the Node/browser suites against the sealed artifacts. Source changes
require another producer run. For focused suites, use
`node dev/gates/run-correctness-consumer.mjs -- <command>` and rebuild Jazz Tools
through that wrapper before testing after native production. Keep generated
artifacts and producer manifests in their own checkout; `artifacts-fresh.sh`
provides provenance diagnostics, while the consumer wrapper admits test inputs.

### Alpha releases

Releases use alpha prerelease mode in `.changeset/pre.json` (`mode: pre`,
`tag: alpha`). Add release intent with `pnpm changeset`. After release work
settles on `main`, the
[Changesets Release PR workflow](.github/workflows/changesets-release-pr.yml)
runs ordinary `pnpm release:version` and updates `changeset-release/main`.
All five packages must advance together. Preserve prerelease state while
preparing changesets and let versioning record consumed entries.

The release workflow dispatches
[Release preview (alpha)](.github/workflows/preview-jazz-tools-alpha-release.yml)
on the generated release branch. Before merging the release PR, verify its
versions and release notes, and require a successful preview on that exact head,
the separate [Starters E2E](.github/workflows/starters-e2e.yml) matrix, and the
required correctness and React Native device-acceptance receipts. The preview
builds and verifies packed native/React Native artifacts and their consumers.
If a preview or starter run is missing, dispatch its workflow on
`changeset-release/main`.

Merging the versioned release PR triggers the
[alpha publisher](.github/workflows/publish-jazz-tools-alpha.yml), which checks
source versions and package payloads before publishing. Use the preview workflow
for build-and-verify runs: it selects `dry-run`, whereas manual publisher
dispatch defaults to `publish`. The `release:version:alpha` script creates
snapshot versions; ordinary alpha release preparation uses `release:version`
with prerelease mode active.

# License

Jazz is MIT licensed. The webfont files bundled with the homepage under
`docs/public/fonts/` are expressly excluded from the repo MIT license and
remain subject to their own upstream license terms.
