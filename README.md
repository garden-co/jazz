# Jazz 2.0

Distributed, local-first relational database. Rust core, TypeScript client layers, WASM + NAPI bindings.

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

`pnpm run ensure:rust-toolchain` runs `scripts/install-jazz-rn-deps.sh` to bootstrap Rust (via `rustup` if needed), required Rust targets, `cargo-ndk`, and platform build tools (`cmake`, `ninja`, `clang-format`).

For docs-only builds (for example on Vercel), set `JAZZ_SKIP_RN_DEPS=1` to skip React Native-specific bootstrap:

```sh
JAZZ_SKIP_RN_DEPS=1 pnpm run ensure:rust-toolchain
```

Vercel builds can use `scripts/install-vercel-deps.sh`, which runs the same Rust bootstrap in docs-only mode without the React Native extras.

Supported server targets now fetch a pinned prebuilt RocksDB archive from GHCR into a local cache on the first build, then reuse that cached archive on later builds. GitHub Actions can use `${{ github.token }}` for that fetch path; local shells can opt in with `JAZZ_ROCKSDB_GHCR_USERNAME` plus `JAZZ_ROCKSDB_GHCR_PASSWORD` (or `GHCR_USERNAME` plus `CR_PAT`). Without GHCR credentials, the build falls back to compiling RocksDB from source. The repo still uses checked-in RocksDB bindings, so `libclang` is no longer required for the fallback source-build path either. To regenerate and publish the supported archive set, run `bash scripts/publish-rocksdb-artifacts.sh`.

## Package versioning

`jazz-tools`, `jazz-wasm`, `jazz-napi`, and `jazz-rn` are configured as a Changesets fixed group for lock-stepped releases. Keep workspace links in source (`workspace:*`) and let pack/publish resolve concrete versions.

Releases are currently locked to the alpha prerelease channel via `.changeset/pre.json` (`tag: alpha`).
The `Changesets Release PR` workflow uses `changesets/action` to auto-create/update a `Version Packages (alpha)` PR on `main`.
Install the [Changeset bot app](https://github.com/apps/changeset-bot) on this repo so PRs get changeset guidance comments.

```sh
# add release intent
pnpm changeset

# apply version bumps from changesets
pnpm release:version

# apply alpha snapshot versions (manual fallback)
pnpm release:version:alpha

# publish with resolved non-workspace dependency versions
pnpm release:publish

# publish with the alpha dist-tag
pnpm release:publish:alpha
```
