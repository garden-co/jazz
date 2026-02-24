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

## Package versioning

`jazz-tools`, `jazz-wasm`, and `jazz-napi` are configured as a Changesets fixed group for lock-stepped releases. Keep workspace links in source (`workspace:*`) and let pack/publish resolve concrete versions.

```sh
# add release intent
pnpm changeset

# apply version bumps from changesets
pnpm release:version

# apply alpha snapshot versions
pnpm release:version:alpha

# publish with resolved non-workspace dependency versions
pnpm release:publish

# publish with the alpha dist-tag
pnpm release:publish:alpha
```
