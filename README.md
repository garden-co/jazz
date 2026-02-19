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
