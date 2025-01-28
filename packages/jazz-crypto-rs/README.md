# Jazz Crypto Rust

This is the Rust implementation of the Jazz Crypto utils library.

## Installation

Get a working Rust environment (rustup).

```bash
rustup install stable
rustup default stable
```

[Install `wasm-pack`](https://rustwasm.github.io/wasm-pack/installer/).


Then add wasm target to your toolchain:

```bash
rustup target add wasm32-unknown-unknown
```

## Build

```bash
wasm-pack build --target web
```

## Test

```bash
cargo test
```
