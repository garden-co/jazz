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

## Development

Install the [`rust-analyzer` extension](https://marketplace.visualstudio.com/items?itemName=rust-lang.rust-analyzer) to get Rust support in VSCode. `rust-analyzer` expects a `Cargo.toml` file in the root of the project, so configure the workspace setting with the root directory of the project:

```json
// .vscode/settings.json
{
  "rust-analyzer.linkedProjects": [
    "packages/jazz-crypto-rs/Cargo.toml"
  ]
}
```


## Test

```bash
cargo test
```
