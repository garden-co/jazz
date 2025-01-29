# Jazz Crypto RS

A Rust implementation of cryptographic primitives for the Jazz project, compiled to WebAssembly.

## Module Structure

The codebase is organized into the following modules:

### Crypto Module (`src/crypto/`)
- `error.rs` - Error types for cryptographic operations
- `ed25519.rs` - Ed25519 signing and verification
- `x25519.rs` - X25519 key exchange
- `xsalsa20.rs` - XSalsa20 and XSalsa20-Poly1305 encryption/decryption
- `mod.rs` - High-level sealing/unsealing operations

### Hash Module (`src/hash/`)
- `mod.rs` - BLAKE3 hashing functionality and nonce generation

## Features

- Ed25519 signing and verification
- X25519 key exchange
- XSalsa20 and XSalsa20-Poly1305 encryption
- BLAKE3 hashing with incremental state updates
- Secure nonce generation
- WebAssembly bindings for all operations

## Usage

The library exposes WebAssembly-compatible functions for all cryptographic operations. See the individual module files for detailed documentation of available functions.

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
