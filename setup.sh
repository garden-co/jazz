#!/usr/bin/env bash
set -euo pipefail

echo "==> Building Rust workspace..."
cargo build --workspace

if ! command -v wasm-pack &> /dev/null; then
  echo "==> Installing wasm-pack via cargo..."
  cargo install wasm-pack
fi

echo "==> Building WASM bindings..."
wasm-pack build crates/groove-wasm --target web

echo "==> Installing JS dependencies..."
pnpm install

echo "==> Building TypeScript packages..."
pnpm build

echo "==> Setup complete!"
