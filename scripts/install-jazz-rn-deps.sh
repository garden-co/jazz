#!/usr/bin/env bash

set -euo pipefail

export CARGO_HOME="${CARGO_HOME:-$HOME/.cargo}"
export RUSTUP_HOME="${RUSTUP_HOME:-$HOME/.rustup}"
export PATH="$CARGO_HOME/bin:$PATH"

if ! command -v rustup >/dev/null 2>&1; then
  curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain stable
fi

rustup toolchain install stable
rustup default stable
rustup target add \
  wasm32-unknown-unknown \
  aarch64-linux-android \
  armv7-linux-androideabi \
  i686-linux-android \
  x86_64-linux-android

if ! command -v cargo-ndk >/dev/null 2>&1; then
  cargo install cargo-ndk --locked
fi

case "$(uname -s)" in
  Darwin)
    if ! command -v brew >/dev/null 2>&1; then
      echo "Homebrew is required to install C/C++ build dependencies (cmake, ninja, clang-format)." >&2
      exit 1
    fi

    brew install cmake ninja clang-format

    if ! xcode-select -p >/dev/null 2>&1; then
      xcode-select --install || true
      echo "Xcode Command Line Tools installation started. Re-run this script when installation finishes." >&2
      exit 1
    fi

    rustup target add aarch64-apple-ios aarch64-apple-ios-sim x86_64-apple-ios
    ;;
  Linux)
    if command -v apt-get >/dev/null 2>&1; then
      if command -v sudo >/dev/null 2>&1; then
        sudo apt-get update
        sudo apt-get install -y cmake ninja-build clang-format
      else
        apt-get update
        apt-get install -y cmake ninja-build clang-format
      fi
    else
      echo "Install cmake, ninja and clang-format with your package manager, then re-run this script." >&2
      exit 1
    fi
    ;;
  *)
    echo "Unsupported OS for automatic dependency installation: $(uname -s)" >&2
    exit 1
    ;;
esac

echo "Jazz prerequisites installed."
