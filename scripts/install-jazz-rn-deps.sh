#!/usr/bin/env bash

set -euo pipefail

export CARGO_HOME="${CARGO_HOME:-$HOME/.cargo}"
export RUSTUP_HOME="${RUSTUP_HOME:-$HOME/.rustup}"
export PATH="$CARGO_HOME/bin:$PATH"
JAZZ_SKIP_RN_DEPS="${JAZZ_SKIP_RN_DEPS:-0}"
JAZZ_RN_PLATFORM="${JAZZ_RN_PLATFORM:-all}"
RUST_TOOLCHAIN="${JAZZ_RUST_TOOLCHAIN:-1.93.1}"

is_truthy() {
  case "$1" in
    1 | true | TRUE | yes | YES | on | ON) return 0 ;;
    *) return 1 ;;
  esac
}

ensure_wrapper_command_exists() {
  local env_var="$1"
  local wrapper_value="$2"
  local wrapper_cmd="${wrapper_value%% *}"

  if ! command -v "$wrapper_cmd" >/dev/null 2>&1; then
    echo "warning: $env_var is set to '$wrapper_value' but '$wrapper_cmd' is unavailable; disabling wrapper for bootstrap." >&2
    unset "$env_var"
  fi
}

install_brew_formula_if_missing() {
  local command_name="$1"
  local formula="$2"

  if ! command -v "$command_name" >/dev/null 2>&1; then
    brew install "$formula"
  fi
}

if [[ -n "${RUSTC_WRAPPER:-}" ]]; then
  ensure_wrapper_command_exists "RUSTC_WRAPPER" "$RUSTC_WRAPPER"
fi

if [[ -n "${CARGO_BUILD_RUSTC_WRAPPER:-}" ]]; then
  ensure_wrapper_command_exists "CARGO_BUILD_RUSTC_WRAPPER" "$CARGO_BUILD_RUSTC_WRAPPER"
fi

if ! command -v rustup >/dev/null 2>&1; then
  curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain "$RUST_TOOLCHAIN"
fi

rustup toolchain install "$RUST_TOOLCHAIN"
rustup default "$RUST_TOOLCHAIN"
rustup target add wasm32-unknown-unknown --toolchain "$RUST_TOOLCHAIN"

case "$JAZZ_RN_PLATFORM" in
  all | ios | android) ;;
  *)
    echo "Unsupported JAZZ_RN_PLATFORM: ${JAZZ_RN_PLATFORM}" >&2
    exit 1
    ;;
esac

if is_truthy "$JAZZ_SKIP_RN_DEPS"; then
  echo "Skipping React Native dependency bootstrap (JAZZ_SKIP_RN_DEPS=$JAZZ_SKIP_RN_DEPS)."
  echo "Jazz prerequisites installed."
  exit 0
fi

if [[ "$JAZZ_RN_PLATFORM" == "all" || "$JAZZ_RN_PLATFORM" == "android" ]]; then
  rustup target add \
    aarch64-linux-android \
    armv7-linux-androideabi \
    i686-linux-android \
    x86_64-linux-android \
    --toolchain "$RUST_TOOLCHAIN"
fi

if [[ "$JAZZ_RN_PLATFORM" == "all" || "$JAZZ_RN_PLATFORM" == "android" ]]; then
  if ! command -v cargo-ndk >/dev/null 2>&1; then
    cargo install cargo-ndk --locked
  fi
fi

case "$(uname -s)" in
  Darwin)
    if ! command -v brew >/dev/null 2>&1; then
      echo "Homebrew is required to install C/C++ build dependencies (cmake, ninja, clang-format)." >&2
      exit 1
    fi

    install_brew_formula_if_missing cmake cmake
    install_brew_formula_if_missing ninja ninja

    if [[ "$JAZZ_RN_PLATFORM" == "all" ]]; then
      install_brew_formula_if_missing clang-format clang-format
    fi

    if ! xcode-select -p >/dev/null 2>&1; then
      xcode-select --install || true
      echo "Xcode Command Line Tools installation started. Re-run this script when installation finishes." >&2
      exit 1
    fi

    if [[ "$JAZZ_RN_PLATFORM" == "all" || "$JAZZ_RN_PLATFORM" == "ios" ]]; then
      rustup target add aarch64-apple-ios aarch64-apple-ios-sim x86_64-apple-ios --toolchain "$RUST_TOOLCHAIN"
    fi
    ;;
  Linux)
    if [[ "$JAZZ_RN_PLATFORM" == "ios" ]]; then
      echo "iOS prerequisites require macOS." >&2
      exit 1
    fi

    if command -v apt-get >/dev/null 2>&1; then
      if command -v sudo >/dev/null 2>&1; then
        sudo apt-get update
        if [[ "$JAZZ_RN_PLATFORM" == "all" ]]; then
          sudo apt-get install -y cmake ninja-build clang-format
        else
          sudo apt-get install -y cmake ninja-build
        fi
      else
        apt-get update
        if [[ "$JAZZ_RN_PLATFORM" == "all" ]]; then
          apt-get install -y cmake ninja-build clang-format
        else
          apt-get install -y cmake ninja-build
        fi
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
