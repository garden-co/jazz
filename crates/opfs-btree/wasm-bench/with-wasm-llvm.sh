#!/usr/bin/env sh
set -eu

if [ -z "${CC_wasm32_unknown_unknown:-}" ]; then
  llvm_prefix="${LLVM_PREFIX:-}"
  if [ -z "$llvm_prefix" ]; then
    llvm_prefix="$(brew --prefix llvm 2>/dev/null || brew --prefix llvm@20 2>/dev/null)" || {
      echo "install Homebrew LLVM: brew install llvm" >&2
      exit 1
    }
  fi

  export CC_wasm32_unknown_unknown="$llvm_prefix/bin/clang"
  export AR_wasm32_unknown_unknown="${AR_wasm32_unknown_unknown:-$llvm_prefix/bin/llvm-ar}"
fi

export CFLAGS_wasm32_unknown_unknown="${CFLAGS_wasm32_unknown_unknown:--O3 -DSQLITE_THREADSAFE=0}"

exec env -u NO_COLOR "$@"
