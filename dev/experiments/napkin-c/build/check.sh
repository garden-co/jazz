#!/usr/bin/env bash
# Compile-check C files of the napkin port (no linking). Usage: check.sh FILE.c...
set -uo pipefail
here=$(cd "$(dirname "$0")/.." && pwd)
WASI_SDK=${WASI_SDK:-/home/user/scratch/wasi-sdk-25.0-x86_64-linux}
CC="$WASI_SDK/bin/clang"
status=0
for f in "$@"; do
  case $f in /*) src=$f ;; *) if [[ -f $f ]]; then src=$f; else src=$here/$f; fi ;; esac
  dir=$(dirname "$src")
  "$CC" --target=wasm32-wasip1 -Oz -std=gnu11 -c -o /dev/null \
    -I "$here/rt" -I "$here/src" -I "$dir" \
    -Wno-unused-function -Wno-unused-variable -Wno-unused-parameter \
    -Werror=implicit-function-declaration -Werror=int-conversion \
    -Werror=incompatible-pointer-types -Werror=return-type \
    "$src" || { echo "FAIL $f"; status=1; }
done
[[ $status == 0 ]] && echo "OK ($# files)"
exit $status
