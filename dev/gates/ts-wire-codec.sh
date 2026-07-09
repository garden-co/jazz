#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

rust_protocol_version="$(
  sed -n 's/^pub const WIRE_PROTOCOL_VERSION: u16 = \([0-9][0-9]*\);$/\1/p' crates/jazz/src/wire.rs
)"
ts_protocol_version="$(
  sed -n 's/^export const WIRE_PROTOCOL_VERSION = \([0-9][0-9]*\);$/\1/p' \
    packages/jazz-tools/src/runtime/native-runtime/websocket.ts
)"

if [[ -z "$rust_protocol_version" || -z "$ts_protocol_version" ]]; then
  echo "failed to read Rust/TS wire protocol versions" >&2
  exit 1
fi

if [[ "$rust_protocol_version" != "$ts_protocol_version" ]]; then
  echo "TS wire protocol version $ts_protocol_version does not match Rust $rust_protocol_version" >&2
  exit 1
fi

pnpm --dir packages/jazz-tools exec vitest run \
  --config vitest.config.ts \
  src/runtime/native-runtime/runtime.test.ts \
  src/runtime/native-runtime/websocket.test.ts
