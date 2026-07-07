#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

pnpm --dir packages/jazz-tools exec vitest run \
  --config vitest.config.ts \
  src/runtime/native-runtime/runtime.test.ts \
  src/runtime/native-runtime/websocket.test.ts
