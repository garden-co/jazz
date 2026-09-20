#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
if ! git -C "${ROOT}" rev-parse --verify HEAD >/dev/null 2>&1; then
  echo "Inspector source builds require Git metadata for native artifact provenance. For a source archive, use the verified inspector-prebuilt artifact and Stage Inspector production workflow (packages/inspector/README.md)." >&2
  exit 1
fi
bash "${ROOT}/dev/scripts/install-vercel-deps.sh"
pnpm --dir "${ROOT}" build:ci
pnpm --dir "${ROOT}/packages/inspector" run build:web
