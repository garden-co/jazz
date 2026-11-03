#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# tsx: the launcher imports the example's TypeScript schema and permissions.
cd "$ROOT_DIR"
exec pnpm exec tsx "$ROOT_DIR/dev/examples/todo-localfirst-launch.mjs" "$@"
