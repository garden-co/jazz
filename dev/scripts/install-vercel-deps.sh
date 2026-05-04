#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

JAZZ_SKIP_RN_DEPS=1 bash "${SCRIPT_DIR}/install-jazz-rn-deps.sh"
