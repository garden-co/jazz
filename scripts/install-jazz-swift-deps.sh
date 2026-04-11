#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

JAZZ_RN_PLATFORM=ios bash "$script_dir/install-jazz-rn-deps.sh"
