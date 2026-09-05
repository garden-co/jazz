#!/usr/bin/env bash
set -euo pipefail

archive=${1:?usage: verify-pinned-archive.sh <archive> <sha256>}
expected=${2:?usage: verify-pinned-archive.sh <archive> <sha256>}
[[ -f "$archive" ]] || { echo "archive does not exist: $archive" >&2; exit 1; }
echo "$expected  $archive" | sha256sum --check --status || {
  echo "archive failed its pinned checksum: $archive" >&2
  exit 1
}
