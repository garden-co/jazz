#!/usr/bin/env bash
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "$ROOT"

PATTERN='pilot-applogs|pilot-app|aashto_cbr|sampling_interval_reading|watermark_mapping|3de33ec4-b6c3-4c6b-aa56-c4a9669daffd|99553aa2-e715-4492-8099-cd9bbceefcb6|jon@pilot-applogs\.com'

if [[ "$#" -gt 0 ]]; then
  rg -a -n -i --hidden --glob '!target/**' --glob '!target-real-policy-check/**' "$PATTERN" "$@"
  matches=$?
else
  matches=1
  while IFS= read -r -d '' file; do
    [[ -f "$file" ]] || continue
    [[ "$file" == "dev/gates/no-sensitive-data.sh" ]] && continue
    if rg -a -n -i "$PATTERN" "$file"; then
      matches=0
    fi
  done < <(git ls-files -z)
fi

case "$matches" in
  0)
    echo "sensitive-data guard failed: customer-specific fixture data is present" >&2
    exit 1
    ;;
  1)
    exit 0
    ;;
  *)
    exit "$matches"
    ;;
esac
