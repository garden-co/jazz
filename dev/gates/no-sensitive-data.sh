#!/usr/bin/env bash
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "$ROOT"

PATTERN='pilot-applogs|pilot-app|aashto_cbr|sampling_interval_reading|watermark_mapping|3de33ec4-b6c3-4c6b-aa56-c4a9669daffd|99553aa2-e715-4492-8099-cd9bbceefcb6|jon@pilot-applogs\.com'

# The guard must fail closed: a missing scanner must never look like a clean
# scan (rg exiting 127 is indistinguishable from "no match" in an `if`).
if command -v rg >/dev/null 2>&1; then
  scan_file() { rg -a -n -i "$PATTERN" "$1"; }
elif command -v grep >/dev/null 2>&1; then
  scan_file() { grep -a -n -i -E "$PATTERN" "$1"; }
else
  echo "sensitive-data guard error: neither rg nor grep is available" >&2
  exit 2
fi

matches=1
if [[ "$#" -gt 0 ]]; then
  files=("$@")
else
  files=()
  while IFS= read -r -d '' file; do
    files+=("$file")
  done < <(git ls-files -z)
fi

for file in "${files[@]}"; do
  [[ -f "$file" ]] || continue
  [[ "$file" == "dev/gates/no-sensitive-data.sh" ]] && continue
  rc=0
  scan_file "$file" || rc=$?
  case "$rc" in
    0) matches=0 ;;
    1) ;;
    *)
      echo "sensitive-data guard error: scanner failed on $file (exit $rc)" >&2
      exit 2
      ;;
  esac
done

if [[ "$matches" -eq 0 ]]; then
  echo "sensitive-data guard failed: customer-specific fixture data is present" >&2
  exit 1
fi
exit 0
