#!/usr/bin/env bash
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "$ROOT"

# Guarded patterns are base64-encoded so this public file does not itself
# publish the strings it exists to keep out of the repo. Decode to inspect;
# re-encode with `printf %s '<pattern>' | base64` when updating.
PATTERN_B64='Ym9yZWRtbG9nc3xib3JlZG18YWFzaHRvX2NicnxzYW1wbGluZ19pbnRlcnZhbF9yZWFkaW5nfHdhdGVybWFya19tYXBwaW5nfDNkZTMzZWM0LWI2YzMtNGM2Yi1hYTU2LWM0YTk2NjlkYWZmZHw5OTU1M2FhMi1lNzE1LTQ0OTItODA5OS1jZDliYmNlZWZjYjZ8am9uQGJvcmVkbWxvZ3NcLmNvbQ=='
PATTERN=$(printf %s "$PATTERN_B64" | base64 -d)
if [[ -z "$PATTERN" ]]; then
  echo "sensitive-data guard error: pattern decode produced empty pattern" >&2
  exit 2
fi

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
