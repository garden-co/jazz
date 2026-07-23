#!/usr/bin/env bash
#
# Download the USDA PLANTS checklist used by the benchmark. The dataset is NOT
# committed to the repository; run this once before running the benchmark.
#
#   dev/benchmarks/plants-rocksdb/scripts/setup.sh
#
set -euo pipefail

# Resolve the crate's data/ directory regardless of where this is invoked from.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="$(cd "$SCRIPT_DIR/.." && pwd)/data"
DEST="$DATA_DIR/plantlst.txt"

URL="${PLANTS_DATASET_URL:-https://plants.sc.egov.usda.gov/DocumentLibrary/Txt/plantlst.txt}"

mkdir -p "$DATA_DIR"

if [[ -s "$DEST" ]]; then
  echo "Dataset already present: $DEST ($(wc -l < "$DEST" | tr -d ' ') lines)"
  exit 0
fi

echo "Downloading USDA PLANTS checklist"
echo "  from: $URL"
echo "  to:   $DEST"

if command -v curl >/dev/null 2>&1; then
  curl -fSL --retry 3 -o "$DEST" "$URL"
elif command -v wget >/dev/null 2>&1; then
  wget -O "$DEST" "$URL"
else
  echo "error: neither curl nor wget is available" >&2
  exit 1
fi

echo "Done: $DEST ($(wc -l < "$DEST" | tr -d ' ') lines)"
