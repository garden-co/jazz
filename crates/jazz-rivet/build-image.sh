#!/bin/sh
# From anywhere: sh crates/jazz-rivet/build-image.sh [image-tag]
# Produces an immutable archive descriptor for rivet({ runtime: { image } }).
# Requires a running Docker daemon. Does not push to a registry or deploy.
set -eu
cd "$(dirname "$0")/../.."
tag="${1:-jazz-rivet:3.0.7}"
mkdir -p crates/jazz-rivet/artifacts
docker build --platform linux/amd64 -f crates/jazz-rivet/Dockerfile -t "$tag" .
docker save -o crates/jazz-rivet/artifacts/image.tar "$tag"
python3 - "$tag" <<'PY'
import hashlib, json, pathlib, sys
p = pathlib.Path("crates/jazz-rivet/artifacts/image.tar").resolve()
with p.open("rb") as f:
    digest = hashlib.file_digest(f, "sha256").hexdigest()
descriptor = {"path": str(p), "tag": sys.argv[1], "sha256": digest}
pathlib.Path("crates/jazz-rivet/artifacts/image.json").write_text(json.dumps(descriptor, indent=2) + "\n")
print(json.dumps(descriptor, indent=2))
PY
