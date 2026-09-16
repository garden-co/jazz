#!/usr/bin/env bash
set -euo pipefail
platform="${1:?usage: run-packed-smoke.sh <linux-platform> <new-fixture-directory>}"
fixture="${2:?missing new fixture directory}"
case "$platform" in
  linux-x64-gnu)
    arch=amd64
    runtime=public.ecr.aws/lambda/nodejs:24@sha256:19c9ce8d55c18e32fd4df26c4db582b8711297f73ca68860e541ee0715111490 ;;
  linux-arm64-gnu)
    arch=arm64
    runtime=public.ecr.aws/lambda/nodejs:24-arm64@sha256:f04cd4c199f7237792c112ac4bb71e41fb0f51bf975e03103854d94b4ea1b5ac ;;
  *) echo "Unsupported platform $platform" >&2; exit 1 ;;
esac
node dev/artifacts/linux-napi/prepare-packed-smoke.mjs "$fixture" "$platform"
fixture="$(realpath "$fixture")"
# Inspect the exact installed payload, not a workspace generation pointer.
node dev/artifacts/linux-napi/verify-elf.mjs \
  "$fixture/node_modules/@garden-co/jazz-napi-$platform/jazz-napi.$platform.node" "$platform"
baseline="$(sha256sum dev/artifacts/linux-napi/Dockerfile | cut -d ' ' -f1)"
image="jazz-napi-al2023:${baseline}-${arch}"
docker build --platform "linux/$arch" --tag "$image" dev/artifacts/linux-napi
for environment in baseline lambda; do
  if [[ "$environment" == baseline ]]; then selected="$image"; entry=node
  else selected="$runtime"; entry=/var/lang/bin/node; fi
  echo "Packed NAPI smoke: $platform / $environment / host $(uname -m)"
  docker run --rm --platform "linux/$arch" --entrypoint "$entry" \
    --volume "$fixture:$fixture:ro" --workdir "$fixture" \
    "$selected" --unhandled-rejections=strict smoke.mjs
done
