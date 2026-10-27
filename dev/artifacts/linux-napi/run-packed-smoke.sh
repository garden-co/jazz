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
# Run where it was built (Bullseye), on Vercel's build userspace (AL2023), and
# in the Lambda Node runtime.
baseline="$(sha256sum dev/artifacts/linux-napi/Dockerfile | cut -d ' ' -f1)"
image="jazz-napi-bullseye:${baseline}-${arch}"
docker build --platform "linux/$arch" --tag "$image" dev/artifacts/linux-napi
al2023_recipe="$(sha256sum dev/artifacts/linux-napi/al2023/Dockerfile | cut -d ' ' -f1)"
al2023="jazz-napi-al2023-runtime:${al2023_recipe}-${arch}"
docker build --platform "linux/$arch" --tag "$al2023" dev/artifacts/linux-napi/al2023
for environment in baseline al2023 lambda; do
  case "$environment" in
    baseline) selected="$image"; entry=node ;;
    al2023) selected="$al2023"; entry=node ;;
    lambda) selected="$runtime"; entry=/var/lang/bin/node ;;
  esac
  echo "Packed NAPI smoke: $platform / $environment / host $(uname -m)"
  docker run --rm --platform "linux/$arch" --entrypoint "$entry" \
    --volume "$fixture:$fixture:ro" --workdir "$fixture" \
    "$selected" --unhandled-rejections=strict smoke.mjs
done
