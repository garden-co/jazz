#!/usr/bin/env bash
set -euo pipefail
# Invoke from the checkout root, on a native x64 or ARM64 Docker host.
platform="${1:?usage: build.sh linux-x64-gnu|linux-arm64-gnu}"
case "$platform" in
  linux-x64-gnu) target=x86_64-unknown-linux-gnu; docker_arch=amd64 ;;
  linux-arm64-gnu) target=aarch64-unknown-linux-gnu; docker_arch=arm64 ;;
  *) echo "Unsupported Linux NAPI platform: $platform" >&2; exit 1 ;;
esac
root="$(pwd)"
baseline="$(sha256sum dev/artifacts/linux-napi/Dockerfile | cut -d ' ' -f1)"
image="jazz-napi-al2023:${baseline}-${docker_arch}"
docker build --platform "linux/$docker_arch" --tag "$image" dev/artifacts/linux-napi
# This directory is deliberately distinct from every host Cargo cache. Its key
# changes with the whole producer image recipe, including both base digests.
cache="$root/target/linux-napi-${baseline}-${docker_arch}"
mkdir -p "$cache/cargo" "$cache/target" "$cache/pnpm-store"
# Worktrees require their Git metadata at the same absolute location. A normal
# CI checkout has its Git directory inside root and needs no extra mount.
git_common="$(git rev-parse --path-format=absolute --git-common-dir)"
mounts=()
if [[ "$git_common" != "$root/"* ]]; then mounts+=(--volume "$git_common:$git_common:ro"); fi
docker run --rm --platform "linux/$docker_arch" \
  --user "$(id -u):$(id -g)" \
  --volume "$root:$root" "${mounts[@]}" --workdir "$root" \
  --env CI=true --env HOME=/tmp --env CARGO_HOME="$cache/cargo" \
  --env CARGO_TARGET_DIR="$cache/target" \
  --env JAZZ_NAPI_PNPM_STORE="$cache/pnpm-store" \
  --env JAZZ_TEST_ARTIFACT_LOCK_PATH="$cache/artifact.lock" \
  --env JAZZ_NAPI_BUILD_BASELINE="al2023-gcc11-$baseline" \
  --env JAZZ_NAPI_BUILD_IMAGE="$(docker image inspect --format '{{.Id}}' "$image")" \
  "$image" bash -c 'set -euo pipefail
    git config --global --add safe.directory "$PWD"
    pnpm install --frozen-lockfile --ignore-scripts --store-dir "$JAZZ_NAPI_PNPM_STORE"
    node dev/artifacts/build.mjs napi release --target "$1"
    node dev/artifacts/provenance.mjs verify napi release --target "$1"
    node dev/artifacts/stage-napi-loader.mjs "$2"
    node dev/artifacts/linux-napi/verify-elf.mjs "crates/jazz-napi/jazz-napi.$2.node" "$2"
  ' -- "$target" "$platform"
