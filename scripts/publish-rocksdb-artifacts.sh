#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"

OUTPUT_DIR="${OUTPUT_DIR:-${REPO_ROOT}/dist/rocksdb}"
GHCR_REPOSITORY="${GHCR_REPOSITORY:-ghcr.io/garden-co/jazz2-rocksdb-prebuilt}"
TAG_PREFIX="${TAG_PREFIX:-rocksdb-10.7.5-v1}"
ARTIFACT_TYPE="application/vnd.garden-co.jazz.rocksdb.archive.v1+gzip"

ALL_TARGETS=(
  aarch64-apple-darwin
  x86_64-apple-darwin
  aarch64-unknown-linux-gnu
  x86_64-unknown-linux-gnu
)

if [[ "$#" -eq 0 ]]; then
  TARGETS=("${ALL_TARGETS[@]}")
else
  TARGETS=("$@")
fi

if [[ "${JAZZ_ROCKSDB_SKIP_BUILD:-0}" != "1" ]]; then
  bash "${SCRIPT_DIR}/build-rocksdb-artifacts.sh" "${TARGETS[@]}"
fi

if ! command -v oras >/dev/null 2>&1; then
  echo "oras is required" >&2
  exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required" >&2
  exit 1
fi

sha256_file() {
  local path="$1"

  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "${path}" | awk '{print $1}'
    return
  fi

  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "${path}" | awk '{print $1}'
    return
  fi

  echo "sha256sum or shasum is required" >&2
  exit 1
}

sha256_stdin() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum | awk '{print $1}'
    return
  fi

  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 | awk '{print $1}'
    return
  fi

  echo "sha256sum or shasum is required" >&2
  exit 1
}

GHCR_USERNAME="${GHCR_USERNAME:-${GITHUB_ACTOR:-}}"
GHCR_PASSWORD="${GHCR_PASSWORD:-${GITHUB_TOKEN:-}}"

if [[ -z "${GHCR_USERNAME}" && -x "$(command -v gh || true)" ]]; then
  GHCR_USERNAME="$(gh api user -q .login)"
fi
if [[ -z "${GHCR_PASSWORD}" && -x "$(command -v gh || true)" ]]; then
  GHCR_PASSWORD="$(gh auth token)"
fi

if [[ -z "${GHCR_USERNAME}" || -z "${GHCR_PASSWORD}" ]]; then
  echo "set GHCR_USERNAME/GHCR_PASSWORD or GITHUB_ACTOR/GITHUB_TOKEN" >&2
  exit 1
fi

printf '%s' "${GHCR_PASSWORD}" | oras login ghcr.io -u "${GHCR_USERNAME}" --password-stdin >/dev/null

manifest_entries=()

for target in "${TARGETS[@]}"; do
  archive_path="${OUTPUT_DIR}/${target}/librocksdb.a.gz"
  if [[ ! -f "${archive_path}" ]]; then
    echo "missing archive: ${archive_path}" >&2
    exit 1
  fi

  tag="${TAG_PREFIX}-${target}"
  manifest_digest="$(oras push \
    --disable-path-validation \
    --no-tty \
    --artifact-type "${ARTIFACT_TYPE}" \
    --annotation "org.opencontainers.image.source=https://github.com/garden-co/jazz2" \
    --annotation "org.opencontainers.image.description=Prebuilt RocksDB archive for ${target}" \
    --format json \
    "${GHCR_REPOSITORY}:${tag}" \
    "${archive_path}:${ARTIFACT_TYPE}" | jq -r '.digest')"

  archive_sha256="$(gzip -dc "${archive_path}" | sha256_stdin)"
  blob_sha256="$(sha256_file "${archive_path}")"

  manifest_entries+=("$(jq -n \
    --arg target "${target}" \
    --arg repository "${GHCR_REPOSITORY}" \
    --arg tag "${tag}" \
    --arg manifest_digest "${manifest_digest}" \
    --arg archive_sha256 "${archive_sha256}" \
    --arg blob_sha256 "${blob_sha256}" \
    '{target: $target, repository: $repository, tag: $tag, manifest_digest: $manifest_digest, archive_sha256: $archive_sha256, blob_sha256: $blob_sha256}')")
done

manifest_path="${OUTPUT_DIR}/manifest.json"
printf '%s\n' "${manifest_entries[@]}" | jq -s '.' > "${manifest_path}"
cat "${manifest_path}"
