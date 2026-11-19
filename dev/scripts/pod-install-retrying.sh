#!/usr/bin/env bash
# `pod install` for CI, robust against a slow or stalled download.
#
#   pod-install-retrying.sh <artifact-cache-dir>   (run in the ios/ directory)
#
# React Native's podspecs curl their prebuilt tarballs (Hermes,
# ReactNativeDependencies, ReactNativeCore; debug and release) from Maven into
# Pods/*-artifacts with no retry or timeout, and `expo prebuild --clean` wipes
# Pods/ first, so every run downloaded them again: one slow Maven edge once
# took this step from ~80 s to ~10 min. So:
#   - the tarballs come from <artifact-cache-dir> (which the caller caches) and
#     go back there afterwards; React Native skips a tarball that exists;
#   - every curl that pod install starts (React Native's and CocoaPods' own)
#     gives up on a transfer slower than 100 kB/s for 20 s and retries it;
#   - pod install itself gets three attempts of at most POD_INSTALL_TIMEOUT
#     seconds (default 300) each.
set -euo pipefail

cache=${1:?usage: pod-install-retrying.sh <artifact-cache-dir>}
timeout_seconds=${POD_INSTALL_TIMEOUT:-300}
artifact_dirs=(hermes-engine-artifacts ReactNativeDependencies-artifacts ReactNativeCore-artifacts)

mkdir -p "$cache" Pods
for dir in "${artifact_dirs[@]}"; do
  if [[ -d "$cache/$dir" ]]; then
    mkdir -p "Pods/$dir"
    # Only finished tarballs: React Native downloads to *.download first.
    find "$cache/$dir" -maxdepth 1 -name '*.tar.gz' -exec cp {} "Pods/$dir/" \;
  fi
done

curl_home=$(mktemp -d)
cat >"$curl_home/.curlrc" <<'CURLRC'
connect-timeout = 20
speed-limit = 100000
speed-time = 20
retry = 3
retry-all-errors
retry-delay = 5
CURLRC
export CURL_HOME="$curl_home"

for attempt in 1 2 3; do
  # perl's alarm survives exec: a stalled pod install is killed, not waited on.
  if perl -e 'alarm shift; exec @ARGV' "$timeout_seconds" pod install; then
    break
  fi
  if ((attempt == 3)); then
    echo "pod install failed three times" >&2
    exit 1
  fi
  echo "::warning::pod install attempt ${attempt} failed or exceeded ${timeout_seconds}s; retrying"
  sleep $((attempt * 15))
done

for dir in "${artifact_dirs[@]}"; do
  if [[ -d "Pods/$dir" ]]; then
    mkdir -p "$cache/$dir"
    find "Pods/$dir" -maxdepth 1 -name '*.tar.gz' -exec cp {} "$cache/$dir/" \;
  fi
done
