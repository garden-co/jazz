#!/usr/bin/env bash
# Post-publish smoke from the real registries: each target installs the published version into a clean project
# outside the repo and runs the t9 privacy checks against a stock TS createJazz backend on npm's jazz-tools.
# Usage: registry-smoke.sh <version, e.g. 2.0.0-alpha.68> npm|create-jazz|crates|go|swift|kotlin
# Registries can lag (Maven Central ~15-30 min): set AG_WAIT_MIN to poll for availability first (default 0).
. "$(dirname "$0")/lib.sh"
V=$1; T=$2; export AG_TGZ=$AG/.reg/$V AG_REGISTRY=1
available() { case $T in
  npm|create-jazz) npm view "jazz-tools@$V" version >/dev/null 2>&1 && npm view "create-jazz@$V" version >/dev/null 2>&1 ;;
  crates) curl -sf -A jazz-adopter-gate "https://crates.io/api/v1/crates/jazz-app-support/$V" >/dev/null ;;
  go) curl -sf "https://proxy.golang.org/github.com/garden-co/jazz/packages/jazz-go/v2/@v/v$V.info" >/dev/null ;;
  swift) git ls-remote --tags https://github.com/garden-co/jazz-swift | grep -q "refs/tags/$V$" ;;
  kotlin) curl -sf -o /dev/null "https://repo1.maven.org/maven2/tools/jazz/jazz-kotlin/$V/jazz-kotlin-$V.pom" ;;
esac; }
end=$(( $(date +%s) + ${AG_WAIT_MIN:-0} * 60 )); until available; do [ "$(date +%s)" -ge $end ] && break; sleep 30; done
available || { echo "FAIL registry-$T: $V not available (waited ${AG_WAIT_MIN:-0} min)" | tee "$AG_RESULTS/registry-$T.txt"; exit 1; }
# The TS backend runs on npm's packages (packed into AG_TGZ, which t9.sh installs from).
mkdir -p "$AG_TGZ" && ( cd "$AG_TGZ" && echo "$V" > VERSION && ag_retry 3 npm pack "jazz-tools@$V" "jazz-wasm@$V" "jazz-napi@$V" "@garden-co/jazz-napi-linux-x64-gnu@$V" >/dev/null 2>&1 ) \
  || { echo "FAIL registry-$T: npm pack $V" | tee "$AG_RESULTS/registry-$T.txt"; exit 1; }
case $T in
  npm) body() { local W=$AG/.work/npm-plain; rm -rf "$W"; mkdir -p "$W" && cd "$W" && echo '{"name":"plain","private":true,"type":"module"}' > package.json
         ag_retry 3 npm install "jazz-tools@$V" ws@8 > install.log 2>&1 && echo "PASS npm install jazz-tools@$V" || { echo "FAIL npm install"; tail -5 install.log; return; }
         node -e 'import("jazz-tools/backend").then(m => process.exit(m.createJazz ? 0 : 1))' && echo "PASS jazz-tools/backend imports" || echo "FAIL jazz-tools/backend import"
         "$AG/t9.sh" ts | grep -E "^(PASS|FAIL) t9 ts"; }
       ag_job registry-npm body ;;
  create-jazz) body() { local W=$AG/.work/create; rm -rf "$W"; mkdir -p "$W" && cd "$W" && npm create "jazz@$V" < /dev/null > out.txt 2>&1
         grep -q "Agent Quickstart" out.txt && echo "PASS npm create jazz@$V prints the Agent Quickstart" || { echo "FAIL npm create jazz output"; head -5 out.txt; }; }
       ag_job registry-create-jazz body ;;
  crates) AG_RESULTS=$AG_RESULTS "$AG/t9.sh" rust; cp "$AG_RESULTS/t9-rust.txt" "$AG_RESULTS/registry-crates.txt" ;;
  go) "$AG/t9.sh" go; cp "$AG_RESULTS/t9-go.txt" "$AG_RESULTS/registry-go.txt" ;;
  kotlin) "$AG/t9.sh" kotlin; cp "$AG_RESULTS/t9-kotlin.txt" "$AG_RESULTS/registry-kotlin.txt" ;;
  swift) FETCH_TAG=v$V FETCH_DEST=$AG_TGZ FETCH_NAMES=libjazz_ffi-linux-x86_64.tar.gz bash "$AG/../../.github/actions/fetch-release-artifacts/fetch.sh" >/dev/null \
           || { echo "FAIL registry-swift: release libjazz_ffi" | tee "$AG_RESULTS/registry-swift.txt"; exit 1; }
         "$AG/t9.sh" swift; cp "$AG_RESULTS/t9-swift.txt" "$AG_RESULTS/registry-swift.txt" ;;
  *) echo "FAIL unknown target $T"; exit 1 ;;
esac
! grep -q "^FAIL" "$AG_RESULTS/registry-$T.txt"
