#!/usr/bin/env bash
# The Swift bindings on Linux: the StagePlan Swift example's app files are
# byte-identical to StagePlan's, its committed JazzApp.swift is what the Swift
# back end writes, and the Jazz Swift package and the example build and pass
# their tests against libjazz_ffi built from this tree. The example's tests
# start a jazz-tools server each; then the CLI demo runs against one.
# Uses the swift on PATH if it is 6.0 or newer, else downloads a pinned one.
set -euo pipefail

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
work=$(mktemp -d)
server_pid=
relay_pid=
cleanup() {
  [[ -n "$relay_pid" ]] && kill "$relay_pid" 2>/dev/null || true
  [[ -n "$server_pid" ]] && kill "$server_pid" 2>/dev/null || true
  rm -rf "$work"
}
trap cleanup EXIT
example="$root/examples/stage-plan/apps/swift-cli"
canonical="$root/examples/stage-plan/apps/react-localfirst"

for file in jazz.app.json jazz.permissions.json; do
  cmp "$canonical/$file" "$example/$file" || {
    echo "examples/stage-plan/apps/swift-cli/$file differs from react-localfirst's: copy it, never edit it" >&2
    exit 1
  }
done

swift_major=$(swift --version 2>/dev/null | sed -n 's/.*Swift version \([0-9]*\)\..*/\1/p' | head -1 || true)
if [[ -z "$swift_major" || "$swift_major" -lt 6 ]]; then
  swift_version=6.2.1
  swift_sha256=4022cb64faf7e2681c19f9b62a22fb7d9055db6194d9e4a4bef9107b6ce10946
  curl -sSfL -o "$work/swift.tgz" \
    "https://download.swift.org/swift-$swift_version-release/ubuntu2404/swift-$swift_version-RELEASE/swift-$swift_version-RELEASE-ubuntu24.04.tar.gz"
  echo "$swift_sha256  $work/swift.tgz" | sha256sum -c -
  mkdir -p "$work/swift" && tar xzf "$work/swift.tgz" -C "$work/swift" --strip-components=1
  PATH="$work/swift/usr/bin:$PATH"
fi
swift --version

cd "$root"
cargo test -q -p jazz-codegen --test golden -- swift
cargo build -q -p jazz-cli --bin jazz-tools
"$root/target/debug/jazz-tools" generate --check --dir "$example"
# The app endpoint the end-user tests sign in through.
cargo build -q -p jazz-app --features test-relay --bin jazz-app-test-relay
# The committed UniFFI sources match what uniffi-bindgen generates from
# this tree's jazz-ffi; then the library and its jazz_ffi.pc.
"$root/packages/jazz-swift/scripts/build-ffi.sh" --debug --check
"$root/packages/jazz-swift/scripts/build-ffi.sh" --debug --lib-only
export PKG_CONFIG_PATH="$root/target/debug/jazz-ffi-static${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}"
export JAZZ_TOOLS_BIN="$root/target/debug/jazz-tools"
export JAZZ_TEST_RELAY_BIN="$root/target/debug/jazz-app-test-relay"
export CI="${CI:-1}"

# The root Package.swift is the Jazz Swift package.
(cd "$root" && swift test)
(cd "$example" && swift test)

# The CLI as an end user: a sync server, and in front of it the app's
# endpoint (the test relay standing in for the TypeScript backend's
# `createJazz` handler), which publishes StagePlan's permissions.
app_id=00000000-0000-4000-8000-00000057a9e0
export JAZZ_BACKEND_SECRET=gate-backend-secret
export JAZZ_ADMIN_SECRET=gate-admin-secret
"$JAZZ_TOOLS_BIN" server "$app_id" --in-memory --port 41625 \
  --backend-secret "$JAZZ_BACKEND_SECRET" --admin-secret "$JAZZ_ADMIN_SECRET" > "$work/server.log" 2>&1 &
server_pid=$!
for _ in $(seq 100); do grep -q "Listening on" "$work/server.log" && break; sleep 0.1; done
"$JAZZ_TEST_RELAY_BIN" --core http://127.0.0.1:41625 --app-id "$app_id" \
  --app "$example/jazz.app.json" --permissions "$example/jazz.permissions.json" > "$work/relay.out" 2>&1 &
relay_pid=$!
for _ in $(seq 300); do grep -q "^http" "$work/relay.out" && break; sleep 0.1; done
export JAZZ_ENDPOINT=$(grep -m1 "^http" "$work/relay.out")
export JAZZ_APP_ID=$app_id
(cd "$example" && swift run stage-plan-swift --key-file "$work/device-key" demo) | tee "$work/demo.out"
grep -q "doing   Load in" "$work/demo.out"
