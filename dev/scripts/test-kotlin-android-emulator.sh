#!/usr/bin/env bash
# The Kotlin Android emulator smoke test: dev/kotlin-android-smoke, a minified
# app of the built jazz-kotlin-android AAR, installed on an x86-64 emulator,
# opens, writes, reads and syncs against a Jazz server and
# jazz-app-test-relay on this machine (adb reverse), and again after a
# process restart. Needs Linux x86-64 and /dev/kvm.
#
# usage: dev/scripts/test-kotlin-android-emulator.sh [<tools/jazz Maven tree>]
#   With a tree (release-artifacts' release-maven), tests that AAR and POM:
#   what ships. Without, builds an x86_64-only AAR from this checkout, with
#   CARGO_PROFILE (default dev) as dev/scripts/test-kotlin-android.sh does.
#
# JAZZ_TOOLS and JAZZ_APP_TEST_RELAY take prebuilt host binaries, as the
# Kotlin e2e tests do. KOTLIN_ANDROID_EMULATOR_ACCEL=off boots without KVM,
# for a machine that has none: 10 times slower or worse, and it needs about
# 6 GB of RAM. Never for CI.
set -euo pipefail
cd "$(dirname "$0")/../.."

# Pinned: the zips whose sha1s Google lists in repository2-3.xml and
# sys-img/aosp_atd/sys-img2-3.xml. The ATD image is made for headless tests.
platform_tools_url=https://dl.google.com/android/repository/platform-tools_r37.0.3-linux.zip
platform_tools_sha256=214548146cadb2ca3508143d8a1d4be4c59de5bb837253e3ede89ed74cf450d1
emulator_url=https://dl.google.com/android/repository/emulator-linux_x64-16970787.zip
emulator_sha256=a4405349d8a9c52136ab7861e5331dc40e85574bd91cb8889a298a2ff4c52e26
image_url=https://dl.google.com/android/repository/sys-img/aosp_atd/x86_64-35_r01.zip
image_sha256=2dffbc565912b97dd17d935b68002b248872e0c83473139856bb31634db249dc
image_dir=system-images/android-35/aosp_atd/x86_64

maven_tree="${1:-}"
if [ -n "$maven_tree" ]; then maven_tree="$(cd "$maven_tree" && pwd)"; fi

accel="${KOTLIN_ANDROID_EMULATOR_ACCEL:-on}"
boot_seconds=240
phase_seconds=150
if [ "$accel" = off ]; then
  # The emulator's own switch for software emulation.
  export ANDROID_I_WANT_MY_TCG=yes
  boot_seconds=2400
  phase_seconds=600
elif [ ! -r /dev/kvm ] || [ ! -w /dev/kvm ]; then
  echo "the emulator needs a readable and writable /dev/kvm" >&2
  exit 1
fi

export KOTLIN_TOOLCHAIN=pinned
source dev/scripts/kotlin-toolchain.sh
source dev/scripts/kotlin-android-sdk.sh
install_zip "$platform_tools_url" "$platform_tools_sha256" "$sdk/platform-tools"
install_zip "$emulator_url" "$emulator_sha256" "$sdk/emulator"
mkdir -p "$(dirname "$sdk/$image_dir")"
install_zip "$image_url" "$image_sha256" "$sdk/$image_dir"

work="$(mktemp -d)"
app=dev/kotlin-android-smoke
package=tools.jazz.smoke
# An emulator and adb server of this test's own: never another one's device.
# The last console port in the range adb supports (5554-5584).
console_port=5584
export ANDROID_ADB_SERVER_PORT=5647 ANDROID_SERIAL="emulator-$console_port"
adb="$sdk/platform-tools/adb"
emulator_pid='' server_pid='' relay_pid=''
passed=false
cleanup() {
  if [ "$passed" != true ]; then
    echo "--- the emulator's log" >&2
    tail -n 40 "$work/emulator.log" >&2 2>/dev/null || true
    if [ -n "$emulator_pid" ]; then
      echo "--- logcat" >&2
      timeout 20 "$adb" logcat -d -b main,crash 2>/dev/null | tail -n 150 >&2 || true
    fi
    echo "--- the server's and the relay's logs" >&2
    tail -n 30 "$work/server.log" "$work/relay.log" >&2 2>/dev/null || true
  fi
  for pid in $relay_pid $server_pid $emulator_pid; do kill "$pid" 2>/dev/null || true; done
  if [ -n "$emulator_pid" ]; then
    for _ in $(seq 50); do kill -0 "$emulator_pid" 2>/dev/null || break; sleep 0.2; done
    kill -KILL "$emulator_pid" 2>/dev/null || true
  fi
  timeout 10 "$adb" kill-server >/dev/null 2>&1 || true
  rm -rf "${work:?}"
}
trap cleanup EXIT
fail() { echo "kotlin-android-smoke: $*" >&2; exit 1; }
mark() { echo "kotlin-android-smoke: ${SECONDS}s: $*"; }

# The AVD by hand: avdmanager would need the command-line tools too.
export ANDROID_AVD_HOME="$work/avd" ANDROID_EMULATOR_HOME="$work/emulator-home"
mkdir -p "$ANDROID_AVD_HOME/jazz-smoke.avd" "$ANDROID_EMULATOR_HOME"
printf '%s\n' "avd.ini.encoding=UTF-8" "path=$ANDROID_AVD_HOME/jazz-smoke.avd" "target=android-35" \
  >"$ANDROID_AVD_HOME/jazz-smoke.ini"
printf '%s\n' "avd.ini.encoding=UTF-8" "AvdId=jazz-smoke" "PlayStore.enabled=false" \
  "abi.type=x86_64" "hw.cpu.arch=x86_64" "hw.cpu.ncore=4" "hw.ramSize=2048" \
  "hw.lcd.width=320" "hw.lcd.height=640" "hw.lcd.density=160" "hw.gpu.enabled=yes" \
  "hw.gpu.mode=swiftshader_indirect" "disk.dataPartition.size=2G" "tag.id=aosp_atd" \
  "image.sysdir.1=$image_dir/" >"$ANDROID_AVD_HOME/jazz-smoke.avd/config.ini"

# Boots while everything builds.
"$adb" start-server
"$sdk/emulator/emulator" -avd jazz-smoke -port "$console_port" -accel "$accel" \
  -no-window -no-audio -no-boot-anim -no-snapshot -no-metrics -gpu swiftshader_indirect \
  >"$work/emulator.log" 2>&1 &
emulator_pid=$!
mark "emulator starting"

version="$(sed -n 's/^  "version": "\(.*\)",$/\1/p' packages/jazz-tools/package.json)"
[ -n "$version" ] || fail "no version in packages/jazz-tools/package.json"
published="$work/repository/tools/jazz"
artifact=jazz-kotlin-android
mkdir -p "$published"
if [ -n "$maven_tree" ]; then
  [ -f "$maven_tree/$artifact/$version/$artifact-$version.aar" ] || fail "$maven_tree has no $artifact $version"
  cp -R "$maven_tree/$artifact" "$published/"
else
  # The POM and module as publishing writes them, without its signing.
  KOTLIN_ANDROID_ABIS=x86_64 \
    KOTLIN_ANDROID_TASKS="generatePomFileForMavenPublication generateMetadataFileForMavenPublication" \
    bash dev/scripts/test-kotlin-android.sh
  built=packages/jazz-kotlin-android/build
  mkdir -p "$published/$artifact/$version"
  cp "$built/outputs/aar/$artifact-release.aar" "$published/$artifact/$version/$artifact-$version.aar"
  cp "$built/publications/maven/pom-default.xml" "$published/$artifact/$version/$artifact-$version.pom"
  cp "$built/publications/maven/module.json" "$published/$artifact/$version/$artifact-$version.module"
fi
echo "testing $(sha256sum "$published/$artifact/$version/$artifact-$version.aar")"
mark "AAR ready"

gradle --no-daemon --console=plain -p "$app" \
  -PjazzRepository="$work/repository" -PjazzVersion="$version" assembleRelease
mark "app built"

if [ -z "${JAZZ_TOOLS:-}" ]; then
  cargo build -p jazz-tools --bin jazz-tools
  JAZZ_TOOLS="${CARGO_TARGET_DIR:-target}/debug/jazz-tools"
fi
if [ -z "${JAZZ_APP_TEST_RELAY:-}" ]; then
  cargo build -p jazz-app-support --features test-relay --bin jazz-app-test-relay
  JAZZ_APP_TEST_RELAY="${CARGO_TARGET_DIR:-target}/debug/jazz-app-test-relay"
fi

# The app's backend, a test harness: a Jazz server and the stand-in for a
# backend's Jazz endpoint, which publishes StagePlan's permissions.
app_id="$(cat /proc/sys/kernel/random/uuid)"
export JAZZ_BACKEND_SECRET="backend-$app_id" JAZZ_ADMIN_SECRET="admin-$app_id"
stage_plan=examples/stage-plan/apps/kotlin-cli
port=41635
"$JAZZ_TOOLS" server "$app_id" --in-memory --port "$port" >"$work/server.log" 2>&1 &
server_pid=$!
for _ in $(seq 300); do
  kill -0 "$server_pid" 2>/dev/null || fail "jazz-tools server exited"
  if (echo >"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then break; fi
  sleep 0.1
done
"$JAZZ_APP_TEST_RELAY" --core "http://127.0.0.1:$port" --app-id "$app_id" \
  --app "$stage_plan/jazz.app.json" --permissions "$stage_plan/jazz.permissions.json" \
  >"$work/relay.out" 2>"$work/relay.log" &
relay_pid=$!
for _ in $(seq 300); do
  kill -0 "$relay_pid" 2>/dev/null || fail "jazz-app-test-relay exited"
  if grep -q '^http' "$work/relay.out"; then break; fi
  sleep 0.1
done
endpoint="$(grep -m1 '^http' "$work/relay.out")" || fail "jazz-app-test-relay didn't start"
relay_port="${endpoint##*:}"
relay_port="${relay_port%%/*}"
mark "server and relay up"

deadline=$((SECONDS + boot_seconds))
until [ "$(timeout 15 "$adb" shell getprop sys.boot_completed 2>/dev/null | tr -d '\r')" = 1 ]; do
  kill -0 "$emulator_pid" 2>/dev/null || fail "the emulator exited before it booted"
  [ "$SECONDS" -lt "$deadline" ] || fail "the emulator didn't boot within ${boot_seconds}s"
  sleep 2
done
mark "emulator booted"

# boot_completed can come just before the package manager answers.
apk="$app/build/outputs/apk/release/kotlin-android-smoke-release.apk"
deadline=$((SECONDS + phase_seconds))
until timeout "$phase_seconds" "$adb" install -r "$apk"; do
  [ "$SECONDS" -lt "$deadline" ] || fail "the app didn't install"
  sleep 3
done
timeout 20 "$adb" reverse "tcp:$relay_port" "tcp:$relay_port"
mark "app installed"

run="run-$app_id"
for phase in write restart; do
  timeout 30 "$adb" shell am force-stop "$package"
  timeout 30 "$adb" logcat -c
  timeout 90 "$adb" shell am start -n "$package/smoke.SmokeActivity" \
    --es run "$run" --es phase "$phase" --es endpoint "$endpoint" --es appId "$app_id"
  deadline=$((SECONDS + phase_seconds))
  while :; do
    log="$(timeout 20 "$adb" logcat -d -v brief -s JazzSmoke:V AndroidRuntime:E libc:F 2>/dev/null || true)"
    if grep -q "$run $phase PASS" <<<"$log"; then break; fi
    if grep -q "$run $phase FAIL\|FATAL EXCEPTION.*\|Fatal signal" <<<"$log"; then
      echo "$log" >&2
      fail "$phase failed"
    fi
    [ "$SECONDS" -lt "$deadline" ] || { echo "$log" >&2; fail "$phase gave no result in ${phase_seconds}s"; }
    sleep 1
  done
  grep 'JazzSmoke' <<<"$log"
  mark "$phase passed"
done
passed=true
mark "passed"
