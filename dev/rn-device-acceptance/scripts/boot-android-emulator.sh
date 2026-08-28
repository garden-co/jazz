#!/usr/bin/env bash
# Start the acceptance emulator and wait for it using only bounded probes.  In
# particular, do not use `adb wait-for-device`: it has no deadline and can
# outlive the workflow step when an emulator never registers with adb.
set -euo pipefail

if [[ $# -ne 3 ]]; then
  echo "usage: $0 <avd-name> <emulator-log> <avd-config>" >&2
  exit 2
fi

avd_name=$1
emulator_log=$2
avd_config=$3
emulator=${JAZZ_DEVICE_EMULATOR:-emulator}
adb=${JAZZ_DEVICE_ADB:-adb}
# `setsid` is deliberately the production default: Android can leave a QEMU
# child behind, so the receipt must be able to terminate the whole launcher
# session. The command and cleanup mode are injectable only to let this
# black-box receipt run on macOS, where `setsid` is not available.
session_launcher=${JAZZ_ANDROID_SESSION_LAUNCHER:-setsid}
session_process_group=${JAZZ_ANDROID_SESSION_PROCESS_GROUP:-1}
timeout_command=${JAZZ_ANDROID_TIMEOUT_COMMAND:-timeout}
boot_timeout=${JAZZ_ANDROID_BOOT_TIMEOUT_SECONDS:-180}
poll_interval=${JAZZ_ANDROID_BOOT_POLL_SECONDS:-2}

if ! [[ "$boot_timeout" =~ ^[1-9][0-9]*$ ]]; then
  echo "JAZZ_ANDROID_BOOT_TIMEOUT_SECONDS must be a positive integer" >&2
  exit 2
fi
if ! [[ "$poll_interval" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
  echo "JAZZ_ANDROID_BOOT_POLL_SECONDS must be a non-negative number" >&2
  exit 2
fi
if [[ "$session_process_group" != 0 && "$session_process_group" != 1 ]]; then
  echo "JAZZ_ANDROID_SESSION_PROCESS_GROUP must be 0 or 1" >&2
  exit 2
fi

bounded_file() {
  local label=$1 file=$2
  echo "--- $label (last 120 lines, at most 16 KiB) ---" >&2
  if [[ -r "$file" ]]; then
    tail -n 120 "$file" | tail -c 16384 >&2 || true
  else
    echo "<unavailable: $file>" >&2
  fi
}

emulator_pid=''
keep_emulator=0
cleanup() {
  # The Android launcher can leave a QEMU child behind.  Start it in a fresh
  # session and signal that whole process group, rather than merely killing
  # the launcher shell.
  if ((keep_emulator == 0)) && [[ -n "$emulator_pid" ]]; then
    if ((session_process_group == 1)) && kill -0 -- "-$emulator_pid" 2>/dev/null; then
      kill -- "-$emulator_pid" 2>/dev/null || true
      for _ in {1..10}; do
        kill -0 -- "-$emulator_pid" 2>/dev/null || break
        sleep 0.1
      done
      kill -KILL -- "-$emulator_pid" 2>/dev/null || true
    elif kill -0 "$emulator_pid" 2>/dev/null; then
      # The macOS test launcher does not create a session. Do not pretend this
      # fallback can clean descendants; it only prevents its direct fixture
      # process from leaking. Production always uses the group path above.
      kill "$emulator_pid" 2>/dev/null || true
    fi
    wait "$emulator_pid" 2>/dev/null || true
  fi
}
trap cleanup EXIT
# A cancellation must terminate this receipt after removing the complete
# emulator process group; merely running cleanup and resuming the polling loop
# would leave the workflow step alive.
trap 'cleanup; exit 130' INT
trap 'cleanup; exit 143' TERM

fail_boot() {
  echo "Android acceptance emulator failed to boot: $1" >&2
  bounded_file "emulator log" "$emulator_log"
  bounded_file "AVD config" "$avd_config"
  exit 1
}

# A dedicated session gives cleanup a single, bounded target even when the
# launcher spawns the actual emulator process.
"$session_launcher" "$emulator" -avd "$avd_name" -no-window -no-audio -gpu swiftshader_indirect \
  -no-boot-anim -no-metrics >"$emulator_log" 2>&1 &
emulator_pid=$!
deadline=$((SECONDS + boot_timeout))

while :; do
  if ! kill -0 "$emulator_pid" 2>/dev/null; then
    wait "$emulator_pid" || true
    fail_boot "emulator process exited before boot completed"
  fi

  remaining=$((deadline - SECONDS))
  if ((remaining <= 0)); then
    fail_boot "no adb device reached sys.boot_completed=1 within ${boot_timeout}s"
  fi
  probe_timeout=$remaining
  if ((probe_timeout > 5)); then probe_timeout=5; fi

  # Both commands are independently bounded so a wedged adb server cannot
  # consume the workflow's full deadline. A missing device is an ordinary
  # polling state, not a fatal command error.
  state=$("$timeout_command" --signal=KILL "${probe_timeout}s" "$adb" get-state 2>/dev/null || true)
  if [[ "$state" == device ]]; then
    # `get-state` may have consumed nearly all of the remaining time. Do not
    # begin a second adb probe using its stale budget.
    remaining=$((deadline - SECONDS))
    if ((remaining <= 0)); then
      fail_boot "no adb device reached sys.boot_completed=1 within ${boot_timeout}s"
    fi
    probe_timeout=$remaining
    if ((probe_timeout > 5)); then probe_timeout=5; fi
    boot_completed=$("$timeout_command" --signal=KILL "${probe_timeout}s" "$adb" shell getprop sys.boot_completed 2>/dev/null | tr -d '\r' || true)
    if [[ "$boot_completed" == 1 ]]; then
      echo "Android acceptance emulator booted (pid $emulator_pid)."
      # The next workflow commands install and exercise the app on this same
      # emulator. Failures/cancellation still clean it up through the trap.
      keep_emulator=1
      exit 0
    fi
  fi

  # The probes above may have consumed their entire allowance. Recompute the
  # remaining budget so the poll itself cannot extend the one boot deadline.
  remaining=$((deadline - SECONDS))
  if ((remaining <= 0)); then
    fail_boot "no adb device reached sys.boot_completed=1 within ${boot_timeout}s"
  fi
  "$timeout_command" --signal=KILL "${remaining}s" sleep "$poll_interval" || true
done
