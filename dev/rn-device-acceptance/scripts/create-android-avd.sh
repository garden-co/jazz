#!/usr/bin/env bash
# Create the pinned acceptance AVD without leaving a live prompt or an
# unbounded stdin producer behind. `avdmanager` asks exactly once whether to
# create a custom hardware profile; the documented default is `no`.
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 <avd-name> <system-image-package>" >&2
  exit 2
fi

avdmanager=${JAZZ_DEVICE_AVDMANAGER:-avdmanager}
avd_name=$1
avd_home=${ANDROID_AVD_HOME:-}

# `avdmanager` only treats ANDROID_AVD_HOME as an AVD root when that directory
# already exists.  Without this, it can successfully create the device under
# its global default while the emulator (which *does* honor the configured
# home) subsequently reports an unknown AVD.  This receipt must never fall
# back to a runner-global AVD location.
if [[ -z "$avd_home" ]]; then
  echo "Android acceptance requires ANDROID_AVD_HOME to keep the AVD lane-local" >&2
  exit 2
fi
mkdir -p "$avd_home"

# Do not use `yes no`: that is the `yes` program with `no` as its repeated
# output, so it can flood avdmanager past its one prompt. One explicit `no`
# followed by EOF both selects the pinned default and makes an unexpected
# additional prompt fail rather than silently accepting a changed tool grammar.
printf 'no\n' | "$avdmanager" create avd --force --name "$avd_name" --package "$2"

avd_ini="$avd_home/$avd_name.ini"
avd_config="$avd_home/$avd_name.avd/config.ini"
if [[ ! -f "$avd_ini" || ! -f "$avd_config" ]]; then
  echo "Android acceptance AVD creation did not produce the configured lane-local device" >&2
  echo "expected AVD registration: $avd_ini" >&2
  echo "expected AVD configuration: $avd_config" >&2
  echo "--- configured AVD home (files, depth <= 2) ---" >&2
  find "$avd_home" -maxdepth 2 -type f -print 2>/dev/null | head -n 120 >&2 || true
  exit 1
fi
