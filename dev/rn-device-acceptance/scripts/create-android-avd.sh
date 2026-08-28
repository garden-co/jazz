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

# Do not use `yes no`: that is the `yes` program with `no` as its repeated
# output, so it can flood avdmanager past its one prompt. One explicit `no`
# followed by EOF both selects the pinned default and makes an unexpected
# additional prompt fail rather than silently accepting a changed tool grammar.
printf 'no\n' | "$avdmanager" create avd --force --name "$1" --package "$2"
