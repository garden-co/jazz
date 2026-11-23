#!/usr/bin/env bash
# The one switch for where jazz-kotlin is released, read by release-publish.yml's Maven leg and the registry smoke.
#
#   kotlin-channel.sh            where a release publishes now
#   kotlin-channel.sh <version>  where that version is installed from
#
# github-release  the jar and AAR on the GitHub release; Maven Central is paused (jazz#6020)
# central         Maven Central
#
# To switch, change publish_to, and give the versions released meanwhile their own line in the case below. A new
# channel needs a publish step in release-publish.yml, an install in dev/adopter-gate/t9.sh and the docs.
set -euo pipefail
publish_to=github-release

case "${1:-}" in
  2.0.0-alpha.* | 2.0.0-beta.0) echo central ;; # released before the pause
  *) echo "${publish_to}" ;;
esac
