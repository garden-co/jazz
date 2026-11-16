#!/usr/bin/env bash
# The Kotlin partition: jazz-kotlin's tests and the StagePlan Kotlin CLI, an
# end-user client, end to end against a StagePlan backend, on the JVM. Fetches a JDK and
# Gradle into ~/.cache/jazz-kotlin when none is on PATH.
set -euo pipefail
cd "$(dirname "$0")/../.."

canonical=examples/stage-plan/apps/react-localfirst
example=examples/stage-plan/apps/kotlin-cli
for file in jazz.app.json jazz.permissions.json; do
  if ! cmp -s "$canonical/$file" "$example/$file"; then
    echo "$example/$file differs from $canonical/$file: copy it over (never edit StagePlan's files for Kotlin)" >&2
    exit 1
  fi
done

source dev/scripts/kotlin-toolchain.sh

cargo build -p jazz-ffi -p jazz-tools --bin jazz-tools
# The e2e tests' StagePlan backend, a test harness (Kotlin is a client): a
# Jazz server and the test stand-in for its Jazz endpoint.
cargo build -p jazz-app-support --features test-relay --bin jazz-app-test-relay
# Kotlin is switched off in the CLI until it ships: the codegen golden test
# checks the example's committed bindings byte for byte instead.
cargo test -p jazz-codegen --test golden the_stage_plan_kotlin_cli_commits_its_bindings_byte_for_byte

gradle --no-daemon --console=plain -p packages/jazz-kotlin test
gradle --no-daemon --console=plain -p "$example" test
