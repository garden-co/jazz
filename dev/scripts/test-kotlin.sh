#!/usr/bin/env bash
# The Kotlin partition: jazz-kotlin's tests and the StagePlan Kotlin CLI
# end to end against `jazz-tools server`, on the JVM. Fetches a JDK and
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

cargo build -p jazz-ffi -p jazz-cli --bin jazz-tools  # jazz-tools: the e2e tests' server
cargo build -p jazz-app --features test-relay --bin jazz-app-test-relay  # the end-user tests' app endpoint
# Kotlin is switched off in the CLI until it ships: the codegen golden test
# checks the example's committed bindings byte for byte instead.
cargo test -p jazz-codegen --test golden the_stage_plan_kotlin_cli_commits_its_bindings_byte_for_byte

gradle --no-daemon --console=plain -p packages/jazz-kotlin test
gradle --no-daemon --console=plain -p "$example" test
