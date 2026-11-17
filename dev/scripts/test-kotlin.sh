#!/usr/bin/env bash
# The Kotlin partition: jazz-kotlin's tests, the StagePlan Kotlin CLI, an
# end-user client, end to end against a StagePlan backend, and the docs todo
# app's Kotlin snippets (examples/docs/todo-client-kotlin), on the JVM. Fetches a JDK and
# Gradle into ~/.cache/jazz-kotlin when none is on PATH.
set -euo pipefail
cd "$(dirname "$0")/../.."

example=examples/stage-plan/apps/kotlin-cli
docs_example=examples/docs/todo-client-kotlin
# Their jazz.app.json / jazz.permissions.json come from schema.ts (a re-export of
# StagePlan's, and of the docs todo app's); dev/gates/example-bindings.mjs
# checks they're current.

source dev/scripts/kotlin-toolchain.sh

cargo build -p jazz-ffi -p jazz-tools --bin jazz-tools
# The e2e tests' backend, a test harness (Kotlin is a client): a
# Jazz server and the test stand-in for its Jazz endpoint.
cargo build -p jazz-app-support --features test-relay --bin jazz-app-test-relay
# Kotlin is switched off in the CLI until it ships: the codegen golden test
# checks the examples' committed bindings byte for byte instead.
cargo test -p jazz-codegen --test golden the_stage_plan_kotlin_cli_commits_its_bindings_byte_for_byte
cargo test -p jazz-codegen --test golden the_docs_todo_kotlin_client_commits_its_bindings_byte_for_byte

gradle --no-daemon --console=plain -p packages/jazz-kotlin test
gradle --no-daemon --console=plain -p "$example" test
gradle --no-daemon --console=plain -p "$docs_example" test
