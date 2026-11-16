#!/usr/bin/env bash
# The Go bindings: packages/jazz-go (over crates/jazz-ffi's C ABI) and the
# StagePlan Go example. Their committed bindings are current, gofmt and go
# vet pass on them and on every jazz-codegen Go golden, and their tests
# (which start a jazz-tools server on a memory store) pass.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${root}"

go_version="1.23.4"
go_sha256="6924efde5de86fe277676e929dc9917d466efa02fb934197bc2eba35d5680971"
if ! command -v go >/dev/null 2>&1; then
  go_root="${RUNNER_TEMP:-/tmp}/go-${go_version}"
  if [ ! -x "${go_root}/go/bin/go" ]; then
    curl -fsSL "https://go.dev/dl/go${go_version}.linux-amd64.tar.gz" -o /tmp/go.tgz
    echo "${go_sha256}  /tmp/go.tgz" | sha256sum -c -
    mkdir -p "${go_root}"
    tar -C "${go_root}" -xzf /tmp/go.tgz
  fi
  export PATH="${go_root}/go/bin:${PATH}"
fi
git config --global --add safe.directory "${root}" || true

cargo build -p jazz-ffi
cargo build -p jazz-tools --bin jazz-tools
# The tests' stand-in for a TS main backend's /jazz (jazz#5754).
cargo build -p jazz-app-support --features test-relay --bin jazz-app-test-relay
cargo test -p jazz-codegen --test golden the_stage_plan_go_example_commits_current_bindings
(cd examples/stage-plan/apps/go-cli && ../../../../target/debug/jazz-tools generate --check)

test -z "$(gofmt -l packages/jazz-go examples/stage-plan/apps/go-cli)"
# The module ships jazz-ffi's header for builds from the module cache.
cmp crates/jazz-ffi/include/jazz_ffi.h packages/jazz-go/include/jazz_ffi.h
(cd packages/jazz-go && go vet ./...)
(cd examples/stage-plan/apps/go-cli && go vet ./...)

# Each committed golden, as the jazzapp package of a module of its own.
for dir in crates/jazz-codegen/tests/apps/*/jazzapp crates/jazz-codegen/tests/go-apps/*/jazzapp; do
  [ -d "${dir}" ] || continue
  module="$(mktemp -d)"
  mkdir "${module}/jazzapp"
  cp "${dir}/jazz_app.go" "${module}/jazzapp/"
  printf 'module example.com/golden\n\ngo 1.22\n\nrequire github.com/garden-co/jazz/packages/jazz-go/v2 v2.0.0\n\nreplace github.com/garden-co/jazz/packages/jazz-go/v2 => %s/packages/jazz-go\n' "${root}" > "${module}/go.mod"
  (cd "${module}" && GOFLAGS=-mod=mod go vet ./... && test -z "$(gofmt -l .)") || { echo "${dir} fails go vet or gofmt"; exit 1; }
done

# jazz-go needs no cgo: it loads target/debug's libjazz_ffi.so at run time.
(cd packages/jazz-go && CGO_ENABLED=0 go build ./... && CGO_ENABLED=0 go vet ./...)
(cd examples/stage-plan/apps/go-cli && CGO_ENABLED=0 go build ./...)
# The race detector needs cgo; purego runs under it too.
(cd packages/jazz-go && CGO_ENABLED=1 go test -race ./...)
(cd examples/stage-plan/apps/go-cli && CGO_ENABLED=1 go test -race -v ./...)

# The same suite with StagePlan's main backend in TypeScript (createJazz,
# go-cli/ts-main) as the server, and Go backend clients of it: needs the
# repository's jazz-tools build (the go partition makes it).
if [ "${JAZZ_GO_TS_MAIN_BACKEND:-}" = "1" ]; then
  (cd examples/stage-plan/apps/go-cli && JAZZ_MAIN_BACKEND=ts CGO_ENABLED=1 go test -race -v -count=1 ./...)
fi
