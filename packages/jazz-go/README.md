# jazz-go

The Go runtime for Jazz: `package jazz`, which opens an app through the
[`jazz-ffi`](../../crates/jazz-ffi) C ABI with cgo. It uses only the standard
library: `context` for every call, channels for subscriptions and
`encoding/json` for rows.

Apps don't use it on its own. `jazz-tools generate` writes typed bindings,
a `jazzapp` package, beside a `go.mod` that requires this module. See
[examples/stage-plan/apps/go-cli](../../examples/stage-plan/apps/go-cli)
and the [Go setup page](https://jazz.tools/docs/setup/go).

Install it with `go get github.com/garden-co/jazz/packages/jazz-go/v2@v2.0.0-alpha.65`.
Its versions are Jazz's, in lockstep: each release tags it as
`packages/jazz-go/v<version>` (e.g. `packages/jazz-go/v2.0.0-alpha.65`), so
its path ends in `/v2`. Building needs cgo and `libjazz_ffi.a` for your platform,
from the GitHub release or `cargo build --release -p jazz-ffi`, with
`CGO_LDFLAGS=-L<its directory>`. Inside this repository cgo also finds it in
`target/debug` and `target/release`. `include/jazz_ffi.h` is a copy of
`crates/jazz-ffi/include/jazz_ffi.h`; a test and the `go` CI partition check
that it stays the same.
