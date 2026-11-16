# jazz-go

The Go runtime for Jazz: `package jazz`, which opens an app through the
[`jazz-ffi`](../../crates/jazz-ffi) C ABI with cgo. It uses only the standard
library: `context` for every call, channels for subscriptions and
`encoding/json` for rows.

Apps don't use it on its own. `jazz-tools generate` writes typed bindings,
a `jazzapp` package, beside a `go.mod` that requires this module. See
[examples/stage-plan/apps/go-cli](../../examples/stage-plan/apps/go-cli)
and the [Go setup page](https://jazz.tools/docs/setup/go).

Building needs `cargo build -p jazz-ffi` (or `--release`) from the repository root.
cgo finds the library in `target/release` (or `target/debug`); anywhere
else, set `CGO_LDFLAGS=-L<dir>`.
