# jazz-go

The Go runtime for Jazz: `package jazz`, which opens an app through the
[`jazz-ffi`](../../crates/jazz-ffi) C ABI with cgo. It uses only the standard
library: `context` for every call, channels for subscriptions and
`encoding/json` for rows.

Apps don't use it on its own. `jazz-tools generate` writes typed bindings,
a `jazzapp` package, beside a `go.mod` that requires this module. See
[examples/stage-plan/apps/go-cli](../../examples/stage-plan/apps/go-cli)
and the [Go setup page](https://jazz.tools/docs/setup/go).

Install it with `go get github.com/garden-co/jazz/packages/jazz-go/v2@v2.0.0-alpha.65`
and build as usual: no cgo, no C compiler, no download. Its versions are
Jazz's, in lockstep (each release tags `packages/jazz-go/v<version>`).

jazz-go calls Jazz's core (`crates/jazz-ffi`, whose C ABI is
`include/jazz_ffi.h`) through [purego](https://github.com/ebitengine/purego),
loading its shared library at run time. Released modules embed it for
linux/amd64, linux/arm64 and darwin/arm64 (`dev/scripts/go-embed-ffi-libraries.sh`
adds them on the release commit) and extract it once into the user cache
dir. Elsewhere, or to use another build, set `JAZZ_FFI_LIBRARY` to a
`libjazz_ffi.so` / `.dylib`. Inside this repository it loads
`target/release` or `target/debug`'s (`cargo build -p jazz-ffi`).
`include/jazz_ffi.h` is a copy of `crates/jazz-ffi/include/jazz_ffi.h`; a
test and the `go` CI partition check that it stays the same.
