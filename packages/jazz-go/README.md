# jazz-go

> **Experimental:** client only, backends are TypeScript. A Go program is an app client
> (an end user) or a backend client of a TypeScript main backend (`createJazz`); Go servers
> come later. See [Language Support](https://jazz.tools/docs/reference/language-support).

The Go runtime for Jazz: `package jazz`, which opens an app through the
[`jazz-ffi`](../../crates/jazz-ffi) C ABI. It needs no cgo; its only
dependency is purego. It uses `context` for every call, channels for
subscriptions and `encoding/json` for rows.

`db.Tx(ctx, fn)` is a mergeable transaction, as TypeScript's
`db.transaction`; `db.Tx(ctx, fn, jazz.Serializable())` is a serializable
one, whose commit fails with `jazz.ErrConflict` if its reads changed.

Apps don't use it on its own: author the app's `schema.ts` beside a `go.mod`
that requires this module, and `npx jazz-tools generate` writes typed
bindings there, a `jazzapp` package. See
[examples/stage-plan/apps/go-cli](../../examples/stage-plan/apps/go-cli)
and the [Go setup page](https://jazz.tools/docs/setup/go).

Install it with `go get github.com/garden-co/jazz/packages/jazz-go/v2@v2.0.0-alpha.66`
and build as usual. Its versions are Jazz's, in lockstep (each release tags
`packages/jazz-go/v<version>`).

Inside this repository, jazz-go uses the core `cargo build -p jazz-ffi`
builds. `include/jazz_ffi.h` is a copy of `crates/jazz-ffi/include/jazz_ffi.h`;
a test and the `go` CI partition check that it stays the same.
