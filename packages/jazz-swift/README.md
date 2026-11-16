# Jazz for Swift

> **Experimental: client only, backends are TypeScript.** A Swift app is an
> end-user client of your app, syncing through its TypeScript backend's
> `createJazz` handler; Swift doesn't run a backend.
>
> - **Supported:** typed bindings from your app's `schema.ts`, queries,
>   includes and hops, subscriptions (`AsyncSequence`), writes at a durability
>   tier, mergeable and serializable transactions, mergeable text, uploads,
>   history reads, device accounts and your sign-in's bearer tokens, sync
>   status. Tested on Linux in CI; each release's package runs a command-line
>   app on macOS and builds for the iOS simulator.
> - **Not supported yet:** running a backend or serving `/jazz`, a prebuilt
>   core on Linux, iOS and macOS UI apps (not tested yet), Keychain storage
>   for device keys, guest users, history subscriptions, payload enums and
>   nested rows.
>
> The full list is the Swift setup page's "Not supported yet"
> (docs/content/docs/setup/swift.mdx) and
> [Language Support](https://jazz.tools/docs/reference/language-support).

The Swift runtime of Jazz: `async`/`await` calls and `AsyncThrowingStream`
subscriptions over `crates/jazz-ffi` (the Jazz core, built from Rust, through
UniFFI). Apps call it through typed bindings: author the app's `schema.ts`
beside a `Package.swift` that depends on Jazz, and `npx jazz-tools generate`
writes `jazz.app.json`, `jazz.permissions.json` and `JazzApp.swift` there
(`--check` in CI fails when any of them is stale). See
docs/content/docs/setup/swift.mdx.

Apps depend on `garden-co/jazz-swift`, the package each release publishes
(`.github/workflows/swift-mirror.yml` builds it from here, with the Jazz core
prebuilt as an XCFramework for Apple platforms). In this repository,
the root `Package.swift` is the same package built from source: the UniFFI
sources over jazz-ffi (`Sources/JazzFFI/jazz_ffi.swift`,
`Sources/jazz_ffiFFI/jazz_ffiFFI.h`) are committed, and `libjazz_ffi` is a
system library found through pkg-config.

## Use it in an app

```swift
.package(url: "https://github.com/garden-co/jazz-swift", exact: "2.0.0-alpha.66"),
// in your target:
.product(name: "Jazz", package: "jazz-swift"),
```

On macOS and iOS that's all: the Jazz core comes prebuilt. On Linux, build
`libjazz_ffi` from the same release (needs Rust) and point pkg-config at it
before `swift build`:

```bash
git clone --depth 1 --branch v2.0.0-alpha.66 https://github.com/garden-co/jazz
jazz/packages/jazz-swift/scripts/build-ffi.sh --lib-only
export PKG_CONFIG_PATH="$PWD/jazz/target/release/jazz-ffi-static"
```

## Work on it

```bash
packages/jazz-swift/scripts/build-ffi.sh      # libjazz_ffi + jazz_ffi.pc; regenerates the UniFFI sources
export PKG_CONFIG_PATH="$PWD/target/release/jazz-ffi-static"
swift test                                    # from the repository root
```

Commit the regenerated sources whenever `crates/jazz-ffi` changes their
surface; `build-ffi.sh --check` (run by `dev/gates/jazz-swift.sh` in CI)
fails when they're stale.

Built and tested on Linux in CI (`dev/gates/jazz-swift.sh`, in the required
`jazz-ffi-bindings` partition). Needs Swift 6.0 or newer. Each release's
published package (`.github/workflows/swift-mirror.yml`) is checked on macOS,
where a command-line app builds and runs against the prebuilt XCFramework,
and built for the iOS simulator; UI apps aren't tested yet. See `examples/stage-plan/apps/swift-cli` for a complete
command-line tool, and the docs' Swift setup page for what's not supported
yet.
