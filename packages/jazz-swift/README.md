# Jazz for Swift

The Swift runtime of Jazz: `async`/`await` calls and `AsyncThrowingStream`
subscriptions over `crates/jazz-ffi` (the Jazz core, built from Rust, through
UniFFI). Apps call it through typed bindings: author the app's `schema.ts`
beside a `Package.swift` that depends on Jazz, and `npx jazz-tools generate`
writes `jazz.app.json`, `jazz.permissions.json` and `JazzApp.swift` there
(`--check` in CI fails when any of them is stale). See
docs/content/docs/setup/swift.mdx.

The package's manifest is the repository's root `Package.swift` (SwiftPM
takes a package by URL only from a repository root); its sources are here.
The UniFFI sources over jazz-ffi (`Sources/JazzFFI/jazz_ffi.swift`,
`Sources/jazz_ffiFFI/jazz_ffiFFI.h`) are committed, and `libjazz_ffi` is a
system library found through pkg-config.

## Use it in an app

Build `libjazz_ffi` from a checkout at the release tag (needs Rust), and
point pkg-config at it:

```bash
git clone --depth 1 --branch v2.0.0-alpha.65 https://github.com/garden-co/jazz
jazz/packages/jazz-swift/scripts/build-ffi.sh --lib-only
export PKG_CONFIG_PATH="$PWD/jazz/target/release/jazz-ffi-static"
```

Then depend on the package by URL (or by path, `.package(path: "jazz")`):

```swift
.package(url: "https://github.com/garden-co/jazz", exact: "2.0.0-alpha.65"),
// in your target:
.product(name: "Jazz", package: "jazz"),
```

and `swift build` with `PKG_CONFIG_PATH` set.

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
`jazz-ffi-bindings` partition). Needs Swift 6.0 or newer. macOS should work
the same way but isn't tested in CI yet; iOS apps aren't supported yet (no
XCFramework). See `examples/stage-plan/apps/swift-cli` for a complete
command-line tool, and the docs' Swift setup page for what's not supported
yet.
