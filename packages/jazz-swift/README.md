# Jazz for Swift

> **Experimental: client only, backends are TypeScript.** A Swift app is an
> end-user client of your app, syncing through its TypeScript backend's
> `createJazz` handler; Swift doesn't run a backend.
>
> - **Supported:** typed bindings from your app's `schema.ts`, queries,
>   includes and hops, subscriptions (`AsyncSequence`), writes at a durability
>   tier, mergeable and serializable transactions, mergeable text, uploads,
>   history reads, device accounts and your sign-in's bearer tokens, sync
>   status, on macOS and iOS. Each release's package runs its tests on macOS
>   and an iPhone simulator, and a SwiftUI example is UI-tested there.
> - **Not supported yet:** running a backend or serving `/jazz`, Linux apps
>   (no prebuilt core: ask on Discord), watchOS, tvOS and visionOS, Keychain
>   storage for device keys, guest users, history subscriptions, payload
>   enums and nested rows.
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

(or the same URL in Xcode, **File → Add Package Dependencies…**). That's all on
macOS and iOS: the Jazz core comes prebuilt. Swift on Linux has no prebuilt
core; ask on [Discord](https://discord.gg/RN9UKh52be) if you need it.

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
published package (`.github/workflows/swift-mirror.yml`) runs these tests on
macOS and on an iPhone simulator against the prebuilt XCFramework, builds
`examples/stage-plan/apps/swiftui` for macOS and runs its UI test on the
simulator. See `examples/stage-plan/apps/swift-cli` for a complete
command-line tool, and the docs' Swift setup page for what's not supported
yet.
