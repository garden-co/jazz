# jazz-rn

This directory is the React Native package for Jazz's native relay boundary.
It is not yet a supported React Native Jazz client: its autolinked Android and
iOS modules deliberately report that the Rust relay artifact is unavailable.

The active restoration work is deliberately split in two:

- `jazz-storage-sqlite` implements the portable ordered-KV storage contract
  that a native host will use.
- `jazz-native-relay` establishes the process-local, scope-keyed native relay
  boundary: a durable relay `Db` and ordinary in-memory UI clients communicating
  through Jazz's normal peer protocol.

Those crates have Rust contract tests, but they are not wired through this
package yet. The former UniFFI/JSI surface has been removed rather than left as
a broken alternative runtime path.

The package now contains the first real foreground-owner substrate. The shared
JSI HostObject source plus Rust C ABI can open one memory-only foreground `Db`
only from an already admitted opaque 32-byte capability, run an ordinary bounded
core `tick`, and close that exact alias. It does not accept storage paths,
schema, claims, identities, or tokens, and it never reads relay SQLite for a
foreground operation. Owner work is bounded and capability revocation makes
all aliases unusable. Android and iOS now install the shared private factory
through their New-Architecture JSI hooks, including a retained host-state lease
that makes late finalizers harmless during bridge teardown. The complete
encoded `NativeDb` read/write/query/subscription codec remains deliberately
unfinished, so this substrate is not yet an end-to-end RN client. The complete
ownership/threading/packaging contract and staged acceptance path are specified
in [`jazz/SPEC/19_native_relays.md`](../jazz/SPEC/19_native_relays.md#196-foreground-native-runtime-execution).

The package reserves a generated `JazzRelay` TurboModule boundary. Android and
iOS autolink the module, report ABI `0` (unavailable), and explicitly reject
commands unless a development or release assembly stages the shared Rust relay
artifact. Staged Android libraries, the shared header, and the iOS XCFramework
are included by the npm package file contract. `expo prebuild` and bare React
Native integration can therefore succeed without an artifact, but they do
**not** make Jazz usable on a device yet.

`jazz-rn` requires the React Native **New Architecture**. Android Gradle and
iOS CocoaPods fail early with an install/configuration instruction otherwise.
For Expo, add `"plugins": ["jazz-rn"]` and run `expo prebuild`; the plugin
sets `newArchEnabled`. Bare React Native apps must enable the New Architecture
themselves. This requirement does not make Expo Go capable of loading Jazz.

The current repository gate executes Expo prebuild for Android and iOS, then
inspects Expo's autolinking contracts for this package. It is intentionally not
a native build receipt: this Linux development environment has neither Java
nor CocoaPods, so Gradle configuration/build and `pod install` must run on the
respective Blacksmith runners before claiming platform or device support.
The shared host codec now stages trusted native scope admission/revocation via
random 256-bit capabilities, client open-close, bounded `Pump`, directional
bounded peer-frame send/drain, and handle/queue diagnostics. Kotlin and
Swift/Objective-C application authentication code supplies the complete strict
scope configuration to a dedicated native entrypoint; Rust validates and
normalizes it, then returns only the opaque 32-byte capability. The generic
TurboModule `execute` channel never accepts scope configuration, claims, or
bearer tokens. On auth switch, trusted code revokes the old capability (closing
all of its relay/client aliases) before admitting the new scope. This is still
a platform checkpoint, not device support: there is no assembled release-package
or Expo development-build receipt yet.

The shared artifact seam is `jazz_native_relay_abi_version` from
`jazz-native-relay`'s C ABI (`include/jazz_native_relay.h`). Android/JNI will
link that artifact directly when the Android build pipeline exists; it must not
route through the obsolete UniFFI library. The remaining Android runner gate is
a real Gradle/NDK AAR build and emulator installation against that linked
artifact.

The wrapper accepts ABI 7, which uses opaque host-generated admission
capabilities and trusted revocation. ABI 7 extends V1 of the shared foreground
`NativeDb` postcard seam with canonical-query prepare/read/subscribe/drain,
plus pending-operation poll/cancel commands for chunk-backed reads. It is
deliberately a byte-oriented native-host contract, not
a new React Native row/query API: the query and row-delta codecs are the same
ones used by NAPI/WASM. This capability-gated slice supports only ordinary
local-first reads; remote tiers, structured relation terminal operations, and
write/transaction commands use the established native encoded-cell and core
transaction semantics, including the public `txId` receipt identity. Branch
targets, custom write attribution, large-value diffs, structured relation
terminal operations, and remote read tiers remain unavailable, so
`jazz-tools/react-native` must not select it as its general runtime yet.

## Expo development-build install path

`jazz-rn` is a direct application dependency: React Native codegen and Expo
autolinking must discover it from the application, not through `jazz-tools`.
For an Expo development build, the minimal configuration is:

```bash
pnpm add jazz-rn@alpha
```

```json
{
  "expo": {
    "plugins": ["jazz-rn"]
  }
}
```

Then run `npx expo prebuild --clean` and make a development or release build.
The plugin turns on the New Architecture, and native autolinking registers
`JazzRelayPackage` on Android and the `JazzRn` pod on iOS. Do not use Expo Go:
it cannot contain the relay module.

Expo Go is not supported. For a bare Android host set `newArchEnabled=true`;
for iOS install pods with `RCT_NEW_ARCH_ENABLED=1 bundle exec pod install`.
Use the `rn-preview-release` pull-request label when a preview must build the
native relay artifacts. This is still not yet a supported high-level React Native Jazz client:
the install path is intentionally documented separately
from the unfinished general runtime facade.

Bare React Native uses the same direct dependency and autolinking metadata, but
has no Expo plugin: enable the New Architecture in the host project before
running its platform install/build commands.

The repository's `jazz-rn` packaging receipt packs the actual npm tarball,
uses it from an otherwise empty Expo SDK 54 app, typechecks an import from its
published declarations, prebuilds Android and iOS, and verifies Android
autolinking plus bare React Native discovery. This intentionally proves only
install-time wiring. A device run also requires a matching release payload with
the sealed Android relay slices or iOS XCFramework plus platform-owned scope admission; those are produced by
the native artifact/release workflows and are checked separately by the device
acceptance app. A source checkout with no staged native artifacts should fail
at relay use rather than pretend to provide persistence.

## What remains before React Native is supported

1. Complete the encoded `NativeDb` binding contract against the already
   attached in-memory clients; do not add a browser-WASM fallback.
2. Extend the staged host lifecycle codec with shared event/peer-frame drainage
   (not an object-per-row API).
3. Build real iOS XCFramework and Android AAR/shared-library slices from that
   module.
4. Verify bare React Native autolinking plus Expo prebuild/development builds
   on Android and iOS in CI, using a first-party device app and structured
   two-foreground-runtime scenario results.

Expo Go cannot load Jazz native code. Expo development builds will be supported
once the native module and artifacts above exist.

The normative host design and its implementation ledger live in
[`crates/jazz/SPEC/19_native_relays.md`](../jazz/SPEC/19_native_relays.md).
