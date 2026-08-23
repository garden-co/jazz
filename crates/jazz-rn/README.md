# jazz-rn

This directory is the **legacy React Native binding scaffold**. It is not a
supported React Native Jazz client and must not be published as one.

The active restoration work is deliberately split in two:

- `jazz-storage-sqlite` implements the portable ordered-KV storage contract
  that a native host will use.
- `jazz-native-relay` establishes the process-local, scope-keyed native relay
  boundary: a durable relay `Db` and ordinary in-memory UI clients communicating
  through Jazz's normal peer protocol.

Those crates have Rust contract tests, but they are not wired through this
package yet. The code under `rust/`, `src/generated/`, `ios/`, `android/`, and
`cpp/` is the prior UniFFI-generated surface. It predates the async core and is
not the planned command/codec ABI; in particular it must not be treated as an
implementation of the relay.

The package now reserves a generated `JazzRelay` TurboModule boundary. Android
registers its ABI probe and explicitly rejects commands until an Android build
embeds the shared Rust relay artifact. This is a thin platform checkpoint, not
device support: there is still no executable relay command codec, XCFramework,
AAR, or Expo development-build receipt.

## What remains before React Native is supported

1. Define and implement the relay command/event codecs, then generate a small
   TurboModule around them (not an object-per-row API).
2. Replace the stale UniFFI surface and build real iOS XCFramework and Android
   AAR/shared-library slices from that module.
3. Verify bare React Native autolinking plus Expo prebuild/development builds
   on Android and iOS in CI, using a first-party device app and structured
   scenario results.

Expo Go cannot load Jazz native code. Expo development builds will be supported
once the native module and artifacts above exist.

The normative host design and its implementation ledger live in
[`crates/jazz/SPEC/19_native_relays.md`](../jazz/SPEC/19_native_relays.md).
