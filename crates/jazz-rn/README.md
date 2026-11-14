# jazz-rn

This directory is the native relay package for Jazz's supported React Native
alpha. Applications use the public `jazz-tools/react-native` client API: open
a session with `createJazzClient` (or `<JazzProvider>` from
`jazz-tools/expo`), which signs in a device account or your backend's user. The
[canonical Expo scaffold](https://github.com/garden-co/jazz/tree/main/examples/todo-client-localfirst-expo) shows the
complete client lifecycle.

The alpha requires a matching native development or release build. Expo Go
cannot contain this native module.

The shared JSI HostObject source plus Rust C ABI opens an account-scoped native
foreground `Db`, runs ordinary bounded core turns, and closes that exact alias.
The relay owns its persistent SQLite scope. Android and iOS install the shared
private factory through their New-Architecture JSI hooks, including a retained
host-state lease that makes late finalizers harmless during bridge teardown.
`jazz-tools/react-native` maps the shared local-first query/subscription and
ordinary full-cell transaction commands onto its existing
`NativeRuntimeAdapter`. The public account-handle path owns native admission
and the account-scoped persistent relay; applications do not construct a
foreground engine or exchange native capabilities directly. The complete
ownership/threading/packaging contract and staged acceptance path are specified in
[`crates/jazz/SPEC/19_native_relays.md`](https://github.com/garden-co/jazz/blob/main/crates/jazz/SPEC/19_native_relays.md#196-foreground-native-runtime-execution).

The package reserves a generated `JazzRelay` TurboModule boundary. Android and
iOS autolink the module and reject relay use unless a matching development or
release assembly stages the shared Rust relay artifact. Staged Android
libraries, the shared header, and the iOS XCFramework are included by the npm
package file contract. `expo prebuild` and bare React Native discovery prove
install-time wiring; they are not a substitute for an installed-device receipt.

`jazz-rn` requires the React Native **New Architecture**. Android Gradle and
iOS CocoaPods fail early with an install/configuration instruction otherwise.
For Expo, add `"plugins": ["jazz-rn"]` and run `expo prebuild`. Expo SDK 54
uses the New Architecture by default (do not set `newArchEnabled: false`), and
SDK 55+ (React Native 0.82+) has no legacy architecture, so the plugin does not
touch `newArchEnabled`. Bare React Native 0.81 apps must keep the New
Architecture enabled themselves. This requirement does not make Expo Go capable of loading Jazz.

The repository runs source/package receipts plus label-gated Android-emulator
and iOS-simulator installed-device workflows. They prove native admission,
capability revocation, process reopen, scope-selected SQLite isolation, and
the public account-handle client path. They do **not** yet prove communication
between two physical JSI runtimes; that acceptance receipt remains pending.
The shared host codec now stages trusted native scope admission/revocation via
random 256-bit capabilities, client open-close, bounded `Pump`, directional
bounded peer-frame send/drain, and handle/queue diagnostics. Kotlin and
Swift/Objective-C application authentication code supplies the complete strict
scope configuration to a dedicated native entrypoint; Rust validates and
normalizes it, then returns only the opaque 32-byte capability. The generic
TurboModule `execute` channel never accepts scope configuration, claims, or
bearer tokens. On auth switch, trusted code revokes the old capability (closing
all of its relay/client aliases) before admitting the new scope.

The shared artifact seam is `jazz_native_relay_abi_version` from
`jazz-native-relay`'s C ABI (`include/jazz_native_relay.h`). Android (JNI) and
iOS link that artifact directly from the `jazz-rn-android` and `jazz-rn-ios`
payloads; neither routes through the obsolete UniFFI library.

The wrapper accepts exactly one ABI version, `NATIVE_RELAY_ABI_V1` (currently
7, generated from `jazz-native-relay`), and refuses any other native build. That
ABI uses opaque host-generated admission capabilities and trusted revocation.
It defines the shared foreground
`NativeDb` postcard seam with canonical-query prepare/read/subscribe/drain,
plus pending-operation poll/cancel commands for chunk-backed reads. It is a
byte-oriented native-host contract, not a new React Native row/query API:
`jazz-tools/react-native` is the public adapter, using the same query and
row-delta codecs as NAPI/WASM. It selects the account-scoped persistent
foreground path and does not fall back to a browser-WASM or generic
TurboModule runtime.

An admitted native foreground is bound to the session used for admission.
`db.updateCookieSession(...)` therefore rejects atomically on this path: it
leaves the previously admitted session and capability usable, and does not
silently apply refreshed claims. Revoke the old capability and create a new
client for a sign-in, sign-out, or scope change.

## Expo development-build install path

`jazz-rn` is a direct application dependency: React Native codegen and Expo
autolinking must discover it from the application, not through `jazz-tools`.
For an Expo development build, the minimal configuration is:

```bash
pnpm add jazz-tools jazz-rn
```

Install the same version of both. A release candidate is not on npm: install
`jazz-tools` and `jazz-rn` from the `.tgz` assets on its GitHub prerelease
(`https://github.com/garden-co/jazz/releases/tag/v<version>`), whose `jazz-rn`
already points `jazz-rn-ios` and `jazz-rn-android` at the same release's assets.

```json
{
  "expo": {
    "plugins": ["jazz-rn"]
  }
}
```

Then run `npx expo prebuild --clean` and make a development or release build.
The New Architecture is Expo's default, and native autolinking registers
`JazzRelayPackage` on Android and the `JazzRn` pod on iOS. Do not use Expo Go:
it cannot contain the relay module.

Expo Go is not supported. React Native 0.82+ always uses the New
Architecture. For a bare React Native 0.81 host keep `newArchEnabled=true` on
Android and do not install pods with `RCT_NEW_ARCH_ENABLED=0`.
Use the `rn-preview-release` pull-request label when a preview must run the
expensive native artifact and device workflows. Then open a client with
`createJazzClient` from `jazz-tools/react-native` (in a bare host, pass an
OS-protected `AccountStore`) or `jazz-tools/expo`.

Bare React Native uses the same direct dependency and autolinking metadata, but
has no Expo plugin: enable the New Architecture in the host project before
running its platform install/build commands.

The repository's `jazz-rn` packaging receipt packs the actual npm tarball,
uses it from an otherwise empty Expo SDK 54 app, typechecks an import from its
published declarations, prebuilds Android and iOS, and verifies Android
autolinking plus bare React Native discovery. This intentionally proves only
install-time wiring. A device run also requires a matching release payload with
the sealed Android relay slices or iOS XCFramework plus account-handle admission;
those are produced by the native artifact/release workflows and are checked separately by the device
acceptance app. A source checkout with no staged native artifacts should fail
at relay use rather than pretend to provide persistence.

## Alpha validation limit

The installed-device receipt currently covers two foreground aliases in one
physical JSI runtime. It does not yet prove two physical JSI runtimes attached
to one relay. This remains an explicit validation gap; it does not change the
application-facing account-handle API.

Expo Go cannot load Jazz native code. Expo development builds and bare React
Native hosts must include a matching native module and open it through an
account handle.

The normative host design and its implementation ledger live in
[`crates/jazz/SPEC/19_native_relays.md`](https://github.com/garden-co/jazz/blob/main/crates/jazz/SPEC/19_native_relays.md).

### Native release payloads

`jazz-rn` owns React Native autolinking and code generation. Its ordinary,
exact-version dependencies `jazz-rn-ios` and `jazz-rn-android` carry the complete
XCFramework and all three Android ABI archives. Both dependencies are installed
on every build host; npm host OS/CPU selectors cannot select a mobile build target.

Native producer scripts stage the matching workspace payload after building.
Release assembly seals and checks both payload packages, then packs all three
packages with `node dev/artifacts/rn-packages.mjs pack <directory>`. The extracted
payloads must match their source, ABI, architecture inventory, and file hashes;
the wrapper must contain exact-version dependencies and no native archives.
Publication consumes the exported, hash-checked receipt, publishing payloads
before the wrapper and skipping versions already present on retry. Preview
publication binds both dependencies to the same exact commit's pkg.pr.new URLs.

Payloads intentionally retain their producer debug symbols. Each upload has a
195,000,000-byte compressed budget, with base64 expansion and a 1,000,000-byte
metadata reserve recorded separately. This is a conservative project budget,
not a claim about npm's undocumented service limit. CI links the packed Android
and iOS packages through isolated consuming applications.
