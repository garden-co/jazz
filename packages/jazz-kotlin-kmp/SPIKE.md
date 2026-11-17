# Spike: jazz-kotlin as Kotlin Multiplatform

**Status:** spike, not for merging. Branch `kotlin/kmp-spike`.

**Question:** can jazz-kotlin become one Kotlin Multiplatform (KMP) library for Android, iOS, the JVM desktop and Kotlin/Native desktop, instead of a JVM jar plus an Android AAR?

**Short answer:** yes. The API ports almost as it is. The cinterop route over jazz-ffi's existing C ABI works: on linuxX64, jazz-kotlin's own tests and a real end-to-end test pass against jazz-ffi. Distribution is the hard part, not the code. If KMP klibs embed the Rust static libraries, as Llamatik does and Gobley does by default, every Apple and native target adds tens of megabytes to each release. We are already over Maven Central's monthly limit ([jazz#6020](https://github.com/garden-co/jazz/issues/6020)). The natives for KMP must ship outside Central, as #6020's D2 or E options propose for the JVM.

## What the prototype is

`packages/jazz-kotlin-kmp`, a KMP module with targets `jvm`, `linuxX64`, `iosArm64` and `iosSimulatorArm64`:

| source set | contents |
|---|---|
| `commonMain` | **jazz-kotlin's whole API**: `Jazz`, `Query`, `Edits`, `Auth`, `Config`, `Errors`, `Relations`, `Serializers` and `Native`, copied with a handful of edits, plus `Platform.kt`, a 5-member `expect` seam |
| `jvmMain` | the seam's actuals over UniFFI/JNA. It reuses `packages/jazz-kotlin/src/ffi/kotlin/UniffiNative.kt` and the uniffi-bindgen output unchanged, and holds the JVM-only `InputStream.chunks` |
| `nativeMain` | `CAbiNative.kt` (~400 lines): `Native` over `jazz_ffi.h` through cinterop, with every method one `jazz_call`. Also the seam's actuals |
| `commonTest` | jazz-kotlin's `JazzTest` and `FakeNative`, ported (16 tests), plus the codegen golden selected by `-PgoldenApp` (default `notes`) |
| `notesTest` | `LocalAppTest`: the real jazz-ffi, through jazz-codegen's `notes` bindings. It opens a local app, then inserts, queries, `watch`es, updates, deletes and runs a transaction; it also checks that closing ends subscriptions with `Closed` and that errors come back typed |
| `jvmTest` | the 2 `JazzTest` cases that need JVM APIs (reflection, `CountDownLatch`) |

The edits that made jazz-kotlin's code common:
- `Dispatchers.IO` becomes `ioDispatcher`.
- The reflective `UniffiNative` opener becomes `platformOpener()`, and `deviceKeys()` becomes an expect.
- `runBlocking` (for the token provider) becomes `blockOn`, and `trySendBlocking` becomes `sendBlocking`. Both exist on the JVM and on Kotlin/Native, but not in common code.
- `java.util.concurrent.CopyOnWriteArrayList` becomes a small `Watches` class over `kotlin.concurrent.atomics.AtomicReference`.
- `@Volatile` becomes `kotlin.concurrent.Volatile`, and `@JvmName` becomes `kotlin.jvm.JvmName`.
- `InputStream.chunks` moves to `jvmMain`.

Nothing else in the 1,900 lines needed to change. `java.util.UUID` appears only in the test fake; the common port uses `kotlin.uuid.Uuid`.

### Results

Run with `gradle jvmTest linuxX64Test`, Kotlin 2.2.21 and Gradle 8.14.3:

```
jvmTest      tools.jazz.JazzTest      16 tests, 0 failures
jvmTest      tools.jazz.LocalAppTest   3 tests, 0 failures   (real jazz-ffi, UniFFI/JNA)
jvmTest      tools.jazz.JvmJazzTest    2 tests, 0 failures
linuxX64Test tools.jazz.JazzTest      16 tests, 0 failures
linuxX64Test tools.jazz.LocalAppTest   3 tests, 0 failures   (real jazz-ffi, cinterop + libjazz_ffi.a)
  "a bad id fails with Refused: the id must be a UUID, not \"not-a-uuid\""   (same on both)
```

**Codegen:** jazz-codegen's committed Kotlin goldens compile **unchanged** in `commonTest`, on both the JVM and linuxX64: `notes`, `chat-react` (the widest types), `tasks`, `kotlin-edges` and `kotlin-computed`. Run them with `gradle -PgoldenApp=<name> compileTestKotlinJvm compileTestKotlinLinuxX64`. The generated `JazzApp.kt` uses only `kotlin.*`, `kotlinx.serialization` and `tools.jazz`, so it is common already.

**What this proves:**
- The `Native` seam carries over completely.
- The C ABI covers the whole surface. The only gap is `bundle()` (backend-only), which has no `jazz_call` method.
- Callbacks from Rust threads work on Kotlin/Native: `staticCFunction` with a `StableRef` as ctx, and the runtime attaches the foreign thread.
- A static `libjazz_ffi.a` links into a Kotlin/Native binary from inside a klib.

**What it doesn't prove:**
- **iOS.** Kotlin/Native refuses cinterop for Apple targets on a Linux host ("Cross compilation … has been disabled because it contains cinterops"), so the iOS klib was not even compiled here. `ci/ios-check.yml` is a ready macOS job; it is not enabled (see "Next steps").
- **Android in the KMP module.** No `androidTarget` yet: AGP needs the Android SDK, which this sandbox doesn't have. The JVM path is the same UniFFI/JNA code the AAR uses today, so risk is low.
- **Bearer sign-in, sessions, uploads and sync to a server on native.** These are implemented in `CAbiNative` but untested. The ported `JazzTest` covers them against the fake only.
- **Production packaging:** natives in the JVM jar, publishing to Central, and stripping.

### Two findings on the way

1. **glibc.** Kotlin/Native links Linux binaries against its own **glibc 2.19** sysroot. A `libjazz_ffi.a` built on a current host references `fcntl64` (glibc 2.28+, from the bundled SQLite), and that was the only missing symbol. The spike links a 10-line shim (`src/nativeInterop/glibc-shim.c`). Production should build the Linux archive for an old glibc instead (cargo-zigbuild `x86_64-unknown-linux-gnu.2.17`), which also helps the JVM `.so`. This does not affect iOS or macOS.
2. jazz-kotlin's `hops_gather_union_history_…` test sits inside `object RefusingNative`, not `class JazzTest`. It does run there (JUnit reports it under `RefusingNative`), so this is cosmetic. In the port it moved into `JazzTest`.

## 1. Bindings route for Kotlin/Native

| | (a) cinterop on the C ABI (`jazz_ffi.h`) | (b) Gobley (UniFFI → KMP) | (c) JNI everywhere / other |
|---|---|---|---|
| exists today | the C ABI, hand-written, used by jazz-go; JSON in and out | nothing; would replace the UniFFI Kotlin bindings | n/a |
| effort | **done in this spike** for linuxX64: `CAbiNative`, ~400 lines | new Gradle plugin and cargo integration, plus regenerating everything | JNI glue in Rust; JVM only, no help for native |
| `Native` seam | carries over fully: `Native` is already JSON-strings, and so is `jazz_call` | carries over: it is the same UniFFI surface | n/a |
| async / callbacks | C function pointer plus `StableRef` ctx; callbacks on Rust threads; `jazz_unsubscribe` waits for an in-flight callback, so disposing the ref after is safe | UniFFI callback interfaces, mapped by Gobley | n/a |
| lifetimes | **manual**: we must enforce "no call concurrent with or after `jazz_close`". The spike has a `Gate` (an in-flight counter that close waits on). Every returned string goes back through `jazz_string_free` | UniFFI's `Arc` handles do this for us | n/a |
| **version fit** | none needed | **Gobley pins UniFFI 0.29.5** (main, Oct 2026; last release v0.3.7, 2025-10). jazz-ffi pins **0.32.2**, shared with jazz-swift. Using it means downgrading UniFFI or waiting for Gobley | n/a |
| WASM | no (no wasmJs from C) | "WASM not supported yet" | n/a |
| JVM side | keep UniFFI/JNA as today | Gobley's JVM is JNA as well, with per-platform classifier jars | n/a |

**Recommendation: (a).**
- It is the Llamatik model, done and working here.
- It keeps one stable, hand-written C ABI that Go already depends on.
- It adds no dependency on a pre-1.0 plugin that is three UniFFI minor versions behind us.

Reconsider (b) only if Gobley catches up with UniFFI 0.32. Its real advantage is that one generated binding would replace `CAbiNative`'s manual lifetime handling.

What (a) needs from Rust:
- An optional `bundle` method in `jazz_call`, for parity.
- Ideally a guarantee in `jazz_ffi.h` about calls racing `jazz_close`, so the Kotlin side can drop its `Gate`.

What it needs from #swift: nothing for cinterop, since the klib build carries `jazz_ffi.h` itself. If the iOS natives come from `JazzFFI.xcframework`, see section 4; its `.a` files already export the C ABI symbols.

The **JVM and Android stay on UniFFI/JNA**. That is the path users test today. Moving them to the C ABI through JNA direct mapping is possible later, and would leave one binding to maintain.

## 2. Module layout

```
packages/jazz-kotlin/            (becomes the KMP module; the artifact id stays jazz-kotlin)
  src/commonMain    Jazz, Query, Edits, Auth, Config, Errors, Relations, Serializers, Native, Platform (expect)
  src/jvmMain       UniffiNative + generated UniFFI, Platform.jvm, InputStream.chunks
  src/androidMain   = jvmMain's UniFFI sources via a shared `jvmAndAndroidMain` (replaces packages/jazz-kotlin-android)
  src/nativeMain    CAbiNative + Platform.native (shared by ios*, macos*, linux*, mingw*)
  src/nativeInterop/cinterop/jazz_ffi.def
```

**Common-safe already:** everything except the five seams above. The `java.*` uses are `CopyOnWriteArrayList`, the reflection opener, `InputStream` and `@Volatile`; `java.util.UUID` appears only in tests. `kotlinx.serialization`, `kotlinx.coroutines` and `kotlin.io.encoding.Base64` are all common.

**API caveat:** commonMain currently sees `runBlocking`/`IO` only through expects. Adding **wasmJs** later means `blockOn` can't exist there: the bearer token callback would have to become non-blocking.

## 3. Codegen

No change needed: the goldens compile as they are in common code. The CLI should say "put `JazzApp.kt` in `commonMain`" in its KMP docs. A later nicety: an `expect`-free `open()` is already what codegen emits.

## 4. Distribution and size

**How KMP ships natives:**
- One publication per target (`jazz-kotlin-jvm`, `-android`, `-iosarm64`, `-iossimulatorarm64`, `-linuxx64`, …), plus the root `jazz-kotlin` with a Gradle `.module` file that sends each consumer to its variant. Maven-only consumers name the `-jvm` artifact.
- Kotlin/Native natives can ride inside the cinterop klib: `-staticLibrary` (`staticLibraries=` in the `.def`), as this spike and Llamatik do.
- Alternatively, the klib carries only the bindings and the app links the library itself, as GitLive's Firebase SDK requires its users to do via CocoaPods or SPM.
- Publishing requires a macOS host for the Apple targets (cinterop), and all targets go out from one machine.

**Llamatik as a data point:**
- `core-iosarm64` has 3 cinterop klibs of 29.6 MB each (the same merged `.a`, three times).
- `core-jvm` is 58 MB (desktop JNI libraries) and `core-android` is 86 MB.
- That is about 260 MB per release.

**Our numbers** (this spike, linux-x86_64):

| build | bytes | deflated (klib/jar zip) |
|---|---:|---:|
| `libjazz_ffi.a`, `release-mobile` built by `cargo build` (crate-types lib+cdylib+staticlib: **LTO is not applied to the staticlib**, 538 objects) | 188.6 MB | 54.5 MB |
| `libjazz_ffi.a`, `release-mobile` via `cargo rustc --crate-type staticlib` (fat LTO, what the build here does) | 38.5 MB | 10.3 MB |
| **published `jazz-kotlin-kmp-linuxx64-…-cinterop-jazzFfi.klib`** (that `.a` inside) | | **10.3 MB** |
| published `jazz-kotlin-kmp-linuxx64-….klib` (the Kotlin code) | | 0.19 MB |
| `libjazz_ffi.so`, `release-mobile`, stripped (for comparison: #6020's B) | 17.0 MB | 7.0 MB |
| today's `JazzFFI.xcframework.zip` (5 arch slices) | n/a | 131 MB (~26 MB/slice) |

Side note for #swift: `build-swift-xcframework.sh` builds its staticlibs with `--release` (opt 3, no LTO), about 26 MB compressed per slice. A `release-mobile` (fat LTO, opt "s") staticlib is 10.3 MB compressed here, so switching could cut the XCFramework zip by roughly 60%. Benchmark speed first, as #6020 says for the desktop natives.

**Per release on Central with natives in klibs**, using the LTO staticlib size above per Kotlin/Native target:
- JVM jar: 5 desktop natives ≈ 34 MB with #6020's option B.
- Android AAR: about 18 MB.
- iosArm64, iosSimulatorArm64 and iosX64: about 10 MB each, assuming Apple `.a` sizes are similar to linux's.
- macosArm64 and linuxX64 (optional).

That comes to roughly **90–110 MB per release**, against a limit of about **78 MB a month**. One KMP release would fill the month by itself, and with today's cadence we would be refused after 2026-12-16.

**So for KMP the natives must not be on Central.** Options, matching #6020:
- **iOS and macOS: klibs without the static library, linked from `JazzFFI.xcframework`.**
  - This is the artifact #swift already builds and attaches to every release, with the C ABI symbols in its `.a` files.
  - KMP-on-iOS apps add the jazz-swift mirror's `JazzFFI` binary package through SPM, or the XCFramework directly, as Firebase-KMP users add Firebase.
  - A small Gradle plugin, D2 below, could instead download the right `.a` from the GitHub release at link time.
  - Either way the klibs are a few hundred KB.
  - #swift's offer to add the C ABI header to the slices isn't needed for cinterop (our klib carries the header), but it would let Swift or C users see it.
- **JVM and Android:** #6020's **D2**, a `tools.jazz.kotlin` Gradle plugin that fetches the release's natives at build time, sha256-pinned, or **E**, our own Maven repository.
  - D2 generalises to KMP: the same plugin could supply `libjazz_ffi.a` per Kotlin/Native target, via cinterop `-libraryPath` or linker options for the final binary. That makes Central carry only Kotlin, about **2–3 MB per release for all targets**.
- **G** (asking Sonatype for an exemption) helps either way, but KMP multiplies the bytes by the number of targets, so we shouldn't depend on it.

## 5. Targets, in order

1. **jvm + android:** what exists today, moved into one KMP module. This is the prerequisite, and it means KMP app developers on Android get the library from Day 1.
2. **iosArm64 + iosSimulatorArm64:** the reason to do KMP at all. Needs the macOS CI job and the XCFramework link story. Add `iosX64` only if someone still uses Intel simulators.
3. **macosArm64:** cheap once iOS works (same Apple toolchain and XCFramework slice); useful for Compose Desktop on Kotlin/Native and for CLI tools.
4. **linuxX64 / linuxArm64:** already working here (linuxX64). Small demand: JVM users on Linux already have the jar. Needs the glibc 2.17 build.
5. **mingwX64:** possible (Rust `x86_64-pc-windows-gnu`, which jazz-go may already build), but low demand; JVM covers Windows.
6. **wasmJs:** no. cinterop doesn't apply, and Jazz's WASM build is a JS/TS-facing `wasm-bindgen` module. A Kotlin/Wasm actual would mean `external` declarations over jazz-wasm's JS API, plus a non-blocking token API: a separate project. Kotlin/JS users are better served by jazz's TypeScript client.

## 6. What Swift users get

A KMP framework exports to Swift with an Objective-C-flavoured API: `suspend` functions become completion handlers or `async` (via SKIE), `Flow` needs SKIE or wrappers, and generics are erased. jazz-swift is idiomatic Swift over UniFFI. **KMP-for-iOS is for KMP app developers** (shared Kotlin code across Android and iOS), not a jazz-swift replacement. The two share the XCFramework natives. Offer both and document which to pick: native Swift app → jazz-swift; shared Kotlin → jazz-kotlin.

## Effort, in rough phases

| phase | what | size |
|---|---|---|
| 0 | this spike | done |
| 1 | **jazz-kotlin becomes KMP with jvm + android only.** Merge jazz-kotlin-android into it (`androidTarget`), keep the `jazz-kotlin` coordinates (the root `.module` resolves to `-jvm`), and port the tests to `commonTest` as here. Users see no API change. | M |
| 2 | **Natives off Central** (#6020 D2 or E), designed so it also serves Kotlin/Native `.a` files. | M (overlaps #6020) |
| 3 | **iOS:** `CAbiNative` hardened (Gate, token-callback lifetime, `bundle`), the macOS CI job (simulator tests), linking against JazzFFI.xcframework, a sample KMP app (Compose Multiplatform or SwiftUI plus shared Kotlin), and docs. | M–L |
| 4 | macOS, then Linux native (glibc 2.17 build), each a CI matrix row. | S each |
| 5 | (optional) the JVM and Android onto the C ABI too, leaving one binding. | M |

## Risks

- **Size and limits:** the biggest risk; see section 4. KMP is only viable alongside moving natives off Central.
- **The manual lifetimes in `CAbiNative`** (a use-after-close race, StableRef leaks) are a new class of bugs that UniFFI spared us. Mitigations: the `Gate`, tests running under ASan, and a stronger C ABI contract.
- **Two bindings to keep in step:** UniFFI on JVM/Android and the C ABI on native, though `Native` keeps the overlap thin. jazz-go already forces the C ABI to keep up.
- **The C ABI is hand-written** (`jazz_ffi.h` "keep it in step with src/c_abi.rs"). A drift test, or cbindgen, becomes more valuable.
- **Apple builds need macOS runners:** 10× the minutes, and publishing must run on macOS.
- **Swift export ergonomics** for KMP-on-iOS consumers (Flow, suspend) depend on SKIE or Kotlin's Swift export, which is still experimental.
- **Kotlin/Native's glibc 2.19 sysroot** constrains the Linux natives (finding 1).

## Next steps

- Run `ci/ios-check.yml` once on a macOS runner by copying it into `.github/workflows` on this branch: it compiles the iOS klibs and runs the common tests on the simulator. Not done without a go-ahead (macOS minutes, CI in the repo).
- Decide #6020 with KMP in mind.
