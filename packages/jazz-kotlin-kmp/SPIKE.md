# Spike: jazz-kotlin as Kotlin Multiplatform

**Status:** spike, not for merging. Branch `kotlin/kmp-spike`. Anselm decided on 2026-11-22: go on KMP, gated on iOS (step 0). **The iOS gate passes** on main of that day (2.0.0-beta.0); see "iOS gate". Kotlin releases are moving to our own Maven repository ([jazz#6020](https://github.com/garden-co/jazz/issues/6020)).

**Question:** can jazz-kotlin become one Kotlin Multiplatform (KMP) library for Android, iOS, the JVM desktop and Kotlin/Native desktop, instead of a JVM jar plus an Android AAR?

**Short answer:** yes. The API ports almost as it is, and the cinterop route over jazz-ffi's existing C ABI builds, links and runs on linuxX64 and iOS. One thing must change before iOS ships: the C ABI needs a way to close an app without freeing its handle, or `close()` can hang forever on Kotlin/Native (see "The close hang"). Distribution needs our own Maven repository: with static libraries in the klibs, each iOS target alone is about 26.5 MB compressed.

## iOS gate (2026-11-22)

[Run 44983562472](https://github.com/garden-co/jazz/actions/runs/44983562472) at `d5152181`: GitHub's `macos-15` (arm64, Xcode 16.4), Rust 1.93.1, Kotlin 2.2.21, Gradle 8.14.3. `.github/workflows/kmp-spike-ios.yml` is temporary and on this branch only (remove it before anything here goes to main); it runs `ci/ios-gate.sh`.

| criterion | result |
|---|---|
| cinterop and klibs compile for `iosArm64` and `iosSimulatorArm64` | pass, `libjazz_ffi.a` inside both |
| binaries link for the device, unsigned | pass: a release executable and the debug test binary, arm64, iOS 16.0 |
| common tests and `LocalAppTest` on the simulator, real jazz-ffi | pass: `JazzTest` 16, `LocalAppTest` 3; none failed or skipped |

| size, iOS arm64, jazz-ffi built `--release` as JazzFFI.xcframework is | bytes | deflated |
|---|---:|---:|
| what jazz-ffi adds to a linked, stripped binary (a C program using the functions `CAbiNative` uses, less an empty one) | 21.3 MB | 9.1 MB |
| the same inside the Kotlin executable, by linker map | 21.1 MB | |
| the whole stripped Kotlin executable | 24.3 MB | 10.1 MB |
| `libjazz_ffi.a`, what a klib carries per target | 83.6 MB | 26.5 MB |

The job took 16 minutes, 10 of them building Rust for two targets without a cache.

**Not covered by the gate:** a physical device, persistent storage on iOS (the tests use memory storage), sign-in and sync, an app consuming the published klib, linking from JazzFFI.xcframework, and `iosX64`. Apps need a deployment target of iOS 16.0 or later, as the XCFramework does.

## The close hang

The C ABI allows no call on a handle concurrent with `jazz_close`, so `CAbiNative`'s `Gate` makes `close()` wait for the calls in flight. But a call that waits on the server only ends when the app closes. Measured on Linux with a device account, an unreachable server, one insert and a pending `waitForSync(Global)`:

| binding | `close()` |
|---|---|
| JVM (UniFFI) | returns after 30.0 s (the core's drain); the pending call fails with `Closed` |
| Kotlin/Native (C ABI) | still running after 75 s, spinning; the pending call never ends |
| jazz-go (C ABI, same wait, in `Close`) | still running after 75 s |
| Kotlin/Native with the experiment below | returns after 30.0 s; the pending call fails with `Closed`, as on the JVM |

The experiment (not committed) adds one method to `jazz_call`, which may run alongside other calls:

```rust
"close" => {
    core.close()?;
    "null".into()
}
```

`CAbiNative.close()` calls it, then waits for the calls in flight, then frees with `jazz_close`. `signOut` needs the same order. It is an additive C ABI change (`c_abi.rs` and the header), and needs no core change.

## What the prototype is

`packages/jazz-kotlin-kmp`, a KMP module with targets `jvm`, `linuxX64`, `iosArm64` and `iosSimulatorArm64`:

| source set | contents |
|---|---|
| `commonMain` | **jazz-kotlin's whole API**: `Jazz`, `Query`, `Edits`, `Auth`, `Config`, `Errors`, `Relations`, `Serializers` and `Native`, copied with a handful of edits, plus `Platform.kt`, a 5-member `expect` seam |
| `jvmMain` | the seam's actuals over UniFFI/JNA. It reuses `packages/jazz-kotlin/src/ffi/kotlin/UniffiNative.kt` and the uniffi-bindgen output unchanged, and holds the JVM-only `InputStream.chunks` |
| `nativeMain` | `CAbiNative.kt` (~400 lines): `Native` over `jazz_ffi.h` through cinterop, with every method one `jazz_call`. Also the seam's actuals |
| `commonTest` | jazz-kotlin's `JazzTest` and `FakeNative`, ported (16 tests), plus the codegen golden selected by `-PgoldenApp` (default `notes`) |
| `notesTest` | `LocalAppTest`: the real jazz-ffi, through jazz-codegen's `notes` bindings. It opens a local app, then inserts, queries, `watch`es, updates, deletes and runs a transaction; it also checks that closing ends subscriptions with `Closed` and that errors come back typed |
| `iosTest` | `DeviceLink.kt`: the executable the gate links for a device |
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

Run with `gradle jvmTest linuxX64Test` (on macOS, `ci/ios-gate.sh rust`, then `ci/ios-gate.sh test`), Kotlin 2.2.21 and Gradle 8.14.3:

```
jvmTest      tools.jazz.JazzTest      16 tests, 0 failures
jvmTest      tools.jazz.LocalAppTest   3 tests, 0 failures   (real jazz-ffi, UniFFI/JNA)
jvmTest      tools.jazz.JvmJazzTest    2 tests, 0 failures
linuxX64Test tools.jazz.JazzTest      16 tests, 0 failures
linuxX64Test tools.jazz.LocalAppTest   3 tests, 0 failures   (real jazz-ffi, cinterop + libjazz_ffi.a)
iosSimulatorArm64Test tools.jazz.JazzTest      16 tests, 0 failures
iosSimulatorArm64Test tools.jazz.LocalAppTest   3 tests, 0 failures   (real jazz-ffi, cinterop + libjazz_ffi.a)
  "a bad id fails with Refused: the id must be a UUID, not \"not-a-uuid\""   (same on all)
```

**Codegen:** jazz-codegen's committed Kotlin goldens compile **unchanged** in `commonTest`, on both the JVM and linuxX64: `notes`, `chat-react` (the widest types), `tasks`, `kotlin-edges` and `kotlin-computed`. Run them with `gradle -PgoldenApp=<name> compileTestKotlinJvm compileTestKotlinLinuxX64`. The generated `JazzApp.kt` uses only `kotlin.*`, `kotlinx.serialization` and `tools.jazz`, so it is common already.

**What this proves:**
- The `Native` seam carries over completely.
- The C ABI covers every `Native` method, `bundle()` too: it is a `jazz_call` method that `jazz_ffi.h` doesn't list. The exception is closing while a call is in flight ("The close hang").
- Callbacks from Rust threads work on Kotlin/Native: `staticCFunction` with a `StableRef` as ctx, and the runtime attaches the foreign thread.
- A static `libjazz_ffi.a` links into a Kotlin/Native binary from inside a klib.

**What it doesn't prove:**
- **iOS beyond the gate:** see the end of "iOS gate".
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
| effort | **done in this spike** for linuxX64 and iOS: `CAbiNative`, ~400 lines | new Gradle plugin and cargo integration, plus regenerating everything | JNI glue in Rust; JVM only, no help for native |
| `Native` seam | carries over fully: `Native` is already JSON-strings, and so is `jazz_call` | carries over: it is the same UniFFI surface | n/a |
| async / callbacks | C function pointer plus `StableRef` ctx; callbacks on Rust threads; `jazz_unsubscribe` waits for an in-flight callback, so disposing the ref after is safe | UniFFI callback interfaces, mapped by Gobley | n/a |
| lifetimes | **manual**: we must enforce "no call concurrent with or after `jazz_close`". The spike has a `Gate` (an in-flight counter that close waits on), which **hangs** behind a blocked call until the C ABI has a non-freeing close. Every returned string goes back through `jazz_string_free` | UniFFI's `Arc` handles do this for us | n/a |
| **version fit** | none needed | **Gobley pins UniFFI 0.29.5** (main, Oct 2026; last release v0.3.7, 2025-10). jazz-ffi pins **0.32.2**, shared with jazz-swift. Using it means downgrading UniFFI or waiting for Gobley | n/a |
| WASM | no (no wasmJs from C) | "WASM not supported yet" | n/a |
| JVM side | keep UniFFI/JNA as today | Gobley's JVM is JNA as well, with per-platform classifier jars | n/a |

**Recommendation: (a).**
- It is the Llamatik model, done and working here.
- It keeps one stable, hand-written C ABI that Go already depends on.
- It adds no dependency on a pre-1.0 plugin that is three UniFFI minor versions behind us.

Reconsider (b) only if Gobley catches up with UniFFI 0.32. Its real advantage is that one generated binding would replace `CAbiNative`'s manual lifetime handling.

What (a) needs from Rust:
- **Required before iOS ships:** a close that doesn't free the handle and may run alongside other calls ("The close hang").
- `bundle` listed in `jazz_ffi.h`.

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
- iosArm64, iosSimulatorArm64 and iosX64: about 10 MB each, **only if** built `release-mobile`, which is unmeasured on Apple. Built `--release` as the XCFramework is, iosArm64's `.a` is 26.5 MB deflated.
- macosArm64 and linuxX64 (optional).

That comes to roughly **90–110 MB per release** (`release-mobile`; more like **130–170 MB** with the Apple libraries built as they are today), against a limit of about **78 MB a month**. One KMP release would fill the month by itself, and with today's cadence we would be refused after 2026-12-16.

**So for KMP the natives must not be on Central, and won't be:** on 2026-11-22 Anselm chose our own Maven repository (#6020's E), and the earlier Gradle plugin recommendation (D2) is withdrawn. Two ways to ship iOS's natives are then open:
- **in the klib**, as the gate built and tested: one Gradle dependency, about 26.5 MB per iOS target per release on our repository (less with `release-mobile`, to be measured);
- **linked by the app from JazzFFI.xcframework**, as Firebase-KMP users do: klibs of a few hundred KB, but a second dependency whose version must match. Untested.

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
| 1 | **jazz-kotlin becomes KMP with jvm + android only.** Merge jazz-kotlin-android into it (`androidTarget`), keep the `jazz-kotlin` coordinates (the root `.module` resolves to `-jvm`), and port the tests to `commonTest` as here. The API is unchanged, but installing changes: `jazz-kotlin-android` goes away, and non-Gradle users name `jazz-kotlin-jvm`. | M |
| 2 | **Natives off Central:** our own Maven repository, under way separately, taking one publication per target. | M (#6020) |
| 3 | **iOS:** first the C ABI's non-freeing close (S; also fixes jazz-go's hang; shares `c_abi.rs` with jazz#6201 and the error-code work, so it needs an order). Then `CAbiNative` hardened (close and signOut, a `Gate` that doesn't spin, token-callback lifetime, parity when a storage delete fails), the macOS CI job (the gate's, with a Rust cache), persistent storage and sync on the simulator, a consumer app linking the published artifacts, and docs. | M–L |
| 4 | macOS, then Linux native (glibc 2.17 build), each a CI matrix row. | S each |
| 5 | (optional) the JVM and Android onto the C ABI too, leaving one binding. | M |

## Risks

- **Size:** about 21 MB (9 MB compressed) of every iOS app, and tens of MB per release in the repository; see "iOS gate" and section 4.
- **The manual lifetimes in `CAbiNative`** (a use-after-close race, StableRef leaks) are a new class of bugs that UniFFI spared us, and one is confirmed ("The close hang"). The gate's tests passed with it present: it takes a blocked call to show. Mitigations: the `Gate`, tests running under ASan, and a stronger C ABI contract.
- **Two bindings to keep in step:** UniFFI on JVM/Android and the C ABI on native, though `Native` keeps the overlap thin. jazz-go already forces the C ABI to keep up.
- **The C ABI is hand-written** (`jazz_ffi.h` "keep it in step with src/c_abi.rs"). A drift test, or cbindgen, becomes more valuable.
- **Apple builds need macOS runners:** free on GitHub's standard runners for this public repo, about 16 minutes uncached; publishing must run on macOS.
- **Toolchain floor:** iOS 16.0, as jazz-ffi is built.
- **Swift export ergonomics** for KMP-on-iOS consumers (Flow, suspend) depend on SKIE or Kotlin's Swift export, which is still experimental.
- **Kotlin/Native's glibc 2.19 sysroot** constrains the Linux natives (finding 1).

## Next steps

- Phase 1, once scheduled; the C ABI's close before phase 3.
- Remove `.github/workflows/kmp-spike-ios.yml` before any of this branch is proposed for main.
