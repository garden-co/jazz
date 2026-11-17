# jazz-kotlin

> **Experimental:** client only, backends are TypeScript. See [Language Support](https://jazz.tools/docs/reference/language-support#jazz-environment-kotlin).

The Kotlin runtime for Jazz apps (JVM and Android apps; your app's main backend runs in TypeScript): `suspend` calls and `Flow` subscriptions over
[`jazz-ffi`](../../crates/jazz-ffi) (UniFFI, through JNA), for the typed bindings
`npx jazz-tools generate` writes from your `schema.ts` to `src/main/kotlin/JazzApp.kt`, beside a `build.gradle.kts`.

```kotlin
val jazz = jazzapp.open(JazzConfig(appId = …, serverUrl = "https://app.example/jazz", auth = JazzAuth.DeviceAccount(key)))
jazz.update(Tasks, task.id) { status(TaskStatus.Done) }
jazz.watch(Tasks.where(Tasks.showId eq show.id)).collect { println(it) }
```

`gradle build` builds `crates/jazz-ffi` with cargo and generates its Kotlin bindings with
`uniffi-bindgen` first; `-PjazzFfi=skip` builds without them (the tests use a fake native core).
Kotlin Multiplatform isn't supported. See `docs/content/docs/setup/kotlin.mdx`.

## Android

[`packages/jazz-kotlin-android`](../jazz-kotlin-android) builds the same sources as an
Android library (AAR, minSdk 24): `jazz-ffi` cross-compiled with
[cargo-ndk](https://github.com/bbqsrc/cargo-ndk) into `jniLibs` for arm64-v8a,
armeabi-v7a and x86_64, the UniFFI bindings generated from that library, and JNA's
Android AAR. The API is the JVM library's, unchanged; consumer R8 rules keep what JNA
and `Jazz.open` reach by reflection.

```sh
dev/scripts/test-kotlin-android.sh   # fetches the pinned JDK, Gradle, SDK and NDK, then builds the AAR
# or, with ANDROID_HOME (platforms;android-36, build-tools;36.0.0, ndk;27.1.12297006) and cargo-ndk set up:
gradle -p packages/jazz-kotlin-android assembleRelease   # -PcargoProfile=dev for a quicker build
```

The AAR is `packages/jazz-kotlin-android/build/outputs/aar/jazz-kotlin-android-release.aar`.
CI only checks that it builds (the `kotlin-android` partition); there are no emulator or
device tests yet.
