# Kotlin Bindings — TODO

Kotlin client library for jazz2 on Android and JVM.

## Overview

Native Kotlin integration for Android and server-side JVM:

- Rust → JNI (via `jni` crate) → Kotlin wrapper
- Kotlin-idiomatic API: coroutines, Flow for reactive queries, suspend functions
- SurrealKV storage on device (Android internal storage)
- Jetpack Compose integration: `collectAsState()` from query Flows
- Maven Central / Gradle distribution with pre-built `.so` for Android ABIs

## Open Questions

- JNI vs. JNA vs. UniFFI (Kotlin support)?
- Kotlin Multiplatform (KMP) for shared code with iOS?
- Android lifecycle integration (sync pause/resume with Activity lifecycle)?
- ProGuard/R8 rules for the native library?
- Server-side JVM use case: Kotlin on the backend with embedded groove?
