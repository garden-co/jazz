---
"jazz-tools": patch
---

**Breaking (Kotlin, Maven users):** jazz-kotlin is now one Kotlin Multiplatform library with JVM and Android targets. On Maven Central, `tools.jazz:jazz-kotlin` now holds only Gradle module metadata, and the JVM jar moved to `tools.jazz:jazz-kotlin-jvm`. With Gradle nothing changes: `implementation("tools.jazz:jazz-kotlin:<version>")` picks `jazz-kotlin-jvm` on the JVM and `tools.jazz:jazz-kotlin-android` (the same coordinates as before) on Android. Maven builds and other builds that don't read module metadata must depend on `tools.jazz:jazz-kotlin-jvm` instead. The jar and AAR attached to GitHub releases keep their names (`jazz-kotlin-<version>.jar`, `jazz-kotlin-android-<version>.aar`) and contents.
