---
"jazz-tools": patch
---

**Kotlin:** jazz-kotlin is now one Kotlin Multiplatform library with JVM and Android targets. With Gradle, `implementation("tools.jazz:jazz-kotlin:<version>")` picks the right one by itself: `tools.jazz:jazz-kotlin-jvm` on the JVM, `tools.jazz:jazz-kotlin-android` (the same coordinates as before) on Android. Maven (no Gradle module metadata) users depend on `tools.jazz:jazz-kotlin-jvm`. The jar and AAR attached to GitHub releases are unchanged.
