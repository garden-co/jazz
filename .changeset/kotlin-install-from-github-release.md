---
"jazz-tools": patch
---

**Breaking (Kotlin, experimental):** from beta.1, `jazz-kotlin` and `jazz-kotlin-android` aren't published to Maven Central. Until they're in a Maven repository of our own, install the jar or AAR from the GitHub release, check it against `SHA256SUMS-languages`, and declare `kotlinx-serialization-json` 1.9.0, `kotlinx-coroutines-core` 1.10.2 and JNA 5.17.0 (`@aar` on Android) yourself: see [Upgrading from beta.0 to beta.1](https://jazz.tools/docs/operating/backups-and-upgrades#upgrading-from-beta0-to-beta1). Versions already on Maven Central stay there.
