---
"jazz-tools": patch
---

jazz-kotlin and jazz-kotlin-android can publish to Maven Central as `tools.jazz:jazz-kotlin` and `tools.jazz:jazz-kotlin-android`, signed, at jazz-tools' version ("Publish jazz-kotlin", dry run first). Their Gradle builds now read that version from jazz-tools' `package.json` instead of hard-coding it.
