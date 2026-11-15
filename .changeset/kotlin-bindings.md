---
"jazz-tools": patch
---

The Rust `jazz-tools generate` (crates/jazz-cli) writes Kotlin bindings (`src/main/kotlin/JazzApp.kt`) for an app whose `build.gradle.kts` beside `jazz.app.json` depends on `jazz-kotlin`, or with `--lang kotlin`. The npm `jazz-tools` CLI doesn't generate Kotlin yet.
