---
"jazz-tools": patch
---

An app's relations resolve the same way in `jazz-tools validate`, backends, Swift/Kotlin/Go and TS. `jazz.app.json` is refused when a relation's column isn't a uuid or uuid array, or when a relation is named `__proto__`, `constructor` or `prototype`. Swift/Kotlin/Go refuse to open an app whose reverse relation doesn't resolve, instead of dropping it.
