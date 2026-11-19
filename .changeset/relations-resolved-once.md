---
"jazz-tools": patch
---

An app's relations resolve the same way in `jazz-tools validate`, backends, Swift/Kotlin/Go and TS. `jazz.app.json` is now also refused when a relation is named `__proto__`, `constructor` or `prototype`, as TS already refused it. Swift/Kotlin/Go now refuse to open an app whose relations don't resolve (for example, a reverse relation that doesn't reverse a forward one pointing back), before opening anything, instead of dropping or misreading that relation; nothing else about opening changes.
