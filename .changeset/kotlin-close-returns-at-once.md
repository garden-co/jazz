---
"jazz-tools": patch
---

jazz-kotlin: `close()` returns at once, so it's safe on Android's main thread and at the end of `use {}`. The app is closed from then on: new calls throw `JazzException.Closed`, subscriptions end and a bearer provider isn't asked again, while the native core finishes shutting down in the background. The new `closeAndWait()` waits for that and throws what it threw. Opening the same `dataDir` again, or `deleteClientStorage` on another app of it, waits for that teardown by itself.
