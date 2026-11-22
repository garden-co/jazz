---
"jazz-tools": patch
---

Kotlin: the `jazz-kotlin` sources jar holds sources only (it used to pack every platform's native library, about 54 MB a release); `Jazz.session` can't be cast to a `MutableStateFlow`; and a `deleteClientStorage()` that fails after the app has stopped ends its watches, with the session reading `SignedOut`. `signOut()` and `close()` do the same when they throw, and still rethrow. A delete refused because another app of the same process holds the store still leaves the app open.
