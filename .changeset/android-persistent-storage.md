---
"jazz-tools": patch
---

Persistent storage works on Android in `jazz-kotlin-android`. Until now every `Jazz.open` with `Storage.Persistent` failed there with `IO error: try_lock_shared() not supported` (alpha.66 to beta.0): Rust's standard library has no file locks on Android, so the store's lock now calls `flock` itself there.
