---
"jazz-tools": patch
---

A handle acting for a user (`for_user`) no longer reaches the backend client's session. Signing out on one used to sign out the backend client, which then stopped renewing its token for every user. In Rust, `sign_out` on such a handle fails with `Error::Unsupported` and `session()` is `None`. Through jazz-ffi (Go, Swift, Kotlin, C), reading or watching the session, `refresh`, `link` and `signOut` fail with kind `notSupported`. Call them on the handle `for_user` was called on.
