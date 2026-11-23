---
"jazz-tools": patch
---

A session follows its client signing out by itself (Rust's `JazzSession` and, through jazz-ffi, Go, Swift and C). When the app's bearer named no one at a renewal or refresh, the client stopped but the session stayed `ready`, and signing the same user in again kept the stopped client. Now the session reads `signed-out` with a new `clientEpoch`, its old handles fail `replaced`, and `refresh` opens a working client for the same user.
