---
"jazz-tools": patch
---

jazz-ffi (Go, Swift, Kotlin, C) rolls back open transactions and aborts uploads whenever their client stops: signing out, a session's sign-out or change of user, and `deleteClientStorage`, which used to abort no uploads.
