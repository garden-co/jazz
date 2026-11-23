---
"jazz-tools": patch
---

Rust, Swift, Kotlin and Go clients trust the CAs the OS trusts for HTTPS requests too (sign-in, linking, `for_user`, the HTTP stream fallback and a backend's requests to its server), as their WebSocket already did, so they work behind a private or corporate CA. The public roots built in are still trusted, also when the OS store is empty or unreadable. The OS store is read once per process: a CA installed later needs a restart. On Android and iOS, CAs installed on the device are still not trusted.
