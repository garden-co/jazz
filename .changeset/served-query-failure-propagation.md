---
"jazz-tools": patch
"jazz-napi": patch
"jazz-wasm": patch
"jazz-rn": patch
---

Server-side failures in served subscriptions now reach clients as typed terminal rejections instead of leaving subscriptions waiting indefinitely. Failed routes are retired while independent write Fate and unaffected subscriptions continue.

Permission revocations still evict cached rows when the triggering write updated the row or the revocation was queued behind the rejection. Rust client streams end after delivering the rejection. React Native decodes rejection messages from older compatible native builds as `Internal` without exposing their diagnostic text.
