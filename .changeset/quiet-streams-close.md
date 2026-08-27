---
"jazz-tools": patch
---

Ensure Rust client shutdown cancels retained subscription forwarding before closing persistent local storage, allowing the same client directory to reopen immediately.
