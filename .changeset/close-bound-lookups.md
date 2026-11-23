---
"jazz-tools": patch
---

Closing the Rust client and the native bindings no longer waits for a host name lookup still in progress, which no timeout bounds (an unresponsive DNS server could hold up close for as long as the system resolver takes). The store is let go of all the same, so it reopens at once.
