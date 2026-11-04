---
"jazz-tools": patch
---

Dropping a Rust client subscription stream now unsubscribes right away, ahead of whatever the client sends next on the same connection, instead of on the next tick. Before, a write landing in between could still reach the dropped subscription's local store.
