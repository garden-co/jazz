---
"jazz-tools": patch
---

**Behaviour change (Kotlin):** a call whose coroutine is cancelled before it starts does nothing: `watch` and `subscribe` no longer subscribe from a cancelled collector (every other call already threw first). A `transaction` whose caller is cancelled before its commit starts now rolls back and throws the cancellation, even if the block returned normally; a commit that has started stands.
