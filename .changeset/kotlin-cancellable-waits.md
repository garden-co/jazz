---
"jazz-tools": patch
---

**Kotlin:** cancelling a coroutine, or `withTimeout`, now ends a wait on the server at once: `waitForSync`, a write at `WriteTier.Global` and a read at `ReadTier.Remote` no longer wait until the server answers or the app closes. A write that was already saved locally throws `JazzException.StillQueued` (new), with the cancellation as its cause: don't retry it, it may still sync, and a later refusal reaches `mutationErrors`.
