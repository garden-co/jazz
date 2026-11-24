---
"jazz-tools": patch
---

Jazz for Swift: cancelling the calling task now ends a wait on the server at once, as in TypeScript and Kotlin. `waitForSync`, a write or transaction commit at `.global`, and a read at `.remote` no longer wait until the server answers or the app closes. A write already saved locally throws `JazzError.stillQueued` (new): don't retry it, it may still sync, and a later refusal reaches `mutationErrors`. A task cancelled before a call starts does nothing, and a transaction cancelled before its commit rolls back. `JazzError` gains the `stillQueued` case, so code switching over every case needs it.
