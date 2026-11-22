---
"jazz-tools": patch
---

Kotlin: a call cancelled while it starts no longer leaks what it was starting. A `Jazz.open` cancelled while it opens closes that app (it used to keep the store open, so a later `deleteClientStorage()` was refused until the process ended), a `transaction` cancelled while it begins rolls back, a streaming write cancelled while it begins aborts its upload, and a `mutationErrors` collection cancelled while it starts stops watching. The KDoc now says when cancelling a transaction or a streaming write doesn't undo it.
