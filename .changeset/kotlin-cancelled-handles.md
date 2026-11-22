---
"jazz-tools": patch
---

Kotlin: a call cancelled while it starts something no longer leaks it. A cancelled `Jazz.open` closes the app it opened (it used to keep the store open, so a later `deleteClientStorage()` was refused until the process ended), a cancelled `transaction` rolls back, a cancelled streaming write aborts its upload, and a cancelled `mutationErrors` collection stops watching.
