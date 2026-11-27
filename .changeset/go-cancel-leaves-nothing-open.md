---
"jazz-tools": patch
---

**Fix (Go, experimental):** a cancelled context no longer leaves anything open. A `Tx` cancelled while it begins, or ended by `runtime.Goexit` (`t.Fatal`), rolls back. A streaming upload cancelled while it begins, or before its reader is done, is aborted. A cancelled `Open` or `OpenSession` stops asking the token provider and returns once the app it was opening is closed again, so its store is free for the next open. A cancelled `SignOut` or `DeleteClientStorage` frees the app once it has closed, and a cancelled `Session.DB` returns at once.
