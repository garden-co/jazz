---
"jazz-tools": patch
---

Fix a client that stops syncing after it receives a streamed large value it did not have yet. When the value's content arrived while later updates that reference it were still being applied, the client could wait forever and stop sending and receiving until the page was reloaded.
