---
"jazz-tools": patch
---

Own the edge server's upstream connector across bootstrap, retry, established disconnect, and shutdown. Reconnect transient failures with bounded exponential backoff, expose fatal lifecycle health, detach the exact upstream peer before retry, and cancel and join connector work before storage closes.
