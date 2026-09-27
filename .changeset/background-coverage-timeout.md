---
"jazz-tools": patch
---

Keep the server connection when the background coverage refresh of a local read times out. Native bindings report the timeout as `NotObserved: Timed out waiting for query coverage`, which was treated as a transport failure and tore down the connection, failing every pending write.
