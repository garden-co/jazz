---
"jazz-tools": patch
---

A client that reconnects after its connection to the server drops no longer fails with a `channel credit sequence mismatch`. Server messages the dropped connection had received but not yet processed are now discarded instead of being fed into the new connection.
