---
"jazz-tools": patch
---

A `remote()` backend now reconnects after its core restarts. While the core was down, a reconnect attempt failed with only a socket error and never a close, so the backend waited on it forever: requests that needed the core hung, and `close()` had to wait out its sync timeout. Failed attempts now end, and the backend keeps retrying until the core is back.
