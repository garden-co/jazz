---
"jazz-tools": patch
---

A server now tells a client from 2.0.0-alpha.59 or earlier to upgrade, with an `unsupported_protocol_version` error that client can read. Before, the old client failed with "websocket received semantic frame before server hello", because the server's error frame used a newer layout.
