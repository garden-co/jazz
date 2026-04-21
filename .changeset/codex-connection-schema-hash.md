---
"jazz-tools": patch
---

Fix misleading schema-mismatch recovery guidance during client/server handshakes.

The transport handshake now sends the client's declared structural schema hash separately from the catalogue-state digest, so server-side connection diagnostics only suggest migrations for real schema hashes that the CLI can resolve.
